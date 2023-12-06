from __future__ import annotations

import functools
import io
import os
import re
from datetime import datetime, timedelta
from io import SEEK_CUR, SEEK_END, SEEK_SET
from pathlib import Path
from typing import TYPE_CHECKING, Generator, Literal, Optional

import sentry_sdk
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

# pylint: disable=wrong-import-position
from azure.storage.file.fileservice import FileService
from azure.storage.file.models import FilePermissions
from azure.storage.file.sharedaccesssignature import FileSharedAccessSignature
from azure.storage.fileshare import (
    CorsRule,
    ShareDirectoryClient,
    ShareFileClient,
    ShareServiceClient,
)
from dotenv import load_dotenv
from pydantic import BaseModel
from slugify import slugify

if TYPE_CHECKING:
    from auth import User

# pylint: disable=wrong-import-position
from ._storage import BaseStorageAzureClient

load_dotenv()


RunDataTypeType = Literal["processed_data", "raw_data", "HDF5"]


class RunDataNotFound(Exception):
    pass


class ProjectDocumentsNotFound(Exception):
    pass


class FolderCreationError(Exception):
    def __init__(self, message, *args) -> None:
        self.message = str(message)
        super().__init__(*args)


class IncorrectDataFilePath(Exception):
    def __init__(self, message: str, *args: object):
        self.message = message
        super().__init__(*args)


class ProjectFile(BaseModel):
    name: str
    last_modified: Optional[datetime] = None
    size: int
    path: Optional[str]


class AzureFileShareFile(io.BytesIO):
    """File-like object for Azure File Share files."""

    _offset = 0
    _content_length: int | None = None

    @property
    def content_length(self) -> int:
        """Get the content length of the file."""
        if self._content_length is None:
            self._content_length = self.file_service.get_file_properties(
                self.share_name,
                self.directory_name,
                self.file_name,
            ).properties.content_length
        return self._content_length

    def __init__(
        self,
        file_service: FileService,
        share_name: str,
        directory_name: str,
        file_name: str,
    ) -> None:
        self.file_service = file_service
        self.share_name = share_name
        self.directory_name = directory_name
        self.file_name = file_name
        super().__init__()

    @functools.lru_cache
    def _read_chunk(self, start_range: int, end_range: int) -> tuple[bytes, int]:
        file = self.file_service.get_file_to_bytes(
            self.share_name,
            self.directory_name,
            self.file_name,
            start_range=start_range,
            end_range=end_range,
        )
        return file.content, file.properties.content_length

    def read(self, size: int | None = -1) -> bytes:
        if not size:
            return b""
        end_range = self._offset + size - 1 if size > -1 else None
        content, file_content_length = self._read_chunk(self._offset, end_range)
        self._offset = (
            self._offset + size if end_range is not None else file_content_length
        )
        return content

    def readinto(self, buffer) -> int:
        data = self.read(len(buffer))
        buffer[: len(data)] = data
        return len(data)

    def seek(self, offset: int, whence: int = SEEK_SET) -> int:
        if whence == SEEK_SET or whence is None:
            self._offset = offset
        if whence == SEEK_CUR:
            self._offset += offset
        if whence == SEEK_END:
            self._offset = self.content_length - offset
        return self._offset

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._offset

    def truncate(self, size: int | None = None) -> int:
        return super().truncate(size)


class DataAzureClient(BaseStorageAzureClient):
    def __init__(self):
        super().__init__()
        self._file_shared_access_signature = FileSharedAccessSignature(
            account_name=self.storage_account_name, account_key=self._storage_key
        )
        self.share_name = os.environ["AZURE_STORAGE_FILESHARE"]

    def get_project_documents(
        self,
        project_name: str,
    ) -> list[ProjectFile]:
        dir_path = os.path.join(_generate_base_dir_path(project_name), "documents")
        files = self._list_files_recursive(dir_path, fetch_detailed_information=True)
        try:
            return list(files)
        except ResourceNotFoundError as error:
            raise ProjectDocumentsNotFound from error

    def get_run_files(
        self,
        project_name: str,
        run_name: str,
        data_type: RunDataTypeType,
    ) -> list[ProjectFile]:
        """Fetches run data files from Fileshare.
        Specify `data_type` to get either 'raw_data', 'processed_data' or 'HDF5'.
        """
        dir_path = os.path.join(
            _generate_base_dir_path(project_name, run_name), data_type
        )
        files = self._list_files_recursive(dir_path)
        try:
            return list(files)
        except ResourceNotFoundError as error:
            raise RunDataNotFound from error

    def iter_project_run_files(
        self, project_name: str, run_name: str, data_type: RunDataTypeType | None = None
    ):
        """
        Yield files from a run directory.

        Args:
            project_name (str): The name of the project.
            run_name (str): The name of the run.
            data_type (RunDataTypeType | None, optional): The data type folder in the run.

        Returns:
            Iterator[ProjectFile]: An iterator of ProjectFile objects representing
            the files in the run directory.
        """
        projects_path_prefix = _get_projects_path()
        dir_path = os.path.join(projects_path_prefix, project_name, "runs", run_name)
        if data_type:
            os.path.join(data_type)
        return self._iter_directory_files(dir_path)

    def download_run_file(
        self,
        filepath: str,
    ):
        """Return a downloader for a share file."""
        directory_name = os.path.dirname(filepath)
        file_name = os.path.basename(filepath)

        return AzureFileShareFile(
            FileService(connection_string=self._storage_connection_string),
            self.share_name,
            directory_name,
            file_name,
        )

    def is_project_data_available(self, project_name: str) -> bool:
        """Check if project data is available on Fileshare."""
        dir_client = ShareDirectoryClient.from_connection_string(
            conn_str=self._storage_connection_string,
            share_name=self.share_name,
            directory_path=os.path.join(_generate_base_dir_path(project_name), "runs"),
        )
        try:
            run_folders = [
                entry["name"]
                for entry in dir_client.list_directories_and_files()
                if entry["is_directory"]
            ]
        except ResourceNotFoundError as error:
            sentry_sdk.capture_exception(error)
            return False
        for run_folder in run_folders:
            # check if raw_data folder has any file or folder
            if list(
                dir_client.get_subdirectory_client(
                    f"{run_folder}/raw_data"
                ).list_directories_and_files()
            ):
                return True
        return False

    def generate_run_data_sas_url(
        self,
        dir_path: str,
        file_name: str,
        is_admin: bool,
    ):
        """Generate URL with Shared Access Signature to manage run data in an
        Azure Fileshare. Regular users can read. Admins can also write, create & delete.
        """
        permission = FilePermissions(
            read=True, create=is_admin, write=is_admin, delete=is_admin
        )
        return self._generate_sas_url(dir_path, file_name, permission)

    def generate_project_documents_upload_sas_url(
        self, project_name: str, file_name: str
    ):
        """Generate URL with Shared Access Signature to manage project documents in
        an Azure Fileshare. Permission are write & create. To download and delete use
        generate_project_documents_sas_url.
        """
        dir_path = os.path.join(_generate_base_dir_path(project_name), "documents")
        permission = FilePermissions(read=False, create=True, write=True, delete=False)
        return self._generate_sas_url(dir_path, file_name, permission)

    def generate_project_documents_sas_url(self, dir_path: str, file_name: str):
        """Generate URL with Shared Access Signature to manage project documents in
        an Azure Fileshare. Permission are read & delete. To upload a document use
        generate_project_documents_upload_sas_url.
        """
        permission = FilePermissions(read=True, create=False, write=False, delete=True)
        return self._generate_sas_url(dir_path, file_name, permission)

    def _generate_sas_url(
        self,
        dir_path: str,
        file_name: str,
        permission: FilePermissions,
    ) -> str:
        """Generate a signed URL (Shared Access Signature) that can be used
        to perform authenticated operations on a file in an Azure Fileshare.
        """
        sas_params = self._file_shared_access_signature.generate_file(
            share_name=self.share_name,
            directory_name=dir_path,
            file_name=file_name,
            permission=permission,
            expiry=datetime.utcnow() + timedelta(minutes=5),
            start=datetime.utcnow(),
        )
        # pylint: disable=line-too-long
        return f"https://{self.storage_account_name}.file.core.windows.net/{self.share_name}/{dir_path}/{file_name}?{sas_params}"

    def init_project_directory(self, project_name: str):
        """Create project folder on Fileshare with empty children folders (documents, runs)."""  # noqa: E501
        dir_client = ShareDirectoryClient.from_connection_string(
            conn_str=self._storage_connection_string,
            share_name=self.share_name,
            directory_path=_generate_base_dir_path(project_name),
        )
        try:
            dir_client.create_directory()
            dir_client.create_subdirectory("documents")
            dir_client.create_subdirectory("runs")
        except (ResourceNotFoundError, ResourceExistsError) as error:
            raise FolderCreationError(error.message) from error

    def init_run_directory(self, run_name: str, project_name: str):
        """Create run folder in project folder on Fileshare
        with empty children folders (processed_data, raw_data)."""
        dir_client = ShareDirectoryClient.from_connection_string(
            conn_str=self._storage_connection_string,
            share_name=self.share_name,
            directory_path=_generate_base_dir_path(project_name, run_name),
        )
        try:
            dir_client.create_directory()
            dir_client.create_subdirectory("processed_data")
            dir_client.create_subdirectory("raw_data")
        except (ResourceNotFoundError, ResourceExistsError) as error:
            raise FolderCreationError(error.message) from error

    def rename_run_directory(self, run_name: str, project_name: str, new_name: str):
        """Change run folder name in project folder on Fileshare."""
        dir_client = ShareDirectoryClient.from_connection_string(
            conn_str=self._storage_connection_string,
            share_name=self.share_name,
            directory_path=_generate_base_dir_path(project_name, run_name),
        )
        try:
            dir_client.rename_directory(
                _generate_base_dir_path(project_name, new_name),
                overwrite=False,
            )
        except (ResourceNotFoundError, ResourceExistsError) as error:
            raise FolderCreationError(error.message) from error

    def set_fileshare_cors_policy(self, allowed_origins: str):
        """Set Cross-Origin Resource Sharing (CORS) for Azure Fileshare.

        Arguments:
        allowed_origins -- A string representing allowed origins as specified here :
            https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Access-Control-Allow-Origin
            Multiple origins can be specified by separating them with commas.
        """
        file_service = ShareServiceClient.from_connection_string(
            self._storage_connection_string
        )
        file_service.set_service_properties(
            cors=[
                CorsRule(
                    allowed_origins=allowed_origins.split(","),
                    allowed_methods=["DELETE", "GET", "HEAD", "POST", "OPTIONS", "PUT"],
                    allowed_headers=[
                        "x-ms-content-length",
                        "x-ms-type",
                        "x-ms-version",
                        "x-ms-write",
                        "x-ms-range",
                        "content-type",
                    ],
                )
            ]
        )

    def _list_files_recursive(
        self, dir_path: str, fetch_detailed_information: bool = False
    ) -> Generator[ProjectFile, None, None]:
        """
        List files from FileShare on Azure Storage Account.

        Parameters
        ----------
        dir_path: str
            Directory path to list files from.
        recursive: bool
            Specifies whether to list files recursively.
        fetch_detailed_information: bool
            If True will make a request for each file to get more metadata
            about it.

        Returns
        -------
        files_list: Iterator[azure_client.ProjectFile]
            List of file properties from FileShare.

        Notes
        -----
        This method only lists files, ignoring empty directories.

        References
        ----------
        .. [1] Credit to joao8tunes :
            https://stackoverflow.com/questions/66532170/azure-file-share-recursive-directory-search-like-os-walk
        .. [2] Recursive files listing: https://stackoverflow.com/a/66543222/16109419
        """

        dir_client = ShareDirectoryClient.from_connection_string(
            conn_str=self._storage_connection_string,
            share_name=self.share_name,
            directory_path=dir_path,
        )

        # Listing files from current directory path:
        for file in dir_client.list_directories_and_files():
            name, is_directory = file["name"], file["is_directory"]
            path = os.path.join(dir_path, name)

            if is_directory:
                # Listing files recursively:
                childrens = self._list_files_recursive(
                    dir_path=path,
                )

                for child in childrens:
                    yield child
            else:
                if fetch_detailed_information:
                    file_client = ShareFileClient.from_connection_string(
                        conn_str=self._storage_connection_string,
                        share_name=self.share_name,
                        file_path=path,
                    )
                    yield ProjectFile(**dict(file_client.get_file_properties()))
                else:
                    yield ProjectFile(**{**file, "path": path})

    def _iter_directory_files(self, dir_path: str):
        """Stream a directory from the Fileshare."""
        dir_client = ShareDirectoryClient.from_connection_string(
            conn_str=self._storage_connection_string,
            share_name=self.share_name,
            directory_path=dir_path,
        )
        if not dir_client.exists():
            raise RunDataNotFound()
        files = self._list_files_recursive(dir_path)
        for file in files:
            file_client = ShareFileClient.from_connection_string(
                conn_str=self._storage_connection_string,
                share_name=self.share_name,
                file_path=file.path,
            )
            yield file_client.download_file()


def extract_info_from_path(path: Path):
    """Extract project and run name from a path."""
    _validate_run_data_file_path_regex(path)
    projects_path_prefix = _get_projects_path()
    path_without_prefix = Path(str(path).replace(projects_path_prefix + "/", "", 1))
    info: dict[str, str | None] = {
        "project_name": None,
        "run_name": None,
        "data_type": None,
    }
    if len(path_without_prefix.parts) > 0:
        info["project_name"] = path_without_prefix.parts[0]
    if len(path_without_prefix.parts) > 2:
        info["run_name"] = path_without_prefix.parts[2]
    if len(path_without_prefix.parts) > 3:
        info["data_type"] = path_without_prefix.parts[3]
    return info


def validate_run_data_file_path(path: Path, current_user: User):
    _validate_run_data_file_path_regex(path)
    _validate_project_file_path(path, current_user)


def validate_project_document_file_path(path: Path, current_user: User):
    if not re.match(rf"^{_get_projects_path()}\/[\w\- ]+\/documents", str(path)):
        raise IncorrectDataFilePath(
            "path must start with {projects_path_prefix}/<project_name>/documents/"
        )
    _validate_project_file_path(path, current_user)


def _get_projects_path():
    return os.getenv("AZURE_STORAGE_PROJECTS_LOCATION_PREFIX", "")


def _validate_project_file_path(path: Path, current_user: User):
    """Given a path, validate the path is valid for project data and the user has
    permission to access it.
    """
    projects_path_prefix = _get_projects_path()
    path_without_prefix = Path(str(path).replace(projects_path_prefix + "/", "", 1))
    project_name = path_without_prefix.parts[0]
    if not current_user.has_project(project_name) and not current_user.is_admin:
        raise IncorrectDataFilePath(f"user is not part of project {project_name}")


def _generate_base_dir_path(project_name: str, run_name: str = ""):
    """Generate a path to a directory in the fileshare for a project and a run if `run_name`
    is passed as a parameter."""
    base_dir_path = os.path.join(_get_projects_path(), slugify(project_name))
    if run_name:
        base_dir_path = os.path.join(base_dir_path, "runs", run_name)
    return base_dir_path


def _validate_run_data_file_path_regex(path: Path):
    if not re.match(
        rf"^{_get_projects_path()}\/[\w\- ]+\/runs\/[\w\- ]+\/(raw_data|processed_data|HDF5)",  # noqa: E501
        str(path),
    ):
        # pylint: disable=line-too-long
        raise IncorrectDataFilePath(
            "path must start with {projects_path_prefix}/<project_name>/runs/<run_name>/(processed_data|raw_data|HDF5)/"  # noqa: E501
        )
