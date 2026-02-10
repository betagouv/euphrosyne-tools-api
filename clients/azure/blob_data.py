from __future__ import annotations

import asyncio
import functools
import io
import os
from datetime import datetime, timedelta, timezone
from io import SEEK_CUR, SEEK_END, SEEK_SET
from typing import AsyncIterator

import sentry_sdk
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.blob import (
    BlobClient,
    BlobPrefix,
    BlobSasPermissions,
    ContainerSasPermissions,
    generate_blob_sas,
    generate_container_sas,
)

from ..data_client import AbstractDataClient
from ..data_models import (
    ProjectFileOrDirectory,
    RunDataTypeType,
    SASCredentials,
    TokenPermissions,
)
from .blob import BlobAzureClient
from .data import (
    FolderCreationError,
    ProjectDocumentsNotFound,
    RunDataNotFound,
    _generate_base_dir_path,
    _get_projects_path,
)
from .utils import iterate_blocking


class AzureBlobFile(io.BytesIO):
    """File-like object for Azure Blob Storage blobs."""

    _offset = 0
    _content_length: int | None = None

    def __init__(self, blob_client: BlobClient) -> None:
        self.blob_client = blob_client
        super().__init__()

    @property
    def content_length(self) -> int:
        """Get the content length of the blob."""
        if self._content_length is None:
            self._content_length = self.blob_client.get_blob_properties().size
        return self._content_length

    @functools.lru_cache(maxsize=128)
    def _read_chunk(self, start_range: int, length: int | None) -> bytes:
        downloader = self.blob_client.download_blob(
            offset=start_range,
            length=length,
        )
        return downloader.readall()

    def read(self, size: int | None = -1) -> bytes:
        if size == 0:
            return b""
        if size is None or size < 0:
            length = None
        else:
            remaining = self.content_length - self._offset
            if remaining <= 0:
                return b""
            length = min(size, remaining)
        content = self._read_chunk(self._offset, length)
        self._offset += len(content)
        return content

    def readinto(self, buffer) -> int:
        data = self.read(len(buffer))
        buffer[: len(data)] = data
        return len(data)

    def seek(self, offset: int, whence: int = SEEK_SET) -> int:
        if whence == SEEK_SET or whence is None:
            self._offset = offset
        elif whence == SEEK_CUR:
            self._offset += offset
        elif whence == SEEK_END:
            self._offset = self.content_length - offset
        return self._offset

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._offset

    def truncate(self, size: int | None = None) -> int:
        return super().truncate(size)


class BlobDataAzureClient(BlobAzureClient, AbstractDataClient):
    """Client to read / write data on Azure Blob Storage."""

    def __init__(self):
        container_name = os.environ.get("AZURE_STORAGE_DATA_CONTAINER")
        if not container_name:
            raise ValueError(
                "AZURE_STORAGE_DATA_CONTAINER environment variable is not set"
            )
        super().__init__(container_name=container_name)

    def list_project_dirs(self) -> list[str]:
        """Returns all directory names in project folder."""
        prefix = self._dir_prefix(_get_projects_path())
        dirs: list[str] = []
        seen: set[str] = set()
        for entry in self.container_client.walk_blobs(
            name_starts_with=prefix,
            delimiter="/",
        ):
            if isinstance(entry, BlobPrefix):
                name = entry.name
            else:
                continue
            if prefix and name.startswith(prefix):
                name = name[len(prefix) :]
            name = name.strip("/")
            if name and name not in seen:
                seen.add(name)
                dirs.append(name)
        return dirs

    def get_project_documents(
        self,
        project_name: str,
    ) -> list[ProjectFileOrDirectory]:
        dir_path = os.path.join(_generate_base_dir_path(project_name), "documents")
        if not self._path_exists(dir_path):
            raise ProjectDocumentsNotFound()
        return self._list_files(dir_path)

    def get_run_files_folders(
        self,
        project_name: str,
        run_name: str,
        data_type: RunDataTypeType,
        folder: str | None = None,
    ) -> list[ProjectFileOrDirectory]:
        """Fetches run data files from Blob Storage.
        Specify `data_type` to get either 'raw_data' or 'processed_data'.
        """
        dir_path = os.path.join(
            _generate_base_dir_path(project_name), "runs", run_name, data_type
        )
        if folder is not None:
            dir_path = os.path.join(dir_path, folder)
        if not self._path_exists(dir_path):
            raise RunDataNotFound()
        return self._list_files(dir_path)

    async def iter_project_run_files_async(
        self, project_name: str, run_name: str, data_type: RunDataTypeType | None = None
    ) -> AsyncIterator[object]:
        """Yield files from a run directory."""
        dir_path = _generate_base_dir_path(
            project_name=project_name, run_name=run_name, data_type=data_type
        )
        if not self._path_exists(dir_path):
            raise RunDataNotFound()
        prefix = self._dir_prefix(dir_path)
        blobs = self.container_client.list_blobs(name_starts_with=prefix)
        async for blob in iterate_blocking(blobs):
            if blob.name.endswith("/"):
                continue
            blob_client = self.container_client.get_blob_client(blob.name)
            yield asyncio.to_thread(blob_client.download_blob)

    def download_run_file(
        self,
        filepath: str,
    ) -> io.BytesIO:
        """Return a downloader for a blob."""
        blob_name = self._normalize_blob_path(filepath)
        blob_client = self.container_client.get_blob_client(blob_name)
        return AzureBlobFile(blob_client)

    def is_project_data_available(self, project_name: str) -> bool:
        """Check if project data is available on Blob Storage."""
        runs_path = os.path.join(_generate_base_dir_path(project_name), "runs")
        if not self._path_exists(runs_path):
            try:
                self.init_project_directory(project_name)
            except FolderCreationError as error:
                sentry_sdk.capture_exception(error)
            return False
        runs_prefix = self._dir_prefix(runs_path)
        try:
            run_entries = [
                entry
                for entry in self.container_client.walk_blobs(
                    name_starts_with=runs_prefix,
                    delimiter="/",
                )
                if isinstance(entry, BlobPrefix)
            ]
        except ResourceNotFoundError as error:
            sentry_sdk.capture_exception(error)
            try:
                self.init_project_directory(project_name)
            except FolderCreationError as init_error:
                sentry_sdk.capture_exception(init_error)
            return False
        for entry in run_entries:
            run_name = entry.name[len(runs_prefix) :].strip("/")
            raw_path = os.path.join(runs_path, run_name, "raw_data")
            raw_prefix = self._dir_prefix(raw_path)
            try:
                has_content = any(
                    not blob.name.endswith("/")
                    for blob in self.container_client.list_blobs(
                        name_starts_with=raw_prefix
                    )
                )
            except ResourceNotFoundError as error:
                sentry_sdk.capture_exception(error)
                has_content = False
            if has_content:
                return True
            if not self._path_exists(raw_path):
                try:
                    self.init_run_directory(run_name, project_name)
                except FolderCreationError as error:
                    sentry_sdk.capture_exception(error)
                return False
        return False

    def generate_run_data_upload_sas(
        self,
        project_name: str,
        run_name: str,
        data_type: RunDataTypeType | None = None,
    ) -> SASCredentials:
        """Generate URL with Shared Access Signature to upload run data."""
        dir_path = _generate_base_dir_path(project_name, run_name, data_type)
        now = datetime.now(timezone.utc)
        token = generate_container_sas(
            account_name=self.storage_account_name,
            container_name=self.container_name,
            account_key=self._storage_key,
            permission=ContainerSasPermissions(
                read=False,
                add=True,
                write=True,
                create=True,
                delete=False,
                list=False,
            ),
            expiry=now + timedelta(hours=1),
            start=now,
        )
        dir_path = self._dir_prefix(dir_path)
        url = (
            f"https://{self.storage_account_name}.blob.core.windows.net/"
            f"{self.container_name}/{dir_path}"
        )
        return {"url": url, "token": token}

    def generate_run_data_sas_url(
        self,
        dir_path: str,
        file_name: str,
        is_admin: bool,
    ) -> str:
        """Generate a signed URL to manage run data in Azure Blob Storage."""
        blob_name = self._join_blob_path(dir_path, file_name)
        now = datetime.now(timezone.utc)
        token = generate_blob_sas(
            account_name=self.storage_account_name,
            container_name=self.container_name,
            blob_name=blob_name,
            account_key=self._storage_key,
            permission=BlobSasPermissions(
                read=True,
                create=is_admin,
                write=is_admin,
                delete=is_admin,
                add=is_admin,
            ),
            expiry=now + timedelta(minutes=5),
            start=now,
        )
        return (
            f"https://{self.storage_account_name}.blob.core.windows.net/"
            f"{self.container_name}/{blob_name}?{token}"
        )

    def generate_project_documents_upload_sas_url(
        self, project_name: str, file_name: str
    ) -> str:
        """Generate a signed URL to upload project documents to blob storage."""
        dir_path = os.path.join(_generate_base_dir_path(project_name), "documents")
        blob_name = self._join_blob_path(dir_path, file_name)
        now = datetime.now(timezone.utc)
        token = generate_blob_sas(
            account_name=self.storage_account_name,
            container_name=self.container_name,
            blob_name=blob_name,
            account_key=self._storage_key,
            permission=BlobSasPermissions(
                read=False,
                create=True,
                write=True,
                delete=False,
                add=True,
            ),
            expiry=now + timedelta(minutes=5),
            start=now,
        )
        return (
            f"https://{self.storage_account_name}.blob.core.windows.net/"
            f"{self.container_name}/{blob_name}?{token}"
        )

    def generate_project_documents_sas_url(self, dir_path: str, file_name: str) -> str:
        """Generate a signed URL to download/delete project documents."""
        blob_name = self._join_blob_path(dir_path, file_name)
        now = datetime.now(timezone.utc)
        token = generate_blob_sas(
            account_name=self.storage_account_name,
            container_name=self.container_name,
            blob_name=blob_name,
            account_key=self._storage_key,
            permission=BlobSasPermissions(
                read=True,
                create=False,
                write=False,
                delete=True,
                add=False,
            ),
            expiry=now + timedelta(minutes=5),
            start=now,
        )
        return (
            f"https://{self.storage_account_name}.blob.core.windows.net/"
            f"{self.container_name}/{blob_name}?{token}"
        )

    def generate_project_directory_token(
        self, project_name: str, permission: TokenPermissions
    ) -> str:
        """Generate a token with permissions to manage project directory
        in an Azure Fileshare."""
        now = datetime.now(timezone.utc)
        container_permission = ContainerSasPermissions(
            read=permission.get("read", False),
            write=permission.get("write", False),
            delete=permission.get("delete", False),
            list=permission.get("list", False),
            delete_previous_version=permission.get("delete_previous_version", False),
            add=permission.get("add", False),
            create=permission.get("create", False),
            update=permission.get("update", False),
            process=permission.get("process", False),
        )
        return generate_container_sas(
            account_name=self.storage_account_name,
            container_name=self.container_name,
            account_key=self._storage_key,
            permission=container_permission,
            expiry=now + timedelta(hours=1),
            start=now,
        )

    def init_project_directory(self, project_name: str):
        """Create project folder in blob storage with empty children folders."""
        base_path = _generate_base_dir_path(project_name)
        if self._path_exists(base_path):
            raise FolderCreationError("project directory already exists")
        try:
            self.container_client.create_container()
        except ResourceExistsError:
            pass
        self._create_directory_marker(base_path)
        self._create_directory_marker(os.path.join(base_path, "documents"))
        self._create_directory_marker(os.path.join(base_path, "runs"))

    def init_run_directory(self, run_name: str, project_name: str) -> None:
        """Create run folder with empty children folders (processed_data, raw_data)."""
        run_path = _generate_base_dir_path(project_name, run_name)
        if self._path_exists(run_path):
            raise FolderCreationError("run directory already exists")
        try:
            self.container_client.create_container()
        except ResourceExistsError:
            pass
        self._create_directory_marker(run_path)
        self._create_directory_marker(os.path.join(run_path, "processed_data"))
        self._create_directory_marker(os.path.join(run_path, "raw_data"))

    def rename_project_directory(self, project_name: str, new_name: str) -> None:
        """Rename the project directory."""
        self._rename_directory(
            directory_path=_generate_base_dir_path(project_name),
            new_directory_path=_generate_base_dir_path(new_name),
        )

    def rename_run_directory(
        self, run_name: str, project_name: str, new_name: str
    ) -> None:
        """Rename the run directory."""
        self._rename_directory(
            directory_path=_generate_base_dir_path(project_name, run_name),
            new_directory_path=_generate_base_dir_path(project_name, new_name),
        )
