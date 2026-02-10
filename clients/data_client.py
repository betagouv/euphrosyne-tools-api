from __future__ import annotations

import abc
import io
from typing import AsyncIterator

from .data_models import (
    ProjectFileOrDirectory,
    RunDataTypeType,
    SASCredentials,
    TokenPermissions,
)


class AbstractDataClient(abc.ABC):
    @abc.abstractmethod
    def list_project_dirs(self) -> list[str]:
        """Returns all directory names in project folder."""

    @abc.abstractmethod
    def get_project_documents(
        self,
        project_name: str,
    ) -> list[ProjectFileOrDirectory]:
        """Return the list of files/directories under a project's documents folder."""

    @abc.abstractmethod
    def get_run_files_folders(
        self,
        project_name: str,
        run_name: str,
        data_type: RunDataTypeType,
        folder: str | None,
    ) -> list[ProjectFileOrDirectory]:
        """Return run data files and folders for a given run."""

    @abc.abstractmethod
    def iter_project_run_files_async(
        self,
        project_name: str,
        run_name: str,
        data_type: RunDataTypeType | None = None,
    ) -> AsyncIterator[
        object
    ]:  # TODO : add better typing than object. We have to check what methods is needed.
        """Yield run files for streaming (provider-specific downloaders)."""

    @abc.abstractmethod
    def download_run_file(
        self,
        filepath: str,
    ) -> io.BytesIO:
        """Return a downloader/file-like object for a run file."""

    @abc.abstractmethod
    def is_project_data_available(self, project_name: str) -> bool:
        """Check if project data is available on the data backend."""

    @abc.abstractmethod
    def generate_run_data_upload_sas(
        self,
        project_name: str,
        run_name: str,
        data_type: RunDataTypeType | None = None,
    ) -> SASCredentials:
        """Generate credentials used to upload run data."""

    @abc.abstractmethod
    def generate_run_data_sas_url(
        self,
        dir_path: str,
        file_name: str,
        is_admin: bool,
    ) -> str:
        """Generate a signed URL to manage run data."""

    @abc.abstractmethod
    def generate_project_documents_upload_sas_url(
        self, project_name: str, file_name: str
    ) -> str:
        """Generate a signed URL to upload project documents."""

    @abc.abstractmethod
    def generate_project_documents_sas_url(self, dir_path: str, file_name: str) -> str:
        """Generate a signed URL to download/delete project documents."""

    @abc.abstractmethod
    def generate_project_directory_token(
        self, project_name: str, permission: TokenPermissions
    ) -> str:
        """Generate credentials used to manage project directory."""

    @abc.abstractmethod
    def init_project_directory(self, project_name: str):
        """Create the project directory with default subfolders."""

    @abc.abstractmethod
    def init_run_directory(self, run_name: str, project_name: str) -> None:
        """Create the run directory with default subfolders."""

    @abc.abstractmethod
    def rename_project_directory(self, project_name: str, new_name: str) -> None:
        """Rename the project directory."""

    @abc.abstractmethod
    def rename_run_directory(
        self, run_name: str, project_name: str, new_name: str
    ) -> None:
        """Rename the run directory."""
