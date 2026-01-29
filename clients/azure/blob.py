from __future__ import annotations

import datetime
import os
import posixpath
import time


from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.blob import (
    BlobPrefix,
    BlobServiceClient,
    ContainerClient,
    ContainerSasPermissions,
    CorsRule,
    generate_container_sas,
)

from ._storage import BaseStorageAzureClient
from .data import FolderCreationError
from ..data_models import ProjectFileOrDirectory


class BlobAzureClient(BaseStorageAzureClient):
    """Client used to perform operation on Azure Blob Storage."""

    def __init__(self, container_name: str) -> None:
        super().__init__()
        self.blob_service_client = BlobServiceClient.from_connection_string(
            self._storage_connection_string
        )
        self.container_name = container_name
        self.container_client = self._get_container_client(container_name)

    @staticmethod
    def _get_container_name_for_project(project_slug: str):
        return f"project-{project_slug}"

    def _get_container_client(self, container_name: str) -> ContainerClient:
        return self.blob_service_client.get_container_client(container_name)

    def _generate_sas_token_for_container(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        return generate_container_sas(
            self.storage_account_name,
            self.container_name,
            self._storage_key,
            permission=ContainerSasPermissions(
                list=True,
                read=True,
                write=False,
                delete=True,
            ),
            start=now,
            expiry=now + datetime.timedelta(minutes=60),
        )

    def get_project_container_sas_token(self):
        return self._generate_sas_token_for_container()

    def get_project_container_base_url(self):
        return f"https://{self.container_client.primary_hostname}"

    def set_cors_policy(self, allowed_origins: str):
        """Set Cross-Origin Resource Sharing (CORS) for Azure Blob."""
        cors = [
            CorsRule(
                allowed_origins=allowed_origins.split(","),
                allowed_methods=["DELETE", "GET", "HEAD", "POST", "OPTIONS", "PUT"],
                allowed_headers=["authorization", "content-type", "x-ms-*"],
            )
        ]
        self.blob_service_client.set_service_properties(cors=cors)

    @staticmethod
    def _normalize_blob_path(path: str) -> str:
        return path.replace("\\", "/").strip("/")

    def _dir_prefix(self, dir_path: str) -> str:
        normalized = self._normalize_blob_path(dir_path)
        if normalized:
            return f"{normalized}/"
        return ""

    def _join_blob_path(self, dir_path: str, file_name: str) -> str:
        normalized_dir = self._normalize_blob_path(dir_path)
        normalized_file = file_name.replace("\\", "/").lstrip("/")
        if normalized_dir:
            return f"{normalized_dir}/{normalized_file}"
        return normalized_file

    def _create_directory_marker(self, dir_path: str) -> None:
        container = self.container_client
        marker_name = self._dir_prefix(dir_path)
        if not marker_name:
            return
        blob_client = container.get_blob_client(marker_name)
        try:
            blob_client.upload_blob(b"", overwrite=False)
        except ResourceExistsError as error:
            raise FolderCreationError(str(error)) from error

    def _path_exists(self, dir_path: str) -> bool:
        container = self.container_client
        prefix = self._dir_prefix(dir_path)
        try:
            if not prefix:
                return container.exists()
            for _ in container.list_blobs(name_starts_with=prefix, results_per_page=1):
                return True
        except ResourceNotFoundError:
            return False
        return False

    def _list_files(self, dir_path: str) -> list[ProjectFileOrDirectory]:
        container = self.container_client
        prefix = self._dir_prefix(dir_path)
        results: list[ProjectFileOrDirectory] = []
        seen_dirs: set[str] = set()

        for entry in container.walk_blobs(
            name_starts_with=prefix,
            delimiter="/",
        ):
            if isinstance(entry, BlobPrefix):
                name = entry.name
                if prefix and name.startswith(prefix):
                    name = name[len(prefix) :]
                dir_name = name.strip("/")
                if not dir_name or dir_name in seen_dirs:
                    continue
                seen_dirs.add(dir_name)
                results.append(
                    ProjectFileOrDirectory(
                        name=dir_name,
                        path=entry.name.rstrip("/"),
                        type="directory",
                        size=None,
                        last_modified=None,
                    )
                )
                continue
            if entry.name.endswith("/"):
                name = entry.name
                if prefix and name.startswith(prefix):
                    name = name[len(prefix) :]
                dir_name = name.strip("/")
                if not dir_name or dir_name in seen_dirs:
                    continue
                seen_dirs.add(dir_name)
                results.append(
                    ProjectFileOrDirectory(
                        name=dir_name,
                        path=entry.name.rstrip("/"),
                        type="directory",
                        size=None,
                        last_modified=None,
                    )
                )
                continue
            name = entry.name
            if prefix and name.startswith(prefix):
                name = name[len(prefix) :]
            name = name.lstrip("/")
            if not name:
                continue
            results.append(
                ProjectFileOrDirectory(
                    name=posixpath.basename(name),
                    path=entry.name,
                    type="file",
                    size=entry.size,
                    last_modified=entry.last_modified,
                )
            )

        return results

    def _rename_directory(
        self,
        directory_path: str,
        new_directory_path: str,
    ) -> None:
        container = self.container_client
        old_prefix = self._normalize_blob_path(directory_path)
        new_prefix = self._normalize_blob_path(new_directory_path)
        if not self._path_exists(old_prefix):
            raise FolderCreationError("directory not found")
        if self._path_exists(new_prefix):
            raise FolderCreationError("destination already exists")
        for blob in container.list_blobs(name_starts_with=self._dir_prefix(old_prefix)):
            source_name = blob.name
            remainder = source_name[len(old_prefix) :].lstrip("/")
            dest_name = f"{new_prefix}/{remainder}" if remainder else f"{new_prefix}/"
            if source_name.endswith("/") and not dest_name.endswith("/"):
                dest_name = f"{dest_name}/"
            self._copy_blob(source_name, dest_name)

    def _copy_blob(
        self,
        source_name: str,
        dest_name: str,
    ) -> None:
        container = self.container_client
        source_blob = container.get_blob_client(source_name)
        dest_blob = container.get_blob_client(dest_name)
        copy_result = dest_blob.start_copy_from_url(source_blob.url)
        copy_status = copy_result.get("copy_status")
        if copy_status and copy_status != "success":
            timeout_seconds = int(
                os.environ.get("AZURE_BLOB_COPY_TIMEOUT_SECONDS", "300")
            )
            deadline = time.monotonic() + timeout_seconds
            while time.monotonic() < deadline:
                props = dest_blob.get_blob_properties()
                status = props.copy.status
                if status == "success":
                    break
                if status in ("failed", "aborted"):
                    raise FolderCreationError(
                        f"copy failed for blob {source_name} with status {status}"
                    )
                time.sleep(0.5)
            else:
                raise FolderCreationError(
                    f"copy timeout for blob {source_name} after {timeout_seconds}s"
                )
        source_blob.delete_blob()
