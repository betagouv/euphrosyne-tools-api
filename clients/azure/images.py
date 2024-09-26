import datetime
from functools import lru_cache
from clients.azure._storage import BaseStorageAzureClient
from azure.storage.blob import (
    BlobServiceClient,
    ContainerClient,
    generate_container_sas,
    generate_blob_sas,
    ContainerSasPermissions,
)
import os

from azure.core.exceptions import ResourceNotFoundError


class BlobAzureClient(BaseStorageAzureClient):
    """Client used to perform operation on Azure Blob Storage."""

    def __init__(self):
        super().__init__()
        self.blob_service_client: BlobServiceClient = (
            BlobServiceClient.from_connection_string(self._storage_connection_string)
        )

    @staticmethod
    def _get_container_name_for_project(project_slug: str):
        return f"project-{project_slug}"

    @lru_cache
    def _get_project_container(self, project_slug: str) -> ContainerClient:
        return self.blob_service_client.get_container_client(
            self._get_container_name_for_project(project_slug)
        )

    def generate_sas_token_for_container(
        self,
        container_name: str,
    ):
        return generate_container_sas(
            self.storage_account_name,
            container_name,
            self._storage_key,
            permission=ContainerSasPermissions(
                list=True,
                read=True,
                write=False,
                delete=False,
            ),
            expiry=datetime.datetime.now(datetime.timezone.utc)
            + datetime.timedelta(minutes=5),
        )


class ImageStorageClient(BlobAzureClient):
    """Client used to query images on Azure Blob Storage."""

    def list_project_object_images(
        self, project_slug: int, object_id: int, with_sas_token: bool = False
    ):
        """List all URLs of project object group images stored in an Azure container. If with_sas_token is
        True, a SAS token will be appended to each URL."""
        container = self._get_project_container(project_slug)
        container_url = container.url
        blobs = container.list_blob_names(
            name_starts_with=self._get_object_image_blob_name(object_id=object_id)
        )
        sas_token: str | None = None
        if with_sas_token:
            sas_token = self.generate_sas_token_for_container(
                container_name=self._get_container_name_for_project(project_slug),
            )
        try:
            for name in blobs:
                url = f"{container_url}/{name}"
                if sas_token:
                    url = f"{url}?{sas_token}"
                yield url
        except ResourceNotFoundError:
            return

    def generate_signed_upload_project_object_image_url(
        self, project_slug: str, object_id: int, file_name: str
    ):
        container = self._get_project_container(project_slug)
        blob_name = self._get_object_image_blob_name(object_id, file_name)
        token = generate_blob_sas(
            account_name=self.storage_account_name,
            container_name=container.container_name,
            blob_name=blob_name,
            account_key=self._storage_key,
            permission=ContainerSasPermissions(
                list=False,
                read=False,
                write=True,
                delete=False,
            ),
            expiry=datetime.datetime.now(datetime.timezone.utc)
            + datetime.timedelta(minutes=2),
        )
        return os.path.join(container.url, blob_name) + "?" + token

    @staticmethod
    def _get_object_image_blob_name(object_id: int, file_name: str | None = None):
        """Returns blob name for object images in a project container. If file_name is omitted,
        it returns the base path where object images are stored."""
        path = f"images/object-groups/{object_id}"
        if file_name:
            path = os.path.join(path, file_name)
        return path
