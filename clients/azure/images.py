import asyncio
import datetime
import os
from functools import lru_cache

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.blob import (
    BlobSasPermissions,
    ContainerSasPermissions,
    CorsRule,
    generate_blob_sas,
    generate_container_sas,
)
from azure.storage.blob.aio import BlobServiceClient, ContainerClient

from clients.azure._storage import BaseStorageAzureClient


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
        # We await so we can cache the result
        return self.blob_service_client.get_container_client(
            self._get_container_name_for_project(project_slug)
        )

    def _generate_sas_token_for_container(
        self,
        container_name: str,
    ):
        now = datetime.datetime.now(datetime.timezone.utc)
        return generate_container_sas(
            self.storage_account_name,
            container_name,
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

    def get_project_container_sas_token(self, project_slug: str):
        return self._generate_sas_token_for_container(
            self._get_container_name_for_project(project_slug)
        )

    def get_project_container_base_url(self, project_slug: str):
        return f"https://{self._get_project_container(project_slug).primary_hostname}"

    async def set_cors_policy(self, allowed_origins: str):
        """Set Cross-Origin Resource Sharing (CORS) for Azure Blob.

        Arguments:
        allowed_origins -- A string representing allowed origins as specified here :
            https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Access-Control-Allow-Origin
            Multiple origins can be specified by separating them with commas.
        """
        await self.blob_service_client.set_service_properties(
            cors=[
                CorsRule(
                    allowed_origins=allowed_origins.split(","),
                    allowed_methods=["DELETE", "GET", "HEAD", "POST", "OPTIONS", "PUT"],
                    allowed_headers=[
                        "content-type",
                        "x-ms-blob-type",
                        "x-ms-client-request-id",
                        "x-ms-useragent",
                        "x-ms-version",
                    ],
                )
            ]
        )


class ImageStorageClient(BlobAzureClient):
    """Client used to query images on Azure Blob Storage."""

    async def list_project_images(
        self,
        project_slug: str,
        object_id: int | None = None,
        with_sas_token: bool = False,
    ):
        """List all URLs of project object group images stored in an Azure container. If with_sas_token is
        True, a SAS token will be appended to each URL."""
        container = self._get_project_container(project_slug)
        container_url = container.url
        blobs = container.list_blob_names(
            name_starts_with=self._get_image_blob_name(object_id=object_id)
        )
        sas_token: str | None = None
        if with_sas_token:
            sas_token = self._generate_sas_token_for_container(
                container_name=self._get_container_name_for_project(project_slug),
            )
        try:
            async for name in blobs:
                url = f"{container_url}/{name}"
                if sas_token:
                    url = f"{url}?{sas_token}"
                yield url
        except ResourceNotFoundError:
            return

    async def generate_signed_upload_project_image_url(
        self, file_name: str, project_slug: str, object_id: int | None = None
    ):
        """Returns a signed URL to upload an image in a project container. If object_id is passed it will
        return a signed URL to upload an image in the object group folder inside the project folder.
        """
        container = self._get_project_container(project_slug)

        # Run a task to create the container. We will await at the end of the fn.
        create_container_task = asyncio.create_task(container.create_container())  # type: ignore

        blob_name = self._get_image_blob_name(object_id=object_id, file_name=file_name)
        token = generate_blob_sas(
            account_name=self.storage_account_name,
            container_name=container.container_name,
            blob_name=blob_name,
            account_key=self._storage_key,
            permission=BlobSasPermissions(
                list=False,
                read=False,
                write=True,
                delete=False,
            ),
            expiry=datetime.datetime.now(datetime.timezone.utc)
            + datetime.timedelta(minutes=2),
        )

        try:
            await create_container_task
        except ResourceExistsError:
            pass
        return os.path.join(container.url, blob_name) + "?" + token

    @staticmethod
    def _get_image_blob_name(
        file_name: str | None = None, object_id: int | None = None
    ):
        """Returns blob name for object images in a project container. If file_name is omitted,
        it returns the base path where object images are stored."""
        path = "images"
        if object_id:
            path = os.path.join(path, f"object-groups/{object_id}")
        if file_name:
            path = os.path.join(path, file_name)
        return path
