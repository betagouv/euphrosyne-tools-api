import datetime
import os

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.blob import BlobSasPermissions, generate_blob_sas

from .blob import BlobAzureClient


class ImageStorageClient(BlobAzureClient):
    """Client used to query images on Azure Blob Storage."""

    def __init__(self, project_slug: str) -> None:
        self.project_slug = project_slug
        super().__init__(
            container_name=self._get_container_name_for_project(project_slug)
        )

    def get_project_container_sas_token(self):
        return self._generate_sas_token_for_container()

    def get_project_container_base_url(self):
        return f"https://{self.container_client.primary_hostname}"

    async def list_project_images(
        self,
        object_id: int | None = None,
        with_sas_token: bool = False,
    ):
        """List all URLs of project object group images stored in an Azure container. If with_sas_token is
        True, a SAS token will be appended to each URL."""
        container_url = self.container_client.url
        sas_token: str | None = None
        if with_sas_token:
            sas_token = self._generate_sas_token_for_container()
        try:
            blobs = self.container_client.list_blob_names(
                name_starts_with=self._get_image_blob_name(object_id=object_id)
            )
            async for name in self._iterate_blocking(blobs):
                url = f"{container_url}/{name}"
                if sas_token:
                    url = f"{url}?{sas_token}"
                yield url
        except ResourceNotFoundError:
            return

    async def generate_signed_upload_project_image_url(
        self,
        file_name: str,
        object_id: int | None = None,
    ):
        """Returns a signed URL to upload an image in a project container. If object_id is passed it will
        return a signed URL to upload an image in the object group folder inside the project folder.
        """
        container = self.container_client

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
