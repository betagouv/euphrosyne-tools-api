import functools
import json
from typing import Optional

from azure.storage.blob import BlobClient, BlobServiceClient, ContainerClient
from dotenv import load_dotenv
from slugify import slugify

from ..common import VMSizes
from ._storage import BaseStorageAzureClient

load_dotenv()


class ConfigAzureClient(BaseStorageAzureClient):
    """Client to read / write configuration on Azure."""

    def __init__(self):
        super().__init__()
        blob_service_client: BlobServiceClient = (
            BlobServiceClient.from_connection_string(self._storage_connection_string)
        )
        self.project_settings_container_client: ContainerClient = (
            blob_service_client.get_container_client("project-settings")
        )
        if not self.project_settings_container_client.exists():
            self.project_settings_container_client.create_container()

    def get_project_vm_size(self, project_name: str) -> VMSizes | None:
        """Fetch VM size for a specific project. Will return a value if the
        project has been added to one of the vm size categories (imagery, ...).
        None will be returned has a default value."""
        project_vm_sizes = self._get_project_vm_sizes_conf()
        for vm_size, project_names in project_vm_sizes.items():
            if slugify(project_name) in project_names:
                return VMSizes[vm_size]
        return None

    def set_project_vm_size(
        self, project_name: str, project_vm_size: Optional[VMSizes] = None
    ):
        """
        Add the project name to a VM size category (imagery, ...). If project_vm_size is
        None, the project will be removed from its current category. Setting a vm size for
        a project overrides any previous configuration, i.e a project can not be added to
        several category.
        """  # noqa: E501
        project_slug = slugify(project_name)
        if project_vm_size is not None and not isinstance(project_vm_size, VMSizes):
            raise TypeError("project_vm_size must be an enum of VMSizes type.")
        project_vm_sizes = self._get_project_vm_sizes_conf()
        for _, project_names in project_vm_sizes.items():
            if project_slug in project_names:
                project_names.remove(project_slug)
        if project_vm_size is not None:
            project_vm_sizes = {
                **project_vm_sizes,
                project_vm_size.name: [
                    *project_vm_sizes.get(project_vm_size.name, []),
                    project_slug,
                ],
            }
        blob_client = self._get_or_create_project_vm_sizes_blob()
        blob_client.upload_blob(json.dumps(project_vm_sizes), overwrite=True)

        # Clear cache when changing conf
        # pylint: disable=no-member
        self._get_project_vm_sizes_conf.cache_clear()

    def get_project_image_definition(self, project_name: str) -> str | None:
        """Fetch image definition for a specific project. Will return a value if the
        project has been added to one of the image definition category.
        None will be returned has a default value.
        """
        project_image_definitions = self._get_project_image_definitions_conf()
        for image_definition, project_names in project_image_definitions.items():
            if project_name in project_names:
                return image_definition
        return None

    def set_project_image_definition(
        self, project_name: str, image_definition: str | None = None
    ):
        """
        Add the project name to an image definition. If image_definition is None, the
        project will be removed from its current category. Setting an image definition for
        a project overrides any previous configuration, i.e a project can not be added to
        several category.
        """
        project_image_definitions = self._get_project_image_definitions_conf()
        for _, project_names in project_image_definitions.items():
            if project_name in project_names:
                project_names.remove(project_name)
        if image_definition is not None:
            project_image_definitions = {
                **project_image_definitions,
                image_definition: [
                    *project_image_definitions.get(image_definition, []),
                    project_name,
                ],
            }
        blob_client = self._get_or_create_project_image_definitions_blob()
        blob_client.upload_blob(json.dumps(project_image_definitions), overwrite=True)

        # Clear cache when changing conf
        # pylint: disable=no-member
        self._get_project_image_definitions_conf.cache_clear()

    @functools.lru_cache
    def _get_project_vm_sizes_conf(self) -> dict:
        blob_client = self._get_or_create_project_vm_sizes_blob()
        return json.loads(blob_client.download_blob().readall())

    @functools.lru_cache
    def _get_project_image_definitions_conf(self) -> dict:
        blob_client = self._get_or_create_project_image_definitions_blob()
        return json.loads(blob_client.download_blob().readall())

    def _get_or_create_project_vm_sizes_blob(self):
        return self._get_or_create_blob(
            self.project_settings_container_client, "project-vm-sizes.json", "{}"
        )

    def _get_or_create_project_image_definitions_blob(self):
        return self._get_or_create_blob(
            self.project_settings_container_client,
            "project-image-definitions.json",
            "{}",
        )

    @functools.lru_cache
    def _get_or_create_blob(
        self, container_client: ContainerClient, blob_name: str, initial_data: str
    ) -> BlobClient:
        blob_client = container_client.get_blob_client(blob_name)
        if not blob_client.exists():
            blob_client.upload_blob(initial_data)
        return blob_client
