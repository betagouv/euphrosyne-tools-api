import enum
import json
from typing import Optional

from azure.storage.blob import BlobClient, BlobServiceClient, ContainerClient
from dotenv import load_dotenv

from ._storage import BaseStorageAzureClient

load_dotenv()


@enum.unique
class VMSizes(str, enum.Enum):
    IMAGERY = "IMAGERY"


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
        blob_client = self._get_or_create_project_vm_sizes_blob()
        project_vm_sizes: dict = json.loads(blob_client.download_blob().readall())
        for vm_size, project_names in project_vm_sizes.items():
            if project_name in project_names:
                return VMSizes[vm_size]
        return None

    def set_project_vm_size(
        self, project_name: str, project_vm_size: Optional[VMSizes] = None
    ):
        """Add the project name to a VM size category (imagery, ...). If project_vm_size is
        None, the project will be removed from its current category. Setting a vm size for
        a project overrides any previous configuration, i.e a project can not be added to
        several category."""
        if project_vm_size is not None and not isinstance(project_vm_size, VMSizes):
            raise TypeError("project_vm_size must be an enum of VMSizes type.")
        blob_client = self._get_or_create_project_vm_sizes_blob()
        project_vm_sizes: dict = json.loads(blob_client.download_blob().readall())
        for _, project_names in project_vm_sizes.items():
            if project_name in project_names:
                project_names.remove(project_name)
        if project_vm_size is not None:
            project_vm_sizes = {
                **project_vm_sizes,
                project_vm_size.name: [
                    *project_vm_sizes.get(project_vm_size.name, []),
                    project_name,
                ],
            }
        blob_client.upload_blob(json.dumps(project_vm_sizes), overwrite=True)

    def _get_or_create_project_vm_sizes_blob(self):
        return self._get_or_create_blob(
            self.project_settings_container_client, "project-vm-sizes.json", "{}"
        )

    def _get_or_create_blob(
        self, container_client: ContainerClient, blob_name: str, initial_data: str
    ) -> BlobClient:
        blob_client = container_client.get_blob_client(blob_name)
        if not blob_client.exists():
            blob_client.upload_blob(initial_data)
        return blob_client
