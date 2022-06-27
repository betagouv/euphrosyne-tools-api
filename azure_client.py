import os
import secrets
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Generator, Literal, Optional

from azure.core.credentials import TokenCredential
from azure.core.exceptions import ResourceNotFoundError
from azure.core.polling import LROPoller
from azure.identity import DefaultAzureCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.resource.resources.models import DeploymentExtended
from azure.mgmt.resource.templatespecs import TemplateSpecsClient
from azure.mgmt.storage import StorageManagementClient
from azure.storage.fileshare import ShareDirectoryClient, ShareFileClient
from dotenv import load_dotenv
from pydantic import BaseModel
from slugify import slugify

load_dotenv()


class DeploymentNotFound(Exception):
    pass


class VMNotFound(Exception):
    pass


@dataclass
class AzureVMDeploymentProperties:
    project_name: str
    username: str
    password: str
    deployment_process: LROPoller[DeploymentExtended]


class ProjectFile(BaseModel):
    name: str
    last_modified: datetime
    size: int
    path: str


class AzureClient:
    """Provides an API to interact with Azure services."""

    def __init__(
        self,
    ):
        self.resource_group_name = os.environ["AZURE_RESOURCE_GROUP_NAME"]
        self.template_specs_name = os.environ["AZURE_TEMPLATE_SPECS_NAME"]
        self.storage_account_name = os.environ["AZURE_STORAGE_ACCOUNT"]

        credentials = DefaultAzureCredential()

        storage_key = _get_storage_key(
            credentials,
            os.environ["AZURE_SUBSCRIPTION_ID"],
            resource_group_name=self.resource_group_name,
            storage_account_name=self.storage_account_name,
        )
        # pylint: disable=line-too-long,consider-using-f-string
        self._storage_connection_string = "DefaultEndpointsProtocol=https;AccountName={};AccountKey={};EndpointSuffix=core.windows.net".format(
            self.storage_account_name, storage_key
        )

        self._resource_mgmt_client = ResourceManagementClient(
            credentials, os.environ["AZURE_SUBSCRIPTION_ID"]
        )
        self._compute_mgmt_client = ComputeManagementClient(
            credentials, os.environ["AZURE_SUBSCRIPTION_ID"]
        )
        self._template_specs_client = TemplateSpecsClient(
            credentials, os.environ["AZURE_SUBSCRIPTION_ID"]
        )

    def get_vm(self, project_name: str):
        """Retrieves VM information with project name."""
        try:
            return self._compute_mgmt_client.virtual_machines.get(
                resource_group_name=self.resource_group_name,
                vm_name=_project_name_to_vm_name(project_name),
            )
        except ResourceNotFoundError as error:
            raise VMNotFound from error

    def delete_deployment(self, project_name: str):
        return self._resource_mgmt_client.deployments.begin_delete(
            resource_group_name=self.resource_group_name,
            deployment_name=slugify(project_name),
        )

    def get_deployment_status(
        self, project_name: str
    ) -> Literal[
        "NotSpecified",
        "Accepted",
        "Running",
        "Ready",
        "Creating",
        "Created",
        "Canceled",
        "Failed",
        "Succeeded",
        "Updating",
    ]:
        """Retrieves VM information."""
        try:
            deployment = self._resource_mgmt_client.deployments.get(
                resource_group_name=self.resource_group_name,
                deployment_name=slugify(project_name),
            )
        except ResourceNotFoundError as error:
            raise DeploymentNotFound() from error
        return deployment.properties.provisioning_state

    def deploy_vm(
        self,
        project_name: str,
        vm_size: Literal[
            "Standard_B8ms",
            "Standard_B20ms",
            "Standard_DS1_v2",
        ] = None,
    ) -> Optional[AzureVMDeploymentProperties]:
        """Deploys a VM based on Template Specs specified
        with AZURE_TEMPLATE_SPECS_NAME env variable.
        In both cases where the deployment is created or it has
        already been created before, the function returns None.
        """
        if self._resource_mgmt_client.deployments.check_existence(
            resource_group_name=self.resource_group_name,
            deployment_name=slugify(project_name),
        ):
            return None
        template = self._get_latest_template_specs()
        parameters = {
            "adminUsername": project_name,
            "adminPassword": secrets.token_urlsafe(),
            "vmName": slugify(project_name),
        }
        if vm_size:
            parameters["vmSize"] = vm_size
        formatted_parameters = {k: {"value": v} for k, v in parameters.items()}
        poller = self._resource_mgmt_client.deployments.begin_create_or_update(
            resource_group_name=self.resource_group_name,
            deployment_name=slugify(project_name),
            parameters={
                "properties": {
                    "template": template,
                    "parameters": formatted_parameters,
                    "mode": "Incremental",
                },
            },
        )
        return AzureVMDeploymentProperties(
            project_name=project_name,
            username=project_name,
            password=parameters["adminPassword"],
            deployment_process=poller,
        )

    def delete_vm(self, project_name: str) -> Literal["Failed", "Succeeded"]:
        try:
            operation = self._compute_mgmt_client.virtual_machines.begin_delete(
                resource_group_name=self.resource_group_name,
                vm_name=_project_name_to_vm_name(project_name),
            )
        except ResourceNotFoundError as error:
            raise VMNotFound from error
        operation.result()
        return operation.status()

    def get_run_files(
        self,
        project_name: str,
        run_name: str,
        data_type: Literal["raw_data", "processed_data"],
    ) -> Generator[ProjectFile, None, None]:
        # pylint: disable=consider-using-f-string
        projects_path_prefix = "{}/".format(
            os.getenv("AZURE_STORAGE_PROJECTS_LOCATION_PREFIX")
        )
        dir_path = projects_path_prefix + project_name + f"/runs/{run_name}/{data_type}"
        files = self._list_files_recursive(dir_path)
        return files

    def _list_files_recursive(
        self, dir_path: str
    ) -> Generator[ProjectFile, None, None]:
        """
        List files from FileShare on Azure Storage Account.

        Parameters
        ----------
        dir_path: str
            Directory path to list files from.
        recursive: bool
            Specifies whether to list files recursively.

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
        share_name = os.environ["AZURE_STORAGE_FILESHARE"]

        dir_client = ShareDirectoryClient.from_connection_string(
            conn_str=self._storage_connection_string,
            share_name=share_name,
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
                file_client = ShareFileClient.from_connection_string(
                    conn_str=self._storage_connection_string,
                    share_name=share_name,
                    file_path=path,
                )

                yield ProjectFile.parse_obj(file_client.get_file_properties())

    def _get_latest_template_specs(self) -> dict[str, Any]:
        """Get latest template specs in a python dict format."""
        template_spec = self._template_specs_client.template_specs.get(
            resource_group_name=self.resource_group_name,
            template_spec_name=self.template_specs_name,
            expand="versions",
        )
        latest_version = sorted(template_spec.versions.keys())[-1]
        return self._template_specs_client.template_spec_versions.get(
            resource_group_name=self.resource_group_name,
            template_spec_name=self.template_specs_name,
            template_spec_version=latest_version,
        ).main_template


def _project_name_to_vm_name(project_name: str):
    """Returns a correct vm name (prefix added, slugified) based on a project name"""
    # pylint: disable=consider-using-f-string
    return "{}{}".format(os.getenv("AZURE_RESOURCE_PREFIX"), slugify(project_name))


def wait_for_deployment_completeness(
    poller: LROPoller[DeploymentExtended],
) -> Optional[DeploymentExtended]:
    deployment = poller.result()
    if deployment.properties.provisioning_state in (
        "Succeeded",
        "Running",
        "Ready",
    ):
        return deployment
    return None


def _get_storage_key(
    credential: TokenCredential,
    subscription_id: str,
    resource_group_name: str,
    storage_account_name: str,
):
    """Fetches a storage account key to use as a credential"""
    storage_mgmt_client = StorageManagementClient(credential, subscription_id)
    key = (
        storage_mgmt_client.storage_accounts.list_keys(
            resource_group_name, storage_account_name
        )
        .keys[0]
        .value
    )

    return key
