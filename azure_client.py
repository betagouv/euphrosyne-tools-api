import os
from dataclasses import dataclass
from datetime import datetime, timedelta
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

load_dotenv()

RunDataTypeType = Literal["processed_data", "raw_data"]


class DeploymentNotFound(Exception):
    pass


class VMNotFound(Exception):
    pass


class RunDataNotFound(Exception):
    pass


class ProjectDocumentsNotFound(Exception):
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

        self._file_shared_access_signature = FileSharedAccessSignature(
            account_name=self.storage_account_name, account_key=storage_key
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
            username=os.environ["VM_LOGIN"],
            password=os.environ["VM_PASSWORD"],
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

    def get_project_documents(
        self,
        project_name: str,
    ) -> list[ProjectFile]:
        dir_path = os.path.join(_get_projects_path(), project_name, "documents")
        files = self._list_files_recursive(dir_path)
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
        Specify `data_type` to get either 'raw_data' or 'processed_data'.
        """
        dir_path = _get_run_data_directory_name(project_name, run_name, data_type)
        files = self._list_files_recursive(dir_path)
        try:
            return list(files)
        except ResourceNotFoundError as error:
            raise RunDataNotFound from error

    def generate_project_documents_sas_url(self, project_name: str, file_name: str):
        """Generate URL with Shared Access Signature to manage project documents in
        an Azure Fileshare. Permission are read, write, create & delete.
        """
        dir_path = os.path.join(_get_projects_path(), project_name, "documents")
        permission = FilePermissions(read=True, create=True, write=True, delete=True)
        return self._generate_sas_url(dir_path, file_name, permission)

    def generate_run_data_sas_url(
        self,
        project_name: str,
        run_name: str,
        data_type: RunDataTypeType,
        file_name: str,
        is_admin: bool,
    ):
        """Generate URL with Shared Access Signature to manage run data in an
        Azure Fileshare. Regular users can read. Admins can also write, create & delete.
        """
        dir_path = _get_run_data_directory_name(project_name, run_name, data_type)
        permission = FilePermissions(
            read=True, create=is_admin, write=is_admin, delete=is_admin
        )
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
        share_name = os.environ["AZURE_STORAGE_FILESHARE"]
        sas_params = self._file_shared_access_signature.generate_file(
            share_name=share_name,
            directory_name=dir_path,
            file_name=file_name,
            permission=permission,
            expiry=datetime.utcnow() + timedelta(minutes=5),
            start=datetime.utcnow(),
        )
        # pylint: disable=line-too-long
        return f"https://{self.storage_account_name}.file.core.windows.net/{share_name}/{dir_path}/{file_name}?{sas_params}"

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


def _get_projects_path():
    return os.getenv("AZURE_STORAGE_PROJECTS_LOCATION_PREFIX", "")


def _get_run_data_directory_name(
    project_name: str, run_name: str, data_type: RunDataTypeType
):
    projects_path_prefix = _get_projects_path()
    return os.path.join(projects_path_prefix, project_name, "runs", run_name, data_type)
