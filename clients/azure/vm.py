import os
from dataclasses import dataclass
from typing import Any, Literal, Optional

from azure.core.exceptions import ResourceNotFoundError
from azure.core.polling import LROPoller
from azure.identity import DefaultAzureCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.resource.resources.models import DeploymentExtended
from azure.mgmt.resource.templatespecs import TemplateSpecsClient
from dotenv import load_dotenv
from slugify import slugify

from clients import VMSizes

load_dotenv()

PROJECT_TYPE_VM_SIZE: dict[VMSizes | None, str] = {
    None: "Standard_B8ms",  # default
    VMSizes.IMAGERY: "Standard_B20ms",
}


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
    vm_size: Optional[VMSizes] = None


@dataclass
class AzureCaptureDeploymentProperties:
    project_name: str
    version: str
    deployment_process: LROPoller[DeploymentExtended]


class VMAzureClient:
    def __init__(self):
        self.resource_group_name = os.environ["AZURE_RESOURCE_GROUP_NAME"]
        credentials = DefaultAzureCredential()

        self.template_specs_name = os.environ["AZURE_TEMPLATE_SPECS_NAME"]

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
        vm_size: Optional[VMSizes] = None,
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
        template = self._get_latest_template_specs(
            template_name=self.template_specs_name
        )
        parameters = {
            "vmName": slugify(project_name),
        }
        parameters["vmSize"] = PROJECT_TYPE_VM_SIZE[vm_size]
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
            vm_size=vm_size,
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

    def create_new_image_version(self, project_name: str, version: str):
        """
        Will use the given vm to create a new specialized image of this image and save it
        to the image gallery with the given version
        """
        vm_name = _project_name_to_vm_name(project_name)
        template = self._get_latest_template_specs(template_name="captureVMSpec")
        parameters = {"vmName": vm_name, "version": version}

        formatted_parameters = {k: {"value": v} for k, v in parameters.items()}

        poller = self._resource_mgmt_client.deployments.begin_create_or_update(
            resource_group_name=self.resource_group_name,
            deployment_name=f"updatevmimage_{vm_name}_{version}",
            parameters={
                "properties": {
                    "template": template,
                    "parameters": formatted_parameters,
                    "mode": "Incremental",
                },
            },
        )

        return AzureCaptureDeploymentProperties(
            project_name=vm_name,
            version=version,
            deployment_process=poller,
        )

    def _get_latest_template_specs(self, template_name: str) -> dict[str, Any]:
        """Get latest template specs in a python dict format."""
        template_spec = self._template_specs_client.template_specs.get(
            resource_group_name=self.resource_group_name,
            template_spec_name=template_name,
            expand="versions",
        )
        latest_version = sorted(template_spec.versions.keys())[-1]
        return self._template_specs_client.template_spec_versions.get(
            resource_group_name=self.resource_group_name,
            template_spec_name=template_name,
            template_spec_version=latest_version,
        ).main_template


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


def _project_name_to_vm_name(project_name: str):
    """Returns a correct vm name (prefix added, slugified) based on a project name"""
    # pylint: disable=consider-using-f-string
    return "{}{}".format(os.getenv("AZURE_RESOURCE_PREFIX"), slugify(project_name))
