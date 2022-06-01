import os
import secrets
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

load_dotenv()


class DeploymentNotFound(Exception):
    pass


class VMNotFound(Exception):
    pass


@dataclass
class AzureVMDeploymentProperties:
    vm_name: str
    username: str
    password: str
    deployment_process: LROPoller[DeploymentExtended]


class AzureClient:
    """Provides an API to interact with Azure services."""

    def __init__(
        self,
    ):
        self.resource_group_name = os.environ["AZURE_RESOURCE_GROUP_NAME"]
        self.template_specs_name = os.environ["AZURE_TEMPLATE_SPECS_NAME"]

        credentials = DefaultAzureCredential()
        self._resource_mgmt_client = ResourceManagementClient(
            credentials, os.environ["AZURE_SUBSCRIPTION_ID"]
        )
        self._compute_mgmt_client = ComputeManagementClient(
            credentials, os.environ["AZURE_SUBSCRIPTION_ID"]
        )
        self._template_specs_client = TemplateSpecsClient(
            credentials, os.environ["AZURE_SUBSCRIPTION_ID"]
        )

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

    def get_vm(self, vm_name: str):
        """Retrieves VM information."""
        try:
            return self._compute_mgmt_client.virtual_machines.get(
                resource_group_name=self.resource_group_name, vm_name=vm_name
            )
        except ResourceNotFoundError as error:
            raise VMNotFound() from error

    def get_deployment_status(
        self, vm_name: str
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
                resource_group_name=self.resource_group_name, deployment_name=vm_name
            )
        except ResourceNotFoundError as error:
            raise DeploymentNotFound() from error
        return deployment.properties.provisioning_state

    def deploy_vm(self, vm_name: str) -> Optional[AzureVMDeploymentProperties]:
        """Deploys a VM based on Template Specs specified
        with AZURE_TEMPLATE_SPECS_NAME env variable.
        In both cases where the deployment is created or it has
        already been created before, the function returns None.
        """
        if self._resource_mgmt_client.deployments.check_existence(
            resource_group_name=self.resource_group_name, deployment_name=vm_name
        ):
            return None
        template = self._get_latest_template_specs()
        parameters = {
            "adminUsername": "euphrosyne",
            "adminPassword": secrets.token_urlsafe(),
            "vmName": vm_name,
            "projectName": vm_name,
            "projectUserPassword": secrets.token_urlsafe(),
        }
        formatted_parameters = {k: {"value": v} for k, v in parameters.items()}
        poller = self._resource_mgmt_client.deployments.begin_create_or_update(
            resource_group_name=self.resource_group_name,
            deployment_name=vm_name,
            parameters={
                "properties": {
                    "template": template,
                    "parameters": formatted_parameters,
                    "mode": "Incremental",
                },
            },
        )
        return AzureVMDeploymentProperties(
            vm_name,
            username=vm_name,
            password=parameters["projectUserPassword"],
            deployment_process=poller,
        )


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
