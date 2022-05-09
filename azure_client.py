import os
import secrets
from typing import Any

from azure.identity import DefaultAzureCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.resource.templatespecs import TemplateSpecsClient
from dotenv import load_dotenv

load_dotenv()


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
        return self._compute_mgmt_client.virtual_machines.get(
            resource_group_name=self.resource_group_name, vm_name=vm_name
        )

    def deploy_vm(self, vm_name: str):
        """Deploys a VM based on Template Specs specified
        with AZURE_TEMPLATE_SPECS_NAME env variable.
        """
        template = self._get_latest_template_specs()
        parameters = {
            "adminUsername": "euphrosyne",
            "adminPassword": secrets.token_urlsafe(32),
            "vmName": vm_name,
            "projectName": vm_name,
            "projectUserPassword": secrets.token_urlsafe(32),
        }
        formatted_parameters = {k: {"value": v} for k, v in parameters.items()}
        deployment_async_operation = (
            self._resource_mgmt_client.deployments.begin_create_or_update(
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
        )
        return deployment_async_operation
