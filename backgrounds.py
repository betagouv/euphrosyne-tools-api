from clients.azure import VMAzureClient
from clients.azure.vm import (
    AzureVMDeploymentProperties,
    wait_for_deployment_completeness,
)
from clients.guacamole import GuacamoleClient


def wait_for_deploy(
    vm_deployment_properties: AzureVMDeploymentProperties,
    guacamole_client: GuacamoleClient,
    azure_client: VMAzureClient,
):
    deployment_information = wait_for_deployment_completeness(
        vm_deployment_properties.deployment_process
    )
    if deployment_information:
        guacamole_client.create_connection(
            name=vm_deployment_properties.project_name,
            ip_address=deployment_information.properties.outputs["privateIPVM"][
                "value"
            ],
            password=vm_deployment_properties.password,
            username=vm_deployment_properties.username,
        )
        azure_client.delete_deployment(deployment_information.name)
