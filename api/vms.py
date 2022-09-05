from fastapi import APIRouter, Depends

from auth import verify_admin_permission
from clients.azure import VMAzureClient
from clients.guacamole import GuacamoleClient, GuacamoleConnectionNotFound
from dependencies import get_guacamole_client, get_vm_azure_client

router = APIRouter(prefix="/vms", tags=["vms"])


# pylint: disable=inconsistent-return-statements
@router.delete(
    "/{project_name}", status_code=202, dependencies=[Depends(verify_admin_permission)]
)
def delete_vm(
    project_name: str,
    azure_client: VMAzureClient = Depends(get_vm_azure_client),
    guacamole_client: GuacamoleClient = Depends(get_guacamole_client),
):
    """Delete a VM and its connection information on Guacamole."""
    azure_client.delete_vm(project_name)
    azure_client.delete_deployment(
        project_name
    )  # should already be deleted during deployment, but just in case
    try:
        guacamole_client.delete_connection(project_name)
    except GuacamoleConnectionNotFound:
        pass
