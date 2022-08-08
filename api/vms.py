from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from auth import User, get_current_user
from azure_client import AzureClient
from dependencies import get_azure_client, get_guacamole_client
from guacamole_client import GuacamoleClient, GuacamoleConnectionNotFound

router = APIRouter(prefix="/vms", tags=["vms"])


# pylint: disable=inconsistent-return-statements
@router.delete("/{project_name}", status_code=202)
def delete_vm(
    project_name: str,
    current_user: User = Depends(get_current_user),
    azure_client: AzureClient = Depends(get_azure_client),
    guacamole_client: GuacamoleClient = Depends(get_guacamole_client),
):
    """Delete a VM and its connection information on Guacamole."""
    if not current_user.is_admin:
        return JSONResponse(status_code=403, content={})
    azure_client.delete_vm(project_name)
    azure_client.delete_deployment(
        project_name
    )  # should already be deleted during deployment, but just in case
    try:
        guacamole_client.delete_connection(project_name)
    except GuacamoleConnectionNotFound:
        pass
