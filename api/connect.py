from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from auth import User, get_current_user, verify_project_membership
from clients.azure import VMAzureClient
from clients.azure.vm import VMNotFound
from clients.guacamole import GuacamoleClient, GuacamoleConnectionNotFound
from dependencies import get_guacamole_client, get_vm_azure_client

router = APIRouter(prefix="/connect", tags=["connect"])


@router.get("/{project_name}", dependencies=[Depends(verify_project_membership)])
def get_connection_link(
    project_name: str,
    current_user: User = Depends(get_current_user),
    azure_client: VMAzureClient = Depends(get_vm_azure_client),
    guacamole_client: GuacamoleClient = Depends(get_guacamole_client),
):
    """Shows connection URL for a deployed VM for a specific project.
    Responds with 404 if no VM is deployed for the project or no connection exists on Guacamole."""
    try:
        azure_client.get_vm(project_name)
    except VMNotFound:
        return JSONResponse({"detail": "Azure VM not found"}, status_code=404)
    try:
        connection_id = guacamole_client.get_connection_by_name(project_name)
    except GuacamoleConnectionNotFound:
        return JSONResponse(
            {"detail": "Guacamole connection not found"}, status_code=404
        )
    guacamole_client.create_user_if_absent(str(current_user.id))
    guacamole_client.assign_user_to_connection(connection_id, str(current_user.id))
    connection_link = guacamole_client.generate_connection_link(
        connection_id, str(current_user.id)
    )
    return {"url": connection_link}
