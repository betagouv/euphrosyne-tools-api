import datetime

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from auth import verify_admin_permission, verify_project_membership
from clients.azure import VMAzureClient
from clients.azure.vm import VMNotFound
from clients.guacamole import GuacamoleClient, GuacamoleConnectionNotFound
from dependencies import get_guacamole_client, get_vm_azure_client

router = APIRouter(prefix="/vms", tags=["vms"])


@router.get("/", status_code=200, dependencies=[Depends(verify_admin_permission)])
def list_vms(
    azure_client: VMAzureClient = Depends(get_vm_azure_client),
    created_before: datetime.datetime | None = None,
):
    """List all vms."""
    vms_to_exclude_exp = [r"euphro-(stg|prod)-vm-hsds"]
    return azure_client.list_vms(
        exclude_regex_patterns=vms_to_exclude_exp, created_before=created_before
    )


@router.get(
    "/image-definitions",
    status_code=200,
    dependencies=[Depends(verify_admin_permission)],
)
def list_image_definitions(
    azure_client: VMAzureClient = Depends(get_vm_azure_client),
):
    """List all available image definitions."""
    return {"image_definitions": azure_client.list_vm_image_definitions()}


@router.get(
    "/{project_name}",
    status_code=200,
    dependencies=[Depends(verify_project_membership)],
)
def get_vm(
    project_name: str,
    azure_client: VMAzureClient = Depends(get_vm_azure_client),
):
    """Get a specific vm."""
    try:
        vm = azure_client.get_vm(project_name)
    except VMNotFound:
        return JSONResponse(status_code=404, content={})
    return {"provisioning_state": vm.provisioning_state}


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
