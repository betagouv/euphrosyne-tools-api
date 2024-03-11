from typing import Annotated, Literal

from fastapi import APIRouter, Body, Depends, HTTPException

from auth import verify_admin_permission
from clients import VMSizes
from clients.azure.config import ConfigAzureClient
from clients.azure.vm import VMAzureClient
from dependencies import get_config_azure_client, get_vm_azure_client

router = APIRouter(prefix="/config", tags=["config"])


@router.get(
    "/{project_name}/vm-size",
    status_code=200,
    dependencies=[Depends(verify_admin_permission)],
)
def get_project_vm_size(
    project_name: str,
    config_client: ConfigAzureClient = Depends(get_config_azure_client),
):
    """Get VM size configuration of a project."""
    return {"vm_size": config_client.get_project_vm_size(project_name)}


@router.post(
    "/{project_name}/vm-size",
    status_code=202,
    dependencies=[Depends(verify_admin_permission)],
)
def edit_project_vm_size(
    project_name: str,
    config_client: ConfigAzureClient = Depends(get_config_azure_client),
    vm_size: VMSizes | Literal[""] = Body(embed=True),
):
    """Edit VM size configuration of a project.
    Passing empty string value will remove project from
    a VM size category."""
    config_client.set_project_vm_size(project_name, vm_size or None)


@router.get(
    "/{project_name}/image-definition",
    status_code=200,
    dependencies=[Depends(verify_admin_permission)],
)
def get_project_image_definition(
    project_name: str,
    config_client: ConfigAzureClient = Depends(get_config_azure_client),
):
    """Get VM size configuration of a project."""
    return {
        "image_definition": config_client.get_project_image_definition(project_name)
    }


@router.post(
    "/{project_name}/image-definition",
    status_code=202,
    dependencies=[Depends(verify_admin_permission)],
)
def edit_project_image_definition(
    project_name: str,
    image_definition: Annotated[str, Body()],
    config_client: ConfigAzureClient = Depends(get_config_azure_client),
    vm_client: VMAzureClient = Depends(get_vm_azure_client),
):
    """Edit which image definition is used to create a VM for a project.
    Passing empty string value will remove project from
    config."""
    if (
        image_definition != ""
        and image_definition not in vm_client.list_vm_image_definitions()
    ):
        raise HTTPException(
            status_code=400,
            detail="Image definition not valid.",
        )
    config_client.set_project_image_definition(project_name, image_definition or None)
