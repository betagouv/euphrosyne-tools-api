from typing import Literal

from fastapi import APIRouter, Body, Depends

from auth import verify_admin_permission
from clients.azure.config import ConfigAzureClient, VMSizes
from dependencies import get_config_azure_client

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
    """Edit VM size configuration of a project. Passing empty string value will remove project from
    a VM size category."""
    config_client.set_project_vm_size(project_name, vm_size or None)
