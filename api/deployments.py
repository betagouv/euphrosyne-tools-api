from fastapi import APIRouter, BackgroundTasks, Depends
from fastapi.responses import JSONResponse

from auth import verify_project_membership
from backgrounds import wait_for_deploy
from clients.azure import VMAzureClient
from clients.azure.config import ConfigAzureClient
from clients.azure.vm import DeploymentNotFound
from clients.guacamole import GuacamoleClient
from dependencies import (
    get_config_azure_client,
    get_guacamole_client,
    get_vm_azure_client,
)

router = APIRouter(prefix="/deployments", tags=["deployments"])


@router.get(
    "/{project_name}",
    status_code=200,
    dependencies=[Depends(verify_project_membership)],
)
def get_deployment_status(
    project_name: str, azure_client: VMAzureClient = Depends(get_vm_azure_client)
):
    """Get deployment status about a VM being deployed."""
    try:
        status = azure_client.get_deployment_status(project_name)
    except DeploymentNotFound:
        return JSONResponse(status_code=404, content={})
    return {"status": status}


@router.post(
    "/{project_name}",
    status_code=202,
    dependencies=[Depends(verify_project_membership)],
)
def deploy_vm(
    project_name: str,
    background_tasks: BackgroundTasks,
    vm_client: VMAzureClient = Depends(get_vm_azure_client),
    config_client: ConfigAzureClient = Depends(get_config_azure_client),
    guacamole_client: GuacamoleClient = Depends(get_guacamole_client),
):
    """Deploys a VM for a specific project."""
    vm_size = config_client.get_project_vm_size(project_name)
    vm_information = vm_client.deploy_vm(project_name, vm_size=vm_size)
    if vm_information:
        background_tasks.add_task(
            wait_for_deploy, vm_information, guacamole_client, vm_client
        )
