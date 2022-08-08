from fastapi import APIRouter, BackgroundTasks, Depends
from fastapi.responses import JSONResponse

from auth import verify_project_membership
from azure_client import AzureClient, DeploymentNotFound
from backgrounds import wait_for_deploy
from dependencies import get_azure_client, get_guacamole_client
from guacamole_client import GuacamoleClient

router = APIRouter(prefix="/deployments", tags=["deployments"])


@router.get(
    "/{project_name}",
    status_code=200,
    dependencies=[Depends(verify_project_membership)],
)
def get_deployment_status(
    project_name: str, azure_client: AzureClient = Depends(get_azure_client)
):
    """Get deployment status about a VM being deployed."""
    try:
        status = azure_client.get_deployment_status(project_name)
    except DeploymentNotFound:
        return JSONResponse(status_code=404)
    return {"status": status}


@router.post(
    "/{project_name}",
    status_code=202,
    dependencies=[Depends(verify_project_membership)],
)
def deploy_vm(
    project_name: str,
    background_tasks: BackgroundTasks,
    azure_client: AzureClient = Depends(get_azure_client),
    guacamole_client: GuacamoleClient = Depends(get_guacamole_client),
):
    """Deploys a VM for a specific project."""
    vm_information = azure_client.deploy_vm(project_name, vm_size="Standard_DS1_v2")
    if vm_information:
        background_tasks.add_task(
            wait_for_deploy, vm_information, guacamole_client, azure_client
        )
