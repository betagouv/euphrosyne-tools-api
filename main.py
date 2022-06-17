import os

from fastapi import BackgroundTasks, Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from auth import User, get_current_user
from azure_client import (
    AzureClient,
    AzureVMDeploymentProperties,
    DeploymentNotFound,
    VMNotFound,
    wait_for_deployment_completeness,
)
from exceptions import (
    NoProjectMembershipException,
    no_project_membership_exception_handler,
)
from guacamole_client import GuacamoleClient, GuacamoleConnectionNotFound

app = FastAPI()

app.add_exception_handler(
    NoProjectMembershipException, no_project_membership_exception_handler
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ALLOWED_ORIGIN", "").split(" "),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


azure_client = AzureClient()
guacamole_client = GuacamoleClient()


@app.get("/connect/{project_name}")
def get_connection_link(
    project_name: str, current_user: User = Depends(get_current_user)
):
    """Shows connection URL for a deployed VM for a specific project.
    Responds with 404 if no VM is deployed for the project or no connection exists on Guacamole."""
    if not current_user.is_admin and not current_user.has_project(project_name):
        raise NoProjectMembershipException()
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


# pylint: disable=inconsistent-return-statements
@app.delete("/vms/{project_name}", status_code=202)
def delete_vm(project_name: str, current_user: User = Depends(get_current_user)):
    """Delete a VM and its connection information on Guacamole."""
    if not current_user.is_admin:
        return JSONResponse(status_code=403)
    try:
        guacamole_client.delete_connection(project_name)
        azure_client.delete_vm(project_name)
    except (VMNotFound, GuacamoleConnectionNotFound):
        return JSONResponse(status_code=404)


@app.get("/deployments/{project_name}", status_code=200)
def get_deployment_status(
    project_name: str, current_user: User = Depends(get_current_user)
):
    """Get deployment status about a VM being deployed."""
    if not current_user.is_admin and not current_user.has_project(project_name):
        raise NoProjectMembershipException()
    try:
        status = azure_client.get_deployment_status(project_name)
    except DeploymentNotFound:
        return JSONResponse(status_code=404)
    return {"status": status}


@app.post("/deployments/{project_name}", status_code=202)
def deploy_vm(
    project_name: str,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_user),
):
    """Deploys a VM for a specific project."""
    if not current_user.is_admin and not current_user.has_project(project_name):
        raise NoProjectMembershipException()
    vm_information = azure_client.deploy_vm(project_name, vm_size="Standard_DS1_v2")
    if vm_information:
        background_tasks.add_task(wait_for_deploy, vm_information)


def wait_for_deploy(vm_deployment_properties: AzureVMDeploymentProperties):
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
