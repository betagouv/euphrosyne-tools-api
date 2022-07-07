import os

from fastapi import BackgroundTasks, Depends, FastAPI, Path
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from auth import User, get_current_user, verify_project_membership
from azure_client import (
    AzureClient,
    AzureVMDeploymentProperties,
    DeploymentNotFound,
    ProjectDocumentsNotFound,
    ProjectFile,
    RunDataNotFound,
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


@app.get("/connect/{project_name}", dependencies=[Depends(verify_project_membership)])
def get_connection_link(
    project_name: str, current_user: User = Depends(get_current_user)
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


# pylint: disable=inconsistent-return-statements
@app.delete("/vms/{project_name}", status_code=202)
def delete_vm(project_name: str, current_user: User = Depends(get_current_user)):
    """Delete a VM and its connection information on Guacamole."""
    if not current_user.is_admin:
        return JSONResponse(status_code=403)
    azure_client.delete_vm(project_name)
    azure_client.delete_deployment(
        project_name
    )  # should already be deleted during deployment, but just in case
    try:
        guacamole_client.delete_connection(project_name)
    except GuacamoleConnectionNotFound:
        pass


@app.get(
    "/deployments/{project_name}",
    status_code=200,
    dependencies=[Depends(verify_project_membership)],
)
def get_deployment_status(project_name: str):
    """Get deployment status about a VM being deployed."""
    try:
        status = azure_client.get_deployment_status(project_name)
    except DeploymentNotFound:
        return JSONResponse(status_code=404)
    return {"status": status}


@app.post(
    "/deployments/{project_name}",
    status_code=202,
    dependencies=[Depends(verify_project_membership)],
)
def deploy_vm(
    project_name: str,
    background_tasks: BackgroundTasks,
):
    """Deploys a VM for a specific project."""
    vm_information = azure_client.deploy_vm(project_name, vm_size="Standard_DS1_v2")
    if vm_information:
        background_tasks.add_task(wait_for_deploy, vm_information)


@app.get(
    "/data/{project_name}/documents",
    status_code=200,
    dependencies=[Depends(verify_project_membership)],
    response_model=list[ProjectFile],
)
def list_project_documents(project_name: str):
    try:
        return azure_client.get_project_documents(project_name)
    except ProjectDocumentsNotFound:
        return JSONResponse(
            {"detail": "Folder for the project documents not found"}, status_code=404
        )


@app.get(
    "/data/{project_name}/runs/{run_name}/raw_data",
    status_code=200,
    dependencies=[Depends(verify_project_membership)],
    response_model=list[ProjectFile],
)
def list_run_raw_data(project_name: str, run_name: str):
    try:
        return azure_client.get_run_files(project_name, run_name, "raw_data")
    except RunDataNotFound:
        return JSONResponse({"detail": "Run data not found"}, status_code=404)


@app.get(
    "/data/{project_name}/runs/{run_name}/processed_data",
    status_code=200,
    dependencies=[Depends(verify_project_membership)],
    response_model=list[ProjectFile],
)
def list_run_processed_data(project_name: str, run_name: str):
    try:
        return azure_client.get_run_files(project_name, run_name, "processed_data")
    except RunDataNotFound:
        return JSONResponse({"detail": "Run data not found"}, status_code=404)


@app.get(
    "/data/{project_name}/documents/shared_access_signature/{file_name}",
    status_code=200,
    dependencies=[Depends(verify_project_membership)],
)
def generate_project_documents_shared_access_signature(
    project_name: str, file_name: str
):
    """Return a token used to directly download/upload/delete project documents
    from the place it is stored.
    """
    url = azure_client.generate_project_documents_sas_url(project_name, file_name)
    return {"url": url}


@app.get(
    "/data/{project_name}/runs/{run_name}/{data_type}/shared_access_signature/{file_name}",
    status_code=200,
    dependencies=[Depends(verify_project_membership)],
)
def generate_run_data_shared_access_signature(
    project_name: str,
    run_name: str,
    file_name: str,
    data_type: str = Path(default=None, regex="^(raw_data|procesed_data)$"),
    current_user: User = Depends(get_current_user),
):
    """Return a token used to directly download/upload/delete run data
    from the place it is stored.
    """
    url = azure_client.generate_run_data_sas_url(
        project_name, run_name, data_type, file_name, is_admin=current_user.is_admin
    )
    return {"url": url}


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
