from fastapi import FastAPI

app = FastAPI()


@app.get("/vms/{project_name}")
def get_vm(
    project_name: str,
):
    """Shows connection information about a deployed VM for a specific project.
    Responds with 404 if no VM is deployed for the project."""
    return {"project_name": project_name}


@app.post("/deployments/{project_name}")
def deploy_vm(project_name: str):
    """Deploys a VM for a specific project."""
    return {"project_name": project_name}
