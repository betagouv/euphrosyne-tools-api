import pathlib

from fastapi import APIRouter, Depends, HTTPException, Path
from fastapi.responses import JSONResponse

from auth import User, get_current_user, verify_project_membership
from clients.azure import StorageAzureClient
from clients.azure.data import (
    IncorrectDataFilePath,
    ProjectDocumentsNotFound,
    ProjectFile,
    RunDataNotFound,
    validate_project_document_file_path,
    validate_run_data_file_path,
)
from dependencies import get_storage_azure_client

router = APIRouter(prefix="/data", tags=["data"])


@router.get(
    "/{project_name}/documents",
    status_code=200,
    dependencies=[Depends(verify_project_membership)],
    response_model=list[ProjectFile],
)
def list_project_documents(
    project_name: str,
    azure_client: StorageAzureClient = Depends(get_storage_azure_client),
):
    try:
        return azure_client.get_project_documents(project_name)
    except ProjectDocumentsNotFound:
        return JSONResponse(
            {"detail": "Folder for the project documents not found"}, status_code=404
        )


@router.get(
    "/{project_name}/runs/{run_name}/{data_type}",
    status_code=200,
    dependencies=[Depends(verify_project_membership)],
    response_model=list[ProjectFile],
)
def list_run_data(
    project_name: str,
    run_name: str,
    data_type: str = Path(default=None, regex="^(raw_data|processed_data)$"),
    azure_client: StorageAzureClient = Depends(get_storage_azure_client),
):
    try:
        return azure_client.get_run_files(project_name, run_name, data_type)  # type: ignore
    except RunDataNotFound:
        return JSONResponse({"detail": "Run data not found"}, status_code=404)


@router.get(
    "/runs/shared_access_signature",
    status_code=200,
)
def generate_run_data_shared_access_signature(
    path: pathlib.Path,
    current_user: User = Depends(get_current_user),
    azure_client: StorageAzureClient = Depends(get_storage_azure_client),
):
    """Return a token used to directly download run data
    from run file storage.
    """
    try:
        validate_run_data_file_path(path, current_user)
    except IncorrectDataFilePath as error:
        raise HTTPException(
            status_code=422,
            detail=[{"loc": ["query", "path"], "msg": error.message}],
        ) from error
    url = azure_client.generate_run_data_sas_url(
        dir_path=str(path.parents[0]),
        file_name=path.name,
        is_admin=current_user.is_admin,
    )
    return {"url": url}


@router.get(
    "/documents/shared_access_signature/",
    status_code=200,
)
def generate_project_documents_shared_access_signature(
    path: pathlib.Path,
    current_user: User = Depends(get_current_user),
    azure_client: StorageAzureClient = Depends(get_storage_azure_client),
):
    """Return a token used to directly download project documents
    from document file storage.
    """
    try:
        validate_project_document_file_path(path, current_user)
    except IncorrectDataFilePath as error:
        raise HTTPException(
            status_code=422,
            detail=[{"loc": ["query", "path"], "msg": error.message}],
        ) from error
    url = azure_client.generate_project_documents_sas_url(
        dir_path=str(path.parents[0]),
        file_name=path.name,
    )
    return {"url": url}


@router.get(
    "/{project_name}/documents/upload/shared_access_signature",
    dependencies=[Depends(verify_project_membership)],
    status_code=200,
)
def generate_project_documents_upload_shared_access_signature(
    project_name: str,
    file_name: str,
    azure_client: StorageAzureClient = Depends(get_storage_azure_client),
):
    """Return a token used to upload project documents
    to document file storage.
    """
    url = azure_client.generate_project_documents_upload_sas_url(
        project_name=project_name,
        file_name=file_name,
    )
    return {"url": url}
