from datetime import datetime
import pathlib
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, BackgroundTasks
from fastapi.responses import JSONResponse, StreamingResponse
from auth import (
    ExtraPayloadTokenGetter,
    generate_token_for_path,
    verify_path_permission,
    User,
    get_current_user,
    verify_is_euphrosyne_backend,
    verify_project_membership,
)
from clients.azure import DataAzureClient
from clients.azure.data import (
    FolderCreationError,
    IncorrectDataFilePath,
    ProjectDocumentsNotFound,
    ProjectFileOrDirectory,
    RunDataNotFound,
    validate_project_document_file_path,
    validate_run_data_file_path,
    extract_info_from_path,
)
from clients.azure.stream import stream_zip_from_azure_files
from dependencies import get_storage_azure_client
from hooks.euphrosyne import post_data_access_event

router = APIRouter(prefix="/data", tags=["data"])


@router.get(
    "/available/{project_name}",
    dependencies=[Depends(verify_is_euphrosyne_backend)],
)
def check_project_data_available(
    project_name: str,
    azure_client: DataAzureClient = Depends(get_storage_azure_client),
):
    return {"available": azure_client.is_project_data_available(project_name)}


@router.get(
    "/{project_name}/documents",
    status_code=200,
    dependencies=[Depends(verify_project_membership)],
    response_model=list[ProjectFileOrDirectory],
)
def list_project_documents(
    project_name: str,
    azure_client: DataAzureClient = Depends(get_storage_azure_client),
):
    try:
        return azure_client.get_project_documents(project_name)
    except ProjectDocumentsNotFound:
        return JSONResponse(
            {"detail": "Folder for the project documents not found"}, status_code=404
        )


@router.get(
    "/run-data-zip",
    status_code=200,
    dependencies=[Depends(verify_path_permission)],
)
def zip_project_run_data(
    path: pathlib.Path,
    data_request: Annotated[
        str | None, Depends(ExtraPayloadTokenGetter(key="data_request"))
    ],
    background_tasks: BackgroundTasks,
    azure_client: DataAzureClient = Depends(get_storage_azure_client),
):
    """
    Stream a zip file containing all the run data files. The path must point
    to a run data directory (raw_data, processed_data, ...).

    Returns:
        StreamingResponse: A streaming response containing the zip file.
    """
    try:
        path_info = extract_info_from_path(path)
    except IncorrectDataFilePath as error:
        raise HTTPException(
            status_code=422,
            detail=[{"loc": ["query", "path"], "msg": error.message}],
        ) from error
    try:
        files = azure_client.iter_project_run_files(
            path_info["project_name"], path_info["run_name"], path_info.get("data_type")
        )
    except RunDataNotFound:
        raise HTTPException(status_code=404, detail="Run data not found.")
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    if data_request:
        # If request come from a data request, log it in Euphrosyne
        background_tasks.add_task(
            post_data_access_event, str(path), data_request=data_request
        )
    return StreamingResponse(
        stream_zip_from_azure_files(files),
        media_type="application/zip",
        headers={
            "Content-Disposition": f"attachment; filename={path_info['run_name']}-{timestamp}.zip"
        },
    )


@router.get(
    "/{project_name}/runs/{run_name}/{data_type}",
    status_code=200,
    dependencies=[Depends(verify_project_membership)],
    response_model=list[ProjectFileOrDirectory],
)
def list_run_data(
    project_name: str,
    run_name: str,
    data_type: str = Path(regex="^(raw_data|processed_data|HDF5)$"),
    folder: str | None = None,
    azure_client: DataAzureClient = Depends(get_storage_azure_client),
):
    try:
        return azure_client.get_run_files_folders(project_name, run_name, data_type, folder)  # type: ignore # noqa: E501
    except RunDataNotFound:
        return JSONResponse(
            {"detail": "Run data not found"},
            status_code=404,
            headers={"Cache-Control": "max-age=3600"},
        )


@router.get(
    "/runs/shared_access_signature",
    status_code=200,
)
def generate_run_data_shared_access_signature(
    path: pathlib.Path,
    current_user: User = Depends(get_current_user),
    azure_client: DataAzureClient = Depends(get_storage_azure_client),
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
    azure_client: DataAzureClient = Depends(get_storage_azure_client),
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
    azure_client: DataAzureClient = Depends(get_storage_azure_client),
):
    """Return a token used to upload project documents
    to document file storage.
    """
    url = azure_client.generate_project_documents_upload_sas_url(
        project_name=project_name,
        file_name=file_name,
    )
    return {"url": url}


@router.get(
    "/{project_name}/token",
    status_code=200,
    dependencies=[Depends(verify_project_membership)],
)
def generate_signed_url_for_path(
    path: pathlib.Path,
    current_user: User = Depends(get_current_user),
    data_request: str | None = None,
    expiration: datetime | None = None,
):
    """Return a auth token for a given path. It is used to grant access to project data via
    a GET request without revealing jwt access token. It is like an Azure SAS token."""
    if expiration:
        _verify_can_set_token_expiration(current_user)
    try:
        validate_run_data_file_path(path, current_user)
    except IncorrectDataFilePath as error:
        raise HTTPException(
            status_code=422,
            detail=[{"loc": ["query", "path"], "msg": error.message}],
        ) from error
    token = generate_token_for_path(
        str(path), expiration=expiration, data_request=data_request
    )
    return {"token": token}


@router.post(
    "/{project_name}/init",
    status_code=204,
    dependencies=[Depends(verify_is_euphrosyne_backend)],
)
def init_project_data(
    project_name: str,
    azure_client: DataAzureClient = Depends(get_storage_azure_client),
):
    try:
        return azure_client.init_project_directory(project_name)
    except FolderCreationError as error:
        return JSONResponse({"detail": error.message}, status_code=400)


@router.post(
    "/{project_name}/runs/{run_name}/init",
    status_code=204,
    dependencies=[Depends(verify_is_euphrosyne_backend)],
)
def init_run_data(
    project_name: str,
    run_name: str,
    azure_client: DataAzureClient = Depends(get_storage_azure_client),
):
    try:
        return azure_client.init_run_directory(run_name, project_name)
    except FolderCreationError as error:
        return JSONResponse({"detail": error.message}, status_code=400)


@router.post(
    "/{project_name}/rename/{new_project_name}",
    status_code=204,
    dependencies=[Depends(verify_is_euphrosyne_backend)],
)
def rename_project_folder(
    project_name: str,
    new_project_name: str,
    azure_client: DataAzureClient = Depends(get_storage_azure_client),
):
    try:
        return azure_client.rename_project_directory(project_name, new_project_name)
    except FolderCreationError as error:
        return JSONResponse({"detail": error.message}, status_code=400)


@router.post(
    "/{project_name}/runs/{run_name}/rename/{new_run_name}",
    status_code=204,
    dependencies=[Depends(verify_is_euphrosyne_backend)],
)
def rename_run_folder(
    project_name: str,
    run_name: str,
    new_run_name: str,
    azure_client: DataAzureClient = Depends(get_storage_azure_client),
):
    try:
        return azure_client.rename_run_directory(run_name, project_name, new_run_name)
    except FolderCreationError as error:
        return JSONResponse({"detail": error.message}, status_code=400)


def _verify_can_set_token_expiration(user: User):
    if not user.is_admin:
        raise HTTPException(
            status_code=403, detail="Only admins can set token expiration"
        )
