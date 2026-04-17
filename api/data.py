import pathlib
from datetime import datetime
from typing import Annotated
from uuid import UUID

import pydantic
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Path, Query
from fastapi.responses import JSONResponse, StreamingResponse

from auth import (
    ExtraPayloadTokenGetter,
    User,
    generate_token_for_path,
    get_current_user,
    verify_admin_permission,
    verify_is_euphrosyne_backend,
    verify_is_euphrosyne_backend_or_admin,
    verify_path_permission,
    verify_project_membership,
)
from clients.azure import DataAzureClient
from clients.azure.data import (
    FolderCreationError,
    ProjectDocumentsNotFound,
    RunDataNotFound,
)
from clients.azure.stream import stream_zip_from_azure_files_async
from clients.data_models import ProjectFileOrDirectory
from data_lifecycle import models
from data_lifecycle.operation import (
    LifecycleOperationNotFoundError,
    get_lifecycle_operation_status,
    schedule_from_data_deletion,
    schedule_lifecycle_operation,
)
from data_lifecycle.storage_types import StorageRole
from dependencies import get_hot_project_data_client, get_project_data_client
from hooks.euphrosyne import post_data_access_event
from path import ProjectDocumentRef, RunDataTypeRef

router = APIRouter(prefix="/data", tags=["data"])


@router.get(
    "/available/{project_slug}",
    dependencies=[Depends(verify_is_euphrosyne_backend)],
)
def check_project_data_available(
    project_slug: str,
    azure_client: DataAzureClient = Depends(get_project_data_client),
):
    return {"available": azure_client.is_project_data_available(project_slug)}


@router.get(
    "/{project_slug}/documents",
    status_code=200,
    dependencies=[Depends(verify_project_membership)],
    response_model=list[ProjectFileOrDirectory],
)
def list_project_documents(
    project_slug: str,
    azure_client: DataAzureClient = Depends(get_project_data_client),
):
    try:
        return azure_client.get_project_documents(project_slug)
    except ProjectDocumentsNotFound:
        return JSONResponse(
            {"detail": "Folder for the project documents not found"}, status_code=404
        )


@router.get(
    "/run-data-zip",
    status_code=200,
    dependencies=[Depends(verify_path_permission)],
)
async def zip_project_run_data(
    path: pathlib.Path,
    data_request: Annotated[
        str | None, Depends(ExtraPayloadTokenGetter(key="data_request"))
    ],
    background_tasks: BackgroundTasks,
    azure_client: DataAzureClient = Depends(get_project_data_client),
):
    """
    Stream a zip file containing all the run data files. The path must point
    to a run data directory (raw_data, processed_data, ...).

    Returns:
        StreamingResponse: A streaming response containing the zip file.
    """
    ref = RunDataTypeRef.from_path(path)
    try:
        files = azure_client.iter_project_run_files_async(
            ref.project_slug, ref.run_name, ref.data_type
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
        stream_zip_from_azure_files_async(files),
        media_type="application/zip",
        headers={
            "Content-Disposition": f"attachment; filename={ref.run_name}-{timestamp}.zip"
        },
    )


@router.get(
    "/{project_slug}/runs/{run_name}/{data_type}",
    status_code=200,
    dependencies=[Depends(verify_project_membership)],
    response_model=list[ProjectFileOrDirectory],
)
def list_run_data(
    project_slug: str,
    run_name: str,
    data_type: str = Path(regex="^(raw_data|processed_data|HDF5)$"),
    folder: str | None = None,
    azure_client: DataAzureClient = Depends(get_project_data_client),
):
    try:
        return azure_client.get_run_files_folders(project_slug, run_name, data_type, folder)  # type: ignore # noqa: E501
    except RunDataNotFound:
        return JSONResponse(
            {"detail": "Run data not found"},
            status_code=404,
            headers={"Cache-Control": "max-age=3600"},
        )


@router.get(
    "/{project_slug}/runs/{run_name}/upload/shared_access_signature",
    status_code=200,
    dependencies=[Depends(verify_admin_permission)],
)
def generate_run_data_upload_shared_access_signature(
    project_slug: str,
    run_name: str,
    data_type: Annotated[
        str | None, Query(pattern="^(raw_data|processed_data|HDF5)$")
    ] = None,
    azure_client: DataAzureClient = Depends(get_project_data_client),
):
    """Return a token used to upload run data
    to file storage.
    """
    credentials = azure_client.generate_run_data_upload_sas(
        project_name=project_slug,
        run_name=run_name,
        data_type=data_type,  # type: ignore
    )
    return credentials


@router.get(
    "/runs/shared_access_signature",
    status_code=200,
)
def generate_run_data_shared_access_signature(
    path: pathlib.Path,
    current_user: User = Depends(get_current_user),
    azure_client: DataAzureClient = Depends(get_project_data_client),
):
    """Return a token used to directly download run data
    from run file storage.
    """
    ref = RunDataTypeRef.from_path(path)
    verify_project_membership(ref.project_slug, current_user)
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
    azure_client: DataAzureClient = Depends(get_project_data_client),
):
    """Return a token used to directly download project documents
    from document file storage.
    """
    ref = ProjectDocumentRef.from_path(path)
    verify_project_membership(ref.project_slug, current_user)
    url = azure_client.generate_project_documents_sas_url(
        dir_path=str(path.parents[0]),
        file_name=path.name,
    )
    return {"url": url}


@router.get(
    "/{project_slug}/documents/upload/shared_access_signature",
    dependencies=[Depends(verify_project_membership)],
    status_code=200,
)
def generate_project_documents_upload_shared_access_signature(
    project_slug: str,
    file_name: str,
    azure_client: DataAzureClient = Depends(get_project_data_client),
):
    """Return a token used to upload project documents
    to document file storage.
    """
    url = azure_client.generate_project_documents_upload_sas_url(
        project_name=project_slug,
        file_name=file_name,
    )
    return {"url": url}


@router.get(
    "/{project_slug}/token",
    status_code=200,
    dependencies=[Depends(verify_project_membership)],
)
def generate_signed_url_for_path(
    project_slug: str,
    path: pathlib.Path,
    current_user: User = Depends(get_current_user),
    data_request: str | None = None,
    expiration: datetime | None = None,
):
    """Return a auth token for a given path. It is used to grant access to project data via
    a GET request without revealing jwt access token. It is like an Azure SAS token."""
    if expiration and not current_user.is_admin:
        raise HTTPException(
            status_code=403, detail="Only admins can set token expiration"
        )
    ref = RunDataTypeRef.from_path(path)
    verify_project_membership(ref.project_slug, current_user)
    token = generate_token_for_path(
        str(path), expiration=expiration, data_request=data_request
    )
    return {"token": token}


@router.post(
    "/{project_slug}/init",
    status_code=204,
    dependencies=[Depends(verify_is_euphrosyne_backend_or_admin)],
)
def init_project_data(
    project_slug: str,
    azure_client: DataAzureClient = Depends(get_hot_project_data_client),
):
    try:
        return azure_client.init_project_directory(project_slug)
    except FolderCreationError as error:
        return JSONResponse({"detail": error.message}, status_code=400)


@router.post(
    "/{project_slug}/runs/{run_name}/init",
    status_code=204,
    dependencies=[Depends(verify_is_euphrosyne_backend_or_admin)],
)
def init_run_data(
    project_slug: str,
    run_name: str,
    azure_client: DataAzureClient = Depends(get_hot_project_data_client),
):
    try:
        return azure_client.init_run_directory(run_name, project_slug)
    except FolderCreationError as error:
        return JSONResponse({"detail": error.message}, status_code=400)


@router.post(
    "/{project_slug}/rename/{new_project_name}",
    status_code=204,
    dependencies=[Depends(verify_is_euphrosyne_backend)],
)
def rename_project_folder(
    project_slug: str,
    new_project_name: str,
    azure_client: DataAzureClient = Depends(get_project_data_client),
):
    try:
        return azure_client.rename_project_directory(project_slug, new_project_name)
    except FolderCreationError as error:
        return JSONResponse({"detail": error.message}, status_code=400)


@router.post(
    "/{project_slug}/runs/{run_name}/rename/{new_run_name}",
    status_code=204,
    dependencies=[Depends(verify_is_euphrosyne_backend)],
)
def rename_run_folder(
    project_slug: str,
    run_name: str,
    new_run_name: str,
    azure_client: DataAzureClient = Depends(get_project_data_client),
):
    try:
        return azure_client.rename_run_directory(run_name, project_slug, new_run_name)
    except FolderCreationError as error:
        return JSONResponse({"detail": error.message}, status_code=400)


class CheckFoldersSyncBody(pydantic.BaseModel):
    project_slugs: list[str]


@router.post(
    "/check-folders-sync",
    status_code=200,
    dependencies=[Depends(verify_is_euphrosyne_backend)],
)
def check_folders_sync(
    body: CheckFoldersSyncBody,
    azure_client: DataAzureClient = Depends(get_hot_project_data_client),
):
    unsynced_dirs = []
    project_dirs = azure_client.list_project_dirs()
    for slug in body.project_slugs:
        if slug not in project_dirs:
            unsynced_dirs.append(slug)
    orphans_dirs = [dir for dir in project_dirs if dir not in body.project_slugs]
    return {"unsynced_dirs": unsynced_dirs, "orphan_dirs": orphans_dirs}


@router.post(
    "/projects/{project_slug}/cool",
    status_code=202,
    dependencies=[Depends(verify_is_euphrosyne_backend)],
    response_model=models.LifecycleOperation,
    response_model_exclude_none=True,
)
def cool_project_data(
    project_slug: str,
    background_tasks: BackgroundTasks,
    operation_id: UUID = Query(...),
):
    operation = models.LifecycleOperation(
        project_slug=project_slug,
        operation_id=operation_id,
        type=models.LifecycleOperationType.COOL,
    )
    return schedule_lifecycle_operation(
        operation=operation,
        background_tasks=background_tasks,
    )


@router.post(
    "/projects/{project_slug}/restore",
    status_code=202,
    dependencies=[Depends(verify_is_euphrosyne_backend)],
    response_model=models.LifecycleOperation,
    response_model_exclude_none=True,
)
def restore_project_data(
    project_slug: str,
    background_tasks: BackgroundTasks,
    operation_id: UUID = Query(...),
):
    operation = models.LifecycleOperation(
        project_slug=project_slug,
        operation_id=operation_id,
        type=models.LifecycleOperationType.RESTORE,
    )
    return schedule_lifecycle_operation(
        operation=operation,
        background_tasks=background_tasks,
    )


@router.post(
    "/projects/{project_slug}/delete/{storage_role}",
    status_code=202,
    dependencies=[Depends(verify_is_euphrosyne_backend)],
    response_model=models.FromDataDeletionAccepted,
    response_model_exclude_none=True,
)
def delete_project_data(
    project_slug: str,
    storage_role: StorageRole,
    background_tasks: BackgroundTasks,
    operation_id: UUID = Query(...),
):
    deletion = models.FromDataDeletionOperation(
        project_slug=project_slug,
        operation_id=operation_id,
        storage_role=storage_role,
    )
    return schedule_from_data_deletion(
        deletion=deletion,
        background_tasks=background_tasks,
    )


@router.get(
    "/projects/{project_slug}/cool/{operation_id}",
    status_code=200,
    dependencies=[Depends(verify_is_euphrosyne_backend)],
    response_model=models.LifecycleOperationStatusView,
    response_model_exclude_none=True,
)
def get_cool_project_data_status(
    project_slug: str,
    operation_id: UUID,
):
    try:
        return get_lifecycle_operation_status(
            project_slug=project_slug,
            operation_id=operation_id,
            operation_type=models.LifecycleOperationType.COOL,
        )
    except LifecycleOperationNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Operation not found") from exc


@router.get(
    "/projects/{project_slug}/restore/{operation_id}",
    status_code=200,
    dependencies=[Depends(verify_is_euphrosyne_backend)],
    response_model=models.LifecycleOperationStatusView,
    response_model_exclude_none=True,
)
def get_restore_project_data_status(
    project_slug: str,
    operation_id: UUID,
):
    try:
        return get_lifecycle_operation_status(
            project_slug=project_slug,
            operation_id=operation_id,
            operation_type=models.LifecycleOperationType.RESTORE,
        )
    except LifecycleOperationNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Operation not found") from exc
