import os
from functools import lru_cache
from pathlib import Path
from typing import Annotated

from fastapi import Depends, HTTPException

from clients.azure import (
    BlobDataAzureClient,
    ConfigAzureClient,
    DataAzureClient,
    InfraAzureClient,
    VMAzureClient,
)
from clients.azure.images import ImageStorageClient
from clients.data_client import AbstractDataClient
from clients.guacamole import GuacamoleClient
from data_lifecycle.dependencies import fetch_project_lifecycle
from data_lifecycle.storage_resolver import resolve_backend
from data_lifecycle.storage_types import StorageBackend, StorageRole
from path import ProjectRef


@lru_cache()
def get_vm_azure_client():
    return VMAzureClient()


def get_project_from_path_or_param(
    project_slug: str | None = None, path: str | None = None
) -> str:
    if project_slug:
        return project_slug
    elif path:
        return ProjectRef.from_path(Path(path)).project_slug
    raise ValueError("project_slug or path must be provided to get the project.")


def get_project_lifecycle(
    project_slug: Annotated[str, Depends(get_project_from_path_or_param)],
) -> StorageRole:
    """Return the storage role for a project.

    The project slug is resolved from either the `project_slug` parameter or a
    filesystem `path` via `get_project_from_path_or_param`.

    If `DATA_BACKEND_COOL` is not set, the function falls back to
    `StorageRole.HOT`. Otherwise, it fetches the project's current storage role
    from the data lifecycle backend.
    """
    if not os.getenv("DATA_BACKEND_COOL"):
        return StorageRole.HOT
    return fetch_project_lifecycle(project_slug=project_slug)


@lru_cache()
def get_hot_project_data_client():
    return get_project_data_client(StorageRole.HOT)


@lru_cache()
def get_project_data_client(
    storage_role: Annotated[StorageRole, Depends(get_project_lifecycle)],
) -> AbstractDataClient:
    if storage_role not in [StorageRole.HOT, StorageRole.COOL]:
        raise HTTPException(
            status_code=409,
            detail="Project data is not in a stable state (HOT or COOL)",
        )
    backend = resolve_backend(storage_role)
    if backend == StorageBackend.AZURE_BLOB:
        return BlobDataAzureClient(storage_role=storage_role)
    if backend == StorageBackend.AZURE_FILESHARE:
        return DataAzureClient(storage_role=storage_role)
    raise ValueError(
        f"Invalid DATA_BACKEND value: {backend!r}. Allowed values are 'azure_blob' and 'azure_fileshare'."
    )


@lru_cache()
def get_config_azure_client():
    return ConfigAzureClient()


@lru_cache()
def get_infra_azure_client():
    return InfraAzureClient()


@lru_cache()
def get_guacamole_client():
    return GuacamoleClient()


@lru_cache()
def get_image_storage_client(project_slug: str) -> ImageStorageClient:
    return ImageStorageClient(project_slug=project_slug)
