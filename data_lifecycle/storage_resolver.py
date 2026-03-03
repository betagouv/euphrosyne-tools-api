from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum

from fastapi import HTTPException, status


class StorageRole(str, Enum):
    HOT = "HOT"
    COOL = "COOL"


class StorageBackend(str, Enum):
    AZURE_FILESHARE = "AZURE_FILESHARE"
    AZURE_BLOB = "AZURE_BLOB"


@dataclass(frozen=True)
class DataLocation:
    role: StorageRole
    backend: StorageBackend
    project_slug: str
    uri: str


class StorageConfigurationError(RuntimeError):
    pass


class CoolingDisabledError(StorageConfigurationError):
    pass


_BACKEND_VALUE_MAP: dict[str, StorageBackend] = {
    "azure_fileshare": StorageBackend.AZURE_FILESHARE,
    "azure_blob": StorageBackend.AZURE_BLOB,
}


def resolve_hot_location(project_slug: str) -> DataLocation:
    return resolve_location(StorageRole.HOT, project_slug)


def resolve_cool_location(project_slug: str) -> DataLocation:
    return resolve_location(StorageRole.COOL, project_slug)


def resolve_location(role: StorageRole, project_slug: str) -> DataLocation:
    _validate_project_slug(project_slug)
    backend = _resolve_backend(role)
    account = _require_env("AZURE_STORAGE_ACCOUNT")
    prefix = _get_prefix(role)

    if backend == StorageBackend.AZURE_FILESHARE:
        share = _require_env(_fileshare_env_var(role))
        uri = _build_fileshare_uri(
            account=account,
            share=share,
            prefix=prefix,
            project_slug=project_slug,
        )
    else:
        container = _require_env(_blob_env_var(role))
        uri = _build_blob_uri(
            account=account,
            container=container,
            prefix=prefix,
            project_slug=project_slug,
        )

    return DataLocation(
        role=role,
        backend=backend,
        project_slug=project_slug,
        uri=uri,
    )


def _resolve_backend(role: StorageRole) -> StorageBackend:
    if role == StorageRole.HOT:
        value = os.getenv("DATA_BACKEND")
        if not value:
            raise StorageConfigurationError(
                "DATA_BACKEND environment variable is not set"
            )
        return _parse_backend_value(value, "DATA_BACKEND")

    value = os.getenv("DATA_BACKEND_COOL")
    if not value:
        raise CoolingDisabledError(
            "Cooling is disabled because DATA_BACKEND_COOL is not set"
        )
    return _parse_backend_value(value, "DATA_BACKEND_COOL")


def _parse_backend_value(value: str, env_name: str) -> StorageBackend:
    normalized = value.strip().lower()
    backend = _BACKEND_VALUE_MAP.get(normalized)
    if backend is None:
        raise StorageConfigurationError(
            f"{env_name} must be 'azure_fileshare' or 'azure_blob'"
        )
    return backend


def _get_prefix(role: StorageRole) -> str:
    env_name = (
        "DATA_PROJECTS_LOCATION_PREFIX"
        if role == StorageRole.HOT
        else "DATA_PROJECTS_LOCATION_PREFIX_COOL"
    )
    raw_prefix = os.getenv(env_name, "")
    return _normalize_prefix(raw_prefix)


def _normalize_prefix(prefix: str) -> str:
    parts = [part for part in prefix.split("/") if part]
    return "/".join(parts)


def _build_fileshare_uri(
    *, account: str, share: str, prefix: str, project_slug: str
) -> str:
    path = _join_path(share, prefix, project_slug)
    return f"https://{account}.file.core.windows.net/{path}"


def _build_blob_uri(
    *, account: str, container: str, prefix: str, project_slug: str
) -> str:
    path = _join_path(container, prefix, project_slug)
    return f"https://{account}.blob.core.windows.net/{path}"


def _join_path(*parts: str) -> str:
    return "/".join([part for part in parts if part])


def _fileshare_env_var(role: StorageRole) -> str:
    return (
        "AZURE_STORAGE_FILESHARE"
        if role == StorageRole.HOT
        else "AZURE_STORAGE_FILESHARE_COOL"
    )


def _blob_env_var(role: StorageRole) -> str:
    return (
        "AZURE_STORAGE_DATA_CONTAINER"
        if role == StorageRole.HOT
        else "AZURE_STORAGE_DATA_CONTAINER_COOL"
    )


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise StorageConfigurationError(f"{name} environment variable is not set")
    return value


def _validate_project_slug(project_slug: str) -> None:
    if project_slug is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="project_slug must be provided",
        )
    if project_slug == "":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="project_slug must not be empty",
        )
    if project_slug != project_slug.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="project_slug must not contain leading or trailing whitespace",
        )
    if "/" in project_slug or "\\" in project_slug:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="project_slug must not contain '/' or '\\'",
        )
    if ".." in project_slug:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="project_slug must not contain '..'",
        )
