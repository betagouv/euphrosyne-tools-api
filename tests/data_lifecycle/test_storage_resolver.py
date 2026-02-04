import pytest
from fastapi import HTTPException

from data_lifecycle.storage_resolver import (
    CoolingDisabledError,
    DataLocation,
    StorageBackend,
    StorageRole,
    resolve_cool_location,
    resolve_hot_location,
)


@pytest.fixture(autouse=True)
def clear_storage_env(monkeypatch: pytest.MonkeyPatch):
    keys = [
        "DATA_BACKEND",
        "DATA_BACKEND_COOL",
        "DATA_PROJECTS_LOCATION_PREFIX",
        "DATA_PROJECTS_LOCATION_PREFIX_COOL",
        "AZURE_STORAGE_ACCOUNT",
        "AZURE_STORAGE_FILESHARE",
        "AZURE_STORAGE_FILESHARE_COOL",
        "AZURE_STORAGE_DATA_CONTAINER",
        "AZURE_STORAGE_DATA_CONTAINER_COOL",
    ]
    for key in keys:
        monkeypatch.delenv(key, raising=False)


def _set_hot_fileshare_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATA_BACKEND", "azure_fileshare")
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "storageaccount")
    monkeypatch.setenv("AZURE_STORAGE_FILESHARE", "fileshare")


def _set_hot_blob_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATA_BACKEND", "azure_blob")
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "storageaccount")
    monkeypatch.setenv("AZURE_STORAGE_DATA_CONTAINER", "container")


def _set_cool_blob_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATA_BACKEND_COOL", "azure_blob")
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "storageaccount")
    monkeypatch.setenv("AZURE_STORAGE_DATA_CONTAINER_COOL", "cool-container")


def _set_cool_fileshare_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATA_BACKEND_COOL", "azure_fileshare")
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "storageaccount")
    monkeypatch.setenv("AZURE_STORAGE_FILESHARE_COOL", "cool-share")


def test_resolver_determinism(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_hot_fileshare_env(monkeypatch)
    monkeypatch.setenv("DATA_PROJECTS_LOCATION_PREFIX", "projects")

    first = resolve_hot_location("project-01")
    second = resolve_hot_location("project-01")

    assert first == second
    assert isinstance(first, DataLocation)
    assert first.uri == second.uri


def test_resolver_hot_fileshare_golden_uri(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_hot_fileshare_env(monkeypatch)
    monkeypatch.setenv("DATA_PROJECTS_LOCATION_PREFIX", "projects")

    location = resolve_hot_location("project-01")

    assert location.role == StorageRole.HOT
    assert location.backend == StorageBackend.AZURE_FILESHARE
    assert (
        location.uri
        == "https://storageaccount.file.core.windows.net/fileshare/projects/project-01"
    )


def test_resolver_cool_blob_golden_uri(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_cool_blob_env(monkeypatch)
    monkeypatch.setenv("DATA_PROJECTS_LOCATION_PREFIX_COOL", "cool/projects")

    location = resolve_cool_location("project-01")

    assert location.role == StorageRole.COOL
    assert location.backend == StorageBackend.AZURE_BLOB
    assert (
        location.uri
        == "https://storageaccount.blob.core.windows.net/cool-container/cool/projects/project-01"
    )


def test_prefix_joining_with_empty_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_hot_fileshare_env(monkeypatch)
    monkeypatch.setenv("DATA_PROJECTS_LOCATION_PREFIX", "")

    location = resolve_hot_location("project-01")

    assert (
        location.uri
        == "https://storageaccount.file.core.windows.net/fileshare/project-01"
    )


def test_prefix_joining_with_normalization(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_hot_blob_env(monkeypatch)
    monkeypatch.setenv("DATA_PROJECTS_LOCATION_PREFIX", "/base//projects/")

    location = resolve_hot_location("project-01")

    assert (
        location.uri
        == "https://storageaccount.blob.core.windows.net/container/base/projects/project-01"
    )


@pytest.mark.parametrize(
    "project_slug",
    ["", "../x", "a/b", "a\\b", "a..b", " a "],
)
def test_invalid_project_slug_rejected(
    monkeypatch: pytest.MonkeyPatch, project_slug: str
) -> None:
    _set_hot_fileshare_env(monkeypatch)

    with pytest.raises(HTTPException) as exc:
        resolve_hot_location(project_slug)

    assert exc.value.status_code == 400


def test_role_backend_selection(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_hot_blob_env(monkeypatch)
    monkeypatch.setenv("DATA_PROJECTS_LOCATION_PREFIX", "projects")

    location = resolve_hot_location("project-01")

    assert location.backend == StorageBackend.AZURE_BLOB

    _set_cool_fileshare_env(monkeypatch)
    monkeypatch.setenv("DATA_PROJECTS_LOCATION_PREFIX_COOL", "cool")

    cool_location = resolve_cool_location("project-01")

    assert cool_location.backend == StorageBackend.AZURE_FILESHARE


def test_cool_resolution_disabled_when_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_hot_fileshare_env(monkeypatch)

    with pytest.raises(CoolingDisabledError):
        resolve_cool_location("project-01")
