from types import SimpleNamespace
from unittest.mock import patch

import pytest
from fastapi import HTTPException

import dependencies
from data_lifecycle.models import LifecycleState
from data_lifecycle.storage_types import StorageBackend, StorageRole


@pytest.fixture(autouse=True)
def clear_dependency_caches():
    dependencies.get_project_data_client.cache_clear()
    dependencies.get_hot_project_data_client.cache_clear()
    dependencies.get_vm_azure_client.cache_clear()
    dependencies.get_config_azure_client.cache_clear()
    dependencies.get_infra_azure_client.cache_clear()
    dependencies.get_guacamole_client.cache_clear()
    dependencies.get_image_storage_client.cache_clear()
    dependencies.get_data_visualization_llm_client.cache_clear()
    dependencies.get_data_visualization_python_sessions_client.cache_clear()
    yield
    dependencies.get_project_data_client.cache_clear()
    dependencies.get_hot_project_data_client.cache_clear()
    dependencies.get_vm_azure_client.cache_clear()
    dependencies.get_config_azure_client.cache_clear()
    dependencies.get_infra_azure_client.cache_clear()
    dependencies.get_guacamole_client.cache_clear()
    dependencies.get_image_storage_client.cache_clear()
    dependencies.get_data_visualization_llm_client.cache_clear()
    dependencies.get_data_visualization_python_sessions_client.cache_clear()


def test_get_project_from_path_or_param_prefers_project_slug():
    assert dependencies.get_project_from_path_or_param(
        project_slug="project-01",
        path="projects/ignored-project",
    ) == ("project-01")


def test_get_project_from_path_or_param_extracts_project_slug_from_path(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("DATA_PROJECTS_LOCATION_PREFIX", "projects")

    assert dependencies.get_project_from_path_or_param(
        path="projects/project-01/runs/run-01/raw_data"
    ) == ("project-01")


def test_get_project_from_path_or_param_raises_without_inputs():
    with pytest.raises(ValueError, match="project_slug or path must be provided"):
        dependencies.get_project_from_path_or_param()


def test_get_project_lifecycle_returns_hot_without_cool_backend(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.delenv("DATA_BACKEND_COOL", raising=False)

    with patch.object(dependencies, "fetch_project_lifecycle") as fetch_mock:
        assert dependencies.get_project_lifecycle("project-01") == LifecycleState.HOT

    fetch_mock.assert_not_called()


def test_get_project_lifecycle_fetches_role_when_cool_backend_enabled(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("DATA_BACKEND_COOL", "azure_blob")

    with patch.object(
        dependencies,
        "fetch_project_lifecycle",
        return_value=LifecycleState.COOL,
    ) as fetch_mock:
        assert dependencies.get_project_lifecycle("project-01") == LifecycleState.COOL

    fetch_mock.assert_called_once_with(project_slug="project-01")


def test_get_project_data_client_returns_blob_client_for_blob_backend():
    blob_client = object()

    with (
        patch.object(
            dependencies,
            "resolve_backend",
            return_value=StorageBackend.AZURE_BLOB,
        ) as resolve_backend_mock,
        patch.object(
            dependencies,
            "BlobDataAzureClient",
            return_value=blob_client,
        ) as blob_client_mock,
        patch.object(dependencies, "DataAzureClient") as data_client_mock,
    ):
        result = dependencies.get_project_data_client(StorageRole.COOL)

    assert result is blob_client
    resolve_backend_mock.assert_called_once_with(StorageRole.COOL)
    blob_client_mock.assert_called_once_with(storage_role=StorageRole.COOL)
    data_client_mock.assert_not_called()


def test_get_project_data_client_returns_fileshare_client_for_fileshare_backend():
    fileshare_client = object()

    with (
        patch.object(
            dependencies,
            "resolve_backend",
            return_value=StorageBackend.AZURE_FILESHARE,
        ) as resolve_backend_mock,
        patch.object(
            dependencies,
            "DataAzureClient",
            return_value=fileshare_client,
        ) as data_client_mock,
        patch.object(dependencies, "BlobDataAzureClient") as blob_client_mock,
    ):
        result = dependencies.get_project_data_client(StorageRole.HOT)

    assert result is fileshare_client
    resolve_backend_mock.assert_called_once_with(StorageRole.HOT)
    data_client_mock.assert_called_once_with(storage_role=StorageRole.HOT)
    blob_client_mock.assert_not_called()


def test_get_project_data_client_raises_when_lifecycle_state_is_not_stable():
    with pytest.raises(HTTPException) as error:
        dependencies.get_project_data_client(LifecycleState.COOLING)
    print(error)
    assert error.value.status_code == 409
    assert error.value.detail == "Project data is not in a stable state (HOT or COOL)"


def test_get_hot_project_data_client_delegates_to_hot_role():
    hot_client = SimpleNamespace(storage_role=StorageRole.HOT)

    with patch.object(
        dependencies,
        "get_project_data_client",
        return_value=hot_client,
    ) as get_project_data_client_mock:
        result = dependencies.get_hot_project_data_client()

    assert result is hot_client
    get_project_data_client_mock.assert_called_once_with(StorageRole.HOT)


def test_get_data_visualization_llm_client_uses_albert_configuration(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("ALBERT_API_KEY", "token")
    monkeypatch.setenv("ALBERT_MODEL", "model")
    llm_client = object()

    with patch.object(
        dependencies,
        "AlbertClient",
        return_value=llm_client,
    ) as client_class:
        result = dependencies.get_data_visualization_llm_client()

    assert result is llm_client
    client_class.assert_called_once_with(
        api_key="token",
        model="model",
        timeout_seconds=dependencies.DATA_VISUALIZATION_TIMEOUT_SECONDS,
    )


@pytest.mark.parametrize("missing_variable", ["ALBERT_API_KEY", "ALBERT_MODEL"])
def test_get_data_visualization_llm_client_requires_configuration(
    monkeypatch: pytest.MonkeyPatch,
    missing_variable: str,
):
    monkeypatch.setenv("ALBERT_API_KEY", "token")
    monkeypatch.setenv("ALBERT_MODEL", "model")
    monkeypatch.delenv(missing_variable)

    with pytest.raises(HTTPException) as error:
        dependencies.get_data_visualization_llm_client()

    assert error.value.status_code == 503
    assert error.value.detail == "Le service de visualisation n'est pas configuré."


def test_get_data_visualization_python_sessions_client_uses_azure_pool(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("AZURE_SESSION_POOL_ENDPOINT", "https://sessions.example/pool")
    sessions = object()

    with patch.object(
        dependencies,
        "AzurePythonSessionsClient",
        return_value=sessions,
    ) as client_class:
        result = dependencies.get_data_visualization_python_sessions_client()

    assert result is sessions
    client_class.assert_called_once_with(
        "https://sessions.example/pool",
        execution_timeout_seconds=dependencies.DATA_VISUALIZATION_TIMEOUT_SECONDS,
    )


def test_get_data_visualization_python_sessions_client_requires_azure_pool(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.delenv("AZURE_SESSION_POOL_ENDPOINT", raising=False)

    with pytest.raises(HTTPException) as error:
        dependencies.get_data_visualization_python_sessions_client()

    assert error.value.status_code == 503
    assert error.value.detail == (
        "Le service d'exécution Python isolé n'est pas configuré."
    )
