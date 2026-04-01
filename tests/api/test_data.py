"""Test routes in api.data.
Some routes may be tested in tests.main
(older tests that haven't been migrated to this module)"""

import datetime
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api import data as data_api
from auth import (
    ExtraPayloadTokenGetter,
    User,
    get_current_user,
    verify_is_euphrosyne_backend,
    verify_is_euphrosyne_backend_or_admin,
    verify_path_permission,
)
from clients.azure.data import FolderCreationError, RunDataNotFound
from dependencies import get_hot_project_data_client, get_project_data_client
from data_lifecycle import operation as lifecycle_operation
from data_lifecycle.models import LifecycleState
from exceptions import StorageWriteNotAllowedError
from hooks.euphrosyne import post_data_access_event
from path import IncorrectDataFilePath


@pytest.fixture(autouse=True)
def authenticate_euphrosyne_backend(app: FastAPI):
    # pylint: disable=unnecessary-lambda
    app.dependency_overrides[verify_is_euphrosyne_backend] = lambda: MagicMock()


@pytest.fixture(autouse=True)
def authenticate_euphrosyne_backend_or_admin(app: FastAPI):
    # pylint: disable=unnecessary-lambda
    app.dependency_overrides[verify_is_euphrosyne_backend_or_admin] = (
        lambda: MagicMock()
    )


def test_init_project_data(app: FastAPI, client: TestClient):
    init_project_directory_mock = MagicMock()
    app.dependency_overrides[get_project_data_client] = lambda: MagicMock(
        init_project_directory=init_project_directory_mock
    )
    response = client.post("/data/project_01/init")

    assert response.status_code == 204
    init_project_directory_mock.assert_called_with("project_01")


def test_init_project_data_when_caught_error(app: FastAPI, client: TestClient):
    init_project_directory_mock = MagicMock(
        **{"side_effect": FolderCreationError("an error")}
    )
    app.dependency_overrides[get_project_data_client] = lambda: MagicMock(
        init_project_directory=init_project_directory_mock
    )
    response = client.post("/data/project_01/init")

    init_project_directory_mock.assert_called_with("project_01")
    assert response.status_code == 400
    assert response.json()["detail"] == "an error"


def test_init_project_data_when_storage_is_cool(app: FastAPI, client: TestClient):
    init_project_directory_mock = MagicMock(
        side_effect=StorageWriteNotAllowedError(
            "Write not allowed for storage role StorageRole.COOL in DataAzureClient."
        )
    )
    app.dependency_overrides[get_project_data_client] = lambda: MagicMock(
        init_project_directory=init_project_directory_mock
    )

    response = client.post("/data/project_01/init")

    init_project_directory_mock.assert_called_with("project_01")
    assert response.status_code == 409
    assert (
        response.json()["detail"]
        == "Write not allowed for storage role StorageRole.COOL in DataAzureClient."
    )


def test_init_run_data(app: FastAPI, client: TestClient):
    init_run_directory_mock = MagicMock()
    app.dependency_overrides[get_project_data_client] = lambda: MagicMock(
        init_run_directory=init_run_directory_mock
    )
    response = client.post("/data/project_01/runs/run1/init")

    assert response.status_code == 204
    init_run_directory_mock.assert_called_with("run1", "project_01")


def test_init_run_data_when_caught_error(app: FastAPI, client: TestClient):
    init_run_directory_mock = MagicMock(
        **{"side_effect": FolderCreationError("an error")}
    )
    app.dependency_overrides[get_project_data_client] = lambda: MagicMock(
        init_run_directory=init_run_directory_mock
    )
    response = client.post("/data/project_01/runs/run1/init")

    init_run_directory_mock.assert_called_with("run1", "project_01")
    assert response.status_code == 400
    assert response.json()["detail"] == "an error"


def test_change_run_name(app: FastAPI, client: TestClient):
    rename_run_directory_mock = MagicMock()
    app.dependency_overrides[get_project_data_client] = lambda: MagicMock(
        rename_run_directory=rename_run_directory_mock
    )
    response = client.post("/data/project_01/runs/run1/rename/run2")

    assert response.status_code == 204
    rename_run_directory_mock.assert_called_with("run1", "project_01", "run2")


def test_change_run_name_when_caught_error(app: FastAPI, client: TestClient):
    rename_run_directory_mock = MagicMock(
        **{"side_effect": FolderCreationError("an error")}
    )
    app.dependency_overrides[get_project_data_client] = lambda: MagicMock(
        rename_run_directory=rename_run_directory_mock
    )
    response = client.post("/data/project_01/runs/run1/rename/run2")

    rename_run_directory_mock.assert_called_with("run1", "project_01", "run2")
    assert response.status_code == 400
    assert response.json()["detail"] == "an error"


def test_change_project_name(app: FastAPI, client: TestClient):
    rename_project_directory_mock = MagicMock()
    app.dependency_overrides[get_project_data_client] = lambda: MagicMock(
        rename_project_directory=rename_project_directory_mock
    )
    response = client.post("/data/project_01/rename/project_02")

    assert response.status_code == 204
    rename_project_directory_mock.assert_called_with("project_01", "project_02")


def test_change_project_name_when_caught_error(app: FastAPI, client: TestClient):
    rename_project_directory_mock = MagicMock(
        **{"side_effect": FolderCreationError("an error")}
    )
    app.dependency_overrides[get_project_data_client] = lambda: MagicMock(
        rename_project_directory=rename_project_directory_mock
    )
    response = client.post("/data/project_01/rename/project_02")

    rename_project_directory_mock.assert_called_with("project_01", "project_02")
    assert response.status_code == 400
    assert response.json()["detail"] == "an error"


def test_delete_project_data_accepts_and_schedules_background_task(
    app: FastAPI, client: TestClient
):
    operation_id = uuid4()
    with (
        patch.object(
            data_api,
            "fetch_project_lifecycle",
            return_value=LifecycleState.COOL,
        ),
        patch("fastapi.BackgroundTasks.add_task") as add_task_mock,
    ):
        response = client.post(
            f"/data/projects/project-01/delete/HOT?operation_id={operation_id}"
        )

    assert response.status_code == 202
    assert response.json() == {
        "project_slug": "project-01",
        "operation_id": str(operation_id),
        "storage_role": "HOT",
        "phase": "FROM_DATA_DELETION",
        "status": "ACCEPTED",
    }
    assert add_task_mock.call_count == 1
    args, kwargs = add_task_mock.call_args
    assert args[0] == lifecycle_operation._execute_from_data_deletion
    assert kwargs["deletion"] == lifecycle_operation.FromDataDeletionOperation(
        project_slug="project-01",
        operation_id=operation_id,
        storage_role=lifecycle_operation.StorageRole.HOT,
    )


def test_delete_project_data_missing_operation_id_returns_422(
    app: FastAPI, client: TestClient
):
    with patch.object(
        data_api,
        "fetch_project_lifecycle",
        return_value=LifecycleState.COOL,
    ):
        response = client.post("/data/projects/project-01/delete/HOT")

    assert response.status_code == 422


def test_delete_project_data_invalid_operation_id_returns_422(
    app: FastAPI, client: TestClient
):
    with patch.object(
        data_api,
        "fetch_project_lifecycle",
        return_value=LifecycleState.COOL,
    ):
        response = client.post("/data/projects/project-01/delete/HOT?operation_id=bad")

    assert response.status_code == 422


def test_delete_project_data_rejects_active_storage_side(
    app: FastAPI, client: TestClient
):
    with patch.object(
        data_api,
        "fetch_project_lifecycle",
        return_value=LifecycleState.COOL,
    ):
        response = client.post(
            f"/data/projects/project-01/delete/COOL?operation_id={uuid4()}"
        )

    assert response.status_code == 409
    assert response.json()["detail"] == "Cannot delete active storage side COOL"


@pytest.mark.parametrize("state", [LifecycleState.COOLING, LifecycleState.RESTORING])
def test_delete_project_data_rejects_transitional_state(
    app: FastAPI,
    client: TestClient,
    state: LifecycleState,
):
    with patch.object(data_api, "fetch_project_lifecycle", return_value=state):
        response = client.post(
            f"/data/projects/project-01/delete/HOT?operation_id={uuid4()}"
        )

    assert response.status_code == 409
    assert (
        response.json()["detail"]
        == "Project data is not in a stable state (HOT or COOL)"
    )


def test_delete_project_data_duplicate_request_does_not_schedule_twice(
    app: FastAPI, client: TestClient
):
    lifecycle_operation._reset_lifecycle_operation_guard()
    operation_id = uuid4()
    with (
        patch.object(
            data_api,
            "fetch_project_lifecycle",
            return_value=LifecycleState.COOL,
        ),
        patch("fastapi.BackgroundTasks.add_task") as add_task_mock,
    ):
        first_response = client.post(
            f"/data/projects/project-01/delete/HOT?operation_id={operation_id}"
        )
        second_response = client.post(
            f"/data/projects/project-01/delete/HOT?operation_id={operation_id}"
        )

    assert first_response.status_code == 202
    assert second_response.status_code == 202
    assert add_task_mock.call_count == 1


@patch("auth._decode_jwt", MagicMock(return_value={}))
def test_zip_project_run_data_when_path_incorrect(
    app: FastAPI, client: TestClient, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("DATA_PROJECTS_LOCATION_PREFIX", "projects")
    app.dependency_overrides[verify_path_permission] = lambda: MagicMock()
    with patch("api.data.RunDataTypeRef.from_path") as from_path_mock:
        from_path_mock.side_effect = IncorrectDataFilePath("incorrect")
        response = client.get("/data/run-data-zip?token=wrong-token&path=/a/wrong/path")
    assert response.status_code == 422


@patch("auth._decode_jwt", MagicMock(return_value={}))
def test_zip_project_run_data_when_path_not_found_in_azure(
    app: FastAPI, client: TestClient, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("DATA_PROJECTS_LOCATION_PREFIX", "projects")
    iter_project_run_files_async_mock = MagicMock(side_effect=RunDataNotFound())
    app.dependency_overrides[get_project_data_client] = lambda: MagicMock(
        iter_project_run_files_async=iter_project_run_files_async_mock
    )
    app.dependency_overrides[verify_path_permission] = lambda: MagicMock()
    response = client.get(
        "/data/run-data-zip?token=token&path=projects/project-01/runs/runur/raw_data"
    )
    assert response.status_code == 404


@patch("auth._decode_jwt", MagicMock(return_value={}))
def test_zip_project_run_data(
    app: FastAPI, client: TestClient, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("DATA_PROJECTS_LOCATION_PREFIX", "projects")

    app.dependency_overrides[verify_path_permission] = lambda: MagicMock()
    app.dependency_overrides[ExtraPayloadTokenGetter] = lambda: MagicMock()
    with patch(
        "api.data.stream_zip_from_azure_files_async"
    ) as stream_zip_from_azure_files_mock:
        stream_zip_from_azure_files_mock.return_value = (s.encode() for s in "abc")
        response = client.get(
            "/data/run-data-zip?token=token&path=projects/project-01/runs/runur/raw_data"
        )

    assert response.status_code == 200, response.content
    assert response.headers.get("Content-Disposition").startswith(
        "attachment; filename=runur"
    )
    assert response.headers.get("Content-Disposition").endswith(".zip")
    assert response.headers.get("Content-Type") == "application/zip"
    assert response.content.decode("utf-8") == "abc"

    app.dependency_overrides = {}


@patch("auth._decode_jwt")
def test_zip_project_run_data_with_data_request(
    decode_jwt_mock: MagicMock,
    app: FastAPI,
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("DATA_PROJECTS_LOCATION_PREFIX", "projects")

    app.dependency_overrides[verify_path_permission] = lambda: MagicMock()
    with patch("fastapi.BackgroundTasks.add_task") as add_background_task_mock:
        with patch(
            "api.data.stream_zip_from_azure_files_async"
        ) as stream_zip_from_azure_files_mock:
            decode_jwt_mock.return_value = {"data_request": "12"}
            stream_zip_from_azure_files_mock.return_value = (s.encode() for s in "abc")
            response = client.get(
                "/data/run-data-zip?token=token&path=projects/project-01/runs/runur/raw_data&data_request=12"
            )

    assert response.status_code == 200
    assert add_background_task_mock.call_count == 1
    assert add_background_task_mock.call_args[0][0] == post_data_access_event
    assert add_background_task_mock.call_args[1]["data_request"] == "12"


def test_generate_signed_url_for_path_with_expiration(
    app: FastAPI, client: TestClient, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("DATA_PROJECTS_LOCATION_PREFIX", "projects")
    app.dependency_overrides[get_current_user] = lambda: User(
        id="1", projects=[], is_admin=True
    )
    with patch("api.data.generate_token_for_path") as generate_token_for_path_mock:
        response = client.get(
            "/data/project-name/token?path=projects/project-01/runs/run-01/HDF5/data.h5&expiration=2024-07-15T15:51:27.911649"
        )
        generate_token_for_path_mock.assert_called_once_with(
            "projects/project-01/runs/run-01/HDF5/data.h5",
            expiration=datetime.datetime.fromisoformat("2024-07-15T15:51:27.911649"),
            data_request=None,
        )
        assert response.status_code == 200
    del app.dependency_overrides[get_current_user]


def test_generate_signed_url_for_path_with_expiration_forbidden_for_non_admin(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("DATA_PROJECTS_LOCATION_PREFIX", "projects")

    response = client.get(
        "/data/project-01/token?path=projects/project-01/runs/run-01/raw_data/file.txt&expiration=2024-07-15T15:51:27.911649"
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "Only admins can set token expiration"


def test_generate_signed_url_for_path_when_path_incorrect(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("DATA_PROJECTS_LOCATION_PREFIX", "projects")

    response = client.get("/data/project-01/token?path=projects/project-01/not-runs")

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["query", "path"]
    assert "path must start with" in response.json()["detail"][0]["msg"]


def test_generate_signed_url_for_path_forbidden_when_path_project_differs(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("DATA_PROJECTS_LOCATION_PREFIX", "projects")

    with patch("api.data.generate_token_for_path") as generate_token_for_path_mock:
        response = client.get(
            "/data/project-01/token?path=projects/project-02/runs/run-01/raw_data/file.txt"
        )

    assert response.status_code == 403
    assert response.json()["detail"] == "User does not have access to this project"
    generate_token_for_path_mock.assert_not_called()


def test_check_folders_sync(app: FastAPI, client: TestClient):
    app.dependency_overrides[verify_is_euphrosyne_backend] = lambda: MagicMock()
    app.dependency_overrides[get_hot_project_data_client] = lambda: MagicMock(
        list_project_dirs=MagicMock(return_value=["project1", "project2"])
    )
    response = client.post(
        "/data/check-folders-sync",
        json={"project_slugs": ["project1", "unsynced project"]},
    )
    assert response.status_code == 200
    assert response.json() == {
        "unsynced_dirs": ["unsynced project"],
        "orphan_dirs": ["project2"],
    }
