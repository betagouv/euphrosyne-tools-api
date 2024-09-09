""" Test routes in api.data.
Some routes may be tested in tests.main
(older tests that haven't been migrated to this module)"""

from unittest.mock import AsyncMock, MagicMock, patch
import datetime
import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from auth import ExtraPayloadTokenGetter, get_current_user, verify_has_azure_permission

from auth import User, verify_is_euphrosyne_backend, verify_path_permission
from clients.azure.data import (
    FolderCreationError,
    IncorrectDataFilePath,
    RunDataNotFound,
)
from dependencies import get_storage_azure_client
from api.data import _verify_can_set_token_expiration
from hooks.euphrosyne import post_data_access_event


@pytest.fixture(autouse=True)
def authenticate_euphrosyne_backend(app: FastAPI):
    # pylint: disable=unnecessary-lambda
    app.dependency_overrides[verify_is_euphrosyne_backend] = lambda: MagicMock()


def test_init_project_data(app: FastAPI, client: TestClient):
    init_project_directory_mock = MagicMock()
    app.dependency_overrides[get_storage_azure_client] = lambda: MagicMock(
        init_project_directory=init_project_directory_mock
    )
    response = client.post("/data/project_01/init")

    assert response.status_code == 204
    init_project_directory_mock.assert_called_with("project_01")


def test_init_project_data_when_caught_error(app: FastAPI, client: TestClient):
    init_project_directory_mock = MagicMock(
        **{"side_effect": FolderCreationError("an error")}
    )
    app.dependency_overrides[get_storage_azure_client] = lambda: MagicMock(
        init_project_directory=init_project_directory_mock
    )
    response = client.post("/data/project_01/init")

    init_project_directory_mock.assert_called_with("project_01")
    assert response.status_code == 400
    assert response.json()["detail"] == "an error"


def test_init_run_data(app: FastAPI, client: TestClient):
    init_run_directory_mock = MagicMock()
    app.dependency_overrides[get_storage_azure_client] = lambda: MagicMock(
        init_run_directory=init_run_directory_mock
    )
    response = client.post("/data/project_01/runs/run1/init")

    assert response.status_code == 204
    init_run_directory_mock.assert_called_with("run1", "project_01")


def test_init_run_data_when_caught_error(app: FastAPI, client: TestClient):
    init_run_directory_mock = MagicMock(
        **{"side_effect": FolderCreationError("an error")}
    )
    app.dependency_overrides[get_storage_azure_client] = lambda: MagicMock(
        init_run_directory=init_run_directory_mock
    )
    response = client.post("/data/project_01/runs/run1/init")

    init_run_directory_mock.assert_called_with("run1", "project_01")
    assert response.status_code == 400
    assert response.json()["detail"] == "an error"


def test_change_run_name(app: FastAPI, client: TestClient):
    rename_run_directory_mock = MagicMock()
    app.dependency_overrides[get_storage_azure_client] = lambda: MagicMock(
        rename_run_directory=rename_run_directory_mock
    )
    response = client.post("/data/project_01/runs/run1/rename/run2")

    assert response.status_code == 204
    rename_run_directory_mock.assert_called_with("run1", "project_01", "run2")


def test_change_run_name_when_caught_error(app: FastAPI, client: TestClient):
    rename_run_directory_mock = MagicMock(
        **{"side_effect": FolderCreationError("an error")}
    )
    app.dependency_overrides[get_storage_azure_client] = lambda: MagicMock(
        rename_run_directory=rename_run_directory_mock
    )
    response = client.post("/data/project_01/runs/run1/rename/run2")

    rename_run_directory_mock.assert_called_with("run1", "project_01", "run2")
    assert response.status_code == 400
    assert response.json()["detail"] == "an error"


def test_change_project_name(app: FastAPI, client: TestClient):
    rename_project_directory_mock = MagicMock()
    app.dependency_overrides[get_storage_azure_client] = lambda: MagicMock(
        rename_project_directory=rename_project_directory_mock
    )
    response = client.post("/data/project_01/rename/project_02")

    assert response.status_code == 204
    rename_project_directory_mock.assert_called_with("project_01", "project_02")


def test_change_project_name_when_caught_error(app: FastAPI, client: TestClient):
    rename_project_directory_mock = MagicMock(
        **{"side_effect": FolderCreationError("an error")}
    )
    app.dependency_overrides[get_storage_azure_client] = lambda: MagicMock(
        rename_project_directory=rename_project_directory_mock
    )
    response = client.post("/data/project_01/rename/project_02")

    rename_project_directory_mock.assert_called_with("project_01", "project_02")
    assert response.status_code == 400
    assert response.json()["detail"] == "an error"


@patch("auth._decode_jwt", MagicMock(return_value={}))
def test_zip_project_run_data_when_path_incorrect(app: FastAPI, client: TestClient):
    app.dependency_overrides[verify_path_permission] = lambda: MagicMock()
    with patch("api.data.extract_info_from_path") as extract_info_from_path_mock:
        extract_info_from_path_mock.side_effect = IncorrectDataFilePath("incorrect")
        response = client.get("/data/run-data-zip?token=wrong-token&path=/a/wrong/path")
    assert response.status_code == 422


@patch("auth._decode_jwt", MagicMock(return_value={}))
def test_zip_project_run_data_when_path_not_found_in_azure(
    app: FastAPI, client: TestClient
):
    iter_project_run_files_async_mock = MagicMock(side_effect=RunDataNotFound())
    app.dependency_overrides[get_storage_azure_client] = lambda: MagicMock(
        iter_project_run_files_async=iter_project_run_files_async_mock
    )
    app.dependency_overrides[verify_path_permission] = lambda: MagicMock()
    with patch("api.data.extract_info_from_path"):
        response = client.get("/data/run-data-zip?token=token&path=/a/wrong/path")
    assert response.status_code == 404


@patch("auth._decode_jwt", MagicMock(return_value={}))
def test_zip_project_run_data(
    app: FastAPI, client: TestClient, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("AZURE_STORAGE_PROJECTS_LOCATION_PREFIX", "projects")

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
    monkeypatch.setenv("AZURE_STORAGE_PROJECTS_LOCATION_PREFIX", "projects")

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


@patch("api.data._verify_can_set_token_expiration", MagicMock())
@patch("api.data.validate_run_data_file_path", MagicMock())
def test_generate_signed_url_for_path_with_expiration(
    app: FastAPI, client: TestClient, monkeypatch: pytest.MonkeyPatch
):
    app.dependency_overrides[get_current_user] = lambda: User(
        id="1", projects=[], is_admin=True
    )
    with patch("api.data.generate_token_for_path") as generate_token_for_path_mock:
        response = client.get(
            "/data/project-name/token?path=/a/path&expiration=2024-07-15T15:51:27.911649"
        )
        generate_token_for_path_mock.assert_called_once_with(
            "/a/path",
            expiration=datetime.datetime.fromisoformat("2024-07-15T15:51:27.911649"),
            data_request=None,
        )
        assert response.status_code == 200
    del app.dependency_overrides[get_current_user]


def test_verify_can_set_token_expiration():
    with pytest.raises(HTTPException):
        _verify_can_set_token_expiration(user=User(id="1", projects=[], is_admin=False))
    _verify_can_set_token_expiration(user=User(id="1", projects=[], is_admin=True))
