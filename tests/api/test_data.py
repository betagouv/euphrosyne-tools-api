""" Test routes in api.data.
Some routes may be tested in tests.main
(older tests that haven't been migrated to this module)"""

from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from auth import verify_is_euphrosyne_backend, verify_path_permission
from clients.azure.data import (
    FolderCreationError,
    IncorrectDataFilePath,
    RunDataNotFound,
)
from dependencies import get_storage_azure_client


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


def test_zip_project_run_data_when_path_incorrect(app: FastAPI, client: TestClient):
    app.dependency_overrides[verify_path_permission] = lambda: MagicMock()
    with patch("api.data.extract_info_from_path") as extract_info_from_path_mock:
        extract_info_from_path_mock.side_effect = IncorrectDataFilePath("incorrect")
        response = client.get("/data/run-data-zip?token=wrong-token&path=/a/wrong/path")
    assert response.status_code == 422


def test_zip_project_run_data_when_path_not_found_in_azure(
    app: FastAPI, client: TestClient
):
    iter_project_run_files_mock = MagicMock(side_effect=RunDataNotFound())
    app.dependency_overrides[get_storage_azure_client] = lambda: MagicMock(
        iter_project_run_files=iter_project_run_files_mock
    )
    app.dependency_overrides[verify_path_permission] = lambda: MagicMock()
    with patch("api.data.extract_info_from_path"):
        response = client.get("/data/run-data-zip?token=wrong-token&path=/a/wrong/path")
    assert response.status_code == 404


def test_zip_project_run_data(app: FastAPI, client: TestClient):
    iter_project_run_files_mock = MagicMock()
    app.dependency_overrides[get_storage_azure_client] = lambda: MagicMock(
        iter_project_run_files=iter_project_run_files_mock
    )
    app.dependency_overrides[verify_path_permission] = lambda: MagicMock()
    with patch(
        "api.data.stream_zip_from_azure_files"
    ) as stream_zip_from_azure_files_mock:
        stream_zip_from_azure_files_mock.return_value = (s.encode() for s in "abc")
        response = client.get(
            "/data/run-data-zip?token=wrong-token&path=projects/project-01/runs/runur/raw_data"
        )

    assert response.status_code == 200, response.content
    iter_project_run_files_mock.assert_called_once_with(
        "project-01", "runur", "raw_data"
    )
    assert response.headers.get("Content-Disposition").startswith(
        "attachment; filename=runur"
    )
    assert response.headers.get("Content-Disposition").endswith(".zip")
    assert response.headers.get["Content-Type"] == "application/zip"
    assert response.content.decode("utf-8") == "abc"
