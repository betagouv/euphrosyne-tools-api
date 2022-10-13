""" Test routes in api.data.
Some routes may be tested in tests.main
(older tests that haven't been migrated to this module)"""

from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from auth import verify_is_euphrosyne_backend
from clients.azure.data import FolderCreationError
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
    init_run_directory_mock.assert_called_with("project_01", "run1")


def test_init_run_data_when_caught_error(app: FastAPI, client: TestClient):
    init_run_directory_mock = MagicMock(
        **{"side_effect": FolderCreationError("an error")}
    )
    app.dependency_overrides[get_storage_azure_client] = lambda: MagicMock(
        init_run_directory=init_run_directory_mock
    )
    response = client.post("/data/project_01/runs/run1/init")

    init_run_directory_mock.assert_called_with("project_01", "run1")
    assert response.status_code == 400
    assert response.json()["detail"] == "an error"
