""" Test routes in api.data.
Some routes may be tested in tests.main
(older tests that haven't been migrated to this module)"""

from unittest import mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.hdf5 import router
from auth import get_current_user
from clients.azure.data import IncorrectDataFilePath
from dependencies import get_storage_azure_client


@pytest.fixture(autouse=True)
def authenticate_user(app: FastAPI):
    app.dependency_overrides[get_current_user] = lambda: mock.MagicMock()


@pytest.mark.parametrize("route", [route.path for route in router.routes])  # type: ignore
@mock.patch("api.hdf5.validate_run_data_file_path")
def test_403_when_wrong_data_path(
    fn_mock: mock.MagicMock, route: str, client: TestClient
):
    fn_mock.side_effect = IncorrectDataFilePath("test")

    response = client.get(f"{route}?file=/&query=/")

    assert response.status_code == 403


@mock.patch("api.hdf5.validate_run_data_file_path", new=mock.MagicMock())
@mock.patch("api.hdf5.DataAzureClient")
def test_calls_download_run_file(
    data_azure_client_mock: mock.MagicMock, client: TestClient
):
    download_run_file_mock = mock.MagicMock()
    data_azure_client_mock.return_value.download_run_file = download_run_file_mock

    # mocking download_run_file_mock should raise an OSError
    # because it will try to open on a MagicMock
    with pytest.raises(OSError):
        client.get(f"/hdf5/meta?file=/filepath&query=/")

    download_run_file_mock.assert_called_once_with("/filepath")
