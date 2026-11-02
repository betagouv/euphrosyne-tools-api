"""Test routes in api.data.
Some routes may be tested in tests.main
(older tests that haven't been migrated to this module)"""

from unittest import mock

import h5py  # type: ignore[import-untyped]
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.hdf5 import resolve_filepath, router
from auth import get_current_user
from path import IncorrectDataFilePath, RunDataTypeRef


@pytest.fixture(autouse=True)
def authenticate_user(app: FastAPI):
    app.dependency_overrides[get_current_user] = lambda: mock.MagicMock()


@pytest.mark.parametrize(
    "route",
    [route.path for route in router.routes],  # type: ignore
)
@mock.patch("api.hdf5.RunDataTypeRef.from_path")
def test_422_when_wrong_data_path(
    fn_mock: mock.MagicMock, route: str, client: TestClient
):
    fn_mock.side_effect = IncorrectDataFilePath("test")

    response = client.get(f"{route}?file=/&query=/")

    assert response.status_code == 422


@mock.patch(
    "api.hdf5.RunDataTypeRef.from_path",
    new=mock.MagicMock(return_value=RunDataTypeRef("project-01", "run-01", "raw_data")),
)
@mock.patch("api.hdf5.DataAzureClient")
def test_calls_download_run_file(
    data_azure_client_mock: mock.MagicMock, client: TestClient
):
    download_run_file_mock = mock.MagicMock()
    data_azure_client_mock.return_value.download_run_file = download_run_file_mock

    # mocking download_run_file_mock should raise an OSError
    # because it will try to open on a MagicMock
    with pytest.raises(OSError):
        client.get("/hdf5/meta?file=/filepath&query=/")

    download_run_file_mock.assert_called_once_with("/filepath")


@pytest.fixture
def hdf5_file(tmp_path):
    filepath = tmp_path / "data.h5"
    with h5py.File(filepath, "w") as f:
        f.create_dataset("values", data=[1, 2, 3])
    return str(filepath)


@pytest.mark.parametrize(
    "endpoint",
    ["attr", "attr/", "data", "data/", "meta", "meta/", "stats", "stats/"],
)
@mock.patch(
    "api.hdf5.RunDataTypeRef.from_path",
    new=mock.MagicMock(return_value=RunDataTypeRef("project-01", "run-01", "raw_data")),
)
@mock.patch("api.hdf5.DataAzureClient")
def test_serves_endpoint_without_redirect(
    data_azure_client_mock: mock.MagicMock,
    endpoint: str,
    hdf5_file: str,
    client: TestClient,
):
    resolve_filepath.cache_clear()
    data_azure_client_mock.return_value.download_run_file.return_value = hdf5_file

    response = client.get(
        f"/hdf5/{endpoint}?file=/filepath&path=/values", follow_redirects=False
    )

    assert response.status_code == 200


@pytest.mark.parametrize("endpoint", ["paths", "paths/"])
@mock.patch(
    "api.hdf5.RunDataTypeRef.from_path",
    new=mock.MagicMock(return_value=RunDataTypeRef("project-01", "run-01", "raw_data")),
)
@mock.patch("api.hdf5.DataAzureClient")
def test_serves_paths_without_redirect(
    data_azure_client_mock: mock.MagicMock,
    endpoint: str,
    hdf5_file: str,
    client: TestClient,
):
    resolve_filepath.cache_clear()
    data_azure_client_mock.return_value.download_run_file.return_value = hdf5_file

    response = client.get(
        f"/hdf5/{endpoint}?file=/filepath&path=/", follow_redirects=False
    )

    assert response.status_code == 200
    assert response.json() == ["/", "/values"]
