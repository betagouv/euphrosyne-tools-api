from unittest.mock import call, patch

import pytest
from pytest import MonkeyPatch

from clients.azure.blob_data import BlobDataAzureClient
from data_lifecycle.storage_types import StorageRole


def _mock_blob_base_init(self, container_name: str):
    self.storage_account_name = "storageaccount"
    self._storage_key = "storage-key"
    self.container_name = container_name


@pytest.fixture
def hot_client(monkeypatch: MonkeyPatch):
    monkeypatch.setenv("AZURE_STORAGE_DATA_CONTAINER", "hot-container")
    monkeypatch.setenv("AZURE_STORAGE_DATA_CONTAINER_COOL", "cool-container")
    with patch(
        "clients.azure.blob_data.BlobAzureClient.__init__", autospec=True
    ) as base_init_mock:
        base_init_mock.side_effect = _mock_blob_base_init
        return BlobDataAzureClient(storage_role=StorageRole.HOT)


@pytest.fixture
def cool_client(monkeypatch: MonkeyPatch):
    monkeypatch.setenv("AZURE_STORAGE_DATA_CONTAINER", "hot-container")
    monkeypatch.setenv("AZURE_STORAGE_DATA_CONTAINER_COOL", "cool-container")
    with patch(
        "clients.azure.blob_data.BlobAzureClient.__init__", autospec=True
    ) as base_init_mock:
        base_init_mock.side_effect = _mock_blob_base_init
        return BlobDataAzureClient(storage_role=StorageRole.COOL)


def test_init_uses_hot_container(monkeypatch: MonkeyPatch):
    monkeypatch.setenv("AZURE_STORAGE_DATA_CONTAINER", "hot-container")
    monkeypatch.setenv("AZURE_STORAGE_DATA_CONTAINER_COOL", "cool-container")
    with patch(
        "clients.azure.blob_data.BlobAzureClient.__init__", return_value=None
    ) as base_init_mock:
        default_client = BlobDataAzureClient()
        hot_client = BlobDataAzureClient(storage_role=StorageRole.HOT)
    assert base_init_mock.call_args_list == [
        call(container_name="hot-container"),
        call(container_name="hot-container"),
    ]
    assert default_client.storage_role == StorageRole.HOT
    assert hot_client.storage_role == StorageRole.HOT


def test_init_uses_cool_container(monkeypatch: MonkeyPatch):
    monkeypatch.setenv("AZURE_STORAGE_DATA_CONTAINER", "hot-container")
    monkeypatch.setenv("AZURE_STORAGE_DATA_CONTAINER_COOL", "cool-container")
    with patch(
        "clients.azure.blob_data.BlobAzureClient.__init__", return_value=None
    ) as base_init_mock:
        cool_client = BlobDataAzureClient(storage_role=StorageRole.COOL)
    base_init_mock.assert_called_once_with(container_name="cool-container")
    assert cool_client.storage_role == StorageRole.COOL


def test_init_raises_if_container_not_configured(monkeypatch: MonkeyPatch):
    monkeypatch.delenv("AZURE_STORAGE_DATA_CONTAINER", raising=False)
    with patch(
        "clients.azure.blob_data.BlobAzureClient.__init__", return_value=None
    ) as base_init_mock:
        with pytest.raises(
            ValueError,
            match="AZURE_STORAGE_DATA_CONTAINER environment variable is not set",
        ):
            BlobDataAzureClient()
    base_init_mock.assert_not_called()


def test_init_raises_for_unsupported_storage_role():
    with patch(
        "clients.azure.blob_data.BlobAzureClient.__init__", return_value=None
    ) as base_init_mock:
        with pytest.raises(ValueError, match="Unsupported storage role"):
            BlobDataAzureClient(storage_role="WARM")  # type: ignore[arg-type]
    base_init_mock.assert_not_called()


@patch("clients.azure.blob_data.generate_blob_sas", return_value="sas-token")
def test_generate_run_data_sas_url_allows_admin_writes_for_hot_storage(
    generate_blob_sas_mock,
    hot_client: BlobDataAzureClient,
):
    hot_client.generate_run_data_sas_url(
        dir_path="dir_path",
        file_name="hello.txt",
        is_admin=True,
    )
    permission = generate_blob_sas_mock.call_args.kwargs["permission"]
    assert permission.read is True
    assert permission.create is True
    assert permission.write is True
    assert permission.delete is True
    assert permission.add is True


@patch("clients.azure.blob_data.generate_blob_sas", return_value="sas-token")
def test_generate_run_data_sas_url_disables_admin_writes_for_cool_storage(
    generate_blob_sas_mock,
    cool_client: BlobDataAzureClient,
):
    cool_client.generate_run_data_sas_url(
        dir_path="dir_path",
        file_name="hello.txt",
        is_admin=True,
    )
    permission = generate_blob_sas_mock.call_args.kwargs["permission"]
    assert permission.read is True
    assert permission.create is False
    assert permission.write is False
    assert permission.delete is False
    assert permission.add is False


@patch("clients.azure.blob_data.generate_blob_sas", return_value="sas-token")
def test_generate_project_documents_upload_sas_url_raises_for_cool_storage(
    generate_blob_sas_mock,
    cool_client: BlobDataAzureClient,
):
    with pytest.raises(PermissionError):
        cool_client.generate_project_documents_upload_sas_url(
            project_name="project",
            file_name="hello.txt",
        )
    generate_blob_sas_mock.assert_not_called()


@patch("clients.azure.blob_data.generate_blob_sas", return_value="sas-token")
def test_generate_project_documents_sas_url_disables_write_for_cool_storage(
    generate_blob_sas_mock,
    cool_client: BlobDataAzureClient,
):
    cool_client.generate_project_documents_sas_url(
        dir_path="dir_path",
        file_name="hello.txt",
    )
    permission = generate_blob_sas_mock.call_args.kwargs["permission"]
    assert permission.read is True
    assert permission.create is False
    assert permission.write is False
    assert permission.delete is False
    assert permission.add is False


@patch("clients.azure.blob_data.generate_container_sas", return_value="container-token")
def test_generate_project_directory_token_raises_for_cool_storage_with_write_permissions(
    generate_container_sas_mock,
    cool_client: BlobDataAzureClient,
):
    with pytest.raises(PermissionError):
        cool_client.generate_project_directory_token(
            project_name="project-name",
            permission={
                "read": True,
                "list": True,
                "write": True,
                "delete": True,
                "add": True,
                "create": True,
            },
        )
    generate_container_sas_mock.assert_not_called()


@patch("clients.azure.blob_data.generate_container_sas", return_value="container-token")
def test_generate_project_directory_token_allows_read_permissions_for_cool_storage(
    generate_container_sas_mock,
    cool_client: BlobDataAzureClient,
):
    token = cool_client.generate_project_directory_token(
        project_name="project-name",
        permission={
            "read": True,
            "list": True,
        },
    )
    assert token == "container-token"
    permission = generate_container_sas_mock.call_args.kwargs["permission"]
    assert permission.read is True
    assert permission.list is True
    assert permission.write is False
    assert permission.delete is False
    assert permission.add is False
    assert permission.create is False


@patch("clients.azure.blob_data.generate_container_sas", return_value="container-token")
def test_generate_project_directory_token_force_write_allows_writes_for_cool_storage(
    generate_container_sas_mock,
    cool_client: BlobDataAzureClient,
):
    token = cool_client.generate_project_directory_token(
        project_name="project-name",
        permission={
            "read": True,
            "list": True,
            "write": True,
            "delete": True,
            "add": True,
            "create": True,
        },
        force_write=True,
    )
    assert token == "container-token"
    permission = generate_container_sas_mock.call_args.kwargs["permission"]
    assert permission.read is True
    assert permission.list is True
    assert permission.write is True
    assert permission.delete is True
    assert permission.add is True
    assert permission.create is True
