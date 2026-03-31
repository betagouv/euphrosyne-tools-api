from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest
from azure.core.exceptions import ResourceNotFoundError
from pytest import MonkeyPatch

from clients.azure.blob_data import BlobDataAzureClient
from clients.azure.data import FolderCreationError, RunDataNotFound
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


@pytest.mark.parametrize(
    ("dir_path", "blob_names"),
    [
        (
            "projects/my-project/runs/run1/raw_data",
            ["projects/my-project/runs/run1/raw_data/"],
        ),
        (
            "projects/my-project/runs/run1/raw_data",
            ["projects/my-project/runs/run1/raw_data/file.txt"],
        ),
        (
            r"\projects\my-project\runs\run1\raw_data\\",
            ["projects/my-project/runs/run1/raw_data/file.txt"],
        ),
    ],
)
def test_path_exists_returns_true_for_exact_directory_prefix(
    hot_client: BlobDataAzureClient,
    dir_path: str,
    blob_names: list[str],
):
    hot_client.container_client = MagicMock()
    hot_client.container_client.list_blob_names.return_value = iter(blob_names)

    assert hot_client._path_exists(dir_path) is True
    hot_client.container_client.list_blob_names.assert_called_once_with(
        name_starts_with="projects/my-project/runs/run1/raw_data/"
    )


def test_path_exists_returns_false_for_sibling_prefix(
    hot_client: BlobDataAzureClient,
):
    hot_client.container_client = MagicMock()
    hot_client.container_client.list_blob_names.return_value = iter([])

    assert hot_client._path_exists("projects/my-project/runs/run1/raw_data") is False
    hot_client.container_client.list_blob_names.assert_called_once_with(
        name_starts_with="projects/my-project/runs/run1/raw_data/"
    )


def test_path_exists_returns_false_when_container_is_missing(
    hot_client: BlobDataAzureClient,
):
    hot_client.container_client = MagicMock()
    hot_client.container_client.list_blob_names.side_effect = ResourceNotFoundError()

    assert hot_client._path_exists("projects/my-project/runs/run1/raw_data") is False
    hot_client.container_client.list_blob_names.assert_called_once_with(
        name_starts_with="projects/my-project/runs/run1/raw_data/"
    )


def test_get_run_files_folders_raises_when_only_sibling_prefix_exists(
    hot_client: BlobDataAzureClient,
    monkeypatch: MonkeyPatch,
):
    monkeypatch.setenv("DATA_PROJECTS_LOCATION_PREFIX", "projects")
    hot_client.container_client = MagicMock()
    hot_client.container_client.list_blob_names.return_value = iter([])

    with pytest.raises(RunDataNotFound):
        hot_client.get_run_files_folders("My Project", "run1", "raw_data")

    hot_client.container_client.list_blob_names.assert_called_once_with(
        name_starts_with="projects/my-project/runs/run1/raw_data/"
    )


def test_rename_directory_raises_when_only_source_sibling_prefix_exists(
    hot_client: BlobDataAzureClient,
):
    hot_client.container_client = MagicMock()
    hot_client.container_client.list_blob_names.side_effect = [
        iter([]),
    ]

    with pytest.raises(FolderCreationError) as error:
        hot_client._rename_directory(
            "projects/my-project/runs/run1",
            "projects/my-project/runs/run2",
        )

    assert error.value.message == "directory not found"
    assert hot_client.container_client.list_blob_names.call_args_list == [
        call(name_starts_with="projects/my-project/runs/run1/"),
    ]


def test_rename_directory_ignores_sibling_prefixed_destination(
    hot_client: BlobDataAzureClient,
):
    hot_client.container_client = MagicMock()
    hot_client.container_client.list_blob_names.side_effect = [
        iter(["projects/my-project/runs/run1/file.txt"]),
        iter([]),
    ]
    hot_client.container_client.list_blobs.return_value = [
        SimpleNamespace(name="projects/my-project/runs/run1/file.txt")
    ]

    with patch.object(hot_client, "_copy_blob") as copy_blob_mock:
        hot_client._rename_directory(
            "projects/my-project/runs/run1",
            "projects/my-project/runs/run10",
        )

    assert hot_client.container_client.list_blob_names.call_args_list == [
        call(name_starts_with="projects/my-project/runs/run1/"),
        call(name_starts_with="projects/my-project/runs/run10/"),
    ]
    hot_client.container_client.list_blobs.assert_called_once_with(
        name_starts_with="projects/my-project/runs/run1/"
    )
    copy_blob_mock.assert_called_once_with(
        "projects/my-project/runs/run1/file.txt",
        "projects/my-project/runs/run10/file.txt",
    )
