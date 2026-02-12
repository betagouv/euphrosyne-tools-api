# pylint: disable=protected-access, no-member, redefined-outer-name
import asyncio
import pathlib
from datetime import datetime
from unittest.mock import MagicMock, call, patch

import pytest
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.fileshare import ShareDirectoryClient
from pytest import MonkeyPatch

from auth import Project, User
from clients.azure import DataAzureClient
from clients.azure.data import (
    FolderCreationError,
    IncorrectDataFilePath,
    extract_info_from_path,
    validate_project_document_file_path,
    validate_run_data_file_path,
)
from clients.data_models import ProjectFile
from data_lifecycle.storage_types import StorageRole

from ..mocks.azure import factories as azure_factories


@pytest.fixture
def client(monkeypatch: MonkeyPatch):
    monkeypatch.setenv("AZURE_RESOURCE_GROUP_NAME", "resource_group_name")
    monkeypatch.setenv("AZURE_SUBSCRIPTION_ID", "ID")
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "storageaccount")
    with patch("clients.azure._storage.StorageManagementClient"):
        with patch("clients.azure.data.FileSharedAccessSignature"):
            return DataAzureClient()


@pytest.fixture(autouse=True)
def setenv(monkeypatch: MonkeyPatch):
    monkeypatch.setenv("AZURE_STORAGE_FILESHARE", "fileshare")


def _mock_base_storage_init(self):
    self.storage_account_name = "storageaccount"
    self._storage_key = "storage-key"


def test_init_uses_hot_fileshare(monkeypatch: MonkeyPatch):
    monkeypatch.setenv("AZURE_STORAGE_FILESHARE", "hot-fileshare")
    monkeypatch.setenv("AZURE_STORAGE_FILESHARE_COOL", "cool-fileshare")
    with patch(
        "clients.azure.data.BaseStorageAzureClient.__init__", autospec=True
    ) as base_init_mock, patch("clients.azure.data.FileSharedAccessSignature"):
        base_init_mock.side_effect = _mock_base_storage_init
        client = DataAzureClient()
    assert client.share_name == "hot-fileshare"


def test_init_uses_cool_fileshare(monkeypatch: MonkeyPatch):
    monkeypatch.setenv("AZURE_STORAGE_FILESHARE", "hot-fileshare")
    monkeypatch.setenv("AZURE_STORAGE_FILESHARE_COOL", "cool-fileshare")
    with patch(
        "clients.azure.data.BaseStorageAzureClient.__init__", autospec=True
    ) as base_init_mock, patch("clients.azure.data.FileSharedAccessSignature"):
        base_init_mock.side_effect = _mock_base_storage_init
        client = DataAzureClient(storage_role=StorageRole.COOL)
    assert client.share_name == "cool-fileshare"


def test_init_raises_for_unsupported_storage_role(monkeypatch: MonkeyPatch):
    monkeypatch.setenv("AZURE_STORAGE_FILESHARE", "hot-fileshare")
    with patch(
        "clients.azure.data.BaseStorageAzureClient.__init__", autospec=True
    ) as base_init_mock, patch("clients.azure.data.FileSharedAccessSignature"):
        base_init_mock.side_effect = _mock_base_storage_init
        with pytest.raises(ValueError, match="Unsupported storage role"):
            DataAzureClient(storage_role="WARM")  # type: ignore[arg-type]


def test_get_project_documents_with_prefix(
    client: DataAzureClient, monkeypatch: MonkeyPatch
):
    monkeypatch.setenv("DATA_PROJECTS_LOCATION_PREFIX", "/prefix")
    _list_files_recursive_mock = MagicMock(
        return_value=(
            p
            for p in [
                ProjectFile(
                    name="file-1.txt",
                    last_modified=datetime(2022, 6, 22, 11, 22, 33),
                    size=222,
                    path="/prefix/project/documents/file-1.txt",
                )
            ]
        )
    )
    with patch.object(client, "_list_files", _list_files_recursive_mock):
        files = list(client.get_project_documents("project"))
        assert _list_files_recursive_mock.call_args[0] == ("/prefix/project/documents",)
        assert len(files) == 1
        assert isinstance(files[0], ProjectFile)
        assert files[0].path == "/prefix/project/documents/file-1.txt"


def test_generate_project_documents_sas_url(
    client: DataAzureClient,
    monkeypatch: MonkeyPatch,
):
    with patch.object(
        client, "_file_shared_access_signature"
    ) as file_shared_access_signature_mock:
        file_shared_access_signature_mock.generate_file.return_value = "params=params"
        monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "storage")

        url = client.generate_project_documents_sas_url(
            dir_path="dir_path",
            file_name="hello.txt",
        )

    # pylint: disable=line-too-long
    assert (
        url
        == "https://storageaccount.file.core.windows.net/fileshare/dir_path/hello.txt?params=params"
    )
    mock_kwargs = file_shared_access_signature_mock.generate_file.call_args.kwargs
    assert mock_kwargs["share_name"] == "fileshare"
    assert mock_kwargs["directory_name"] == "dir_path"
    assert mock_kwargs["file_name"] == "hello.txt"
    assert mock_kwargs["permission"].read is True
    assert mock_kwargs["permission"].delete is True
    assert mock_kwargs["permission"].create is False
    assert mock_kwargs["permission"].write is False


@patch("clients.azure.data._get_projects_path")
def test_generate_project_documents_upload_sas_url(
    _get_projects_path_mock: MagicMock,
    client: DataAzureClient,
    monkeypatch: MonkeyPatch,
):
    with patch.object(
        client, "_file_shared_access_signature"
    ) as file_shared_access_signature_mock:
        file_shared_access_signature_mock.generate_file.return_value = "params=params"
        monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "storage")
        _get_projects_path_mock.return_value = "projects"

        url = client.generate_project_documents_upload_sas_url(
            project_name="project",
            file_name="hello.txt",
        )

    # pylint: disable=line-too-long
    assert (
        url
        == "https://storageaccount.file.core.windows.net/fileshare/projects/project/documents/hello.txt?params=params"
    )
    mock_kwargs = file_shared_access_signature_mock.generate_file.call_args.kwargs
    assert mock_kwargs["share_name"] == "fileshare"
    assert mock_kwargs["directory_name"] == "projects/project/documents"
    assert mock_kwargs["file_name"] == "hello.txt"
    assert mock_kwargs["permission"].read is False
    assert mock_kwargs["permission"].delete is False
    assert mock_kwargs["permission"].create is True
    assert mock_kwargs["permission"].write is True


@pytest.mark.parametrize(
    ("is_admin"),
    (True, False),
)
def test_generate_run_data_sas_url(
    client: DataAzureClient,
    monkeypatch: MonkeyPatch,
    is_admin: bool,
):
    with patch.object(
        client, "_file_shared_access_signature"
    ) as file_shared_access_signature_mock:
        file_shared_access_signature_mock.generate_file.return_value = "params=params"
        monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "storage")

        url = client.generate_run_data_sas_url(
            dir_path="dir_path",
            file_name="hello.txt",
            is_admin=is_admin,
        )

    assert (
        url
        == "https://storageaccount.file.core.windows.net/fileshare/dir_path/hello.txt?params=params"
    )
    mock_kwargs = file_shared_access_signature_mock.generate_file.call_args.kwargs
    assert mock_kwargs["share_name"] == "fileshare"
    assert mock_kwargs["directory_name"] == "dir_path"
    assert mock_kwargs["file_name"] == "hello.txt"
    assert mock_kwargs["permission"].read is True
    assert mock_kwargs["permission"].create == is_admin
    assert mock_kwargs["permission"].delete == is_admin
    assert mock_kwargs["permission"].write == is_admin


@patch("clients.azure.data.ShareDirectoryClient")
@patch("clients.azure.data.ShareFileClient")
def test_list_files_recursive_without_detailed_info(
    share_file_client: MagicMock,
    share_directory_client: MagicMock,
    client: DataAzureClient,
):
    files_and_folders__root = [
        {"name": "file-1.txt", "is_directory": False, "size": 123},
        {"name": "directory-1", "is_directory": True},
    ]
    files_and_folders__dir_1 = [
        {"name": "file-2.txt", "is_directory": False, "size": 124},
    ]
    share_directory_client.from_connection_string.return_value = share_directory_client
    share_file_client.from_connection_string.return_value = share_file_client

    share_directory_client.list_directories_and_files.side_effect = [
        files_and_folders__root,
        files_and_folders__dir_1,
    ]

    gen = client._list_files_recursive_async(dir_path="/")
    files_list = []
    while True:
        try:
            files_list.append(asyncio.run(gen.__anext__()))
        except StopAsyncIteration:
            break

    assert len(files_list) == 2
    assert all(isinstance(file, ProjectFile) for file in files_list)
    assert len(share_directory_client.list_directories_and_files.call_args) == 2
    assert (
        share_directory_client.from_connection_string.call_args_list[0][1][
            "directory_path"
        ]
        == "/"
    )
    assert (
        share_directory_client.from_connection_string.call_args_list[1][1][
            "directory_path"
        ]
        == "/directory-1"
    )
    assert all(file.last_modified is None for file in files_list)


@patch("clients.azure.data.ShareDirectoryClient")
@patch("clients.azure.data.ShareFileClient")
def test_list_files_recursive_with_detailed_info(
    share_file_client: MagicMock,
    share_directory_client: MagicMock,
    client: DataAzureClient,
):
    files_and_folders__root = [
        {"name": "file-1.txt", "is_directory": False, "size": 123},
        {"name": "directory-1", "is_directory": True},
    ]
    files_and_folders__dir_1 = [
        {"name": "file-2.txt", "is_directory": False, "size": 124},
    ]
    share_directory_client.from_connection_string.return_value = share_directory_client
    share_file_client.from_connection_string.return_value = share_file_client

    share_directory_client.list_directories_and_files.side_effect = [
        files_and_folders__root,
        files_and_folders__dir_1,
    ]
    share_file_client.get_file_properties.side_effect = [
        azure_factories.file_properties_factory(
            name="file-1.txt",
            last_modified=datetime(2022, 6, 22, 11, 22, 33),
            size=123,
            path="/file-1.txt",
        ),
        azure_factories.file_properties_factory(
            name="file-3.txt",
            last_modified=datetime(2022, 6, 22, 11, 22, 33),
            size=123,
            path="directory-1/file-2.txt",
        ),
    ]

    files = client._list_files_recursive_async(
        dir_path="/", fetch_detailed_information=True
    )
    files_list = []
    while True:
        try:
            files_list.append(asyncio.run(files.__anext__()))
        except StopAsyncIteration:
            break

    assert len(files_list) == 2
    assert all(isinstance(file, ProjectFile) for file in files_list)
    assert len(share_directory_client.list_directories_and_files.call_args) == 2
    assert (
        share_directory_client.from_connection_string.call_args_list[0][1][
            "directory_path"
        ]
        == "/"
    )
    assert (
        share_directory_client.from_connection_string.call_args_list[1][1][
            "directory_path"
        ]
        == "/directory-1"
    )


@patch("clients.azure.data._get_projects_path", MagicMock(return_value="projects"))
@pytest.mark.parametrize(
    ("path,is_valid"),
    (
        ("projects/hello/runs/world/raw_data/", True),
        ("projects/hello/runs/world/processed_data/", True),
        ("projects/hello/runs/world/processed_data/and/the/path", True),
        ("projects/otherproject/runs/world/processed_data/", False),
        ("projects/hello/notruns/world/processed_data/", False),
        ("projects/hel|lo/runs/world/processed_data/", False),
        ("projects/hello/runs/wor|ld/processed_data/", False),
        ("start/differently/hello/runs/world/processed_data/", False),
        ("projects/hello/runs/world/other_data/", False),
    ),
)
def test_validate_run_data_file_path(path, is_valid):
    is_invalid = False
    try:
        validate_run_data_file_path(
            path,
            User(
                id="1",
                is_admin=False,
                projects=[Project(id=2, name="hello", slug="hello")],
            ),
        )
    except IncorrectDataFilePath:
        is_invalid = True
    assert is_valid is not is_invalid


@patch("clients.azure.data._get_projects_path", MagicMock(return_value="projects"))
@pytest.mark.parametrize(
    ("path,is_valid"),
    (
        ("projects/hello/documents/", True),
        ("projects/hello/world/documents/and/the/path", False),
        ("projects/otherproject/documents/", False),
        ("projects/hel|lo/documents/", False),
        ("start/differently/hello/documents/", False),
    ),
)
def test_validate_document_file_path(path, is_valid):
    is_invalid = False
    try:
        validate_project_document_file_path(
            path,
            User(
                id="1",
                is_admin=False,
                projects=[Project(id=2, name="hello", slug="hello")],
            ),
        )
    except IncorrectDataFilePath:
        is_invalid = True
    assert is_valid is not is_invalid


def test_init_project_directory(client: DataAzureClient, monkeypatch: MonkeyPatch):
    monkeypatch.setenv("DATA_PROJECTS_LOCATION_PREFIX", "projects")
    share_directory_client_mock = MagicMock(spec=ShareDirectoryClient)
    with patch(
        "clients.azure.data.ShareDirectoryClient",
        new=MagicMock(
            **{"from_connection_string.return_value": share_directory_client_mock}
        ),
    ) as mock:
        client.init_project_directory("My Project")
        # Test project name to slug conversion
        assert (
            mock.from_connection_string.call_args.kwargs["directory_path"]
            == "projects/my-project"
        )
    share_directory_client_mock.create_directory.assert_called_once()
    share_directory_client_mock.create_subdirectory.assert_has_calls(
        [call("documents"), call("runs")]
    )


@pytest.mark.parametrize("error_type", (ResourceNotFoundError, ResourceExistsError))
def test_init_project_directory_raise_error(
    error_type: Exception,
    client: DataAzureClient,
):
    share_directory_client_mock = MagicMock(
        spec=ShareDirectoryClient, **{"create_directory.side_effect": error_type}
    )
    has_errored = False
    with patch(
        "clients.azure.data.ShareDirectoryClient",
        new=MagicMock(
            **{"from_connection_string.return_value": share_directory_client_mock}
        ),
    ):
        try:
            client.init_project_directory("myproject")
        except FolderCreationError:
            has_errored = True
    assert has_errored


def test_init_run_directory(client: DataAzureClient, monkeypatch: MonkeyPatch):
    monkeypatch.setenv("DATA_PROJECTS_LOCATION_PREFIX", "projects")
    share_directory_client_mock = MagicMock(spec=ShareDirectoryClient)
    with patch(
        "clients.azure.data.ShareDirectoryClient",
        new=MagicMock(
            **{"from_connection_string.return_value": share_directory_client_mock}
        ),
    ) as mock:
        client.init_run_directory("myrun", "My Project")
        # Test project name to slug conversion
        assert (
            mock.from_connection_string.call_args.kwargs["directory_path"]
            == "projects/my-project/runs/myrun"
        )
    share_directory_client_mock.create_directory.assert_called_once()
    share_directory_client_mock.create_subdirectory.assert_has_calls(
        [call("raw_data"), call("processed_data")], any_order=True
    )


@pytest.mark.parametrize("error_type", (ResourceNotFoundError, ResourceExistsError))
def test_init_run_directory_raise_error(
    error_type: Exception,
    client: DataAzureClient,
):
    share_directory_client_mock = MagicMock(
        spec=ShareDirectoryClient, **{"create_directory.side_effect": error_type}
    )
    has_errored = False
    with patch(
        "clients.azure.data.ShareDirectoryClient",
        new=MagicMock(
            **{"from_connection_string.return_value": share_directory_client_mock}
        ),
    ):
        try:
            client.init_run_directory("myproject", "myrun")
        except FolderCreationError:
            has_errored = True
    assert has_errored


def test_change_run_name(client: DataAzureClient, monkeypatch: MonkeyPatch):
    monkeypatch.setenv("DATA_PROJECTS_LOCATION_PREFIX", "projects")
    with patch.object(client, "_rename_directory") as mock:
        client.rename_run_directory("myrun", "My Project", "myrun2")
        mock.assert_called_once_with(
            directory_path="projects/my-project/runs/myrun",
            new_directory_path="projects/my-project/runs/myrun2",
        )


def test_change_project_name(client: DataAzureClient, monkeypatch: MonkeyPatch):
    monkeypatch.setenv("DATA_PROJECTS_LOCATION_PREFIX", "projects")
    with patch.object(client, "_rename_directory") as mock:
        client.rename_project_directory("Old project", "New project")
        mock.assert_called_once_with(
            directory_path="projects/old-project",
            new_directory_path="projects/new-project",
        )


def test_rename_directory(client: DataAzureClient, monkeypatch: MonkeyPatch):
    share_directory_client_mock = MagicMock(spec=ShareDirectoryClient)
    with patch(
        "clients.azure.data.ShareDirectoryClient",
        new=MagicMock(
            **{"from_connection_string.return_value": share_directory_client_mock}
        ),
    ) as mock:
        client._rename_directory("oldpath", "newpath")
        assert (
            mock.from_connection_string.call_args.kwargs["directory_path"] == "oldpath"
        )
    share_directory_client_mock.rename_directory.assert_called_once_with(
        new_name="newpath", overwrite=False
    )


@pytest.mark.parametrize("error_type", (ResourceNotFoundError, ResourceExistsError))
def test_change_run_name_raise_error(
    error_type: Exception,
    client: DataAzureClient,
):
    share_directory_client_mock = MagicMock(
        spec=ShareDirectoryClient, **{"rename_directory.side_effect": error_type}
    )
    has_errored = False
    with patch(
        "clients.azure.data.ShareDirectoryClient",
        new=MagicMock(
            **{"from_connection_string.return_value": share_directory_client_mock}
        ),
    ):
        try:
            client.rename_run_directory("myproject", "myrun", "myrun2")
        except FolderCreationError:
            has_errored = True
    assert has_errored


def test_is_project_data_available_returns_true(
    client: DataAzureClient,
):
    with patch.object(
        ShareDirectoryClient,
        "list_directories_and_files",
        return_value=[
            {"name": "run1", "is_directory": True},
            {"name": "run2", "is_directory": True},
        ],
    ), patch.object(
        ShareDirectoryClient,
        "get_subdirectory_client",
        return_value=MagicMock(list_directories_and_files=lambda: [{"name": "file1"}]),
    ):
        result = client.is_project_data_available("test_project")
        assert result


@patch("clients.azure.data.DataAzureClient.init_project_directory")
def test_is_project_data_available_when_project_dir_not_exist(
    init_project_directory_mock: MagicMock,
    client: DataAzureClient,
):
    with patch.object(
        ShareDirectoryClient,
        "list_directories_and_files",
        side_effect=ResourceNotFoundError,
    ):
        result = client.is_project_data_available("test_project")
        init_project_directory_mock.assert_called_once_with("test_project")
        assert not result


def test_is_project_data_available_when_run_dir_empty(
    client: DataAzureClient,
):
    with patch.object(
        ShareDirectoryClient,
        "list_directories_and_files",
        return_value=[{"name": "run1", "is_directory": True}],
    ), patch.object(
        ShareDirectoryClient,
        "get_subdirectory_client",
        return_value=MagicMock(list_directories_and_files=lambda: []),
    ):
        result = client.is_project_data_available("test_project")
        assert not result


@patch("clients.azure.data.DataAzureClient.init_run_directory")
def test_is_project_data_available_when_run_dir_not_exists(
    init_run_directory_mock: MagicMock,
    client: DataAzureClient,
):
    with patch.object(
        ShareDirectoryClient,
        "list_directories_and_files",
        return_value=[{"name": "run1", "is_directory": True}],
    ), patch.object(
        ShareDirectoryClient,
        "get_subdirectory_client",
        return_value=MagicMock(
            list_directories_and_files=MagicMock(side_effect=ResourceNotFoundError)
        ),
    ):
        result = client.is_project_data_available("test_project")
        assert not result
        init_run_directory_mock.assert_called_once_with("run1", "test_project")


@patch("clients.azure.data._validate_run_data_file_path_regex", MagicMock())
def test_extract_info_from_path(monkeypatch: MonkeyPatch):
    monkeypatch.setenv("DATA_PROJECTS_LOCATION_PREFIX", "projects")

    path1 = pathlib.Path("projects/project1/runs/run1/data")
    path2 = pathlib.Path("projects/project2/runs/run2")
    path3 = pathlib.Path("projects/project3")

    info1 = extract_info_from_path(path1)
    info2 = extract_info_from_path(path2)
    info3 = extract_info_from_path(path3)

    assert info1["project_name"] == "project1"
    assert info1["run_name"] == "run1"
    assert info1["data_type"] == "data"
    assert info2["project_name"] == "project2"
    assert info2["run_name"] == "run2"
    assert info2["data_type"] is None
    assert info3["project_name"] == "project3"
    assert info3["run_name"] is None
    assert info3["data_type"] is None


@patch("clients.azure.data.ShareDirectoryClient")
@patch("clients.azure.data.ShareFileClient")
def test__iter_directory_files_directory(
    mock_file_client: MagicMock,
    mock_dir_client: MagicMock,
    client: DataAzureClient,
):
    async def gen_async():
        yield ProjectFile(name="file-1", path="/1", size=123)

    with patch.object(
        client,
        "_list_files_recursive_async",
        MagicMock(return_value=gen_async()),
    ):
        mock_dir_client.from_connection_string.return_value.exists.return_value = True

        gen = client._iter_directory_files_async("project1/run1")
        while True:
            try:
                asyncio.run(gen.__anext__())
            except StopAsyncIteration:
                break

        assert len(mock_file_client.from_connection_string.call_args_list) == 1
        assert (
            mock_file_client.from_connection_string.call_args_list[0][1]["file_path"]
            == "/1"
        )
