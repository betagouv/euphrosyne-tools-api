# pylint: disable=protected-access, no-member, redefined-outer-name

from datetime import datetime
from unittest.mock import DEFAULT, MagicMock, patch

import pytest
from azure.core.exceptions import ResourceNotFoundError
from pytest import MonkeyPatch

from azure_client import (
    AzureClient,
    AzureVMDeploymentProperties,
    DeploymentNotFound,
    ProjectFile,
    VMNotFound,
    _project_name_to_vm_name,
    wait_for_deployment_completeness,
)


@pytest.fixture
def client(monkeypatch: MonkeyPatch):
    monkeypatch.setenv("AZURE_RESOURCE_GROUP_NAME", "resource_group_name")
    monkeypatch.setenv("AZURE_TEMPLATE_SPECS_NAME", "template_specs")
    monkeypatch.setenv("AZURE_SUBSCRIPTION_ID", "ID")
    monkeypatch.setenv("AZURE_RESOURCE_PREFIX", "test-")
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "storageaccount")
    with patch.multiple(
        "azure_client",
        ResourceManagementClient=DEFAULT,
        ComputeManagementClient=DEFAULT,
        TemplateSpecsClient=DEFAULT,
        StorageManagementClient=DEFAULT,
        FileSharedAccessSignature=DEFAULT,
    ):
        return AzureClient()


@patch("azure_client.AzureClient._get_latest_template_specs", dict)
def test_deploy_exits_when_vm_exists(client: AzureClient):
    client._resource_mgmt_client.deployments.check_existence.return_value = True
    client.deploy_vm("vm-test")

    client._resource_mgmt_client.deployments.begin_create_or_update.assert_not_called()


@patch("azure_client.AzureClient._get_latest_template_specs", dict)
@patch("azure_client._project_name_to_vm_name", lambda x: x)
def test_deploys_with_proper_parameters(client: AzureClient):
    client._resource_mgmt_client.deployments.check_existence.return_value = False
    result = client.deploy_vm("vm-test", vm_size="Standard_B8ms")

    call_args = (
        client._resource_mgmt_client.deployments.begin_create_or_update.call_args[1]
    )
    assert "parameters" in call_args
    assert "properties" in call_args["parameters"]
    assert "template" in call_args["parameters"]["properties"]
    assert "parameters" in call_args["parameters"]["properties"]
    assert (
        call_args["parameters"]["properties"]["parameters"]["adminUsername"]["value"]
        == "vm-test"
    )
    assert (
        call_args["parameters"]["properties"]["parameters"]["vmName"]["value"]
        == "vm-test"
    )
    assert (
        call_args["parameters"]["properties"]["parameters"]["vmSize"]["value"]
        == "Standard_B8ms"
    )
    assert isinstance(result, AzureVMDeploymentProperties)
    assert result.project_name == "vm-test"
    assert result.username == "vm-test"
    assert isinstance(result.password, str)
    assert (
        result.deployment_process
        is client._resource_mgmt_client.deployments.begin_create_or_update.return_value
    )


def test_get_latest_template_specs(client: AzureClient):
    client._template_specs_client.template_specs.get.return_value.versions = {
        "1.0.0": {},
        "1.1.1": {},
    }
    client._get_latest_template_specs()

    client._template_specs_client.template_spec_versions.get.assert_called_with(
        resource_group_name="resource_group_name",
        template_spec_name="template_specs",
        template_spec_version="1.1.1",
    )


def test_get_vm_raises_if_absent(client: AzureClient):
    client._compute_mgmt_client.virtual_machines.get.side_effect = (
        ResourceNotFoundError()
    )
    with pytest.raises(VMNotFound):
        client.get_vm("VM")


@patch("azure_client._project_name_to_vm_name", lambda x: x)
def test_get_vm_calls_azure_method_with_proper_args(client: AzureClient):
    client.get_vm("VM")
    client._compute_mgmt_client.virtual_machines.get.assert_called_with(
        resource_group_name="resource_group_name",
        vm_name="VM",
    )


def test_get_deployment_status_returns_status(client: AzureClient):
    deployment_get_return = MagicMock(
        properties=MagicMock(provisioning_state="Succeeded")
    )
    client._resource_mgmt_client.deployments.get.return_value = deployment_get_return
    status = client.get_deployment_status("VM")

    client._resource_mgmt_client.deployments.get.assert_called_with(
        resource_group_name="resource_group_name", deployment_name="vm"
    )
    assert status == "Succeeded"


def test_get_deployment_raises_if_deployment_absent(client: AzureClient):
    client._resource_mgmt_client.deployments.get.side_effect = ResourceNotFoundError()

    with pytest.raises(DeploymentNotFound):
        client.get_deployment_status("VM")


@pytest.mark.parametrize(
    ("status", "is_ok"),
    (("Succeeded", True), ("Running", True), ("Ready", True), ("Failed", False)),
)
def test_wait_for_deployment_completeness(status, is_ok):
    poller = MagicMock(result=MagicMock())
    poller.result.return_value = MagicMock(
        properties=MagicMock(provisioning_state=status)
    )
    deployment = wait_for_deployment_completeness(poller)
    if is_ok:
        assert deployment
    else:
        assert not deployment


def test_delete_vm(client: AzureClient):
    client._resource_mgmt_client.deployments.check_existence.return_value = False
    client.delete_vm("vm-test")

    client._compute_mgmt_client.virtual_machines.begin_delete.assert_called_with(
        resource_group_name="resource_group_name",
        vm_name="test-vm-test",
    )


def test_delete_vm_raises_if_vm_absent(client: AzureClient):
    client._compute_mgmt_client.virtual_machines.begin_delete.side_effect = (
        ResourceNotFoundError()
    )
    with pytest.raises(VMNotFound):
        client.delete_vm("vm-test")


def test_project_name_to_vm_name(monkeypatch: MonkeyPatch):
    monkeypatch.setenv("AZURE_RESOURCE_PREFIX", "test-")
    assert _project_name_to_vm_name("BLABLA") == "test-blabla"


def test_get_project_documents_with_prefix(
    client: AzureClient, monkeypatch: MonkeyPatch
):
    monkeypatch.setenv("AZURE_STORAGE_PROJECTS_LOCATION_PREFIX", "/prefix")
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
    with patch.object(client, "_list_files_recursive", _list_files_recursive_mock):
        files = client.get_project_documents("project")
        assert _list_files_recursive_mock.call_args[0] == ("/prefix/project/documents",)
        assert isinstance(files, list)
        assert len(list(files)) == 1


def test_get_run_files_with_prefix(client: AzureClient, monkeypatch: MonkeyPatch):
    monkeypatch.setenv("AZURE_STORAGE_PROJECTS_LOCATION_PREFIX", "/prefix")
    _list_files_recursive_mock = MagicMock(
        return_value=(
            p
            for p in [
                ProjectFile(
                    name="file-1.txt",
                    last_modified=datetime(2022, 6, 22, 11, 22, 33),
                    size=222,
                    path="/prefix/project/file-1.txt",
                )
            ]
        )
    )
    with patch.object(client, "_list_files_recursive", _list_files_recursive_mock):
        files = client.get_run_files("project", "run", "processed_data")
        assert _list_files_recursive_mock.call_args[0] == (
            "/prefix/project/runs/run/processed_data",
        )
        assert isinstance(files, list)
        assert len(list(files)) == 1


@patch("azure_client._get_projects_path")
def test_generate_project_documents_sas_url(
    _get_projects_path_mock: MagicMock,
    client: AzureClient,
    monkeypatch: MonkeyPatch,
):
    with patch.object(
        client, "_file_shared_access_signature"
    ) as file_shared_access_signature_mock:
        file_shared_access_signature_mock.generate_file.return_value = "params=params"
        monkeypatch.setenv("AZURE_STORAGE_FILESHARE", "fileshare")
        monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "storage")
        _get_projects_path_mock.return_value = "dir_path"

        url = client.generate_project_documents_sas_url(
            project_name="project",
            file_name="hello.txt",
        )

    # pylint: disable=line-too-long
    assert (
        url
        == "https://storageaccount.file.core.windows.net/fileshare/dir_path/project/documents/hello.txt?params=params"
    )
    mock_kwargs = file_shared_access_signature_mock.generate_file.call_args.kwargs
    assert mock_kwargs["share_name"] == "fileshare"
    assert mock_kwargs["directory_name"] == "dir_path/project/documents"
    assert mock_kwargs["file_name"] == "hello.txt"
    assert mock_kwargs["permission"].read is True
    assert mock_kwargs["permission"].create is True
    assert mock_kwargs["permission"].delete is True
    assert mock_kwargs["permission"].write is True


@pytest.mark.parametrize(
    ("is_admin"),
    (True, False),
)
@patch("azure_client._get_run_data_directory_name")
def test_generate_run_data_sas_url(
    get_run_data_directory_name_mock: MagicMock,
    client: AzureClient,
    monkeypatch: MonkeyPatch,
    is_admin: bool,
):
    with patch.object(
        client, "_file_shared_access_signature"
    ) as file_shared_access_signature_mock:
        file_shared_access_signature_mock.generate_file.return_value = "params=params"
        monkeypatch.setenv("AZURE_STORAGE_FILESHARE", "fileshare")
        monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "storage")
        get_run_data_directory_name_mock.return_value = "dir_path"

        url = client.generate_run_data_sas_url(
            project_name="project",
            run_name="run",
            data_type="processed_data",
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


@patch("azure_client.ShareDirectoryClient")
@patch("azure_client.ShareFileClient")
def test_list_files_recursive(
    share_file_client: MagicMock, share_directory_client: MagicMock, client: AzureClient
):
    files_and_folders__root = [
        {"name": "file-1.txt", "is_directory": False},
        {"name": "directory-1", "is_directory": True},
    ]
    files_and_folders__dir_1 = [
        {"name": "file-2.txt", "is_directory": False},
        {"name": "file-3.txt", "is_directory": False},
    ]
    share_directory_client.from_connection_string.return_value = share_directory_client
    share_file_client.from_connection_string.return_value = share_file_client

    share_directory_client.list_directories_and_files.side_effect = [
        files_and_folders__root,
        files_and_folders__dir_1,
    ]
    share_file_client.get_file_properties.side_effect = [
        {
            "name": "file-1.txt",
            "last_modified": datetime(2022, 6, 22, 11, 22, 33),
            "size": 123,
            "path": "/file-1.txt",
        },
        {
            "name": "file-2.txt",
            "last_modified": datetime(2022, 6, 22, 11, 22, 33),
            "size": 345,
            "path": "directory-1/file-2.txt",
        },
        {
            "name": "file-3.txt",
            "last_modified": datetime(2022, 6, 22, 11, 22, 33),
            "size": 123,
            "path": "directory-1/file-3.txt",
        },
    ]

    files = client._list_files_recursive(dir_path="/")
    files_list = list(files)

    assert len(files_list) == 3
    assert all(isinstance(file, ProjectFile) for file in files_list)
    assert len(share_directory_client.list_directories_and_files.call_args) == 2
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
