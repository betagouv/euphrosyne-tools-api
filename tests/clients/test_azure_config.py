# pylint: disable=protected-access, no-member, redefined-outer-name

import json
from unittest.mock import MagicMock, patch

import pytest
from azure.storage.blob import BlobClient, ContainerClient
from pytest import MonkeyPatch

from clients import VMSizes
from clients.azure.config import ConfigAzureClient


@pytest.fixture
def client(monkeypatch: MonkeyPatch):
    monkeypatch.setenv("AZURE_RESOURCE_GROUP_NAME", "resource_group_name")
    monkeypatch.setenv("AZURE_SUBSCRIPTION_ID", "ID")
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "storageaccount")
    with patch("clients.azure._storage.StorageManagementClient"):
        with patch("clients.azure.config.BlobServiceClient"):
            return ConfigAzureClient()


def test_get_or_create_blob_when_blob_exists(client: ConfigAzureClient):
    blob_client_mock = MagicMock(spec=BlobClient)
    container_mock = MagicMock(
        spec=ContainerClient, **{"get_blob_client.return_value": blob_client_mock}
    )
    client._get_or_create_blob(container_mock, "blob", "init")

    container_mock.get_blob_client.assert_called_once_with("blob")
    blob_client_mock.upload_blob.assert_not_called()


def test_get_or_create_blob_when_blob_do_not_exist(client: ConfigAzureClient):
    blob_client_mock = MagicMock(spec=BlobClient, **{"exists.return_value": False})
    container_mock = MagicMock(
        spec=ContainerClient, **{"get_blob_client.return_value": blob_client_mock}
    )
    client._get_or_create_blob(container_mock, "blob", "init")

    container_mock.get_blob_client.assert_called_once_with("blob")
    blob_client_mock.upload_blob.assert_called_once_with("init")


# VM SIZES


@patch("clients.azure.config.ConfigAzureClient._get_or_create_project_vm_sizes_blob")
def test_get_project_vm_size_when_empty_config(
    method_mock: MagicMock, client: ConfigAzureClient
):
    method_mock.return_value.download_blob.return_value.readall.return_value = "{}"
    vm_size = client.get_project_vm_size("project-hello")
    assert vm_size is None


@patch("clients.azure.config.ConfigAzureClient._get_or_create_project_vm_sizes_blob")
def test_get_project_vm_size_when_project_in_imagery(
    method_mock: MagicMock, client: ConfigAzureClient
):
    method_mock.return_value.download_blob.return_value.readall.return_value = (
        json.dumps({"IMAGERY": ["project-hello", "project-ferrandi"]})
    )
    vm_size = client.get_project_vm_size("project-hello")
    assert vm_size == VMSizes.IMAGERY


@patch("clients.azure.config.ConfigAzureClient._get_or_create_project_vm_sizes_blob")
def test_get_project_vm_size_when_project_nowhere(
    method_mock: MagicMock, client: ConfigAzureClient
):
    method_mock.return_value.download_blob.return_value.readall.return_value = (
        json.dumps({"IMAGERY": ["project-ferrandi"]})
    )
    vm_size = client.get_project_vm_size("project-hello")
    assert vm_size is None


@patch("clients.azure.config.ConfigAzureClient._get_or_create_project_vm_sizes_blob")
def test_set_project_vm_size(method_mock: MagicMock, client: ConfigAzureClient):
    method_mock.return_value.download_blob.return_value.readall.return_value = (
        json.dumps({"IMAGERY": ["project-ferrandi"]})
    )
    client.set_project_vm_size("project-hello", VMSizes.IMAGERY)

    method_mock.return_value.upload_blob.assert_called_with(
        '{"IMAGERY": ["project-ferrandi", "project-hello"]}', overwrite=True
    )


@patch("clients.azure.config.ConfigAzureClient._get_or_create_project_vm_sizes_blob")
def test_set_project_vm_size_with_none_removes_project(
    method_mock: MagicMock, client: ConfigAzureClient
):
    method_mock.return_value.download_blob.return_value.readall.return_value = (
        json.dumps({"IMAGERY": ["project-ferrandi", "project-hello"]})
    )
    client.set_project_vm_size("project-hello", None)

    method_mock.return_value.upload_blob.assert_called_with(
        '{"IMAGERY": ["project-ferrandi"]}', overwrite=True
    )


@patch("clients.azure.config.ConfigAzureClient._get_or_create_project_vm_sizes_blob")
def test_set_project_vm_size_when_empty_config(
    method_mock: MagicMock, client: ConfigAzureClient
):
    method_mock.return_value.download_blob.return_value.readall.return_value = "{}"
    client.set_project_vm_size("project-hello", VMSizes.IMAGERY)

    method_mock.return_value.upload_blob.assert_called_with(
        '{"IMAGERY": ["project-hello"]}', overwrite=True
    )


# END VM SIZE

# IMAGE DEFINITION


@patch(
    "clients.azure.config.ConfigAzureClient._get_or_create_project_image_definitions_blob"
)
def test_get_project_image_definition_when_empty_config(
    method_mock: MagicMock, client: ConfigAzureClient
):
    method_mock.return_value.download_blob.return_value.readall.return_value = "{}"
    image_definition = client.get_project_image_definition("project-hello")
    assert image_definition is None


@patch(
    "clients.azure.config.ConfigAzureClient._get_or_create_project_image_definitions_blob"
)
def test_get_project_image_definition_when_project_in_imagery(
    method_mock: MagicMock, client: ConfigAzureClient
):
    method_mock.return_value.download_blob.return_value.readall.return_value = (
        json.dumps({"animage": ["project-hello", "project-ferrandi"]})
    )
    image_definition = client.get_project_image_definition("project-hello")
    assert image_definition == "animage"


@patch(
    "clients.azure.config.ConfigAzureClient._get_or_create_project_image_definitions_blob"
)
def test_get_project_image_definition_when_project_nowhere(
    method_mock: MagicMock, client: ConfigAzureClient
):
    method_mock.return_value.download_blob.return_value.readall.return_value = (
        json.dumps({"animage": ["project-ferrandi"]})
    )
    image_definition = client.get_project_image_definition("project-hello")
    assert image_definition is None


@patch(
    "clients.azure.config.ConfigAzureClient._get_or_create_project_image_definitions_blob"
)
def test_set_project_image_definition(
    method_mock: MagicMock, client: ConfigAzureClient
):
    method_mock.return_value.download_blob.return_value.readall.return_value = (
        json.dumps({"animage": ["project-ferrandi"]})
    )
    client.set_project_image_definition("project-hello", "animage")

    method_mock.return_value.upload_blob.assert_called_with(
        '{"animage": ["project-ferrandi", "project-hello"]}', overwrite=True
    )


@patch(
    "clients.azure.config.ConfigAzureClient._get_or_create_project_image_definitions_blob"
)
def test_set_project_image_definition_with_none_removes_project(
    method_mock: MagicMock, client: ConfigAzureClient
):
    method_mock.return_value.download_blob.return_value.readall.return_value = (
        json.dumps({"animage": ["project-ferrandi", "project-hello"]})
    )
    client.set_project_image_definition("project-hello", None)

    method_mock.return_value.upload_blob.assert_called_with(
        '{"animage": ["project-ferrandi"]}', overwrite=True
    )


@patch(
    "clients.azure.config.ConfigAzureClient._get_or_create_project_image_definitions_blob"
)
def test_set_project_image_definition_when_empty_config(
    method_mock: MagicMock, client: ConfigAzureClient
):
    method_mock.return_value.download_blob.return_value.readall.return_value = "{}"
    client.set_project_image_definition("project-hello", "animage")

    method_mock.return_value.upload_blob.assert_called_with(
        '{"animage": ["project-hello"]}', overwrite=True
    )
