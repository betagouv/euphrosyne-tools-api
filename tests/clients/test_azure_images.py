# pylint: disable=protected-access, no-member, redefined-outer-name

import json
from unittest.mock import MagicMock, patch

import pytest
from azure.storage.blob import BlobClient, ContainerClient
from pytest import MonkeyPatch

from clients import VMSizes
from clients.azure.images import ImageStorageClient
from azure.core.exceptions import ResourceNotFoundError


@pytest.fixture
def client(monkeypatch: MonkeyPatch):
    monkeypatch.setenv("AZURE_RESOURCE_GROUP_NAME", "resource_group_name")
    monkeypatch.setenv("AZURE_SUBSCRIPTION_ID", "ID")
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "storageaccount")
    with patch("clients.azure._storage.StorageManagementClient"):
        with patch("clients.azure.config.BlobServiceClient"):
            return ImageStorageClient()


def test_list_project_object_images(client: ImageStorageClient):
    with patch.object(client, "_get_project_container") as get_project_container_mock:
        get_project_container_mock.return_value = MagicMock(
            url="url", list_blob_names=MagicMock(return_value=["name1", "name2"])
        )
        with patch.object(
            client, "generate_sas_token_for_project_container"
        ) as generate_sas_mock:
            generate_sas_mock.return_value = "token"

            names = client.list_project_object_images("project", 123)
            assert list(names) == ["url/name1", "url/name2"]

            names = client.list_project_object_images(
                "project", 123, with_sas_token=True
            )
            assert list(names) == ["url/name1?token", "url/name2?token"]


@patch("clients.azure.images.generate_blob_sas")
def test_generate_signed_upload_project_object_image_url(
    generate_blob_sas_mock: MagicMock, client: ImageStorageClient
):
    generate_blob_sas_mock.return_value = "token"
    with patch.object(client, "_get_project_container") as get_project_container_mock:
        get_project_container_mock.return_value = MagicMock(url="url")
        url = client.generate_signed_upload_project_object_image_url(
            "project", 123, "file"
        )
        assert url == "url/images/object-groups/123/file?token"
        permission = generate_blob_sas_mock.call_args_list[0][1]["permission"]
        assert permission.write
        assert not permission.list
        assert not permission.read
        assert not permission.delete


def test_get_object_image_blob_name(client: ImageStorageClient):
    assert client._get_object_image_blob_name(123) == "images/object-groups/123"
    assert (
        client._get_object_image_blob_name(123, "filename")
        == "images/object-groups/123/filename"
    )
