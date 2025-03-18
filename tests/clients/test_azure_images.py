# pylint: disable=protected-access, no-member, redefined-outer-name

import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob.aio import ContainerClient
from pytest import MonkeyPatch

from clients.azure.images import ImageStorageClient


@pytest.fixture
def client(monkeypatch: MonkeyPatch):
    monkeypatch.setenv("AZURE_RESOURCE_GROUP_NAME", "resource_group_name")
    monkeypatch.setenv("AZURE_SUBSCRIPTION_ID", "ID")
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "storageaccount")
    with patch("clients.azure._storage.StorageManagementClient"):
        with patch("clients.azure.images.BlobServiceClient"):
            return ImageStorageClient()


def test_get_object_image_blob_name(client: ImageStorageClient):
    assert client._get_image_blob_name(object_id=123) == "images/object-groups/123"
    assert (
        client._get_image_blob_name(object_id=123, file_name="filename")
        == "images/object-groups/123/filename"
    )
    assert client._get_image_blob_name(file_name="filename") == "images/filename"


def test_get_container_name_for_project(client):
    project_slug = "test-project"
    expected_container_name = f"project-{project_slug}"
    assert (
        client._get_container_name_for_project(project_slug) == expected_container_name
    )


def test_get_project_container(client):
    project_slug = "test-project"
    container_client_mock = MagicMock(spec=ContainerClient)

    client.blob_service_client.get_container_client.return_value = container_client_mock

    container_client = client._get_project_container(project_slug)
    assert container_client == container_client_mock
    client.blob_service_client.get_container_client.assert_called_once_with(
        f"project-{project_slug}"
    )


def test_generate_sas_token_for_container(client):
    container_name = "test-container"
    now = datetime.datetime.now(datetime.timezone.utc)
    with patch("clients.azure.images.datetime") as mock_datetime:
        mock_datetime.datetime.now.return_value = now
        with patch(
            "clients.azure.images.generate_container_sas"
        ) as mock_generate_container_sas:
            mock_generate_container_sas.return_value = "sas-token"
            sas_token = client._generate_sas_token_for_container(container_name)

            assert sas_token == "sas-token"
            mock_generate_container_sas.assert_called_once()
            assert mock_generate_container_sas.call_args_list[0][0] == (
                client.storage_account_name,
                container_name,
                client._storage_key,
            )
            assert (
                mock_generate_container_sas.call_args_list[0][1]["permission"].list
                is True
            )
            assert (
                mock_generate_container_sas.call_args_list[0][1]["permission"].read
                is True
            )
            assert (
                mock_generate_container_sas.call_args_list[0][1]["permission"].write
                is False
            )
            assert (
                mock_generate_container_sas.call_args_list[0][1]["permission"].delete
                is True
            )
            assert mock_generate_container_sas.call_args_list[0][1]["start"] == now
            assert "expiry" in mock_generate_container_sas.call_args_list[0][1]
            mock_datetime.timedelta.assert_called_once_with(minutes=60)


def test_get_project_container_sas_token(client):
    project_slug = "test-project"
    with patch.object(
        client, "_generate_sas_token_for_container", return_value="sas-token"
    ) as mock_generate_sas_token:
        sas_token = client.get_project_container_sas_token(project_slug)
        assert sas_token == "sas-token"
        mock_generate_sas_token.assert_called_once_with(f"project-{project_slug}")


def test_get_project_container_base_url(client):
    project_slug = "test-project"
    container_client_mock = MagicMock(spec=ContainerClient)
    container_client_mock.primary_hostname = "test-hostname"
    with patch.object(
        client, "_get_project_container", return_value=container_client_mock
    ):
        base_url = client.get_project_container_base_url(project_slug)
        assert base_url == "https://test-hostname"


@pytest.mark.asyncio
async def test_set_cors_policy(client):
    allowed_origins = "https://example.com,https://another.com"
    cors_rule = {
        "allowed_origins": allowed_origins.split(","),
        "allowed_methods": ["DELETE", "GET", "HEAD", "POST", "OPTIONS", "PUT"],
        "allowed_headers": [
            "content-type",
            "x-ms-blob-type",
            "x-ms-client-request-id",
            "x-ms-useragent",
            "x-ms-version",
        ],
    }
    with patch.object(
        client.blob_service_client, "set_service_properties", new_callable=AsyncMock
    ) as mock_set_service_properties:
        with patch("clients.azure.images.CorsRule") as mock_cors_rule:
            await client.set_cors_policy(allowed_origins)
            mock_set_service_properties.assert_called_once()
            mock_cors_rule.assert_called_once_with(**cors_rule)


@pytest.mark.asyncio
async def test_list_project_object_images_with_sas_token(client: ImageStorageClient):
    project_slug = "test-project"
    object_id = 123
    with_sas_token = True

    async def list_blob_names_mock(name_starts_with=None):
        yield "blob1"
        yield "blob2"

    container_mock = AsyncMock()
    container_mock.url = "https://test.blob.core.windows.net/test-container"
    container_mock.list_blob_names = list_blob_names_mock
    client._get_project_container = MagicMock(return_value=container_mock)
    client._generate_sas_token_for_container = MagicMock(return_value="sas-token")  # type: ignore[method-assign]

    result = []
    async for url in client.list_project_images(
        project_slug, object_id, with_sas_token
    ):
        result.append(url)

    assert len(result) == 2
    assert (
        result[0] == "https://test.blob.core.windows.net/test-container/blob1?sas-token"
    )
    assert (
        result[1] == "https://test.blob.core.windows.net/test-container/blob2?sas-token"
    )


@pytest.mark.asyncio
async def test_list_project_object_images_without_sas_token(client: ImageStorageClient):
    project_slug = "test-project"
    object_id = 123
    with_sas_token = False

    async def list_blob_names_mock(name_starts_with=None):
        yield "blob1"
        yield "blob2"

    container_mock = AsyncMock()
    container_mock.url = "https://test.blob.core.windows.net/test-container"
    container_mock.list_blob_names = list_blob_names_mock
    client._get_project_container = MagicMock(return_value=container_mock)
    client._generate_sas_token_for_container = MagicMock(return_value="sas-token")  # type: ignore[method-assign]

    result = []
    async for url in client.list_project_images(
        project_slug, object_id, with_sas_token
    ):
        result.append(url)

    assert len(result) == 2
    assert result[0] == "https://test.blob.core.windows.net/test-container/blob1"
    assert result[1] == "https://test.blob.core.windows.net/test-container/blob2"


@pytest.mark.asyncio
async def test_list_project_object_images_resource_not_found(
    client: ImageStorageClient,
):
    project_slug = "test-project"
    object_id = 123
    with_sas_token = False

    async def list_blob_names_mock(name_starts_with=None):
        raise ResourceNotFoundError()
        yield "blob1"
        yield "blob2"

    container_mock = AsyncMock()
    container_mock.url = "https://test.blob.core.windows.net/test-container"
    container_mock.list_blob_names = list_blob_names_mock
    client._get_project_container = MagicMock(return_value=container_mock)
    client._generate_sas_token_for_container = MagicMock(return_value="sas-token")  # type: ignore[method-assign]

    result = []
    async for url in client.list_project_images(
        project_slug, object_id, with_sas_token
    ):
        result.append(url)

        assert result is None  # No URLs should be returned


@pytest.mark.asyncio
async def test_generate_signed_upload_project_object_image_url(
    client: ImageStorageClient,
):
    project_slug = "test-project"
    object_id = 123
    file_name = "test.jpg"

    container_mock = MagicMock()
    container_mock.url = "https://test.blob.core.windows.net/test-container"
    container_mock.create_container = AsyncMock()
    client._get_project_container = MagicMock(return_value=container_mock)
    client._get_image_blob_name = MagicMock(  # type: ignore[method-assign]
        return_value="images/object-groups/123/test.jpg"
    )

    with patch("clients.azure.images.generate_blob_sas", return_value="sas-token"):
        result = await client.generate_signed_upload_project_image_url(
            project_slug=project_slug, object_id=object_id, file_name=file_name
        )

    assert (
        result
        == "https://test.blob.core.windows.net/test-container/images/object-groups/123/test.jpg?sas-token"
    )
    client._get_project_container.assert_called_once_with(project_slug)
    client._get_image_blob_name.assert_called_once_with(
        object_id=object_id, file_name=file_name
    )
