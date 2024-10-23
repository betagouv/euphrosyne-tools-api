from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from auth import verify_project_membership
from clients.azure.images import ImageStorageClient
from dependencies import get_image_storage_client


@pytest.fixture(name="image_storage_client")
def fixture_image_storage_client():
    return MagicMock(spec=ImageStorageClient)


@pytest.fixture
def override_dependencies(app: FastAPI, image_storage_client: ImageStorageClient):
    app.dependency_overrides[get_image_storage_client] = lambda: image_storage_client
    app.dependency_overrides[verify_project_membership] = lambda: None
    yield
    app.dependency_overrides = {}


def test_list_project_object_images(
    client: TestClient, image_storage_client, override_dependencies
):
    image_storage_client.list_project_images.return_value = AsyncMock()
    image_storage_client.list_project_images.return_value.__aiter__.return_value = [
        "image1.png",
        "image2.jpg",
    ]

    response = client.get("/images/projects/test_project/object-groups/1")

    assert response.status_code == 200
    assert response.json() == {"images": ["image1.png", "image2.jpg"]}


def test_get_upload_signed_url_valid_extension(
    client: TestClient, image_storage_client, override_dependencies
):
    image_storage_client.generate_signed_upload_project_image_url.return_value = (
        "http://signed.url"
    )

    response = client.get(
        "/images/upload/signed-url",
        params={
            "project_name": "test_project",
            "object_group_id": 1,
            "file_name": "image.png",
        },
    )

    assert response.status_code == 200
    assert response.json() == {"url": "http://signed.url"}


def test_get_upload_signed_url_invalid_extension(
    client: TestClient, override_dependencies
):
    response = client.get(
        "/images/upload/signed-url",
        params={
            "project_name": "test_project",
            "object_group_id": 1,
            "file_name": "image.txt",
        },
    )

    assert response.status_code == 400
    assert response.json() == {
        "detail": {
            "message": "File extension not supported",
            "error_code": "extension-not-supported",
        }
    }


def test_get_readonly_project_container_signed_url(
    client: TestClient, image_storage_client, override_dependencies
):
    image_storage_client.get_project_container_sas_token.return_value = "sas_token"
    image_storage_client.get_project_container_base_url.return_value = "http://base.url"

    response = client.get("/images/projects/test_project/signed-url")

    assert response.status_code == 200
    assert response.json() == {"token": "sas_token", "base_url": "http://base.url"}
