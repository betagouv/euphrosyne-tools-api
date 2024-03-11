from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from auth import User, get_current_user
from clients.azure.vm import VMAzureClient
from dependencies import get_vm_azure_client
from main import app


async def get_admin_user_override():
    return User(id=1, projects=[], is_admin=True)


_client = TestClient(app)


@pytest.fixture(name="client")
def fixture_client():
    app.dependency_overrides[get_vm_azure_client] = lambda: MagicMock(
        spec=VMAzureClient
    )
    app.dependency_overrides[get_current_user] = get_admin_user_override
    return _client


def test_list_image_definitions(client: TestClient):
    app.dependency_overrides[get_vm_azure_client] = lambda: MagicMock(
        spec=VMAzureClient,
        **{"list_vm_image_definitions": MagicMock(return_value=["image1", "image2"])}
    )

    response = client.get("vms/image-definitions")

    assert response.status_code == 200
    assert response.json() == {"image_definitions": ["image1", "image2"]}
