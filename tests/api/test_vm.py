import datetime
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from auth import User, get_current_user
from clients.azure.vm import VMAzureClient, VMNotFound
from dependencies import get_vm_azure_client
from main import app


async def get_admin_user_override():
    return User(id="1", projects=[], is_admin=True)


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


def test_list_vms(client: TestClient):
    app.dependency_overrides[get_vm_azure_client] = lambda: MagicMock(
        spec=VMAzureClient, **{"list_vms": MagicMock(return_value=["vm1", "vm2"])}
    )
    response = client.get("vms/")

    assert response.status_code == 200
    assert response.json() == ["vm1", "vm2"]


def test_list_vms_created_before(client: TestClient):
    list_vms_mock = MagicMock()
    app.dependency_overrides[get_vm_azure_client] = lambda: MagicMock(
        spec=VMAzureClient, **{"list_vms": list_vms_mock}
    )
    response = client.get("vms?created_before=2022-01-01T00:00:00")

    assert response.status_code == 200
    assert list_vms_mock.call_args.kwargs["created_before"] == datetime.datetime(
        2022, 1, 1, 0, 0
    )


def test_get_vm(client: TestClient):
    app.dependency_overrides[get_vm_azure_client] = lambda: MagicMock(
        spec=VMAzureClient,
        **{"get_vm": MagicMock(return_value=MagicMock(provisioning_state="state"))}
    )

    response = client.get("vms/project_name")

    assert response.status_code == 200
    assert response.json() == {"provisioning_state": "state"}


def test_get_vm_not_found(client: TestClient):
    app.dependency_overrides[get_vm_azure_client] = lambda: MagicMock(
        spec=VMAzureClient, **{"get_vm": MagicMock(side_effect=VMNotFound)}
    )

    response = client.get("vms/project_name")

    assert response.status_code == 404
