from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from auth import User, get_current_user
from clients import VMSizes
from clients.azure.config import ConfigAzureClient
from clients.azure.vm import VMAzureClient
from dependencies import get_config_azure_client, get_vm_azure_client
from main import app


async def get_admin_user_override():
    return User(id="1", projects=[], is_admin=True)


_client = TestClient(app)


@pytest.fixture(name="client")
def fixture_client():
    app.dependency_overrides[get_config_azure_client] = lambda: MagicMock(
        spec=ConfigAzureClient
    )
    app.dependency_overrides[get_current_user] = get_admin_user_override
    return _client


def test_get_project_vm_size_when_set(client: TestClient):
    app.dependency_overrides[get_config_azure_client] = lambda: MagicMock(
        spec=ConfigAzureClient, **{"get_project_vm_size.return_value": VMSizes.IMAGERY}
    )
    response = client.get("config/project/vm-size")
    assert response.json() == {"vm_size": "IMAGERY"}


def test_get_project_vm_size_when_unset(client: TestClient):
    app.dependency_overrides[get_config_azure_client] = lambda: MagicMock(
        spec=ConfigAzureClient, **{"get_project_vm_size.return_value": None}
    )
    response = client.get("config/project/vm-size")
    assert response.json() == {"vm_size": None}


def test_set_project_vm_size(client: TestClient):
    config_client_mock = MagicMock(spec=ConfigAzureClient)
    app.dependency_overrides[get_config_azure_client] = lambda: config_client_mock
    response = client.post("config/project/vm-size", json={"vm_size": "IMAGERY"})

    assert response.status_code == 202
    config_client_mock.set_project_vm_size.assert_called_with("project", "IMAGERY")


def test_set_project_vm_size_with_empty_value(client: TestClient):
    config_client_mock = MagicMock(spec=ConfigAzureClient)
    app.dependency_overrides[get_config_azure_client] = lambda: config_client_mock
    response = client.post("config/project/vm-size", json={"vm_size": ""})

    assert response.status_code == 202
    config_client_mock.set_project_vm_size.assert_called_with("project", None)


@pytest.mark.parametrize(
    "wrong_vm_size",
    ["BLABLA", None],
)
def test_set_project_vm_size_accepts_only_empty_string_or_value(
    client: TestClient, wrong_vm_size: str
):
    config_client_mock = MagicMock(spec=ConfigAzureClient)
    app.dependency_overrides[get_config_azure_client] = lambda: config_client_mock
    response = client.post("config/project/vm-size", json={"vm_size": wrong_vm_size})

    assert response.status_code == 422


def test_get_project_image_definition_when_set(client: TestClient):
    app.dependency_overrides[get_config_azure_client] = lambda: MagicMock(
        spec=ConfigAzureClient,
        **{
            "get_project_image_definition.return_value": "animage",
        }
    )
    response = client.get("config/project/image-definition")
    assert response.json() == {"image_definition": "animage"}


def test_get_project_image_definition_when_unset(client: TestClient):
    app.dependency_overrides[get_config_azure_client] = lambda: MagicMock(
        spec=ConfigAzureClient,
        **{
            "get_project_image_definition.return_value": None,
        }
    )
    response = client.get("config/project/image-definition")
    assert response.json() == {"image_definition": None}


def test_set_project_image_definition(client: TestClient):
    app.dependency_overrides[get_vm_azure_client] = lambda: MagicMock(
        spec=VMAzureClient,
        **{
            "list_vm_image_definitions.return_value": ["animage"],
        }
    )
    config_client_mock = MagicMock(spec=ConfigAzureClient)
    app.dependency_overrides[get_config_azure_client] = lambda: config_client_mock

    response = client.post("config/project/image-definition", json="animage")

    assert response.status_code == 202
    config_client_mock.set_project_image_definition.assert_called_with(
        "project", "animage"
    )


def test_set_project_image_definition_with_empty_value(client: TestClient):
    app.dependency_overrides[get_vm_azure_client] = lambda: MagicMock(
        spec=VMAzureClient,
        **{
            "list_vm_image_definitions.return_value": ["animage"],
        }
    )
    config_client_mock = MagicMock(spec=ConfigAzureClient)
    app.dependency_overrides[get_config_azure_client] = lambda: config_client_mock

    response = client.post("config/project/image-definition", json="")

    assert response.status_code == 202, response.json()
    config_client_mock.set_project_image_definition.assert_called_with("project", None)


def test_set_project_image_definition_with_non_existant_image_raises(
    client: TestClient,
):
    app.dependency_overrides[get_vm_azure_client] = lambda: MagicMock(
        spec=VMAzureClient,
        **{
            "list_vm_image_definitions.return_value": ["animage"],
        }
    )
    config_client_mock = MagicMock(spec=ConfigAzureClient)
    app.dependency_overrides[get_config_azure_client] = lambda: config_client_mock

    response = client.post("config/project/image-definition", json="an-unknown-image")

    assert response.status_code == 400, response.json()
    config_client_mock.set_project_image_definition.assert_not_called()
