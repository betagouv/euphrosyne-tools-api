from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from auth import User, get_current_user
from backgrounds import wait_for_deploy
from clients.azure import VMAzureClient
from clients.azure.data import IncorrectDataFilePath, ProjectFile
from clients.azure.vm import AzureVMDeploymentProperties, DeploymentNotFound, VMNotFound
from clients.guacamole import GuacamoleClient, GuacamoleConnectionNotFound
from dependencies import (
    get_config_azure_client,
    get_guacamole_client,
    get_storage_azure_client,
    get_vm_azure_client,
)
from tests.conftest import get_current_user_override


async def get_admin_user_override():
    return User(id=1, projects=[], is_admin=True)


def test_no_project_membership_exception_handler(app: FastAPI, client: TestClient):
    def get_not_permitted_user_override():
        return User(id=1, projects=[], is_admin=False)

    app.dependency_overrides[get_current_user] = get_not_permitted_user_override
    response = client.get("/connect/project_01")
    assert response.status_code == 403
    assert response.json()["detail"] == "User does not have access to this project"


def test_get_connection_link_when_no_vm(app: FastAPI, client: TestClient):
    app.dependency_overrides[get_vm_azure_client] = lambda: MagicMock(
        get_vm=MagicMock(side_effect=VMNotFound())
    )
    response = client.get("/connect/project_01")
    assert response.status_code == 404
    assert response.json()["detail"] == "Azure VM not found"


def test_get_connection_link_when_no_guaca_conn(app: FastAPI, client: TestClient):
    app.dependency_overrides[get_guacamole_client] = lambda: MagicMock(
        get_connection_by_name=MagicMock(side_effect=GuacamoleConnectionNotFound())
    )
    response = client.get("/connect/project_01")
    assert response.status_code == 404
    assert response.json()["detail"] == "Guacamole connection not found"


def test_get_connection_link_ok(app: FastAPI, client: TestClient):
    guacamole_client_mock = MagicMock(
        generate_connection_link=MagicMock(return_value="url")
    )
    app.dependency_overrides[get_guacamole_client] = lambda: guacamole_client_mock
    response = client.get("/connect/project_01")
    assert response.status_code == 200
    assert response.json()["url"] == "url"
    guacamole_client_mock.create_user_if_absent.assert_called_once_with("1")
    guacamole_client_mock.create_user_if_absent.assert_called_once()
    guacamole_client_mock.generate_connection_link.assert_called_once()


def test_get_deployment_status_when_no_deployment(app: FastAPI, client: TestClient):
    app.dependency_overrides[get_vm_azure_client] = lambda: MagicMock(
        get_deployment_status=MagicMock(side_effect=DeploymentNotFound())
    )
    response = client.get("/deployments/project_01")
    assert response.status_code == 404


def test_get_deployment_status_ok(app: FastAPI, client: TestClient):
    app.dependency_overrides[get_vm_azure_client] = lambda: MagicMock(
        get_deployment_status=MagicMock(return_value="Succeeded")
    )
    response = client.get("/deployments/project_01")
    assert response.status_code == 200
    assert response.json()["status"] == "Succeeded"


def test_deploy_vm_ok(app: FastAPI, client: TestClient):
    deploy_return_value = AzureVMDeploymentProperties(
        deployment_process=MagicMock(),
        password="password",
        username="username",
        project_name="project_name",
    )
    app.dependency_overrides[get_vm_azure_client] = lambda: MagicMock(
        deploy_vm=MagicMock(return_value=deploy_return_value)
    )
    app.dependency_overrides[get_config_azure_client] = lambda: MagicMock(
        get_project_vm_size=MagicMock(return_value=None)
    )
    with patch("fastapi.BackgroundTasks.add_task") as mock:
        response = client.post("/deployments/project_01")
        assert response.status_code == 202
        assert mock.call_count == 1
        assert mock.call_args_list[0][0][:2] == (wait_for_deploy, deploy_return_value)


def test_deploy_vm_when_already_deployed(app: FastAPI, client: TestClient):
    app.dependency_overrides[get_vm_azure_client] = lambda: MagicMock(
        deploy_vm=MagicMock(return_value=None)
    )
    app.dependency_overrides[get_config_azure_client] = lambda: MagicMock(
        get_project_vm_size=MagicMock(return_value=None)
    )
    with patch("fastapi.BackgroundTasks.add_task") as mock:
        response = client.post("/deployments/project_01")
        assert response.status_code == 202
        mock.assert_not_called()


@patch("fastapi.BackgroundTasks.add_task", MagicMock())
def test_deploy_vm_when_project_has_image_definition(app: FastAPI, client: TestClient):
    client_mock = MagicMock()
    app.dependency_overrides[get_vm_azure_client] = lambda: client_mock
    app.dependency_overrides[get_config_azure_client] = lambda: MagicMock(
        get_project_vm_size=MagicMock(return_value=None),
        get_project_image_definition=MagicMock(return_value="animage"),
    )
    response = client.post("/deployments/project_01")
    assert response.status_code == 202
    assert client_mock.deploy_vm.call_args[1]["image_definition"] == "animage"


def test_delete_vm(app: FastAPI, client: TestClient):
    azure_mock = MagicMock(spec=VMAzureClient)
    app.dependency_overrides[get_vm_azure_client] = lambda: azure_mock
    guacamole_mock = MagicMock(spec=GuacamoleClient)
    app.dependency_overrides[get_guacamole_client] = lambda: guacamole_mock
    app.dependency_overrides[get_current_user] = get_admin_user_override

    response = client.delete("/vms/project_01")
    assert response.status_code == 202
    azure_mock.delete_vm.assert_called()
    guacamole_mock.delete_connection.assert_called()

    app.dependency_overrides[get_current_user] = get_current_user_override


def test_delete_vm_restriced_when_not_admin(client: TestClient):
    response = client.delete("/vms/project_01")

    assert response.status_code == 403


def test_delete_vm_when_no_connection(app: FastAPI, client: TestClient):
    app.dependency_overrides[get_current_user] = get_admin_user_override
    app.dependency_overrides[get_guacamole_client] = lambda: MagicMock(
        delete_connection=MagicMock(side_effect=GuacamoleConnectionNotFound())
    )

    response = client.delete("/vms/project_01")
    assert response.status_code == 202

    app.dependency_overrides[get_current_user] = get_current_user_override


@pytest.mark.parametrize(
    ("data_type"),
    (("raw_data"), ("processed_data"), ("HDF5")),
)
def test_list_run_data(app: FastAPI, client: TestClient, data_type: tuple[str]):
    def yield_project_files():
        for i in range(4):
            yield ProjectFile(
                name=f"file-{i}.txt",
                last_modified=datetime(2022, 6, 22, 11, 22, 33),
                size=i * 222,
                path=f"somepath/file-{i}.txt",
            )

    get_run_files_mock = MagicMock(
        get_run_files=MagicMock(return_value=yield_project_files())
    )
    app.dependency_overrides[get_storage_azure_client] = lambda: get_run_files_mock

    response = client.get(f"/data/project_01/runs/run_01/{data_type}")
    files = response.json()

    assert get_run_files_mock.mock_calls[0][1] == (
        "project_01",
        "run_01",
        f"{data_type}",
    )
    assert response.status_code == 200
    assert len(files) == 4
    assert "name" in files[0]
    assert "last_modified" in files[0]
    assert "size" in files[0]
    assert "path" in files[0]


def test_generate_project_documents_upload_sas_url_success(
    app: FastAPI, client: TestClient
):
    generate_project_documents_upload_sas_url_mock = MagicMock(return_value="url")
    app.dependency_overrides[get_storage_azure_client] = lambda: MagicMock(
        generate_project_documents_upload_sas_url=generate_project_documents_upload_sas_url_mock
    )

    response = client.get(
        "/data/project_01/documents/upload/shared_access_signature?file_name=file.txt"
    )

    assert response.status_code == 200
    assert response.json()["url"] == "url"
    assert generate_project_documents_upload_sas_url_mock.call_args[1] == {
        "project_name": "project_01",
        "file_name": "file.txt",
    }


@patch("api.data.validate_project_document_file_path", MagicMock())
def test_generate_project_documents_sas_url_success(app: FastAPI, client: TestClient):
    generate_shared_access_signature_url_mock = MagicMock(return_value="url")
    app.dependency_overrides[get_storage_azure_client] = lambda: MagicMock(
        generate_project_documents_sas_url=generate_shared_access_signature_url_mock,
    )
    file_path = "file/path/to/document"
    response = client.get(f"/data/documents/shared_access_signature?path={file_path}")

    assert response.status_code == 200
    assert response.json()["url"] == "url"
    assert generate_shared_access_signature_url_mock.call_args[1] == {
        "dir_path": "file/path/to",
        "file_name": "document",
    }


@patch(
    "api.data.validate_project_document_file_path",
    MagicMock(side_effect=IncorrectDataFilePath("wrong file path")),
)
def test_generate_project_documents_sas_url_wrong_path(client: TestClient):
    response = client.get("/data/documents/shared_access_signature?path=file_path")

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["query", "path"]
    assert response.json()["detail"][0]["msg"] == "wrong file path"


@patch("api.data.validate_run_data_file_path", MagicMock())
def test_generate_run_data_sas_url_success(app: FastAPI, client: TestClient):
    generate_shared_access_signature_url_mock = MagicMock(return_value="url")
    app.dependency_overrides[get_storage_azure_client] = lambda: MagicMock(
        generate_run_data_sas_url=generate_shared_access_signature_url_mock
    )
    file_path = "file/path/to/run"
    response = client.get(f"/data/runs/shared_access_signature?path={file_path}")

    assert response.status_code == 200
    assert response.json()["url"] == "url"
    assert generate_shared_access_signature_url_mock.call_args[1] == {
        "dir_path": "file/path/to",
        "file_name": "run",
        "is_admin": False,
    }


@patch(
    "api.data.validate_run_data_file_path",
    MagicMock(side_effect=IncorrectDataFilePath("wrong file path")),
)
def test_generate_run_data_sas_url_wrong_path(
    client: TestClient,
):
    response = client.get("/data/runs/shared_access_signature?path=file_path")

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["query", "path"]
    assert response.json()["detail"][0]["msg"] == "wrong file path"


def test_wait_for_deploy_when_success():
    deployment_properties = AzureVMDeploymentProperties(
        deployment_process=MagicMock(),
        password="password",
        username="username",
        project_name="project_name",
    )
    deployment_information = MagicMock(
        properties=MagicMock(outputs={"privateIPVM": {"value": "1.1.1.1"}})
    )
    with patch("backgrounds.wait_for_deployment_completeness") as wait_deployment_mock:
        wait_deployment_mock.return_value = deployment_information
        guacamole_client_mock = MagicMock(spec=GuacamoleClient)
        wait_for_deploy(
            deployment_properties,
            guacamole_client=guacamole_client_mock,
        )
        guacamole_client_mock.create_connection.assert_called_once_with(
            name=deployment_properties.project_name,
            ip_address="1.1.1.1",
            password=deployment_properties.password,
            username=deployment_properties.username,
            vm_size=None,
        )


def test_wait_for_deploy_when_failed():
    deployment_properties = AzureVMDeploymentProperties(
        deployment_process=MagicMock,
        password="password",
        username="username",
        project_name="project_name",
    )
    with patch("backgrounds.wait_for_deployment_completeness") as wait_deployment_mock:
        wait_deployment_mock.return_value = None
        guacamole_client_mock = MagicMock(spec=GuacamoleClient)
        wait_for_deploy(
            deployment_properties,
            guacamole_client=guacamole_client_mock,
        )
        guacamole_client_mock.create_connection.assert_not_called()
