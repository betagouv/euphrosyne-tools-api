from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from auth import Project, User, get_current_user
from azure_client import AzureVMDeploymentProperties, DeploymentNotFound, VMNotFound
from guacamole_client import GuacamoleConnectionNotFound
from main import app, wait_for_deploy

client = TestClient(app)


async def get_current_user_override():
    return User(id=1, projects=[Project(id=1, name="project_01")], is_admin=False)


def get_admin_user_override():
    return User(id=1, projects=[], is_admin=True)


app.dependency_overrides[get_current_user] = get_current_user_override


def test_no_project_membership_exception_handler():
    def get_not_permitted_user_override():
        return User(id=1, projects=[], is_admin=False)

    app.dependency_overrides[get_current_user] = get_not_permitted_user_override
    response = client.get("/connect/project_01")
    assert response.status_code == 403
    assert response.json()["detail"] == "User does not have access to this project"
    app.dependency_overrides[get_current_user] = get_current_user_override


@patch("main.azure_client")
def test_get_connection_link_when_no_vm(azure_mock: MagicMock):
    azure_mock.get_vm.side_effect = VMNotFound()
    response = client.get("/connect/project_01")
    assert response.status_code == 404
    assert response.json()["detail"] == "Azure VM not found"


@patch("main.azure_client", MagicMock())
@patch("main.guacamole_client")
def test_get_connection_link_when_no_guaca_conn(guacamole_client):
    guacamole_client.get_connection_by_name.side_effect = GuacamoleConnectionNotFound()
    response = client.get("/connect/project_01")
    assert response.status_code == 404
    assert response.json()["detail"] == "Guacamole connection not found"


@patch("main.azure_client", MagicMock())
@patch("main.guacamole_client")
def test_get_connection_link_ok(guacamole_client):
    guacamole_client.generate_connection_link.return_value = "url"
    response = client.get("/connect/project_01")
    assert response.status_code == 200
    assert response.json()["url"] == "url"
    guacamole_client.create_user_if_absent.assert_called_once_with("1")
    guacamole_client.create_user_if_absent.assert_called_once()
    guacamole_client.generate_connection_link.assert_called_once()


@patch("main.azure_client")
def test_get_deployment_status_when_no_deployment(azure_mock: MagicMock):
    azure_mock.get_deployment_status.side_effect = DeploymentNotFound()
    response = client.get("/deployments/project_01")
    assert response.status_code == 404


@patch("main.azure_client")
def test_get_deployment_status_ok(azure_mock: MagicMock):
    azure_mock.get_deployment_status.return_value = "Succeeded"
    response = client.get("/deployments/project_01")
    assert response.status_code == 200
    assert response.json()["status"] == "Succeeded"


@patch("main.azure_client")
def test_deploy_vm_ok(azure_mock: MagicMock):
    deploy_return_value = AzureVMDeploymentProperties(
        deployment_process=MagicMock(),
        password="password",
        username="username",
        project_name="project_name",
    )
    azure_mock.deploy_vm.return_value = deploy_return_value
    with patch("fastapi.BackgroundTasks.add_task") as mock:
        response = client.post("/deployments/project_01")
        assert response.status_code == 202
        mock.assert_called_once_with(wait_for_deploy, deploy_return_value)


@patch("main.azure_client")
def test_deploy_vm_when_already_deployed(azure_mock: MagicMock):
    azure_mock.deploy_vm.return_value = None
    with patch("fastapi.BackgroundTasks.add_task") as mock:
        response = client.post("/deployments/project_01")
        assert response.status_code == 202
        mock.assert_not_called()


@patch("main.azure_client")
@patch("main.guacamole_client")
def test_delete_vm(guacamole_mock: MagicMock, azure_mock: MagicMock):
    app.dependency_overrides[get_current_user] = get_admin_user_override

    response = client.delete("/vms/project_01")
    assert response.status_code == 202
    azure_mock.delete_vm.assert_called()
    guacamole_mock.delete_connection.assert_called()

    app.dependency_overrides[get_current_user] = get_current_user


def test_delete_vm_restriced_when_not_admin():
    response = client.delete("/vms/project_01")
    assert response.status_code == 403


@patch("main.azure_client")
@patch("main.guacamole_client")
def test_delete_vm_when_no_connection(guacamole_mock: MagicMock, _: MagicMock):
    app.dependency_overrides[get_current_user] = get_admin_user_override

    guacamole_mock.delete_connection.side_effect = GuacamoleConnectionNotFound()
    response = client.delete("/vms/project_01")
    assert response.status_code == 202

    app.dependency_overrides[get_current_user] = get_current_user


@patch("azure_client.AzureClient.delete_deployment")
def test_wait_for_deploy_when_success(delete_deployment_mock: MagicMock):
    deployment_properties = AzureVMDeploymentProperties(
        deployment_process=MagicMock(),
        password="password",
        username="username",
        project_name="project_name",
    )
    deployment_information = MagicMock(
        properties=MagicMock(outputs={"privateIPVM": {"value": "1.1.1.1"}})
    )
    with patch("main.wait_for_deployment_completeness") as wait_deployment_mock:
        wait_deployment_mock.return_value = deployment_information
        with patch(
            "guacamole_client.GuacamoleClient.create_connection"
        ) as create_connection_mock:
            wait_for_deploy(deployment_properties)
            create_connection_mock.assert_called_once_with(
                name=deployment_properties.project_name,
                ip_address="1.1.1.1",
                password=deployment_properties.password,
                username=deployment_properties.username,
            )
            delete_deployment_mock.assert_called_once_with(deployment_information.name)


def test_wait_for_deploy_when_failed():
    deployment_properties = AzureVMDeploymentProperties(
        deployment_process=MagicMock,
        password="password",
        username="username",
        project_name="project_name",
    )
    with patch("main.wait_for_deployment_completeness") as wait_deployment_mock:
        wait_deployment_mock.return_value = None
        with patch(
            "guacamole_client.GuacamoleClient.create_connection"
        ) as create_connection_mock:
            wait_for_deploy(deployment_properties)
            create_connection_mock.assert_not_called()
