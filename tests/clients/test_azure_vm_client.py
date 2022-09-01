# pylint: disable=protected-access, no-member, redefined-outer-name

from unittest.mock import DEFAULT, MagicMock, patch

import pytest
from azure.core.exceptions import ResourceNotFoundError
from pytest import MonkeyPatch

from clients.azure import VMAzureClient
from clients.azure.vm import (
    AzureCaptureDeploymentProperties,
    AzureVMDeploymentProperties,
    DeploymentNotFound,
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
    monkeypatch.setenv("VM_LOGIN", "username")
    monkeypatch.setenv("VM_PASSWORD", "password")
    with patch.multiple(
        "clients.azure.vm",
        ResourceManagementClient=DEFAULT,
        ComputeManagementClient=DEFAULT,
        TemplateSpecsClient=DEFAULT,
    ):
        return VMAzureClient()


@patch("clients.azure.vm.VMAzureClient._get_latest_template_specs", dict)
def test_deploy_exits_when_vm_exists(client: VMAzureClient):
    client._resource_mgmt_client.deployments.check_existence.return_value = True
    client.deploy_vm("vm-test")

    client._resource_mgmt_client.deployments.begin_create_or_update.assert_not_called()


@patch("clients.azure.vm.VMAzureClient._get_latest_template_specs", dict)
@patch("clients.azure.vm._project_name_to_vm_name", lambda x: x)
def test_deploys_with_proper_parameters(client: VMAzureClient):
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
        call_args["parameters"]["properties"]["parameters"]["vmName"]["value"]
        == "vm-test"
    )
    assert (
        call_args["parameters"]["properties"]["parameters"]["vmSize"]["value"]
        == "Standard_B8ms"
    )
    assert isinstance(result, AzureVMDeploymentProperties)
    assert result.project_name == "vm-test"
    assert result.username == "username"
    assert isinstance(result.password, str)
    assert (
        result.deployment_process
        is client._resource_mgmt_client.deployments.begin_create_or_update.return_value
    )


@patch("clients.azure.vm.VMAzureClient._get_latest_template_specs", dict)
@patch("clients.azure.vm._project_name_to_vm_name", lambda x: x)
def test_create_image(client: VMAzureClient):
    client._resource_mgmt_client.deployments.check_existence.return_value = False
    result = client.create_new_image_version("vm-test", version="1.1.1")

    call_args = (
        client._resource_mgmt_client.deployments.begin_create_or_update.call_args[1]
    )

    assert "parameters" in call_args
    assert "properties" in call_args["parameters"]
    assert "template" in call_args["parameters"]["properties"]
    assert "parameters" in call_args["parameters"]["properties"]
    assert (
        call_args["parameters"]["properties"]["parameters"]["vmName"]["value"]
        == "vm-test"
    )
    assert (
        call_args["parameters"]["properties"]["parameters"]["version"]["value"]
        == "1.1.1"
    )
    assert isinstance(result, AzureCaptureDeploymentProperties)
    assert result.project_name == "vm-test"
    assert result.version == "1.1.1"
    assert (
        result.deployment_process
        is client._resource_mgmt_client.deployments.begin_create_or_update.return_value
    )


def test_get_latest_template_specs(client: VMAzureClient):
    client._template_specs_client.template_specs.get.return_value.versions = {
        "1.0.0": {},
        "1.1.1": {},
    }
    client._get_latest_template_specs(template_name="template_specs")

    client._template_specs_client.template_spec_versions.get.assert_called_with(
        resource_group_name="resource_group_name",
        template_spec_name="template_specs",
        template_spec_version="1.1.1",
    )


def test_get_vm_raises_if_absent(client: VMAzureClient):
    client._compute_mgmt_client.virtual_machines.get.side_effect = (
        ResourceNotFoundError()
    )
    with pytest.raises(VMNotFound):
        client.get_vm("VM")


@patch("clients.azure.vm._project_name_to_vm_name", lambda x: x)
def test_get_vm_calls_azure_method_with_proper_args(client: VMAzureClient):
    client.get_vm("VM")
    client._compute_mgmt_client.virtual_machines.get.assert_called_with(
        resource_group_name="resource_group_name",
        vm_name="VM",
    )


def test_get_deployment_status_returns_status(client: VMAzureClient):
    deployment_get_return = MagicMock(
        properties=MagicMock(provisioning_state="Succeeded")
    )
    client._resource_mgmt_client.deployments.get.return_value = deployment_get_return
    status = client.get_deployment_status("VM")

    client._resource_mgmt_client.deployments.get.assert_called_with(
        resource_group_name="resource_group_name", deployment_name="vm"
    )
    assert status == "Succeeded"


def test_get_deployment_raises_if_deployment_absent(client: VMAzureClient):
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


def test_delete_vm(client: VMAzureClient):
    client._resource_mgmt_client.deployments.check_existence.return_value = False
    client.delete_vm("vm-test")

    client._compute_mgmt_client.virtual_machines.begin_delete.assert_called_with(
        resource_group_name="resource_group_name",
        vm_name="test-vm-test",
    )


def test_delete_vm_raises_if_vm_absent(client: VMAzureClient):
    client._compute_mgmt_client.virtual_machines.begin_delete.side_effect = (
        ResourceNotFoundError()
    )
    with pytest.raises(VMNotFound):
        client.delete_vm("vm-test")


def test_project_name_to_vm_name(monkeypatch: MonkeyPatch):
    monkeypatch.setenv("AZURE_RESOURCE_PREFIX", "test-")
    assert _project_name_to_vm_name("BLABLA") == "test-blabla"
