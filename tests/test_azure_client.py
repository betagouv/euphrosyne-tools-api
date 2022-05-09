from unittest.mock import MagicMock, patch

from pytest import MonkeyPatch

from azure_client import AzureClient


@patch("azure_client.ComputeManagementClient")
@patch("azure_client.ResourceManagementClient")
@patch("azure_client.AzureClient._get_latest_template_specs", dict)
def test_deploys_with_proper_parameters(
    compute_client: MagicMock,
    resource_client: MagicMock,
    monkeypatch: MonkeyPatch,
):
    monkeypatch.setenv("AZURE_RESOURCE_GROUP_NAME", "resource_group_name")
    monkeypatch.setenv("AZURE_TEMPLATE_SPECS_NAME", "template_specs")
    client = AzureClient()

    client.deploy_vm("vm-test")
    call_args = (
        client._resource_mgmt_client.deployments.begin_create_or_update.call_args[1]
    )
    assert "parameters" in call_args
    assert "properties" in call_args["parameters"]
    assert "template" in call_args["parameters"]["properties"]
    assert "parameters" in call_args["parameters"]["properties"]
    assert (
        call_args["parameters"]["properties"]["parameters"]["adminUsername"]["value"]
        == "euphrosyne"
    )
    assert (
        call_args["parameters"]["properties"]["parameters"]["vmName"]["value"]
        == "vm-test"
    )


@patch("azure_client.TemplateSpecsClient")
def test_get_latest_template_specs(
    template_specs_client: MagicMock,
    monkeypatch: MonkeyPatch,
):
    monkeypatch.setenv("AZURE_RESOURCE_GROUP_NAME", "resource_group_name")
    monkeypatch.setenv("AZURE_TEMPLATE_SPECS_NAME", "template_specs")

    client = AzureClient()
    client._template_specs_client.template_specs.get.return_value.versions = {
        "1.0.0": {},
        "1.1.1": {},
    }

    client._get_latest_template_specs()
    client._template_specs_client.template_spec_versions.get.assert_called_with(
        resource_group_name="resource_group_name",
        template_spec_name="template_specs",
        template_spec_version="1.1.1",
    )
