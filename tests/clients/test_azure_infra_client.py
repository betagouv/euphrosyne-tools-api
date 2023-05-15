from unittest.mock import DEFAULT, MagicMock, patch

import pytest
from azure.mgmt.web.models import StringDictionary
from pytest import MonkeyPatch

from clients.azure.infra import InfraAzureClient


@pytest.fixture(name="client")
def infra_client(monkeypatch: MonkeyPatch):
    monkeypatch.setenv("AZURE_RESOURCE_GROUP_NAME", "resource_group_name")
    monkeypatch.setenv("AZURE_SUBSCRIPTION_ID", "subscription_id")
    with patch("clients.azure.infra.WebSiteManagementClient"):
        with patch("clients.azure.infra.ContainerInstanceManagementClient"):
            return InfraAzureClient()


def test_client_restart_app(client: InfraAzureClient):
    # pylint: disable=protected-access
    client._restart_app("app_name")
    client.web_site_mgmt_client.web_apps.restart.assert_called_once_with(
        "resource_group_name", name="app_name"
    )


def test_client_list_webapp_settings(client: InfraAzureClient):
    client.web_site_mgmt_client.web_apps.list_application_settings.return_value = (
        MagicMock(properties={})
    )
    # pylint: disable=protected-access
    properties = client._list_webapp_settings("app_name")
    client.web_site_mgmt_client.web_apps.list_application_settings.assert_called_once_with(
        "resource_group_name", name="app_name"
    )
    assert properties == {}


def test_client_update_webapp_settings(client: InfraAzureClient):
    # pylint: disable=protected-access
    client._update_webapp_settings("app_name", {"props1": "value1"})
    client.web_site_mgmt_client.web_apps.update_application_settings.assert_called_once_with(
        "resource_group_name",
        name="app_name",
        app_settings=StringDictionary(properties={"props1": "value1"}),
    )


def test_client_update_guacd_hostname_returns_if_same_host(client: InfraAzureClient):
    with patch.multiple(
        client,
        list_guacamole_webapp_settings=DEFAULT,
        _update_webapp_settings=DEFAULT,
        restart_guacamole_weppapp=DEFAULT,
    ) as client_mock:
        client_mock["list_guacamole_webapp_settings"].return_value = {
            "GUACD_HOSTNAME": "hostname"
        }
        client.update_guacamole_webapp_guacd_hostname("hostname")
        client_mock["_update_webapp_settings"].assert_not_called()
        client_mock["restart_guacamole_weppapp"].assert_not_called()


def test_client_update_guacd_hostname_returns_if_different_host(
    client: InfraAzureClient, monkeypatch: MonkeyPatch
):
    monkeypatch.setenv("AZURE_RESOURCE_PREFIX", "prefix")
    with patch.multiple(
        client,
        list_guacamole_webapp_settings=DEFAULT,
        _update_webapp_settings=DEFAULT,
        restart_guacamole_weppapp=DEFAULT,
    ) as client_mock:
        settings = {"GUACD_HOSTNAME": "hostname"}
        client_mock["list_guacamole_webapp_settings"].return_value = settings
        client.update_guacamole_webapp_guacd_hostname("newhost")
        client_mock["_update_webapp_settings"].assert_called_with(
            "prefix-guacamole", settings
        )
        client_mock["restart_guacamole_weppapp"].assert_called()


def test_client_list_guacamole_webapp_settings(
    client: InfraAzureClient, monkeypatch: MonkeyPatch
):
    monkeypatch.setenv("AZURE_RESOURCE_PREFIX", "prefix")
    with patch.object(client, "_list_webapp_settings") as mock_method:
        client.list_guacamole_webapp_settings()
        # pylint: disable=protected-access
        mock_method.assert_called_once_with("prefix-guacamole")


def test_client_restart_guacamole_weppapp(
    client: InfraAzureClient, monkeypatch: MonkeyPatch
):
    monkeypatch.setenv("AZURE_RESOURCE_PREFIX", "prefix")
    with patch.object(client, "_restart_app") as mock_method:
        client.restart_guacamole_weppapp()
        # pylint: disable=protected-access
        mock_method.assert_called_once_with("prefix-guacamole")


def test_client_get_guacd_ip(client: InfraAzureClient, monkeypatch: MonkeyPatch):
    monkeypatch.setenv("AZURE_RESOURCE_PREFIX", "prefix")
    client.container_instance_mgmt_client.container_groups.get.return_value = MagicMock(
        ip_address=MagicMock(ip="123")
    )
    assert client.get_guacd_ip() == "123"
    client.container_instance_mgmt_client.container_groups.get.assert_called_with(
        "resource_group_name", "prefix-guacd-container"
    )


@pytest.mark.parametrize(
    "return_value",
    (
        MagicMock(ip_address=MagicMock(ip=None)),
        MagicMock(ip_address=None),
    ),
)
def test_client_get_guacd_ip_raises_when_no_ip(
    return_value: MagicMock, client: InfraAzureClient, monkeypatch: MonkeyPatch
):
    monkeypatch.setenv("AZURE_RESOURCE_PREFIX", "prefix")
    client.container_instance_mgmt_client.container_groups.get.return_value = (
        return_value
    )
    with pytest.raises(ValueError):
        client.get_guacd_ip()
