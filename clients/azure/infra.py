import os

from azure.identity import DefaultAzureCredential
from azure.mgmt.containerinstance import ContainerInstanceManagementClient
from azure.mgmt.web import WebSiteManagementClient
from azure.mgmt.web.models import StringDictionary
from dotenv import load_dotenv

load_dotenv()


class InfraAzureClient:
    """Client related to infrastructure on Azure."""

    def __init__(self):
        self.resource_group_name = os.environ["AZURE_RESOURCE_GROUP_NAME"]
        credentials = DefaultAzureCredential()

        self.web_site_mgmt_client = WebSiteManagementClient(
            credentials, os.environ["AZURE_SUBSCRIPTION_ID"]
        )
        self.container_instance_mgmt_client = ContainerInstanceManagementClient(
            credentials, os.environ["AZURE_SUBSCRIPTION_ID"]
        )

    def update_guacamole_webapp_guacd_hostname(self, new_hostname: str):
        settings = self.list_guacamole_webapp_settings()
        if settings["GUACD_HOSTNAME"] == new_hostname:
            return None
        settings["GUACD_HOSTNAME"] = new_hostname
        self._update_webapp_settings(
            f'{os.environ["AZURE_RESOURCE_PREFIX"]}-guacamole', settings
        )
        self.restart_guacamole_weppapp()
        return None

    def list_guacamole_webapp_settings(self) -> dict[str, str]:
        return self._list_webapp_settings(
            f'{os.environ["AZURE_RESOURCE_PREFIX"]}-guacamole'
        )

    def restart_guacamole_weppapp(self):
        self._restart_app(f'{os.environ["AZURE_RESOURCE_PREFIX"]}-guacamole')

    def get_guacd_ip(self) -> str:
        container_group = self.container_instance_mgmt_client.container_groups.get(
            self.resource_group_name,
            f'{os.environ["AZURE_RESOURCE_PREFIX"]}-guacd-container',
        )
        if container_group.ip_address and container_group.ip_address.ip:
            return container_group.ip_address.ip
        raise ValueError("Could not retrieve Guacd IP address (is None).")

    def _restart_app(self, webapp_name: str):
        self.web_site_mgmt_client.web_apps.restart(
            self.resource_group_name,
            name=webapp_name,
        )

    def _list_webapp_settings(self, webapp_name: str) -> dict[str, str]:
        return self.web_site_mgmt_client.web_apps.list_application_settings(
            self.resource_group_name,
            name=webapp_name,
        ).properties

    def _update_webapp_settings(self, webapp_name: str, settings: dict[str, str]):
        self.web_site_mgmt_client.web_apps.update_application_settings(
            self.resource_group_name,
            name=webapp_name,
            app_settings=StringDictionary(properties=settings),
        )
