#!/usr/bin/env python3
from clients.azure import InfraAzureClient

from . import get_logger

logger = get_logger(__name__)


def sync_guacd_hostname():
    azure_client = InfraAzureClient()

    guacd_ip = azure_client.get_guacd_ip()
    azure_client.update_guacamole_webapp_guacd_hostname(guacd_ip)


if __name__ == "__main__":
    sync_guacd_hostname()
