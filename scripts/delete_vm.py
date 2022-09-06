#!/usr/bin/env python3
"""
Delete an Azure VM, its Guacamole connection, and its Azure deployment.
VM & deployment won't raise if they don't exist on Azure. Guacamole connection will.
"""
import argparse

from clients.azure import VMAzureClient
from clients.guacamole import GuacamoleClient, GuacamoleConnectionNotFound

from . import get_logger

logger = get_logger(__name__)


def delete_vm_script():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "project_name", help="Project name related to the VM to delete."
    )
    args = parser.parse_args()
    delete_vm(args.project_name)


def delete_vm(
    project_name: str, azure_client=VMAzureClient(), guacamole_client=GuacamoleClient()
):
    logger.info("%s - Deleting Azure VM...", project_name)
    status = azure_client.delete_vm(project_name)
    logger.info(
        "%s - Azure VM deletion operation finished with satus : %s",
        project_name,
        status,
    )
    if status == "Failed":
        logger.error("%s - Couldn't delete Azure VM.", project_name)
    logger.info("%s - Deleting Azure deployment...", project_name)
    azure_client.delete_deployment(project_name)
    logger.info("%s - Deleting Guacamole connection...", project_name)
    try:
        guacamole_client.delete_connection(project_name)
    except GuacamoleConnectionNotFound:
        logger.warning(
            "%s - Did not find a Guacamole connection related to this VM.", project_name
        )
    logger.info("%s - Done deleting vm", project_name)


if __name__ == "__main__":
    delete_vm_script()
