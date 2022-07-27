#!/usr/bin/env python3
"""
Delete an Azure VM, its Guacamole connection, and its Azure deployment.
VM & deployment won't raise if they don't exist on Azure. Guacamole connection will.
"""
import argparse

from azure_client import AzureClient
from guacamole_client import GuacamoleClient, GuacamoleConnectionNotFound

from . import get_logger

logger = get_logger(__name__)


def delete_vm():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "project_name", help="Project name related to the VM to delete."
    )
    args = parser.parse_args()

    logger.info("Deleting Azure VM...")
    status = AzureClient().delete_vm(args.project_name)
    logger.info("Azure VM deletion operation finished with satus : %s", status)
    if status == "Failed":
        logger.error("Couldn't delete Azure VM.")
    logger.info("Deleting Azure deployment...")
    AzureClient().delete_deployment(args.project_name)
    logger.info("Deleting Guacamole connection...")
    try:
        GuacamoleClient().delete_connection(args.project_name)
    except GuacamoleConnectionNotFound:
        logger.warning("Did not find a Guacamole connection related to this VM.")
    logger.info("Done")


if __name__ == "__main__":
    delete_vm()
