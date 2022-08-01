"""
Capture VM into a new image
"""

import argparse

from azure_client import AzureClient
from guacamole_client import GuacamoleClient

from . import get_logger

logger = get_logger(__name__)


def capture_vm():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-p",
        "--project",
        help="Name of the project vm you want to capture",
        required=True,
    )
    parser.add_argument("--version", help="Version of this new image", required=True)
    parser.add_argument(
        "-k",
        "--kill",
        help="Destroy the VM once the image has been captured",
        action="store_true",
    )

    args = parser.parse_args()

    azure_client = AzureClient()

    logger.info("Capturing VM...")
    deployment = azure_client.create_new_image_version(args.project, args.version)
    if not deployment:
        logger.info("Image already captured")
        return

    deployment_information = deployment.deployment_process.result()

    if deployment_information.properties.provisioning_state == "Succeeded":
        logger.info("Image captured")

        if args.kill:
            kill_vm(azure_client=azure_client, project_name=args.project)
    else:
        logger.error("Capture failed")


def kill_vm(azure_client: AzureClient, project_name: str):
    guacamole_client = GuacamoleClient()
    deletion_resp = azure_client.delete_vm(project_name)
    if deletion_resp == "Succeed":
        logger.info("VM deleted")
    elif deletion_resp == "Failed":
        logger.error("Error deleting vm")

    guacamole_client.delete_connection(project_name)


if __name__ == "__main__":
    capture_vm()
