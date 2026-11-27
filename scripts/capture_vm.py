"""
Capture VM into a new image
"""

import argparse

from clients.azure import VMAzureClient
from clients.azure.vm import DEFAULT_IMAGE_VERSIONS_TO_KEEP
from clients.guacamole import GuacamoleClient

from . import get_logger
from .purge_image_versions import purge_image_versions

logger = get_logger(__name__)


def capture_vm():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-p",
        "--project",  # pylint: disable=duplicate-code
        help="Name of the project vm you want to capture",
        required=True,
    )
    parser.add_argument("--version", help="Version of this new image")
    parser.add_argument(
        "-k",
        "--kill",
        help="Destroy the VM once the image has been captured",
        action="store_true",
    )
    parser.add_argument(
        "-i",
        "--image",
        help="Name of the image definition to create. If not provided, \
        the default image will be used",
    )

    parser.add_argument(
        "--purge-old-versions",
        help="Once the image has been captured, delete the old versions of this "
        "image definition (those used by a VM are kept)",
        action="store_true",
    )
    parser.add_argument(
        "--keep",
        type=int,
        default=DEFAULT_IMAGE_VERSIONS_TO_KEEP,
        help="Number of most recent versions to keep with --purge-old-versions "
        f"(default: {DEFAULT_IMAGE_VERSIONS_TO_KEEP})",
    )

    args = parser.parse_args()
    if args.keep < 1:
        parser.error("--keep must be at least 1")

    azure_client = VMAzureClient()

    logger.info("Capturing VM...")
    deployment = azure_client.create_new_image_version(
        args.project, args.version, args.image
    )
    if not deployment:
        logger.info("Image already captured")
        return

    deployment_information = deployment.deployment_process.result()

    if deployment_information.properties.provisioning_state == "Succeeded":
        logger.info("Image captured")

        if args.kill:
            kill_vm(azure_client=azure_client, project_name=args.project)

        # After the kill, so that the captured VM no longer protects its version.
        if args.purge_old_versions:
            purge_image_versions(
                azure_client,
                image_definitions=[
                    args.image or azure_client.template_specs_image_definition
                ],
                keep=args.keep,
                dry_run=False,
            )
    else:
        logger.error("Capture failed")


def kill_vm(azure_client: VMAzureClient, project_name: str):
    guacamole_client = GuacamoleClient()
    deletion_resp = azure_client.delete_vm(project_name)
    if deletion_resp == "Succeed":
        logger.info("VM deleted")
    elif deletion_resp == "Failed":
        logger.error("Error deleting vm")

    guacamole_client.delete_connection(project_name)


if __name__ == "__main__":
    capture_vm()
