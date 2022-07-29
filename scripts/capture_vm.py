"""
Capture VM into a new image
"""

import argparse

from azure_client import AzureClient

from . import get_logger

logger = get_logger(__name__)

def capture_vm():
    parser = argparse.ArgumentParser()
    parser.add_argument("--vm", help="Name of the VM you want to capture", required=True)
    parser.add_argument("--version", help="Version of this new image", required=True)

    args = parser.parse_args()

    azure_client = AzureClient()

    logger.info("Capturing VM...")
    deployment = azure_client.create_new_image_version(args.vm, args.version)
    if not deployment:
        logger.info("Image already captured")
        return

    deployment_information = deployment.deployment_process.result()

    if deployment_information.properties.provisioning_state == "Succeeded":
        logger.info("Image captured")
    else:
        logger.error("Capture failed")

if __name__ == "__main__":
    capture_vm()
