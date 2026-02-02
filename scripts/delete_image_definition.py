#!/usr/bin/env python3
"""
Delete an Azure image definition
"""

import argparse
import time

from clients.azure.vm import VMAzureClient, ImageDefinitionNotFound

from . import get_logger

logger = get_logger(__name__)


def create_vm():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        dest="image_definition",
        help="Image definition to delete.",
    )
    args = parser.parse_args()

    poller = None

    confirm = input(
        f"Are you sure you want to delete image definition {args.image_definition}? (y/n): "
    )
    if confirm.lower() != "y":
        logger.info("Aborting.")
        return

    try:
        logger.info("Deleting image definition versions...")
        pollers = VMAzureClient().delete_vm_image_definition_versions(
            args.image_definition
        )
        logger.info("%s image definition versions to delete.", len(pollers))
        for version, poller in pollers:
            logger.info("Deleting image definition version %s.", version)
            while not poller.done():
                poller.result()
            logger.info("Done deleting image definition version %s.", version)

        logger.info("Deleting image definition...")
        time.sleep(
            7
        )  # sleep a bit, otheriwse azure will say image versions are not deleted yet.

        poller = VMAzureClient().delete_vm_image_definition(args.image_definition)

    except ImageDefinitionNotFound:
        logger.error("Image definition %s not found.", args.image_definition)
        logger.error(
            "Choices are: %s", ", ".join(VMAzureClient().list_vm_image_definitions())
        )

    if poller:
        poller.result()
        logger.info("Done.")


if __name__ == "__main__":
    create_vm()
