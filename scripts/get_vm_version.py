#!/usr/bin/env python3
"""
Get versions for the VM
"""

from clients.azure import VMAzureClient
from clients.version import Version

from . import get_logger

logger = get_logger(__name__)


def get_version():
    azure_client = VMAzureClient()
    logger.info("Getting version...")

    versions = azure_client._get_image_versions(
        gallery_name=azure_client.template_specs_image_gallery,
        gallery_image_name=azure_client.template_specs_image_definition,
    )

    versions = sorted(map(lambda x: Version(x), versions))
    for idx, version in enumerate(versions):
        if idx == len(versions) - 1:
            logger.info(f"{version} (latest)")
        else:
            logger.info(version)


if __name__ == "__main__":
    get_version()
