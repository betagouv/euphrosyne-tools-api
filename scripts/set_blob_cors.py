#!/usr/bin/env python3
"""
Create up CORS for Azure Blob Storage.
"""

import argparse

from clients.azure.blob import BlobAzureClient

from . import get_logger

logger = get_logger(__name__)


def set_blob_cors():
    parser = argparse.ArgumentParser()
    parser.add_argument("allowed_origins", help="Allowed origins.")
    parser.add_argument("container_name", help="Azure Blob container name.")
    args = parser.parse_args()

    azure_client = BlobAzureClient(container_name=args.container_name)

    logger.info("Settings CORS allowed origins...")
    azure_client.set_cors_policy(args.allowed_origins)
    logger.info("OK")


if __name__ == "__main__":
    set_blob_cors()
