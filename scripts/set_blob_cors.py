#!/usr/bin/env python3
"""
Create up CORS for Azure fileshare
"""
import argparse

from clients.azure.images import BlobAzureClient

from . import get_logger

logger = get_logger(__name__)


def set_file_share_cors():
    parser = argparse.ArgumentParser()
    parser.add_argument("allowed_origins", help="Allowed origins.")
    args = parser.parse_args()

    azure_client = BlobAzureClient()

    logger.info("Settings CORS allowed origins...")
    azure_client.set_cors_policy(args.allowed_origins)
    logger.info("OK")


if __name__ == "__main__":
    set_file_share_cors()
