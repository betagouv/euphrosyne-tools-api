#!/usr/bin/env python3
"""
Create a Azure VM and a Guacamole connection
"""
import argparse

from clients.azure import DataAzureClient

from . import get_logger

logger = get_logger(__name__)


def set_file_share_cors():
    parser = argparse.ArgumentParser()
    parser.add_argument("allowed_origins", help="Allowed origins.")
    args = parser.parse_args()

    azure_client = DataAzureClient()

    logger.info("Settings CORS allowed origins...")
    azure_client.set_fileshare_cors_policy(args.allowed_origins)
    logger.info("OK")


if __name__ == "__main__":
    set_file_share_cors()
