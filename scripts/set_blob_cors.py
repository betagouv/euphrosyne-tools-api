#!/usr/bin/env python3
"""
Create up CORS for Azure fileshare
"""
import argparse
import asyncio

from clients.azure.images import BlobAzureClient

from . import get_logger

logger = get_logger(__name__)


async def set_file_share_cors():
    parser = argparse.ArgumentParser()
    parser.add_argument("allowed_origins", help="Allowed origins.")
    args = parser.parse_args()

    azure_client = BlobAzureClient()

    logger.info("Settings CORS allowed origins...")
    await azure_client.set_cors_policy(args.allowed_origins)
    logger.info("OK")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(set_file_share_cors())
