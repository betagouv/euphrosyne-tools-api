#!/usr/bin/env python3
import argparse

from clients.azure import ConfigAzureClient
from clients.azure.config import VMSizes

from . import get_logger

logger = get_logger(__name__)


def set_project_vm_size():
    parser = argparse.ArgumentParser()
    parser.add_argument("project_name")
    parser.add_argument("vm_size")
    args = parser.parse_args()

    azure_client = ConfigAzureClient()

    azure_client.set_project_vm_size(args.project_name, VMSizes[args.vm_size])
    print(azure_client.get_project_vm_size(args.project_name))


if __name__ == "__main__":
    set_project_vm_size()
