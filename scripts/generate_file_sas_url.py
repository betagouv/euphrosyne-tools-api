#!/usr/bin/env python3
"""
Create a Azure VM and a Guacamole connection
"""
import argparse

from azure_client import AzureClient


def generate_file_sas_url():
    parser = argparse.ArgumentParser()
    parser.add_argument("dir_path", help="Directory path in the file storage.")
    parser.add_argument("file_name", help="File name.")
    args = parser.parse_args()

    azure_client = AzureClient()

    url = azure_client.generate_run_data_sas_url(
        dir_path=args.dir_path,
        file_name=args.file_name,
        is_admin=True,
    )
    print(url)


if __name__ == "__main__":
    generate_file_sas_url()
