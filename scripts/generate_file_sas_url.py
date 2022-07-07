#!/usr/bin/env python3
"""
Create a Azure VM and a Guacamole connection
"""
import argparse

from azure_client import AzureClient


def generate_file_sas_url():
    parser = argparse.ArgumentParser()
    parser.add_argument("project_name", help="Project name.")
    parser.add_argument("run_name", help="Run name.")
    parser.add_argument("data_type", help="Run data type: processed_data or raw_data.")
    parser.add_argument("file_name", help="File name.")
    args = parser.parse_args()

    azure_client = AzureClient()

    url = azure_client.generate_run_data_sas_url(
        project_name=args.project_name,
        run_name=args.run_name,
        data_type=args.data_type,
        file_name=args.file_name,
        is_admin=True,
    )
    print(url)


if __name__ == "__main__":
    generate_file_sas_url()
