import os
from functools import lru_cache

from clients.azure import (
    BlobDataAzureClient,
    ConfigAzureClient,
    DataAzureClient,
    InfraAzureClient,
    VMAzureClient,
)
from clients.azure.images import ImageStorageClient
from clients.guacamole import GuacamoleClient


@lru_cache()
def get_vm_azure_client():
    return VMAzureClient()


@lru_cache()
def get_project_data_client():
    backend = os.getenv("DATA_BACKEND")
    if not backend:
        raise ValueError("DATA_BACKEND environment variable is not set")
    backend = backend.strip().lower()
    if backend == "azure_blob":
        return BlobDataAzureClient()
    if backend == "azure_fileshare":
        return DataAzureClient()
    raise ValueError(
        f"Invalid DATA_BACKEND value: {backend!r}. Allowed values are 'azure_blob' and 'azure_fileshare'."
    )


@lru_cache()
def get_config_azure_client():
    return ConfigAzureClient()


@lru_cache()
def get_infra_azure_client():
    return InfraAzureClient()


@lru_cache()
def get_guacamole_client():
    return GuacamoleClient()


@lru_cache()
def get_image_storage_client(project_name: str) -> ImageStorageClient:
    return ImageStorageClient(project_slug=project_name)
