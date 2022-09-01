from functools import lru_cache

from clients.azure import StorageAzureClient, VMAzureClient
from clients.guacamole import GuacamoleClient


@lru_cache()
def get_vm_azure_client():
    return VMAzureClient()


@lru_cache()
def get_storage_azure_client():
    return StorageAzureClient()


@lru_cache()
def get_guacamole_client():
    return GuacamoleClient()
