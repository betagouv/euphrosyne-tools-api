from functools import lru_cache

from clients.azure import DataAzureClient, VMAzureClient
from clients.guacamole import GuacamoleClient


@lru_cache()
def get_vm_azure_client():
    return VMAzureClient()


@lru_cache()
def get_storage_azure_client():
    return DataAzureClient()


@lru_cache()
def get_guacamole_client():
    return GuacamoleClient()
