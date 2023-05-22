from functools import lru_cache

from clients.azure import (
    ConfigAzureClient,
    DataAzureClient,
    InfraAzureClient,
    VMAzureClient,
)
from clients.guacamole import GuacamoleClient


@lru_cache()
def get_vm_azure_client():
    return VMAzureClient()


@lru_cache()
def get_storage_azure_client():
    return DataAzureClient()


@lru_cache()
def get_config_azure_client():
    return ConfigAzureClient()


@lru_cache()
def get_infra_azure_client():
    return InfraAzureClient()


@lru_cache()
def get_guacamole_client():
    return GuacamoleClient()
