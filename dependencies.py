from functools import lru_cache

from azure_client import AzureClient
from guacamole_client import GuacamoleClient


@lru_cache()
def get_azure_client():
    return AzureClient()


@lru_cache()
def get_guacamole_client():
    return GuacamoleClient()
