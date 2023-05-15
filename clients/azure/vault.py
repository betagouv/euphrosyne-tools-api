import os

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from dotenv import load_dotenv

load_dotenv()


class VaultClient:
    """Manage access to vault secrets in Azure."""

    def __init__(self, vault_name: str):
        vault_url = f"https://{vault_name}.vault.azure.net"
        self.resource_group_name = os.environ["AZURE_RESOURCE_GROUP_NAME"]
        credential = DefaultAzureCredential()

        self._client = SecretClient(vault_url=vault_url, credential=credential)

    def get_secret_value(self, secret_name: str):
        return self._client.get_secret(secret_name)
