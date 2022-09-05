import os

from azure.core.credentials import TokenCredential
from azure.identity import DefaultAzureCredential
from azure.mgmt.storage import StorageManagementClient
from dotenv import load_dotenv

load_dotenv()


class BaseStorageAzureClient:
    """Base class for all Azure storage related operations."""

    def __init__(self):
        credentials = DefaultAzureCredential()
        self.resource_group_name = os.environ["AZURE_RESOURCE_GROUP_NAME"]
        self.storage_account_name = os.environ["AZURE_STORAGE_ACCOUNT"]

        self._storage_key = _get_storage_key(
            credentials,
            os.environ["AZURE_SUBSCRIPTION_ID"],
            resource_group_name=self.resource_group_name,
            storage_account_name=self.storage_account_name,
        )

        # pylint: disable=line-too-long,consider-using-f-string
        self._storage_connection_string = "DefaultEndpointsProtocol=https;AccountName={};AccountKey={};EndpointSuffix=core.windows.net".format(
            self.storage_account_name, self._storage_key
        )


def _get_storage_key(
    credential: TokenCredential,
    subscription_id: str,
    resource_group_name: str,
    storage_account_name: str,
):
    """Fetches a storage account key to use as a credential"""
    storage_mgmt_client = StorageManagementClient(credential, subscription_id)
    key = (
        storage_mgmt_client.storage_accounts.list_keys(
            resource_group_name, storage_account_name
        )
        .keys[0]
        .value
    )

    return key
