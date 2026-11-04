from unittest.mock import patch

import pytest
from azure.mgmt.storage.models import StorageAccountListKeysResult

from clients.azure._storage import _get_storage_key


def _get_key_with_result(result: StorageAccountListKeysResult):
    with patch("clients.azure._storage.StorageManagementClient") as client_mock:
        client_mock.return_value.storage_accounts.list_keys.return_value = result
        return _get_storage_key(
            credential=object(),  # type: ignore[arg-type]
            subscription_id="subscription",
            resource_group_name="resource_group",
            storage_account_name="storageaccount",
        )


def test_get_storage_key_returns_first_key():
    result = StorageAccountListKeysResult(
        {
            "keys": [
                {"keyName": "key1", "value": "secret1", "permissions": "FULL"},
                {"keyName": "key2", "value": "secret2", "permissions": "FULL"},
            ]
        }
    )

    assert _get_key_with_result(result) == "secret1"


@pytest.mark.parametrize("payload", [{}, {"keys": []}])
def test_get_storage_key_raises_when_no_key(payload: dict):
    with pytest.raises(ValueError, match="missing key"):
        _get_key_with_result(StorageAccountListKeysResult(payload))
