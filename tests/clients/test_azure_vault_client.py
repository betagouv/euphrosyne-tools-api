from unittest.mock import patch

import pytest

from clients.azure.vault import VaultClient


def test_vault_client_initing_secret_client(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("AZURE_RESOURCE_GROUP_NAME", "resource_group_name")
    monkeypatch.setenv("AZURE_SUBSCRIPTION_ID", "ID")
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "storageaccount")
    vault_name = "vault_name"
    with patch("clients.azure.vault.SecretClient") as mock:
        VaultClient(vault_name)
        assert mock.call_args[1]["vault_url"] == f"https://{vault_name}.vault.azure.net"
