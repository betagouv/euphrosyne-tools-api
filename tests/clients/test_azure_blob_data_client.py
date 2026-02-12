from unittest.mock import call, patch

import pytest
from pytest import MonkeyPatch

from clients.azure.blob_data import BlobDataAzureClient
from data_lifecycle.storage_types import StorageRole


def test_init_uses_hot_container(monkeypatch: MonkeyPatch):
    monkeypatch.setenv("AZURE_STORAGE_DATA_CONTAINER", "hot-container")
    monkeypatch.setenv("AZURE_STORAGE_DATA_CONTAINER_COOL", "cool-container")
    with patch(
        "clients.azure.blob_data.BlobAzureClient.__init__", return_value=None
    ) as base_init_mock:
        BlobDataAzureClient()
        BlobDataAzureClient(storage_role=StorageRole.HOT)
    assert base_init_mock.call_args_list == [
        call(container_name="hot-container"),
        call(container_name="hot-container"),
    ]


def test_init_uses_cool_container(monkeypatch: MonkeyPatch):
    monkeypatch.setenv("AZURE_STORAGE_DATA_CONTAINER", "hot-container")
    monkeypatch.setenv("AZURE_STORAGE_DATA_CONTAINER_COOL", "cool-container")
    with patch(
        "clients.azure.blob_data.BlobAzureClient.__init__", return_value=None
    ) as base_init_mock:
        BlobDataAzureClient(storage_role=StorageRole.COOL)
    base_init_mock.assert_called_once_with(container_name="cool-container")


def test_init_raises_if_container_not_configured(monkeypatch: MonkeyPatch):
    monkeypatch.delenv("AZURE_STORAGE_DATA_CONTAINER", raising=False)
    with patch(
        "clients.azure.blob_data.BlobAzureClient.__init__", return_value=None
    ) as base_init_mock:
        with pytest.raises(
            ValueError,
            match="AZURE_STORAGE_DATA_CONTAINER environment variable is not set",
        ):
            BlobDataAzureClient()
    base_init_mock.assert_not_called()


def test_init_raises_for_unsupported_storage_role():
    with patch(
        "clients.azure.blob_data.BlobAzureClient.__init__", return_value=None
    ) as base_init_mock:
        with pytest.raises(ValueError, match="Unsupported storage role"):
            BlobDataAzureClient(storage_role="WARM")  # type: ignore[arg-type]
    base_init_mock.assert_not_called()
