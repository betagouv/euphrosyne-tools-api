import abc

import pytest

from clients.data_client import WriteMethodsGuardClass, write_method
from data_lifecycle.storage_types import StorageRole


def test_data_client_write_methods():
    """Test that write methods raise PermissionError for non-HOT storage roles."""

    class AbstractDataClient(WriteMethodsGuardClass):

        @abc.abstractmethod
        def list_project_dirs(self) -> list[str]: ...

        @abc.abstractmethod
        @write_method
        def generate_project_documents_upload_sas_url(self) -> str: ...

    class DataClient(AbstractDataClient):
        def __init__(self, storage_role: StorageRole):
            self.storage_role = storage_role

        def list_project_dirs(self) -> list[str]:
            return ["gelato", "vanilla"]

        def generate_project_documents_upload_sas_url(
            self,
        ) -> str:
            return "returned_from_write_method"

    cold_client = DataClient(StorageRole.COOL)
    hot_client = DataClient(StorageRole.HOT)

    assert cold_client.list_project_dirs() == ["gelato", "vanilla"]
    assert hot_client.list_project_dirs() == ["gelato", "vanilla"]

    with pytest.raises(PermissionError):
        cold_client.generate_project_documents_upload_sas_url()

    assert (
        hot_client.generate_project_documents_upload_sas_url()
        == "returned_from_write_method"
    )
