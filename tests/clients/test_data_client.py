import abc
import io

import pytest

from clients.data_client import AbstractDataClient, WriteMethodsGuardClass, write_method
from clients.data_models import TokenPermissions
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


def test_write_method_raises_if_storage_role_is_missing():
    class AbstractDataClientWithWrite(WriteMethodsGuardClass):
        @abc.abstractmethod
        @write_method
        def guarded(self) -> str: ...

    class DataClient(AbstractDataClientWithWrite):
        def guarded(self) -> str:
            return "ok"

    client = DataClient()
    with pytest.raises(AttributeError, match="missing storage_role attribute"):
        client.guarded()


def test_abstract_data_client_can_write_helpers():
    class DataClient(AbstractDataClient):
        def __init__(self, storage_role: StorageRole):
            self.storage_role = storage_role

        def list_project_dirs(self) -> list[str]:
            return []

        def get_project_documents(self, project_name: str):
            return []

        def get_run_files_folders(
            self,
            project_name: str,
            run_name: str,
            data_type: str,
            folder: str | None,
        ):
            return []

        async def iter_project_run_files_async(
            self,
            project_name: str,
            run_name: str,
            data_type: str | None = None,
        ):
            if False:
                yield None

        def download_run_file(self, filepath: str):
            return io.BytesIO()

        def is_project_data_available(self, project_name: str) -> bool:
            return False

        def generate_run_data_upload_sas(
            self,
            project_name: str,
            run_name: str,
            data_type: str | None = None,
        ):
            return {"url": "", "token": ""}

        def generate_run_data_sas_url(
            self, dir_path: str, file_name: str, is_admin: bool
        ) -> str:
            return ""

        def generate_project_documents_upload_sas_url(
            self, project_name: str, file_name: str
        ) -> str:
            return ""

        def generate_project_documents_sas_url(
            self, dir_path: str, file_name: str
        ) -> str:
            return ""

        def generate_project_directory_token(
            self,
            project_name: str,
            permission: TokenPermissions,
            force_write: bool = False,
        ) -> str:
            return ""

        def init_project_directory(self, project_name: str):
            return None

        def init_run_directory(self, run_name: str, project_name: str) -> None:
            return None

        def rename_project_directory(self, project_name: str, new_name: str) -> None:
            return None

        def rename_run_directory(
            self, run_name: str, project_name: str, new_name: str
        ) -> None:
            return None

    hot_client = DataClient(StorageRole.HOT)
    cool_client = DataClient(StorageRole.COOL)

    assert hot_client.can_write_run_data(is_admin=True) is True
    assert hot_client.can_write_run_data(is_admin=False) is False
    assert cool_client.can_write_run_data(is_admin=True) is False
    assert cool_client.can_write_run_data(is_admin=False) is False

    assert hot_client.can_write_project_documents() is True
    assert cool_client.can_write_project_documents() is False

    hot_client.check_write_permissions({"write": True})
    cool_client.check_write_permissions({"read": True, "list": True})
    cool_client.check_write_permissions({"write": True}, force_write=True)
    with pytest.raises(PermissionError):
        cool_client.check_write_permissions({"write": True})
