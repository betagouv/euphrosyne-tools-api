from io import SEEK_CUR, SEEK_END, SEEK_SET
from unittest import mock

import pytest

from clients.azure.data import AzureFileShareFile


@pytest.fixture
def file():
    return AzureFileShareFile(
        file_service=mock.MagicMock,
        share_name="share_name",
        directory_name="directory_name",
        file_name="file_name",
    )


def test_fetch_content_length_file(file: AzureFileShareFile):
    with mock.patch.object(file, "file_service") as file_service_mk:
        file_service_mk.get_file_properties.return_value.properties.content_length = 444
        assert file.content_length == 444
        assert file._content_length == 444


def test_use_content_length_private_property(file: AzureFileShareFile):
    with mock.patch.object(file, "file_service") as file_service_mk:
        file_service_mk.get_file_properties.return_value.properties.content_length = 444
        file._content_length = 555
        assert file.content_length == 555


def test_read_calls_file_service(file: AzureFileShareFile):
    with mock.patch.object(file, "file_service") as file_service_mk:
        file.read(40)
        file_service_mk.get_file_to_bytes.assert_called_once_with(
            "share_name",
            "directory_name",
            "file_name",
            start_range=0,
            end_range=39,
        )
        assert file._offset == 40


def test_read_when_offset_is_set(file: AzureFileShareFile):
    with mock.patch.object(file, "file_service") as file_service_mk:
        file._offset = 20
        file.read(40)
        file_service_mk.get_file_to_bytes.assert_called_once_with(
            "share_name",
            "directory_name",
            "file_name",
            start_range=20,
            end_range=20 + 40 - 1,
        )
        assert file._offset == 20 + 40


@pytest.mark.parametrize(
    ("seek_param", "value"), ((None, 4), (SEEK_SET, 4), (SEEK_CUR, 14), (SEEK_END, 96))
)
def test_seek_method(seek_param, value: int, file: AzureFileShareFile):
    file._content_length = 100
    file._offset = 10
    file.seek(4, seek_param)
    assert file._offset == value, f"Wrong value for seek param {seek_param}"
