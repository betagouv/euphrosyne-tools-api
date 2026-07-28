from io import SEEK_CUR, SEEK_END, SEEK_SET, BytesIO
from types import SimpleNamespace
from unittest import mock
from zipfile import ZipFile

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
        file._content_length = 100
        file_service_mk.get_file_to_bytes.return_value.content = b"x" * 40
        file_service_mk.get_file_to_bytes.return_value.properties.content_length = 100
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
        file._content_length = 100
        file._offset = 20
        file_service_mk.get_file_to_bytes.return_value.content = b"x" * 40
        file_service_mk.get_file_to_bytes.return_value.properties.content_length = 100
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
    ("offset", "seek_param", "value"),
    (
        (4, None, 4),
        (4, SEEK_SET, 4),
        (4, SEEK_CUR, 14),
        (-4, SEEK_END, 96),
        (4, SEEK_END, 104),
    ),
)
def test_seek_method(
    offset: int,
    seek_param: int | None,
    value: int,
    file: AzureFileShareFile,
):
    file._content_length = 100
    file._offset = 10
    if seek_param is None:
        file.seek(offset)
    else:
        file.seek(offset, seek_param)
    assert file._offset == value, f"Wrong value for seek param {seek_param}"


def test_partial_read_advances_by_returned_bytes_and_then_returns_eof(
    file: AzureFileShareFile,
):
    file._content_length = 5
    with mock.patch.object(file, "file_service") as file_service_mk:
        file_service_mk.get_file_to_bytes.return_value.content = b"hello"
        file_service_mk.get_file_to_bytes.return_value.properties.content_length = 5

        assert file.read(1024) == b"hello"
        assert file.tell() == 5
        assert file.read(1024) == b""

    file_service_mk.get_file_to_bytes.assert_called_once_with(
        "share_name",
        "directory_name",
        "file_name",
        start_range=0,
        end_range=4,
    )


def test_multiple_reads_use_the_full_file_length_not_the_range_length(
    file: AzureFileShareFile,
):
    content = b"0123456789"
    file._content_length = len(content)
    with mock.patch.object(file, "file_service") as file_service_mk:
        file_service_mk.get_file_to_bytes.side_effect = (
            lambda *_args, start_range, end_range: SimpleNamespace(
                content=content[start_range : end_range + 1],
                properties=SimpleNamespace(content_length=end_range - start_range + 1),
            )
        )

        assert file.read(4) == b"0123"
        assert file.read(4) == b"4567"
        assert file.read(4) == b"89"
        assert file.read(4) == b""

    assert file.tell() == len(content)
    assert file._content_length == len(content)


def test_seek_rejects_negative_positions_and_invalid_whence(
    file: AzureFileShareFile,
):
    file._content_length = 100

    with pytest.raises(ValueError, match="Negative seek position"):
        file.seek(-1, SEEK_SET)
    with pytest.raises(ValueError, match="Invalid whence"):
        file.seek(0, 999)


def test_file_supports_the_seek_and_partial_read_contract_used_by_zipfile() -> None:
    archive_buffer = BytesIO()
    with ZipFile(archive_buffer, "w") as archive:
        archive.writestr("content.txt", b"hello")
    content = archive_buffer.getvalue()

    class FakeFileService:
        def get_file_properties(self, *_args):
            return SimpleNamespace(
                properties=SimpleNamespace(content_length=len(content))
            )

        def get_file_to_bytes(
            self,
            *_args,
            start_range: int,
            end_range: int,
        ):
            return SimpleNamespace(
                content=content[start_range : end_range + 1],
                properties=SimpleNamespace(content_length=len(content)),
            )

    source = AzureFileShareFile(
        file_service=FakeFileService(),  # type: ignore[arg-type]
        share_name="share",
        directory_name="directory",
        file_name="source.xlsx",
    )

    with ZipFile(source) as archive:
        assert archive.read("content.txt") == b"hello"
