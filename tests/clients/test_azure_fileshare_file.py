import io
from io import SEEK_CUR, SEEK_END, SEEK_SET
from unittest import mock

import h5py  # type: ignore[import-untyped]
import pytest
from azure.storage.fileshare import ShareFileClient

from clients.azure.data import AzureFileShareFile


def make_file(content: bytes) -> AzureFileShareFile:
    file_client = mock.MagicMock(spec=ShareFileClient)
    file_client.get_file_properties.return_value.size = len(content)

    def download_file(offset, length):
        return mock.Mock(readall=lambda: content[offset : offset + length])

    file_client.download_file.side_effect = download_file
    return AzureFileShareFile(file_client)


@pytest.fixture
def file():
    return make_file(bytes(range(100)))


@pytest.fixture
def file_client(file) -> mock.MagicMock:
    return file.file_client


def test_fetch_content_length_file(
    file: AzureFileShareFile, file_client: mock.MagicMock
):
    assert file.content_length == 100
    assert file.content_length == 100
    file_client.get_file_properties.assert_called_once_with()


def test_read_calls_file_client(file: AzureFileShareFile, file_client: mock.MagicMock):
    assert file.read(40) == bytes(range(40))
    file_client.download_file.assert_called_once_with(offset=0, length=40)
    assert file.tell() == 40


def test_read_when_offset_is_set(file: AzureFileShareFile, file_client: mock.MagicMock):
    file.seek(20)
    assert file.read(40) == bytes(range(20, 60))
    file_client.download_file.assert_called_once_with(offset=20, length=40)
    assert file.tell() == 60


@pytest.mark.parametrize("size", [-1, -2, None])
def test_read_to_end(file: AzureFileShareFile, file_client: mock.MagicMock, size):
    file.seek(20)
    assert file.read(size) == bytes(range(20, 100))
    file_client.download_file.assert_called_once_with(offset=20, length=80)
    assert file.tell() == 100
    assert file.read() == b""
    file_client.download_file.assert_called_once()


def test_read_without_size(file: AzureFileShareFile):
    assert file.read() == bytes(range(100))
    assert file.tell() == 100


def test_read_zero_does_not_call_azure(
    file: AzureFileShareFile, file_client: mock.MagicMock
):
    assert file.read(0) == b""
    assert file.tell() == 0
    file_client.get_file_properties.assert_not_called()
    file_client.download_file.assert_not_called()


def test_read_past_eof(file: AzureFileShareFile, file_client: mock.MagicMock):
    file.seek(90)
    assert file.read(40) == bytes(range(90, 100))
    assert file.tell() == 100
    assert file.read(40) == b""
    file_client.download_file.assert_called_once_with(offset=90, length=10)


def test_read_after_seek_beyond_eof(
    file: AzureFileShareFile, file_client: mock.MagicMock
):
    file.seek(110)
    assert file.read() == b""
    assert file.tell() == 110
    file_client.download_file.assert_not_called()


def test_read_empty_file():
    file = make_file(b"")
    file_client = file.file_client
    assert file.read() == b""
    assert file.tell() == 0
    file_client.download_file.assert_not_called()


def test_read_updates_offset_by_bytes_received(
    file: AzureFileShareFile, file_client: mock.MagicMock
):
    file_client.download_file.side_effect = None
    file_client.download_file.return_value.readall.return_value = b"short"
    assert file.read(40) == b"short"
    assert file.tell() == 5


def test_readinto(file: AzureFileShareFile):
    file.seek(95)
    buffer = bytearray(10)
    assert file.readinto(buffer) == 5
    assert buffer == bytes(range(95, 100)) + b"\x00" * 5
    assert file.tell() == 100


def test_repeated_range_uses_cache(
    file: AzureFileShareFile, file_client: mock.MagicMock
):
    content = file.read(10)
    file.seek(0)
    assert file.read(10) == content
    assert file.tell() == 10
    file_client.download_file.assert_called_once()


@pytest.mark.parametrize(
    ("seek_param", "offset", "value"),
    [
        (None, 4, 4),
        (SEEK_SET, 4, 4),
        (SEEK_CUR, 4, 14),
        (SEEK_END, -4, 96),
        (SEEK_END, 4, 104),
    ],
)
def test_seek_method(seek_param, offset, value, file: AzureFileShareFile):
    file.seek(10)
    assert file.seek(offset, seek_param) == value
    assert file.tell() == value


@pytest.mark.parametrize(
    ("offset", "whence"), [(-1, SEEK_SET), (-11, SEEK_CUR), (-101, SEEK_END), (0, 3)]
)
def test_invalid_seek_preserves_position(file: AzureFileShareFile, offset, whence):
    file.seek(10)
    with pytest.raises(ValueError):
        file.seek(offset, whence)
    assert file.tell() == 10


def test_hdf5_reads_through_file_client():
    buffer = io.BytesIO()
    with h5py.File(buffer, "w") as hdf5:
        hdf5.create_dataset("values", data=list(range(1000)))
        hdf5.attrs["title"] = "scientific data"

    file = make_file(buffer.getvalue())
    file_client = file.file_client
    with h5py.File(file, "r") as hdf5:
        assert hdf5.attrs["title"] == "scientific data"
        assert hdf5["values"][10:15].tolist() == list(range(10, 15))

    assert file_client.download_file.called
    file_client.get_file_properties.assert_called_once_with()
