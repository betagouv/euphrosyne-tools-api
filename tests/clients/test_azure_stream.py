import pytest
from datetime import datetime
from stat import S_IFREG
from clients.azure.stream import iter_files_zip_attr, stream_zip_from_azure_files
from stream_zip import ZIP_AUTO


# Mock StorageStreamDownloader class for testing
class MockStorageStreamDownloader:
    def __init__(self, name, last_modified, size):
        self.name = name
        self.properties = MockProperties(last_modified)
        self.size = size

    def chunks(self):
        # Mock implementation for chunks()
        yield b"chunk1"
        yield b"chunk2"
        yield b"chunk3"


class MockProperties:
    def __init__(self, last_modified):
        self.last_modified = last_modified


# Test iter_files_zip_attr function
def test_iter_files_zip_attr():
    # Create mock files generator
    files = [
        MockStorageStreamDownloader("file1.txt", datetime(2022, 1, 1), 100),
        MockStorageStreamDownloader("file2.txt", datetime(2022, 1, 2), 200),
        MockStorageStreamDownloader("file3.txt", datetime(2022, 1, 3), 300),
    ]

    # Call iter_files_zip_attr function
    result = list(iter_files_zip_attr(files))

    # Check the result
    assert len(result) == 3
    assert result[0][0] == "file1.txt"
    assert result[0][1] == datetime(2022, 1, 1)
    assert result[0][2] == S_IFREG | 0o600
    assert isinstance(result[0][3], type(ZIP_AUTO(files[0].size)))
    assert list(result[0][4]) == [b"chunk1", b"chunk2", b"chunk3"]


# Test stream_zip_from_azure_files function
def test_stream_zip_from_azure_files():
    # Create mock files generator
    files = [
        MockStorageStreamDownloader("file1.txt", datetime(2022, 1, 1), 100),
        MockStorageStreamDownloader("file2.txt", datetime(2022, 1, 2), 200),
        MockStorageStreamDownloader("file3.txt", datetime(2022, 1, 3), 300),
    ]

    # Call stream_zip_from_azure_files function
    result = stream_zip_from_azure_files(files)

    # Check the result
    bytes_list = list(result)
    assert bytes_list
    assert isinstance(bytes_list[0], bytes)
