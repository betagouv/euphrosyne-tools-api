import asyncio
from datetime import datetime
from stat import S_IFREG

from clients.azure.stream import (iter_files_zip_attr_async,
                                  stream_zip_from_azure_files_async)


# Mock StorageStreamDownloader class for testing
class MockStorageStreamDownloader:
    def __init__(self, name, last_modified, size):
        self.name = name
        self.properties = MockProperties(last_modified)
        self.size = size

    def chunks(self):
        # Mock implementation for chunks()
        yield b"chunk1"


class MockProperties:
    def __init__(self, last_modified):
        self.last_modified = last_modified


async def get_files_async():
    async def get_file_async(f):
        return f

    for file in [
        MockStorageStreamDownloader("file1.txt", datetime(2022, 1, 1), 100),
        MockStorageStreamDownloader("file2.txt", datetime(2022, 1, 2), 200),
        MockStorageStreamDownloader("file3.txt", datetime(2022, 1, 3), 300),
    ]:
        yield get_file_async(file)


def async_gen_to_sync(generator):
    while True:
        try:
            n = generator.__anext__()
            yield asyncio.run(n)
        except StopAsyncIteration:
            break


def test_iter_files_zip_attr():
    # Call iter_files_zip_attr function
    result = list(async_gen_to_sync(iter_files_zip_attr_async(get_files_async())))

    # Check the result
    assert len(result) == 3
    assert result[0][0] == "file1.txt"
    assert result[0][1] == datetime(2022, 1, 1)
    assert result[0][2] == S_IFREG | 0o600
    assert list(async_gen_to_sync(result[0][4])) == [b"chunk1"]


def test_stream_zip_from_azure_files():
    # Call stream_zip_from_azure_files function
    generator = stream_zip_from_azure_files_async(get_files_async())

    # Check the result
    bytes_list = list(async_gen_to_sync(generator))
    assert bytes_list
    assert isinstance(bytes_list[0], bytes)
