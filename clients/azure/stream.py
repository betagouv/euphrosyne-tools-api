""" 
Streaming zip files from Azure file shares.

This module provides functions for streaming and zipping files from Azure file shares.
"""

from typing import Any, Generator
from stat import S_IFREG

from azure.storage.fileshare._download import StorageStreamDownloader

from stream_zip import stream_zip, ZIP_AUTO


def iter_files_zip_attr(files: Generator[StorageStreamDownloader, Any, None]):
    """
    Iterates over a generator of `StorageStreamDownloader` objects and
    yields file attributes required for zipping.

    Args:
        files: A generator of `StorageStreamDownloader` objects.

    Yields:
        A tuple containing the file attributes required for zipping:
        - File name
        - Last modified timestamp
        - File mode (S_IFREG | 0o600)
        - File size (ZIP_AUTO)
        - File contents (generator)

    """

    def contents(stream_obj: StorageStreamDownloader):
        for chunk in stream_obj.chunks():
            yield chunk

    for stream_obj in files:
        yield (
            stream_obj.name,
            stream_obj.properties.last_modified,
            S_IFREG | 0o600,
            ZIP_AUTO(stream_obj.size),
            contents(stream_obj),
        )


def stream_zip_from_azure_files(files: Generator[StorageStreamDownloader, Any, None]):
    """
    Streams a zip file from Azure files.

    Args:
        files: A generator of `StorageStreamDownloader` objects.

    Returns:
        A streaming zip file.

    """
    return stream_zip(iter_files_zip_attr(files))
