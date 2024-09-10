""" 
Streaming zip files from Azure file shares.

This module provides functions for streaming and zipping files from Azure file shares.
"""

from typing import Any, AsyncGenerator, Coroutine, Generator
from stat import S_IFREG
import os
from azure.storage.fileshare._download import StorageStreamDownloader

from stream_zip import async_stream_zip, ZIP_AUTO, Method  # type: ignore[attr-defined]
import asyncio
import logging
import datetime

logger = logging.getLogger(__name__)

StreamZipFile = tuple[str, datetime.datetime, int, Method, AsyncGenerator[bytes, None]]


async def iterate_blocking(iterator):
    loop = asyncio.get_running_loop()
    DONE = object()
    while True:
        obj = await loop.run_in_executor(None, next, iterator, DONE)
        if obj is DONE:
            break
        yield obj


async def iter_files_zip_attr_async(
    files: Generator[Coroutine[StorageStreamDownloader, None, None], Any, None]
):
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

    async def contents(stream_obj: StorageStreamDownloader):
        async for chunk in iterate_blocking(stream_obj.chunks()):
            yield chunk

    async def download_file_async(
        stream_obj_coro: Coroutine[StorageStreamDownloader, None, None]
    ) -> StreamZipFile:

        stream_obj: StorageStreamDownloader = await stream_obj_coro  # type: ignore[func-returns-value, assignment]
        return (
            stream_obj.name,  # type: ignore[attr-defined]
            stream_obj.properties.last_modified,  # type: ignore[attr-defined]
            S_IFREG | 0o600,
            ZIP_AUTO(stream_obj.size),  # type: ignore[attr-defined]
            contents(stream_obj),
        )

    tasks: list[asyncio.Task[Coroutine[StreamZipFile, None, None]]] = []

    limit = 50  # default
    try:
        limit = int(os.environ["FILESHARE_DOWNLOAD_CONCURRENT_FILES_LIMIT"])
    except (ValueError, KeyError) as e:
        # Keep default if not in env or wrong value
        if isinstance(e, ValueError):
            logger.error(e)

    while True:
        for _ in range(limit):
            try:
                # Schedule the file download task
                file = await anext(files)  # type: ignore[call-overload] # noqa: F821
                tasks.append(
                    asyncio.create_task(download_file_async(file))  # type: ignore[arg-type]
                )
            except StopAsyncIteration:
                # Exit for loop when no more files to process
                break

        # If no tasks were added, break out of the loop
        if not tasks:
            break

        # Process completed tasks and yield results
        for f in asyncio.as_completed(tasks):
            file = await f
            yield file

        # Reset tasks for the next batch
        tasks.clear()


async def stream_zip_from_azure_files_async(
    files: Generator[Coroutine[StorageStreamDownloader, None, None], Any, None]
):
    """
    Streams a zip file from Azure files.

    Args:
        files: A generator of `StorageStreamDownloader` objects.

    Returns:
        A streaming zip file.

    """
    async for b in async_stream_zip(iter_files_zip_attr_async(files)):
        yield b
