from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterable
from pathlib import Path
from typing import TypeVar, cast
from urllib.parse import quote

_T = TypeVar("_T")

FORCED_DOWNLOAD_FILE_EXTENSIONS = {".h5", ".hdf5"}


def get_download_content_disposition(file_name: str) -> str | None:
    """Return an attachment disposition for file types that must be downloaded."""
    if Path(file_name).suffix.lower() not in FORCED_DOWNLOAD_FILE_EXTENSIONS:
        return None

    quoted_file_name = quote(file_name, safe="")
    if quoted_file_name != file_name:
        return f"attachment; filename*=utf-8''{quoted_file_name}"
    return f'attachment; filename="{file_name}"'


async def iterate_blocking(iterator: Iterable[_T]) -> AsyncIterator[_T]:
    """Iterate a blocking iterator in a thread to avoid blocking the event loop."""
    loop = asyncio.get_running_loop()
    done = object()
    iterator = iter(iterator)
    while True:
        item = await loop.run_in_executor(None, next, iterator, done)
        if item is done:
            break
        yield cast(_T, item)
