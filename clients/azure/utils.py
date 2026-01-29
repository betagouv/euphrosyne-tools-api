from __future__ import annotations

import asyncio
from typing import AsyncIterator, Iterable, TypeVar, cast

_T = TypeVar("_T")


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
