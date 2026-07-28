from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable

from clients.data_models import ProjectFileOrDirectory

TRAUPIXE_EXTENSION = ".xlsx"
TRAUPIXE_NAME_MARKER = "traupixe"
MAX_SOURCE_SIZE_BYTES = 100 * 1024 * 1024


def is_traupixe_workbook(entry: ProjectFileOrDirectory) -> bool:
    """Return whether storage metadata describes a TRAUPIXE candidate."""

    return (
        entry.type == "file"
        and entry.name.casefold().endswith(TRAUPIXE_EXTENSION)
        and TRAUPIXE_NAME_MARKER in entry.name.casefold()
        and entry.size is not None
        and 0 < entry.size <= MAX_SOURCE_SIZE_BYTES
    )


def select_traupixe_workbooks(
    entries: Iterable[ProjectFileOrDirectory],
) -> list[ProjectFileOrDirectory]:
    """Filter and order storage metadata without downloading any workbook."""

    return sorted(filter(is_traupixe_workbook, entries), key=_sort_key)


def _sort_key(entry: ProjectFileOrDirectory) -> tuple[float, str, str, str]:
    return (
        -_timestamp(entry.last_modified),
        entry.name.casefold(),
        entry.name,
        entry.path,
    )


def _timestamp(value: datetime | None) -> float:
    if value is None:
        return float("-inf")
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).timestamp()
