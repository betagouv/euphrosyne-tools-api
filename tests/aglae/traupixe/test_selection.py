from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Literal

import pytest

from aglae.traupixe import (
    MAX_SOURCE_SIZE_BYTES,
    is_traupixe_workbook,
    select_traupixe_workbooks,
)
from clients.data_models import ProjectFileOrDirectory

NOW = datetime(2026, 7, 28, 10, 0, tzinfo=timezone.utc)


def _entry(
    name: str,
    *,
    path: str | None = None,
    size: int | None = 1,
    last_modified: datetime | None = NOW,
    entry_type: Literal["file", "directory"] = "file",
) -> ProjectFileOrDirectory:
    return ProjectFileOrDirectory(
        name=name,
        path=path or f"projects/project-01/runs/run-01/raw_data/{name}",
        size=size,
        last_modified=last_modified,
        type=entry_type,
    )


@pytest.mark.parametrize(
    "name",
    [
        "TRAUPIXE-TACT_MAT-X0.xlsx",
        "traupixe-20260615-SRV.XLSX",
        "CONSO_IV_TRAUPIXE-20260605.xlsx",
        "CONSO_OS_TRAUPIXE-20260605.xlsx",
        "STD_TRAUPIXE-20260605.xlsx",
    ],
)
def test_accepts_supported_filename_variants(name: str) -> None:
    assert is_traupixe_workbook(_entry(name))


@pytest.mark.parametrize(
    "entry",
    [
        _entry("TRAUPIXE.xls"),
        _entry("results.xlsx"),
        _entry("TRAUPIXE.xlsx", size=0),
        _entry("TRAUPIXE.xlsx", size=None),
        _entry("TRAUPIXE.xlsx", size=MAX_SOURCE_SIZE_BYTES + 1),
        _entry("TRAUPIXE.xlsx", entry_type="directory"),
    ],
)
def test_rejects_metadata_outside_the_detection_rule(
    entry: ProjectFileOrDirectory,
) -> None:
    assert not is_traupixe_workbook(entry)


def test_selects_candidates_without_interpreting_filename_segments() -> None:
    newest = _entry(
        "anything_TRAUPIXE_anything.xlsx",
        path="processed/arbitrary.xlsx",
        last_modified=NOW + timedelta(minutes=1),
    )
    older = _entry(
        "TRAUPIXE-no-common-pattern.xlsx",
        path="raw/arbitrary.xlsx",
    )

    result = select_traupixe_workbooks(
        [
            older,
            _entry("unrelated.xlsx"),
            newest,
        ]
    )

    assert result == [newest, older]


def test_selection_is_empty_when_no_candidate_matches() -> None:
    result = select_traupixe_workbooks([_entry("unrelated.xlsx")])

    assert result == []


def test_equal_dates_have_a_stable_name_and_path_order() -> None:
    second = _entry("TRAUPIXE-b.xlsx", path="raw/b.xlsx")
    first = _entry("TRAUPIXE-a.xlsx", path="raw/a.xlsx")

    result = select_traupixe_workbooks([second, first])

    assert result == [first, second]
