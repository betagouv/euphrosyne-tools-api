from __future__ import annotations

from dataclasses import replace
from io import BytesIO
from pathlib import Path
from typing import Any

import pytest

import aglae.traupixe.loader as loader_module
from aglae.traupixe.exceptions import (
    TraupixeIncompatibleWorkbookError,
    TraupixeTooLargeError,
    TraupixeUnreadableError,
    TraupixeUnsupportedFileError,
    TraupixeValidationCode,
)
from aglae.traupixe.format import TRAUPIXE_FORMAT
from aglae.traupixe.loader import load_traupixe_workbook, validate_traupixe_workbook
from aglae.traupixe.models import Detector, MeasurementUnit
from tests.aglae.traupixe.fixture_factory import write_traupixe_fixture


def test_loader_uses_read_only_data_only_and_joins_rows_by_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = write_traupixe_fixture(
        tmp_path / "source.xlsx",
        analysis_ids=("opaque_a", "opaque_b"),
        row_orders={
            "S_Conc. & Unc %": ("opaque_b", "opaque_a"),
            "S_Conc. & Unc ppm": ("opaque_a", "opaque_b"),
            "S_Best Det.": ("opaque_a", "opaque_b"),
            "Exp. data": ("opaque_b", "opaque_a"),
        },
        values={
            (
                "opaque_b",
                "Fe2O3",
                MeasurementUnit.PERCENT,
            ): ("7,125", "3,5"),
        },
    )
    real_load_workbook = loader_module.load_workbook
    options: dict[str, object] = {}

    def load_workbook_spy(*args: Any, **kwargs: Any):
        options.update(kwargs)
        return real_load_workbook(*args, **kwargs)

    monkeypatch.setattr(
        loader_module,
        "load_workbook",
        load_workbook_spy,
    )

    workbook = load_traupixe_workbook(source)

    assert options["read_only"] is True
    assert options["data_only"] is True
    assert [analysis.analysis_id for analysis in workbook.analyses] == [
        "opaque_b",
        "opaque_a",
    ]
    measurement = next(
        item
        for item in workbook.measurements
        if (
            item.analysis_id,
            item.analyte,
            item.unit,
        )
        == ("opaque_b", "Fe2O3", MeasurementUnit.PERCENT)
    )
    assert measurement.raw_value == "7,125"
    assert measurement.raw_uncertainty == "3,5"
    assert len(workbook.measurements) == 2 * 36 * 2
    assert workbook.detectors == (Detector.X0, Detector.X10)
    assert workbook.source_name == "source.xlsx"
    assert len(workbook.source_sha256) == 64


def test_loader_resets_incorrect_a1_dimensions_and_skips_empty_information(
    tmp_path: Path,
) -> None:
    source = write_traupixe_fixture(
        tmp_path / "source.xlsx",
        force_a1_dimensions=True,
    )

    workbook = load_traupixe_workbook(source)

    assert len(workbook.analyses) == 2
    assert len(workbook.measurements) == 144


def test_validator_returns_none_for_a_compatible_stream(
    tmp_path: Path,
) -> None:
    source = write_traupixe_fixture(tmp_path / "source.xlsx")
    stream = BytesIO(source.read_bytes())
    stream.seek(9)

    validate_traupixe_workbook(stream)

    assert stream.tell() == 9


def test_corrupt_stream_is_mapped_to_unreadable_and_position_is_restored() -> None:
    stream = BytesIO(b"not an xlsx workbook")
    stream.seek(4)

    with pytest.raises(TraupixeUnreadableError):
        load_traupixe_workbook(stream)

    assert stream.tell() == 4


def test_loader_rejects_extension_and_size_before_opening_workbook(
    tmp_path: Path,
) -> None:
    source = write_traupixe_fixture(tmp_path / "source.xlsx")
    content = source.read_bytes()

    with pytest.raises(TraupixeUnsupportedFileError):
        load_traupixe_workbook(
            BytesIO(content),
            source_name="source.csv",
        )

    with pytest.raises(TraupixeTooLargeError):
        load_traupixe_workbook(
            BytesIO(content),
            source_name="source.xlsx",
            traupixe_format=replace(
                TRAUPIXE_FORMAT,
                maximum_source_size=len(content) - 1,
            ),
        )


def test_loader_stops_after_maximum_size_plus_one_and_restores_position() -> None:
    class TrackingBytesIO(BytesIO):
        maximum_position = 0

        def read(self, size: int | None = -1) -> bytes:
            content = super().read(size)
            self.maximum_position = max(self.maximum_position, self.tell())
            return content

    stream = TrackingBytesIO(b"x" * 100)
    stream.seek(3)

    with pytest.raises(TraupixeTooLargeError) as error:
        load_traupixe_workbook(
            stream,
            source_name="source.xlsx",
            traupixe_format=replace(
                TRAUPIXE_FORMAT,
                maximum_source_size=8,
            ),
        )

    assert error.value.size == 9
    assert stream.maximum_position == 9
    assert stream.tell() == 3


@pytest.mark.parametrize(
    ("fixture_options", "expected_code"),
    [
        (
            {"missing_sheet": "Matrix"},
            TraupixeValidationCode.MISSING_SHEET,
        ),
        (
            {"extra_sheet": "Unexpected"},
            TraupixeValidationCode.INVALID_HEADER,
        ),
        (
            {
                "header_overrides": {
                    ("S_Conc. & Unc %", 23): "V",
                }
            },
            TraupixeValidationCode.INVALID_HEADER,
        ),
        (
            {
                "detectors": {
                    ("opaque-analysis-a", "Na2O"): "X1",
                }
            },
            TraupixeValidationCode.UNKNOWN_DETECTOR,
        ),
    ],
)
def test_loader_rejects_incompatible_structure(
    tmp_path: Path,
    fixture_options: dict[str, Any],
    expected_code: TraupixeValidationCode,
) -> None:
    source = write_traupixe_fixture(
        tmp_path / "source.xlsx",
        **fixture_options,
    )

    with pytest.raises(TraupixeIncompatibleWorkbookError) as raised:
        load_traupixe_workbook(source)

    assert expected_code in {issue.code for issue in raised.value.issues}


def test_loader_rejects_duplicate_analysis_ids(tmp_path: Path) -> None:
    source = write_traupixe_fixture(
        tmp_path / "source.xlsx",
        row_orders={
            sheet_name: ("opaque-analysis-a", "opaque-analysis-a")
            for sheet_name in (
                "S_Conc. & Unc %",
                "S_Conc. & Unc ppm",
                "S_Best Det.",
                "Exp. data",
            )
        },
    )

    with pytest.raises(TraupixeIncompatibleWorkbookError) as raised:
        load_traupixe_workbook(source)

    assert TraupixeValidationCode.DUPLICATE_ANALYSIS_ID in {
        issue.code for issue in raised.value.issues
    }


def test_loader_rejects_misaligned_analysis_ids(tmp_path: Path) -> None:
    source = write_traupixe_fixture(
        tmp_path / "source.xlsx",
        row_orders={
            "S_Conc. & Unc ppm": (
                "opaque-analysis-a",
                "unexpected-analysis",
            ),
        },
    )

    with pytest.raises(TraupixeIncompatibleWorkbookError) as raised:
        load_traupixe_workbook(source)

    assert TraupixeValidationCode.MISALIGNED_ANALYSIS_IDS in {
        issue.code for issue in raised.value.issues
    }


def test_loader_treats_analysis_ids_as_opaque_text(tmp_path: Path) -> None:
    source = write_traupixe_fixture(
        tmp_path / "source.xlsx",
        analysis_ids=(
            "identifier with spaces_and_suffixes",
            "another/opaque:id",
        ),
    )

    workbook = load_traupixe_workbook(source)

    assert [analysis.analysis_id for analysis in workbook.analyses] == [
        "identifier with spaces_and_suffixes",
        "another/opaque:id",
    ]
