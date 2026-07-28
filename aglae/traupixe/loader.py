from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any, BinaryIO
from zipfile import BadZipFile

from openpyxl import load_workbook
from openpyxl.utils.exceptions import InvalidFileException

from .exceptions import (
    TraupixeIncompatibleWorkbookError,
    TraupixeTooLargeError,
    TraupixeUnreadableError,
    TraupixeUnsupportedFileError,
    TraupixeValidationCode,
    TraupixeValidationIssue,
)
from .format import TRAUPIXE_FORMAT, TraupixeFormat, WorksheetFormat
from .models import Analysis, Detector, MeasurementUnit

WorkbookSource = str | Path | BinaryIO


@dataclass(frozen=True)
class LoadedMeasurement:
    analysis_id: str
    analyte: str
    unit: MeasurementUnit
    raw_value: object
    raw_uncertainty: object
    detector: Detector


@dataclass(frozen=True)
class LoadedTraupixeWorkbook:
    source_name: str
    source_sha256: str
    analyses: tuple[Analysis, ...]
    measurements: tuple[LoadedMeasurement, ...]
    analytes: tuple[str, ...]
    units: tuple[MeasurementUnit, ...]
    detectors: tuple[Detector, ...]


@dataclass(frozen=True)
class _SourceDetails:
    name: str
    size: int
    sha256: str
    initial_position: int | None = None


def _read_source_details(
    source: WorkbookSource,
    source_name: str | None,
) -> _SourceDetails:
    digest = sha256()

    if isinstance(source, (str, Path)):
        path = Path(source)
        name = source_name or path.name
        size = path.stat().st_size
        with path.open("rb") as source_file:
            for chunk in iter(lambda: source_file.read(1024 * 1024), b""):
                digest.update(chunk)
        return _SourceDetails(
            name=name,
            size=size,
            sha256=digest.hexdigest(),
        )

    if not source.seekable():
        raise TraupixeUnreadableError(
            "A seekable stream is required to read a TRAUPIXE workbook"
        )

    initial_position = source.tell()
    source.seek(0)
    size = 0
    for chunk in iter(lambda: source.read(1024 * 1024), b""):
        size += len(chunk)
        digest.update(chunk)
    source.seek(0)

    stream_name = source_name
    if stream_name is None:
        raw_name = getattr(source, "name", "source.xlsx")
        stream_name = Path(str(raw_name)).name

    return _SourceDetails(
        name=stream_name,
        size=size,
        sha256=digest.hexdigest(),
        initial_position=initial_position,
    )


def _restore_stream_position(
    source: WorkbookSource,
    initial_position: int | None,
) -> None:
    if isinstance(source, (str, Path)) or initial_position is None:
        return
    source.seek(initial_position)


def _prepare_worksheet(worksheet: Any) -> None:
    dimension = worksheet.calculate_dimension()
    if dimension not in {"A1", "A1:A1"}:
        return
    worksheet.reset_dimensions()
    worksheet.calculate_dimension(force=True)


def _validate_sheet_names(
    sheet_names: tuple[str, ...],
    traupixe_format: TraupixeFormat,
) -> list[TraupixeValidationIssue]:
    expected = set(traupixe_format.required_sheets)
    actual = set(sheet_names)
    issues = [
        TraupixeValidationIssue(
            code=TraupixeValidationCode.MISSING_SHEET,
            message=f"Missing required worksheet: {sheet_name}",
            sheet=sheet_name,
        )
        for sheet_name in traupixe_format.required_sheets
        if sheet_name not in actual
    ]
    unexpected = sorted(actual - expected)
    if unexpected:
        issues.append(
            TraupixeValidationIssue(
                code=TraupixeValidationCode.INVALID_HEADER,
                message=("Unexpected worksheets: " + ", ".join(unexpected)),
            )
        )
    return issues


def _validate_headers(
    worksheet: Any,
    worksheet_format: WorksheetFormat,
) -> list[TraupixeValidationIssue]:
    if worksheet_format.header_row is None:
        return []

    expected = worksheet_format.headers
    maximum_column = max(len(expected), worksheet.max_column or 1)
    row = next(
        worksheet.iter_rows(
            min_row=worksheet_format.header_row,
            max_row=worksheet_format.header_row,
            max_col=maximum_column,
            values_only=True,
        ),
        (),
    )
    actual = tuple(row[: len(expected)])
    issues: list[TraupixeValidationIssue] = []

    if actual != expected:
        for column_index, expected_value in enumerate(expected, start=1):
            actual_value = (
                actual[column_index - 1] if column_index <= len(actual) else None
            )
            if actual_value != expected_value:
                issues.append(
                    TraupixeValidationIssue(
                        code=TraupixeValidationCode.INVALID_HEADER,
                        message=(
                            f"Expected header {expected_value!r}, "
                            f"got {actual_value!r}"
                        ),
                        sheet=worksheet.title,
                        cell=worksheet.cell(
                            worksheet_format.header_row,
                            column_index,
                        ).coordinate,
                    )
                )

    if any(value is not None for value in row[len(expected) :]):
        issues.append(
            TraupixeValidationIssue(
                code=TraupixeValidationCode.INVALID_HEADER,
                message="Unexpected header columns",
                sheet=worksheet.title,
            )
        )

    return issues


def _identified_rows(
    worksheet: Any,
    worksheet_format: WorksheetFormat,
) -> tuple[
    tuple[tuple[int, str, tuple[object, ...]], ...],
    list[TraupixeValidationIssue],
]:
    if worksheet_format.data_start_row is None:
        return (), []

    rows: list[tuple[int, str, tuple[object, ...]]] = []
    issues: list[TraupixeValidationIssue] = []
    seen_ids: set[str] = set()
    for row_number, row in enumerate(
        worksheet.iter_rows(
            min_row=worksheet_format.data_start_row,
            max_col=len(worksheet_format.headers),
            values_only=True,
        ),
        start=worksheet_format.data_start_row,
    ):
        analysis_id = row[0] if row else None
        if analysis_id is None or (
            isinstance(analysis_id, str) and not analysis_id.strip()
        ):
            continue
        if not isinstance(analysis_id, str):
            issues.append(
                TraupixeValidationIssue(
                    code=TraupixeValidationCode.INVALID_ANALYSIS_ID,
                    message="Analysis identifiers must be non-empty text",
                    sheet=worksheet.title,
                    cell=f"A{row_number}",
                )
            )
            continue
        if analysis_id in seen_ids:
            issues.append(
                TraupixeValidationIssue(
                    code=TraupixeValidationCode.DUPLICATE_ANALYSIS_ID,
                    message=f"Duplicate analysis identifier: {analysis_id}",
                    sheet=worksheet.title,
                    cell=f"A{row_number}",
                )
            )
            continue
        seen_ids.add(analysis_id)
        rows.append((row_number, analysis_id, tuple(row)))
    return tuple(rows), issues


def _validate_aligned_ids(
    rows_by_sheet: dict[
        str,
        tuple[tuple[int, str, tuple[object, ...]], ...],
    ],
    source_sheets: tuple[str, ...],
) -> list[TraupixeValidationIssue]:
    reference_sheet = source_sheets[0]
    reference_ids = {
        analysis_id for _, analysis_id, _ in rows_by_sheet[reference_sheet]
    }
    issues: list[TraupixeValidationIssue] = []

    for sheet_name in source_sheets[1:]:
        actual_ids = {analysis_id for _, analysis_id, _ in rows_by_sheet[sheet_name]}
        if actual_ids == reference_ids:
            continue
        missing = sorted(reference_ids - actual_ids)
        unexpected = sorted(actual_ids - reference_ids)
        details = []
        if missing:
            details.append("missing: " + ", ".join(missing))
        if unexpected:
            details.append("unexpected: " + ", ".join(unexpected))
        issues.append(
            TraupixeValidationIssue(
                code=TraupixeValidationCode.MISALIGNED_ANALYSIS_IDS,
                message=(
                    f"Analysis identifiers do not match {reference_sheet}"
                    + (f" ({'; '.join(details)})" if details else "")
                ),
                sheet=sheet_name,
            )
        )
    return issues


def _normalize_detector(
    raw_detector: object,
    *,
    sheet: str,
    cell: str,
    traupixe_format: TraupixeFormat,
) -> tuple[Detector | None, TraupixeValidationIssue | None]:
    try:
        detector = Detector(raw_detector)
    except (TypeError, ValueError):
        return None, TraupixeValidationIssue(
            code=TraupixeValidationCode.UNKNOWN_DETECTOR,
            message=f"Unknown detector: {raw_detector!r}",
            sheet=sheet,
            cell=cell,
        )
    if detector not in traupixe_format.detectors:
        return None, TraupixeValidationIssue(
            code=TraupixeValidationCode.UNKNOWN_DETECTOR,
            message=f"Detector is not allowed by TRAUPIXE_FORMAT: {detector}",
            sheet=sheet,
            cell=cell,
        )
    return detector, None


def _load_records(
    rows_by_sheet: dict[
        str,
        tuple[tuple[int, str, tuple[object, ...]], ...],
    ],
    traupixe_format: TraupixeFormat,
) -> tuple[
    tuple[Analysis, ...],
    tuple[LoadedMeasurement, ...],
    tuple[Detector, ...],
    list[TraupixeValidationIssue],
]:
    analysis_order = tuple(
        analysis_id
        for _, analysis_id, _ in rows_by_sheet[traupixe_format.unit_sheets[0][0]]
    )
    row_maps = {
        sheet_name: {
            analysis_id: (row_number, row) for row_number, analysis_id, row in rows
        }
        for sheet_name, rows in rows_by_sheet.items()
    }

    analyses = tuple(
        Analysis(
            analysis_id=analysis_id,
            description=(
                ""
                if row_maps["Exp. data"][analysis_id][1][1] is None
                else str(row_maps["Exp. data"][analysis_id][1][1])
            ),
        )
        for analysis_id in analysis_order
    )

    detector_rows = row_maps["S_Best Det."]
    detector_map: dict[tuple[str, str], Detector] = {}
    issues: list[TraupixeValidationIssue] = []
    used_detectors: set[Detector] = set()
    for analysis_id in analysis_order:
        row_number, row = detector_rows[analysis_id]
        for analyte_index, analyte in enumerate(
            traupixe_format.analytes,
        ):
            column_index = analyte_index + 3
            detector, issue = _normalize_detector(
                row[analyte_index + 2],
                sheet="S_Best Det.",
                cell=f"{_column_letter(column_index)}{row_number}",
                traupixe_format=traupixe_format,
            )
            if issue is not None:
                issues.append(issue)
                continue
            assert detector is not None
            detector_map[(analysis_id, analyte)] = detector
            used_detectors.add(detector)

    if issues:
        return analyses, (), (), issues

    measurements: list[LoadedMeasurement] = []
    unit_rows = {
        unit: row_maps[sheet_name] for sheet_name, unit in traupixe_format.unit_sheets
    }
    for analysis_id in analysis_order:
        for analyte_index, analyte in enumerate(
            traupixe_format.analytes,
        ):
            value_index = 2 + (analyte_index * 2)
            uncertainty_index = value_index + 1
            for unit in traupixe_format.units:
                _, row = unit_rows[unit][analysis_id]
                measurements.append(
                    LoadedMeasurement(
                        analysis_id=analysis_id,
                        analyte=analyte,
                        unit=unit,
                        raw_value=row[value_index],
                        raw_uncertainty=row[uncertainty_index],
                        detector=detector_map[(analysis_id, analyte)],
                    )
                )

    detectors = tuple(
        detector for detector in traupixe_format.detectors if detector in used_detectors
    )
    return analyses, tuple(measurements), detectors, []


def _column_letter(column: int) -> str:
    letters = ""
    while column:
        column, remainder = divmod(column - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters


def load_traupixe_workbook(
    source: WorkbookSource,
    *,
    source_name: str | None = None,
    traupixe_format: TraupixeFormat = TRAUPIXE_FORMAT,
) -> LoadedTraupixeWorkbook:
    try:
        source_details = _read_source_details(source, source_name)
    except OSError as error:
        raise TraupixeUnreadableError(
            f"Unable to read TRAUPIXE source: {error}"
        ) from error
    extension = Path(source_details.name).suffix
    if extension.lower() != traupixe_format.extension:
        _restore_stream_position(source, source_details.initial_position)
        raise TraupixeUnsupportedFileError(extension)
    if source_details.size > traupixe_format.maximum_source_size:
        _restore_stream_position(source, source_details.initial_position)
        raise TraupixeTooLargeError(
            source_details.size,
            traupixe_format.maximum_source_size,
        )

    workbook = None
    try:
        workbook = load_workbook(
            source,
            read_only=True,
            data_only=True,
        )
        issues = _validate_sheet_names(
            tuple(workbook.sheetnames),
            traupixe_format,
        )
        if issues:
            raise TraupixeIncompatibleWorkbookError(tuple(issues))

        for worksheet_format in traupixe_format.worksheets:
            worksheet = workbook[worksheet_format.name]
            if worksheet_format.header_row is not None:
                _prepare_worksheet(worksheet)
            issues.extend(_validate_headers(worksheet, worksheet_format))
        if issues:
            raise TraupixeIncompatibleWorkbookError(tuple(issues))

        rows_by_sheet: dict[
            str,
            tuple[tuple[int, str, tuple[object, ...]], ...],
        ] = {}
        for sheet_name in traupixe_format.source_sheets:
            worksheet_format = traupixe_format.worksheet(sheet_name)
            rows, row_issues = _identified_rows(
                workbook[sheet_name],
                worksheet_format,
            )
            rows_by_sheet[sheet_name] = rows
            issues.extend(row_issues)
        issues.extend(
            _validate_aligned_ids(
                rows_by_sheet,
                traupixe_format.source_sheets,
            )
        )
        if issues:
            raise TraupixeIncompatibleWorkbookError(tuple(issues))

        analyses, measurements, detectors, record_issues = _load_records(
            rows_by_sheet,
            traupixe_format,
        )
        if record_issues:
            raise TraupixeIncompatibleWorkbookError(tuple(record_issues))
        return LoadedTraupixeWorkbook(
            source_name=source_details.name,
            source_sha256=source_details.sha256,
            analyses=analyses,
            measurements=measurements,
            analytes=traupixe_format.analytes,
            units=traupixe_format.units,
            detectors=detectors,
        )
    except TraupixeIncompatibleWorkbookError:
        raise
    except (
        BadZipFile,
        InvalidFileException,
        OSError,
        ValueError,
        KeyError,
    ) as error:
        raise TraupixeUnreadableError(
            f"Unable to read TRAUPIXE workbook: {error}"
        ) from error
    finally:
        if workbook is not None:
            workbook.close()
        _restore_stream_position(source, source_details.initial_position)


def validate_traupixe_workbook(
    source: WorkbookSource,
    *,
    source_name: str | None = None,
    traupixe_format: TraupixeFormat = TRAUPIXE_FORMAT,
) -> None:
    load_traupixe_workbook(
        source,
        source_name=source_name,
        traupixe_format=traupixe_format,
    )
