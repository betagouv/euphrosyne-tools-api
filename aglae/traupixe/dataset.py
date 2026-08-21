from __future__ import annotations

import math
import re
from dataclasses import dataclass
from io import BytesIO
from typing import Any
from zipfile import BadZipFile

from openpyxl import load_workbook
from openpyxl.utils.exceptions import InvalidFileException
from openpyxl.worksheet.worksheet import Worksheet

CONCENTRATION_SHEETS = ("S_Conc. %", "S_Conc. ppm")
BEST_DETECTOR_SHEET = "S_Best Det."
DEFAULT_MAJOR_ANALYTES = ("Na2O-PIGE", "MgO", "Al2O3", "SiO2", "K2O", "CaO")
HEADER_SCAN_ROWS = 10
MIN_ANALYTE_COLUMNS = 3
ZONE_SUFFIX = re.compile(r"_(?:pt|point)\s*\d+$", re.IGNORECASE)
NON_DETECTION = re.compile(r"^\s*<\s*")
DECIMAL_VALUE = re.compile(r"[-+]?(?:\d+(?:[.,]\d*)?|[.,]\d+)(?:[Ee][-+]?\d+)?")


class TraupixeDatasetError(ValueError):
    """Raised when a workbook cannot be interpreted as a supported TRAUPIXE file."""


@dataclass(frozen=True)
class TraupixeMeasurement:
    value: float
    detected: bool
    detector: str | None


@dataclass(frozen=True)
class TraupixeAnalysis:
    identifier: str
    label: str
    zone: str
    is_object: bool
    measurements: dict[str, TraupixeMeasurement]


@dataclass(frozen=True)
class TraupixeDataset:
    concentration_sheet: str
    detector_sheet: str | None
    unit: str
    analytes: tuple[str, ...]
    analyses: tuple[TraupixeAnalysis, ...]

    @property
    def object_analyses(self) -> tuple[TraupixeAnalysis, ...]:
        return tuple(analysis for analysis in self.analyses if analysis.is_object)

    def descriptor(self) -> dict[str, Any]:
        objects = self.object_analyses
        zones: dict[str, int] = {}
        for analysis in objects:
            zones[analysis.zone] = zones.get(analysis.zone, 0) + 1
        analytes = []
        for analyte in self.analytes:
            detected = sum(
                analysis.measurements[analyte].detected
                for analysis in objects
                if analyte in analysis.measurements
            )
            detectors = sorted(
                {
                    measurement.detector
                    for analysis in objects
                    if (measurement := analysis.measurements.get(analyte)) is not None
                    and measurement.detector is not None
                }
            )
            analytes.append(
                {
                    "name": analyte,
                    "detected": detected,
                    "total_objects": len(objects),
                    "detection_rate": (
                        round(detected / len(objects), 4) if objects else 0
                    ),
                    "detectors": detectors,
                }
            )
        return {
            "concentration_sheet": self.concentration_sheet,
            "detector_sheet": self.detector_sheet,
            "unit": self.unit,
            "default_major_analytes": [
                analyte
                for analyte in DEFAULT_MAJOR_ANALYTES
                if analyte in self.analytes
            ],
            "analyses": {
                "total": len(self.analyses),
                "objects": len(objects),
                "other": len(self.analyses) - len(objects),
                "zones": [
                    {"name": name, "analyses": count} for name, count in zones.items()
                ],
            },
            "analytes": analytes,
        }


def serialize_traupixe_for_model(dataset: TraupixeDataset) -> dict[str, Any]:
    """Serialize normalized TRAUPIXE data into the compact model contract."""
    descriptor = dataset.descriptor()
    analyses = []
    for analysis in dataset.analyses:
        measurements = [
            analysis.measurements.get(analyte) for analyte in dataset.analytes
        ]
        analyses.append(
            {
                "identifier": analysis.identifier,
                "label": analysis.label,
                "zone": analysis.zone,
                "kind": "object" if analysis.is_object else "reference",
                "values": [
                    measurement.value if measurement is not None else None
                    for measurement in measurements
                ],
                "detected": [
                    measurement.detected if measurement is not None else None
                    for measurement in measurements
                ],
                "detectors": [
                    measurement.detector if measurement is not None else None
                    for measurement in measurements
                ],
            }
        )
    return {
        "concentration_sheet": dataset.concentration_sheet,
        "detector_sheet": dataset.detector_sheet,
        "unit": dataset.unit,
        "default_major_analytes": descriptor["default_major_analytes"],
        "summary": descriptor["analyses"],
        "analytes": descriptor["analytes"],
        "analyses": analyses,
    }


def load_traupixe_dataset(content: bytes) -> TraupixeDataset:
    try:
        workbook = load_workbook(BytesIO(content), data_only=True, read_only=False)
    except (BadZipFile, InvalidFileException, KeyError, OSError, ValueError) as error:
        raise TraupixeDatasetError("Unable to read the TRAUPIXE workbook") from error

    try:
        concentration_sheet = _find_sheet(workbook.sheetnames, CONCENTRATION_SHEETS)
        if concentration_sheet is None:
            raise TraupixeDatasetError(
                "No supported TRAUPIXE concentration sheet was found"
            )
        concentration = workbook[concentration_sheet]
        header_row = _find_header_row(concentration)
        analyte_columns = _analyte_columns(concentration, header_row)
        detector_sheet = _find_sheet(workbook.sheetnames, (BEST_DETECTOR_SHEET,))
        detector_values = (
            _detector_values(workbook[detector_sheet], tuple(analyte_columns))
            if detector_sheet is not None
            else {}
        )
        analyses = _analyses(
            concentration,
            header_row,
            analyte_columns,
            detector_values,
        )
    finally:
        workbook.close()

    if not analyses:
        raise TraupixeDatasetError("The TRAUPIXE workbook contains no analyses")
    return TraupixeDataset(
        concentration_sheet=concentration_sheet,
        detector_sheet=detector_sheet,
        unit="%" if concentration_sheet.casefold().endswith("%") else "ppm",
        analytes=tuple(analyte_columns),
        analyses=tuple(analyses),
    )


def _find_sheet(sheet_names: list[str], candidates: tuple[str, ...]) -> str | None:
    by_name = {_normalized_text(name): name for name in sheet_names}
    return next(
        (
            by_name[_normalized_text(candidate)]
            for candidate in candidates
            if _normalized_text(candidate) in by_name
        ),
        None,
    )


def _find_header_row(sheet: Worksheet) -> int:
    candidates = []
    for row in range(1, min(sheet.max_row, HEADER_SCAN_ROWS) + 1):
        populated = sum(
            isinstance(sheet.cell(row, column).value, str)
            and bool(str(sheet.cell(row, column).value).strip())
            for column in range(3, sheet.max_column + 1)
        )
        candidates.append((populated, row))
    populated, row = max(candidates, key=lambda candidate: candidate[0], default=(0, 0))
    if populated < MIN_ANALYTE_COLUMNS:
        raise TraupixeDatasetError("Unable to locate the TRAUPIXE analyte header")
    return row


def _analyte_columns(sheet: Worksheet, header_row: int) -> dict[str, int]:
    columns: dict[str, int] = {}
    for column in range(3, sheet.max_column + 1):
        value = sheet.cell(header_row, column).value
        if isinstance(value, str) and value.strip():
            columns.setdefault(value.strip(), column)
    if len(columns) < MIN_ANALYTE_COLUMNS:
        raise TraupixeDatasetError("The TRAUPIXE analyte header is incomplete")
    return columns


def _detector_values(
    sheet: Worksheet,
    analytes: tuple[str, ...],
) -> dict[tuple[str, str], str]:
    header_row = _find_header_row(sheet)
    headers = _analyte_columns(sheet, header_row)
    matching_columns = {
        analyte: headers[analyte] for analyte in analytes if analyte in headers
    }
    values: dict[tuple[str, str], str] = {}
    for row in range(header_row + 1, sheet.max_row + 1):
        identifier = _text(sheet.cell(row, 1).value)
        if identifier is None:
            continue
        for analyte, column in matching_columns.items():
            detector = _text(sheet.cell(row, column).value)
            if detector is not None:
                values[(identifier, analyte)] = detector
    return values


def _analyses(
    sheet: Worksheet,
    header_row: int,
    analyte_columns: dict[str, int],
    detector_values: dict[tuple[str, str], str],
) -> list[TraupixeAnalysis]:
    analyses = []
    for row in range(header_row + 1, sheet.max_row + 1):
        identifier = _text(sheet.cell(row, 1).value)
        label = _text(sheet.cell(row, 2).value)
        if identifier is None or label is None:
            continue
        measurements = {}
        for analyte, column in analyte_columns.items():
            parsed = _parse_measurement(sheet.cell(row, column).value)
            if parsed is None:
                continue
            value, detected = parsed
            measurements[analyte] = TraupixeMeasurement(
                value=value,
                detected=detected,
                detector=detector_values.get((identifier, analyte)),
            )
        if not measurements:
            continue
        analyses.append(
            TraupixeAnalysis(
                identifier=identifier,
                label=label,
                zone=ZONE_SUFFIX.sub("", label),
                is_object=_is_object_analysis(identifier, label),
                measurements=measurements,
            )
        )
    return analyses


def _parse_measurement(value: Any) -> tuple[float, bool] | None:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return (float(value), True) if math.isfinite(value) else None
    if not isinstance(value, str):
        return None
    match = DECIMAL_VALUE.search(value.strip())
    if match is None:
        return None
    parsed = float(match.group(0).replace(",", "."))
    if not math.isfinite(parsed):
        return None
    return parsed, NON_DETECTION.match(value) is None


def _is_object_analysis(identifier: str, label: str) -> bool:
    normalized_identifier = identifier.casefold()
    normalized_label = _normalized_text(label)
    return "_std_" not in normalized_identifier and normalized_label not in {
        "mesurecharge",
        "mesure charge",
    }


def _text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _normalized_text(value: str) -> str:
    return " ".join(value.strip().casefold().split())
