from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from pathlib import Path


class MeasurementUnit(str, Enum):
    PERCENT = "%"
    PPM = "ppm"


class MeasurementQualifier(str, Enum):
    DETECTED = "detected"
    BELOW_LOD = "below_lod"
    MISSING = "missing"


class Detector(str, Enum):
    X0 = "X0"
    X10 = "X10"


class ExclusionReason(str, Enum):
    EMPTY = "empty"
    NOT_DETECTED = "not_detected"
    SENTINEL = "sentinel"
    BELOW_LOD = "below_lod"
    INVALID_VALUE = "invalid_value"


@dataclass(frozen=True)
class Analysis:
    analysis_id: str
    description: str


@dataclass(frozen=True)
class Measurement:
    analysis_id: str
    analyte: str
    value: Decimal | None
    unit: MeasurementUnit
    qualifier: MeasurementQualifier
    detection_limit: Decimal | None
    uncertainty: Decimal | None
    detector: Detector


@dataclass(frozen=True)
class ExclusionSummary:
    reason: ExclusionReason
    count: int


@dataclass(frozen=True)
class DatasetMetadata:
    source_name: str
    source_sha256: str
    analytes: tuple[str, ...]
    units: tuple[MeasurementUnit, ...]
    analysis_count: int
    measurement_count: int
    detectors: tuple[Detector, ...]
    aliases: tuple[tuple[str, str], ...]
    exclusions: tuple[ExclusionSummary, ...] = ()
    conventions: tuple[str, ...] = ()


@dataclass(frozen=True)
class NormalizedDataset:
    analyses: tuple[Analysis, ...]
    measurements: tuple[Measurement, ...]
    metadata: DatasetMetadata


@dataclass(frozen=True)
class DatasetExport:
    analyses_csv: Path
    measurements_csv: Path
    metadata_json: Path
