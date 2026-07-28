from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Iterable, Mapping

from .exceptions import TraupixeNormalizationError
from .format import TRAUPIXE_FORMAT, TraupixeFormat
from .loader import LoadedMeasurement, LoadedTraupixeWorkbook
from .models import (
    DatasetMetadata,
    Detector,
    ExclusionReason,
    ExclusionSummary,
    Measurement,
    MeasurementQualifier,
    NormalizedDataset,
)

SENTINEL_VALUE = Decimal("999999")


@dataclass(frozen=True)
class DetectionLimitResult:
    value: Decimal | None
    qualifier: MeasurementQualifier
    detection_limit: Decimal | None
    exclusion_reason: ExclusionReason | None


@dataclass(frozen=True)
class _DecimalResult:
    value: Decimal | None
    reason: ExclusionReason | None
    is_below_lod: bool = False


def _parse_decimal(raw_value: object, *, allow_below_lod: bool) -> _DecimalResult:
    if raw_value is None:
        return _DecimalResult(None, ExclusionReason.EMPTY)
    if isinstance(raw_value, bool):
        return _DecimalResult(None, ExclusionReason.INVALID_VALUE)

    if isinstance(raw_value, str):
        text = raw_value.strip()
        if not text:
            return _DecimalResult(None, ExclusionReason.EMPTY)
        if text.casefold() == "n.d.":
            return _DecimalResult(None, ExclusionReason.NOT_DETECTED)
    else:
        text = str(raw_value)

    is_below_lod = text.startswith("<")
    if is_below_lod:
        if not allow_below_lod:
            return _DecimalResult(None, ExclusionReason.INVALID_VALUE)
        text = text[1:].strip()

    normalized = (
        text.replace("\N{NO-BREAK SPACE}", "")
        .replace("\N{NARROW NO-BREAK SPACE}", "")
        .replace(" ", "")
    )
    if "," in normalized and "." in normalized:
        return _DecimalResult(None, ExclusionReason.INVALID_VALUE)
    normalized = normalized.replace(",", ".")

    try:
        value = Decimal(normalized)
    except (InvalidOperation, ValueError):
        return _DecimalResult(None, ExclusionReason.INVALID_VALUE)
    if not value.is_finite():
        return _DecimalResult(None, ExclusionReason.INVALID_VALUE)
    if value == SENTINEL_VALUE:
        return _DecimalResult(None, ExclusionReason.SENTINEL)
    if is_below_lod and value <= 0:
        return _DecimalResult(None, ExclusionReason.INVALID_VALUE)
    return _DecimalResult(value, None, is_below_lod)


def apply_detection_limits(raw_value: object) -> DetectionLimitResult:
    parsed = _parse_decimal(raw_value, allow_below_lod=True)
    if parsed.is_below_lod:
        return DetectionLimitResult(
            value=None,
            qualifier=MeasurementQualifier.BELOW_LOD,
            detection_limit=parsed.value,
            exclusion_reason=ExclusionReason.BELOW_LOD,
        )
    if parsed.reason is not None:
        return DetectionLimitResult(
            value=None,
            qualifier=MeasurementQualifier.MISSING,
            detection_limit=None,
            exclusion_reason=parsed.reason,
        )
    return DetectionLimitResult(
        value=parsed.value,
        qualifier=MeasurementQualifier.DETECTED,
        detection_limit=None,
        exclusion_reason=None,
    )


def normalize_detectors(
    raw_detectors: Iterable[object],
    *,
    traupixe_format: TraupixeFormat = TRAUPIXE_FORMAT,
) -> tuple[Detector, ...]:
    detectors: list[Detector] = []
    for raw_detector in raw_detectors:
        try:
            detector = Detector(raw_detector)
        except (TypeError, ValueError) as error:
            raise TraupixeNormalizationError(
                f"Unknown TRAUPIXE detector: {raw_detector!r}"
            ) from error
        if detector not in traupixe_format.detectors:
            raise TraupixeNormalizationError(
                f"Detector is not allowed by TRAUPIXE_FORMAT: {detector}"
            )
        if detector not in detectors:
            detectors.append(detector)
    return tuple(
        detector for detector in traupixe_format.detectors if detector in detectors
    )


def resolve_analytes(
    requested_analytes: str | Iterable[str],
    *,
    available_analytes: Iterable[str] | None = None,
    traupixe_format: TraupixeFormat = TRAUPIXE_FORMAT,
) -> tuple[str, ...]:
    requested: tuple[str, ...]
    if isinstance(requested_analytes, str):
        requested = (requested_analytes,)
    else:
        requested = tuple(requested_analytes)

    available = tuple(
        available_analytes
        if available_analytes is not None
        else traupixe_format.analytes
    )
    by_normalized_name = {analyte.casefold(): analyte for analyte in available}
    aliases = {
        alias.casefold(): analyte
        for alias, analyte in traupixe_format.analyte_aliases
        if analyte in available
    }
    resolved: list[str] = []

    for requested_analyte in requested:
        normalized = requested_analyte.strip().casefold()
        analyte = aliases.get(normalized) or by_normalized_name.get(normalized)
        if analyte is None:
            raise TraupixeNormalizationError(
                f"Analyte is not available in this TRAUPIXE dataset: "
                f"{requested_analyte}"
            )
        if analyte not in resolved:
            resolved.append(analyte)
    return tuple(resolved)


def describe_exclusions(
    counts: Mapping[ExclusionReason, int],
) -> tuple[ExclusionSummary, ...]:
    return tuple(
        ExclusionSummary(reason=reason, count=counts[reason])
        for reason in ExclusionReason
        if counts.get(reason, 0) > 0
    )


def _normalize_measurement(
    loaded_measurement: LoadedMeasurement,
    exclusion_counts: Counter[ExclusionReason],
) -> Measurement:
    detection_result = apply_detection_limits(loaded_measurement.raw_value)
    if detection_result.exclusion_reason is not None:
        exclusion_counts[detection_result.exclusion_reason] += 1

    uncertainty: Decimal | None = None
    if detection_result.qualifier is MeasurementQualifier.DETECTED:
        parsed_uncertainty = _parse_decimal(
            loaded_measurement.raw_uncertainty,
            allow_below_lod=False,
        )
        uncertainty = parsed_uncertainty.value
        if parsed_uncertainty.reason is not None:
            exclusion_counts[parsed_uncertainty.reason] += 1

    return Measurement(
        analysis_id=loaded_measurement.analysis_id,
        analyte=loaded_measurement.analyte,
        value=detection_result.value,
        unit=loaded_measurement.unit,
        qualifier=detection_result.qualifier,
        detection_limit=detection_result.detection_limit,
        uncertainty=uncertainty,
        detector=loaded_measurement.detector,
    )


def normalize_traupixe(
    workbook: LoadedTraupixeWorkbook,
    *,
    traupixe_format: TraupixeFormat = TRAUPIXE_FORMAT,
) -> NormalizedDataset:
    analysis_ids = tuple(analysis.analysis_id for analysis in workbook.analyses)
    if len(analysis_ids) != len(set(analysis_ids)):
        raise TraupixeNormalizationError("Analysis identifiers must be unique")

    if workbook.analytes != traupixe_format.analytes:
        raise TraupixeNormalizationError(
            "The loaded analyte sequence does not match TRAUPIXE_FORMAT"
        )
    if workbook.units != traupixe_format.units:
        raise TraupixeNormalizationError(
            "The loaded unit sequence does not match TRAUPIXE_FORMAT"
        )

    expected_measurement_count = (
        len(workbook.analyses) * len(workbook.analytes) * len(workbook.units)
    )
    if len(workbook.measurements) != expected_measurement_count:
        raise TraupixeNormalizationError("The loaded measurement grid is incomplete")

    allowed_analysis_ids = set(analysis_ids)
    seen_keys: set[tuple[str, str, object]] = set()
    for measurement in workbook.measurements:
        if measurement.analysis_id not in allowed_analysis_ids:
            raise TraupixeNormalizationError(
                "A measurement references an unknown analysis identifier"
            )
        key = (
            measurement.analysis_id,
            measurement.analyte,
            measurement.unit,
        )
        if key in seen_keys:
            raise TraupixeNormalizationError(
                "The loaded measurement grid contains duplicate rows"
            )
        seen_keys.add(key)

    expected_keys = {
        (analysis_id, analyte, unit)
        for analysis_id in analysis_ids
        for analyte in traupixe_format.analytes
        for unit in traupixe_format.units
    }
    if seen_keys != expected_keys:
        raise TraupixeNormalizationError(
            "The loaded measurement grid does not match " "analyses × analytes × units"
        )

    exclusion_counts: Counter[ExclusionReason] = Counter()
    measurements = tuple(
        _normalize_measurement(measurement, exclusion_counts)
        for measurement in workbook.measurements
    )
    detectors = normalize_detectors(
        (measurement.detector for measurement in measurements),
        traupixe_format=traupixe_format,
    )
    available_analytes = set(workbook.analytes)
    aliases = tuple(
        (alias, analyte)
        for alias, analyte in traupixe_format.analyte_aliases
        if analyte in available_analytes
    )

    metadata = DatasetMetadata(
        source_name=workbook.source_name,
        source_sha256=workbook.source_sha256,
        analytes=workbook.analytes,
        units=workbook.units,
        analysis_count=len(workbook.analyses),
        measurement_count=len(measurements),
        detectors=detectors,
        aliases=aliases,
        exclusions=describe_exclusions(exclusion_counts),
        conventions=(
            "Analysis identifiers are opaque and are not interpreted.",
            "Decimal commas are normalized without using the server locale.",
            "Values below LOD are null and keep their detection limit.",
            "Missing, n.d. and sentinel values are never converted to zero.",
            "X10 values are preserved and are not recalculated.",
        ),
    )
    return NormalizedDataset(
        analyses=workbook.analyses,
        measurements=measurements,
        metadata=metadata,
    )
