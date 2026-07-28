from dataclasses import FrozenInstanceError, fields
from decimal import Decimal
from pathlib import Path

import pytest

from aglae.traupixe.exceptions import (
    TraupixeFormatError,
    TraupixeIncompatibleWorkbookError,
    TraupixeSourceChangedError,
    TraupixeTooLargeError,
    TraupixeValidationCode,
    TraupixeValidationIssue,
    TraupixeWorkbookNotFoundError,
)
from aglae.traupixe.models import (
    Analysis,
    DatasetExport,
    DatasetMetadata,
    Detector,
    ExclusionReason,
    ExclusionSummary,
    Measurement,
    MeasurementQualifier,
    MeasurementUnit,
    NormalizedDataset,
)


def test_domain_enums_use_contract_values() -> None:
    assert {unit.value for unit in MeasurementUnit} == {"%", "ppm"}
    assert {qualifier.value for qualifier in MeasurementQualifier} == {
        "detected",
        "below_lod",
        "missing",
    }
    assert Detector.X0.value == "X0"
    assert Detector.X10.value == "X10"
    assert Detector("Gamma").value == "Gamma"


def test_analysis_id_is_opaque_and_analysis_has_only_two_fields() -> None:
    analysis = Analysis(
        analysis_id="an opaque value with _ and spaces",
        description="Reference sample",
    )

    assert [field.name for field in fields(Analysis)] == [
        "analysis_id",
        "description",
    ]
    assert analysis.analysis_id == "an opaque value with _ and spaces"


def test_measurement_preserves_decimal_values() -> None:
    measurement = Measurement(
        analysis_id="analysis-id",
        analyte="Fe2O3",
        value=Decimal("1.20"),
        unit=MeasurementUnit.PERCENT,
        qualifier=MeasurementQualifier.DETECTED,
        detection_limit=None,
        uncertainty=Decimal("0.50"),
        detector=Detector.X0,
    )

    assert measurement.value == Decimal("1.20")
    assert measurement.uncertainty == Decimal("0.50")
    assert isinstance(measurement.value, Decimal)


def test_domain_models_are_frozen() -> None:
    analysis = Analysis(analysis_id="analysis-id", description="Sample")

    with pytest.raises(FrozenInstanceError):
        analysis.description = "Changed"  # type: ignore[misc]


def test_normalized_dataset_groups_typed_records() -> None:
    analysis = Analysis(analysis_id="analysis-id", description="Sample")
    measurement = Measurement(
        analysis_id=analysis.analysis_id,
        analyte="PbO",
        value=None,
        unit=MeasurementUnit.PPM,
        qualifier=MeasurementQualifier.BELOW_LOD,
        detection_limit=Decimal("7"),
        uncertainty=None,
        detector=Detector.X10,
    )
    exclusion = ExclusionSummary(reason=ExclusionReason.BELOW_LOD, count=1)
    metadata = DatasetMetadata(
        source_name="source.xlsx",
        source_sha256="a" * 64,
        analytes=("PbO",),
        units=(MeasurementUnit.PPM,),
        analysis_count=1,
        measurement_count=1,
        detectors=(Detector.X10,),
        aliases=(("plomb", "PbO"),),
        exclusions=(exclusion,),
    )

    dataset = NormalizedDataset(
        analyses=(analysis,),
        measurements=(measurement,),
        metadata=metadata,
    )

    assert dataset.metadata.exclusions == (exclusion,)
    assert dataset.measurements[0].detection_limit == Decimal("7")


def test_dataset_export_contains_the_three_normalized_files(tmp_path: Path) -> None:
    export = DatasetExport(
        analyses_csv=tmp_path / "analyses.csv",
        measurements_csv=tmp_path / "measurements.csv",
        metadata_json=tmp_path / "dataset_metadata.json",
    )

    assert export.analyses_csv.name == "analyses.csv"
    assert export.measurements_csv.name == "measurements.csv"
    assert export.metadata_json.name == "dataset_metadata.json"


def test_format_error_requires_and_exposes_typed_issues() -> None:
    issue = TraupixeValidationIssue(
        code=TraupixeValidationCode.MISSING_SHEET,
        message="Missing S_Best Det.",
        sheet="S_Best Det.",
    )
    error = TraupixeFormatError((issue,))

    assert error.issues == (issue,)
    with pytest.raises(ValueError):
        TraupixeFormatError(())


def test_typed_errors_expose_machine_readable_context() -> None:
    too_large = TraupixeTooLargeError(size=101, maximum_size=100)
    changed = TraupixeSourceChangedError(
        expected_sha256="expected",
        actual_sha256="actual",
    )

    assert (too_large.size, too_large.maximum_size) == (101, 100)
    assert changed.expected_sha256 == "expected"
    assert changed.actual_sha256 == "actual"


def test_selection_errors_have_stable_types() -> None:
    issue = TraupixeValidationIssue(
        code=TraupixeValidationCode.INVALID_HEADER,
        message="Invalid header",
    )
    incompatible = TraupixeIncompatibleWorkbookError((issue,))
    not_found = TraupixeWorkbookNotFoundError("opaque-id")

    assert incompatible.issues == (issue,)
    assert not_found.file_id == "opaque-id"
