from decimal import Decimal
from pathlib import Path

import pytest

from aglae.traupixe.export import export_analysis_dataset, temporary_analysis_dataset
from aglae.traupixe.models import (
    Analysis,
    DatasetMetadata,
    Detector,
    ExclusionReason,
    ExclusionSummary,
    Measurement,
    MeasurementQualifier,
    MeasurementUnit,
    NormalizedDataset,
)


@pytest.fixture(name="dataset")
def fixture_dataset() -> NormalizedDataset:
    analyses = (Analysis(analysis_id="opaque-1", description="Échantillon"),)
    measurements = (
        Measurement(
            analysis_id="opaque-1",
            analyte="Fe2O3",
            value=Decimal("1.20"),
            unit=MeasurementUnit.PERCENT,
            qualifier=MeasurementQualifier.DETECTED,
            detection_limit=None,
            uncertainty=Decimal("2.5"),
            detector=Detector.X10,
        ),
        Measurement(
            analysis_id="opaque-1",
            analyte="PbO",
            value=None,
            unit=MeasurementUnit.PPM,
            qualifier=MeasurementQualifier.BELOW_LOD,
            detection_limit=Decimal("7"),
            uncertainty=None,
            detector=Detector.X0,
        ),
    )
    metadata = DatasetMetadata(
        source_name="fixture.xlsx",
        source_sha256="a" * 64,
        analytes=("Fe2O3", "PbO"),
        units=(MeasurementUnit.PERCENT, MeasurementUnit.PPM),
        analysis_count=1,
        measurement_count=2,
        detectors=(Detector.X0, Detector.X10),
        aliases=(("fer", "Fe2O3"), ("plomb", "PbO")),
        exclusions=(ExclusionSummary(ExclusionReason.BELOW_LOD, 1),),
        conventions=("analysis_id is opaque",),
    )
    return NormalizedDataset(
        analyses=analyses,
        measurements=measurements,
        metadata=metadata,
    )


def test_export_analysis_dataset_writes_stable_files(
    tmp_path: Path, dataset: NormalizedDataset
):
    first = export_analysis_dataset(dataset, tmp_path / "first")
    second = export_analysis_dataset(dataset, tmp_path / "second")

    assert first.analyses_csv.read_text(encoding="utf-8") == (
        "analysis_id,description\nopaque-1,Échantillon\n"
    )
    assert first.measurements_csv.read_text(encoding="utf-8").splitlines() == [
        (
            "analysis_id,analyte,value,unit,qualifier,detection_limit,"
            "uncertainty,detector"
        ),
        "opaque-1,Fe2O3,1.20,%,detected,,2.5,X10",
        "opaque-1,PbO,,ppm,below_lod,7,,X0",
    ]
    assert '"analysis_id is opaque"' in first.metadata_json.read_text(encoding="utf-8")

    for first_path, second_path in zip(
        (
            first.analyses_csv,
            first.measurements_csv,
            first.metadata_json,
        ),
        (
            second.analyses_csv,
            second.measurements_csv,
            second.metadata_json,
        ),
        strict=True,
    ):
        assert first_path.read_bytes() == second_path.read_bytes()


def test_temporary_analysis_dataset_removes_files_after_success(
    dataset: NormalizedDataset,
):
    with temporary_analysis_dataset(dataset) as export:
        root = export.analyses_csv.parent
        assert export.analyses_csv.exists()
        assert export.measurements_csv.exists()
        assert export.metadata_json.exists()

    assert not root.exists()


def test_temporary_analysis_dataset_removes_files_after_exception(
    dataset: NormalizedDataset,
):
    root: Path | None = None
    with pytest.raises(RuntimeError, match="stop"):
        with temporary_analysis_dataset(dataset) as export:
            root = export.analyses_csv.parent
            raise RuntimeError("stop")

    assert root is not None
    assert not root.exists()
