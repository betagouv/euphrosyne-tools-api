from __future__ import annotations

from collections import Counter
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from aglae.traupixe.exceptions import TraupixeNormalizationError
from aglae.traupixe.loader import load_traupixe_workbook
from aglae.traupixe.models import (
    Detector,
    ExclusionReason,
    MeasurementQualifier,
    MeasurementUnit,
)
from aglae.traupixe.normalization import (
    apply_detection_limits,
    normalize_detectors,
    normalize_traupixe,
    resolve_analytes,
)
from tests.aglae.traupixe.fixture_factory import write_traupixe_fixture


def test_normalization_uses_decimal_and_classifies_special_values(
    tmp_path: Path,
) -> None:
    analysis_id = "opaque-analysis"
    source = write_traupixe_fixture(
        tmp_path / "source.xlsx",
        analysis_ids=(analysis_id,),
        values={
            (
                analysis_id,
                "Na2O",
                MeasurementUnit.PERCENT,
            ): ("1,25", "2,50"),
            (
                analysis_id,
                "MgO",
                MeasurementUnit.PERCENT,
            ): ("<0,00070", "n.d."),
            (
                analysis_id,
                "Al2O3",
                MeasurementUnit.PERCENT,
            ): ("n.d.", "n.d."),
            (
                analysis_id,
                "SiO2",
                MeasurementUnit.PERCENT,
            ): (None, None),
            (
                analysis_id,
                "P2O5",
                MeasurementUnit.PERCENT,
            ): ("999999", "2,5"),
            (
                analysis_id,
                "SO3",
                MeasurementUnit.PERCENT,
            ): ("invalid", "2,5"),
            (
                analysis_id,
                "Cl",
                MeasurementUnit.PERCENT,
            ): ("0,75", "invalid"),
        },
    )

    dataset = normalize_traupixe(load_traupixe_workbook(source))
    measurements = {
        (measurement.analyte, measurement.unit): measurement
        for measurement in dataset.measurements
    }

    detected = measurements[("Na2O", MeasurementUnit.PERCENT)]
    assert detected.value == Decimal("1.25")
    assert detected.uncertainty == Decimal("2.50")
    assert detected.qualifier is MeasurementQualifier.DETECTED

    below_lod = measurements[("MgO", MeasurementUnit.PERCENT)]
    assert below_lod.value is None
    assert below_lod.detection_limit == Decimal("0.00070")
    assert below_lod.uncertainty is None
    assert below_lod.qualifier is MeasurementQualifier.BELOW_LOD

    for analyte in ("Al2O3", "SiO2", "P2O5", "SO3"):
        assert (
            measurements[(analyte, MeasurementUnit.PERCENT)].qualifier
            is MeasurementQualifier.MISSING
        )
        assert measurements[(analyte, MeasurementUnit.PERCENT)].value is None

    invalid_uncertainty = measurements[("Cl", MeasurementUnit.PERCENT)]
    assert invalid_uncertainty.qualifier is MeasurementQualifier.DETECTED
    assert invalid_uncertainty.value == Decimal("0.75")
    assert invalid_uncertainty.uncertainty is None

    exclusions = {
        exclusion.reason: exclusion.count for exclusion in dataset.metadata.exclusions
    }
    assert exclusions == {
        ExclusionReason.EMPTY: 1,
        ExclusionReason.NOT_DETECTED: 1,
        ExclusionReason.SENTINEL: 1,
        ExclusionReason.BELOW_LOD: 1,
        ExclusionReason.INVALID_VALUE: 2,
    }


def test_normalization_preserves_deterministic_analysis_analyte_unit_order(
    tmp_path: Path,
) -> None:
    source = write_traupixe_fixture(
        tmp_path / "source.xlsx",
        row_orders={
            "S_Conc. & Unc %": (
                "opaque-analysis-b",
                "opaque-analysis-a",
            ),
        },
    )

    dataset = normalize_traupixe(load_traupixe_workbook(source))

    assert [
        (
            measurement.analysis_id,
            measurement.analyte,
            measurement.unit,
        )
        for measurement in dataset.measurements[:4]
    ] == [
        (
            "opaque-analysis-b",
            "Na2O",
            MeasurementUnit.PERCENT,
        ),
        (
            "opaque-analysis-b",
            "Na2O",
            MeasurementUnit.PPM,
        ),
        (
            "opaque-analysis-b",
            "MgO",
            MeasurementUnit.PERCENT,
        ),
        (
            "opaque-analysis-b",
            "MgO",
            MeasurementUnit.PPM,
        ),
    ]
    assert dataset.metadata.analysis_count == 2
    assert dataset.metadata.measurement_count == 144
    assert dataset.metadata.analytes[10] == "V"
    assert dataset.metadata.analytes[25] == "Y"
    assert dataset.metadata.analytes[32] == "W"


def test_reference_fixture_normalizes_expected_counts() -> None:
    source = Path(__file__).parent / "fixtures" / "traupixe_reference_anonymized.xlsx"

    dataset = normalize_traupixe(load_traupixe_workbook(source))
    below_lod_by_unit = Counter(
        measurement.unit
        for measurement in dataset.measurements
        if measurement.qualifier is MeasurementQualifier.BELOW_LOD
    )

    assert len(dataset.analyses) == 48
    assert len(dataset.metadata.analytes) == 36
    assert len(dataset.measurements) == 3456
    assert below_lod_by_unit == {
        MeasurementUnit.PERCENT: 299,
        MeasurementUnit.PPM: 299,
    }
    assert dataset.metadata.detectors == (Detector.X0, Detector.X10)


def test_controlled_aliases_resolve_only_to_available_analytes() -> None:
    assert resolve_analytes(
        ("fer", "CuO", "plomb", "silice", "fer"),
    ) == ("Fe2O3", "CuO", "PbO", "SiO2")

    with pytest.raises(TraupixeNormalizationError):
        resolve_analytes(
            "fer",
            available_analytes=("CuO", "PbO"),
        )
    with pytest.raises(TraupixeNormalizationError):
        resolve_analytes("titane")


def test_detector_normalization_accepts_only_x0_and_x10() -> None:
    assert normalize_detectors(("X10", Detector.X0, "X10")) == (
        Detector.X0,
        Detector.X10,
    )

    with pytest.raises(TraupixeNormalizationError):
        normalize_detectors(("X1",))


@pytest.mark.parametrize(
    ("raw_value", "qualifier", "value", "detection_limit"),
    [
        (
            "1 234,50",
            MeasurementQualifier.DETECTED,
            Decimal("1234.50"),
            None,
        ),
        (
            "<7",
            MeasurementQualifier.BELOW_LOD,
            None,
            Decimal("7"),
        ),
        ("n.d.", MeasurementQualifier.MISSING, None, None),
        ("999999", MeasurementQualifier.MISSING, None, None),
        ("", MeasurementQualifier.MISSING, None, None),
    ],
)
def test_detection_limit_rules(
    raw_value: object,
    qualifier: MeasurementQualifier,
    value: Decimal | None,
    detection_limit: Decimal | None,
) -> None:
    result = apply_detection_limits(raw_value)

    assert result.qualifier is qualifier
    assert result.value == value
    assert result.detection_limit == detection_limit


def test_normalization_rejects_an_incomplete_measurement_grid(
    tmp_path: Path,
) -> None:
    source = write_traupixe_fixture(tmp_path / "source.xlsx")
    loaded = load_traupixe_workbook(source)
    incomplete = replace(
        loaded,
        measurements=loaded.measurements[:-1],
    )

    with pytest.raises(TraupixeNormalizationError):
        normalize_traupixe(incomplete)


def test_normalization_rejects_a_same_size_but_incorrect_measurement_grid(
    tmp_path: Path,
) -> None:
    source = write_traupixe_fixture(tmp_path / "source.xlsx")
    loaded = load_traupixe_workbook(source)
    incorrect_measurement = replace(
        loaded.measurements[0],
        analyte="Unknown analyte",
    )
    incorrect = replace(
        loaded,
        measurements=(incorrect_measurement, *loaded.measurements[1:]),
    )

    with pytest.raises(
        TraupixeNormalizationError,
        match="analyses × analytes × units",
    ):
        normalize_traupixe(incorrect)
