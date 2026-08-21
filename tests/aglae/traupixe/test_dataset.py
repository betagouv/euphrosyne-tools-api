from io import BytesIO

import pytest
from openpyxl import Workbook

from aglae.traupixe.dataset import (
    TraupixeDatasetError,
    load_traupixe_dataset,
    serialize_traupixe_for_model,
)


def test_interprets_concentrations_detections_zones_and_detectors(
    traupixe_workbook: bytes,
) -> None:
    dataset = load_traupixe_dataset(traupixe_workbook)

    assert dataset.concentration_sheet == "S_Conc. %"
    assert dataset.detector_sheet == "S_Best Det."
    assert dataset.unit == "%"
    assert len(dataset.analyses) == 5
    assert len(dataset.object_analyses) == 4
    assert [analysis.zone for analysis in dataset.object_analyses] == [
        "Zone_A",
        "Zone_A",
        "Zone_B",
        "Zone_B",
    ]
    zone_a_second = dataset.object_analyses[1]
    assert zone_a_second.measurements["Na2O-PIGE"].value == 4
    assert zone_a_second.measurements["Zr"].detected is False
    assert zone_a_second.measurements["Zr"].value == pytest.approx(0.2)
    assert zone_a_second.measurements["Zr"].detector in {"X1", "X3"}


def test_builds_a_compact_descriptor_without_measurement_values(
    traupixe_workbook: bytes,
) -> None:
    descriptor = load_traupixe_dataset(traupixe_workbook).descriptor()

    assert descriptor["analyses"]["objects"] == 4
    assert descriptor["default_major_analytes"] == [
        "Na2O-PIGE",
        "MgO",
        "Al2O3",
        "SiO2",
        "K2O",
        "CaO",
    ]
    assert descriptor["analyses"]["zones"] == [
        {"name": "Zone_A", "analyses": 2},
        {"name": "Zone_B", "analyses": 2},
    ]
    by_analyte = {analyte["name"]: analyte for analyte in descriptor["analytes"]}
    assert by_analyte["Zr"]["detection_rate"] == 0.75
    assert by_analyte["Ge"]["detection_rate"] == 0.25
    assert "value" not in str(descriptor)


def test_serializes_an_aligned_compact_model_payload(
    traupixe_workbook: bytes,
) -> None:
    dataset = load_traupixe_dataset(traupixe_workbook)

    payload = serialize_traupixe_for_model(dataset)

    assert payload["unit"] == "%"
    assert payload["summary"]["objects"] == 4
    assert len(payload["analyses"]) == len(dataset.analyses)
    assert all(
        len(analysis[key]) == len(payload["analytes"])
        for analysis in payload["analyses"]
        for key in ("values", "detected", "detectors")
    )
    assert {analysis["kind"] for analysis in payload["analyses"]} == {
        "object",
        "reference",
    }


def test_rejects_a_workbook_without_a_supported_concentration_sheet() -> None:
    workbook = Workbook()
    active_sheet = workbook.active
    assert active_sheet is not None
    active_sheet.title = "Other"
    output = BytesIO()
    workbook.save(output)
    workbook.close()

    with pytest.raises(TraupixeDatasetError, match="concentration sheet"):
        load_traupixe_dataset(output.getvalue())
