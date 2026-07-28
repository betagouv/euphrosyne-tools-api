import pytest

from aglae.traupixe.format import (
    ANALYTES,
    MAX_SOURCE_SIZE_BYTES,
    RAW_ANALYTE_HEADERS,
    REQUIRED_SHEETS,
    SELECTED_HEADERS_WITH_UNCERTAINTY,
    TRAUPIXE_FORMAT,
)
from aglae.traupixe.models import Detector, MeasurementUnit


def test_format_declares_the_exact_required_worksheets() -> None:
    assert TRAUPIXE_FORMAT.required_sheets == REQUIRED_SHEETS
    assert len(TRAUPIXE_FORMAT.required_sheets) == 17
    assert len(set(TRAUPIXE_FORMAT.required_sheets)) == 17


def test_format_declares_source_traceability_and_ignored_sheets() -> None:
    assert TRAUPIXE_FORMAT.source_sheets == (
        "S_Conc. & Unc %",
        "S_Conc. & Unc ppm",
        "S_Best Det.",
        "Exp. data",
    )
    assert TRAUPIXE_FORMAT.traceability_sheets == ("LOD", "Matrix")
    assert TRAUPIXE_FORMAT.ignored_sheets == (
        "S_Conc. ppm (RED)",
        "S_Conc. % (RED)",
    )
    assert {sheet for sheet in REQUIRED_SHEETS if "RED" in sheet} == set(
        TRAUPIXE_FORMAT.ignored_sheets
    )


def test_format_restricts_extension_and_source_size() -> None:
    assert TRAUPIXE_FORMAT.extension == ".xlsx"
    assert TRAUPIXE_FORMAT.maximum_source_size == MAX_SOURCE_SIZE_BYTES
    assert MAX_SOURCE_SIZE_BYTES == 100 * 1024 * 1024


def test_format_preserves_raw_analytes_and_exposes_canonical_names() -> None:
    assert len(RAW_ANALYTE_HEADERS) == 36
    assert len(ANALYTES) == 36
    assert RAW_ANALYTE_HEADERS[10] == "V "
    assert RAW_ANALYTE_HEADERS[25] == "Y "
    assert RAW_ANALYTE_HEADERS[32] == "W "
    assert ANALYTES[10] == "V"
    assert ANALYTES[25] == "Y"
    assert ANALYTES[32] == "W"
    assert TRAUPIXE_FORMAT.canonicalize_header("V ") == "V"
    assert TRAUPIXE_FORMAT.canonicalize_header("Fe2O3") == "Fe2O3"


def test_selected_headers_alternate_analyte_and_uncertainty() -> None:
    assert len(SELECTED_HEADERS_WITH_UNCERTAINTY) == 74
    assert SELECTED_HEADERS_WITH_UNCERTAINTY[:2] == (None, None)
    assert SELECTED_HEADERS_WITH_UNCERTAINTY[2:6] == (
        "Na2O",
        "Unc%",
        "MgO",
        "Unc%",
    )
    assert SELECTED_HEADERS_WITH_UNCERTAINTY[22:26] == (
        "V ",
        "Unc%",
        "Cr",
        "Unc%",
    )
    assert SELECTED_HEADERS_WITH_UNCERTAINTY[-4:] == (
        "PbO",
        "Unc%",
        "Bi",
        "Unc%",
    )


def test_format_uses_the_reference_header_rows() -> None:
    exp_data = TRAUPIXE_FORMAT.worksheet("Exp. data")
    selected = TRAUPIXE_FORMAT.worksheet("S_Conc. & Unc %")
    best_detector = TRAUPIXE_FORMAT.worksheet("S_Best Det.")
    information = TRAUPIXE_FORMAT.worksheet("Informations")
    matrix = TRAUPIXE_FORMAT.worksheet("Matrix")

    assert (exp_data.header_row, exp_data.data_start_row) == (1, 3)
    assert exp_data.headers[:3] == (None, None, "Mat * by ")
    assert (selected.header_row, selected.data_start_row) == (2, 3)
    assert selected.headers == SELECTED_HEADERS_WITH_UNCERTAINTY
    assert best_detector.headers == (None, None, *RAW_ANALYTE_HEADERS)
    assert information.header_row is None
    assert information.headers == ()
    assert information.data_start_row is None
    assert matrix.headers[2:5] == (11, 12, 13)
    assert matrix.headers[-1] == "8"


def test_format_declares_units_detectors_and_controlled_aliases() -> None:
    assert TRAUPIXE_FORMAT.units == (
        MeasurementUnit.PERCENT,
        MeasurementUnit.PPM,
    )
    assert TRAUPIXE_FORMAT.unit_sheets == (
        ("S_Conc. & Unc %", MeasurementUnit.PERCENT),
        ("S_Conc. & Unc ppm", MeasurementUnit.PPM),
    )
    assert TRAUPIXE_FORMAT.detectors == (Detector.X0, Detector.X10)
    assert dict(TRAUPIXE_FORMAT.detector_labels)[Detector.X0] == "LE0"
    assert dict(TRAUPIXE_FORMAT.analyte_aliases) == {
        "fer": "Fe2O3",
        "cuivre": "CuO",
        "plomb": "PbO",
        "silice": "SiO2",
    }
    assert set(dict(TRAUPIXE_FORMAT.analyte_aliases).values()) <= set(ANALYTES)


def test_unknown_worksheet_is_rejected() -> None:
    with pytest.raises(KeyError):
        TRAUPIXE_FORMAT.worksheet("Unknown")
