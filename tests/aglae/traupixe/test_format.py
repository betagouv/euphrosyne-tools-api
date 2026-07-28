import pytest

from aglae.traupixe.format import (
    ANALYTES,
    MAX_SOURCE_SIZE_BYTES,
    RAW_ANALYTE_HEADERS,
    REQUIRED_SHEETS,
    TRAUPIXE_FORMAT,
)
from aglae.traupixe.models import MeasurementUnit


def test_format_requires_only_the_consumed_worksheets() -> None:
    assert TRAUPIXE_FORMAT.required_sheets == REQUIRED_SHEETS
    assert TRAUPIXE_FORMAT.source_sheets == REQUIRED_SHEETS
    assert REQUIRED_SHEETS == (
        "S_Conc. & Unc %",
        "S_Conc. & Unc ppm",
        "S_Best Det.",
        "Exp. data",
    )


def test_format_restricts_extension_and_source_size() -> None:
    assert TRAUPIXE_FORMAT.extension == ".xlsx"
    assert TRAUPIXE_FORMAT.maximum_source_size == MAX_SOURCE_SIZE_BYTES
    assert MAX_SOURCE_SIZE_BYTES == 100 * 1024 * 1024


def test_reference_analytes_are_not_part_of_the_validation_contract() -> None:
    assert len(RAW_ANALYTE_HEADERS) == 36
    assert len(ANALYTES) == 36
    assert TRAUPIXE_FORMAT.reference_analytes == ANALYTES
    assert TRAUPIXE_FORMAT.canonicalize_header("V ") == "V"
    assert TRAUPIXE_FORMAT.canonicalize_header(" Bi2O3 ") == "Bi2O3"


def test_format_declares_dynamic_source_layouts_and_units() -> None:
    assert {
        (
            worksheet.name,
            worksheet.header_row,
            worksheet.data_start_row,
        )
        for worksheet in TRAUPIXE_FORMAT.worksheets
    } == {
        ("S_Conc. & Unc %", 2, 3),
        ("S_Conc. & Unc ppm", 2, 3),
        ("S_Best Det.", 2, 3),
        ("Exp. data", 2, 3),
    }
    assert TRAUPIXE_FORMAT.units == (
        MeasurementUnit.PERCENT,
        MeasurementUnit.PPM,
    )
    assert TRAUPIXE_FORMAT.unit_sheets == (
        ("S_Conc. & Unc %", MeasurementUnit.PERCENT),
        ("S_Conc. & Unc ppm", MeasurementUnit.PPM),
    )
    assert dict(TRAUPIXE_FORMAT.analyte_aliases) == {
        "fer": "Fe2O3",
        "cuivre": "CuO",
        "plomb": "PbO",
        "silice": "SiO2",
    }


def test_unknown_worksheet_is_rejected() -> None:
    with pytest.raises(KeyError):
        TRAUPIXE_FORMAT.worksheet("Unknown")
