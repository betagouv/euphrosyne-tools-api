from __future__ import annotations

from dataclasses import dataclass

from .models import MeasurementUnit

MAX_SOURCE_SIZE_BYTES = 100 * 1024 * 1024

# These are the only worksheets consumed by the V1 dataset loader. A TRAUPIXE
# workbook may contain any number of additional calculation or traceability
# worksheets.
REQUIRED_SHEETS = (
    "S_Conc. & Unc %",
    "S_Conc. & Unc ppm",
    "S_Best Det.",
    "Exp. data",
)

# Reference analytes are retained for controlled aliases and synthetic fixtures.
# They are not an allow-list: every workbook declares its own analyte sequence.
RAW_ANALYTE_HEADERS = (
    "Na2O",
    "MgO",
    "Al2O3",
    "SiO2",
    "P2O5",
    "SO3",
    "Cl",
    "K2O",
    "CaO",
    "Ti",
    "V ",
    "Cr",
    "MnO",
    "Fe2O3",
    "Co",
    "Ni",
    "CuO",
    "Zn",
    "Ga",
    "Ge",
    "As",
    "Se",
    "Br",
    "Rb",
    "Sr",
    "Y ",
    "Zr",
    "Nb",
    "Mo",
    "Sn",
    "Sb",
    "Ba",
    "W ",
    "Au",
    "PbO",
    "Bi",
)
ANALYTES = tuple(header.rstrip() for header in RAW_ANALYTE_HEADERS)


@dataclass(frozen=True)
class WorksheetFormat:
    name: str
    header_row: int
    data_start_row: int


@dataclass(frozen=True)
class TraupixeFormat:
    extension: str
    maximum_source_size: int
    worksheets: tuple[WorksheetFormat, ...]
    reference_analytes: tuple[str, ...]
    units: tuple[MeasurementUnit, ...]
    unit_sheets: tuple[tuple[str, MeasurementUnit], ...]
    analyte_aliases: tuple[tuple[str, str], ...]

    @property
    def required_sheets(self) -> tuple[str, ...]:
        return tuple(worksheet.name for worksheet in self.worksheets)

    @property
    def source_sheets(self) -> tuple[str, ...]:
        return self.required_sheets

    def worksheet(self, name: str) -> WorksheetFormat:
        for worksheet in self.worksheets:
            if worksheet.name == name:
                return worksheet
        raise KeyError(name)

    @staticmethod
    def canonicalize_header(header: str) -> str:
        return header.strip()


TRAUPIXE_FORMAT = TraupixeFormat(
    extension=".xlsx",
    maximum_source_size=MAX_SOURCE_SIZE_BYTES,
    worksheets=tuple(
        WorksheetFormat(name=name, header_row=2, data_start_row=3)
        for name in REQUIRED_SHEETS
    ),
    reference_analytes=ANALYTES,
    units=(MeasurementUnit.PERCENT, MeasurementUnit.PPM),
    unit_sheets=(
        ("S_Conc. & Unc %", MeasurementUnit.PERCENT),
        ("S_Conc. & Unc ppm", MeasurementUnit.PPM),
    ),
    analyte_aliases=(
        ("fer", "Fe2O3"),
        ("cuivre", "CuO"),
        ("plomb", "PbO"),
        ("silice", "SiO2"),
    ),
)
