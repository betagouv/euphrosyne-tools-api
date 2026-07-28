from __future__ import annotations

from dataclasses import dataclass

from .models import Detector, MeasurementUnit

MAX_SOURCE_SIZE_BYTES = 100 * 1024 * 1024
HeaderValue = str | int | None

REQUIRED_SHEETS = (
    "Exp. data",
    "Elemental Conc.",
    "Oxide Conc.",
    "LOD",
    "S_Conc. & Unc ppm",
    "S_Conc. & Unc %",
    "S_Conc. ppm",
    "S_Conc. %",
    "S_Conc. ppm (RED)",
    "S_Conc. % (RED)",
    "Total Unc",
    "Fit-Error",
    "Peak Height",
    "Area",
    "Matrix",
    "Informations",
    "S_Best Det.",
)

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

EXP_DATA_HEADERS: tuple[HeaderValue, ...] = (
    None,
    None,
    "Mat * by ",
    "Q (X10)",
    "Av.(nA) X0",
    "Av.(nA) X10)",
    "Chi2 (X0)",
    "Chi2 (X10)",
    "Res. (X0)",
    "Res. (X10)",
    "Cnt/sec. X0)",
    "Cnt/sec. (X10)",
    "Z Pivot (X10)",
    "Filters (X0)",
    "Filters (X10)",
    "Par file (X0)",
    ".par file (X10)",
)

ELEMENTAL_HEADERS: tuple[HeaderValue, ...] = (
    None,
    None,
    "Na",
    "Mg",
    "Al",
    "Si",
    "P ",
    "S ",
    "Cl",
    "K ",
    "Ca",
    "Ti",
    "V ",
    "Cr",
    "Mn",
    "Fe",
    "Co",
    "Ni",
    "Cu",
    "Zn",
    "Sr",
    "Zr",
    "Sn # K *",
    "Sb # K *",
    "Ba # K *",
    "Sn # LA",
    "Sb # LA",
    "Ba # LA",
    "W  # LA",
    "Pb # LA",
    "Bi # LA",
    "Pb # MA*",
    "K ",
    "Ca",
    "Ti",
    "V ",
    "Cr",
    "Mn",
    "Fe",
    "Co",
    "Ni",
    "Cu",
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
    "Sn # LG",
    "Sb # LB",
    "Ba # LB",
    "W  # LA",
    "Au # LA",
    "Pb # LA",
    "Bi # LA",
)

OXIDE_HEADERS: tuple[HeaderValue, ...] = (
    None,
    None,
    "Na2O",
    "MgO",
    "Al2O3",
    "SiO2",
    "P2O5",
    "SO3",
    "Cl",
    "K2O",
    "CaO",
    "TiO2",
    "V2O3",
    "Cr2O3",
    "MnO",
    "Fe2O3",
    "CoO",
    "NiO",
    "CuO",
    "ZnO",
    "SrO",
    "ZrO2",
    "SnO2 # K *",
    "Sb2O5 # K *",
    "BaO # K *",
    "SnO2 # LA",
    "Sb2O5 # LA",
    "BaO # LA",
    "WO3 # LA",
    "PbO # LA",
    "Bi2O3 # LA",
    "PbO # MA*",
    "K2O",
    "CaO",
    "TiO2",
    "V2O3",
    "Cr2O3",
    "MnO",
    "Fe2O3",
    "CoO",
    "NiO",
    "CuO",
    "ZnO",
    "Ga2O3",
    "GeO2",
    "As2O5",
    "SeO2",
    "Br",
    "Rb2O",
    "SrO",
    "Y2O3",
    "ZrO2",
    "Nb2O5",
    "MoO3",
    "SnO2",
    "Sb2O5",
    "BaO",
    "SnO2 # LG",
    "Sb2O5 # LB",
    "BaO # LB",
    "WO3 # LA",
    "Au2O3 # LA",
    "PbO # LA",
    "Bi2O3 # LA",
)

MATRIX_HEADERS: tuple[HeaderValue, ...] = (
    None,
    None,
    11,
    12,
    13,
    14,
    15,
    16,
    17,
    19,
    20,
    22,
    23,
    24,
    25,
    26,
    27,
    28,
    29,
    30,
    38,
    40,
    50,
    51,
    56,
    50,
    51,
    56,
    74,
    82,
    83,
    82,
    "8",
)


def _selected_headers_with_uncertainty() -> tuple[HeaderValue, ...]:
    headers: list[HeaderValue] = [None, None]
    for analyte in RAW_ANALYTE_HEADERS:
        headers.extend((analyte, "Unc%"))
    return tuple(headers)


SELECTED_HEADERS_WITH_UNCERTAINTY = _selected_headers_with_uncertainty()
SELECTED_HEADERS = (None, None, *RAW_ANALYTE_HEADERS)


def _structural_row(
    width: int,
    *values: tuple[int, HeaderValue],
) -> tuple[HeaderValue, ...]:
    row: list[HeaderValue] = [None] * width
    for column, value in values:
        row[column - 1] = value
    return tuple(row)


EXP_DATA_EMPTY_ROW = _structural_row(len(EXP_DATA_HEADERS))
ELEMENTAL_X0_X10_ROW = _structural_row(
    len(ELEMENTAL_HEADERS),
    (3, "X0"),
    (33, "X10"),
)
ELEMENTAL_X10_ROW = _structural_row(
    len(ELEMENTAL_HEADERS),
    (33, "X10"),
)
SELECTED_EMPTY_ROW = _structural_row(len(SELECTED_HEADERS))
SELECTED_WITH_UNCERTAINTY_EMPTY_ROW = _structural_row(
    len(SELECTED_HEADERS_WITH_UNCERTAINTY)
)
MATRIX_EMPTY_ROW = _structural_row(len(MATRIX_HEADERS))


@dataclass(frozen=True)
class WorksheetFormat:
    name: str
    header_row: int | None
    headers: tuple[HeaderValue, ...]
    data_start_row: int | None
    structural_rows: tuple[tuple[int, tuple[HeaderValue, ...]], ...] = ()
    must_be_empty: bool = False


@dataclass(frozen=True)
class TraupixeFormat:
    extension: str
    maximum_source_size: int
    worksheets: tuple[WorksheetFormat, ...]
    source_sheets: tuple[str, ...]
    traceability_sheets: tuple[str, ...]
    ignored_sheets: tuple[str, ...]
    raw_analytes: tuple[str, ...]
    analytes: tuple[str, ...]
    column_aliases: tuple[tuple[str, str], ...]
    units: tuple[MeasurementUnit, ...]
    unit_sheets: tuple[tuple[str, MeasurementUnit], ...]
    detectors: tuple[Detector, ...]
    detector_labels: tuple[tuple[Detector, str], ...]
    analyte_aliases: tuple[tuple[str, str], ...]

    @property
    def required_sheets(self) -> tuple[str, ...]:
        return tuple(worksheet.name for worksheet in self.worksheets)

    def worksheet(self, name: str) -> WorksheetFormat:
        for worksheet in self.worksheets:
            if worksheet.name == name:
                return worksheet
        raise KeyError(name)

    def canonicalize_header(self, header: str) -> str:
        for raw_header, canonical_header in self.column_aliases:
            if header == raw_header:
                return canonical_header
        return header


TRAUPIXE_FORMAT = TraupixeFormat(
    extension=".xlsx",
    maximum_source_size=MAX_SOURCE_SIZE_BYTES,
    worksheets=(
        WorksheetFormat(
            "Exp. data",
            1,
            EXP_DATA_HEADERS,
            3,
            structural_rows=((2, EXP_DATA_EMPTY_ROW),),
        ),
        WorksheetFormat(
            "Elemental Conc.",
            2,
            ELEMENTAL_HEADERS,
            3,
            structural_rows=((1, ELEMENTAL_X0_X10_ROW),),
        ),
        WorksheetFormat(
            "Oxide Conc.",
            2,
            OXIDE_HEADERS,
            3,
            structural_rows=((1, ELEMENTAL_X10_ROW),),
        ),
        WorksheetFormat(
            "LOD",
            2,
            ELEMENTAL_HEADERS,
            3,
            structural_rows=((1, ELEMENTAL_X10_ROW),),
        ),
        WorksheetFormat(
            "S_Conc. & Unc ppm",
            2,
            SELECTED_HEADERS_WITH_UNCERTAINTY,
            3,
            structural_rows=((1, SELECTED_WITH_UNCERTAINTY_EMPTY_ROW),),
        ),
        WorksheetFormat(
            "S_Conc. & Unc %",
            2,
            SELECTED_HEADERS_WITH_UNCERTAINTY,
            3,
            structural_rows=((1, SELECTED_WITH_UNCERTAINTY_EMPTY_ROW),),
        ),
        WorksheetFormat(
            "S_Conc. ppm",
            2,
            SELECTED_HEADERS,
            3,
            structural_rows=((1, SELECTED_EMPTY_ROW),),
        ),
        WorksheetFormat(
            "S_Conc. %",
            2,
            SELECTED_HEADERS,
            3,
            structural_rows=((1, SELECTED_EMPTY_ROW),),
        ),
        WorksheetFormat(
            "S_Conc. ppm (RED)",
            2,
            SELECTED_HEADERS,
            3,
            structural_rows=((1, SELECTED_EMPTY_ROW),),
        ),
        WorksheetFormat(
            "S_Conc. % (RED)",
            2,
            SELECTED_HEADERS,
            3,
            structural_rows=((1, SELECTED_EMPTY_ROW),),
        ),
        WorksheetFormat(
            "Total Unc",
            2,
            ELEMENTAL_HEADERS,
            3,
            structural_rows=((1, ELEMENTAL_X0_X10_ROW),),
        ),
        WorksheetFormat(
            "Fit-Error",
            2,
            ELEMENTAL_HEADERS,
            3,
            structural_rows=((1, ELEMENTAL_X10_ROW),),
        ),
        WorksheetFormat(
            "Peak Height",
            2,
            ELEMENTAL_HEADERS,
            3,
            structural_rows=((1, ELEMENTAL_X10_ROW),),
        ),
        WorksheetFormat(
            "Area",
            2,
            ELEMENTAL_HEADERS,
            3,
            structural_rows=((1, ELEMENTAL_X10_ROW),),
        ),
        WorksheetFormat(
            "Matrix",
            2,
            MATRIX_HEADERS,
            3,
            structural_rows=((1, MATRIX_EMPTY_ROW),),
        ),
        WorksheetFormat(
            "Informations",
            None,
            (),
            None,
            must_be_empty=True,
        ),
        WorksheetFormat(
            "S_Best Det.",
            2,
            SELECTED_HEADERS,
            3,
            structural_rows=((1, SELECTED_EMPTY_ROW),),
        ),
    ),
    source_sheets=(
        "S_Conc. & Unc %",
        "S_Conc. & Unc ppm",
        "S_Best Det.",
        "Exp. data",
    ),
    traceability_sheets=("LOD", "Matrix"),
    ignored_sheets=("S_Conc. ppm (RED)", "S_Conc. % (RED)"),
    raw_analytes=RAW_ANALYTE_HEADERS,
    analytes=ANALYTES,
    column_aliases=(("V ", "V"), ("Y ", "Y"), ("W ", "W")),
    units=(MeasurementUnit.PERCENT, MeasurementUnit.PPM),
    unit_sheets=(
        ("S_Conc. & Unc %", MeasurementUnit.PERCENT),
        ("S_Conc. & Unc ppm", MeasurementUnit.PPM),
    ),
    detectors=(Detector.X0, Detector.X10),
    detector_labels=(
        (Detector.X0, "LE0"),
        (Detector.X10, "X1, X2, X3 and X4 combined by TRAUPIXE"),
    ),
    analyte_aliases=(
        ("fer", "Fe2O3"),
        ("cuivre", "CuO"),
        ("plomb", "PbO"),
        ("silice", "SiO2"),
    ),
)
