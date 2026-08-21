from io import BytesIO

import pytest
from openpyxl import Workbook

ANALYTES = [
    "Na2O-PIGE",
    "MgO",
    "Al2O3",
    "SiO2",
    "K2O",
    "CaO",
    "Ti",
    "Zr",
    "Co",
    "Ni",
    "CuO",
    "Fe2O3",
    "MnO",
    "Ge",
    "Au",
]


@pytest.fixture
def traupixe_workbook() -> bytes:
    workbook = Workbook()
    concentrations = workbook.active
    assert concentrations is not None
    concentrations.title = "S_Conc. %"
    concentrations.append([None, None, *([None] * len(ANALYTES))])
    concentrations.append([None, None, *ANALYTES])
    rows = [
        ("run_0001_STD_project", "Standard", "1,0"),
        ("run_0002_OBJ_project", "Zone_A_pt1", "2,0"),
        ("run_0003_OBJ_project", "Zone_A_pt2", "4,0"),
        ("run_0004_OBJ_project", "Zone_B_pt1", "6,0"),
        ("run_0005_OBJ_project", "Zone_B_pt2", "8,0"),
    ]
    for index, (identifier, label, sodium) in enumerate(rows):
        traces = [
            "0,10",
            "0,20" if index != 2 else "<0,20",
            "0,30" if index != 3 else "<0,30",
            "0,40",
            "0,50",
            "1,00",
            "0,60",
            "0,01" if index == 1 else "<0,01",
            "0,02" if index == 1 else "<0,02",
        ]
        concentrations.append(
            [
                identifier,
                label,
                sodium,
                "1,0",
                "3,0",
                "70,0",
                "1,0",
                "10,0",
                *traces,
            ]
        )

    detectors = workbook.create_sheet("S_Best Det.")
    detectors.append([None, None, *([None] * len(ANALYTES))])
    detectors.append([None, None, *ANALYTES])
    for index, (identifier, label, _) in enumerate(rows):
        detectors.append(
            [
                identifier,
                label,
                *(["X0"] * 6),
                *("X1" if (index + column) % 2 else "X3" for column in range(9)),
            ]
        )

    output = BytesIO()
    workbook.save(output)
    workbook.close()
    return output.getvalue()
