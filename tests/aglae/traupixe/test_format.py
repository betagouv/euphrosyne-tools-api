from pathlib import Path

import pytest

from aglae.traupixe.format import is_traupixe_path


@pytest.mark.parametrize(
    "name",
    [
        "TRAUPIXE-TACT_MAT-X0.xlsx",
        "traupixe-20260615-SRV.XLSX",
        "CONSO_IV_TRAUPIXE-20260605.xlsx",
    ],
)
def test_accepts_supported_filename_variants(name: str) -> None:
    assert is_traupixe_path(Path(name))


@pytest.mark.parametrize(
    "name",
    ["TRAUPIXE.xls", "results.xlsx"],
)
def test_rejects_unsupported_paths(name: str) -> None:
    assert not is_traupixe_path(Path(name))
