import json

from aglae.traupixe.format import MAX_SOURCE_SIZE_BYTES
from aglae.traupixe.visualization import (
    DATASET_FILENAME,
    TraupixeVisualizationHandler,
)


def test_prepares_a_normalized_traupixe_visualization_dataset(
    traupixe_workbook: bytes,
) -> None:
    handler = TraupixeVisualizationHandler()

    prepared = handler.prepare(traupixe_workbook)

    assert handler.max_source_size_bytes == MAX_SOURCE_SIZE_BYTES
    assert prepared.filename == DATASET_FILENAME
    assert json.loads(prepared.content)["analyses"]
    assert prepared.descriptor["analyses"]["total"] > 0
    assert 'data["analytes"]' in prepared.calculation_instructions
    assert 'a["kind"] != "reference"' in prepared.calculation_instructions
    assert 'or data["analyses"]' in prepared.calculation_instructions
    assert "values[row_index][column_index]" in prepared.calculation_instructions
    assert "liste plate cells" in prepared.calculation_instructions
    assert "matrice de détecteurs TRAUPIXE" in prepared.visualization_instructions
