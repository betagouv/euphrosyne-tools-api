from __future__ import annotations

from pathlib import Path
from typing import Protocol

from aglae.traupixe.format import is_traupixe_path
from aglae.traupixe.visualization import TraupixeVisualizationHandler
from data_visualization.service import PreparedDataVisualization


class UnsupportedDataVisualizationFile(ValueError):
    """Raised when no visualization handler supports a selected file."""


class DataVisualizationHandler(Protocol):
    max_source_size_bytes: int

    def prepare(self, content: bytes) -> PreparedDataVisualization: ...


def resolve_data_visualization_handler(path: Path) -> DataVisualizationHandler:
    if is_traupixe_path(path):
        return TraupixeVisualizationHandler()
    raise UnsupportedDataVisualizationFile(f"Unsupported visualization file: {path}")
