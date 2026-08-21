from functools import cache

from clients.albert import AlbertClient
from clients.azure.python_sessions import AzurePythonSessionsClient
from data_visualization.service import (
    DATA_VISUALIZATION_TIMEOUT_SECONDS,
    DataVisualizationService,
)


@cache
def get_data_visualization_service() -> DataVisualizationService:
    return DataVisualizationService(
        AlbertClient(timeout_seconds=DATA_VISUALIZATION_TIMEOUT_SECONDS),
        AzurePythonSessionsClient(
            execution_timeout_seconds=DATA_VISUALIZATION_TIMEOUT_SECONDS
        ),
    )
