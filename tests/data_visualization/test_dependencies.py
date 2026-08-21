from unittest.mock import patch

import pytest

from data_visualization import dependencies


@pytest.fixture(autouse=True)
def clear_dependency_cache():
    dependencies.get_data_visualization_service.cache_clear()
    yield
    dependencies.get_data_visualization_service.cache_clear()


def test_get_data_visualization_service_composes_configured_clients():
    llm = object()
    sessions = object()
    service = object()

    with (
        patch.object(dependencies, "AlbertClient", return_value=llm) as albert_class,
        patch.object(
            dependencies,
            "AzurePythonSessionsClient",
            return_value=sessions,
        ) as sessions_class,
        patch.object(
            dependencies,
            "DataVisualizationService",
            return_value=service,
        ) as service_class,
    ):
        result = dependencies.get_data_visualization_service()

    assert result is service
    albert_class.assert_called_once_with(
        timeout_seconds=dependencies.DATA_VISUALIZATION_TIMEOUT_SECONDS
    )
    sessions_class.assert_called_once_with(
        execution_timeout_seconds=dependencies.DATA_VISUALIZATION_TIMEOUT_SECONDS
    )
    service_class.assert_called_once_with(llm, sessions)
