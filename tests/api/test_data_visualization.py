from __future__ import annotations

import io
import logging
from collections.abc import Iterator
from unittest.mock import MagicMock, patch
from uuid import UUID

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from aglae.traupixe.format import MAX_SOURCE_SIZE_BYTES
from aglae.traupixe.visualization import TraupixeVisualizationHandler
from clients.data_client import AbstractDataClient
from data_visualization.dependencies import get_data_visualization_service
from data_visualization.exchange_log import (
    DataVisualizationExchangeLogger,
    get_data_visualization_exchange_logger,
)
from data_visualization.models import DataVisualization, PreparedDataVisualization
from data_visualization.service import (
    DataVisualizationError,
    DataVisualizationResult,
    DataVisualizationService,
)
from dependencies import get_project_data_client

PROJECT_SLUG = "project-01"
WORKBOOK_PATH = "projects/project-01/runs/run-01/raw_data/" "TRAUPIXE-example.xlsx"
ENDPOINT = f"/data/{PROJECT_SLUG}/visualizations"


@pytest.fixture
def visualization_dependencies(
    app: FastAPI,
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[tuple[MagicMock, MagicMock, MagicMock]]:
    monkeypatch.setenv("DATA_PROJECTS_LOCATION_PREFIX", "projects")
    data_client = MagicMock(spec=AbstractDataClient)
    data_client.download_run_file.return_value = io.BytesIO(b"data-file")
    visualization_service = MagicMock(spec=DataVisualizationService)
    prepare = MagicMock(return_value=_prepared())
    monkeypatch.setattr(TraupixeVisualizationHandler, "prepare", prepare)
    previous_data = app.dependency_overrides.get(get_project_data_client)
    previous_service = app.dependency_overrides.get(get_data_visualization_service)
    app.dependency_overrides[get_project_data_client] = lambda: data_client
    app.dependency_overrides[get_data_visualization_service] = (
        lambda: visualization_service
    )
    yield data_client, visualization_service, prepare
    if previous_data is None:
        app.dependency_overrides.pop(get_project_data_client, None)
    else:
        app.dependency_overrides[get_project_data_client] = previous_data
    if previous_service is None:
        app.dependency_overrides.pop(get_data_visualization_service, None)
    else:
        app.dependency_overrides[get_data_visualization_service] = previous_service


@pytest.fixture(autouse=True)
def data_visualization_exchange_logger() -> Iterator[MagicMock]:
    exchange_logger = MagicMock(spec=DataVisualizationExchangeLogger)
    with patch(
        "api.data_visualization.get_data_visualization_exchange_logger",
        return_value=exchange_logger,
    ):
        yield exchange_logger


def _visualization() -> DataVisualization:
    return DataVisualization.model_validate(
        {
            "title": "Fer",
            "option": {
                "title": {"text": "Fer"},
                "xAxis": {"type": "category", "data": ["Ligne Excel 3"]},
                "yAxis": {"type": "value"},
                "series": [
                    {
                        "name": "Fe",
                        "type": "bar",
                        "data": [10],
                    }
                ],
            },
        }
    )


def _prepared() -> PreparedDataVisualization:
    return PreparedDataVisualization(
        filename="traupixe.json",
        content=b"{}",
        descriptor={"format": "TRAUPIXE"},
    )


def _result() -> DataVisualizationResult:
    return DataVisualizationResult(
        answer="Réponse du modèle",
        visualizations=(_visualization(),),
        elapsed_seconds=1.25,
        llm_calls=1,
        usage=({"total_tokens": 300},),
    )


def _post(client: TestClient, **overrides: object):
    body = {
        "path": WORKBOOK_PATH,
        "question": "Compare le fer.",
        **overrides,
    }
    return client.post(ENDPOINT, json=body)


def test_creates_a_visualization_from_the_selected_project_file(
    client: TestClient,
    visualization_dependencies: tuple[MagicMock, MagicMock, MagicMock],
    data_visualization_exchange_logger: MagicMock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    data_client, visualization_service, prepare = visualization_dependencies

    def run(
        prepared: PreparedDataVisualization,
        question: str,
        *,
        exchange_logger,
    ) -> DataVisualizationResult:
        exchange_logger.info(
            "llm_request",
            extra={
                "exchange": {
                    "messages": [{"role": "user", "content": question}],
                }
            },
        )
        exchange_logger.info(
            "visualization_result",
            extra={
                "exchange": {
                    "visualization": _visualization().model_dump(mode="json"),
                }
            },
        )
        assert prepared == _prepared()
        return _result()

    visualization_service.run.side_effect = run
    with caplog.at_level(logging.INFO, logger="api.data_visualization"):
        response = _post(client)

    assert response.status_code == 200
    payload = response.json()
    UUID(payload["request_id"])
    assert response.headers["X-Request-ID"] == payload["request_id"]
    assert payload["answer"] == "Réponse du modèle"
    assert payload["visualizations"] == [_visualization().model_dump(mode="json")]
    assert "visualization" not in payload
    data_client.download_run_file.assert_called_once_with(WORKBOOK_PATH)
    prepare.assert_called_once_with(b"data-file")
    visualization_service.run.assert_called_once()
    assert "data_visualization_completed" in caplog.text
    assert "total_tokens=300" in caplog.text
    assert "question='Compare le fer.'" in caplog.text
    assert "Ligne Excel 3" not in caplog.text
    exchange_calls = data_visualization_exchange_logger.info.call_args_list
    assert [call.args[0] for call in exchange_calls] == [
        "request_started",
        "llm_request",
        "visualization_result",
        "request_completed",
    ]
    exchange_details = [call.kwargs["extra"]["exchange"] for call in exchange_calls]
    assert exchange_details[2]["visualization"]["option"] == (_visualization().option)


def test_rejects_a_file_from_another_project(
    client: TestClient,
    visualization_dependencies: tuple[MagicMock, MagicMock, MagicMock],
) -> None:
    data_client, _, _ = visualization_dependencies

    response = _post(
        client,
        path=("projects/project-02/runs/run-01/raw_data/" "TRAUPIXE-example.xlsx"),
    )

    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "INVALID_FILE_PATH"
    data_client.download_run_file.assert_not_called()


@pytest.mark.parametrize(
    ("path", "error_code"),
    [
        (
            "projects/project-01/runs/run-01/raw_data/results.xlsx",
            "UNSUPPORTED_FILE_TYPE",
        ),
        (
            "projects/project-01/runs/run-01/raw_data/TRAUPIXE-example.xls",
            "UNSUPPORTED_FILE_TYPE",
        ),
        (
            "projects/project-01/runs/run-01/raw_data/../TRAUPIXE-example.xlsx",
            "INVALID_FILE_PATH",
        ),
    ],
)
def test_rejects_files_outside_the_current_traupixe_scope(
    client: TestClient,
    visualization_dependencies: tuple[MagicMock, MagicMock, MagicMock],
    path: str,
    error_code: str,
) -> None:
    data_client, _, _ = visualization_dependencies

    response = _post(client, path=path)

    assert response.status_code == 422
    assert response.json()["detail"]["code"] == error_code
    data_client.download_run_file.assert_not_called()


def test_rejects_an_oversized_data_file_before_reading_it(
    client: TestClient,
    visualization_dependencies: tuple[MagicMock, MagicMock, MagicMock],
) -> None:
    data_client, _, _ = visualization_dependencies
    data_file = MagicMock()
    data_file.content_length = MAX_SOURCE_SIZE_BYTES + 1
    data_client.download_run_file.return_value = data_file

    response = _post(client)

    assert response.status_code == 413
    assert response.json()["detail"]["code"] == "FILE_TOO_LARGE"
    data_file.read.assert_not_called()
    data_file.close.assert_called_once_with()


def test_propagates_storage_errors_for_error_monitoring(
    client: TestClient,
    visualization_dependencies: tuple[MagicMock, MagicMock, MagicMock],
) -> None:
    data_client, _, _ = visualization_dependencies
    data_client.download_run_file.side_effect = RuntimeError("storage details")

    with pytest.raises(RuntimeError, match="storage details"):
        _post(client)


def test_propagates_model_errors_for_error_monitoring(
    client: TestClient,
    visualization_dependencies: tuple[MagicMock, MagicMock, MagicMock],
) -> None:
    _, visualization_service, _ = visualization_dependencies
    visualization_service.run.side_effect = DataVisualizationError(
        "invalid analysis plan"
    )
    with pytest.raises(DataVisualizationError, match="invalid analysis plan"):
        _post(client)


def test_hides_data_file_parser_details_from_the_client(
    client: TestClient,
    visualization_dependencies: tuple[MagicMock, MagicMock, MagicMock],
    caplog: pytest.LogCaptureFixture,
) -> None:
    _, _, prepare = visualization_dependencies
    prepare.side_effect = ValueError("private path: /tmp/data-file.xlsx")
    with caplog.at_level(logging.INFO, logger="api.data_visualization"):
        response = _post(client)

    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "INVALID_DATA_FILE"
    assert "/tmp/data-file.xlsx" not in response.text
    assert "/tmp/data-file.xlsx" in caplog.text


def test_propagates_model_timeouts_for_error_monitoring(
    client: TestClient,
    visualization_dependencies: tuple[MagicMock, MagicMock, MagicMock],
) -> None:
    _, visualization_service, _ = visualization_dependencies
    visualization_service.run.side_effect = httpx.ReadTimeout("timeout")

    with pytest.raises(httpx.ReadTimeout, match="timeout"):
        _post(client)


def test_does_not_write_complete_exchanges_outside_development(
    client: TestClient,
    visualization_dependencies: tuple[MagicMock, MagicMock, MagicMock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, visualization_service, _ = visualization_dependencies
    visualization_service.run.return_value = _result()
    monkeypatch.setenv("EUPHROSYNE_TOOLS_ENVIRONMENT", "production")

    with patch(
        "api.data_visualization.get_data_visualization_exchange_logger",
        side_effect=get_data_visualization_exchange_logger,
    ):
        response = _post(client)

    assert response.status_code == 200
    exchange_logger = visualization_service.run.call_args.kwargs["exchange_logger"]
    assert not exchange_logger.isEnabledFor(logging.INFO)


def test_requires_project_membership(
    client: TestClient,
    visualization_dependencies: tuple[MagicMock, MagicMock, MagicMock],
) -> None:
    data_client, _, _ = visualization_dependencies

    response = client.post(
        "/data/project-02/visualizations",
        json={
            "path": (
                "projects/project-02/runs/run-01/raw_data/" "TRAUPIXE-example.xlsx"
            ),
            "question": "Compare le fer.",
        },
    )

    assert response.status_code == 403
    data_client.download_run_file.assert_not_called()
