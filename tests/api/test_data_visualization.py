from __future__ import annotations

import io
import logging
from typing import Iterator
from unittest.mock import MagicMock, patch
from uuid import UUID

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from aglae.traupixe.analysis import (
    TraupixeAnalysisError,
    TraupixeAnalysisResult,
)
from aglae.traupixe.format import MAX_SOURCE_SIZE_BYTES
from clients.albert import AlbertClient
from clients.data_client import AbstractDataClient
from data_visualization.models import DataVisualization
from dependencies import (
    get_data_visualization_llm_client,
    get_data_visualization_python_sessions_client,
    get_project_data_client,
)

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
    data_client.download_run_file.return_value = io.BytesIO(b"workbook")
    llm_client = MagicMock(spec=AlbertClient)
    python_sessions = MagicMock()
    previous_data = app.dependency_overrides.get(get_project_data_client)
    previous_llm = app.dependency_overrides.get(get_data_visualization_llm_client)
    previous_sessions = app.dependency_overrides.get(
        get_data_visualization_python_sessions_client
    )
    app.dependency_overrides[get_project_data_client] = lambda: data_client
    app.dependency_overrides[get_data_visualization_llm_client] = lambda: llm_client
    app.dependency_overrides[get_data_visualization_python_sessions_client] = (
        lambda: python_sessions
    )
    yield data_client, llm_client, python_sessions
    if previous_data is None:
        app.dependency_overrides.pop(get_project_data_client, None)
    else:
        app.dependency_overrides[get_project_data_client] = previous_data
    if previous_llm is None:
        app.dependency_overrides.pop(get_data_visualization_llm_client, None)
    else:
        app.dependency_overrides[get_data_visualization_llm_client] = previous_llm
    if previous_sessions is None:
        app.dependency_overrides.pop(
            get_data_visualization_python_sessions_client, None
        )
    else:
        app.dependency_overrides[get_data_visualization_python_sessions_client] = (
            previous_sessions
        )


@pytest.fixture(autouse=True)
def albert_exchange_log_writer() -> Iterator[MagicMock]:
    with patch("api.data_visualization.write_albert_exchange") as writer:
        yield writer


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


def _result() -> TraupixeAnalysisResult:
    return TraupixeAnalysisResult(
        answer="Réponse du modèle",
        visualizations=(_visualization(),),
        elapsed_seconds=1.25,
        albert_calls=1,
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
    albert_exchange_log_writer: MagicMock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    data_client, llm_client, python_sessions = visualization_dependencies
    analysis = MagicMock()

    def run(
        workbook: bytes,
        question: str,
        *,
        exchange_logger,
    ) -> TraupixeAnalysisResult:
        exchange_logger(
            {
                "event": "albert_request",
                "messages": [{"role": "user", "content": question}],
            }
        )
        exchange_logger(
            {
                "event": "visualization_result",
                "visualization": _visualization().model_dump(mode="json"),
            }
        )
        assert workbook == b"workbook"
        return _result()

    analysis.run.side_effect = run
    with (
        patch(
            "api.data_visualization.TraupixeAlbertAnalysis", return_value=analysis
        ) as analysis_class,
        caplog.at_level(logging.INFO, logger="api.data_visualization"),
    ):
        response = _post(client)

    assert response.status_code == 200
    payload = response.json()
    UUID(payload["request_id"])
    assert response.headers["X-Request-ID"] == payload["request_id"]
    assert payload["answer"] == "Réponse du modèle"
    assert payload["visualizations"] == [_visualization().model_dump(mode="json")]
    assert "visualization" not in payload
    data_client.download_run_file.assert_called_once_with(WORKBOOK_PATH)
    analysis_class.assert_called_once_with(llm_client, python_sessions)
    analysis.run.assert_called_once()
    assert "data_visualization_exchange" in caplog.text
    assert "data_visualization_completed" in caplog.text
    assert "total_tokens=300" in caplog.text
    visualization_log = next(
        record.message
        for record in caplog.records
        if "event=visualization_result" in record.message
    )
    assert "option" not in visualization_log
    assert "Ligne Excel 3" not in caplog.text
    written_exchanges = [
        call.args[1] for call in albert_exchange_log_writer.call_args_list
    ]
    assert [exchange["event"] for exchange in written_exchanges] == [
        "request_started",
        "albert_request",
        "visualization_result",
        "request_completed",
    ]
    assert written_exchanges[2]["visualization"]["option"] == (_visualization().option)


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
    data_client.download_run_file.assert_not_called()


@pytest.mark.parametrize(
    "path",
    [
        "projects/project-01/runs/run-01/raw_data/results.xlsx",
        "projects/project-01/runs/run-01/raw_data/TRAUPIXE-example.xls",
        "projects/project-01/runs/run-01/raw_data/../TRAUPIXE-example.xlsx",
    ],
)
def test_rejects_files_outside_the_current_traupixe_scope(
    client: TestClient,
    visualization_dependencies: tuple[MagicMock, MagicMock, MagicMock],
    path: str,
) -> None:
    data_client, _, _ = visualization_dependencies

    response = _post(client, path=path)

    assert response.status_code == 422
    data_client.download_run_file.assert_not_called()


def test_rejects_an_oversized_workbook_before_reading_it(
    client: TestClient,
    visualization_dependencies: tuple[MagicMock, MagicMock, MagicMock],
) -> None:
    data_client, _, _ = visualization_dependencies
    workbook_file = MagicMock()
    workbook_file.content_length = MAX_SOURCE_SIZE_BYTES + 1
    data_client.download_run_file.return_value = workbook_file

    response = _post(client)

    assert response.status_code == 413
    workbook_file.read.assert_not_called()
    workbook_file.close.assert_called_once_with()


def test_returns_a_simple_error_when_the_workbook_cannot_be_downloaded(
    client: TestClient,
    visualization_dependencies: tuple[MagicMock, MagicMock, MagicMock],
) -> None:
    data_client, _, _ = visualization_dependencies
    data_client.download_run_file.side_effect = RuntimeError("storage details")

    response = _post(client)

    assert response.status_code == 502
    UUID(response.headers["X-Request-ID"])
    assert response.json() == {
        "detail": "Impossible de traiter cette demande. Veuillez réessayer."
    }
    assert "storage details" not in response.text


def test_returns_debug_information_when_the_model_response_is_invalid(
    client: TestClient,
    visualization_dependencies: tuple[MagicMock, MagicMock, MagicMock],
    albert_exchange_log_writer: MagicMock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    with (
        patch(
            "api.data_visualization.TraupixeAlbertAnalysis.run",
            side_effect=TraupixeAnalysisError("invalid analysis plan"),
        ),
        caplog.at_level(logging.INFO, logger="api.data_visualization"),
    ):
        response = _post(client)

    assert response.status_code == 422
    request_id = response.headers["X-Request-ID"]
    UUID(request_id)
    assert response.json() == {
        "detail": {
            "message": "Impossible de traiter cette demande. Veuillez réessayer.",
            "reason": "invalid analysis plan",
            "request_id": request_id,
        }
    }
    assert "reason='invalid analysis plan'" in caplog.text
    rejected_exchange = next(
        call.args[1]
        for call in albert_exchange_log_writer.call_args_list
        if call.args[1]["event"] == "analysis_rejected"
    )
    assert rejected_exchange["error_type"] == "TraupixeAnalysisError"
    assert "invalid analysis plan" in rejected_exchange["traceback"]


def test_hides_workbook_parser_details_from_the_client(
    client: TestClient,
    visualization_dependencies: tuple[MagicMock, MagicMock, MagicMock],
    caplog: pytest.LogCaptureFixture,
) -> None:
    with (
        patch(
            "api.data_visualization.TraupixeAlbertAnalysis.run",
            side_effect=ValueError("private path: /tmp/workbook.xlsx"),
        ),
        caplog.at_level(logging.INFO, logger="api.data_visualization"),
    ):
        response = _post(client)

    assert response.status_code == 422
    assert response.json()["detail"]["reason"] == (
        "Le classeur TRAUPIXE n'a pas pu être interprété."
    )
    assert "/tmp/workbook.xlsx" not in response.text
    assert "/tmp/workbook.xlsx" in caplog.text


def test_returns_a_simple_error_when_the_model_times_out(
    client: TestClient,
    visualization_dependencies: tuple[MagicMock, MagicMock, MagicMock],
) -> None:
    with patch(
        "api.data_visualization.TraupixeAlbertAnalysis.run",
        side_effect=httpx.ReadTimeout("timeout"),
    ):
        response = _post(client)

    assert response.status_code == 504
    UUID(response.headers["X-Request-ID"])
    assert response.json() == {
        "detail": "Impossible de traiter cette demande. Veuillez réessayer."
    }


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
