import json
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

from clients.python_sessions import (
    PythonExecutionResult,
    PythonSessionFile,
    PythonSessionsClient,
)
from data_visualization.llm import (
    DataVisualizationCompletion,
    DataVisualizationLlmClient,
)
from data_visualization.models import PreparedDataVisualization
from data_visualization.service import (
    CALCULATION_RESULT_FILENAME,
    MAX_PYTHON_EXECUTIONS,
    MAX_VISUALIZATION_ATTEMPTS,
    PYTHON_TOOL,
    VISUALIZATION_RESPONSE_FORMAT,
    DataVisualizationError,
    DataVisualizationService,
)


def _prepared(content: bytes = b'{"analyses":[]}') -> PreparedDataVisualization:
    return PreparedDataVisualization(
        filename="dataset.json",
        content=content,
        descriptor={"format": "test"},
        calculation_instructions='Les analyses sont dans data["analyses"].',
        visualization_instructions="Conserve toutes les analyses.",
    )


def _final_completion(payload: dict[str, Any]) -> DataVisualizationCompletion:
    return DataVisualizationCompletion(
        message={"role": "assistant", "content": json.dumps(payload)},
        usage={"total_tokens": 200},
        model="model",
    )


def _python_completion(
    code: str = "result = {'rows': []}",
) -> DataVisualizationCompletion:
    return DataVisualizationCompletion(
        message={
            "role": "assistant",
            "content": None,
            "tool_calls": [
                {
                    "id": "python-1",
                    "type": "function",
                    "function": {
                        "name": "execute_python",
                        "arguments": json.dumps({"code": code}),
                    },
                }
            ],
        },
        usage={"total_tokens": 100},
        model="model",
    )


def _sessions(calculation: dict[str, Any] | None = None) -> MagicMock:
    payload = calculation or {
        "unit": "%",
        "rows": [
            {
                "analysis": "object-1",
                "zone": "zone-1",
                "analyte": "Fe2O3",
                "value": 1.2,
            }
        ],
    }
    sessions = MagicMock(spec=PythonSessionsClient)
    sessions.data_directory = "."
    sessions.execute.return_value = PythonExecutionResult(
        status="Succeeded",
        stdout="calculated\n",
        stderr="",
        duration_ms=12,
    )
    encoded = json.dumps(payload, separators=(",", ":")).encode()
    sessions.list_files.return_value = [
        PythonSessionFile(
            name=CALCULATION_RESULT_FILENAME,
            directory="",
            resource_type="File",
            content_type="application/json",
            size_in_bytes=len(encoded),
            last_modified_at=datetime(2026, 8, 21, tzinfo=timezone.utc),
        )
    ]
    sessions.download_file.return_value = encoded
    return sessions


def _generated_visualizations() -> dict[str, Any]:
    return {
        "answer": "Les données calculées sont représentées en barres.",
        "visualizations": [
            {
                "title": "Concentrations calculées",
                "option": {
                    "xAxis": {"type": "category", "data": ["object-1"]},
                    "yAxis": {"type": "value", "name": "%"},
                    "series": [{"name": "Fe₂O₃", "type": "bar", "data": [1.2]}],
                },
            }
        ],
    }


def test_executes_python_then_generates_echarts_json() -> None:
    generated = _generated_visualizations()
    llm = MagicMock(spec=DataVisualizationLlmClient)
    llm.complete.side_effect = [
        _python_completion(),
        _final_completion(generated),
    ]
    sessions = _sessions()
    exchanges: list[dict[str, Any]] = []

    result = DataVisualizationService(llm, sessions).run(
        _prepared(),
        "Génère un histogramme des éléments traces",
        exchange_logger=exchanges.append,
    )

    assert result.answer == generated["answer"]
    assert result.llm_calls == 2
    assert result.usage == ({"total_tokens": 100}, {"total_tokens": 200})
    assert result.visualizations[0].model_dump() == generated["visualizations"][0]

    first_messages, tools = llm.complete.call_args_list[0].args
    assert tools == [PYTHON_TOOL]
    assert llm.complete.call_args_list[0].kwargs["tool_choice"] == {
        "type": "function",
        "function": {"name": "execute_python"},
    }
    assert 'data["analyses"]' in first_messages[0]["content"]
    final_messages = llm.complete.call_args_list[1].args[0]
    assert llm.complete.call_args_list[1].kwargs["response_format"] == (
        VISUALIZATION_RESPONSE_FORMAT
    )
    assert '"analysis":"object-1"' in final_messages[1]["content"]
    assert "Conserve toutes les analyses" in final_messages[0]["content"]

    sessions.upload_file.assert_called_once_with(
        sessions.upload_file.call_args.args[0],
        "dataset.json",
        b'{"analyses":[]}',
    )
    executed_code = sessions.execute.call_args.args[1]
    assert "_euphrosyne_json.dumps(" in executed_code
    assert "isinstance(result, (dict, list))" in executed_code
    assert "separators=(',', ':')" in executed_code
    assert CALCULATION_RESULT_FILENAME in executed_code
    sessions.delete_session.assert_called_once()
    assert [exchange["event"] for exchange in exchanges] == [
        "llm_request",
        "llm_response",
        "python_execution",
        "llm_request",
        "llm_response",
        "visualization_result",
    ]


def test_retries_python_after_a_failed_execution() -> None:
    llm = MagicMock(spec=DataVisualizationLlmClient)
    llm.complete.side_effect = [
        _python_completion("raise ValueError('first')"),
        _python_completion("result = {'fixed': True}"),
        _final_completion(_generated_visualizations()),
    ]
    sessions = _sessions()
    sessions.execute.side_effect = [
        PythonExecutionResult("Failed", "", "ValueError: first", 10),
        PythonExecutionResult("Succeeded", "fixed\n", "", 10),
    ]

    result = DataVisualizationService(llm, sessions).run(_prepared(), "Question")

    assert result.llm_calls == 3
    assert sessions.execute.call_count == 2
    second_messages = llm.complete.call_args_list[1].args[0]
    assert "ValueError: first" in second_messages[-1]["content"]


def test_stops_after_python_execution_budget() -> None:
    llm = MagicMock(spec=DataVisualizationLlmClient)
    llm.complete.return_value = _python_completion("raise ValueError('invalid')")
    sessions = _sessions()
    sessions.execute.return_value = PythonExecutionResult(
        "Failed", "", "ValueError: invalid", 10
    )

    with pytest.raises(DataVisualizationError, match="valid Python calculation"):
        DataVisualizationService(llm, sessions).run(_prepared(), "Question")

    assert llm.complete.call_count == MAX_PYTHON_EXECUTIONS
    assert sessions.execute.call_count == MAX_PYTHON_EXECUTIONS


def test_retries_an_invalid_visualization() -> None:
    invalid = {
        "answer": "Réponse",
        "visualizations": [
            {
                "title": "Graphique",
                "option": {
                    "series": [{"type": "bar", "data": [1]}],
                    "graphic": {"image": "https://example.test/image.png"},
                },
            }
        ],
    }
    llm = MagicMock(spec=DataVisualizationLlmClient)
    llm.complete.side_effect = [
        _python_completion(),
        _final_completion(invalid),
        _final_completion(_generated_visualizations()),
    ]

    result = DataVisualizationService(llm, _sessions()).run(_prepared(), "Question")

    assert result.llm_calls == 3
    correction = llm.complete.call_args_list[2].args[0][-1]["content"]
    assert "external resource" in correction


@pytest.mark.parametrize(
    "option",
    [
        {
            "series": [{"type": "bar", "data": [1]}],
            "tooltip": {"extraCssText": "position: fixed"},
        },
        {
            "series": [{"type": "bar", "data": [1]}],
            "graphic": {"image": "data:image/png;base64,AA=="},
        },
    ],
)
def test_rejects_invalid_echarts_options(option: dict[str, Any]) -> None:
    llm = MagicMock(spec=DataVisualizationLlmClient)
    llm.complete.side_effect = [
        _python_completion(),
        *[
            _final_completion(
                {
                    "answer": "Réponse",
                    "visualizations": [{"title": "Graphique", "option": option}],
                }
            )
            for _ in range(MAX_VISUALIZATION_ATTEMPTS)
        ],
    ]

    with pytest.raises(DataVisualizationError):
        DataVisualizationService(llm, _sessions()).run(_prepared(), "Question")

    assert llm.complete.call_count == MAX_VISUALIZATION_ATTEMPTS + 1


def test_rejects_an_invalid_final_json_response() -> None:
    llm = MagicMock(spec=DataVisualizationLlmClient)
    invalid_completion = DataVisualizationCompletion(
        message={"role": "assistant", "content": "not JSON"},
        usage={},
        model="model",
    )
    llm.complete.side_effect = [
        _python_completion(),
        *[invalid_completion for _ in range(MAX_VISUALIZATION_ATTEMPTS)],
    ]

    with pytest.raises(
        DataVisualizationError,
        match="invalid JSON visualization response",
    ):
        DataVisualizationService(llm, _sessions()).run(_prepared(), "Question")


def test_rejects_a_dataset_that_exceeds_the_model_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("data_visualization.service.MAX_MODEL_DATA_BYTES", 1)
    llm = MagicMock(spec=DataVisualizationLlmClient)
    sessions = _sessions()

    with pytest.raises(DataVisualizationError, match="dataset is too large"):
        DataVisualizationService(llm, sessions).run(_prepared(b"{}"), "Question")

    llm.complete.assert_not_called()
    sessions.upload_file.assert_not_called()
