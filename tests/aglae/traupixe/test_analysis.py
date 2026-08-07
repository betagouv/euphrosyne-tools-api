import json
from typing import Any
from unittest.mock import MagicMock

import pytest

from aglae.traupixe.analysis import (
    ALBERT_RESPONSE_FORMAT,
    CALCULATION_RESULT_FILENAME,
    DATASET_FILENAME,
    MAX_VISUALIZATION_ATTEMPTS,
    PYTHON_TOOL,
    WORKBOOK_FILENAME,
    TraupixeAlbertAnalysis,
    TraupixeAnalysisError,
)
from clients.albert import AlbertClient, AlbertCompletion
from clients.local_python import LocalPythonSessionsClient
from clients.python_sessions import PythonExecutionResult


def _final_completion(payload: dict[str, Any]) -> AlbertCompletion:
    return AlbertCompletion(
        message={"role": "assistant", "content": json.dumps(payload)},
        usage={"total_tokens": 200},
        model="model",
    )


def _python_completion(code: str = "result = {'rows': []}") -> AlbertCompletion:
    return AlbertCompletion(
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
    sessions = MagicMock(spec=LocalPythonSessionsClient)
    sessions.data_directory = "."
    sessions.execute.return_value = PythonExecutionResult(
        status="Succeeded",
        stdout="calculated\n",
        stderr="",
        duration_ms=12,
    )
    encoded = json.dumps(payload).encode()
    sessions.list_files.return_value = [
        {
            "name": CALCULATION_RESULT_FILENAME,
            "contentType": "application/json",
            "sizeInBytes": len(encoded),
        }
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


def test_executes_python_then_generates_echarts_json(
    traupixe_workbook: bytes,
) -> None:
    generated = _generated_visualizations()
    albert = MagicMock(spec=AlbertClient)
    albert.complete.side_effect = [
        _python_completion(),
        _final_completion(generated),
    ]
    sessions = _sessions()
    exchanges: list[dict[str, Any]] = []

    result = TraupixeAlbertAnalysis(albert, sessions).run(
        traupixe_workbook,
        "Génère un histogramme des éléments traces",
        exchange_logger=exchanges.append,
    )

    assert result.answer == generated["answer"]
    assert result.albert_calls == 2
    assert result.usage == ({"total_tokens": 100}, {"total_tokens": 200})
    assert result.visualizations[0].model_dump() == generated["visualizations"][0]

    first_messages, tools = albert.complete.call_args_list[0].args
    assert tools == [PYTHON_TOOL]
    assert albert.complete.call_args_list[0].kwargs["tool_choice"] == {
        "type": "function",
        "function": {"name": "execute_python"},
    }
    final_messages = albert.complete.call_args_list[1].args[0]
    assert albert.complete.call_args_list[1].kwargs["response_format"] == (
        ALBERT_RESPONSE_FORMAT
    )
    assert '"analysis":"object-1"' in final_messages[1]["content"]
    assert (
        "sans titre, tableau ou autre syntaxe Markdown" in final_messages[0]["content"]
    )

    uploaded_names = [call.args[1] for call in sessions.upload_file.call_args_list]
    assert uploaded_names == [WORKBOOK_FILENAME, DATASET_FILENAME]
    executed_code = sessions.execute.call_args.args[1]
    assert "json.dumps(result" in executed_code
    assert CALCULATION_RESULT_FILENAME in executed_code
    sessions.delete_session.assert_called_once()
    assert [exchange["event"] for exchange in exchanges] == [
        "albert_request",
        "albert_response",
        "python_execution",
        "albert_request",
        "albert_response",
        "visualization_result",
    ]
    assert first_messages[0]["role"] == "system"


def test_retries_python_after_a_failed_execution(traupixe_workbook: bytes) -> None:
    albert = MagicMock(spec=AlbertClient)
    albert.complete.side_effect = [
        _python_completion("raise ValueError('first')"),
        _python_completion("result = {'fixed': True}"),
        _final_completion(_generated_visualizations()),
    ]
    sessions = _sessions()
    sessions.execute.side_effect = [
        PythonExecutionResult(
            status="Failed",
            stdout="",
            stderr="ValueError: first",
            duration_ms=10,
        ),
        PythonExecutionResult(
            status="Succeeded",
            stdout="fixed\n",
            stderr="",
            duration_ms=10,
        ),
    ]

    result = TraupixeAlbertAnalysis(albert, sessions).run(
        traupixe_workbook,
        "Question",
    )

    assert result.albert_calls == 3
    assert sessions.execute.call_count == 2
    second_messages = albert.complete.call_args_list[1].args[0]
    assert "ValueError: first" in second_messages[-1]["content"]


def test_retries_an_invalid_visualization(traupixe_workbook: bytes) -> None:
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
    albert = MagicMock(spec=AlbertClient)
    albert.complete.side_effect = [
        _python_completion(),
        _final_completion(invalid),
        _final_completion(_generated_visualizations()),
    ]

    result = TraupixeAlbertAnalysis(albert, _sessions()).run(
        traupixe_workbook,
        "Question",
    )

    assert result.albert_calls == 3
    correction = albert.complete.call_args_list[2].args[0][-1]["content"]
    assert "external resource" in correction


@pytest.mark.parametrize(
    "option",
    [
        {"series": []},
        {
            "series": [{"type": "bar", "data": [1]}],
            "graphic": {"image": "data:image/png;base64,AA=="},
        },
    ],
)
def test_rejects_invalid_echarts_options(
    traupixe_workbook: bytes,
    option: dict[str, Any],
) -> None:
    albert = MagicMock(spec=AlbertClient)
    albert.complete.side_effect = [
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

    with pytest.raises(TraupixeAnalysisError):
        TraupixeAlbertAnalysis(albert, _sessions()).run(
            traupixe_workbook,
            "Question",
        )

    assert albert.complete.call_count == MAX_VISUALIZATION_ATTEMPTS + 1


def test_rejects_an_invalid_final_json_response(traupixe_workbook: bytes) -> None:
    albert = MagicMock(spec=AlbertClient)
    invalid_completion = AlbertCompletion(
        message={"role": "assistant", "content": "not JSON"},
        usage={},
        model="model",
    )
    albert.complete.side_effect = [
        _python_completion(),
        *[invalid_completion for _ in range(MAX_VISUALIZATION_ATTEMPTS)],
    ]

    with pytest.raises(
        TraupixeAnalysisError,
        match="invalid JSON visualization response",
    ):
        TraupixeAlbertAnalysis(albert, _sessions()).run(
            traupixe_workbook,
            "Question",
        )


def test_rejects_a_dataset_that_exceeds_the_model_limit(
    traupixe_workbook: bytes,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("aglae.traupixe.analysis.MAX_MODEL_DATA_BYTES", 1)
    albert = MagicMock(spec=AlbertClient)
    sessions = _sessions()

    with pytest.raises(TraupixeAnalysisError, match="dataset is too large"):
        TraupixeAlbertAnalysis(albert, sessions).run(
            traupixe_workbook,
            "Question",
        )

    albert.complete.assert_not_called()
    sessions.upload_file.assert_not_called()
