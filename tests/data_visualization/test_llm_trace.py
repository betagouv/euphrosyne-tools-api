from __future__ import annotations

import json
import stat
from pathlib import Path
from uuid import uuid4

import pytest

from data_visualization.llm_trace import trace_llm_exchange


def test_writes_complete_llm_exchanges_when_explicitly_enabled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATA_VISUALIZATION_TRACE", "1")
    monkeypatch.setenv("EUPHROSYNE_TOOLS_ENVIRONMENT", "production")
    request_id = uuid4()
    log_path = tmp_path / "user-logs" / "data-visualization-exchanges.jsonl"
    details = {
        "message": {
            "tool_calls": [
                {
                    "function": {
                        "name": "execute_python",
                        "arguments": {"option": {"series": [{"data": [1, 2]}]}},
                    }
                }
            ]
        },
    }

    written_path = trace_llm_exchange(
        request_id,
        "llm_response",
        details,
        path=log_path,
    )

    assert written_path == log_path
    record = json.loads(log_path.read_text(encoding="utf-8"))
    assert record.pop("timestamp").endswith("+00:00")
    assert record == {
        **details,
        "event": "llm_response",
        "request_id": str(request_id),
    }
    assert stat.S_IMODE(log_path.stat().st_mode) == 0o600


@pytest.mark.parametrize("value", [None, "", "0", "true"])
def test_does_not_write_llm_exchanges_unless_explicitly_enabled(
    value: str | None,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if value is None:
        monkeypatch.delenv("DATA_VISUALIZATION_TRACE", raising=False)
    else:
        monkeypatch.setenv("DATA_VISUALIZATION_TRACE", value)
    log_path = tmp_path / "data-visualization-exchanges.jsonl"

    written_path = trace_llm_exchange(
        uuid4(),
        "llm_request",
        {"messages": []},
        path=log_path,
    )

    assert written_path is None
    assert not log_path.exists()
