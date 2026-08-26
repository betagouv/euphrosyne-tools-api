from __future__ import annotations

import json
import stat
from pathlib import Path
from uuid import uuid4

import pytest

from data_visualization.exchange_log import (
    is_data_visualization_exchange_logging_enabled,
    write_data_visualization_exchange,
)


def test_writes_complete_jsonl_exchanges_outside_the_project(tmp_path: Path) -> None:
    request_id = uuid4()
    log_path = tmp_path / "user-logs" / "data-visualization-exchanges.jsonl"
    exchange = {
        "event": "albert_response",
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

    written_path = write_data_visualization_exchange(
        request_id,
        exchange,
        path=log_path,
    )

    assert written_path == log_path
    record = json.loads(log_path.read_text(encoding="utf-8"))
    assert record.pop("timestamp").endswith("+00:00")
    assert record == {**exchange, "request_id": str(request_id)}
    assert stat.S_IMODE(log_path.stat().st_mode) == 0o600


def test_enables_complete_exchanges_only_in_development(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("EUPHROSYNE_TOOLS_ENVIRONMENT", raising=False)
    assert not is_data_visualization_exchange_logging_enabled()

    monkeypatch.setenv("EUPHROSYNE_TOOLS_ENVIRONMENT", "dev")
    assert is_data_visualization_exchange_logging_enabled()

    monkeypatch.setenv("EUPHROSYNE_TOOLS_ENVIRONMENT", "development")
    assert not is_data_visualization_exchange_logging_enabled()

    monkeypatch.setenv("EUPHROSYNE_TOOLS_ENVIRONMENT", "production")
    assert not is_data_visualization_exchange_logging_enabled()
