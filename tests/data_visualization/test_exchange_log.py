from __future__ import annotations

import json
import logging
import stat
from pathlib import Path
from uuid import uuid4

import pytest

from data_visualization.exchange_log import (
    get_data_visualization_exchange_logger,
)


def test_writes_complete_jsonl_exchanges_outside_the_project(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("EUPHROSYNE_TOOLS_ENVIRONMENT", "dev")
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

    exchange_logger = get_data_visualization_exchange_logger(
        request_id,
        path=log_path,
    )
    exchange_logger.info(
        "albert_response",
        extra={"exchange": details},
    )
    for handler in exchange_logger.logger.handlers:
        handler.flush()

    record = json.loads(log_path.read_text(encoding="utf-8"))
    assert record.pop("timestamp").endswith("+00:00")
    assert record == {
        **details,
        "event": "albert_response",
        "request_id": str(request_id),
    }
    assert stat.S_IMODE(log_path.stat().st_mode) == 0o600
    assert not exchange_logger.logger.propagate


def test_enables_complete_exchanges_only_in_development(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    request_id = uuid4()
    log_path = tmp_path / "data-visualization-exchanges.jsonl"

    monkeypatch.delenv("EUPHROSYNE_TOOLS_ENVIRONMENT", raising=False)
    exchange_logger = get_data_visualization_exchange_logger(
        request_id,
        path=log_path,
    )
    assert not exchange_logger.isEnabledFor(logging.INFO)

    monkeypatch.setenv("EUPHROSYNE_TOOLS_ENVIRONMENT", "dev")
    exchange_logger = get_data_visualization_exchange_logger(
        request_id,
        path=log_path,
    )
    assert exchange_logger.isEnabledFor(logging.INFO)

    monkeypatch.setenv("EUPHROSYNE_TOOLS_ENVIRONMENT", "development")
    exchange_logger = get_data_visualization_exchange_logger(
        request_id,
        path=log_path,
    )
    assert not exchange_logger.isEnabledFor(logging.INFO)

    monkeypatch.setenv("EUPHROSYNE_TOOLS_ENVIRONMENT", "production")
    exchange_logger = get_data_visualization_exchange_logger(
        request_id,
        path=log_path,
    )
    assert not exchange_logger.isEnabledFor(logging.INFO)
