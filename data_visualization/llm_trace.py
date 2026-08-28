from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from functools import cache
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any
from uuid import UUID

from platformdirs import user_log_path

APP_NAME = "euphrosyne-tools-api"
TRACE_ENVIRONMENT_VARIABLE = "DATA_VISUALIZATION_TRACE"
LOG_FILENAME = "data-visualization-exchanges.jsonl"
MAX_LOG_BYTES = 20 * 1024 * 1024
LOG_BACKUP_COUNT = 5

logger = logging.getLogger(__name__)


class _TraceJsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        details = getattr(record, "trace_details", {})
        payload = {
            "timestamp": datetime.fromtimestamp(
                record.created,
                tz=timezone.utc,
            ).isoformat(),
            **(details if isinstance(details, dict) else {}),
            "event": record.getMessage(),
            "request_id": str(getattr(record, "request_id", "unknown")),
        }
        return json.dumps(
            payload,
            ensure_ascii=False,
            default=str,
            separators=(",", ":"),
        )


class _PrivateRotatingFileHandler(RotatingFileHandler):
    def _open(self):
        stream = super()._open()
        os.chmod(self.baseFilename, 0o600)
        return stream


def is_data_visualization_trace_enabled() -> bool:
    return os.getenv(TRACE_ENVIRONMENT_VARIABLE, "") == "1"


def get_data_visualization_trace_path() -> Path:
    return user_log_path(APP_NAME, appauthor=False) / LOG_FILENAME


@cache
def _file_logger(path: Path) -> logging.Logger:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)
    logger_suffix = hashlib.sha256(str(path).encode("utf-8")).hexdigest()[:12]
    trace_logger = logging.getLogger(f"data_visualization.llm_trace.{logger_suffix}")
    trace_logger.setLevel(logging.INFO)
    trace_logger.propagate = False
    if not trace_logger.handlers:
        handler = _PrivateRotatingFileHandler(
            path,
            maxBytes=MAX_LOG_BYTES,
            backupCount=LOG_BACKUP_COUNT,
            encoding="utf-8",
        )
        handler.setFormatter(_TraceJsonFormatter())
        trace_logger.addHandler(handler)
    return trace_logger


def trace_llm_exchange(
    request_id: UUID | str,
    event: str,
    details: dict[str, Any],
    *,
    path: Path | None = None,
) -> Path | None:
    """Write one complete LLM request or response when tracing is enabled."""
    if not is_data_visualization_trace_enabled():
        return None
    log_path = path or get_data_visualization_trace_path()
    try:
        trace_logger = _file_logger(log_path)
    except OSError:
        logger.warning(
            "data_visualization_trace_file_error request_id=%s",
            request_id,
            exc_info=True,
        )
        return None
    trace_logger.info(
        event,
        extra={
            "request_id": str(request_id),
            "trace_details": details,
        },
    )
    return log_path
