from __future__ import annotations

import hashlib
import json
import logging
import os
from collections.abc import MutableMapping
from datetime import datetime, timezone
from functools import cache
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any
from uuid import UUID

from platformdirs import user_log_path

APP_NAME = "euphrosyne-tools-api"
LOG_FILENAME = "data-visualization-exchanges.jsonl"
MAX_LOG_BYTES = 20 * 1024 * 1024
LOG_BACKUP_COUNT = 5


class DataVisualizationExchangeLogger(logging.LoggerAdapter):
    """Request-scoped adapter for private visualization exchange records."""

    def process(
        self,
        msg: Any,
        kwargs: MutableMapping[str, Any],
    ) -> tuple[Any, MutableMapping[str, Any]]:
        extra = dict(self.extra or {})
        call_extra = kwargs.get("extra")
        if isinstance(call_extra, dict):
            extra.update(call_extra)
        kwargs["extra"] = extra
        return msg, kwargs


class _ExchangeJsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        exchange = getattr(record, "exchange", {})
        details = exchange if isinstance(exchange, dict) else {}
        payload = {
            "timestamp": datetime.fromtimestamp(
                record.created,
                tz=timezone.utc,
            ).isoformat(),
            **details,
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


def is_data_visualization_exchange_logging_enabled() -> bool:
    return os.getenv("EUPHROSYNE_TOOLS_ENVIRONMENT", "").casefold() == "dev"


def get_data_visualization_exchange_log_path() -> Path:
    return user_log_path(APP_NAME, appauthor=False) / LOG_FILENAME


@cache
def _disabled_logger() -> logging.Logger:
    logger = logging.getLogger("data_visualization.exchanges.disabled")
    logger.setLevel(logging.CRITICAL + 1)
    logger.propagate = False
    if not logger.handlers:
        logger.addHandler(logging.NullHandler())
    return logger


DISABLED_EXCHANGE_LOGGER = DataVisualizationExchangeLogger(
    _disabled_logger(),
    {"request_id": "unknown"},
)


@cache
def _file_logger(path: Path) -> logging.Logger:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)
    logger_suffix = hashlib.sha256(str(path).encode("utf-8")).hexdigest()[:12]
    logger = logging.getLogger(f"data_visualization.exchanges.{logger_suffix}")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if not logger.handlers:
        handler = _PrivateRotatingFileHandler(
            path,
            maxBytes=MAX_LOG_BYTES,
            backupCount=LOG_BACKUP_COUNT,
            encoding="utf-8",
        )
        handler.setFormatter(_ExchangeJsonFormatter())
        logger.addHandler(handler)
    return logger


def get_data_visualization_exchange_logger(
    request_id: UUID | str,
    *,
    path: Path | None = None,
) -> DataVisualizationExchangeLogger:
    if not is_data_visualization_exchange_logging_enabled():
        return DataVisualizationExchangeLogger(
            _disabled_logger(),
            {"request_id": str(request_id)},
        )
    log_path = path or get_data_visualization_exchange_log_path()
    try:
        logger = _file_logger(log_path)
    except OSError:
        logging.getLogger(__name__).warning(
            "data_visualization_exchange_file_error request_id=%s",
            request_id,
            exc_info=True,
        )
        logger = _disabled_logger()
    return DataVisualizationExchangeLogger(
        logger,
        {"request_id": str(request_id)},
    )
