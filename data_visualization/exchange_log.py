from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from functools import lru_cache
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any
from uuid import UUID

from platformdirs import user_log_path

APP_NAME = "euphrosyne-tools-api"
LOG_FILENAME = "albert-exchanges.jsonl"
MAX_LOG_BYTES = 20 * 1024 * 1024
LOG_BACKUP_COUNT = 5


class _PrivateRotatingFileHandler(RotatingFileHandler):
    def _open(self):
        stream = super()._open()
        os.chmod(self.baseFilename, 0o600)
        return stream


def get_albert_exchange_log_path() -> Path:
    return user_log_path(APP_NAME, appauthor=False) / LOG_FILENAME


@lru_cache(maxsize=None)
def _file_logger(path: Path) -> logging.Logger:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)
    logger_suffix = hashlib.sha256(str(path).encode("utf-8")).hexdigest()[:12]
    logger = logging.getLogger(f"data_visualization.albert.exchanges.{logger_suffix}")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if not logger.handlers:
        handler = _PrivateRotatingFileHandler(
            path,
            maxBytes=MAX_LOG_BYTES,
            backupCount=LOG_BACKUP_COUNT,
            encoding="utf-8",
        )
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
    return logger


def write_albert_exchange(
    request_id: UUID | str,
    exchange: dict[str, Any],
    *,
    path: Path | None = None,
) -> Path:
    log_path = path or get_albert_exchange_log_path()
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        **exchange,
        "request_id": str(request_id),
    }
    _file_logger(log_path).info(
        json.dumps(
            record,
            ensure_ascii=False,
            default=str,
            separators=(",", ":"),
        )
    )
    return log_path
