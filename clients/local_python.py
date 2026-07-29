from __future__ import annotations

import mimetypes
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

OUTPUT_MAX_LENGTH = 20_000
PASSTHROUGH_ENVIRONMENT = (
    "LANG",
    "LC_ALL",
    "PATH",
    "PYTHONPATH",
    "SYSTEMROOT",
    "TEMP",
    "TMP",
    "TMPDIR",
    "VIRTUAL_ENV",
)


class LocalPythonExecutionError(RuntimeError):
    """Raised when a local execution session cannot be used."""


@dataclass(frozen=True)
class PythonExecutionResult:
    status: str
    stdout: str
    stderr: str
    duration_ms: int

    def tool_output(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "execution_time_ms": self.duration_ms,
        }


class LocalPythonSessionsClient:
    """Execute generated Python locally during TRAUPIXE development."""

    data_directory = "."

    def __init__(self, *, execution_timeout_seconds: float = 120) -> None:
        self._execution_timeout_seconds = execution_timeout_seconds
        self._sessions: dict[str, TemporaryDirectory[str]] = {}

    def upload_file(self, session_id: str, filename: str, content: bytes) -> None:
        safe_name = _safe_filename(filename)
        directory = self._sessions.get(session_id)
        if directory is None:
            directory = TemporaryDirectory(prefix="traupixe-local-")
            self._sessions[session_id] = directory
        Path(directory.name, safe_name).write_bytes(content)

    def execute(self, session_id: str, code: str) -> PythonExecutionResult:
        directory = self._directory(session_id)
        started_at = time.monotonic()
        try:
            result = subprocess.run(
                [sys.executable, "-c", code],
                cwd=directory,
                env=_execution_environment(),
                capture_output=True,
                text=True,
                timeout=self._execution_timeout_seconds,
                check=False,
            )
            stdout = result.stdout
            stderr = result.stderr
            status = "Succeeded" if result.returncode == 0 else "Failed"
        except subprocess.TimeoutExpired as error:
            stdout = _text_output(error.stdout)
            stderr = (
                f"{_text_output(error.stderr)}\n"
                f"Execution timed out after {self._execution_timeout_seconds} seconds"
            ).strip()
            status = "Failed"
        return PythonExecutionResult(
            status=status,
            stdout=stdout[-OUTPUT_MAX_LENGTH:],
            stderr=stderr[-OUTPUT_MAX_LENGTH:],
            duration_ms=int((time.monotonic() - started_at) * 1000),
        )

    def list_files(self, session_id: str) -> list[dict[str, Any]]:
        directory = self._directory(session_id)
        return [
            {
                "name": path.name,
                "contentType": mimetypes.guess_type(path.name)[0]
                or "application/octet-stream",
                "sizeInBytes": path.stat().st_size,
            }
            for path in sorted(directory.iterdir())
            if path.is_file()
        ]

    def download_file(self, session_id: str, filename: str) -> bytes:
        return (self._directory(session_id) / _safe_filename(filename)).read_bytes()

    def delete_session(self, session_id: str) -> None:
        directory = self._sessions.pop(session_id, None)
        if directory is not None:
            directory.cleanup()

    def _directory(self, session_id: str) -> Path:
        try:
            return Path(self._sessions[session_id].name)
        except KeyError as error:
            raise LocalPythonExecutionError(
                "The local session does not exist"
            ) from error


def _safe_filename(filename: str) -> str:
    safe_name = Path(filename).name
    if safe_name != filename or not safe_name:
        raise ValueError("The filename must not contain a path")
    return safe_name


def _execution_environment() -> dict[str, str]:
    environment = {
        key: os.environ[key] for key in PASSTHROUGH_ENVIRONMENT if key in os.environ
    }
    environment["MPLBACKEND"] = "Agg"
    return environment


def _text_output(output: str | bytes | None) -> str:
    if output is None:
        return ""
    if isinstance(output, bytes):
        return output.decode(errors="replace")
    return output
