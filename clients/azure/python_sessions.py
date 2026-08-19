from __future__ import annotations

import mimetypes
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx
from azure.core.credentials import TokenCredential
from azure.identity import DefaultAzureCredential

from clients.python_sessions import PythonExecutionResult

API_VERSION = "2025-10-02-preview"
TOKEN_SCOPE = "https://dynamicsessions.io/.default"
MAX_EXECUTION_SECONDS = 220
OUTPUT_MAX_LENGTH = 20_000


class AzurePythonSessionsClient:
    """Execute generated Python in an Azure Container Apps session pool."""

    data_directory = "/mnt/data"

    def __init__(
        self,
        endpoint: str,
        *,
        execution_timeout_seconds: float = MAX_EXECUTION_SECONDS,
        credential: TokenCredential | None = None,
        http_client: httpx.Client | None = None,
    ) -> None:
        self._endpoint = endpoint.rstrip("/")
        self._execution_timeout_seconds = min(
            max(1, int(execution_timeout_seconds)),
            MAX_EXECUTION_SECONDS,
        )
        self._credential = credential or DefaultAzureCredential()
        self._http_client = http_client or httpx.Client(
            timeout=execution_timeout_seconds
        )

    def upload_file(self, session_id: str, filename: str, content: bytes) -> None:
        safe_name = _safe_filename(filename)
        content_type = mimetypes.guess_type(safe_name)[0] or "application/octet-stream"
        response = self._request(
            "POST",
            "files",
            session_id,
            files={"file": (safe_name, content, content_type)},
        )
        response.raise_for_status()

    def execute(self, session_id: str, code: str) -> PythonExecutionResult:
        response = self._request(
            "POST",
            "executions",
            session_id,
            json={
                "codeInputType": "Inline",
                "executionType": "Synchronous",
                "code": code,
                "timeoutInSeconds": self._execution_timeout_seconds,
                "outputStreamsMaxLength": OUTPUT_MAX_LENGTH,
            },
        )
        response.raise_for_status()
        execution = response.json()
        result = execution.get("result") or {}
        stderr = str(result.get("stderr") or "")
        if not stderr and execution.get("error"):
            stderr = str(execution["error"])
        return PythonExecutionResult(
            status=str(execution.get("status") or "Unknown"),
            stdout=str(result.get("stdout") or "")[-OUTPUT_MAX_LENGTH:],
            stderr=stderr[-OUTPUT_MAX_LENGTH:],
            duration_ms=int(result.get("executionTimeInMilliseconds") or 0),
        )

    def list_files(self, session_id: str) -> list[dict[str, Any]]:
        response = self._request("GET", "files", session_id)
        response.raise_for_status()
        return response.json().get("value", [])

    def download_file(self, session_id: str, filename: str) -> bytes:
        safe_name = quote(_safe_filename(filename), safe="")
        response = self._request(
            "GET",
            f"files/{safe_name}/content",
            session_id,
        )
        response.raise_for_status()
        return response.content

    def delete_session(self, session_id: str) -> None:
        response = self._request("DELETE", "session", session_id)
        if response.status_code != 404:
            response.raise_for_status()

    def _request(
        self,
        method: str,
        path: str,
        session_id: str,
        **kwargs: Any,
    ) -> httpx.Response:
        token = self._credential.get_token(TOKEN_SCOPE).token
        return self._http_client.request(
            method,
            f"{self._endpoint}/{path}",
            params={"api-version": API_VERSION, "identifier": session_id},
            headers={"Authorization": f"Bearer {token}"},
            **kwargs,
        )


def _safe_filename(filename: str) -> str:
    safe_name = Path(filename).name
    if safe_name != filename or not safe_name:
        raise ValueError("The filename must not contain a path")
    return safe_name
