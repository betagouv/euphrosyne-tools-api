import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import httpx
import pytest

from clients.azure.python_sessions import (
    API_VERSION,
    MAX_EXECUTION_SECONDS,
    TOKEN_SCOPE,
    AzurePythonSessionsClient,
)


def _client(handler):
    credential = MagicMock()
    credential.get_token.return_value = SimpleNamespace(token="access-token")
    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    return (
        AzurePythonSessionsClient(
            "https://westeurope.dynamicsessions.io/pool/",
            execution_timeout_seconds=300,
            credential=credential,
            http_client=http_client,
        ),
        credential,
    )


def test_executes_python_synchronously() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/pool/executions"
        assert dict(request.url.params) == {
            "api-version": API_VERSION,
            "identifier": "session-1234",
        }
        assert request.headers["Authorization"] == "Bearer access-token"
        assert json.loads(request.content) == {
            "codeInputType": "Inline",
            "executionType": "Synchronous",
            "code": "print('ok')",
            "timeoutInSeconds": MAX_EXECUTION_SECONDS,
            "outputStreamsMaxLength": 20_000,
        }
        return httpx.Response(
            200,
            json={
                "status": "Succeeded",
                "result": {
                    "stdout": "ok\n",
                    "stderr": "",
                    "executionTimeInMilliseconds": 12,
                },
            },
        )

    client, credential = _client(handler)

    result = client.execute("session-1234", "print('ok')")

    assert result.status == "Succeeded"
    assert result.stdout == "ok\n"
    assert result.stderr == ""
    assert result.duration_ms == 12
    credential.get_token.assert_called_once_with(TOKEN_SCOPE)


def test_manages_session_files_and_cleanup() -> None:
    calls: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path))
        assert request.url.params["identifier"] == "session-1234"
        if request.method == "POST":
            assert b'filename="dataset.json"' in request.content
            assert b'{"value":1}' in request.content
            return httpx.Response(200, json={"name": "dataset.json"})
        if request.url.path.endswith("/content"):
            return httpx.Response(200, content=b'{"value":1}')
        if request.method == "DELETE":
            return httpx.Response(204)
        return httpx.Response(
            200,
            json={
                "value": [
                    {
                        "name": "dataset.json",
                        "contentType": "application/json",
                        "sizeInBytes": 11,
                    }
                ]
            },
        )

    client, _ = _client(handler)

    client.upload_file("session-1234", "dataset.json", b'{"value":1}')
    assert client.list_files("session-1234") == [
        {
            "name": "dataset.json",
            "contentType": "application/json",
            "sizeInBytes": 11,
        }
    ]
    assert client.download_file("session-1234", "dataset.json") == b'{"value":1}'
    client.delete_session("session-1234")

    assert calls == [
        ("POST", "/pool/files"),
        ("GET", "/pool/files"),
        ("GET", "/pool/files/dataset.json/content"),
        ("DELETE", "/pool/session"),
    ]


def test_rejects_filename_paths() -> None:
    client, _ = _client(lambda _request: httpx.Response(200))

    with pytest.raises(ValueError, match="must not contain a path"):
        client.upload_file("session-1234", "../dataset.json", b"{}")


def test_cleanup_is_idempotent_when_session_does_not_exist() -> None:
    client, _ = _client(lambda _request: httpx.Response(404))

    client.delete_session("session-1234")


def test_reads_session_pool_endpoint_from_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "AZURE_SESSION_POOL_ENDPOINT",
        "https://westeurope.dynamicsessions.io/from-environment/",
    )
    credential = MagicMock()
    credential.get_token.return_value = SimpleNamespace(token="access-token")

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/from-environment/session"
        return httpx.Response(204)

    client = AzurePythonSessionsClient(
        credential=credential,
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )

    client.delete_session("session-1234")
