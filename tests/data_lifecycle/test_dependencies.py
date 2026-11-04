from unittest.mock import MagicMock, patch

import pytest
import requests
from fastapi import HTTPException
from requests import RequestException

from data_lifecycle.dependencies import (
    FETCH_PROJECT_LIFECYCLE_RETRIES,
    RESPONSE_BODY_LOG_EXCERPT_LENGTH,
    fetch_project_lifecycle,
)
from data_lifecycle.models import LifecycleState


def test_fetch_project_lifecycle_returns_storage_role(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("EUPHROSYNE_BACKEND_URL", "https://backend.example")
    response = MagicMock(status_code=200, ok=True)
    response.json.return_value = {"lifecycle_state": LifecycleState.COOL.value}

    with (
        patch(
            "data_lifecycle.dependencies.generate_token_for_euphrosyne_backend",
            return_value="token",
        ),
        patch(
            "data_lifecycle.dependencies.requests.get", return_value=response
        ) as get_mock,
    ):
        result = fetch_project_lifecycle("project-01")

    assert result == LifecycleState.COOL
    get_mock.assert_called_once_with(
        "https://backend.example/api/data-management/projects/project-01/lifecycle",
        headers={"Authorization": "Bearer token"},
        timeout=3,
    )


def test_fetch_project_lifecycle_raises_not_found(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("EUPHROSYNE_BACKEND_URL", "https://backend.example")
    response = MagicMock(status_code=404, ok=False)

    with (
        patch(
            "data_lifecycle.dependencies.generate_token_for_euphrosyne_backend",
            return_value="token",
        ),
        patch("data_lifecycle.dependencies.requests.get", return_value=response),
        pytest.raises(HTTPException) as error,
    ):
        fetch_project_lifecycle("project-01")

    assert error.value.status_code == 404


def _make_response(
    status_code: int, body: bytes, content_type: str
) -> requests.Response:
    response = requests.Response()
    response.status_code = status_code
    response._content = body  # pylint: disable=protected-access
    response.headers["Content-Type"] = content_type
    response.encoding = "utf-8"
    return response


def _fetch_with_response(response: requests.Response) -> LifecycleState:
    with (
        patch(
            "data_lifecycle.dependencies.generate_token_for_euphrosyne_backend",
            return_value="token",
        ),
        patch("data_lifecycle.dependencies.requests.get", return_value=response),
    ):
        return fetch_project_lifecycle("project-01")


def test_fetch_project_lifecycle_returns_error_state(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("EUPHROSYNE_BACKEND_URL", "https://backend.example")
    response = _make_response(200, b'{"lifecycle_state": "ERROR"}', "application/json")

    assert _fetch_with_response(response) == LifecycleState.ERROR


def test_fetch_project_lifecycle_propagates_html_response_error(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    monkeypatch.setenv("EUPHROSYNE_BACKEND_URL", "https://backend.example")
    html = b"<html><head><title>Challenge</title></head><body>" + b"x" * 1000
    response = _make_response(200, html, "text/html; charset=utf-8")

    with pytest.raises(requests.exceptions.JSONDecodeError):
        _fetch_with_response(response)

    assert "non-JSON response" in caplog.text
    assert "status=200" in caplog.text
    assert "content-type=text/html; charset=utf-8" in caplog.text
    assert "<title>Challenge</title>" in caplog.text
    assert "...[truncated]" in caplog.text
    assert "x" * (RESPONSE_BODY_LOG_EXCERPT_LENGTH + 1) not in caplog.text


def test_fetch_project_lifecycle_raises_bad_gateway_for_unknown_state(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    monkeypatch.setenv("EUPHROSYNE_BACKEND_URL", "https://backend.example")
    response = _make_response(200, b'{"lifecycle_state": "WARM"}', "application/json")

    with pytest.raises(HTTPException) as error:
        _fetch_with_response(response)

    assert error.value.status_code == 502
    assert error.value.detail == (
        "Storage backend returned an invalid lifecycle state (fetch_project_lifecycle)"
    )
    assert "'WARM'" in caplog.text


@pytest.mark.parametrize("body", [b"{}", b"[]"])
def test_fetch_project_lifecycle_raises_bad_gateway_for_missing_state(
    monkeypatch: pytest.MonkeyPatch,
    body: bytes,
):
    monkeypatch.setenv("EUPHROSYNE_BACKEND_URL", "https://backend.example")
    response = _make_response(200, body, "application/json")

    with pytest.raises(HTTPException) as error:
        _fetch_with_response(response)

    assert error.value.status_code == 502
    assert error.value.detail == (
        "Storage backend returned an invalid response (fetch_project_lifecycle)"
    )


def test_fetch_project_lifecycle_retries_then_raises_service_unavailable(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("EUPHROSYNE_BACKEND_URL", "https://backend.example")
    response = MagicMock(status_code=503, ok=False)

    with (
        patch(
            "data_lifecycle.dependencies.generate_token_for_euphrosyne_backend",
            return_value="token",
        ),
        patch(
            "data_lifecycle.dependencies.requests.get", return_value=response
        ) as get_mock,
        patch("data_lifecycle.dependencies.time.sleep") as sleep_mock,
        pytest.raises(HTTPException) as error,
    ):
        fetch_project_lifecycle("project-01")

    assert error.value.status_code == 503
    assert get_mock.call_count == FETCH_PROJECT_LIFECYCLE_RETRIES
    sleep_mock.assert_called_once_with(1)


def test_fetch_project_lifecycle_retries_when_request_exc(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("EUPHROSYNE_BACKEND_URL", "https://backend.example")
    get_mock = MagicMock(side_effect=RequestException)

    with (
        patch(
            "data_lifecycle.dependencies.generate_token_for_euphrosyne_backend",
            return_value="token",
        ),
        patch("data_lifecycle.dependencies.requests.get", new=get_mock),
        patch("data_lifecycle.dependencies.time.sleep") as sleep_mock,
        pytest.raises(HTTPException) as error,
    ):
        fetch_project_lifecycle("project-01")

    assert error.value.status_code == 503
    assert get_mock.call_count == FETCH_PROJECT_LIFECYCLE_RETRIES
    sleep_mock.assert_called_once_with(1)
