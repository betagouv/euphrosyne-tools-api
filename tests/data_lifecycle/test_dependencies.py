from unittest.mock import MagicMock, call, patch

import pytest
from fastapi import HTTPException
from requests import RequestException

from data_lifecycle.dependencies import (
    FETCH_PROJECT_LIFECYCLE_RETRIES,
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
    ):
        with pytest.raises(HTTPException) as error:
            fetch_project_lifecycle("project-01")

    assert error.value.status_code == 404


@pytest.mark.parametrize(
    "json_side_effect,json_payload",
    [
        (ValueError("bad json"), None),
        (None, {}),
        (None, {"lifecycle_state": "WARM"}),
    ],
)
def test_fetch_project_lifecycle_raises_bad_gateway_for_invalid_payload(
    monkeypatch: pytest.MonkeyPatch,
    json_side_effect: ValueError | None,
    json_payload: dict[str, object] | None,
):
    monkeypatch.setenv("EUPHROSYNE_BACKEND_URL", "https://backend.example")
    response = MagicMock(status_code=200, ok=True)
    if json_side_effect is not None:
        response.json.side_effect = json_side_effect
    else:
        response.json.return_value = json_payload

    with (
        patch(
            "data_lifecycle.dependencies.generate_token_for_euphrosyne_backend",
            return_value="token",
        ),
        patch("data_lifecycle.dependencies.requests.get", return_value=response),
    ):
        with pytest.raises(HTTPException) as error:
            fetch_project_lifecycle("project-01")

    assert error.value.status_code == 502


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
    ):
        with pytest.raises(HTTPException) as error:
            fetch_project_lifecycle("project-01")

    assert error.value.status_code == 503
    assert get_mock.call_count == FETCH_PROJECT_LIFECYCLE_RETRIES
    sleep_mock.assert_has_calls([call(1)] * FETCH_PROJECT_LIFECYCLE_RETRIES)


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
    ):
        with pytest.raises(HTTPException) as error:
            fetch_project_lifecycle("project-01")

    assert error.value.status_code == 503
    assert get_mock.call_count == FETCH_PROJECT_LIFECYCLE_RETRIES
    sleep_mock.assert_has_calls([call(1)] * FETCH_PROJECT_LIFECYCLE_RETRIES)
