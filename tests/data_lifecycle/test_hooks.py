import json
import datetime
from unittest import mock
from uuid import uuid4

import pytest
import requests

from data_lifecycle.hooks import post_lifecycle_operation_callback
from data_lifecycle.models import (
    LifecycleOperation,
    LifecycleOperationStatus,
    LifecycleOperationType,
)


def test_post_lifecycle_operation_callback_retries_on_transient_failure(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("EUPHROSYNE_BACKEND_URL", "http://localhost")
    payload = LifecycleOperation(
        project_slug="project-1",
        operation_id=uuid4(),
        type=LifecycleOperationType.COOL,
        status=LifecycleOperationStatus.SUCCEEDED,
    )
    success_response = mock.MagicMock()
    success_response.status_code = 200
    success_response.ok = True
    success_response.text = "ok"

    with mock.patch(
        "data_lifecycle.hooks.generate_token_for_euphrosyne_backend",
        return_value="token",
    ):
        with mock.patch("data_lifecycle.hooks.requests.post") as post_mock:
            post_mock.side_effect = [
                requests.RequestException("network"),
                success_response,
            ]
            result = post_lifecycle_operation_callback(
                payload,
                max_attempts=2,
                initial_backoff_seconds=0,
                sleep=lambda _: None,
            )

    assert result is True
    assert post_mock.call_count == 2


def test_post_lifecycle_operation_callback_retries_on_5xx(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("EUPHROSYNE_BACKEND_URL", "http://localhost")
    payload = LifecycleOperation(
        project_slug="project-5",
        operation_id=uuid4(),
        type=LifecycleOperationType.COOL,
        status=LifecycleOperationStatus.SUCCEEDED,
    )
    error_response = mock.MagicMock()
    error_response.status_code = 500
    error_response.ok = False
    error_response.text = "server error"
    success_response = mock.MagicMock()
    success_response.status_code = 200
    success_response.ok = True
    success_response.text = "ok"

    with mock.patch(
        "data_lifecycle.hooks.generate_token_for_euphrosyne_backend", return_value="token"
    ):
        with mock.patch("data_lifecycle.hooks.requests.post") as post_mock:
            post_mock.side_effect = [error_response, success_response]
            result = post_lifecycle_operation_callback(
                payload,
                max_attempts=2,
                initial_backoff_seconds=0,
                sleep=lambda _: None,
            )

    assert result is True
    assert post_mock.call_count == 2


def test_post_lifecycle_operation_callback_success(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("EUPHROSYNE_BACKEND_URL", "http://localhost")
    payload = LifecycleOperation(
        project_slug="project-2",
        operation_id=uuid4(),
        type=LifecycleOperationType.RESTORE,
        status=LifecycleOperationStatus.SUCCEEDED,
    )
    success_response = mock.MagicMock()
    success_response.status_code = 200
    success_response.ok = True
    success_response.text = "ok"

    with mock.patch(
        "data_lifecycle.hooks.generate_token_for_euphrosyne_backend",
        return_value="token",
    ):
        with mock.patch("data_lifecycle.hooks.requests.post") as post_mock:
            post_mock.return_value = success_response
            result = post_lifecycle_operation_callback(payload)

    assert result is True
    assert post_mock.call_count == 1


def test_post_lifecycle_operation_callback_rejected_on_4xx(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("EUPHROSYNE_BACKEND_URL", "http://localhost")
    payload = LifecycleOperation(
        project_slug="project-3",
        operation_id=uuid4(),
        type=LifecycleOperationType.COOL,
        status=LifecycleOperationStatus.SUCCEEDED,
    )
    response = mock.MagicMock()
    response.status_code = 400
    response.ok = False
    response.text = "bad request"

    with mock.patch(
        "data_lifecycle.hooks.generate_token_for_euphrosyne_backend",
        return_value="token",
    ):
        with mock.patch("data_lifecycle.hooks.requests.post") as post_mock:
            post_mock.return_value = response
            result = post_lifecycle_operation_callback(payload)

    assert result is False
    assert post_mock.call_count == 1


def test_post_lifecycle_operation_callback_fails_when_url_missing(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.delenv("EUPHROSYNE_BACKEND_URL", raising=False)
    payload = LifecycleOperation(
        project_slug="project-4",
        operation_id=uuid4(),
        type=LifecycleOperationType.COOL,
        status=LifecycleOperationStatus.SUCCEEDED,
    )
    result = post_lifecycle_operation_callback(payload)
    assert result is False


def test_post_lifecycle_operation_serialization(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("EUPHROSYNE_BACKEND_URL", "http://localhost")
    operation_id = uuid4()
    finished_at = datetime.datetime.now(datetime.timezone.utc)
    payload = LifecycleOperation(
        project_slug="project-5",
        operation_id=operation_id,
        type=LifecycleOperationType.RESTORE,
        status=LifecycleOperationStatus.FAILED,
        finished_at=finished_at,
        bytes_copied=12345,
        files_copied=67,
        error_message=None,
        error_details=None,
    )

    with mock.patch(
        "data_lifecycle.hooks.generate_token_for_euphrosyne_backend",
        return_value="token",
    ):
        with mock.patch(
            "requests.sessions.Session.send",
            mock.MagicMock(return_value=mock.MagicMock(status_code=200)),
        ) as send_mock:
            post_lifecycle_operation_callback(payload)

    assert send_mock.call_count == 1

    json_body = json.loads(send_mock.call_args[0][0].body)
    assert json_body["project_slug"] == "project-5"
    assert json_body["operation_id"] == str(operation_id)
    assert json_body["type"] == "RESTORE"
    assert json_body["status"] == "FAILED"
    assert json_body["finished_at"] == finished_at.isoformat()
    assert json_body["bytes_copied"] == 12345
    assert json_body["files_copied"] == 67
    assert json_body["error_message"] is None
    assert json_body["error_details"] is None
