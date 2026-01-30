from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from data_lifecycle import operation as lifecycle_operation
from data_lifecycle.models import LifecycleOperationType
from auth import verify_is_euphrosyne_backend


@pytest.fixture(autouse=True)
def authenticate_euphrosyne_backend(app: FastAPI):
    app.dependency_overrides[verify_is_euphrosyne_backend] = MagicMock
    yield
    app.dependency_overrides.pop(verify_is_euphrosyne_backend, None)


@pytest.fixture(autouse=True)
def reset_lifecycle_guard():
    lifecycle_operation._reset_lifecycle_operation_guard()
    yield
    lifecycle_operation._reset_lifecycle_operation_guard()


def test_cool_endpoint_accepts_and_schedules_background_task(
    app: FastAPI, client: TestClient
):
    operation_id = uuid4()
    with patch("fastapi.BackgroundTasks.add_task") as add_task_mock:
        response = client.post(
            f"/data/projects/project-1/cool?operation_id={operation_id}"
        )

    assert response.status_code == 202
    assert response.json() == {
        "operation_id": str(operation_id),
        "project_slug": "project-1",
        "type": "COOL",
        "status": "ACCEPTED",
    }
    assert add_task_mock.call_count == 1
    args, kwargs = add_task_mock.call_args
    assert args[0] == lifecycle_operation._execute_lifecycle_operation
    assert kwargs["operation"] == lifecycle_operation.LifecycleOperation(
        project_slug="project-1",
        operation_id=operation_id,
        type=LifecycleOperationType.COOL,
        status=lifecycle_operation.LifecycleOperationStatus.ACCEPTED,
    )


def test_duplicate_request_does_not_schedule_twice(app: FastAPI, client: TestClient):
    operation_id = uuid4()
    with patch("fastapi.BackgroundTasks.add_task") as add_task_mock:
        first_response = client.post(
            f"/data/projects/project-1/cool?operation_id={operation_id}"
        )
        second_response = client.post(
            f"/data/projects/project-1/cool?operation_id={operation_id}"
        )

    assert first_response.status_code == 202
    assert second_response.status_code == 202
    assert add_task_mock.call_count == 1


def test_execute_operation_sends_success_callback_payload(
    monkeypatch: pytest.MonkeyPatch,
):
    operation_id = uuid4()
    captured: dict[str, object] = {}

    def fake_post(operation: object) -> bool:
        captured["operation"] = operation
        return True

    monkeypatch.setattr(
        lifecycle_operation, "post_lifecycle_operation_callback", fake_post
    )

    lifecycle_operation._execute_lifecycle_operation(
        operation=lifecycle_operation.LifecycleOperation(
            project_slug="project-1",
            operation_id=operation_id,
            type=LifecycleOperationType.COOL,
        )
    )

    operation = captured["operation"]
    assert operation.operation_id == operation_id
    assert operation.project_slug == "project-1"
    assert operation.type == LifecycleOperationType.COOL
    assert operation.status == lifecycle_operation.LifecycleOperationStatus.SUCCEEDED
    assert operation.bytes_copied is None
    assert operation.files_copied is None
    assert operation.error_message is None
    assert operation.finished_at is not None


def test_execute_operation_sends_failed_callback_payload(
    monkeypatch: pytest.MonkeyPatch,
):
    operation_id = uuid4()
    captured: dict[str, object] = {}

    def fake_post(operation: object) -> bool:
        captured["operation"] = operation
        return True

    def fail_operation(**_kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(
        lifecycle_operation, "post_lifecycle_operation_callback", fake_post
    )
    monkeypatch.setattr(
        lifecycle_operation, "_perform_lifecycle_operation", fail_operation
    )

    lifecycle_operation._execute_lifecycle_operation(
        operation=lifecycle_operation.LifecycleOperation(
            project_slug="project-1",
            operation_id=operation_id,
            type=LifecycleOperationType.RESTORE,
        )
    )

    operation = captured["operation"]
    assert operation.operation_id == operation_id
    assert operation.project_slug == "project-1"
    assert operation.type == LifecycleOperationType.RESTORE
    assert operation.status == lifecycle_operation.LifecycleOperationStatus.FAILED
    assert operation.error_message == "boom"
    assert operation.bytes_copied is None
    assert operation.files_copied is None
    assert operation.error_details is not None


def test_restore_endpoint_accepts_operation_id(app: FastAPI, client: TestClient):
    operation_id = uuid4()
    with patch("fastapi.BackgroundTasks.add_task"):
        response = client.post(
            f"/data/projects/project-2/restore?operation_id={operation_id}"
        )

    assert response.status_code == 202
    assert response.json()["operation_id"] == str(operation_id)
    assert response.json()["type"] == "RESTORE"


def test_operation_guard_is_cleared_after_execution(monkeypatch: pytest.MonkeyPatch):
    operation_id = uuid4()
    operation = lifecycle_operation.LifecycleOperation(
        project_slug="project-1",
        operation_id=operation_id,
        type=LifecycleOperationType.COOL,
    )

    monkeypatch.setattr(
        lifecycle_operation, "post_lifecycle_operation_callback", lambda _: True
    )

    assert (
        lifecycle_operation._register_lifecycle_operation(operation=operation) is True
    )
    lifecycle_operation._execute_lifecycle_operation(operation=operation)
    assert (
        lifecycle_operation._register_lifecycle_operation(operation=operation) is True
    )
