from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import auth
from auth import verify_is_euphrosyne_backend
from data_lifecycle import operation as lifecycle_operation
from data_lifecycle.azcopy_runner import (
    AzCopyJobNotFoundError,
    AzCopyJobRef,
    AzCopySummary,
)
from data_lifecycle.models import (
    LifecycleOperation,
    LifecycleOperationProgressStatus,
    LifecycleOperationType,
)
from data_lifecycle.storage_resolver import StorageRole
from data_lifecycle.storage_types import StorageRole


@pytest.fixture(autouse=True)
def authenticate_euphrosyne_backend(app: FastAPI):
    app.dependency_overrides[verify_is_euphrosyne_backend] = lambda: MagicMock()
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
    assert lifecycle_operation._LIFECYCLE_OPERATION_GUARD == {
        ("project-1", "COOL", str(operation_id))
    }
    assert operation_id not in lifecycle_operation._LIFECYCLE_OPERATION_JOB_ID


def test_cool_endpoint_missing_operation_id_returns_422(
    app: FastAPI, client: TestClient
):
    with patch("fastapi.BackgroundTasks.add_task") as add_task_mock:
        response = client.post("/data/projects/project-1/cool")

    assert response.status_code == 422
    assert add_task_mock.call_count == 0


def test_cool_endpoint_invalid_operation_id_returns_422(
    app: FastAPI, client: TestClient
):
    with patch("fastapi.BackgroundTasks.add_task") as add_task_mock:
        response = client.post("/data/projects/project-1/cool?operation_id=not-a-uuid")

    assert response.status_code == 422
    assert add_task_mock.call_count == 0


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
    assert lifecycle_operation._LIFECYCLE_OPERATION_GUARD == {
        ("project-1", "COOL", str(operation_id))
    }


def test_same_operation_id_for_another_project_schedules_again(
    app: FastAPI, client: TestClient
):
    operation_id = uuid4()
    with patch("fastapi.BackgroundTasks.add_task") as add_task_mock:
        first_response = client.post(
            f"/data/projects/project-1/cool?operation_id={operation_id}"
        )
        second_response = client.post(
            f"/data/projects/project-2/cool?operation_id={operation_id}"
        )

    assert first_response.status_code == 202
    assert second_response.status_code == 202
    assert add_task_mock.call_count == 2


def test_execute_cool_operation_sets_job_id_and_sends_success_callback(
    monkeypatch: pytest.MonkeyPatch,
):
    operation_id = uuid4()
    captured: dict[str, LifecycleOperation] = {}

    def fake_post(operation: LifecycleOperation) -> bool:
        captured["operation"] = operation
        return True

    monkeypatch.setattr(
        lifecycle_operation, "post_lifecycle_operation_callback", fake_post
    )
    monkeypatch.setattr(
        lifecycle_operation,
        "_build_signed_cool_copy_urls",
        lambda **_kwargs: (
            "https://hot.example/project-1/*?source-token",
            "https://cool.example/project-1?dest-token",
        ),
    )
    monkeypatch.setattr(
        lifecycle_operation.azcopy_runner,
        "start_copy",
        lambda _source, _dest: AzCopyJobRef(
            job_id="job-1",
            started_at=datetime.now(timezone.utc),
            command=["azcopy", "copy"],
            environment={},
            log_dir="/tmp/.azcopy",
        ),
    )
    monkeypatch.setattr(
        lifecycle_operation.azcopy_runner,
        "poll",
        lambda _job_id: AzCopySummary(
            state="SUCCEEDED",
            files_transferred=7,
            bytes_transferred=1024,
            failed_transfers=0,
            skipped_transfers=0,
            files_total=7,
            bytes_total=1024,
            progress_percent=100.0,
            stdout_log_path="/tmp/.azcopy/azcopy-job-1-stdout.log",
            stderr_log_path="/tmp/.azcopy/azcopy-job-1-stderr.log",
        ),
    )
    monkeypatch.setattr(
        lifecycle_operation.azcopy_runner,
        "get_summary",
        lambda _job_id: AzCopySummary(
            state="SUCCEEDED",
            files_transferred=7,
            bytes_transferred=1024,
            failed_transfers=0,
            skipped_transfers=0,
            files_total=7,
            bytes_total=1024,
            progress_percent=100.0,
            stdout_log_path="/tmp/.azcopy/azcopy-job-1-stdout.log",
            stderr_log_path="/tmp/.azcopy/azcopy-job-1-stderr.log",
        ),
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
    assert operation.bytes_copied == 1024
    assert operation.files_copied == 7
    assert operation.error_message is None
    assert operation.finished_at is not None
    assert lifecycle_operation._LIFECYCLE_OPERATION_JOB_ID[operation_id] == "job-1"


def test_execute_cool_operation_failure_before_job_id_keeps_job_map_empty(
    monkeypatch: pytest.MonkeyPatch,
):
    operation_id = uuid4()
    captured: dict[str, LifecycleOperation] = {}

    def fake_post(operation: LifecycleOperation) -> bool:
        captured["operation"] = operation
        return True

    monkeypatch.setattr(
        lifecycle_operation, "post_lifecycle_operation_callback", fake_post
    )
    monkeypatch.setattr(
        lifecycle_operation,
        "_build_signed_cool_copy_urls",
        lambda **_kwargs: (
            "https://hot.example/project-1/*?source-token",
            "https://cool.example/project-1?dest-token",
        ),
    )
    monkeypatch.setattr(
        lifecycle_operation.azcopy_runner,
        "start_copy",
        lambda _source, _dest: (_ for _ in ()).throw(RuntimeError("boom")),
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
    assert operation.status == lifecycle_operation.LifecycleOperationStatus.FAILED
    assert operation.error_message == "boom"
    assert operation.bytes_copied is None
    assert operation.files_copied is None
    assert operation.error_details is not None
    assert operation.error_details["type"] == "RuntimeError"
    assert operation_id not in lifecycle_operation._LIFECYCLE_OPERATION_JOB_ID


def test_execute_cool_operation_failure_after_job_id_contains_diagnostics(
    monkeypatch: pytest.MonkeyPatch,
):
    operation_id = uuid4()
    captured: dict[str, LifecycleOperation] = {}

    def fake_post(operation: LifecycleOperation) -> bool:
        captured["operation"] = operation
        return True

    monkeypatch.setattr(
        lifecycle_operation, "post_lifecycle_operation_callback", fake_post
    )
    monkeypatch.setattr(
        lifecycle_operation,
        "_build_signed_cool_copy_urls",
        lambda **_kwargs: (
            "https://hot.example/project-1/*?source-token",
            "https://cool.example/project-1?dest-token",
        ),
    )
    monkeypatch.setattr(
        lifecycle_operation.azcopy_runner,
        "start_copy",
        lambda _source, _dest: AzCopyJobRef(
            job_id="job-2",
            started_at=datetime.now(timezone.utc),
            command=["azcopy", "copy"],
            environment={},
            log_dir="/tmp/.azcopy",
        ),
    )
    monkeypatch.setattr(
        lifecycle_operation.azcopy_runner,
        "poll",
        lambda _job_id: AzCopySummary(
            state="FAILED",
            files_transferred=1,
            bytes_transferred=10,
            failed_transfers=2,
            skipped_transfers=0,
            files_total=3,
            bytes_total=30,
            progress_percent=33.3,
            stdout_log_path="/tmp/.azcopy/azcopy-job-2-stdout.log",
            stderr_log_path="/tmp/.azcopy/azcopy-job-2-stderr.log",
        ),
    )
    monkeypatch.setattr(
        lifecycle_operation.azcopy_runner,
        "get_summary",
        lambda _job_id: AzCopySummary(
            state="FAILED",
            files_transferred=1,
            bytes_transferred=10,
            failed_transfers=2,
            skipped_transfers=0,
            files_total=3,
            bytes_total=30,
            progress_percent=33.3,
            stdout_log_path="/tmp/.azcopy/azcopy-job-2-stdout.log",
            stderr_log_path="/tmp/.azcopy/azcopy-job-2-stderr.log",
        ),
    )

    lifecycle_operation._execute_lifecycle_operation(
        operation=lifecycle_operation.LifecycleOperation(
            project_slug="project-1",
            operation_id=operation_id,
            type=LifecycleOperationType.COOL,
        )
    )

    operation = captured["operation"]
    assert operation.status == lifecycle_operation.LifecycleOperationStatus.FAILED
    assert operation.error_details is not None
    assert operation.error_details["job_id"] == "job-2"
    assert operation.error_details["azcopy_state"] == "FAILED"
    assert operation.error_details["failed_transfers"] == 2


def test_execute_restore_operation_sets_job_id_and_sends_success_callback(
    monkeypatch: pytest.MonkeyPatch,
):
    operation_id = uuid4()
    captured: dict[str, LifecycleOperation] = {}

    def fake_post(operation: LifecycleOperation) -> bool:
        captured["operation"] = operation
        return True

    monkeypatch.setattr(
        lifecycle_operation, "post_lifecycle_operation_callback", fake_post
    )
    monkeypatch.setattr(
        lifecycle_operation,
        "_build_signed_restore_copy_urls",
        lambda **_kwargs: (
            "https://cool.example/project-1/*?source-token",
            "https://hot.example/project-1?dest-token",
        ),
    )
    monkeypatch.setattr(
        lifecycle_operation.azcopy_runner,
        "start_copy",
        lambda _source, _dest: AzCopyJobRef(
            job_id="job-restore-1",
            started_at=datetime.now(timezone.utc),
            command=["azcopy", "copy"],
            environment={},
            log_dir="/tmp/.azcopy",
        ),
    )
    monkeypatch.setattr(
        lifecycle_operation.azcopy_runner,
        "poll",
        lambda _job_id: AzCopyProgress(
            state="SUCCEEDED",
            last_updated_at=datetime.now(timezone.utc),
            raw_status="Completed",
        ),
    )
    monkeypatch.setattr(
        lifecycle_operation.azcopy_runner,
        "get_summary",
        lambda _job_id: AzCopySummary(
            state="SUCCEEDED",
            files_transferred=4,
            bytes_transferred=2048,
            failed_transfers=0,
            skipped_transfers=0,
            stdout_log_path="/tmp/.azcopy/azcopy-job-restore-1-stdout.log",
            stderr_log_path="/tmp/.azcopy/azcopy-job-restore-1-stderr.log",
        ),
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
    assert operation.status == lifecycle_operation.LifecycleOperationStatus.SUCCEEDED
    assert operation.bytes_copied == 2048
    assert operation.files_copied == 4
    assert operation.error_message is None
    assert operation.finished_at is not None
    assert (
        lifecycle_operation._LIFECYCLE_OPERATION_JOB_ID[operation_id] == "job-restore-1"
    )


def test_build_signed_cool_copy_urls_matches_script_pattern(
    monkeypatch: pytest.MonkeyPatch,
):
    hot_client = MagicMock()
    hot_client.generate_project_directory_token.return_value = "hot-token"
    cool_client = MagicMock()
    cool_client.generate_project_directory_token.return_value = "cool-token"

    def fake_resolve_location(role: StorageRole, project_slug: str):
        assert project_slug == "project-1"
        if role == StorageRole.HOT:
            return MagicMock(uri="https://hot.example/project-1")
        return MagicMock(uri="https://cool.example/project-1")

    def fake_resolve_backend_client(role: StorageRole):
        if role == StorageRole.HOT:
            return hot_client
        return cool_client

    monkeypatch.setattr(lifecycle_operation, "resolve_location", fake_resolve_location)
    monkeypatch.setattr(
        lifecycle_operation,
        "resolve_backend_client",
        fake_resolve_backend_client,
    )

    source_uri, destination_uri = lifecycle_operation._build_signed_cool_copy_urls(
        project_slug="project-1"
    )

    assert source_uri == "https://hot.example/project-1/*?hot-token"
    assert destination_uri == "https://cool.example/project-1?cool-token"
    hot_client.generate_project_directory_token.assert_called_once_with(
        project_name="project-1",
        permission=lifecycle_operation._COPY_SOURCE_TOKEN_PERMISSIONS,
    )
    cool_client.generate_project_directory_token.assert_called_once_with(
        project_name="project-1",
        permission=lifecycle_operation._COPY_DEST_TOKEN_PERMISSIONS,
        force_write=True,
    )


def test_build_signed_restore_copy_urls_matches_script_pattern(
    monkeypatch: pytest.MonkeyPatch,
):
    hot_client = MagicMock()
    hot_client.generate_project_directory_token.return_value = "hot-token"
    cool_client = MagicMock()
    cool_client.generate_project_directory_token.return_value = "cool-token"

    def fake_resolve_location(role: StorageRole, project_slug: str):
        assert project_slug == "project-1"
        if role == StorageRole.COOL:
            return MagicMock(uri="https://cool.example/project-1")
        return MagicMock(uri="https://hot.example/project-1")

    def fake_resolve_backend_client(role: StorageRole):
        if role == StorageRole.COOL:
            return cool_client
        return hot_client

    monkeypatch.setattr(lifecycle_operation, "resolve_location", fake_resolve_location)
    monkeypatch.setattr(
        lifecycle_operation,
        "resolve_backend_client",
        fake_resolve_backend_client,
    )

    source_uri, destination_uri = lifecycle_operation._build_signed_restore_copy_urls(
        project_slug="project-1"
    )

    assert source_uri == "https://cool.example/project-1/*?cool-token"
    assert destination_uri == "https://hot.example/project-1?hot-token"
    cool_client.generate_project_directory_token.assert_called_once_with(
        project_name="project-1",
        permission=lifecycle_operation._COPY_SOURCE_TOKEN_PERMISSIONS,
    )
    hot_client.generate_project_directory_token.assert_called_once_with(
        project_name="project-1",
        permission=lifecycle_operation._COPY_DEST_TOKEN_PERMISSIONS,
        force_write=True,
    )


def test_await_terminal_azcopy_summary_retries_when_job_not_found(
    monkeypatch: pytest.MonkeyPatch,
):
    summary = AzCopySummary(
        state="SUCCEEDED",
        files_transferred=3,
        bytes_transferred=11,
        failed_transfers=0,
        skipped_transfers=0,
        files_total=3,
        bytes_total=11,
        progress_percent=100.0,
        stdout_log_path="/tmp/stdout.log",
        stderr_log_path="/tmp/stderr.log",
    )
    job_not_found = AzCopyJobNotFoundError(
        "not found",
        job_id="job-1",
        log_dir="/tmp/.azcopy",
        stdout_excerpt=None,
        stderr_excerpt=None,
    )
    poll_mock = MagicMock(
        side_effect=[
            job_not_found,
            AzCopySummary(
                state="SUCCEEDED",
                files_transferred=3,
                bytes_transferred=11,
                failed_transfers=0,
                skipped_transfers=0,
                files_total=3,
                bytes_total=11,
                progress_percent=100.0,
                stdout_log_path="/tmp/stdout.log",
                stderr_log_path="/tmp/stderr.log",
            ),
        ]
    )

    monkeypatch.setattr(lifecycle_operation.azcopy_runner, "poll", poll_mock)
    monkeypatch.setattr(
        lifecycle_operation.azcopy_runner,
        "get_summary",
        lambda _job_id: summary,
    )
    sleep_mock = MagicMock()
    monkeypatch.setattr(lifecycle_operation.time, "sleep", sleep_mock)

    result = lifecycle_operation._await_terminal_azcopy_summary(job_id="job-1")

    assert result == summary
    assert poll_mock.call_count == 2
    sleep_mock.assert_called_once()


def test_restore_endpoint_accepts_operation_id(app: FastAPI, client: TestClient):
    operation_id = uuid4()
    with patch("fastapi.BackgroundTasks.add_task"):
        response = client.post(
            f"/data/projects/project-2/restore?operation_id={operation_id}"
        )

    assert response.status_code == 202
    assert response.json()["operation_id"] == str(operation_id)
    assert response.json()["type"] == "RESTORE"


def test_operation_guard_is_not_cleared_after_execution(
    monkeypatch: pytest.MonkeyPatch,
):
    operation_id = uuid4()
    operation = lifecycle_operation.LifecycleOperation(
        project_slug="project-1",
        operation_id=operation_id,
        type=LifecycleOperationType.RESTORE,
    )

    monkeypatch.setattr(
        lifecycle_operation, "post_lifecycle_operation_callback", lambda _: True
    )
    monkeypatch.setattr(
        lifecycle_operation,
        "_perform_lifecycle_operation",
        lambda **_kwargs: (None, None),
    )

    assert (
        lifecycle_operation._register_lifecycle_operation(operation=operation) is True
    )
    lifecycle_operation._execute_lifecycle_operation(operation=operation)
    assert (
        lifecycle_operation._register_lifecycle_operation(operation=operation) is False
    )


def test_cool_status_endpoint_returns_pending_when_job_not_assigned(
    app: FastAPI, client: TestClient
):
    operation_id = uuid4()
    operation = lifecycle_operation.LifecycleOperation(
        project_slug="project-1",
        operation_id=operation_id,
        type=LifecycleOperationType.COOL,
    )
    assert (
        lifecycle_operation._register_lifecycle_operation(operation=operation) is True
    )

    response = client.get(f"/data/projects/project-1/cool/{operation_id}")

    assert response.status_code == 200
    assert response.json() == {
        "operation_id": str(operation_id),
        "project_slug": "project-1",
        "type": "COOL",
        "status": "PENDING",
        "bytes_total": 0,
        "files_total": 0,
        "bytes_copied": 0,
        "files_copied": 0,
        "progress_percent": 0.0,
    }


def test_restore_status_endpoint_returns_running_with_progress_fallback(
    app: FastAPI, client: TestClient, monkeypatch: pytest.MonkeyPatch
):
    operation_id = uuid4()
    operation = lifecycle_operation.LifecycleOperation(
        project_slug="project-2",
        operation_id=operation_id,
        type=LifecycleOperationType.RESTORE,
    )
    assert (
        lifecycle_operation._register_lifecycle_operation(operation=operation) is True
    )
    lifecycle_operation._set_lifecycle_operation_job_id(
        operation_id=operation_id, job_id="job-restore"
    )
    monkeypatch.setattr(
        lifecycle_operation.azcopy_runner,
        "poll",
        lambda _job_id: AzCopySummary(
            state="RUNNING",
            files_transferred=5,
            bytes_transferred=100,
            failed_transfers=0,
            skipped_transfers=0,
            files_total=20,
            bytes_total=200,
            progress_percent=50.0,
            stdout_log_path="/tmp/stdout.log",
            stderr_log_path="/tmp/stderr.log",
        ),
    )

    response = client.get(f"/data/projects/project-2/restore/{operation_id}")

    assert response.status_code == 200
    assert response.json() == {
        "operation_id": str(operation_id),
        "project_slug": "project-2",
        "type": "RESTORE",
        "status": "RUNNING",
        "bytes_total": 200,
        "files_total": 20,
        "bytes_copied": 100,
        "files_copied": 5,
        "progress_percent": 50.0,
    }


def test_status_endpoint_returns_failed_with_error_details(
    app: FastAPI, client: TestClient, monkeypatch: pytest.MonkeyPatch
):
    operation_id = uuid4()
    operation = lifecycle_operation.LifecycleOperation(
        project_slug="project-3",
        operation_id=operation_id,
        type=LifecycleOperationType.COOL,
    )
    assert (
        lifecycle_operation._register_lifecycle_operation(operation=operation) is True
    )
    lifecycle_operation._set_lifecycle_operation_job_id(
        operation_id=operation_id, job_id="job-failed"
    )
    monkeypatch.setattr(
        lifecycle_operation.azcopy_runner,
        "poll",
        lambda _job_id: AzCopySummary(
            state="FAILED",
            files_transferred=8,
            bytes_transferred=875,
            failed_transfers=2,
            skipped_transfers=0,
            files_total=10,
            bytes_total=1000,
            progress_percent=87.5,
            stdout_log_path="/tmp/stdout.log",
            stderr_log_path="/tmp/stderr.log",
        ),
    )

    response = client.get(f"/data/projects/project-3/cool/{operation_id}")

    assert response.status_code == 200
    json = response.json()
    assert json["status"] == "FAILED"
    assert json["progress_percent"] == 87.5
    assert json["error_details"]["message"] == "AzCopy reported 2 failed transfer(s)"
    assert json["error_details"]["raw"]["job_id"] == "job-failed"
    assert json["error_details"]["raw"]["azcopy_state"] == "FAILED"


def test_status_endpoint_returns_error_details_for_canceled_job(
    app: FastAPI, client: TestClient, monkeypatch: pytest.MonkeyPatch
):
    operation_id = uuid4()
    operation = lifecycle_operation.LifecycleOperation(
        project_slug="project-4",
        operation_id=operation_id,
        type=LifecycleOperationType.COOL,
    )
    assert (
        lifecycle_operation._register_lifecycle_operation(operation=operation) is True
    )
    lifecycle_operation._set_lifecycle_operation_job_id(
        operation_id=operation_id, job_id="job-canceled"
    )
    monkeypatch.setattr(
        lifecycle_operation.azcopy_runner,
        "poll",
        lambda _job_id: AzCopySummary(
            state="CANCELED",
            files_transferred=4,
            bytes_transferred=400,
            failed_transfers=0,
            skipped_transfers=0,
            files_total=10,
            bytes_total=1000,
            progress_percent=40.0,
            stdout_log_path="/tmp/stdout.log",
            stderr_log_path="/tmp/stderr.log",
        ),
    )

    response = client.get(f"/data/projects/project-4/cool/{operation_id}")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "FAILED"
    assert body["error_details"]["message"] == "AzCopy job was canceled"
    assert body["error_details"]["raw"]["job_id"] == "job-canceled"
    assert body["error_details"]["raw"]["azcopy_state"] == "CANCELED"


def test_status_endpoint_returns_404_when_operation_unknown_for_project_or_type(
    app: FastAPI, client: TestClient
):
    operation_id = uuid4()
    tracked = lifecycle_operation.LifecycleOperation(
        project_slug="project-a",
        operation_id=operation_id,
        type=LifecycleOperationType.COOL,
    )
    assert lifecycle_operation._register_lifecycle_operation(operation=tracked) is True

    wrong_project = client.get(f"/data/projects/project-b/cool/{operation_id}")
    wrong_type = client.get(f"/data/projects/project-a/restore/{operation_id}")

    assert wrong_project.status_code == 404
    assert wrong_type.status_code == 404


def test_status_endpoint_requires_backend_token_returns_401(
    app: FastAPI, client: TestClient
):
    app.dependency_overrides.pop(verify_is_euphrosyne_backend, None)
    response = client.get(f"/data/projects/project-1/cool/{uuid4()}")
    assert response.status_code == 401


def test_status_endpoint_rejects_non_backend_token_with_403(
    app: FastAPI, client: TestClient, monkeypatch: pytest.MonkeyPatch
):
    app.dependency_overrides.pop(verify_is_euphrosyne_backend, None)
    monkeypatch.setattr(auth, "_is_euphrosyne_backend", lambda _token: False)

    response = client.get(
        f"/data/projects/project-1/cool/{uuid4()}",
        headers={"Authorization": "Bearer not-backend"},
    )

    assert response.status_code == 403


def test_map_azcopy_status_to_lifecycle_status():
    assert (
        lifecycle_operation._map_azcopy_status("PENDING")
        == LifecycleOperationProgressStatus.PENDING
    )
    assert (
        lifecycle_operation._map_azcopy_status("RUNNING")
        == LifecycleOperationProgressStatus.RUNNING
    )
    assert (
        lifecycle_operation._map_azcopy_status("SUCCEEDED")
        == LifecycleOperationProgressStatus.SUCCEEDED
    )
    assert (
        lifecycle_operation._map_azcopy_status("CANCELED")
        == LifecycleOperationProgressStatus.FAILED
    )
