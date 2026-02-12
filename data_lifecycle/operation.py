import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from fastapi import BackgroundTasks

from clients.data_models import TokenPermissions
from data_lifecycle import azcopy_runner
from data_lifecycle.hooks import post_lifecycle_operation_callback
from data_lifecycle.models import (
    LifecycleOperation,
    LifecycleOperationProgressStatus,
    LifecycleOperationStatus,
    LifecycleOperationStatusView,
    LifecycleOperationType,
)
from data_lifecycle.storage_resolver import (
    StorageRole,
    resolve_backend_client,
    resolve_location,
)

from . import azcopy_runner
from .hooks import post_lifecycle_operation_callback
from .models import LifecycleOperation, LifecycleOperationStatus, LifecycleOperationType
from .storage_resolver import resolve_backend_client, resolve_location
from .storage_types import StorageRole

logger = logging.getLogger(__name__)


# In-process dedupe for (project_slug, type, operation_id).
# Protected by the lock to avoid races between concurrent requests.
# Per-process only: no persistence, no cross-worker coordination.
_LIFECYCLE_OPERATION_GUARD: set[tuple[str, str, str]] = set()
_LIFECYCLE_OPERATION_GUARD_LOCK = threading.Lock()
_LIFECYCLE_OPERATION_JOB_ID: dict[UUID, str] = {}
_LIFECYCLE_OPERATION_JOB_ID_LOCK = threading.Lock()

_TERMINAL_JOB_STATES = {"SUCCEEDED", "FAILED", "CANCELED", "UNKNOWN"}
_AZCOPY_POLL_INTERVAL_SECONDS = 5.0
_AZCOPY_POLL_JOB_NOT_FOUND_MAX_RETRIES = 3

_COPY_SOURCE_TOKEN_PERMISSIONS: TokenPermissions = {
    "list": True,
    "read": True,
    "add": False,
    "create": False,
    "write": False,
    "delete": False,
}

_COPY_DEST_TOKEN_PERMISSIONS: TokenPermissions = {
    "read": True,
    "add": True,
    "create": True,
    "write": True,
    "delete": True,
}


class LifecycleOperationExecutionError(Exception):
    def __init__(self, message: str, *, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.details = details or {}


class LifecycleOperationNotFoundError(Exception):
    pass


def get_lifecycle_operation_status(
    *,
    project_slug: str,
    operation_id: UUID,
    operation_type: LifecycleOperationType,
) -> LifecycleOperationStatusView:
    if not _is_tracked_lifecycle_operation(
        project_slug=project_slug,
        operation_id=operation_id,
        operation_type=operation_type,
    ):
        raise LifecycleOperationNotFoundError

    azcopy_job_id = _get_lifecycle_operation_job_id(operation_id=operation_id)
    if azcopy_job_id is None:
        return LifecycleOperationStatusView(
            operation_id=operation_id,
            project_slug=project_slug,
            type=operation_type,
            status=LifecycleOperationProgressStatus.PENDING,
            bytes_total=0,
            files_total=0,
            bytes_copied=0,
            files_copied=0,
            progress_percent=0.0,
            error_details=None,
        )

    try:
        azcopy_status = azcopy_runner.poll(azcopy_job_id)
    except azcopy_runner.AzCopyJobNotFoundError:
        logger.warning(
            "Lifecycle status query returned job-not-found, treating as pending: operation_id=%s project_slug=%s type=%s azcopy_job_id=%s",
            operation_id,
            project_slug,
            operation_type.value,
            azcopy_job_id,
        )
        return LifecycleOperationStatusView(
            operation_id=operation_id,
            project_slug=project_slug,
            type=operation_type,
            status=LifecycleOperationProgressStatus.PENDING,
            bytes_total=0,
            files_total=0,
            bytes_copied=0,
            files_copied=0,
            progress_percent=0.0,
            error_details=None,
        )
    except azcopy_runner.AzCopyRunnerError as exc:
        logger.error(
            "Lifecycle status query failed: operation_id=%s project_slug=%s type=%s azcopy_job_id=%s error=%s",
            operation_id,
            project_slug,
            operation_type.value,
            azcopy_job_id,
            exc,
        )
        return LifecycleOperationStatusView(
            operation_id=operation_id,
            project_slug=project_slug,
            type=operation_type,
            status=LifecycleOperationProgressStatus.FAILED,
            bytes_total=0,
            files_total=0,
            bytes_copied=0,
            files_copied=0,
            progress_percent=0.0,
            error_details={
                "message": "Failed to query AzCopy job status",
                "raw": {
                    "job_id": exc.job_id,
                    "log_dir": exc.log_dir,
                    "stdout_path": exc.stdout_path,
                    "stderr_path": exc.stderr_path,
                    "stdout_excerpt": exc.stdout_excerpt,
                    "stderr_excerpt": exc.stderr_excerpt,
                },
            },
        )

    status = _map_azcopy_status(azcopy_status.state)
    error_details = None
    if status == LifecycleOperationProgressStatus.FAILED:
        error_details = {
            "message": _build_failed_message(
                azcopy_state=azcopy_status.state,
                failed_transfers=azcopy_status.failed_transfers,
            ),
            "raw": {
                "job_id": azcopy_job_id,
                "azcopy_state": azcopy_status.state,
                "failed_transfers": azcopy_status.failed_transfers,
                "skipped_transfers": azcopy_status.skipped_transfers,
                "stdout_log_path": azcopy_status.stdout_log_path,
                "stderr_log_path": azcopy_status.stderr_log_path,
            },
        }

    return LifecycleOperationStatusView(
        operation_id=operation_id,
        project_slug=project_slug,
        type=operation_type,
        status=status,
        bytes_total=azcopy_status.bytes_total,
        files_total=azcopy_status.files_total,
        bytes_copied=azcopy_status.bytes_transferred,
        files_copied=azcopy_status.files_transferred,
        progress_percent=azcopy_status.progress_percent,
        error_details=error_details,
    )


def _is_tracked_lifecycle_operation(
    *,
    project_slug: str,
    operation_id: UUID,
    operation_type: LifecycleOperationType,
) -> bool:
    key = (
        project_slug,
        operation_type.value,
        str(operation_id),
    )
    with _LIFECYCLE_OPERATION_GUARD_LOCK:
        return key in _LIFECYCLE_OPERATION_GUARD


def _map_azcopy_status(
    azcopy_state: azcopy_runner.AzCopyJobState,
) -> LifecycleOperationProgressStatus:
    if azcopy_state == "PENDING":
        return LifecycleOperationProgressStatus.PENDING
    if azcopy_state == "RUNNING":
        return LifecycleOperationProgressStatus.RUNNING
    if azcopy_state == "SUCCEEDED":
        return LifecycleOperationProgressStatus.SUCCEEDED
    return LifecycleOperationProgressStatus.FAILED


def _build_failed_message(*, azcopy_state: str, failed_transfers: int) -> str:
    if azcopy_state == "CANCELED":
        return "AzCopy job was canceled"
    if failed_transfers > 0:
        return f"AzCopy reported {failed_transfers} failed transfer(s)"
    if azcopy_state == "UNKNOWN":
        return "AzCopy returned an unknown status"
    return "AzCopy job failed"


def schedule_lifecycle_operation(
    *,
    operation: LifecycleOperation,
    background_tasks: BackgroundTasks,
) -> LifecycleOperation:
    """Register a lifecycle operation and enqueue its execution if not duplicate."""
    accepted = _register_lifecycle_operation(operation=operation)
    if accepted:
        background_tasks.add_task(
            _execute_lifecycle_operation,
            operation=operation,
        )
        logger.info(
            "Lifecycle operation accepted: operation_id=%s project_slug=%s type=%s",
            operation.operation_id,
            operation.project_slug,
            operation.type.value,
        )
    else:
        logger.info(
            "Lifecycle operation already tracked, skipping duplicate: operation_id=%s project_slug=%s type=%s",
            operation.operation_id,
            operation.project_slug,
            operation.type.value,
        )
    operation.status = LifecycleOperationStatus.ACCEPTED
    return operation


def _register_lifecycle_operation(
    *,
    operation: LifecycleOperation,
) -> bool:
    """Track an operation in-memory to avoid duplicate execution in this process."""
    key = operation.guard_key()
    with _LIFECYCLE_OPERATION_GUARD_LOCK:
        if key in _LIFECYCLE_OPERATION_GUARD:
            return False
        _LIFECYCLE_OPERATION_GUARD.add(key)
        return True


def _reset_lifecycle_operation_guard() -> None:
    """Clear the in-memory guard; intended for tests to keep state isolated."""
    with _LIFECYCLE_OPERATION_GUARD_LOCK:
        _LIFECYCLE_OPERATION_GUARD.clear()
    with _LIFECYCLE_OPERATION_JOB_ID_LOCK:
        _LIFECYCLE_OPERATION_JOB_ID.clear()


def _perform_lifecycle_operation(
    *,
    operation: LifecycleOperation,
) -> tuple[int | None, int | None]:
    """Execute lifecycle data movement and return bytes/files copied."""
    if operation.type == LifecycleOperationType.COOL:
        source_uri, destination_uri = _build_signed_cool_copy_urls(
            project_slug=operation.project_slug
        )
        return _perform_azcopy_lifecycle_operation(
            operation=operation,
            source_uri=source_uri,
            destination_uri=destination_uri,
        )
    if operation.type == LifecycleOperationType.RESTORE:
        source_uri, destination_uri = _build_signed_restore_copy_urls(
            project_slug=operation.project_slug
        )
        return _perform_azcopy_lifecycle_operation(
            operation=operation,
            source_uri=source_uri,
            destination_uri=destination_uri,
        )
    return None, None


def _build_signed_cool_copy_urls(*, project_slug: str) -> tuple[str, str]:
    return _build_signed_copy_urls(
        source_role=StorageRole.HOT,
        destination_role=StorageRole.COOL,
        project_slug=project_slug,
    )


def _build_signed_restore_copy_urls(*, project_slug: str) -> tuple[str, str]:
    return _build_signed_copy_urls(
        source_role=StorageRole.COOL,
        destination_role=StorageRole.HOT,
        project_slug=project_slug,
    )


def _build_signed_copy_urls(
    *,
    source_role: StorageRole,
    destination_role: StorageRole,
    project_slug: str,
) -> tuple[str, str]:
    source_location = resolve_location(source_role, project_slug)
    destination_location = resolve_location(destination_role, project_slug)

    source_client = resolve_backend_client(source_role)
    destination_client = resolve_backend_client(destination_role)

    source_uri = (
        f"{source_location.uri}/*?"
        f"{source_client.generate_project_directory_token(project_name=project_slug, permission=_COPY_SOURCE_TOKEN_PERMISSIONS)}"
    )
    destination_uri = (
        f"{destination_location.uri}?"
        f"{destination_client.generate_project_directory_token(project_name=project_slug, permission=_COPY_DEST_TOKEN_PERMISSIONS, force_write=True)}"
    )
    return source_uri, destination_uri


def _perform_azcopy_lifecycle_operation(
    *,
    operation: LifecycleOperation,
    source_uri: str,
    destination_uri: str,
) -> tuple[int, int]:
    job = azcopy_runner.start_copy(source_uri, destination_uri)
    _set_lifecycle_operation_job_id(
        operation_id=operation.operation_id,
        job_id=job.job_id,
    )
    logger.info(
        "Lifecycle operation mapped to AzCopy job: operation_id=%s project_slug=%s type=%s azcopy_job_id=%s",
        operation.operation_id,
        operation.project_slug,
        operation.type.value,
        job.job_id,
    )

    summary = _await_terminal_azcopy_summary(job_id=job.job_id)
    if summary.state != "SUCCEEDED":
        raise LifecycleOperationExecutionError(
            f"AzCopy {operation.type.value} job did not succeed",
            details={
                "job_id": job.job_id,
                "azcopy_state": summary.state,
                "failed_transfers": summary.failed_transfers,
                "skipped_transfers": summary.skipped_transfers,
                "stdout_log_path": summary.stdout_log_path,
                "stderr_log_path": summary.stderr_log_path,
            },
        )

    return summary.bytes_transferred, summary.files_transferred


def _await_terminal_azcopy_summary(
    *,
    job_id: str,
    poll_interval_seconds: float = _AZCOPY_POLL_INTERVAL_SECONDS,
    poll_job_not_found_max_retries: int = _AZCOPY_POLL_JOB_NOT_FOUND_MAX_RETRIES,
) -> azcopy_runner.AzCopySummary:
    job_not_found_retries = 0
    while True:
        try:
            progress = azcopy_runner.poll(job_id)
        except azcopy_runner.AzCopyJobNotFoundError:
            if job_not_found_retries >= poll_job_not_found_max_retries:
                raise
            job_not_found_retries += 1
            logger.warning(
                "AzCopy job not found while polling, retrying: job_id=%s retry=%s/%s",
                job_id,
                job_not_found_retries,
                poll_job_not_found_max_retries,
            )
            time.sleep(poll_interval_seconds)
            continue
        if progress.state in _TERMINAL_JOB_STATES:
            summary = azcopy_runner.get_summary(job_id)
            if summary.state == "RUNNING":
                time.sleep(poll_interval_seconds)
                continue
            return summary
        time.sleep(poll_interval_seconds)


def _set_lifecycle_operation_job_id(*, operation_id: UUID, job_id: str) -> None:
    with _LIFECYCLE_OPERATION_JOB_ID_LOCK:
        _LIFECYCLE_OPERATION_JOB_ID[operation_id] = job_id


def _get_lifecycle_operation_job_id(*, operation_id: UUID) -> str | None:
    with _LIFECYCLE_OPERATION_JOB_ID_LOCK:
        return _LIFECYCLE_OPERATION_JOB_ID.get(operation_id)


def _build_error_details(
    *,
    operation: LifecycleOperation,
    exc: Exception,
) -> dict[str, Any]:
    details: dict[str, Any] = {"type": exc.__class__.__name__}

    job_id = _get_lifecycle_operation_job_id(operation_id=operation.operation_id)
    if job_id is not None:
        details["job_id"] = job_id

    if isinstance(exc, LifecycleOperationExecutionError):
        details.update(exc.details)

    if isinstance(exc, azcopy_runner.AzCopyRunnerError):
        if exc.job_id:
            details["job_id"] = exc.job_id
        if exc.log_dir:
            details["log_dir"] = exc.log_dir
        if exc.stdout_path:
            details["stdout_path"] = exc.stdout_path
        if exc.stderr_path:
            details["stderr_path"] = exc.stderr_path

    return details


def _execute_lifecycle_operation(
    *,
    operation: LifecycleOperation,
) -> None:
    """Run the lifecycle operation and send a completion callback."""
    logger.info(
        "Lifecycle operation started: operation_id=%s project_slug=%s type=%s",
        operation.operation_id,
        operation.project_slug,
        operation.type.value,
    )
    try:
        operation.bytes_copied, operation.files_copied = _perform_lifecycle_operation(
            operation=operation,
        )
        operation.status = LifecycleOperationStatus.SUCCEEDED
    except Exception as exc:  # pylint: disable=broad-except
        operation.status = LifecycleOperationStatus.FAILED
        operation.error_message = str(exc)
        operation.error_details = _build_error_details(
            operation=operation,
            exc=exc,
        )
        logger.error(
            "Lifecycle operation failed: operation_id=%s project_slug=%s type=%s error=%s",
            operation.operation_id,
            operation.project_slug,
            operation.type.value,
            exc,
        )
    finally:
        operation.finished_at = datetime.now(timezone.utc)

        delivered = post_lifecycle_operation_callback(operation)
        if not delivered:
            logger.error(
                "Lifecycle callback delivery failed: operation_id=%s project_slug=%s type=%s",
                operation.operation_id,
                operation.project_slug,
                operation.type.value,
            )

        logger.info(
            "Lifecycle operation finished: operation_id=%s project_slug=%s type=%s status=%s",
            operation.operation_id,
            operation.project_slug,
            operation.type.value,
            (
                operation.status.value
                if operation.status
                else LifecycleOperationStatus.SUCCEEDED.value
            ),
        )
