import logging
import threading
from datetime import datetime, timezone

from fastapi import BackgroundTasks

from data_lifecycle.hooks import post_lifecycle_operation_callback
from data_lifecycle.models import LifecycleOperation, LifecycleOperationStatus

logger = logging.getLogger(__name__)


# In-process dedupe for (project_slug, type, operation_id).
# Protected by the lock to avoid races between concurrent requests.
# Per-process only: no persistence, no cross-worker coordination.
_LIFECYCLE_OPERATION_GUARD: set[tuple[str, str, str]] = set()
_LIFECYCLE_OPERATION_GUARD_LOCK = threading.Lock()


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


def _unregister_lifecycle_operation(operation: LifecycleOperation) -> None:
    """Remove an operation from the in-memory guard after completion."""
    key = operation.guard_key()
    with _LIFECYCLE_OPERATION_GUARD_LOCK:
        _LIFECYCLE_OPERATION_GUARD.discard(key)


def _perform_lifecycle_operation(
    *,
    operation: LifecycleOperation,
) -> tuple[int | None, int | None]:
    """Execute the lifecycle operation (placeholder for future data movement)."""
    # TODO: Return real bytes/files counts once data movement is implemented.
    _ = operation
    return None, None


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
        operation.error_details = {"type": exc.__class__.__name__}
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
        _unregister_lifecycle_operation(operation)
