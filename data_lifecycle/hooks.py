import logging
import os
import time
from dataclasses import asdict
from typing import Any, Callable

import requests

from auth import generate_token_for_euphrosyne_backend
from data_lifecycle.models import FromDataDeletionCallback, LifecycleOperation

logger = logging.getLogger(__name__)

LIFECYCLE_CALLBACK_PATH = "/api/data-management/operations/callback"
LIFECYCLE_CALLBACK_MAX_ATTEMPTS = 5
LIFECYCLE_CALLBACK_INITIAL_BACKOFF_SECONDS = 1.0
LIFECYCLE_CALLBACK_TIMEOUT_SECONDS = 10.0


def post_lifecycle_operation_callback(
    operation: LifecycleOperation,
    *,
    max_attempts: int = LIFECYCLE_CALLBACK_MAX_ATTEMPTS,
    initial_backoff_seconds: float = LIFECYCLE_CALLBACK_INITIAL_BACKOFF_SECONDS,
    timeout_seconds: float = LIFECYCLE_CALLBACK_TIMEOUT_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
) -> bool:
    """
    Posts lifecycle operation result to the Euphrosyne backend with retries.
    """
    payload = {
        **asdict(operation),
        "operation_id": str(operation.operation_id),
        "finished_at": (
            operation.finished_at.isoformat() if operation.finished_at else None
        ),
    }
    return _post_callback(
        payload=payload,
        operation_id=str(operation.operation_id),
        log_context={
            "project_slug": operation.project_slug,
            "type": operation.type.value,
        },
        max_attempts=max_attempts,
        initial_backoff_seconds=initial_backoff_seconds,
        timeout_seconds=timeout_seconds,
        sleep=sleep,
    )


def post_from_data_deletion_callback(
    callback: FromDataDeletionCallback,
    *,
    max_attempts: int = LIFECYCLE_CALLBACK_MAX_ATTEMPTS,
    initial_backoff_seconds: float = LIFECYCLE_CALLBACK_INITIAL_BACKOFF_SECONDS,
    timeout_seconds: float = LIFECYCLE_CALLBACK_TIMEOUT_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
) -> bool:
    """Post source data deletion status to the Euphrosyne backend with retries."""
    payload = {
        **asdict(callback),
        "operation_id": str(callback.operation_id),
    }
    return _post_callback(
        payload=payload,
        operation_id=str(callback.operation_id),
        log_context={
            "phase": callback.phase.value,
        },
        max_attempts=max_attempts,
        initial_backoff_seconds=initial_backoff_seconds,
        timeout_seconds=timeout_seconds,
        sleep=sleep,
    )


def _post_callback(
    *,
    payload: dict[str, Any],
    operation_id: str,
    log_context: dict[str, str],
    max_attempts: int,
    initial_backoff_seconds: float,
    timeout_seconds: float,
    sleep: Callable[[float], None],
) -> bool:
    try:
        euphrosyne_backend_url = os.environ["EUPHROSYNE_BACKEND_URL"]
    except KeyError:
        logger.error("EUPHROSYNE_BACKEND_URL environment variable is not set")
        return False

    token = generate_token_for_euphrosyne_backend()
    headers = {"Authorization": f"Bearer {token}"}
    callback_url = f"{euphrosyne_backend_url}{LIFECYCLE_CALLBACK_PATH}"
    context = " ".join(f"{key}={value}" for key, value in log_context.items())

    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.post(
                callback_url,
                headers=headers,
                json=payload,
                timeout=timeout_seconds,
            )
        except requests.RequestException as exc:
            logger.warning(
                "Lifecycle callback attempt %s failed: operation_id=%s %s error=%s",
                attempt,
                operation_id,
                context,
                exc,
            )
            if attempt < max_attempts:
                sleep(initial_backoff_seconds * (2 ** (attempt - 1)))
                continue
            logger.error(
                "Lifecycle callback failed after %s attempts: operation_id=%s %s",
                attempt,
                operation_id,
                context,
            )
            return False

        if response.status_code >= 500:
            logger.warning(
                "Lifecycle callback attempt %s received %s: operation_id=%s %s response=%s",
                attempt,
                response.status_code,
                operation_id,
                context,
                response.text,
            )
            if attempt < max_attempts:
                sleep(initial_backoff_seconds * (2 ** (attempt - 1)))
                continue
            logger.error(
                "Lifecycle callback failed after %s attempts: operation_id=%s %s",
                attempt,
                operation_id,
                context,
            )
            return False

        if response.status_code >= 400:
            logger.error(
                "Lifecycle callback rejected with %s: operation_id=%s %s response=%s",
                response.status_code,
                operation_id,
                context,
                response.text,
            )
            return False

        logger.info(
            "Lifecycle callback delivered: operation_id=%s %s status=%s",
            operation_id,
            context,
            response.status_code,
        )
        return True

    # Ensure a boolean is returned on all code paths (e.g., if max_attempts == 0)
    return False
