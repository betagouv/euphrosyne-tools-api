import logging
import os
import time
from dataclasses import asdict
from typing import Callable

import requests

from auth import generate_token_for_euphrosyne_backend
from data_lifecycle.models import LifecycleOperation

logger = logging.getLogger(__name__)

LIFECYCLE_CALLBACK_PATH = "/api/data-lifecycle/operations/callback"
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
    try:
        euphroyne_backend_url = os.environ["EUPHROSYNE_BACKEND_URL"]
    except KeyError:
        logger.error("EUPHROSYNE_BACKEND_URL environment variable is not set")
        return False

    token = generate_token_for_euphrosyne_backend()
    headers = {"Authorization": f"Bearer {token}"}
    callback_url = f"{euphroyne_backend_url}{LIFECYCLE_CALLBACK_PATH}"

    operation_id = operation.operation_id
    project_slug = operation.project_slug
    operation_type = operation.type.value
    payload = {**asdict(operation), "operation_id": str(operation.operation_id)}

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
                "Lifecycle callback attempt %s failed: operation_id=%s project_slug=%s type=%s error=%s",
                attempt,
                operation_id,
                project_slug,
                operation_type,
                exc,
            )
            if attempt < max_attempts:
                sleep(initial_backoff_seconds * (2 ** (attempt - 1)))
                continue
            logger.error(
                "Lifecycle callback failed after %s attempts: operation_id=%s project_slug=%s type=%s",
                attempt,
                operation_id,
                project_slug,
                operation_type,
            )
            return False

        if response.status_code >= 500:
            logger.warning(
                "Lifecycle callback attempt %s received %s: operation_id=%s project_slug=%s type=%s response=%s",
                attempt,
                response.status_code,
                operation_id,
                project_slug,
                operation_type,
                response.text,
            )
            if attempt < max_attempts:
                sleep(initial_backoff_seconds * (2 ** (attempt - 1)))
                continue
            logger.error(
                "Lifecycle callback failed after %s attempts: operation_id=%s project_slug=%s type=%s",
                attempt,
                operation_id,
                project_slug,
                operation_type,
            )
            return False

        if response.status_code >= 400:
            logger.error(
                "Lifecycle callback rejected with %s: operation_id=%s project_slug=%s type=%s response=%s",
                response.status_code,
                operation_id,
                project_slug,
                operation_type,
                response.text,
            )
            return False

        logger.info(
            "Lifecycle callback delivered: operation_id=%s project_slug=%s type=%s status=%s",
            operation_id,
            project_slug,
            operation_type,
            response.status_code,
        )
        return True

    logger.error(
        "Lifecycle callback failed after %s attempts: operation_id=%s project_slug=%s type=%s",
        max_attempts,
        operation_id,
        project_slug,
        operation_type,
    )
    return False
