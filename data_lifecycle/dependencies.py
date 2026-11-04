import logging
import os
import time

import requests
from fastapi import HTTPException

from auth import generate_token_for_euphrosyne_backend

from .models import LifecycleState

logger = logging.getLogger(__name__)

FETCH_PROJECT_LIFECYCLE_RETRIES = 2
RESPONSE_BODY_LOG_EXCERPT_LENGTH = 200


def _response_body_excerpt(response: requests.Response) -> str:
    """Return a truncated excerpt of the response body, for logging purposes."""
    body = response.text
    if len(body) > RESPONSE_BODY_LOG_EXCERPT_LENGTH:
        return body[:RESPONSE_BODY_LOG_EXCERPT_LENGTH] + "...[truncated]"
    return body


def _parse_lifecycle_response(response: requests.Response) -> LifecycleState:
    try:
        content = response.json()
    except ValueError:
        # requests.exceptions.JSONDecodeError is a ValueError subclass. This
        # typically happens when a proxy / WAF answers with an HTML page. It is
        # not expected, so let it propagate as a 500 to be reported in Sentry;
        # the warning below is attached to the event as a breadcrumb.
        logger.warning(
            "failed to fetch project data lifecycle.\n"
            "Reason: storage backend returned a non-JSON response "
            "(status=%s, content-type=%s).\nBody excerpt: %r",
            response.status_code,
            response.headers.get("Content-Type"),
            _response_body_excerpt(response),
        )
        raise

    try:
        raw_state = content["lifecycle_state"]
    except (KeyError, TypeError) as exc:
        logger.error(
            "failed to fetch project data lifecycle.\n"
            "Reason: storage backend returned an invalid response.\nBody excerpt: %r",
            _response_body_excerpt(response),
        )
        raise HTTPException(
            status_code=502,
            detail="Storage backend returned an invalid response (fetch_project_lifecycle)",
        ) from exc

    try:
        return LifecycleState(raw_state)
    except ValueError as exc:
        logger.error(
            "failed to fetch project data lifecycle.\n"
            "Reason: unknown lifecycle state returned by storage backend: %r",
            raw_state,
        )
        raise HTTPException(
            status_code=502,
            detail="Storage backend returned an invalid lifecycle state (fetch_project_lifecycle)",
        ) from exc


# TODO: implement CACHE
def fetch_project_lifecycle(project_slug: str) -> LifecycleState:
    """Given project_slug, fetch project data current lifecycle"""
    euphroyne_backend_url = os.environ["EUPHROSYNE_BACKEND_URL"]
    token = generate_token_for_euphrosyne_backend()
    headers = {"Authorization": f"Bearer {token}"}

    retry = 0

    while retry < FETCH_PROJECT_LIFECYCLE_RETRIES:
        try:
            response = requests.get(
                euphroyne_backend_url
                + f"/api/data-management/projects/{project_slug}/lifecycle",
                headers=headers,
                timeout=3,
            )
        except requests.RequestException as e:
            logger.warning(
                "failed to fetch project data lifecycle. Retrying...\nReason:\n%s",
                str(e),
            )
            retry += 1
            if retry < FETCH_PROJECT_LIFECYCLE_RETRIES:
                time.sleep(1)
            continue
        if response.status_code == 404:
            raise HTTPException(
                status_code=404, detail="Project not found (fetch_project_lifecycle)"
            )

        if response.ok:
            return _parse_lifecycle_response(response)

        retry += 1
        if retry < FETCH_PROJECT_LIFECYCLE_RETRIES:
            time.sleep(1)

    logger.error(
        "failed to fetch project data lifecycle.",
    )
    raise HTTPException(
        status_code=503,
        detail="Storage backend is unavailable (fetch_project_lifecycle)",
    )
