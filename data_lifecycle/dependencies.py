import logging
import os
import time

import requests
from fastapi import HTTPException

from auth import generate_token_for_euphrosyne_backend

from .storage_types import StorageRole

logger = logging.getLogger(__name__)

FETCH_PROJECT_LIFECYCLE_RETRIES = 2


# TODO: implement CACHE
def fetch_project_lifecycle(project_slug: str) -> StorageRole:
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
            time.sleep(1)
            continue
        if response.status_code == 404:
            raise HTTPException(
                status_code=404, detail="Project not found (fetch_project_lifecycle)"
            )

        if response.ok:
            try:
                content = response.json()
                return content["lifecycle_state"]
            except (ValueError, KeyError) as exc:
                logger.error(
                    "failed to fetch project data lifecycle.\nReason: storage backend returned an invalid response.\n%s",
                    response.content,
                )
                raise HTTPException(
                    status_code=502,
                    detail="Storage backend returned an invalid response (fetch_project_lifecycle)",
                ) from exc

        retry += 1
        time.sleep(1)

    logger.error(
        "failed to fetch project data lifecycle.",
    )
    raise HTTPException(
        status_code=503,
        detail="Storage backend is unavailable (fetch_project_lifecycle)",
    )
