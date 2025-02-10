import logging
import os

import requests

from auth import generate_token_for_euphrosyne_backend

logger = logging.getLogger(__name__)


def post_data_access_event(path: str, data_request: str):
    """
    Posts a data access event to the Euphrosyne backend.
    """
    try:
        euphroyne_backend_url = os.environ["EUPHROSYNE_BACKEND_URL"]
    except KeyError:
        logger.error("EUPHROSYNE_BACKEND_URL environment variable is not set")
        return None
    token = generate_token_for_euphrosyne_backend()
    headers = {"Authorization": f"Bearer {token}"}
    data = {"path": path, "data_request": data_request}
    response = requests.post(
        f"{euphroyne_backend_url}/api/data-request/access-event",
        headers=headers,
        json=data,
    )
    if not response.ok:
        logger.error(
            "Failed to post data access event to Euphrosyne backend for path %s of request #%s (%s): %s",
            path,
            data_request,
            response.status_code,
            response.text,
        )
    return None
