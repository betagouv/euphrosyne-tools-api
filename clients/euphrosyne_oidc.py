"""Access token of the Tools API to call Euphrosyne, obtained from Euphrosyne's
OpenID Connect provider with the client credentials grant."""

import os
import threading
import time

import requests

TOKEN_PATH = "/identity/o/api/token"
SCOPE = "euphrosyne:backend"
# Fetch a new token this long before the cached one expires.
EXPIRY_MARGIN_SECONDS = 30


class EuphrosyneTokenError(Exception):
    """Euphrosyne did not issue a token."""


_lock = threading.Lock()
_cached_token: tuple[str, float] | None = None


def get_euphrosyne_backend_token() -> str:
    """Return a valid access token, fetching a new one when needed."""
    global _cached_token  # pylint: disable=global-statement
    with _lock:
        if _cached_token and _cached_token[1] > time.time() + EXPIRY_MARGIN_SECONDS:
            return _cached_token[0]
        token, expires_in = _fetch_token()
        _cached_token = (token, time.time() + expires_in)
        return token


def clear_cached_token() -> None:
    global _cached_token  # pylint: disable=global-statement
    with _lock:
        _cached_token = None


def _fetch_token() -> tuple[str, int]:
    url = os.environ["EUPHROSYNE_BACKEND_URL"].rstrip("/") + TOKEN_PATH
    try:
        response = requests.post(
            url,
            data={"grant_type": "client_credentials", "scope": SCOPE},
            auth=(
                os.environ["EUPHROSYNE_OIDC_CLIENT_ID"],
                os.environ["EUPHROSYNE_OIDC_CLIENT_SECRET"],
            ),
            timeout=5,
        )
    except requests.RequestException as error:
        raise EuphrosyneTokenError(f"Token request failed: {error}") from error
    if not response.ok:
        raise EuphrosyneTokenError(
            f"Token request refused ({response.status_code}): {response.text}"
        )
    body = response.json()
    return body["access_token"], int(body.get("expires_in", 0))
