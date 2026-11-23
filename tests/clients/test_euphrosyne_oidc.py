from unittest.mock import MagicMock, patch

import pytest
import requests

from clients import euphrosyne_oidc
from clients.euphrosyne_oidc import EuphrosyneTokenError, get_euphrosyne_backend_token


@pytest.fixture(autouse=True)
def no_cached_token():
    euphrosyne_oidc.clear_cached_token()
    yield
    euphrosyne_oidc.clear_cached_token()


def _token_response(token: str, expires_in: int = 300) -> MagicMock:
    response = MagicMock(ok=True)
    response.json.return_value = {
        "access_token": token,
        "token_type": "Bearer",
        "expires_in": expires_in,
    }
    return response


@patch("clients.euphrosyne_oidc.requests.post")
def test_gets_a_token_with_client_credentials(post: MagicMock):
    post.return_value = _token_response("token")

    assert get_euphrosyne_backend_token() == "token"

    post.assert_called_once_with(
        "http://euphrosyne.test/identity/o/api/token",
        data={"grant_type": "client_credentials", "scope": "euphrosyne:backend"},
        auth=("euphrosyne-tools-api", "client-secret"),
        timeout=5,
    )


@patch("clients.euphrosyne_oidc.requests.post")
def test_token_is_reused_until_it_nearly_expires(post: MagicMock):
    post.side_effect = [_token_response("first"), _token_response("second")]

    with patch("clients.euphrosyne_oidc.time.time", return_value=1000):
        assert get_euphrosyne_backend_token() == "first"
    with patch("clients.euphrosyne_oidc.time.time", return_value=1260):
        assert get_euphrosyne_backend_token() == "first"
    with patch("clients.euphrosyne_oidc.time.time", return_value=1271):
        assert get_euphrosyne_backend_token() == "second"


@patch("clients.euphrosyne_oidc.requests.post")
def test_refused_token_request_raises(post: MagicMock):
    post.return_value = MagicMock(ok=False, status_code=401, text="invalid_client")

    with pytest.raises(EuphrosyneTokenError):
        get_euphrosyne_backend_token()


@patch(
    "clients.euphrosyne_oidc.requests.post",
    side_effect=requests.ConnectionError("down"),
)
def test_unreachable_euphrosyne_raises(_post: MagicMock):
    with pytest.raises(EuphrosyneTokenError):
        get_euphrosyne_backend_token()
