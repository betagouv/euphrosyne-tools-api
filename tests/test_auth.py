from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import HTTPException, status

import auth
from auth import (
    ACCESS_TOKEN_EXPIRE_MINUTES,
    ALGORITHM,
    JWT_CREDENTIALS_EXCEPTION,
    Project,
    User,
    _decode_euphrosyne_jwt,
    _generate_jwt_token,
    generate_token_for_path,
    get_current_user,
    verify_admin_permission,
    verify_eros_query_url_jwt,
    verify_has_azure_permission,
    verify_is_euphrosyne_backend,
    verify_is_euphrosyne_backend_or_admin,
    verify_path_permission,
    verify_project_membership,
)
from exceptions import NoProjectMembershipException

from .conftest import EUPHROSYNE_KID, EUPHROSYNE_PRIVATE_KEY, EUPHROSYNE_URL


def _generate_token(data: dict, key=EUPHROSYNE_PRIVATE_KEY, kid=EUPHROSYNE_KID):
    """A token as Euphrosyne issues it: RS256 with its key, published in its
    JWKS under ``kid``."""
    to_encode = {
        "iss": EUPHROSYNE_URL,
        "exp": datetime.now(UTC) + timedelta(minutes=15),
        "token_use": "access",
        **data,
    }
    return jwt.encode(to_encode, key, algorithm="RS256", headers={"kid": kid})


def _user_token(**claims):
    return _generate_token({"sub": "1", "scope": "tools", "projects": [], **claims})


def _backend_token():
    return _generate_token({"scope": "tools:backend", "is_admin": True})


@pytest.mark.anyio
@pytest.mark.parametrize(
    "claims",
    [
        pytest.param({"scope": "tools"}, id="no-sub"),
        # Tokens Euphrosyne issued to another application.
        pytest.param({"sub": "1", "scope": "openid"}, id="no-tools-scope"),
    ],
)
async def test_get_current_user_rejects_tokens_not_issued_to_a_tools_user(claims):
    with pytest.raises(HTTPException) as exception:
        await get_current_user(_generate_token(claims))
    assert exception.value.status_code == status.HTTP_401_UNAUTHORIZED


@pytest.mark.anyio
async def test_returns_user_with_projects():
    token = _generate_token(
        {
            "sub": "1",
            "scope": "openid tools",
            "is_admin": False,
            "projects": [{"id": 444, "name": "Projet AD", "slug": "projet-ad"}],
        }
    )
    user = await get_current_user(token)
    assert isinstance(user, User)
    assert user.id == "1"
    assert user.projects
    assert user.projects[0].id == 444


@pytest.mark.anyio
async def test_login_with_api_key_when_env_set(monkeypatch):
    monkeypatch.setenv("API_TOKEN", "token")
    user = await get_current_user(None, "token")

    assert isinstance(user, User)
    assert user.is_admin
    assert user.id == "0"


@pytest.mark.anyio
async def test_cannot_login_with_api_key_when_env_not_set(monkeypatch):
    monkeypatch.setenv("API_TOKEN", None)
    with pytest.raises(HTTPException):
        await get_current_user(None, "token")


@pytest.mark.anyio
async def test_cannot_login_with_api_key_when_wrong_token(monkeypatch):
    monkeypatch.setenv("API_TOKEN", "token")
    with pytest.raises(HTTPException):
        await get_current_user(None, "wrongtoken")


@pytest.mark.anyio
async def test_jwt_takes_precedence_over_api_key(monkeypatch):
    monkeypatch.setenv("API_TOKEN", "api_token")
    jwt_token = _user_token(is_admin=False)
    user = await get_current_user(jwt_token, "api_token")
    assert isinstance(user, User)
    assert user.id == "1"
    assert not user.is_admin


def test_verify_project_membership_passes_for_ownership():
    # pylint: disable=expression-not-assigned
    assert (
        verify_project_membership(
            "hello-world",
            User(
                id="1",
                is_admin=False,
                projects=[
                    Project(id=1, name="hello-world", slug="hello-world"),
                    Project(id=2, name="bye-world", slug="bye-world"),
                ],
            ),
        )
        is None
    )


def test_verify_project_membership_passes_for_admin():
    # pylint: disable=expression-not-assigned
    assert (
        verify_project_membership(
            "hello-world",
            User(
                id="1",
                is_admin=True,
                projects=[],
            ),
        )
        is None
    )


def test_verify_project_membership_fails_for_regular_user():
    with pytest.raises(NoProjectMembershipException):
        verify_project_membership(
            "hello-world",
            User(
                id="1",
                is_admin=False,
                projects=[],
            ),
        )


def test_verify_admin_permission():
    with pytest.raises(HTTPException):
        verify_admin_permission(
            User(
                id="1",
                is_admin=False,
                projects=[],
            ),
        )


def test_verify_is_euphrosyne_backend_accepts_the_backend_token():
    verify_is_euphrosyne_backend(_backend_token())


@pytest.mark.parametrize(
    "claims",
    [
        pytest.param({"sub": "1", "scope": "tools", "is_admin": True}, id="admin"),
        pytest.param(
            {"sub": "1", "scope": "tools:backend"}, id="backend-scope-for-a-user"
        ),
        pytest.param({"scope": "tools:eros"}, id="eros"),
    ],
)
def test_verify_is_euphrosyne_backend_rejects_other_tokens(claims):
    with pytest.raises(HTTPException) as exc:
        verify_is_euphrosyne_backend(_generate_token(claims))
    assert exc.value.status_code == 403


@pytest.mark.anyio
async def test_backend_token_acts_as_an_administrator():
    user = await get_current_user(_backend_token())

    assert user.id == "euphrosyne"
    assert user.is_admin


@pytest.mark.parametrize("token", ["", None, "right", "wrong"])
def test_verify_has_azure_permission(
    token: str | None, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("AZURE_RESOURCE_PREFIX", "prefix")
    with patch("auth.VaultClient") as vault_client_mock:
        vault_client_mock.return_value.get_secret_value.return_value.value = "right"
        if token != "right":
            with pytest.raises(HTTPException):
                verify_has_azure_permission()
        else:
            verify_has_azure_permission(token)
        vault_client_mock.assert_called_with("prefix-key-vault")
        vault_client_mock.return_value.get_secret_value.assert_called_with(
            "secret-api-key"
        )


def test_verify_path_permission_with_valid_token():
    valid_token = generate_token_for_path("/valid_path")
    verify_path_permission("/valid_path", valid_token)


def test_verify_path_permission_with_invalid_token():
    valid_token = generate_token_for_path("/valid_path")
    with pytest.raises(HTTPException):
        verify_path_permission("/valid_path", valid_token + "invalid")


def test_verify_path_permission_with_wrong_path():
    valid_token = generate_token_for_path("/valid_path")
    with pytest.raises(HTTPException):
        verify_path_permission("/wrong_path", valid_token)


def test_generate_token_for_path():
    path = "/example"
    token = generate_token_for_path(path)
    assert isinstance(token, str)
    decoded_token = jwt.decode(token, "secret", algorithms=[ALGORITHM])
    assert decoded_token["path"] == path


def test_generate_token_for_path_with_expiration():
    path = "/example"
    exp = datetime.now(UTC) + timedelta(minutes=5)
    token = generate_token_for_path(path, expiration=exp)
    decoded_token = jwt.decode(token, "secret", algorithms=[ALGORITHM])

    assert decoded_token["exp"] == int(exp.timestamp())


def test_generate_jwt_token():
    with patch.object(auth, "datetime") as datetime_mock:
        utcnow = datetime.now(UTC)
        datetime_mock.now.return_value = utcnow
        token = _generate_jwt_token({"test": "test"}, secret_key="secret")
    assert isinstance(token, str)
    decoded_token = jwt.decode(token, "secret", algorithms=[ALGORITHM])
    assert decoded_token["test"] == "test"
    assert decoded_token["exp"] == int(
        (utcnow + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)).timestamp()
    )


def test_decode_jwt_accepts_euphrosyne_access_tokens():
    assert _decode_euphrosyne_jwt(_user_token())["sub"] == "1"


VALID_CLAIMS = {"iss": EUPHROSYNE_URL, "exp": 4102444800, "token_use": "access"}


def _without(claim: str) -> dict:
    return {key: value for key, value in VALID_CLAIMS.items() if key != claim}


@pytest.mark.parametrize(
    ("algorithm", "key", "kid", "claims"),
    [
        ("none", "", EUPHROSYNE_KID, VALID_CLAIMS),
        # A secret the Tools API holds cannot be used to forge Euphrosyne tokens.
        ("HS256", "secret", EUPHROSYNE_KID, VALID_CLAIMS),
        (
            "RS256",
            rsa.generate_private_key(public_exponent=65537, key_size=2048),
            EUPHROSYNE_KID,
            VALID_CLAIMS,
        ),
        ("RS256", EUPHROSYNE_PRIVATE_KEY, "unknown-key", VALID_CLAIMS),
        ("RS256", EUPHROSYNE_PRIVATE_KEY, EUPHROSYNE_KID, _without("exp")),
        ("RS256", EUPHROSYNE_PRIVATE_KEY, EUPHROSYNE_KID, _without("iss")),
        ("RS256", EUPHROSYNE_PRIVATE_KEY, EUPHROSYNE_KID, _without("token_use")),
        (
            "RS256",
            EUPHROSYNE_PRIVATE_KEY,
            EUPHROSYNE_KID,
            {**VALID_CLAIMS, "iss": "http://other.test"},
        ),
        ("RS256", EUPHROSYNE_PRIVATE_KEY, EUPHROSYNE_KID, {**VALID_CLAIMS, "exp": 1}),
        (
            "RS256",
            EUPHROSYNE_PRIVATE_KEY,
            EUPHROSYNE_KID,
            {**VALID_CLAIMS, "nbf": 4102444800},
        ),
        (
            "RS256",
            EUPHROSYNE_PRIVATE_KEY,
            EUPHROSYNE_KID,
            {**VALID_CLAIMS, "token_use": "id"},
        ),
    ],
)
def test_decode_jwt_rejects_invalid_tokens(algorithm, key, kid, claims):
    token = jwt.encode(claims, key, algorithm=algorithm, headers={"kid": kid})

    with pytest.raises(HTTPException) as exc:
        _decode_euphrosyne_jwt(token)

    assert exc.value.status_code == status.HTTP_401_UNAUTHORIZED
    assert exc.value.headers == {"WWW-Authenticate": "Bearer"}


def test_decode_jwt_rejects_malformed_token():
    with pytest.raises(HTTPException) as exc:
        _decode_euphrosyne_jwt("not-a-jwt")

    assert exc.value.status_code == status.HTTP_401_UNAUTHORIZED


def test_extra_payload_token_getter():
    token = jwt.encode(
        {
            "test": "value",
            "exp": datetime.now(UTC) + timedelta(minutes=15),
        },
        "secret",
        algorithm=ALGORITHM,
    )
    getter = auth.ExtraPayloadTokenGetter("test")

    assert getter(token) == "value"


def test_verify_is_euphrosyne_backend_or_admin_no_token():
    with pytest.raises(HTTPException) as exc:
        verify_is_euphrosyne_backend_or_admin(jwt_token=None)
    assert exc.value.status_code == JWT_CREDENTIALS_EXCEPTION.status_code


def test_verify_is_euphrosyne_backend_or_admin_backend():
    verify_is_euphrosyne_backend_or_admin(jwt_token=_backend_token())


def test_verify_is_euphrosyne_backend_or_admin_admin():
    verify_is_euphrosyne_backend_or_admin(jwt_token=_user_token(is_admin=True))


@pytest.mark.parametrize(
    "claims",
    [
        pytest.param({"sub": "1", "scope": "tools", "is_admin": False}, id="user"),
        pytest.param({"sub": "1", "scope": "openid", "is_admin": True}, id="no-tools"),
    ],
)
def test_verify_is_euphrosyne_backend_or_admin_rejects_others(claims):
    with pytest.raises(HTTPException) as exc:
        verify_is_euphrosyne_backend_or_admin(jwt_token=_generate_token(claims))
    assert exc.value.status_code == 403
    assert exc.value.detail == "Not allowed"


@pytest.mark.anyio
async def test_path_token_is_not_a_user_token():
    with pytest.raises(HTTPException):
        await get_current_user(generate_token_for_path("/a/path"))


def test_path_token_signed_with_unknown_secret_is_rejected():
    token = _generate_jwt_token({"path": "/a/path"}, secret_key="unknown")

    with pytest.raises(HTTPException):
        verify_path_permission("/a/path", token)


def test_eros_accepts_euphrosyne_image_token():
    verify_eros_query_url_jwt(_generate_token({"scope": "tools:eros"}))


def test_eros_accepts_user_token():
    verify_eros_query_url_jwt(_user_token())


@pytest.mark.parametrize(
    "token",
    [
        pytest.param(lambda: _generate_token({"scope": "other"}), id="other-scope"),
        pytest.param(lambda: generate_token_for_path("/a/path"), id="path-token"),
        pytest.param(_backend_token, id="backend-token"),
        pytest.param(
            lambda: _generate_token({"sub": "1", "scope": "openid"}),
            id="user-token-without-tools-scope",
        ),
    ],
)
def test_eros_rejects_other_tokens(token):
    with pytest.raises(HTTPException):
        verify_eros_query_url_jwt(token())
