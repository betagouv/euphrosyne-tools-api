from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch
import pytest
from fastapi import HTTPException, status
from jose import jwt
import auth
from auth import (
    ALGORITHM,
    EUPHROSYNE_TOKEN_USER_ID_VALUE,
    Project,
    User,
    _decode_jwt,
    generate_token_for_path,
    get_current_user,
    verify_admin_permission,
    verify_has_azure_permission,
    verify_is_euphrosyne_backend,
    verify_project_membership,
    verify_path_permission,
    _generate_jwt_token,
    ACCESS_TOKEN_EXPIRE_MINUTES,
)
from exceptions import NoProjectMembershipException


def _generate_token(data: dict):
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, "secret", algorithm=ALGORITHM)
    return encoded_jwt


@pytest.mark.anyio
async def test_get_current_user_raises_if_no_user_id():
    token = _generate_token({"user_id": None})
    with pytest.raises(HTTPException) as exception:
        await get_current_user(token)
        assert exception.status_code == status.HTTP_401_UNAUTHORIZED


@pytest.mark.anyio
async def test_returns_user_with_projects():
    token = _generate_token(
        {
            "user_id": 1,
            "is_admin": False,
            "projects": [{"id": 444, "name": "Projet AD", "slug": "projet-ad"}],
        }
    )
    user = await get_current_user(token)
    assert isinstance(user, User)
    assert user.id == 1
    assert user.projects
    assert user.projects[0].id == 444


@pytest.mark.anyio
async def test_login_with_api_key_when_env_set(monkeypatch):
    monkeypatch.setenv("API_TOKEN", "token")
    user = await get_current_user(None, "token")

    assert isinstance(user, User)
    assert user.is_admin
    assert user.id == 0


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
    jwt_token = _generate_token(
        {
            "user_id": 1,
            "is_admin": False,
            "projects": [],
        }
    )
    user = await get_current_user(jwt_token, "api_token")
    assert isinstance(user, User)
    assert user.id == 1
    assert not user.is_admin


def test_verify_project_membership_passes_for_ownership():
    # pylint: disable=expression-not-assigned
    verify_project_membership(
        "hello-world",
        User(
            id=1,
            is_admin=False,
            projects=[
                Project(id=1, name="hello-world", slug="hello-world"),
                Project(id=2, name="bye-world", slug="bye-world"),
            ],
        ),
    ) is None


def test_verify_project_membership_passes_for_admin():
    # pylint: disable=expression-not-assigned
    verify_project_membership(
        "hello-world",
        User(
            id=1,
            is_admin=True,
            projects=[],
        ),
    ) is None


def test_verify_project_membership_fails_for_regular_user():
    with pytest.raises(NoProjectMembershipException):
        verify_project_membership(
            "hello-world",
            User(
                id=1,
                is_admin=False,
                projects=[],
            ),
        )


def test_verify_admin_permission():
    with pytest.raises(HTTPException):
        verify_admin_permission(
            User(
                id=1,
                is_admin=False,
                projects=[],
            ),
        )


@pytest.mark.parametrize(
    "decoded_user_id", [EUPHROSYNE_TOKEN_USER_ID_VALUE, "wrongtoken"]
)
@patch("auth.jwt.decode")
def test_verify_is_euphrosyne_backend(decode_mock: MagicMock, decoded_user_id: str):
    has_error = False
    decode_mock.return_value = {"user_id": decoded_user_id}
    try:
        verify_is_euphrosyne_backend("atoken")
    except HTTPException:
        has_error = True
    assert has_error is not (EUPHROSYNE_TOKEN_USER_ID_VALUE == decoded_user_id)


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


def test_generate_token_for_path(monkeypatch: pytest.MonkeyPatch):
    path = "/example"
    monkeypatch.setenv("JWT_SECRET_KEY", "secret")
    token = generate_token_for_path(path)
    assert isinstance(token, str)
    decoded_token = jwt.decode(token, "secret", algorithms=[ALGORITHM])
    assert decoded_token["path"] == path


def test_generate_token_for_path_with_expiration(monkeypatch: pytest.MonkeyPatch):
    path = "/example"
    exp = datetime.now(timezone.utc)
    monkeypatch.setenv("JWT_SECRET_KEY", "secret")
    token = generate_token_for_path(path, expiration=exp)
    decoded_token = jwt.decode(token, "secret", algorithms=[ALGORITHM])

    assert decoded_token["exp"] == int(exp.timestamp())


def test_generate_jwt_token(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("JWT_SECRET_KEY", "secret")
    with patch.object(auth, "datetime") as datetime_mock:
        utcnow = datetime.now(timezone.utc)
        datetime_mock.now.return_value = utcnow
        token = _generate_jwt_token({"test": "test"})
    assert isinstance(token, str)
    decoded_token = jwt.decode(token, "secret", algorithms=[ALGORITHM])
    decoded_token["test"] == "test"
    decoded_token["exp"] == (
        utcnow + timedelta(ACCESS_TOKEN_EXPIRE_MINUTES)
    ).timestamp()


def test_decode_jwt_without_exp_raises(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("JWT_SECRET_KEY", "secret")
    token = jwt.encode(
        {},
        "secret",
        algorithm=ALGORITHM,
    )
    with pytest.raises(HTTPException):
        _decode_jwt(token)
