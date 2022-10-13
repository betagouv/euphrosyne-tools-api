from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException, status
from jose import jwt

from auth import (
    ALGORITHM,
    EUPHROSYNE_TOKEN_USER_ID_VALUE,
    Project,
    User,
    get_current_user,
    verify_admin_permission,
    verify_is_euphrosyne_backend,
    verify_project_membership,
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
            "projects": [{"id": 444, "name": "Projet AD"}],
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
                Project(id=1, name="hello-world"),
                Project(id=2, name="bye-world"),
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
