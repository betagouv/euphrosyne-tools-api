from datetime import datetime, timedelta

import pytest
from fastapi import HTTPException, status
from jose import jwt

from auth import (
    ALGORITHM,
    Project,
    User,
    get_current_user,
    verify_admin_permission,
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
async def test_get_current_user_raises_if_no_user_id(monkeypatch):
    monkeypatch.setenv("JWT_SECRET_KEY", "secret")
    token = _generate_token({"user_id": None})
    with pytest.raises(HTTPException) as exception:
        await get_current_user(token)
        assert exception.status_code == status.HTTP_401_UNAUTHORIZED


@pytest.mark.anyio
async def test_returns_user_with_projects(monkeypatch):
    monkeypatch.setenv("JWT_SECRET_KEY", "secret")
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
