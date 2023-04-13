import pytest

from auth import Project, User


@pytest.mark.parametrize(
    "user,expected",
    [
        (
            User(
                id=1,
                projects=[Project(id=1, name="Project Nono", slug="project-nono")],
                is_admin=False,
            ),
            False,
        ),
        (
            User(
                id=1,
                projects=[Project(id=1, name="Project Yesyes", slug="project-yesyes")],
                is_admin=False,
            ),
            True,
        ),
    ],
)
def test_has_project(user, expected):
    assert user.has_project("project-yesyes") is expected
