import pytest

from auth import Project, User


@pytest.mark.parametrize(
    "user,expected",
    [
        (User(id=1, projects=[Project(id=1, name="Project Nono")]), False),
        (User(id=1, projects=[Project(id=1, name="Project Yesyes")]), True),
    ],
)
def test_has_project(user, expected):
    assert user.has_project("Project Yesyes") is expected
