import pytest
from clients.version import Version, InvalidVersion


def test_version_to_tuple():
    version = Version("1.2.4")
    assert version.major == 1
    assert version.minor == 2
    assert version.patch == 4

    version = Version("0.0.1")
    assert version.major == 0
    assert version.minor == 0
    assert version.patch == 1

    version = Version("1.5000000.90")
    assert version.major == 1
    assert version.minor == 5000000
    assert version.patch == 90

    version = Version("1234567.56789.34567")
    assert version.major == 1234567
    assert version.minor == 56789
    assert version.patch == 34567


def test_raise_version():
    with pytest.raises(InvalidVersion):
        Version("hello_")

    with pytest.raises(InvalidVersion):
        Version("1.2.4b")

    with pytest.raises(InvalidVersion):
        Version("hj231#j")

    with pytest.raises(InvalidVersion):
        Version("+1.2.35")


def test_sort_versions():
    versions = [Version("1.2.3"), Version("1.1.4"), Version("0.1.4")]

    assert sorted(versions) == [
        Version("0.1.4"),
        Version("1.1.4"),
        Version("1.2.3"),
    ]
