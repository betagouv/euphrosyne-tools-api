import re

VERSION_REGEX = r"^(\d+\.\d+\.\d+)$"


class InvalidVersion(ValueError):
    pass


class Version:
    """
    Class to represent a version number.
    Can only be of the following format "major.minor.patch"

    ```
    version = Version("1.2.4")
    print(version)
    >>> <Version(1.2.4)>
    ```
    """

    def __init__(self, version: str) -> None:
        match = re.match(VERSION_REGEX, version)
        if not match:
            raise InvalidVersion(f"Invalid version: {version}")

        self._release = tuple(map(int, version.split(".")))

    @property
    def major(self) -> int:
        return self._release[0]

    @property
    def minor(self) -> int:
        return self._release[1]

    @property
    def patch(self) -> int:
        return self._release[2]

    def _hash(self) -> int:
        return hash(self._release)

    def __lt__(self, other: "Version") -> bool:
        if not isinstance(other, Version):
            return NotImplemented
        return self._release < other._release

    def __le__(self, other: "Version") -> bool:
        if not isinstance(other, Version):
            return NotImplemented
        return self._release <= other._release

    def __eq__(self, other: "Version") -> bool:
        if not isinstance(other, Version):
            return NotImplemented
        return self._release == other._release

    def __ne__(self, other: "Version") -> bool:
        if not isinstance(other, Version):
            return NotImplemented
        return self._release != other._release

    def __gt__(self, other: "Version") -> bool:
        if not isinstance(other, Version):
            return NotImplemented
        return self._release > other._release

    def __ge__(self, other: "Version") -> bool:
        if not isinstance(other, Version):
            return NotImplemented
        return self._release >= other._release

    def __repr__(self) -> str:
        return f"<Version({self._release})>"

    def __str__(self) -> str:
        return ".".join(map(str, self._release))
