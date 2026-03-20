import os
import re
import typing
from dataclasses import dataclass
from pathlib import Path

from clients.data_models import RunDataTypeType


@dataclass(frozen=True)
class ProjectRef:
    project_slug: str

    @classmethod
    def from_path(cls, path: Path) -> typing.Self:
        """Validate and parse path"""
        cls.validate_path(path)
        parts = _get_path_without_prefix(path).parts
        return cls(parts[0])

    @staticmethod
    def validate_path(path: Path):
        if not re.match(
            rf"^{_get_projects_path()}\/[\w\- ]+(?:\/.*)?$",
            str(path),
        ):
            raise IncorrectDataFilePath(
                "path must be like {projects_path_prefix}/<project_slug>"
            )


@dataclass(frozen=True)
class RunRef(ProjectRef):
    run_name: str

    @classmethod
    def from_path(cls, path: Path) -> typing.Self:
        cls.validate_path(path)
        parts = _get_path_without_prefix(path).parts
        return cls(parts[0], parts[2])

    @staticmethod
    def validate_path(path: Path):
        if not re.match(
            rf"^{_get_projects_path()}\/[\w\- ]+\/runs\/[\w\- ]+$",
            str(path),
        ):
            raise IncorrectDataFilePath(
                "path must be like {projects_path_prefix}/<project_slug>/runs/<run_name>"
            )


@dataclass(frozen=True)
class RunDataTypeRef(RunRef):
    data_type: RunDataTypeType

    @classmethod
    def from_path(cls, path: Path) -> typing.Self:
        cls.validate_path(path)
        parts = _get_path_without_prefix(path).parts
        return cls(
            parts[0],
            parts[2],
            typing.cast(RunDataTypeType, parts[3]),
        )

    @staticmethod
    def validate_path(path: Path):
        if not re.match(
            rf"^{_get_projects_path()}\/[\w\- ]+\/runs\/[\w\- ]+\/(raw_data|processed_data|HDF5)",  # noqa: E501
            str(path),
        ):
            raise IncorrectDataFilePath(
                "path must start with {projects_path_prefix}/<project_slug>/runs/<run_name>/(processed_data|raw_data|HDF5)/"  # noqa: E501
            )


@dataclass(frozen=True)
class ProjectDocumentRef(ProjectRef):
    @staticmethod
    def validate_path(path: Path):
        if not re.match(
            rf"^{_get_projects_path()}\/[\w\- ]+\/documents",
            str(path),
        ):
            raise IncorrectDataFilePath(
                "path must start with {projects_path_prefix}/<project_slug>/documents/"
            )


class IncorrectDataFilePath(Exception):
    def __init__(self, message: str, *args: object):
        self.message = message
        super().__init__(*args)


def _get_projects_path():
    return os.getenv("DATA_PROJECTS_LOCATION_PREFIX", "")


def _get_path_without_prefix(path: Path):
    projects_path_prefix = _get_projects_path()
    return Path(str(path).replace(projects_path_prefix + "/", "", 1))
