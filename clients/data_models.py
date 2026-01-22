from __future__ import annotations

from datetime import datetime
from typing import Literal, TypedDict

from pydantic import BaseModel

RunDataTypeType = Literal["processed_data", "raw_data", "HDF5"]


class ProjectFile(BaseModel):
    name: str
    last_modified: datetime | None = None
    size: int
    path: str


class ProjectFileOrDirectory(BaseModel):
    name: str
    last_modified: datetime | None = None
    size: int | None
    path: str
    type: Literal["file", "directory"]


class SASCredentials(TypedDict):
    url: str
    token: str
