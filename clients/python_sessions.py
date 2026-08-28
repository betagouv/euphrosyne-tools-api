from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, Field


class PythonSessionFile(BaseModel):
    """File metadata returned by a Python execution session."""

    model_config = ConfigDict(extra="ignore", frozen=True, populate_by_name=True)

    name: str
    directory: str
    resource_type: str = Field(alias="type")
    content_type: str = Field(alias="contentType")
    size_in_bytes: int = Field(alias="sizeInBytes", ge=0)
    last_modified_at: datetime = Field(alias="lastModifiedAt")


@dataclass(frozen=True)
class PythonExecutionResult:
    status: str
    stdout: str
    stderr: str
    duration_ms: int

    def tool_output(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "execution_time_ms": self.duration_ms,
        }


class PythonSessionsClient(Protocol):
    """Isolated Python execution session used by data visualizations."""

    data_directory: str

    def upload_file(self, session_id: str, filename: str, content: bytes) -> None:
        """Upload one file to a session's data directory."""
        ...

    def execute(self, session_id: str, code: str) -> PythonExecutionResult:
        """Execute Python synchronously in an existing session."""
        ...

    def list_files(self, session_id: str) -> list[PythonSessionFile]:
        """List files currently available in a session."""
        ...

    def download_file(self, session_id: str, filename: str) -> bytes:
        """Download one generated session file."""
        ...

    def delete_session(self, session_id: str) -> None:
        """Delete a session; implementations should tolerate missing sessions."""
        ...
