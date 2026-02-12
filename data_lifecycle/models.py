from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID


class LifecycleOperationType(str, Enum):
    COOL = "COOL"
    RESTORE = "RESTORE"


class LifecycleOperationStatus(str, Enum):
    ACCEPTED = "ACCEPTED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class LifecycleOperationProgressStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


@dataclass
class LifecycleOperation:
    project_slug: str
    operation_id: UUID
    type: LifecycleOperationType
    status: LifecycleOperationStatus | None = None
    finished_at: datetime | None = None
    bytes_copied: int | None = None
    files_copied: int | None = None
    error_message: str | None = None
    error_details: dict[str, Any] | None = None

    def guard_key(self) -> tuple[str, str, str]:
        return (
            self.project_slug,
            self.type.value,
            str(self.operation_id),
        )


@dataclass
class LifecycleOperationStatusView:
    operation_id: UUID
    project_slug: str
    type: LifecycleOperationType
    status: LifecycleOperationProgressStatus
    bytes_total: int
    files_total: int
    bytes_copied: int
    files_copied: int
    progress_percent: float
    error_details: dict[str, Any] | None = None
