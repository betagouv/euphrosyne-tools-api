from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID

from .storage_types import StorageRole


class LifecycleOperationType(str, Enum):
    COOL = "COOL"
    RESTORE = "RESTORE"


class LifecycleState(str, Enum):
    HOT = "HOT"
    COOL = "COOL"
    COOLING = "COOLING"
    RESTORING = "RESTORING"


class LifecycleOperationStatus(str, Enum):
    ACCEPTED = "ACCEPTED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class LifecycleOperationProgressStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class LifecycleOperationPhase(str, Enum):
    FROM_DATA_DELETION = "FROM_DATA_DELETION"


class FromDataDeletionStatus(str, Enum):
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
    bytes_total: int | None = None
    files_total: int | None = None
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


@dataclass
class FromDataDeletionOperation:
    project_slug: str
    operation_id: UUID
    storage_role: StorageRole
    file_count: int
    total_size: int
    phase: LifecycleOperationPhase = LifecycleOperationPhase.FROM_DATA_DELETION

    def guard_key(self) -> tuple[str, str, str, str]:
        return (
            self.project_slug,
            str(self.operation_id),
            self.storage_role.value,
            self.phase.value,
        )


@dataclass
class FromDataDeletionAccepted:
    project_slug: str
    operation_id: UUID
    storage_role: StorageRole
    phase: LifecycleOperationPhase
    status: LifecycleOperationStatus


@dataclass
class FromDataDeletionError:
    title: str
    message: str


@dataclass
class FromDataDeletionCallback:
    operation_id: UUID
    phase: LifecycleOperationPhase
    from_data_deletion_status: FromDataDeletionStatus
    error: FromDataDeletionError | None = None
