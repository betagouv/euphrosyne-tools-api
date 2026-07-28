from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class TraupixeValidationCode(str, Enum):
    UNSUPPORTED_EXTENSION = "unsupported_extension"
    SOURCE_TOO_LARGE = "source_too_large"
    UNREADABLE_WORKBOOK = "unreadable_workbook"
    MISSING_SHEET = "missing_sheet"
    INVALID_HEADER = "invalid_header"
    INVALID_ANALYSIS_ID = "invalid_analysis_id"
    DUPLICATE_ANALYSIS_ID = "duplicate_analysis_id"
    MISALIGNED_ANALYSIS_IDS = "misaligned_analysis_ids"
    UNKNOWN_DETECTOR = "unknown_detector"
    INVALID_VALUE = "invalid_value"
    SOURCE_CHANGED = "source_changed"


@dataclass(frozen=True)
class TraupixeValidationIssue:
    code: TraupixeValidationCode
    message: str
    sheet: str | None = None
    cell: str | None = None


class TraupixeError(Exception):
    """Base class for failures raised by the TRAUPIXE domain package."""


class TraupixeFormatError(TraupixeError):
    def __init__(self, issues: tuple[TraupixeValidationIssue, ...]):
        if not issues:
            raise ValueError("At least one validation issue is required")
        self.issues = issues
        super().__init__(
            "Ce fichier TRAUPIXE ne respecte pas le contrat minimal pris en charge."
        )


class TraupixeIncompatibleWorkbookError(TraupixeFormatError):
    pass


class TraupixeWorkbookNotFoundError(TraupixeError):
    def __init__(self, file_id: str):
        self.file_id = file_id
        super().__init__("Le fichier TRAUPIXE sélectionné est introuvable.")


class TraupixeUnsupportedFileError(TraupixeError):
    def __init__(self, extension: str):
        self.extension = extension
        super().__init__(f"Unsupported TRAUPIXE file extension: {extension}")


class TraupixeTooLargeError(TraupixeError):
    def __init__(self, size: int, maximum_size: int):
        self.size = size
        self.maximum_size = maximum_size
        super().__init__(
            f"TRAUPIXE source is too large: {size} bytes, maximum is "
            f"{maximum_size} bytes"
        )


class TraupixeUnreadableError(TraupixeError):
    pass


class TraupixeNormalizationError(TraupixeError):
    pass


class TraupixeSourceChangedError(TraupixeError):
    def __init__(self, expected_sha256: str, actual_sha256: str):
        self.expected_sha256 = expected_sha256
        self.actual_sha256 = actual_sha256
        super().__init__(
            "Le fichier TRAUPIXE a été modifié. Veuillez le sélectionner à "
            "nouveau et relancer votre requête."
        )
