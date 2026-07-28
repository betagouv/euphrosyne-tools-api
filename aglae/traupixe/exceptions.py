from __future__ import annotations


class TraupixeError(Exception):
    """Base class for TRAUPIXE file-selection failures."""


class InvalidTraupixeScopeError(ValueError):
    def __init__(self, component: str):
        self.component = component
        super().__init__(
            f"{component} must contain only letters, numbers, underscores, "
            "hyphens, or spaces"
        )


class TraupixeIncompatibleWorkbookError(TraupixeError):
    def __init__(self, missing_sheets: set[str] | frozenset[str]):
        self.missing_sheets = frozenset(missing_sheets)
        super().__init__("Le fichier ne présente pas la signature minimale TRAUPIXE.")


class TraupixeWorkbookNotFoundError(TraupixeError):
    def __init__(self, file_id: str):
        self.file_id = file_id
        super().__init__("Le fichier TRAUPIXE sélectionné est introuvable.")


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


class TraupixeSourceChangedError(TraupixeError):
    def __init__(self, expected_sha256: str, actual_sha256: str):
        self.expected_sha256 = expected_sha256
        self.actual_sha256 = actual_sha256
        super().__init__(
            "Le fichier TRAUPIXE a été modifié. Veuillez le sélectionner à "
            "nouveau et relancer votre requête."
        )
