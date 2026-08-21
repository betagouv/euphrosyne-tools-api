from pathlib import Path

TRAUPIXE_EXTENSION = ".xlsx"
TRAUPIXE_NAME_MARKER = "traupixe"
MAX_SOURCE_SIZE_BYTES = 100 * 1024 * 1024


def is_traupixe_path(path: Path) -> bool:
    return (
        path.suffix.casefold() == TRAUPIXE_EXTENSION
        and TRAUPIXE_NAME_MARKER in path.name.casefold()
    )
