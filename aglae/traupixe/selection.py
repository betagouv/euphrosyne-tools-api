from __future__ import annotations

import base64
import hashlib
import json
import re
import xml.etree.ElementTree as ElementTree
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import BinaryIO, Callable, Iterator, cast
from zipfile import BadZipFile, ZipFile

from cryptography.fernet import Fernet, InvalidToken

from clients.data_client import AbstractDataClient
from clients.data_models import ProjectFileOrDirectory, RunDataTypeType

from .exceptions import (
    InvalidTraupixeScopeError,
    TraupixeError,
    TraupixeIncompatibleWorkbookError,
    TraupixeSourceChangedError,
    TraupixeTooLargeError,
    TraupixeUnreadableError,
    TraupixeWorkbookNotFoundError,
)

TRAUPIXE_EXTENSION = ".xlsx"
MAX_SOURCE_SIZE_BYTES = 100 * 1024 * 1024
REQUIRED_TRAUPIXE_SHEETS = frozenset(
    {
        "S_Conc. & Unc %",
        "S_Conc. & Unc ppm",
        "S_Best Det.",
    }
)

_RUN_DATA_TYPES: tuple[RunDataTypeType, ...] = ("raw_data", "processed_data")
_WORKBOOK_XML_PATH = "xl/workbook.xml"
_WORKBOOK_XML_MAX_BYTES = 1024 * 1024
_SPREADSHEET_NAMESPACE = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
_SHA256_SIZE = hashlib.sha256().digest_size
_HASH_CHUNK_SIZE = 1024 * 1024
_MINIMUM_FILE_ID_SECRET_SIZE = 32
_SCOPE_COMPONENT_PATTERN = re.compile(r"^[\w -]+$")

TraupixeValidator = Callable[[BinaryIO], object]


@dataclass(frozen=True)
class TraupixeFile:
    file_id: str
    name: str
    last_modified: datetime | None
    size: int


@dataclass(frozen=True)
class TraupixeFileDiscovery:
    files: tuple[TraupixeFile, ...]
    default_file_id: str | None


@dataclass(frozen=True)
class ResolvedTraupixeFile:
    file_id: str
    name: str
    last_modified: datetime | None
    size: int
    sha256: str
    data_type: RunDataTypeType
    path: str
    source: BinaryIO


@dataclass(frozen=True)
class _Candidate:
    name: str
    last_modified: datetime | None
    size: int | None
    data_type: RunDataTypeType
    path: str


@dataclass(frozen=True)
class _FileIdClaims:
    project_slug: str
    run_name: str
    data_type: RunDataTypeType
    path: str
    size: int
    last_modified: str | None
    expected_sha256: str


class FileIdCodec:
    """Encrypt and authenticate file selection details for the browser."""

    def __init__(self, secret: str | bytes):
        secret_bytes = secret.encode("utf-8") if isinstance(secret, str) else secret
        if len(secret_bytes) < _MINIMUM_FILE_ID_SECRET_SIZE:
            raise ValueError("file_id secret must contain at least 32 bytes")
        key = base64.urlsafe_b64encode(hashlib.sha256(secret_bytes).digest())
        self._fernet = Fernet(key)

    def issue(
        self,
        *,
        project_slug: str,
        run_name: str,
        candidate: _Candidate,
        sha256: str,
    ) -> str:
        if candidate.size is None:
            raise ValueError("candidate size is required")
        _parse_sha256(sha256)
        payload = json.dumps(
            {
                "project_slug": project_slug,
                "run_name": run_name,
                "data_type": candidate.data_type,
                "path": candidate.path,
                "size": candidate.size,
                "last_modified": _canonical_datetime(candidate.last_modified),
                "sha256": sha256,
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        return self._fernet.encrypt(payload).decode("ascii")

    def decode(self, file_id: str) -> _FileIdClaims | None:
        try:
            payload = json.loads(
                self._fernet.decrypt(file_id.encode("ascii")).decode("utf-8")
            )
        except (
            InvalidToken,
            UnicodeError,
            json.JSONDecodeError,
            TypeError,
            ValueError,
        ):
            return None
        if not isinstance(payload, dict):
            return None

        project_slug = payload.get("project_slug")
        run_name = payload.get("run_name")
        data_type = payload.get("data_type")
        path = payload.get("path")
        size = payload.get("size")
        last_modified = payload.get("last_modified")
        expected_sha256 = payload.get("sha256")
        if (
            not isinstance(project_slug, str)
            or not isinstance(run_name, str)
            or data_type not in _RUN_DATA_TYPES
            or not isinstance(path, str)
            or not isinstance(size, int)
            or isinstance(size, bool)
            or not isinstance(expected_sha256, str)
            or (last_modified is not None and not isinstance(last_modified, str))
        ):
            return None
        try:
            _parse_sha256(expected_sha256)
        except ValueError:
            return None
        return _FileIdClaims(
            project_slug=project_slug,
            run_name=run_name,
            data_type=cast(RunDataTypeType, data_type),
            path=path,
            size=size,
            last_modified=last_modified,
            expected_sha256=expected_sha256,
        )


def validate_traupixe_workbook(source: BinaryIO) -> None:
    """Check only that the XLSX is readable and has the minimal sheet signature."""

    source.seek(0)
    try:
        with ZipFile(source) as archive:
            workbook_xml = archive.getinfo(_WORKBOOK_XML_PATH)
            if workbook_xml.file_size > _WORKBOOK_XML_MAX_BYTES:
                raise TraupixeUnreadableError("Workbook metadata is too large")
            root = ElementTree.fromstring(archive.read(workbook_xml))
    except TraupixeError:
        raise
    except (
        BadZipFile,
        ElementTree.ParseError,
        EOFError,
        KeyError,
        NotImplementedError,
        OSError,
        RuntimeError,
        ValueError,
    ) as error:
        raise TraupixeUnreadableError("Unreadable XLSX workbook") from error
    finally:
        if not source.closed:
            source.seek(0)

    sheet_names = {
        name
        for sheet in root.findall(f".//{{{_SPREADSHEET_NAMESPACE}}}sheet")
        if (name := sheet.get("name")) is not None
    }
    missing_sheets = REQUIRED_TRAUPIXE_SHEETS - sheet_names
    if missing_sheets:
        raise TraupixeIncompatibleWorkbookError(missing_sheets)


def detect_traupixe_workbooks(
    data_client: AbstractDataClient,
    project_slug: str,
    run_name: str,
    *,
    file_id_codec: FileIdCodec,
    validator: TraupixeValidator = validate_traupixe_workbook,
) -> TraupixeFileDiscovery:
    _validate_scope(project_slug, run_name)
    detected: list[tuple[TraupixeFile, _Candidate]] = []

    for candidate in _iter_candidates(data_client, project_slug, run_name):
        if candidate.size is None or candidate.size > MAX_SOURCE_SIZE_BYTES:
            continue
        with closing(data_client.download_run_file(candidate.path)) as source:
            try:
                sha256 = _sha256(source)
            except TraupixeTooLargeError:
                continue
            if not _is_valid(source, validator):
                continue
        file = TraupixeFile(
            file_id=file_id_codec.issue(
                project_slug=project_slug,
                run_name=run_name,
                candidate=candidate,
                sha256=sha256,
            ),
            name=candidate.name,
            last_modified=candidate.last_modified,
            size=candidate.size,
        )
        detected.append((file, candidate))

    detected.sort(key=_detected_sort_key)
    files = tuple(file for file, _candidate in detected)
    return TraupixeFileDiscovery(
        files=files,
        default_file_id=files[0].file_id if files else None,
    )


def resolve_traupixe_workbook(
    data_client: AbstractDataClient,
    project_slug: str,
    run_name: str,
    file_id: str,
    *,
    file_id_codec: FileIdCodec,
    validator: TraupixeValidator = validate_traupixe_workbook,
) -> ResolvedTraupixeFile:
    _validate_scope(project_slug, run_name)
    claims = file_id_codec.decode(file_id)
    if (
        claims is None
        or claims.project_slug != project_slug
        or claims.run_name != run_name
    ):
        raise TraupixeWorkbookNotFoundError(file_id)

    for candidate in _iter_candidates(data_client, project_slug, run_name):
        if candidate.data_type != claims.data_type or candidate.path != claims.path:
            continue
        if (
            candidate.size is None
            or candidate.size > MAX_SOURCE_SIZE_BYTES
            or candidate.size != claims.size
            or _canonical_datetime(candidate.last_modified) != claims.last_modified
        ):
            raise _changed(claims)

        source = data_client.download_run_file(candidate.path)
        try:
            try:
                actual_sha256 = _sha256(source)
            except TraupixeTooLargeError as error:
                raise _changed(claims) from error
            if claims.expected_sha256 != actual_sha256:
                raise _changed(claims, actual_sha256)
            if not _is_valid(source, validator):
                raise _changed(claims, actual_sha256)
            source.seek(0)
            return ResolvedTraupixeFile(
                file_id=file_id,
                name=candidate.name,
                last_modified=candidate.last_modified,
                size=candidate.size,
                sha256=actual_sha256,
                data_type=candidate.data_type,
                path=candidate.path,
                source=source,
            )
        except Exception:
            source.close()
            raise

    raise TraupixeWorkbookNotFoundError(file_id)


def _iter_candidates(
    data_client: AbstractDataClient,
    project_slug: str,
    run_name: str,
) -> Iterator[_Candidate]:
    for data_type in _RUN_DATA_TYPES:
        yield from _walk_run_directory(
            data_client=data_client,
            project_slug=project_slug,
            run_name=run_name,
            data_type=data_type,
            folder=None,
            visited=set(),
        )


def _walk_run_directory(
    *,
    data_client: AbstractDataClient,
    project_slug: str,
    run_name: str,
    data_type: RunDataTypeType,
    folder: str | None,
    visited: set[str],
) -> Iterator[_Candidate]:
    folder_key = folder or ""
    if folder_key in visited:
        return
    visited.add(folder_key)

    entries = data_client.get_run_files_folders(
        project_slug,
        run_name,
        data_type,
        folder,
    )
    for entry in sorted(entries, key=_entry_sort_key):
        if entry.type == "directory":
            child_folder = _safe_child_folder(folder, entry)
            if child_folder is not None:
                yield from _walk_run_directory(
                    data_client=data_client,
                    project_slug=project_slug,
                    run_name=run_name,
                    data_type=data_type,
                    folder=child_folder,
                    visited=visited,
                )
        elif entry.name.casefold().endswith(TRAUPIXE_EXTENSION):
            yield _Candidate(
                name=entry.name,
                last_modified=entry.last_modified,
                size=entry.size,
                data_type=data_type,
                path=entry.path,
            )


def _safe_child_folder(
    parent: str | None,
    entry: ProjectFileOrDirectory,
) -> str | None:
    child = PurePosixPath(entry.name.replace("\\", "/"))
    if len(child.parts) != 1 or child.name in {"", ".", ".."} or child.is_absolute():
        return None
    return str(PurePosixPath(parent or "") / child.name)


def _sha256(source: BinaryIO) -> str:
    source.seek(0)
    digest = hashlib.sha256()
    size = 0
    try:
        while chunk := source.read(
            min(_HASH_CHUNK_SIZE, MAX_SOURCE_SIZE_BYTES - size + 1)
        ):
            size += len(chunk)
            if size > MAX_SOURCE_SIZE_BYTES:
                raise TraupixeTooLargeError(size, MAX_SOURCE_SIZE_BYTES)
            digest.update(chunk)
        return digest.hexdigest()
    finally:
        source.seek(0)


def _is_valid(source: BinaryIO, validator: TraupixeValidator) -> bool:
    source.seek(0)
    try:
        return validator(source) is not False
    except TraupixeError:
        return False
    finally:
        if not source.closed:
            source.seek(0)


def _changed(
    claims: _FileIdClaims,
    actual_sha256: str = "",
) -> TraupixeSourceChangedError:
    return TraupixeSourceChangedError(
        expected_sha256=claims.expected_sha256,
        actual_sha256=actual_sha256,
    )


def _entry_sort_key(entry: ProjectFileOrDirectory) -> tuple[bool, str, str, str]:
    return (
        entry.type != "directory",
        entry.name.casefold(),
        entry.name,
        entry.path,
    )


def _detected_sort_key(
    item: tuple[TraupixeFile, _Candidate],
) -> tuple[float, str, str, str, str]:
    file, candidate = item
    return (
        -_timestamp(file.last_modified),
        file.name.casefold(),
        file.name,
        candidate.data_type,
        candidate.path,
    )


def _timestamp(value: datetime | None) -> float:
    if value is None:
        return float("-inf")
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).timestamp()


def _validate_scope(project_slug: str, run_name: str) -> None:
    for value, component in (
        (project_slug, "project_slug"),
        (run_name, "run_name"),
    ):
        if (
            not value
            or value != value.strip()
            or not _SCOPE_COMPONENT_PATTERN.fullmatch(value)
            or value in {".", ".."}
        ):
            raise InvalidTraupixeScopeError(component)


def _canonical_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _parse_sha256(value: str) -> bytes:
    try:
        parsed = bytes.fromhex(value)
    except ValueError as error:
        raise ValueError("sha256 must be a hexadecimal digest") from error
    if len(parsed) != _SHA256_SIZE:
        raise ValueError("sha256 must contain 32 bytes")
    return parsed
