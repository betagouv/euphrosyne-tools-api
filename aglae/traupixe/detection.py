from __future__ import annotations

import base64
import hashlib
import hmac
import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import BinaryIO, Callable, Iterator

from clients.data_client import AbstractDataClient
from clients.data_models import ProjectFileOrDirectory, RunDataTypeType

from .exceptions import (
    TraupixeError,
    TraupixeSourceChangedError,
    TraupixeWorkbookNotFoundError,
)
from .format import TRAUPIXE_FORMAT

RUN_DATA_TYPES: tuple[RunDataTypeType, ...] = ("raw_data", "processed_data")
_FILE_ID_VERSION = 1
_DIGEST_SIZE = hashlib.sha256().digest_size
_FILE_ID_PAYLOAD_SIZE = 1 + 4 * _DIGEST_SIZE
_HASH_CHUNK_SIZE = 1024 * 1024

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
    location_digest: bytes
    metadata_digest: bytes
    expected_sha256: str


class FileIdCodec:
    """Issue authenticated, opaque identifiers for run files.

    The identifier contains no plaintext storage path. Its content fingerprint is
    masked with an HMAC-derived key and the complete payload is authenticated.
    """

    def __init__(self, secret: str | bytes):
        secret_bytes = secret.encode("utf-8") if isinstance(secret, str) else secret
        if not secret_bytes:
            raise ValueError("file_id secret must not be empty")
        self._secret = secret_bytes

    def issue(
        self,
        *,
        project_slug: str,
        run_name: str,
        candidate: _Candidate,
        sha256: str,
    ) -> str:
        sha256_bytes = _parse_sha256(sha256)
        location_digest = self._location_digest(
            project_slug=project_slug,
            run_name=run_name,
            candidate=candidate,
        )
        metadata_digest = self._metadata_digest(
            location_digest=location_digest,
            candidate=candidate,
        )
        fingerprint_mask = self._hmac(
            b"fingerprint-mask\0" + location_digest + metadata_digest
        )
        masked_fingerprint = bytes(
            left ^ right
            for left, right in zip(sha256_bytes, fingerprint_mask, strict=True)
        )
        body = (
            bytes((_FILE_ID_VERSION,))
            + location_digest
            + metadata_digest
            + masked_fingerprint
        )
        authentication_tag = self._hmac(b"file-id\0" + body)
        return _base64url_encode(body + authentication_tag)

    def decode(self, file_id: str) -> _FileIdClaims | None:
        try:
            payload = _base64url_decode(file_id)
        except (TypeError, ValueError):
            return None
        if len(payload) != _FILE_ID_PAYLOAD_SIZE:
            return None

        body = payload[:-_DIGEST_SIZE]
        authentication_tag = payload[-_DIGEST_SIZE:]
        if body[0] != _FILE_ID_VERSION or not hmac.compare_digest(
            authentication_tag,
            self._hmac(b"file-id\0" + body),
        ):
            return None

        location_start = 1
        metadata_start = location_start + _DIGEST_SIZE
        fingerprint_start = metadata_start + _DIGEST_SIZE
        location_digest = body[location_start:metadata_start]
        metadata_digest = body[metadata_start:fingerprint_start]
        masked_fingerprint = body[fingerprint_start:]
        fingerprint_mask = self._hmac(
            b"fingerprint-mask\0" + location_digest + metadata_digest
        )
        fingerprint = bytes(
            left ^ right
            for left, right in zip(
                masked_fingerprint,
                fingerprint_mask,
                strict=True,
            )
        )
        return _FileIdClaims(
            location_digest=location_digest,
            metadata_digest=metadata_digest,
            expected_sha256=fingerprint.hex(),
        )

    def matches_location(
        self,
        claims: _FileIdClaims,
        *,
        project_slug: str,
        run_name: str,
        candidate: _Candidate,
    ) -> bool:
        return hmac.compare_digest(
            claims.location_digest,
            self._location_digest(
                project_slug=project_slug,
                run_name=run_name,
                candidate=candidate,
            ),
        )

    def matches_metadata(
        self,
        claims: _FileIdClaims,
        *,
        candidate: _Candidate,
    ) -> bool:
        return hmac.compare_digest(
            claims.metadata_digest,
            self._metadata_digest(
                location_digest=claims.location_digest,
                candidate=candidate,
            ),
        )

    def _location_digest(
        self,
        *,
        project_slug: str,
        run_name: str,
        candidate: _Candidate,
    ) -> bytes:
        scope = _canonical_json(
            (
                project_slug,
                run_name,
                candidate.data_type,
                candidate.path,
            )
        )
        return self._hmac(b"location\0" + scope)

    def _metadata_digest(
        self,
        *,
        location_digest: bytes,
        candidate: _Candidate,
    ) -> bytes:
        metadata = _canonical_json(
            (
                candidate.size,
                _canonical_datetime(candidate.last_modified),
            )
        )
        return self._hmac(b"metadata\0" + location_digest + metadata)

    def _hmac(self, value: bytes) -> bytes:
        return hmac.new(self._secret, value, hashlib.sha256).digest()


def detect_traupixe_workbooks(
    data_client: AbstractDataClient,
    project_slug: str,
    run_name: str,
    *,
    validator: TraupixeValidator,
    file_id_codec: FileIdCodec,
) -> TraupixeFileDiscovery:
    detected: list[tuple[TraupixeFile, _Candidate]] = []

    for candidate in _iter_xlsx_candidates(data_client, project_slug, run_name):
        if (
            candidate.size is None
            or candidate.size > TRAUPIXE_FORMAT.maximum_source_size
        ):
            continue
        with closing(data_client.download_run_file(candidate.path)) as source:
            sha256 = _sha256(source)
            if not _is_valid(source, validator):
                continue
        file_id = file_id_codec.issue(
            project_slug=project_slug,
            run_name=run_name,
            candidate=candidate,
            sha256=sha256,
        )
        detected.append(
            (
                TraupixeFile(
                    file_id=file_id,
                    name=candidate.name,
                    last_modified=candidate.last_modified,
                    size=candidate.size,
                ),
                candidate,
            )
        )

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
    validator: TraupixeValidator,
    file_id_codec: FileIdCodec,
) -> ResolvedTraupixeFile:
    claims = file_id_codec.decode(file_id)
    if claims is None:
        raise TraupixeWorkbookNotFoundError(file_id)

    for candidate in _iter_xlsx_candidates(data_client, project_slug, run_name):
        if not file_id_codec.matches_location(
            claims,
            project_slug=project_slug,
            run_name=run_name,
            candidate=candidate,
        ):
            continue
        if (
            candidate.size is None
            or candidate.size > TRAUPIXE_FORMAT.maximum_source_size
        ):
            raise TraupixeSourceChangedError(
                expected_sha256=claims.expected_sha256,
                actual_sha256="",
            )

        source = data_client.download_run_file(candidate.path)
        try:
            actual_sha256 = _sha256(source)
            if not file_id_codec.matches_metadata(
                claims, candidate=candidate
            ) or not hmac.compare_digest(
                claims.expected_sha256,
                actual_sha256,
            ):
                raise TraupixeSourceChangedError(
                    expected_sha256=claims.expected_sha256,
                    actual_sha256=actual_sha256,
                )
            if not _is_valid(source, validator):
                raise TraupixeSourceChangedError(
                    expected_sha256=claims.expected_sha256,
                    actual_sha256=actual_sha256,
                )
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


def _iter_xlsx_candidates(
    data_client: AbstractDataClient,
    project_slug: str,
    run_name: str,
) -> Iterator[_Candidate]:
    for data_type in RUN_DATA_TYPES:
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
            continue
        if not entry.name.casefold().endswith(TRAUPIXE_FORMAT.extension):
            continue
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
    while chunk := source.read(_HASH_CHUNK_SIZE):
        digest.update(chunk)
    source.seek(0)
    return digest.hexdigest()


def _is_valid(source: BinaryIO, validator: TraupixeValidator) -> bool:
    source.seek(0)
    try:
        result = validator(source)
    except TraupixeError:
        return False
    finally:
        if not source.closed:
            source.seek(0)
    return result is not False


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


def _canonical_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _canonical_json(values: tuple[object, ...]) -> bytes:
    return json.dumps(
        values,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")


def _parse_sha256(value: str) -> bytes:
    try:
        parsed = bytes.fromhex(value)
    except ValueError as error:
        raise ValueError("sha256 must be a hexadecimal digest") from error
    if len(parsed) != _DIGEST_SIZE:
        raise ValueError("sha256 must contain 32 bytes")
    return parsed


def _base64url_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _base64url_decode(value: str) -> bytes:
    if not value:
        raise ValueError("file_id must not be empty")
    encoded = value.encode("ascii")
    padding = b"=" * (-len(encoded) % 4)
    return base64.b64decode(encoded + padding, altchars=b"-_", validate=True)
