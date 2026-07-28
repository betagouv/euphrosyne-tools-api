from __future__ import annotations

import hashlib
import io
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import BinaryIO

import pytest

from aglae.traupixe.detection import (
    FileIdCodec,
    ResolvedTraupixeFile,
    detect_traupixe_workbooks,
    resolve_traupixe_workbook,
)
from aglae.traupixe.exceptions import (
    TraupixeSourceChangedError,
    TraupixeWorkbookNotFoundError,
)
from aglae.traupixe.format import MAX_SOURCE_SIZE_BYTES
from aglae.traupixe.loader import (
    load_traupixe_workbook,
    validate_traupixe_workbook,
)
from aglae.traupixe.normalization import normalize_traupixe
from clients.data_models import ProjectFileOrDirectory, RunDataTypeType

VALID_CONTENT = b"valid TRAUPIXE workbook"
NOW = datetime(2026, 7, 28, 10, 0, tzinfo=timezone.utc)


class FakeDataClient:
    def __init__(self) -> None:
        self.directories: dict[
            tuple[str, str, RunDataTypeType, str | None],
            list[ProjectFileOrDirectory],
        ] = {}
        self.contents: dict[str, bytes] = {}
        self.downloaded_paths: list[str] = []

    def add_directory(
        self,
        project_slug: str,
        run_name: str,
        data_type: RunDataTypeType,
        *,
        folder: str | None,
        name: str,
        path: str,
    ) -> None:
        self._entries(project_slug, run_name, data_type, folder).append(
            ProjectFileOrDirectory(
                name=name,
                last_modified=None,
                size=None,
                path=path,
                type="directory",
            )
        )

    def add_file(
        self,
        project_slug: str,
        run_name: str,
        data_type: RunDataTypeType,
        *,
        folder: str | None = None,
        name: str,
        path: str,
        content: bytes = VALID_CONTENT,
        size: int | None = None,
        last_modified: datetime | None = NOW,
    ) -> None:
        self._entries(project_slug, run_name, data_type, folder).append(
            ProjectFileOrDirectory(
                name=name,
                last_modified=last_modified,
                size=len(content) if size is None else size,
                path=path,
                type="file",
            )
        )
        self.contents[path] = content

    def replace_file_entry(
        self,
        project_slug: str,
        run_name: str,
        data_type: RunDataTypeType,
        *,
        name: str,
        path: str,
        content: bytes,
        size: int,
        last_modified: datetime | None,
    ) -> None:
        entries = self._entries(project_slug, run_name, data_type, None)
        entries[:] = [
            entry
            for entry in entries
            if not (entry.type == "file" and entry.path == path)
        ]
        self.add_file(
            project_slug,
            run_name,
            data_type,
            name=name,
            path=path,
            content=content,
            size=size,
            last_modified=last_modified,
        )

    def get_run_files_folders(
        self,
        project_name: str,
        run_name: str,
        data_type: RunDataTypeType,
        folder: str | None,
    ) -> list[ProjectFileOrDirectory]:
        return list(
            self.directories.get(
                (project_name, run_name, data_type, folder),
                (),
            )
        )

    def download_run_file(self, filepath: str) -> io.BytesIO:
        self.downloaded_paths.append(filepath)
        return io.BytesIO(self.contents[filepath])

    def _entries(
        self,
        project_slug: str,
        run_name: str,
        data_type: RunDataTypeType,
        folder: str | None,
    ) -> list[ProjectFileOrDirectory]:
        return self.directories.setdefault(
            (project_slug, run_name, data_type, folder),
            [],
        )


def _validator(source: BinaryIO) -> bool:
    return source.read().startswith(b"valid")


def _detect(
    client: FakeDataClient,
    *,
    project_slug: str = "project-01",
    run_name: str = "run-01",
    codec: FileIdCodec | None = None,
):
    return detect_traupixe_workbooks(
        client,  # type: ignore[arg-type]
        project_slug,
        run_name,
        validator=_validator,
        file_id_codec=codec or FileIdCodec("test-secret"),
    )


def test_detection_returns_empty_result_when_no_workbook_is_available() -> None:
    result = _detect(FakeDataClient())

    assert result.files == ()
    assert result.default_file_id is None


def test_detection_walks_both_data_types_recursively_and_filters_before_read() -> None:
    client = FakeDataClient()
    client.add_directory(
        "project-01",
        "run-01",
        "raw_data",
        folder=None,
        name="nested",
        path="projects/project-01/runs/run-01/raw_data/nested",
    )
    client.add_file(
        "project-01",
        "run-01",
        "raw_data",
        folder="nested",
        name="nested.xlsx",
        path="projects/project-01/runs/run-01/raw_data/nested/nested.xlsx",
    )
    client.add_file(
        "project-01",
        "run-01",
        "processed_data",
        name="processed.XLSX",
        path="projects/project-01/runs/run-01/processed_data/processed.XLSX",
    )
    client.add_file(
        "project-01",
        "run-01",
        "raw_data",
        name="ignored.xls",
        path="projects/project-01/runs/run-01/raw_data/ignored.xls",
    )
    client.add_file(
        "project-01",
        "run-01",
        "raw_data",
        name="too-large.xlsx",
        path="projects/project-01/runs/run-01/raw_data/too-large.xlsx",
        size=MAX_SOURCE_SIZE_BYTES + 1,
    )
    client.add_file(
        "project-01",
        "run-01",
        "processed_data",
        name="invalid.xlsx",
        path="projects/project-01/runs/run-01/processed_data/invalid.xlsx",
        content=b"not a TRAUPIXE workbook",
    )
    client.add_file(
        "project-01",
        "run-01",
        "HDF5",
        name="not-scanned.xlsx",
        path="projects/project-01/runs/run-01/HDF5/not-scanned.xlsx",
    )

    result = _detect(client)

    assert {file.name for file in result.files} == {
        "nested.xlsx",
        "processed.XLSX",
    }
    assert set(client.downloaded_paths) == {
        "projects/project-01/runs/run-01/raw_data/nested/nested.xlsx",
        "projects/project-01/runs/run-01/processed_data/processed.XLSX",
        "projects/project-01/runs/run-01/processed_data/invalid.xlsx",
    }


def test_detection_sorts_by_mtime_descending_then_by_stable_file_identity() -> None:
    client = FakeDataClient()
    client.add_file(
        "project-01",
        "run-01",
        "raw_data",
        name="b.xlsx",
        path="raw/b.xlsx",
        last_modified=NOW,
    )
    client.add_file(
        "project-01",
        "run-01",
        "processed_data",
        name="newest.xlsx",
        path="processed/newest.xlsx",
        last_modified=NOW + timedelta(minutes=1),
    )
    client.add_file(
        "project-01",
        "run-01",
        "raw_data",
        name="a.xlsx",
        path="raw/a.xlsx",
        last_modified=NOW,
    )

    result = _detect(client)

    assert [file.name for file in result.files] == [
        "newest.xlsx",
        "a.xlsx",
        "b.xlsx",
    ]
    assert result.default_file_id == result.files[0].file_id


def test_file_id_is_stable_opaque_and_scoped_to_project_and_run() -> None:
    client = FakeDataClient()
    path = "projects/project-01/runs/run-01/raw_data/source.xlsx"
    for project_slug, run_name in (
        ("project-01", "run-01"),
        ("project-02", "run-01"),
        ("project-01", "run-02"),
    ):
        client.add_file(
            project_slug,
            run_name,
            "raw_data",
            name="source.xlsx",
            path=path,
        )
    codec = FileIdCodec("test-secret")

    original = _detect(client, codec=codec).files[0].file_id
    repeated = _detect(client, codec=codec).files[0].file_id
    other_project = (
        _detect(
            client,
            project_slug="project-02",
            codec=codec,
        )
        .files[0]
        .file_id
    )
    other_run = (
        _detect(
            client,
            run_name="run-02",
            codec=codec,
        )
        .files[0]
        .file_id
    )

    assert original == repeated
    assert original != other_project
    assert original != other_run
    assert "source" not in original
    assert "project" not in original
    assert "run-01" not in original
    assert hashlib.sha256(VALID_CONTENT).hexdigest() not in original


def test_file_id_rejects_an_authenticated_payload_that_was_tampered_with() -> None:
    client = FakeDataClient()
    client.add_file(
        "project-01",
        "run-01",
        "raw_data",
        name="source.xlsx",
        path="raw/source.xlsx",
    )
    codec = FileIdCodec("test-secret")
    file_id = _detect(client, codec=codec).files[0].file_id
    replacement = "A" if file_id[-1] != "A" else "B"

    assert codec.decode(file_id[:-1] + replacement) is None


def test_resolve_returns_the_same_validated_source_in_the_same_scope() -> None:
    client = FakeDataClient()
    path = "projects/project-01/runs/run-01/raw_data/source.xlsx"
    client.add_file(
        "project-01",
        "run-01",
        "raw_data",
        name="source.xlsx",
        path=path,
    )
    codec = FileIdCodec("test-secret")
    file_id = _detect(client, codec=codec).files[0].file_id

    resolved = resolve_traupixe_workbook(
        client,  # type: ignore[arg-type]
        "project-01",
        "run-01",
        file_id,
        validator=_validator,
        file_id_codec=codec,
    )

    assert isinstance(resolved, ResolvedTraupixeFile)
    assert resolved.name == "source.xlsx"
    assert resolved.path == path
    assert resolved.sha256 == hashlib.sha256(VALID_CONTENT).hexdigest()
    assert resolved.source.read() == VALID_CONTENT
    resolved.source.close()


@pytest.mark.parametrize(
    ("project_slug", "run_name", "file_id"),
    [
        ("project-02", "run-01", None),
        ("project-01", "run-02", None),
        ("project-01", "run-01", "malformed"),
    ],
)
def test_resolve_rejects_unknown_or_out_of_scope_file_id(
    project_slug: str,
    run_name: str,
    file_id: str | None,
) -> None:
    client = FakeDataClient()
    client.add_file(
        "project-01",
        "run-01",
        "raw_data",
        name="source.xlsx",
        path="raw/source.xlsx",
    )
    codec = FileIdCodec("test-secret")
    issued_file_id = _detect(client, codec=codec).files[0].file_id

    with pytest.raises(TraupixeWorkbookNotFoundError):
        resolve_traupixe_workbook(
            client,  # type: ignore[arg-type]
            project_slug,
            run_name,
            file_id or issued_file_id,
            validator=_validator,
            file_id_codec=codec,
        )


@pytest.mark.parametrize(
    ("new_size", "new_mtime"),
    [
        (len(VALID_CONTENT) + 1, NOW),
        (len(VALID_CONTENT), NOW + timedelta(seconds=1)),
    ],
)
def test_resolve_detects_size_or_date_replacement(
    new_size: int,
    new_mtime: datetime,
) -> None:
    client = FakeDataClient()
    path = "raw/source.xlsx"
    client.add_file(
        "project-01",
        "run-01",
        "raw_data",
        name="source.xlsx",
        path=path,
    )
    codec = FileIdCodec("test-secret")
    file_id = _detect(client, codec=codec).files[0].file_id
    client.replace_file_entry(
        "project-01",
        "run-01",
        "raw_data",
        name="source.xlsx",
        path=path,
        content=VALID_CONTENT,
        size=new_size,
        last_modified=new_mtime,
    )

    with pytest.raises(TraupixeSourceChangedError):
        resolve_traupixe_workbook(
            client,  # type: ignore[arg-type]
            "project-01",
            "run-01",
            file_id,
            validator=_validator,
            file_id_codec=codec,
        )


def test_resolve_detects_content_replacement_with_same_size_and_date() -> None:
    client = FakeDataClient()
    path = "raw/source.xlsx"
    original = b"valid source content"
    replacement = b"valid source replace"
    assert len(original) == len(replacement)
    client.add_file(
        "project-01",
        "run-01",
        "raw_data",
        name="source.xlsx",
        path=path,
        content=original,
    )
    codec = FileIdCodec("test-secret")
    file_id = _detect(client, codec=codec).files[0].file_id
    client.replace_file_entry(
        "project-01",
        "run-01",
        "raw_data",
        name="source.xlsx",
        path=path,
        content=replacement,
        size=len(replacement),
        last_modified=NOW,
    )

    with pytest.raises(TraupixeSourceChangedError) as error:
        resolve_traupixe_workbook(
            client,  # type: ignore[arg-type]
            "project-01",
            "run-01",
            file_id,
            validator=_validator,
            file_id_codec=codec,
        )

    assert error.value.expected_sha256 == hashlib.sha256(original).hexdigest()
    assert error.value.actual_sha256 == hashlib.sha256(replacement).hexdigest()


def test_resolve_distinguishes_deleted_source_from_modified_source() -> None:
    client = FakeDataClient()
    client.add_file(
        "project-01",
        "run-01",
        "raw_data",
        name="source.xlsx",
        path="raw/source.xlsx",
    )
    codec = FileIdCodec("test-secret")
    file_id = _detect(client, codec=codec).files[0].file_id
    client.directories[("project-01", "run-01", "raw_data", None)].clear()

    with pytest.raises(TraupixeWorkbookNotFoundError):
        resolve_traupixe_workbook(
            client,  # type: ignore[arg-type]
            "project-01",
            "run-01",
            file_id,
            validator=_validator,
            file_id_codec=codec,
        )


def test_discovery_resolution_and_normalization_use_the_real_contract() -> None:
    fixture = Path(__file__).parent / "fixtures" / "traupixe_reference_anonymized.xlsx"
    client = FakeDataClient()
    client.add_file(
        "project-01",
        "run-01",
        "raw_data",
        name=fixture.name,
        path=f"raw_data/{fixture.name}",
        content=fixture.read_bytes(),
    )
    codec = FileIdCodec("test-secret")

    discovery = detect_traupixe_workbooks(
        client,  # type: ignore[arg-type]
        "project-01",
        "run-01",
        validator=validate_traupixe_workbook,
        file_id_codec=codec,
    )
    resolved = resolve_traupixe_workbook(
        client,  # type: ignore[arg-type]
        "project-01",
        "run-01",
        discovery.files[0].file_id,
        validator=validate_traupixe_workbook,
        file_id_codec=codec,
    )
    try:
        dataset = normalize_traupixe(
            load_traupixe_workbook(
                resolved.source,
                source_name=resolved.name,
            )
        )
    finally:
        resolved.source.close()

    assert len(dataset.analyses) == 48
    assert len(dataset.measurements) == 3456
