from __future__ import annotations

import hashlib
import io
import xml.etree.ElementTree as ElementTree
from datetime import datetime, timedelta, timezone
from typing import BinaryIO
from zipfile import ZipFile

import pytest

import aglae.traupixe.selection as selection_module
from aglae.traupixe import (
    MAX_SOURCE_SIZE_BYTES,
    REQUIRED_TRAUPIXE_SHEETS,
    FileIdCodec,
    InvalidTraupixeScopeError,
    ResolvedTraupixeFile,
    TraupixeIncompatibleWorkbookError,
    TraupixeSourceChangedError,
    TraupixeUnreadableError,
    TraupixeWorkbookNotFoundError,
    detect_traupixe_workbooks,
    resolve_traupixe_workbook,
    validate_traupixe_workbook,
)
from clients.data_models import ProjectFileOrDirectory, RunDataTypeType

NOW = datetime(2026, 7, 28, 10, 0, tzinfo=timezone.utc)
TEST_FILE_ID_SECRET = b"s" * 32
_SPREADSHEET_NAMESPACE = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"


def _workbook_bytes(
    sheet_names: set[str] | frozenset[str] = REQUIRED_TRAUPIXE_SHEETS,
) -> bytes:
    ElementTree.register_namespace("", _SPREADSHEET_NAMESPACE)
    workbook = ElementTree.Element(f"{{{_SPREADSHEET_NAMESPACE}}}workbook")
    sheets = ElementTree.SubElement(
        workbook,
        f"{{{_SPREADSHEET_NAMESPACE}}}sheets",
    )
    for index, name in enumerate(sorted(sheet_names), start=1):
        ElementTree.SubElement(
            sheets,
            f"{{{_SPREADSHEET_NAMESPACE}}}sheet",
            {"name": name, "sheetId": str(index)},
        )

    output = io.BytesIO()
    with ZipFile(output, "w") as archive:
        archive.writestr(
            "xl/workbook.xml",
            ElementTree.tostring(
                workbook,
                encoding="utf-8",
                xml_declaration=True,
            ),
        )
    return output.getvalue()


VALID_WORKBOOK = _workbook_bytes()


class FakeDataClient:
    def __init__(self) -> None:
        self.directories: dict[
            tuple[str, str, RunDataTypeType, str | None],
            list[ProjectFileOrDirectory],
        ] = {}
        self.contents: dict[str, bytes] = {}
        self.downloaded_paths: list[str] = []
        self.list_calls: list[tuple[str, str, RunDataTypeType, str | None]] = []

    def add_directory(
        self,
        data_type: RunDataTypeType,
        *,
        name: str,
        path: str,
        folder: str | None = None,
    ) -> None:
        self._entries(data_type, folder).append(
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
        data_type: RunDataTypeType,
        *,
        name: str,
        path: str,
        content: bytes = VALID_WORKBOOK,
        size: int | None = None,
        last_modified: datetime | None = NOW,
        folder: str | None = None,
    ) -> None:
        self._entries(data_type, folder).append(
            ProjectFileOrDirectory(
                name=name,
                last_modified=last_modified,
                size=len(content) if size is None else size,
                path=path,
                type="file",
            )
        )
        self.contents[path] = content

    def get_run_files_folders(
        self,
        project_name: str,
        run_name: str,
        data_type: RunDataTypeType,
        folder: str | None,
    ) -> list[ProjectFileOrDirectory]:
        self.list_calls.append((project_name, run_name, data_type, folder))
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
        data_type: RunDataTypeType,
        folder: str | None,
        *,
        project_slug: str = "project-01",
        run_name: str = "run-01",
    ) -> list[ProjectFileOrDirectory]:
        return self.directories.setdefault(
            (project_slug, run_name, data_type, folder),
            [],
        )


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
        file_id_codec=codec or FileIdCodec(TEST_FILE_ID_SECRET),
    )


def test_minimal_validation_checks_only_sheet_names_and_resets_stream() -> None:
    source = io.BytesIO(
        _workbook_bytes(
            REQUIRED_TRAUPIXE_SHEETS
            | {
                "Unrelated calculation",
                "Any internal layout is accepted",
            }
        )
    )

    validate_traupixe_workbook(source)

    assert source.tell() == 0


def test_minimal_validation_rejects_a_missing_signature_sheet() -> None:
    source = io.BytesIO(_workbook_bytes(REQUIRED_TRAUPIXE_SHEETS - {"S_Best Det."}))

    with pytest.raises(TraupixeIncompatibleWorkbookError) as error:
        validate_traupixe_workbook(source)

    assert error.value.missing_sheets == {"S_Best Det."}


def test_minimal_validation_rejects_an_unreadable_xlsx() -> None:
    with pytest.raises(TraupixeUnreadableError):
        validate_traupixe_workbook(io.BytesIO(b"not an xlsx"))


def test_detection_returns_empty_result_without_a_candidate() -> None:
    result = _detect(FakeDataClient())

    assert result.files == ()
    assert result.default_file_id is None


def test_detection_walks_run_data_and_filters_before_download() -> None:
    client = FakeDataClient()
    client.add_directory(
        "raw_data",
        name="nested",
        path="raw/nested",
    )
    client.add_file(
        "raw_data",
        folder="nested",
        name="arbitrary-name.xlsx",
        path="raw/nested/arbitrary-name.xlsx",
    )
    client.add_file(
        "processed_data",
        name="another.XLSX",
        path="processed/another.XLSX",
    )
    client.add_file(
        "raw_data",
        name="ignored.xls",
        path="raw/ignored.xls",
    )
    client.add_file(
        "raw_data",
        name="too-large.xlsx",
        path="raw/too-large.xlsx",
        size=MAX_SOURCE_SIZE_BYTES + 1,
    )
    client.add_file(
        "processed_data",
        name="corrupt.xlsx",
        path="processed/corrupt.xlsx",
        content=b"invalid",
    )
    client.add_file(
        "HDF5",
        name="outside-scope.xlsx",
        path="HDF5/outside-scope.xlsx",
    )

    result = _detect(client)

    assert {file.name for file in result.files} == {
        "arbitrary-name.xlsx",
        "another.XLSX",
    }
    assert set(client.downloaded_paths) == {
        "raw/nested/arbitrary-name.xlsx",
        "processed/another.XLSX",
        "processed/corrupt.xlsx",
    }


def test_detection_preselects_the_most_recent_candidate() -> None:
    client = FakeDataClient()
    client.add_file(
        "raw_data",
        name="older.xlsx",
        path="raw/older.xlsx",
        last_modified=NOW,
    )
    client.add_file(
        "processed_data",
        name="newer.xlsx",
        path="processed/newer.xlsx",
        last_modified=NOW + timedelta(minutes=1),
    )

    result = _detect(client)

    assert [file.name for file in result.files] == ["newer.xlsx", "older.xlsx"]
    assert result.default_file_id == result.files[0].file_id


@pytest.mark.parametrize(
    ("project_slug", "run_name"),
    [
        ("../other-project", "run-01"),
        ("project/other", "run-01"),
        ("project-01", ".."),
        ("project-01", r"other\run"),
        (" project-01", "run-01"),
        ("project-01", ""),
    ],
)
def test_detection_rejects_unsafe_storage_scope(
    project_slug: str,
    run_name: str,
) -> None:
    client = FakeDataClient()

    with pytest.raises(InvalidTraupixeScopeError):
        _detect(
            client,
            project_slug=project_slug,
            run_name=run_name,
        )

    assert client.list_calls == []


def test_file_id_is_opaque_authenticated_and_scoped() -> None:
    client = FakeDataClient()
    path = "projects/project-01/runs/run-01/raw_data/source.xlsx"
    client.add_file(
        "raw_data",
        name="source.xlsx",
        path=path,
    )
    client._entries(
        "raw_data",
        None,
        project_slug="project-02",
    ).append(
        ProjectFileOrDirectory(
            name="source.xlsx",
            last_modified=NOW,
            size=len(VALID_WORKBOOK),
            path=path,
            type="file",
        )
    )
    codec = FileIdCodec(TEST_FILE_ID_SECRET)

    original = _detect(client, codec=codec).files[0].file_id
    other_project = (
        _detect(
            client,
            project_slug="project-02",
            codec=codec,
        )
        .files[0]
        .file_id
    )
    tampered_index = len(original) // 2
    replacement = "A" if original[tampered_index] != "A" else "B"
    tampered = original[:tampered_index] + replacement + original[tampered_index + 1 :]

    assert original != other_project
    assert "source" not in original
    assert "project" not in original
    assert hashlib.sha256(VALID_WORKBOOK).hexdigest() not in original
    assert codec.decode(tampered) is None


def test_file_id_codec_requires_a_256_bit_secret() -> None:
    with pytest.raises(ValueError, match="at least 32 bytes"):
        FileIdCodec(b"s" * 31)


def test_resolution_returns_the_selected_raw_workbook() -> None:
    client = FakeDataClient()
    client.add_file(
        "raw_data",
        name="source.xlsx",
        path="raw/source.xlsx",
    )
    codec = FileIdCodec(TEST_FILE_ID_SECRET)
    file_id = _detect(client, codec=codec).files[0].file_id

    resolved = resolve_traupixe_workbook(
        client,  # type: ignore[arg-type]
        "project-01",
        "run-01",
        file_id,
        file_id_codec=codec,
    )

    assert isinstance(resolved, ResolvedTraupixeFile)
    assert resolved.name == "source.xlsx"
    assert resolved.source.read() == VALID_WORKBOOK
    resolved.source.close()


def test_resolution_rejects_a_file_id_from_another_project() -> None:
    client = FakeDataClient()
    client.add_file(
        "raw_data",
        name="source.xlsx",
        path="raw/source.xlsx",
    )
    codec = FileIdCodec(TEST_FILE_ID_SECRET)
    file_id = _detect(client, codec=codec).files[0].file_id

    with pytest.raises(TraupixeWorkbookNotFoundError):
        resolve_traupixe_workbook(
            client,  # type: ignore[arg-type]
            "project-02",
            "run-01",
            file_id,
            file_id_codec=codec,
        )


@pytest.mark.parametrize("change_metadata", [False, True])
def test_resolution_rejects_a_replaced_workbook(change_metadata: bool) -> None:
    client = FakeDataClient()
    path = "raw/source.xlsx"
    client.add_file(
        "raw_data",
        name="source.xlsx",
        path=path,
    )
    codec = FileIdCodec(TEST_FILE_ID_SECRET)
    file_id = _detect(client, codec=codec).files[0].file_id

    if change_metadata:
        entry = client._entries("raw_data", None)[0]
        client._entries("raw_data", None)[0] = entry.model_copy(
            update={"last_modified": NOW + timedelta(seconds=1)}
        )
    else:
        replacement = bytearray(VALID_WORKBOOK)
        replacement[-1] ^= 1
        client.contents[path] = bytes(replacement)

    with pytest.raises(TraupixeSourceChangedError):
        resolve_traupixe_workbook(
            client,  # type: ignore[arg-type]
            "project-01",
            "run-01",
            file_id,
            file_id_codec=codec,
        )


def test_actual_download_is_bounded_when_listed_size_is_stale(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(selection_module, "MAX_SOURCE_SIZE_BYTES", 8)
    client = FakeDataClient()
    client.add_file(
        "raw_data",
        name="source.xlsx",
        path="raw/source.xlsx",
        content=b"123456789",
        size=5,
    )

    assert _detect(client).files == ()


def test_custom_validator_may_be_used_without_consuming_the_stream() -> None:
    client = FakeDataClient()
    client.add_file(
        "raw_data",
        name="source.xlsx",
        path="raw/source.xlsx",
        content=b"custom",
    )

    def validator(source: BinaryIO) -> bool:
        return source.read() == b"custom"

    result = detect_traupixe_workbooks(
        client,  # type: ignore[arg-type]
        "project-01",
        "run-01",
        file_id_codec=FileIdCodec(TEST_FILE_ID_SECRET),
        validator=validator,
    )

    assert len(result.files) == 1
