from pathlib import Path

import pytest

from path import (
    IncorrectDataFilePath,
    ProjectDocumentRef,
    ProjectRef,
    RunDataTypeRef,
    RunRef,
)


@pytest.mark.parametrize(
    ("path", "expected_slug"),
    [
        (Path("projects/project-01"), "project-01"),
        (Path("projects/Project Name"), "Project Name"),
        (Path("projects/project-01/runs/run-01"), "project-01"),
    ],
)
def test_project_ref_from_path(
    monkeypatch: pytest.MonkeyPatch, path: Path, expected_slug: str
):
    monkeypatch.setenv("DATA_PROJECTS_LOCATION_PREFIX", "projects")

    assert ProjectRef.from_path(path) == ProjectRef(project_slug=expected_slug)


@pytest.mark.parametrize(
    "path",
    [
        Path("invalid-prefix/project-01"),
    ],
)
def test_project_ref_from_path_raises_for_invalid_path(
    monkeypatch: pytest.MonkeyPatch, path: Path
):
    monkeypatch.setenv("DATA_PROJECTS_LOCATION_PREFIX", "projects")

    with pytest.raises(IncorrectDataFilePath) as error:
        ProjectRef.from_path(path)

    assert (
        error.value.message == "path must be like {projects_path_prefix}/<project_slug>"
    )


@pytest.mark.parametrize(
    ("path", "expected_project_slug", "expected_run_name"),
    [
        (
            Path("projects/project-01/runs/run-01"),
            "project-01",
            "run-01",
        ),
        (
            Path("projects/Project Name/runs/Run Name"),
            "Project Name",
            "Run Name",
        ),
    ],
)
def test_run_ref_from_path(
    monkeypatch: pytest.MonkeyPatch,
    path: Path,
    expected_project_slug: str,
    expected_run_name: str,
):
    monkeypatch.setenv("DATA_PROJECTS_LOCATION_PREFIX", "projects")

    assert RunRef.from_path(path) == RunRef(
        project_slug=expected_project_slug,
        run_name=expected_run_name,
    )


@pytest.mark.parametrize(
    "path",
    [
        Path("projects/project-01"),
        Path("projects/project-01/data/run-01"),
    ],
)
def test_run_ref_from_path_raises_for_invalid_path(
    monkeypatch: pytest.MonkeyPatch, path: Path
):
    monkeypatch.setenv("DATA_PROJECTS_LOCATION_PREFIX", "projects")

    with pytest.raises(IncorrectDataFilePath) as error:
        RunRef.from_path(path)

    assert (
        error.value.message
        == "path must be like {projects_path_prefix}/<project_slug>/runs/<run_name>"
    )


@pytest.mark.parametrize("data_type", ["raw_data", "processed_data", "HDF5"])
def test_run_data_type_ref_from_directory_path(
    monkeypatch: pytest.MonkeyPatch, data_type: str
):
    monkeypatch.setenv("DATA_PROJECTS_LOCATION_PREFIX", "projects")

    path = Path(f"projects/project-01/runs/run-01/{data_type}")

    assert RunDataTypeRef.from_path(path) == RunDataTypeRef(
        project_slug="project-01",
        run_name="run-01",
        data_type=data_type,  # type: ignore
    )


@pytest.mark.parametrize("data_type", ["raw_data", "processed_data", "HDF5"])
def test_run_data_type_ref_from_nested_file_path(
    monkeypatch: pytest.MonkeyPatch, data_type: str
):
    monkeypatch.setenv("DATA_PROJECTS_LOCATION_PREFIX", "projects")

    path = Path(f"projects/project-01/runs/run-01/{data_type}/nested/file.txt")

    assert RunDataTypeRef.from_path(path) == RunDataTypeRef(
        project_slug="project-01",
        run_name="run-01",
        data_type=data_type,  # type: ignore
    )


@pytest.mark.parametrize(
    "path",
    [
        Path("projects/project-01/runs/run-01"),
        Path("projects/project-01/runs/run-01/other_data"),
    ],
)
def test_run_data_type_ref_from_path_raises_for_invalid_path(
    monkeypatch: pytest.MonkeyPatch, path: Path
):
    monkeypatch.setenv("DATA_PROJECTS_LOCATION_PREFIX", "projects")

    with pytest.raises(IncorrectDataFilePath) as error:
        RunDataTypeRef.from_path(path)

    assert error.value.message == (
        "path must start with {projects_path_prefix}/<project_slug>/runs/"
        "<run_name>/(processed_data|raw_data|HDF5)/"
    )


@pytest.mark.parametrize(
    "path",
    [
        Path("projects/project-01/documents/document.pdf"),
        Path("projects/project-01/documents/subdir/document.pdf"),
    ],
)
def test_project_document_ref_from_path(monkeypatch: pytest.MonkeyPatch, path: Path):
    monkeypatch.setenv("DATA_PROJECTS_LOCATION_PREFIX", "projects")

    assert ProjectDocumentRef.from_path(path) == ProjectDocumentRef(
        project_slug="project-01"
    )


def test_project_document_ref_from_path_raises_for_invalid_path(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("DATA_PROJECTS_LOCATION_PREFIX", "projects")

    with pytest.raises(IncorrectDataFilePath) as error:
        ProjectDocumentRef.from_path(Path("projects/project-01/run-data/file.txt"))

    assert error.value.message == (
        "path must start with {projects_path_prefix}/<project_slug>/documents/"
    )
