import os

import pytest

from clients.local_python import LocalPythonExecutionError, LocalPythonSessionsClient


def test_executes_code_with_uploaded_file_and_returns_generated_result() -> None:
    client = LocalPythonSessionsClient()
    client.upload_file("session", "TRAUPIXE.xlsx", b"workbook")
    client.upload_file("session", "traupixe_data.json", b"{}")

    execution = client.execute(
        "session",
        """
from pathlib import Path
print(Path("TRAUPIXE.xlsx").read_bytes().decode())
Path("analysis_result.json").write_text("{}")
""",
    )

    assert execution.status == "Succeeded"
    assert execution.stdout == "workbook\n"
    assert execution.stderr == ""
    assert {file["name"] for file in client.list_files("session")} == {
        "TRAUPIXE.xlsx",
        "analysis_result.json",
        "traupixe_data.json",
    }
    assert client.download_file("session", "analysis_result.json") == b"{}"

    client.delete_session("session")
    with pytest.raises(LocalPythonExecutionError, match="does not exist"):
        client.list_files("session")


def test_reports_python_failure_to_albert() -> None:
    client = LocalPythonSessionsClient()
    client.upload_file("session", "TRAUPIXE.xlsx", b"workbook")

    execution = client.execute("session", "raise ValueError('invalid sheet')")

    assert execution.status == "Failed"
    assert "ValueError: invalid sheet" in execution.stderr
    client.delete_session("session")


def test_stops_execution_after_timeout() -> None:
    client = LocalPythonSessionsClient(execution_timeout_seconds=0.01)
    client.upload_file("session", "TRAUPIXE.xlsx", b"workbook")

    execution = client.execute("session", "import time; time.sleep(1)")

    assert execution.status == "Failed"
    assert "timed out" in execution.stderr
    client.delete_session("session")


def test_passes_only_required_environment_to_generated_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ALBERT_API_KEY", "secret")
    monkeypatch.setenv("DATABASE_URL", "secret")
    monkeypatch.setenv("PATH", "/test/bin")
    client = LocalPythonSessionsClient()
    client.upload_file("session", "TRAUPIXE.xlsx", b"workbook")

    execution = client.execute(
        "session",
        (
            "import os; print("
            "os.getenv('ALBERT_API_KEY'), "
            "os.getenv('DATABASE_URL'), "
            "os.getenv('PATH'))"
        ),
    )

    assert execution.status == "Succeeded"
    assert execution.stdout == "None None /test/bin\n"
    assert os.environ["ALBERT_API_KEY"] == "secret"
    client.delete_session("session")
