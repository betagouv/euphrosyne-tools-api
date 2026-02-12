import io
from subprocess import CompletedProcess
from unittest import mock

import pytest

import data_lifecycle.azcopy_runner as azcopy_runner


class FakePopen:
    def __init__(self, stdout_text: str, stderr_text: str = "", returncode=None):
        self.stdout = io.StringIO(stdout_text)
        self.stderr = io.StringIO(stderr_text)
        self.returncode = returncode

    def poll(self):
        return self.returncode

    def terminate(self):
        self.returncode = -15

    def wait(self, timeout=None):
        if self.returncode is None:
            self.returncode = 0
        return self.returncode

    def kill(self):
        self.returncode = -9


def _azcopy_json_line(
    message_content: dict[str, object], message_type: str = "Info"
) -> str:
    return (
        azcopy_runner.json.dumps(
            {
                "TimeStamp": "2026-02-05T17:53:31Z",
                "MessageType": message_type,
                "MessageContent": azcopy_runner.json.dumps(message_content),
            }
        )
        + "\n"
    )


@mock.patch("data_lifecycle.azcopy_runner.subprocess.Popen")
def test_start_copy_extracts_job_id(mock_popen: mock.MagicMock, monkeypatch, tmp_path):
    stdout_text = _azcopy_json_line(
        {"JobID": "123e4567-e89b-12d3-a456-426614174000", "JobStatus": "InProgress"}
    )
    mock_popen.return_value = FakePopen(stdout_text)

    monkeypatch.setenv("AZCOPY_WORK_DIR", str(tmp_path))
    monkeypatch.setenv("AZCOPY_LOG_DIR", str(tmp_path))

    job_ref = azcopy_runner.start_copy("source", "dest")

    assert job_ref.job_id == "123e4567-e89b-12d3-a456-426614174000"
    assert job_ref.log_dir == str(tmp_path)
    assert job_ref.command[0].endswith("azcopy")


@mock.patch("data_lifecycle.azcopy_runner.subprocess.Popen")
def test_start_copy_extracts_job_id_from_json_message(
    mock_popen: mock.MagicMock, monkeypatch, tmp_path
):
    stdout_text = _azcopy_json_line(
        {
            "JobID": "123e4567-e89b-12d3-a456-426614174001",
            "JobStatus": "InProgress",
            "LogLevel": "INFO",
        }
    )
    mock_popen.return_value = FakePopen(stdout_text)

    monkeypatch.setenv("AZCOPY_WORK_DIR", str(tmp_path))
    monkeypatch.setenv("AZCOPY_LOG_DIR", str(tmp_path))

    job_ref = azcopy_runner.start_copy("source", "dest")

    assert job_ref.job_id == "123e4567-e89b-12d3-a456-426614174001"


@mock.patch("data_lifecycle.azcopy_runner.subprocess.Popen")
def test_start_copy_missing_job_id_raises(
    mock_popen: mock.MagicMock, monkeypatch, tmp_path
):
    stdout_text = _azcopy_json_line({"JobStatus": "InProgress"})
    mock_popen.return_value = FakePopen(stdout_text)

    monkeypatch.setenv("AZCOPY_WORK_DIR", str(tmp_path))
    monkeypatch.setenv("AZCOPY_LOG_DIR", str(tmp_path))
    monkeypatch.setattr(azcopy_runner, "_JOB_ID_INTERVAL_SECONDS", 0)
    monkeypatch.setattr(azcopy_runner, "_JOB_ID_RETRY_TIMES", 1)

    with pytest.raises(azcopy_runner.AzCopyJobIdNotFoundError):
        azcopy_runner.start_copy("source", "dest")


@mock.patch("data_lifecycle.azcopy_runner.subprocess.run")
def test_poll_parses_json_state(mock_run: mock.MagicMock):
    payload = {
        "JobID": "job-1",
        "JobStatus": "InProgress",
        "TransfersCompleted": 1,
        "TotalBytesTransferred": 10,
        "TransfersFailed": 0,
        "TransfersSkipped": 0,
        "TotalTransfers": 10,
        "TotalBytesExpected": 100,
        "PercentComplete": 10.0,
    }
    mock_run.return_value = CompletedProcess(
        args=["azcopy", "jobs", "show", "job-1"],
        returncode=0,
        stdout=_azcopy_json_line(payload),
        stderr="",
    )

    summary = azcopy_runner.poll("job-1")

    assert summary.state == "RUNNING"
    assert summary.files_total == 10
    assert summary.bytes_total == 100
    assert summary.progress_percent == 10.0


@mock.patch("data_lifecycle.azcopy_runner.subprocess.run")
def test_poll_failed_transfers_marks_failed(mock_run: mock.MagicMock):
    payload = {
        "JobID": "job-2",
        "JobStatus": "Completed",
        "TransfersCompleted": 8,
        "TotalBytesTransferred": 80,
        "TransfersFailed": 2,
        "TransfersSkipped": 0,
        "TotalTransfers": 10,
        "TotalBytesExpected": 100,
        "PercentComplete": 80.0,
    }
    mock_run.return_value = CompletedProcess(
        args=["azcopy", "jobs", "show", "job-2"],
        returncode=0,
        stdout=_azcopy_json_line(payload),
        stderr="",
    )

    summary = azcopy_runner.poll("job-2")

    assert summary.state == "FAILED"


@mock.patch("data_lifecycle.azcopy_runner.subprocess.run")
def test_poll_unknown_status(mock_run: mock.MagicMock):
    payload = {
        "JobID": "job-3",
        "JobStatus": "Mystery",
        "TransfersCompleted": 0,
        "TotalBytesTransferred": 0,
        "TransfersFailed": 0,
        "TransfersSkipped": 0,
        "TotalTransfers": 10,
        "TotalBytesExpected": 100,
        "PercentComplete": 0.0,
    }
    mock_run.return_value = CompletedProcess(
        args=["azcopy", "jobs", "show", "job-3"],
        returncode=0,
        stdout=_azcopy_json_line(payload),
        stderr="",
    )

    summary = azcopy_runner.poll("job-3")

    assert summary.state == "UNKNOWN"


@mock.patch("data_lifecycle.azcopy_runner.subprocess.run")
def test_get_summary_parses_bytes_and_files(mock_run: mock.MagicMock):
    payload = {
        "JobID": "job-4",
        "JobStatus": "Completed",
        "TransfersCompleted": 10,
        "TotalBytesTransferred": 2048,
        "TransfersFailed": 0,
        "TransfersSkipped": 1,
        "TotalTransfers": 10,
        "TotalBytesExpected": 2048,
        "PercentComplete": 100.0,
    }
    mock_run.return_value = CompletedProcess(
        args=["azcopy", "jobs", "show", "job-4"],
        returncode=0,
        stdout=_azcopy_json_line(payload),
        stderr="",
    )

    summary = azcopy_runner.get_summary("job-4")

    assert summary.state == "SUCCEEDED"
    assert summary.files_transferred == 10
    assert summary.bytes_transferred == 2048
    assert summary.skipped_transfers == 1
    assert summary.files_total == 10
    assert summary.bytes_total == 2048
    assert summary.progress_percent == 100.0


@mock.patch("data_lifecycle.azcopy_runner.subprocess.run")
def test_get_summary_running_returns(mock_run: mock.MagicMock):
    payload = {
        "JobID": "job-5",
        "JobStatus": "InProgress",
        "TransfersCompleted": 0,
        "TotalBytesTransferred": 0,
        "TransfersFailed": 0,
        "TransfersSkipped": 0,
        "TotalTransfers": 3,
        "TotalBytesExpected": 9,
        "PercentComplete": 0.0,
    }
    mock_run.return_value = CompletedProcess(
        args=["azcopy", "jobs", "show", "job-5"],
        returncode=0,
        stdout=_azcopy_json_line(payload),
        stderr="",
    )

    summary = azcopy_runner.get_summary("job-5")

    assert summary.state == "RUNNING"


@mock.patch("data_lifecycle.azcopy_runner.subprocess.run")
def test_poll_parses_progress_and_totals(mock_run: mock.MagicMock):
    payload = {
        "JobID": "job-8",
        "JobStatus": "InProgress",
        "TransfersCompleted": 4,
        "TotalBytesTransferred": 250,
        "TransfersFailed": 0,
        "TransfersSkipped": 1,
        "TotalTransfers": 10,
        "TotalBytesExpected": 1000,
        "PercentComplete": 25.0,
    }
    mock_run.return_value = CompletedProcess(
        args=["azcopy", "jobs", "show", "job-8"],
        returncode=0,
        stdout=_azcopy_json_line(payload),
        stderr="",
    )

    summary = azcopy_runner.poll("job-8")

    assert summary.state == "RUNNING"
    assert summary.files_total == 10
    assert summary.files_transferred == 4
    assert summary.bytes_total == 1000
    assert summary.bytes_transferred == 250
    assert summary.failed_transfers == 0
    assert summary.skipped_transfers == 1
    assert summary.progress_percent == 25.0


@mock.patch("data_lifecycle.azcopy_runner.subprocess.run")
def test_poll_job_not_found_error_detection(mock_run: mock.MagicMock):
    mock_run.return_value = CompletedProcess(
        args=["azcopy", "jobs", "show", "job-6"],
        returncode=1,
        stdout="",
        stderr="No job with JobId abc-def",
    )

    with pytest.raises(azcopy_runner.AzCopyJobNotFoundError):
        azcopy_runner.poll("job-6")


@mock.patch("data_lifecycle.azcopy_runner.subprocess.run")
def test_poll_generic_not_found_is_parse_error(mock_run: mock.MagicMock):
    mock_run.return_value = CompletedProcess(
        args=["azcopy", "jobs", "show", "job-7"],
        returncode=1,
        stdout="",
        stderr="Resource not found",
    )

    with pytest.raises(azcopy_runner.AzCopyParseError):
        azcopy_runner.poll("job-7")
