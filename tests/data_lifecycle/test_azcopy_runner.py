import io
from subprocess import CompletedProcess

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


def test_start_copy_extracts_job_id(monkeypatch, tmp_path):
    stdout_text = "INFO: JobID: 123e4567-e89b-12d3-a456-426614174000\n"

    def fake_popen(*_args, **_kwargs):
        return FakePopen(stdout_text)

    monkeypatch.setenv("AZCOPY_WORK_DIR", str(tmp_path))
    monkeypatch.setenv("AZCOPY_LOG_DIR", str(tmp_path))
    monkeypatch.setattr(azcopy_runner.subprocess, "Popen", fake_popen)

    job_ref = azcopy_runner.start_copy("source", "dest")

    assert job_ref.job_id == "123e4567-e89b-12d3-a456-426614174000"
    assert job_ref.log_dir == str(tmp_path)
    assert job_ref.command[0].endswith("azcopy")


def test_start_copy_extracts_job_id_from_json_message(monkeypatch, tmp_path):
    stdout_text = (
        '{"TimeStamp":"2026-02-05T17:53:31Z","MessageType":"Info",'
        '"MessageContent":"INFO: Job 123e4567-e89b-12d3-a456-426614174001 has started"}\n'
    )

    def fake_popen(*_args, **_kwargs):
        return FakePopen(stdout_text)

    monkeypatch.setenv("AZCOPY_WORK_DIR", str(tmp_path))
    monkeypatch.setenv("AZCOPY_LOG_DIR", str(tmp_path))
    monkeypatch.setattr(azcopy_runner.subprocess, "Popen", fake_popen)

    job_ref = azcopy_runner.start_copy("source", "dest")

    assert job_ref.job_id == "123e4567-e89b-12d3-a456-426614174001"


def test_start_copy_missing_job_id_raises(monkeypatch, tmp_path):
    stdout_text = "INFO: starting\n"

    def fake_popen(*_args, **_kwargs):
        return FakePopen(stdout_text)

    monkeypatch.setenv("AZCOPY_WORK_DIR", str(tmp_path))
    monkeypatch.setenv("AZCOPY_LOG_DIR", str(tmp_path))
    monkeypatch.setattr(azcopy_runner.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(azcopy_runner, "_JOB_ID_INTERVAL_SECONDS", 0)
    monkeypatch.setattr(azcopy_runner, "_JOB_ID_RETRY_TIMES", 1)

    with pytest.raises(azcopy_runner.AzCopyJobIdNotFoundError):
        azcopy_runner.start_copy("source", "dest")


def test_poll_parses_json_state(monkeypatch):
    payload = {
        "JobID": "job-1",
        "JobStatus": "InProgress",
    }

    def fake_run(*_args, **_kwargs):
        return CompletedProcess(
            _args, 0, stdout=azcopy_runner.json.dumps(payload), stderr=""
        )

    monkeypatch.setattr(azcopy_runner.subprocess, "run", fake_run)

    progress = azcopy_runner.poll("job-1")

    assert progress.state == "RUNNING"
    assert progress.raw_status == "InProgress"


def test_poll_failed_transfers_marks_failed(monkeypatch):
    payload = {
        "JobID": "job-2",
        "JobStatus": "Completed",
        "Summary": {"TotalFilesFailed": 2},
    }

    def fake_run(*_args, **_kwargs):
        return CompletedProcess(
            _args, 0, stdout=azcopy_runner.json.dumps(payload), stderr=""
        )

    monkeypatch.setattr(azcopy_runner.subprocess, "run", fake_run)

    progress = azcopy_runner.poll("job-2")

    assert progress.state == "FAILED"


def test_poll_unknown_status(monkeypatch):
    payload = {
        "JobID": "job-3",
        "JobStatus": "Mystery",
    }

    def fake_run(*_args, **_kwargs):
        return CompletedProcess(
            _args, 0, stdout=azcopy_runner.json.dumps(payload), stderr=""
        )

    monkeypatch.setattr(azcopy_runner.subprocess, "run", fake_run)

    progress = azcopy_runner.poll("job-3")

    assert progress.state == "UNKNOWN"


def test_get_summary_parses_bytes_and_files(monkeypatch):
    payload = {
        "JobID": "job-4",
        "JobStatus": "Completed",
        "Summary": {
            "TotalFilesTransferred": 10,
            "TotalBytesTransferred": 2048,
            "TotalFilesFailed": 0,
            "TotalFilesSkipped": 1,
        },
    }

    def fake_run(*_args, **_kwargs):
        return CompletedProcess(
            _args, 0, stdout=azcopy_runner.json.dumps(payload), stderr=""
        )

    monkeypatch.setattr(azcopy_runner.subprocess, "run", fake_run)

    summary = azcopy_runner.get_summary("job-4")

    assert summary.state == "SUCCEEDED"
    assert summary.files_transferred == 10
    assert summary.bytes_transferred == 2048
    assert summary.skipped_transfers == 1


def test_get_summary_running_returns(monkeypatch):
    payload = {
        "JobID": "job-5",
        "JobStatus": "InProgress",
    }

    def fake_run(*_args, **_kwargs):
        return CompletedProcess(
            _args, 0, stdout=azcopy_runner.json.dumps(payload), stderr=""
        )

    monkeypatch.setattr(azcopy_runner.subprocess, "run", fake_run)

    summary = azcopy_runner.get_summary("job-5")

    assert summary.state == "RUNNING"
