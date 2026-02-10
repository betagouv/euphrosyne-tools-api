from __future__ import annotations

import json
import os
import subprocess
import tempfile
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Literal
from urllib.parse import urlsplit, urlunsplit

AzCopyJobState = Literal[
    "PENDING", "RUNNING", "SUCCEEDED", "FAILED", "CANCELED", "UNKNOWN"
]

_DEFAULT_AZCOPY_PATH = "azcopy"
_DEFAULT_AZCOPY_WORK_DIR = "/app/.azcopy"
_FALLBACK_AZCOPY_WORK_DIR = "/tmp/.azcopy"
_DEFAULT_OUTPUT_TYPE = "json"
_DEFAULT_LOG_LEVEL = "INFO"
_JOB_ID_INTERVAL_SECONDS = 10.0
_JOB_ID_RETRY_TIMES = 8


_TERMINAL_STATES: set[AzCopyJobState] = {
    "SUCCEEDED",
    "FAILED",
    "CANCELED",
    "UNKNOWN",
}

_RUNNING_STATUS = {
    "running",
    "inprogress",
    "in progress",
    "progress",
    "started",
    "cancelling",
    "canceling",
    "paused",
    "resuming",
    "resumed",
}

_PENDING_STATUS = {"queued", "pending", "new"}

_SUCCESS_STATUS = {
    "completed",
    "completedsuccessfully",
    "success",
    "succeeded",
}

_FAILED_STATUS = {
    "failed",
    "completedwitherrors",
    "completed with errors",
    "error",
}

_CANCELED_STATUS = {"cancelled", "canceled", "canceling", "cancelling"}


@dataclass(frozen=True)
class AzCopyJobRef:
    job_id: str
    started_at: datetime
    command: list[str]
    environment: dict[str, str]
    log_dir: str


@dataclass(frozen=True)
class AzCopyProgress:
    state: AzCopyJobState
    last_updated_at: datetime
    raw_status: str


@dataclass(frozen=True)
class AzCopySummary:
    state: AzCopyJobState
    files_transferred: int
    bytes_transferred: int
    failed_transfers: int
    skipped_transfers: int
    stdout_log_path: str
    stderr_log_path: str


@dataclass(frozen=True)
class AzCopyCopyOptions:
    recursive: bool = True
    overwrite: str = "true"
    from_to: str | None = None
    log_level: str | None = None
    output_type: Literal["json", "text"] = "json"
    extra_args: list[str] | None = None


class AzCopyRunnerError(Exception):
    def __init__(
        self,
        message: str,
        *,
        job_id: str | None,
        log_dir: str | None,
        stdout_path: str | None = None,
        stdout_excerpt: str | None = None,
        stderr_path: str | None = None,
        stderr_excerpt: str | None = None,
    ) -> None:
        safe_message = (
            f"{message} job_id={job_id!r} log_dir={log_dir!r} "
            f"stdout_excerpt={stdout_excerpt!r} stderr_excerpt={stderr_excerpt!r}"
        )
        if stdout_path:
            safe_message += f" stdout_path={stdout_path!r}"
        if stderr_path:
            safe_message += f" stderr_path={stderr_path!r}"
        super().__init__(safe_message)
        self.job_id = job_id
        self.log_dir = log_dir
        self.stdout_path = stdout_path
        self.stdout_excerpt = stdout_excerpt
        self.stderr_path = stderr_path
        self.stderr_excerpt = stderr_excerpt


class AzCopyNotInstalledError(AzCopyRunnerError):
    pass


class AzCopyStartError(AzCopyRunnerError):
    pass


class AzCopyJobIdNotFoundError(AzCopyRunnerError):
    pass


class AzCopyJobNotFoundError(AzCopyRunnerError):
    pass


class AzCopyParseError(AzCopyRunnerError):
    pass


class AzCopyPermissionError(Exception):
    pass


def start_copy(
    source_uri: str, dest_uri: str, *, options: AzCopyCopyOptions | None = None
) -> AzCopyJobRef:
    """Start an AzCopy copy job and return a job reference.

    Logic:
    - Spawn `azcopy copy ...` via `Popen` so the process can keep running after
      we return.
    - Stream stdout/stderr to local files (best-effort) and parse early output
      to extract the AzCopy `job_id`.
    - Return `AzCopyJobRef` with a redacted command/env snapshot for audit/debug.
    """
    resolved_options = options or AzCopyCopyOptions()
    azcopy_path = os.getenv("AZCOPY_PATH", _DEFAULT_AZCOPY_PATH)
    log_dir = _resolve_log_dir()
    work_env = _build_azcopy_env()
    command = _build_copy_command(
        azcopy_path=azcopy_path,
        source_uri=source_uri,
        dest_uri=dest_uri,
        options=resolved_options,
    )

    started_at = datetime.now(timezone.utc)
    stdout_path, stderr_path = _create_log_files(log_dir, started_at)

    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=work_env,
        )
    except FileNotFoundError as exc:
        raise AzCopyNotInstalledError(
            "AzCopy executable not found",
            job_id=None,
            log_dir=log_dir,
            stdout_excerpt=None,
            stderr_excerpt=str(exc),
        ) from exc
    except OSError as exc:
        raise AzCopyStartError(
            "Failed to start AzCopy",
            job_id=None,
            log_dir=log_dir,
            stdout_excerpt=None,
            stderr_excerpt=str(exc),
        ) from exc

    job_id_holder: dict[str, str | None] = {"job_id": None}
    job_id_event = threading.Event()

    def capture_job_id(line: str) -> None:
        if job_id_event.is_set():
            return
        extracted = _extract_job_id(line)
        if extracted:
            job_id_holder["job_id"] = extracted
            job_id_event.set()

    stdout_thread = threading.Thread(
        target=_drain_stream,
        args=(process.stdout, stdout_path, capture_job_id),
        daemon=True,
    )
    stderr_thread = threading.Thread(
        target=_drain_stream,
        args=(process.stderr, stderr_path, capture_job_id),
        daemon=True,
    )
    stdout_thread.start()
    stderr_thread.start()

    stdout_tail = None
    stderr_tail = None

    job_id_retry_count = 0
    while job_id_retry_count < _JOB_ID_RETRY_TIMES:

        job_id_event.wait(timeout=_JOB_ID_INTERVAL_SECONDS)

        job_id = job_id_holder["job_id"]
        if job_id:
            # Best-effort rename log files to include job id for easier debugging.
            _rename_log_files(stdout_path, stderr_path, job_id)
            return AzCopyJobRef(
                job_id=job_id,
                started_at=started_at,
                command=_redact_command(command),
                environment=_safe_env_subset(work_env),
                log_dir=log_dir,
            )

        # Did not find job id within the retry limit.
        # Let's figureout what went wrong by reading excerpts from the logs.

        stdout_tail_lines = _read_tail_lines(stdout_path)
        stderr_tail_lines = _read_tail_lines(stderr_path)

        stdout_tail = "\n".join(stdout_tail_lines)
        stderr_tail = "\n".join(stderr_tail_lines)

        if stdout_tail_lines:
            last_stdout_line = stdout_tail_lines[-1]
            try:
                raise_for_error_in_line_log(last_stdout_line)
            except AzCopyPermissionError as exc:
                raise AzCopyStartError(
                    "AzCopy permission error",
                    job_id=None,
                    log_dir=log_dir,
                    stdout_path=stdout_path,
                    stdout_excerpt=stdout_tail,
                    stderr_path=stderr_path,
                    stderr_excerpt=stderr_tail,
                ) from exc

        # If the process has already exited and we haven't captured a job id, it's likely
        # that AzCopy failed before it could emit the job id. In this case, we can provide
        # more helpful error messages by including excerpts from the logs.
        if process.poll() is not None and process.returncode not in (0, None):
            raise AzCopyStartError(
                "AzCopy exited before providing a job id",
                job_id=None,
                log_dir=log_dir,
                stdout_path=stdout_path,
                stdout_excerpt=stdout_tail,
                stderr_path=stderr_path,
                stderr_excerpt=stderr_tail,
            )

        job_id_retry_count += 1

    # If we didn't capture a job id but the process is still running, it's possible that
    # AzCopy is still starting up and hasn't emitted the job id yet. However, since
    # we have a timeout for capturing the job id, we'll assume that something went wrong
    # and terminate the process to avoid leaving orphaned AzCopy processes running.
    _terminate_process(process)
    raise AzCopyJobIdNotFoundError(
        "AzCopy job id not found in output",
        job_id=None,
        log_dir=log_dir,
        stdout_path=stdout_path,
        stdout_excerpt=stdout_tail,
        stderr_path=stderr_path,
        stderr_excerpt=stderr_tail,
    )


def poll(job_id: str) -> AzCopyProgress:
    """Poll an AzCopy job without blocking."""
    result = _run_azcopy_jobs_show(job_id, output_type="json")
    raw_status, summary_data = _parse_jobs_show_output(
        job_id, result.stdout, result.stderr
    )
    failed_transfers = _extract_failed_transfers(summary_data)
    state = _map_status(raw_status, failed_transfers)
    return AzCopyProgress(
        state=state,
        last_updated_at=datetime.now(timezone.utc),
        raw_status=raw_status or "UNKNOWN",
    )


def get_summary(job_id: str) -> AzCopySummary:
    """Return the final summary for a completed AzCopy job."""
    result = _run_azcopy_jobs_show(job_id, output_type="json")
    raw_status, summary_data = _parse_jobs_show_output(
        job_id, result.stdout, result.stderr
    )
    failed_transfers = _extract_failed_transfers(summary_data)
    state = _map_status(raw_status, failed_transfers)

    stdout_log_path, stderr_log_path = _resolve_log_files(job_id)

    parsed = _parse_summary_data(summary_data)
    if state not in _TERMINAL_STATES:
        return AzCopySummary(
            state="RUNNING",
            files_transferred=parsed["files_transferred"],
            bytes_transferred=parsed["bytes_transferred"],
            failed_transfers=parsed["failed_transfers"],
            skipped_transfers=parsed["skipped_transfers"],
            stdout_log_path=stdout_log_path,
            stderr_log_path=stderr_log_path,
        )

    return AzCopySummary(
        state=state,
        files_transferred=parsed["files_transferred"],
        bytes_transferred=parsed["bytes_transferred"],
        failed_transfers=parsed["failed_transfers"],
        skipped_transfers=parsed["skipped_transfers"],
        stdout_log_path=stdout_log_path,
        stderr_log_path=stderr_log_path,
    )


def _run_azcopy_jobs_show(
    job_id: str, *, output_type: str
) -> subprocess.CompletedProcess:
    azcopy_path = os.getenv("AZCOPY_PATH", _DEFAULT_AZCOPY_PATH)
    command = [
        azcopy_path,
        "jobs",
        "show",
        job_id,
        f"--output-type={output_type}",
    ]
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            env=_build_azcopy_env(),
        )
    except FileNotFoundError as exc:
        raise AzCopyNotInstalledError(
            "AzCopy executable not found",
            job_id=job_id,
            log_dir=_resolve_log_dir(),
            stdout_excerpt=None,
            stderr_excerpt=str(exc),
        ) from exc

    if result.returncode != 0:
        if _is_job_not_found(result.stdout, result.stderr):
            raise AzCopyJobNotFoundError(
                "AzCopy job not found",
                job_id=job_id,
                log_dir=_resolve_log_dir(),
                stdout_excerpt=result.stdout,
                stderr_excerpt=result.stderr,
            )
        raise AzCopyParseError(
            "AzCopy job status query failed",
            job_id=job_id,
            log_dir=_resolve_log_dir(),
            stdout_excerpt=result.stdout,
            stderr_excerpt=result.stderr,
        )
    return result


def _parse_jobs_show_output(
    job_id: str, stdout: str, stderr: str
) -> tuple[str, dict[str, Any] | str]:
    lines = stdout.splitlines()
    if lines:
        payload = json.loads(lines[0])
        if payload is None and stderr:
            payload = json.loads(stderr)

        if payload is not None:
            job_data = json.loads(payload["MessageContent"])
            raw_status = job_data.get("JobStatus")
            return raw_status, job_data

    raise AzCopyParseError(
        "Unable to parse AzCopy job status",
        job_id=job_id,
        log_dir=_resolve_log_dir(),
        stdout_excerpt=stdout,
        stderr_excerpt=stderr,
    )


def _build_copy_command(
    *,
    azcopy_path: str,
    source_uri: str,
    dest_uri: str,
    options: AzCopyCopyOptions,
) -> list[str]:
    resolved_log_level = options.log_level or os.getenv(
        "AZCOPY_DEFAULT_LOG_LEVEL", _DEFAULT_LOG_LEVEL
    )
    output_type = options.output_type or _DEFAULT_OUTPUT_TYPE
    extra_args = options.extra_args or []
    command = [
        azcopy_path,
        "copy",
        source_uri,
        dest_uri,
        f"--recursive={'true' if options.recursive else 'false'}",
        f"--overwrite={options.overwrite}",
        f"--log-level={resolved_log_level}",
        f"--output-type={output_type}",
    ]
    if options.from_to:
        command.append(f"--from-to={options.from_to}")
    if extra_args:
        command.extend(extra_args)
    return command


def _build_azcopy_env() -> dict[str, str]:
    env = dict(os.environ)
    work_dir = _resolve_work_dir()
    if work_dir:
        env["AZCOPY_LOG_LOCATION"] = work_dir
        env["AZCOPY_JOB_PLAN_LOCATION"] = work_dir
    return env


def _resolve_work_dir() -> str | None:
    configured = os.getenv("AZCOPY_WORK_DIR")
    candidate = configured or _DEFAULT_AZCOPY_WORK_DIR
    if _ensure_dir(candidate):
        return candidate
    if configured:
        return None
    if _ensure_dir(_FALLBACK_AZCOPY_WORK_DIR):
        return _FALLBACK_AZCOPY_WORK_DIR
    return None


def _resolve_log_dir() -> str:
    configured = os.getenv("AZCOPY_LOG_DIR")
    if configured and _ensure_dir(configured):
        return configured
    work_dir = _resolve_work_dir()
    if work_dir and _ensure_dir(work_dir):
        return work_dir
    temp_dir = Path(tempfile.gettempdir()) / ".azcopy"
    _ensure_dir(str(temp_dir))
    return str(temp_dir)


def _resolve_log_files(job_id: str) -> tuple[str, str]:
    log_dir = _resolve_log_dir()
    stdout_path = os.path.join(log_dir, f"azcopy-{job_id}-stdout.log")
    stderr_path = os.path.join(log_dir, f"azcopy-{job_id}-stderr.log")
    return stdout_path, stderr_path


def _ensure_dir(path: str) -> bool:
    try:
        Path(path).mkdir(parents=True, exist_ok=True)
    except OSError:
        return False
    return os.access(path, os.W_OK)


def _create_log_files(log_dir: str, started_at: datetime) -> tuple[str, str]:
    safe_timestamp = started_at.strftime("%Y%m%dT%H%M%SZ")
    stdout_path = os.path.join(log_dir, f"azcopy-{safe_timestamp}-stdout.log")
    stderr_path = os.path.join(log_dir, f"azcopy-{safe_timestamp}-stderr.log")
    for path in (stdout_path, stderr_path):
        _touch_file(path)
    return stdout_path, stderr_path


def _rename_log_files(stdout_path: str, stderr_path: str, job_id: str) -> None:
    """Best-effort rename to per-job filenames for easier debugging."""
    for path in (stdout_path, stderr_path):
        if not os.path.exists(path):
            continue
        suffix = "stdout" if "stdout" in path else "stderr"
        new_path = os.path.join(os.path.dirname(path), f"azcopy-{job_id}-{suffix}.log")
        try:
            os.replace(path, new_path)
        except OSError:
            continue


def _touch_file(path: str) -> None:
    fd = os.open(path, os.O_CREAT | os.O_WRONLY, 0o600)
    os.close(fd)


def _drain_stream(
    stream: Any, path: str, capture_callback: Callable[[str], None]
) -> None:
    """Read lines from the stream, write them to the file at path, and pass them to capture_callback."""
    if stream is None:
        return
    try:
        with open(path, "a", encoding="utf-8") as handle:
            for line in iter(stream.readline, ""):
                handle.write(line)
                handle.flush()
                capture_callback(line)
    finally:
        try:
            stream.close()
        except Exception:
            pass


def _extract_job_id(text: str) -> str | None:
    payload = json.loads(text)
    try:
        message_content = json.loads(payload.get("MessageContent"))
    except (TypeError, json.JSONDecodeError):
        return None
    return message_content.get("JobID")


def _extract_failed_transfers(summary_data: dict[str, Any] | str) -> int | None:
    if isinstance(summary_data, dict):
        transfers_failed = summary_data.get("TransfersFailed")
        if not transfers_failed:
            raise ValueError("Expected 'TransfersFailed' key in summary data")
        return int(transfers_failed)
    raise TypeError(
        "Expected summary data to be a dict when extracting failed transfers"
    )


def _map_status(raw_status: str | None, failed_transfers: int | None) -> AzCopyJobState:
    if failed_transfers is not None and failed_transfers > 0:
        return "FAILED"
    if not raw_status:
        return "UNKNOWN"
    status = raw_status.strip().lower()
    if status in _PENDING_STATUS:
        return "PENDING"
    if status in _RUNNING_STATUS:
        return "RUNNING"
    if status in _SUCCESS_STATUS:
        return "SUCCEEDED"
    if status in _FAILED_STATUS:
        return "FAILED"
    if status in _CANCELED_STATUS or "cancel" in status:
        return "CANCELED"
    return "UNKNOWN"


def _parse_summary_data(summary_data: dict[str, Any] | str) -> dict[str, Any]:
    if isinstance(summary_data, dict):
        return {
            "files_transferred": summary_data["FileTransfers"],
            "bytes_transferred": summary_data["TotalBytesTransferred"],
            "failed_transfers": summary_data["TransfersFailed"],
            "skipped_transfers": summary_data["TransfersSkipped"],
        }
    raise TypeError("Expected summary data to be a dict for parsing summary details")


def _safe_env_subset(env: dict[str, str]) -> dict[str, str]:
    allowed_keys = {
        "AZCOPY_LOG_LOCATION",
        "AZCOPY_JOB_PLAN_LOCATION",
        "AZCOPY_LOG_DIR",
    }
    return {key: value for key, value in env.items() if key in allowed_keys}


def _redact_command(command: list[str]) -> list[str]:
    redacted: list[str] = []
    for arg in command:
        redacted.append(_redact_uri(arg))
    return redacted


def _redact_uri(value: str) -> str:
    if "?" not in value:
        return value
    try:
        parts = urlsplit(value)
    except ValueError:
        return value
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "REDACTED", ""))


def _read_tail_lines(
    path: str,
    *,
    max_lines: int = 5,
    block_size: int = 8192,
) -> list[str]:
    """Return the last `max_lines` lines from a (potentially large) log file."""
    if max_lines <= 0:
        return []

    if not os.path.exists(path):
        return []

    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            end = f.tell()
            if end == 0:
                return []

            buffer = b""
            pos = end
            newlines = 0

            # Read backwards until we have enough newlines for max_lines lines.
            while pos > 0 and newlines <= max_lines:
                read_size = block_size if pos >= block_size else pos
                pos -= read_size
                f.seek(pos, os.SEEK_SET)
                chunk = f.read(read_size)
                buffer = chunk + buffer
                newlines = buffer.count(b"\n")

            # Keep only the last max_lines lines.
            lines_bytes = buffer.splitlines()[-max_lines:]
            text = b"\n".join(lines_bytes).decode("utf-8", errors="replace")
    except OSError:
        return []

    return text.splitlines()[-max_lines:]


def _is_job_not_found(stdout: str, stderr: str) -> bool:
    combined = f"{stdout}\n{stderr}".lower()
    patterns = [
        "not found",
        "does not exist",
        "no such job",
        "job plan file",
        "cannot find the job",
    ]
    return any(pattern in combined for pattern in patterns)


def _terminate_process(process: subprocess.Popen) -> None:
    try:
        process.terminate()
        process.wait(timeout=2)
    except Exception:
        try:
            process.kill()
        except Exception:
            return


def raise_for_error_in_line_log(line: str) -> None:
    payload = json.loads(line)
    if payload.get("MessageType") == "Error":
        if "403" in payload.get("MessageContent", ""):
            raise AzCopyPermissionError(f"AzCopy permission error: {line}")
