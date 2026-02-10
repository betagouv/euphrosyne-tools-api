# [TASK] Implement AzCopy runner abstraction (start, poll, parse summary)

## Context

PRD – FR1/FR2 require **verification via AzCopy job results** (bytes + files).
`euphrosyne-tools-api` must run AzCopy as a **long-running, monitorable** copy job and expose **reliable summary stats** for Euphrosyne verification.

---

## Goal

Provide a single abstraction that can:

1. **Start** an AzCopy copy job (Files ↔ Blob)
2. **Poll** job status until terminal state
3. **Parse** final job summary into structured stats:
   - bytes copied
   - files transferred
   - plus any relevant errors/warnings

This runner is an internal building block used by COOL/RESTORE operations.

---

## Non-goals (v1)

- No “delete source” (`remove` / sync) behavior
- No partial verification logic here (runner only reports stats; higher layer compares vs expected)
- No multi-job orchestration (one operation = one AzCopy job)

---

## Public API (internal module interface)

### Types

```text
AzCopyJobState = "PENDING" | "RUNNING" | "SUCCEEDED" | "FAILED" | "CANCELED" | "UNKNOWN"

AzCopyJobRef:
- job_id: str
- started_at: datetime
- command: list[str]              # for audit/debug (redact secrets)
- environment: dict[str, str]     # safe subset (no secrets)
- log_dir: str                    # path where logs live

AzCopyProgress:
- state: AzCopyJobState
- last_updated_at: datetime
- raw_status: str                 # raw azcopy status string, if available

AzCopySummary:
- state: AzCopyJobState           # terminal only for summary
- files_transferred: int
- bytes_transferred: int
- failed_transfers: int           # if available
- skipped_transfers: int          # if available
- warnings: int                   # if available
- errors: int                     # if available
- started_at: datetime | null
- finished_at: datetime | null
- raw_summary: dict | str         # store minimally; size-bounded
```

### Functions

#### `start_copy(source_uri, dest_uri, *, options) -> AzCopyJobRef`

Starts AzCopy and returns the job reference (including `job_id`).

Options (v1):

- `recursive: bool = True`
- `overwrite: str = "true"` (or `"ifSourceNewer"` depending on your restore strategy)
- `from_to: str | null` (optional explicit AzCopy FromTo)
- `log_level: str = "INFO"`
- `output_type: "json" | "text" = "json"` (prefer json if stable)
- `extra_args: list[str] = []`

**Requirements**

- Must capture and return the **AzCopy job id**.
- Must persist stdout/stderr to per-job log files.

---

#### `poll(job_id) -> AzCopyProgress`

Returns current job state, without blocking.

**Requirements**

- Must be safe to call frequently (polling).
- Must not parse full logs each time if avoidable.

---

#### `get_summary(job_id) -> AzCopySummary`

Returns final stats when job is terminal.

**Requirements**

- If job is still running, return current-known summary with `state=RUNNING`. Return progress only from `poll`, and require terminal state for `get_summary`.
- Must return **bytes/files transferred** in a stable structure.

---

## AzCopy invocation requirements

### Command shape (recommended)

Use AzCopy jobs so we can poll:

- Start:
  - `azcopy copy "<source>" "<dest>" --recursive=true ...`

- Poll:
  - `azcopy jobs show <jobId> --output-type=json` (or text)

- Summary:
  - Prefer JSON output if supported; otherwise parse the final summary text.

### Job id extraction

AzCopy typically prints the JobID on start output. The runner must:

- read stdout/stderr from the start process
- extract the job id deterministically (regex fallback allowed)
- if job id cannot be found:
  - mark the operation as failed at a higher layer (runner returns error)

---

## Output & log storage

The AzCopy runner produces job artifacts (stdout/stderr snapshots, optional status/summary dumps) primarily for:

debugging failures

inspecting what AzCopy reported

keeping a final “summary snapshot” when available

Important: these artifacts are best-effort only.
They must not be required for correctness, because the filesystem may be ephemeral (e.g., Scalingo) and artifacts may disappear on restart/redeploy.

Correctness requirements (job tracking, polling, final status) must rely on:

persisted job_id + operation state in the database

re-polling AzCopy via azcopy jobs show <jobId>

### Log directory layout

👍 Totally fair. Let’s strip it down to **the minimum that still works everywhere (including Scalingo)**.

Here’s the **simplified, final spec** for the AzCopy runner **log / directory handling**.

---

## AzCopy log & job plan handling (simple version)

### Principle

- **AzCopy manages its own logs and job plans.**
- tools-api does **not** define or depend on any directory layout.
- Local files are **best-effort debugging aids only**.
- Correctness must rely **only on `job_id` + `azcopy jobs show`**.

---

### Configuration

tools-api sets **one optional directory** for AzCopy:

- `AZCOPY_WORK_DIR` (env var)
  - Default: `/app/.azcopy` (Scalingo-compatible)
  - Alternative: `/tmp/.azcopy`

At runtime, tools-api maps this to AzCopy’s native env vars:

- `AZCOPY_LOG_LOCATION = AZCOPY_WORK_DIR`
- `AZCOPY_JOB_PLAN_LOCATION = AZCOPY_WORK_DIR`

If the directory:

- does not exist → create it
- is not writable → **do not fail** (AzCopy will fall back to defaults)

---

## State mapping

Map AzCopy job status to our `AzCopyJobState`:

- Running-like statuses → `RUNNING`
- Completed successfully → `SUCCEEDED`
- Completed with failures / canceled → `FAILED` or `CANCELED` (depending on AzCopy status)
- Unknown/unparseable → `UNKNOWN`

**Rule**

- If AzCopy reports any **failed transfers > 0**, runner should set `state=FAILED` even if process exit code is 0, unless AzCopy explicitly indicates success with failures (in which case we still treat as failed for v1).

---

## Summary parsing contract

The runner must output, at minimum:

- `files_transferred` (int)
- `bytes_transferred` (int)

Preferred additional fields if available from AzCopy:

- failed transfers count
- skipped transfers count
- warnings/errors count
- started/finished timestamps

### Parsing precedence

1. **JSON output** from `azcopy jobs show --output-type=json` (preferred)
2. Fallback: parse the summary text block in logs (robust parsing, not brittle line offsets)

### Robustness requirements

- Must handle large numbers (use int64)
- Must handle missing fields gracefully (default 0 or null)
- Must produce deterministic results for a given job output

---

## Error handling

Define explicit exceptions (or error result types) for:

- `AzCopyNotInstalledError`
- `AzCopyStartError` (process failed to start / non-zero exit)
- `AzCopyJobIdNotFoundError`
- `AzCopyJobNotFoundError` (polling unknown job)
- `AzCopyParseError` (could not parse status/summary)
- `AzCopyNotFinishedError` (if `get_summary` called before terminal)

All errors must include:

- job_id (if available)
- pointer to log directory
- safe excerpt of stderr/stdout (bounded)

---

## Configuration

Environment variables (tools-api):

- `AZCOPY_PATH` (default: `azcopy`)
- `AZCOPY_LOG_DIR` (default: app data dir)
- `AZCOPY_POLL_INTERVAL_SECONDS` (default used by higher-level loops, not necessarily runner)
- `AZCOPY_DEFAULT_LOG_LEVEL` (optional)

---

## Security considerations

- Never log full URIs if they contain SAS tokens; redact query strings.
- Ensure files written to log dir have restricted permissions.
- If running in containers, ensure log dir is on persistent storage if needed for later inspection.

---

## Testing strategy

### Unit tests (no real AzCopy)

- Mock subprocess calls:
  - start output includes a job id → extracted correctly
  - missing job id → error
  - poll JSON parsing → correct state mapping
  - summary JSON parsing → bytes/files parsed correctly
  - failure conditions (failed transfers > 0) → `FAILED`
  - unknown statuses → `UNKNOWN`

### Integration tests (optional, gated)

- Run AzCopy against a local/emulated environment only if available.
- Otherwise keep as manual QA checklist.

---

## Acceptance criteria mapping

- **Job can be executed and monitored**
  - `start_copy()` returns `job_id`
  - `poll()` returns non-terminal and terminal states correctly

- **Summary stats available for verification**
  - `get_summary()` returns `files_transferred` + `bytes_transferred` reliably for SUCCEEDED jobs
  - Failure jobs produce a terminal summary or a clear error with logs
