# Implement the **tools-api GET status endpoints** for COOL and RESTORE operations

## Context

PRD “Hot → Cool Project Data Management (Immutable Cool)” defines the **tools-api** surface for lifecycle operations, including **status endpoints** per project + operation_id:

- `GET /data/projects/{project_slug}/cool/{operation_id}`
- `GET /data/projects/{project_slug}/restore/{operation_id}`

Admin visibility (FR4) requires surfacing **operation status + bytes/files moved + error details**. The tools-api is the natural place to expose **live progress** (AzCopy job status) because it owns the job execution.

So the “/data/operations/{operation_id}” endpoint we discussed earlier does **not** match the PRD API specs.

---

## Description

Implement the **tools-api GET status endpoints** for COOL and RESTORE operations:

- `GET /data/projects/{project_slug}/cool/{operation_id}`
- `GET /data/projects/{project_slug}/restore/{operation_id}`

### Access control

- Protect endpoints using the same **backend-only auth** mechanism as other tools-api backend views (Euphrosyne backend token).
- Unauthorized requests return `401/403` consistently.

### Operation lookup

- Identify the operation using `operation_id` (provided by Euphrosyne).
- Use the in-memory structures already in tools-api:
  - lifecycle guard entry keyed by `operation_id`
  - mapping `operation_id → azcopy_job_id` (may be missing early)

### Status resolution (including “early” phase)

Return a status even if the AzCopy job id is not yet known:

- If `operation_id` unknown → `404`
- If known but `azcopy_job_id` not set yet → `status = PENDING` (or equivalent) with 0 progress
- If job id known → query AzCopy (`azcopy jobs show <job_id>`) and map to:
  - `RUNNING` when `InProgress`
  - `SUCCEEDED` when `Completed` (or equivalent AzCopy terminal success)
  - `FAILED` when `Failed/Cancelled` (terminal failure)

### Progress & stats

Expose:

- `bytes_total`, `files_total` (from AzCopy “Total Number of Bytes Transferred” / “Number of File Transfers” or the best available “total” values AzCopy provides)
- `bytes_copied`, `files_copied` (completed so far)
- `progress_percent` (AzCopy “Percent Complete (approx)” if available; otherwise compute from bytes/files)

### Errors

If the job failed:

- return `error_details` populated from AzCopy job output (summary fields + any available failure reason)
- keep it structured enough for the caller to display (message + optional raw details)

> Note: verification gating (bytes/files match expected totals) is handled on the **Euphrosyne callback side** per PRD; these GET endpoints are about reporting tools-api’s view of the operation/job progress and terminal result.

---

## Acceptance criteria

- Implements:
  - `GET /data/projects/{project_slug}/cool/{operation_id}`
  - `GET /data/projects/{project_slug}/restore/{operation_id}`

- Each endpoint returns (at minimum):
  - `operation_id`, `project_slug`, `type` (COOL/RESTORE)
  - `status` in `{PENDING, RUNNING, SUCCEEDED, FAILED}`
  - `progress_percent`
  - `bytes_total`, `files_total`
  - `bytes_copied`, `files_copied`
  - `error_details` when `FAILED`

- Works in the “fast enqueue” timeline:
  - if `operation_id` exists but job id not yet assigned, endpoint still returns `PENDING` without error

- Access control enforced with backend-only token; unauthorized access returns `401/403`
- Returns `404` when `operation_id` is unknown for the given project/type

If you want, I can also propose a concrete JSON response schema (including exact field names) that matches what you already send in `post_lifecycle_operation_callback(...)` so both sides stay consistent.
