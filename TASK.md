## [TASK] Implement `POST /data/projects/{project_slug}/cool` (enqueue fast) + guard + job-id map + callback

### Context

- AzCopy COOL copy logic is already implemented
- Endpoint must return quickly (202) **before** AzCopy starts / before job id exists
- tools-api uses in-memory structures to ensure best-effort idempotency
- Completion is reported via `post_lifecycle_operation_callback(...)` (already implemented)
- Endpoint must be protected using the same auth mechanism as other backend-only views

---

## Goal

Accept COOL operations idempotently and run the existing AzCopy job asynchronously, while:

- preventing duplicate runs per `operation_id`
- recording `operation_id → azcopy_job_id` once available
- reporting terminal result via existing callback hook

---

## Authorization

This endpoint is **backend-only**:

- Only requests authenticated with the existing **Euphrosyne backend token** (already implemented and used in other views) are authorized.
- Unauthorized requests return `401/403` consistent with current auth behavior.

---

## Endpoint

`POST /data/projects/{project_slug}/cool?operation_id=<uuid>`
Empty body.

### Immediate response

Return `202 Accepted` with:

- `operation_id`
- `project_slug`
- `type: "COOL"`
- `status: "ACCEPTED"`

---

## In-memory structures

### 1) `_LIFECYCLE_OPERATION_GUARD`

A set-like structure containing only:

- `operation_id`

Purpose:

- ensure idempotency (do not launch duplicate background jobs)

### 2) `_LIFECYCLE_OPERATION_JOB_ID`

A mapping/dict:

- `operation_id → azcopy_job_id` (string)

Purpose:

- store job id when it becomes available

> No other state/timestamps are tracked in tools-api; lifecycle state is owned by Euphrosyne.

---

## Request handling flow

1. Validate `operation_id` is present and a UUID; else `400`.
2. If `operation_id` is already in `_LIFECYCLE_OPERATION_GUARD`:
   - return `202` immediately (idempotent accept)
   - do **not** schedule another background job

3. Otherwise:
   - add `operation_id` to `_LIFECYCLE_OPERATION_GUARD`
   - return `202` immediately
   - schedule background execution

---

## Background execution flow

1. Resolve HOT source + COOL destination (existing resolver)
2. Invoke existing AzCopy COOL runner
3. When AzCopy job id is known:
   - set `_LIFECYCLE_OPERATION_JOB_ID[operation_id] = job_id`

4. Await completion using existing AzCopy job monitoring/summary parsing code
5. Determine terminal `status` (`SUCCEEDED` / `FAILED`)
6. Collect stats (as available in existing implementation):
   - `bytes_copied`
   - `files_copied`

7. Call `post_lifecycle_operation_callback(...)` with:
   - `operation_id`
   - `project_slug`
   - `type="COOL"`
   - `status`
   - `bytes_copied`, `files_copied`
   - error details on failure

Notes:

- tools-api does **not** enforce expected-vs-actual verification; it reports stats only.

---

## Error handling

- If failure occurs before a job id exists:
  - no entry is added to `_LIFECYCLE_OPERATION_JOB_ID`
  - callback with `FAILED` + error details

- If failure occurs after job id exists:
  - callback with `FAILED` + job-related diagnostics as supported

---

## Acceptance criteria

- Endpoint is protected: only authenticated backend token can call it.
- Endpoint returns `202` immediately, before AzCopy starts and before job id exists.
- `_LIFECYCLE_OPERATION_GUARD` stores only operation ids.
- `_LIFECYCLE_OPERATION_JOB_ID` is populated once AzCopy job id is known.
- Duplicate calls with same `operation_id` do not start another job.
- On completion, `post_lifecycle_operation_callback(...)` is invoked with terminal status + stats.
