## [TASK] Implement lifecycle operation execution (callback-based, no persistence)

### Context

PRD – FR5 *Idempotency & retries*
Architecture decision: **tools-api is stateless** for lifecycle operations.
Euphrosyne is the **source of truth** and persists lifecycle operations.

euphrosyne-tools-api already posts to Euphrosyne and has an existing
authenticated server-to-server communication mechanism; this must be reused.

---

### Goal

Enable euphrosyne-tools-api to:

* accept project-level COOL / RESTORE requests referencing an `operation_id`
* start an asynchronous operation
* report completion back to Euphrosyne via callback
* avoid duplicate execution *best-effort* during the same process lifetime

This task establishes the **execution contract**; it does **not** implement
physical data movement yet.

---

### Scope (what to implement)

#### API endpoints

Implement the following endpoints:

* `POST /data/projects/{project_slug}/cool?operation_id=<uuid>`
* `POST /data/projects/{project_slug}/restore?operation_id=<uuid>`

**Request**

* Empty body
* `operation_id` provided by Euphrosyne
* Auth: reuse existing tools-api authentication

**Response**

* `202 Accepted`

```json
{
  "operation_id": "<uuid>",
  "project_slug": "<slug>",
  "type": "COOL",
  "status": "ACCEPTED"
}
```

---

#### Execution model

* Start an asynchronous/background task for the operation.
* Do **not** persist operation state in tools-api (no DB, no Redis).
* Keep a **best-effort in-memory guard** to avoid starting duplicate operations
  for the same `(project_slug, type, operation_id)` while the process is alive.

If the service restarts, duplicate execution is acceptable; reconciliation is
handled by Euphrosyne.

---

#### Callback to Euphrosyne

On operation completion (success or failure), tools-api must POST back to
Euphrosyne using the **existing authenticated posting mechanism**.

Callback payload must include:

* `operation_id`
* `project_slug`
* `type` (`COOL` or `RESTORE`)
* `status` (`SUCCEEDED` or `FAILED`)
* `finished_at`
* `bytes_copied`, `files_copied` (nullable for now)
* `error_message` (if failed)
* optional `error_details`

Euphrosyne is responsible for:

* persisting the result
* enforcing idempotency
* ignoring late or duplicate callbacks

---

#### Callback retry policy

* Retry callback delivery on transient failures (5xx, network errors)
* Use a simple exponential backoff (limited number of attempts)
* If delivery ultimately fails:

  * log clearly with `operation_id`
  * exit the task
  * rely on Euphrosyne reconciliation cron to handle stuck operations

---

### Explicit non-goals (out of scope)

* No AzCopy execution
* No Azure access
* No operation persistence in tools-api
* No GET /operation status endpoint in tools-api
* No lifecycle state machine logic (owned by Euphrosyne)

---

### Observability

* Structured logs must include:

  * operation_id
  * project_slug
  * operation type
  * job start / end
  * callback attempts and responses

---

### Tests

Add tests to cover:

* POST endpoint returns 202 and schedules background task
* Duplicate POST with same identifiers does not start a second in-process task
* Callback is sent with expected payload
* Callback retry logic on transient failure

---

### Acceptance criteria

* tools-api accepts COOL / RESTORE requests and returns 202 with operation_id
* Completion callback is sent to Euphrosyne on success or failure
* Duplicate execution is avoided while the process is running
* Callback retries occur on transient errors
* Tests cover happy path and failure path

---

### Notes

* Strong idempotency and reconciliation are enforced in Euphrosyne.
* tools-api is intentionally stateless to fit Scalingo constraints.
* This task defines the execution contract used by later AzCopy tasks.
