## [TASK] Consume Euphrosyne storage-role endpoint in get_project_data_client

## Context

tools-api already supports deterministic HOT/COOL storage resolution and enforces read-only access on non-HOT storage. It now needs to consume Euphrosyne lifecycle-based storage-role information for user-facing project data access.

`DATA_BACKEND` is always configured. `DATA_BACKEND_COOL` may be absent.

## Description

Update `dependencies.get_project_data_client()` to resolve the effective project storage role before instantiating the data client.

Behavior:

- if `DATA_BACKEND_COOL` is not set:
  - resolve directly to HOT storage
  - do not call Euphrosyne
- if `DATA_BACKEND_COOL` is set:
  - call Euphrosyne backend endpoint to retrieve the project storage role
  - use that role to select the correct storage backend/path via the existing resolver
- keep write permissions enforced by the existing non-HOT write guard

Euphrosyne endpoint is `GET /api/data-management/projects/{project_slug}/storage-role`

## Acceptance criteria

- When `DATA_BACKEND_COOL` is unset, project data access resolves directly to HOT storage.
- When `DATA_BACKEND_COOL` is set, tools-api fetches the project storage role from Euphrosyne.
- `get_project_data_client()` uses the resolved storage role to instantiate the correct client/backend.
- No endpoint performs ad hoc HOT/COOL probing.
- Existing non-HOT read-only enforcement remains unchanged.
- Tests cover both configuration modes.
