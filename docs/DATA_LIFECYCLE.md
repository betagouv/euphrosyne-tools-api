# Project Data Lifecycle

This document describes how project data storage works in `euphrosyne-tools-api`
when HOT/COOL lifecycle management is enabled.

## Overview

Project data can live in two storage roles:

- `HOT`: primary working storage used for normal project activity
- `COOL`: read-only storage used for cooled projects

This service is responsible for:

- resolving the correct backend and path for a project
- serving project data from HOT or COOL storage depending on lifecycle state
- enforcing read-only access on non-HOT storage
- running COOL and RESTORE operations through AzCopy
- reporting lifecycle operation outcomes back to Euphrosyne

Project data is handled at the project level. Runs and documents move together.

## Storage Model

Two backends are supported:

- Azure Fileshare
- Azure Blob Storage

The runtime configuration separates HOT and COOL storage:

- `DATA_BACKEND` defines the HOT backend
- `DATA_BACKEND_COOL` defines the COOL backend

If `DATA_BACKEND_COOL` is unset, lifecycle-based routing is disabled and all project
data is resolved as HOT.

The base path prefix is shared by both roles:

- `DATA_PROJECTS_LOCATION_PREFIX`

Role-specific storage targets are:

- HOT Fileshare: `AZURE_STORAGE_FILESHARE`
- HOT Blob: `AZURE_STORAGE_DATA_CONTAINER`
- COOL Fileshare: `AZURE_STORAGE_FILESHARE_COOL`
- COOL Blob: `AZURE_STORAGE_DATA_CONTAINER_COOL`

The service builds storage URIs deterministically from:

- storage role
- configured backend
- `AZURE_STORAGE_ACCOUNT`
- `DATA_PROJECTS_LOCATION_PREFIX`
- `project_slug`

## Lifecycle-Aware Data Access

Normal data endpoints do not hardcode HOT or COOL storage. They resolve the effective
storage role per project.

Resolution flow:

1. Resolve `project_slug` from the route parameter or from an input path.
2. If `DATA_BACKEND_COOL` is unset, use `HOT`.
3. If `DATA_BACKEND_COOL` is set, call Euphrosyne:
   `GET /api/data-management/projects/{project_slug}/lifecycle`
4. Read `lifecycle_state` from the response.
5. Resolve the backend client and project location from that storage role.

The current implementation only serves stable storage roles:

- `HOT`
- `COOL`

If a project is in a transitional or unsupported state, the API returns `409 Conflict`
instead of guessing where data should be read or written.

## Read and Write Rules

The service enforces storage-role-specific access centrally in the data client layer.

Behavior:

- HOT storage supports normal read and write operations
- COOL storage supports browsing and download access
- write-capable operations are rejected on non-HOT storage

This affects:

- document upload URLs
- run upload URLs
- project/run initialization
- project/run rename operations
- any token/SAS generation that requests write-capable permissions

Error behavior:

- invalid project/document/run paths return `422 Unprocessable Entity`
- write attempts on non-HOT storage return `409 Conflict`

Internal lifecycle transfers are the only place where write access can be forced for a
COOL destination. That is limited to the AzCopy-based lifecycle flow.

## Path Resolution

Project paths are parsed through typed refs in `path.py`.

These refs validate and extract structured identifiers from incoming paths:

- `ProjectRef`
- `RunRef`
- `RunDataTypeRef`
- `ProjectDocumentRef`

They are used to:

- validate incoming path parameters
- extract `project_slug`, `run_name`, and `data_type`
- route membership checks consistently
- route lifecycle-aware backend selection through the resolved project slug

This avoids per-endpoint path parsing logic and keeps the data API behavior consistent.

## Lifecycle Operations

The lifecycle subsystem lives under `data_lifecycle/`.

Main responsibilities:

- resolve HOT and COOL locations
- resolve the correct backend client for each storage role
- create signed source and destination URLs for AzCopy
- schedule and execute lifecycle operations in background tasks
- poll AzCopy jobs and expose status to callers
- post the final lifecycle result back to Euphrosyne

Supported operation types:

- `COOL`
- `RESTORE`

Operations are keyed by `operation_id`. Within a process, duplicate submissions for the
same `(project_slug, operation type, operation_id)` are tracked and not executed twice.

Operation execution model:

1. Register the lifecycle operation.
2. Start an AzCopy job for the required direction.
3. Poll until the job reaches a terminal state.
4. Fetch the AzCopy summary.
5. Mark the operation as succeeded or failed.
6. Send the callback payload to Euphrosyne.

Status views exposed by the API are derived from AzCopy progress and include:

- `PENDING`
- `RUNNING`
- `SUCCEEDED`
- `FAILED`

Failure payloads include AzCopy metadata when available so errors can be inspected
operationally.

## Lifecycle API

The service exposes project-level lifecycle endpoints under `/data`:

- `POST /data/projects/{project_slug}/cool`
- `GET /data/projects/{project_slug}/cool/{operation_id}`
- `POST /data/projects/{project_slug}/restore`
- `GET /data/projects/{project_slug}/restore/{operation_id}`
- `POST /data/projects/{project_slug}/delete/{storage_role}`

These endpoints are intended for Euphrosyne/backend orchestration:

- `POST` endpoints register a lifecycle operation and schedule background execution
- `GET` endpoints return the current progress/status for a known `operation_id`

The data API also standardizes on `project_slug` naming for data-related routes.

The delete endpoint deletes an inactive storage side only. It accepts
`operation_id`, `file_count`, and `total_size` query parameters. Before deletion,
the background task fetches the current project lifecycle, rejects deletion of the
active storage side, then compares `file_count` and `total_size` against the active
storage side. This proves the retained copy has the expected number of files and
bytes before the inactive side is removed. Validation failures are reported through
the lifecycle deletion callback. If the active project data directory or blob prefix
is missing, deletion fails closed instead of treating missing data as an empty
project.

## Operational Notes

AzCopy is a runtime dependency for COOL and RESTORE operations.

Relevant pieces:

- `bin/post_compile` installs AzCopy into `bin/azcopy`
- `.env.example` exposes `AZCOPY_PATH=bin/azcopy`
- `scripts/cool_data.py` provides a manual helper for triggering a HOT to COOL copy

If COOL storage is configured, the service also depends on:

- `EUPHROSYNE_BACKEND_URL`
- backend authentication via `generate_token_for_euphrosyne_backend()`

The lifecycle lookup currently retries transient failures before returning `503 Service
Unavailable`.
