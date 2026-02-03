## [TASK] Implement deterministic HOT/COOL storage resolver for `project_slug` (URI-based)

### Context

PRD – Storage path resolution

`euphrosyne-tools-api` must deterministically compute **where project data lives** for two logical storage roles:

* **HOT** — workspace / active project data
* **COOL** — immutable cooled project data

The resolver must be **role-based**, not “Azure Files vs Azure Blob”-based, because HOT and COOL may both live on blob containers in the future.

This resolver is **foundational**: all later operations (AzCopy, listing, restore) must rely on it as the **single source of truth** for project storage locations.

---

## Goal

Implement a deterministic resolver that returns a canonical `DataLocation` for:

* HOT project data
* COOL project data (when cooling is enabled)

using only:

* `project_slug`
* environment configuration

No network calls. No Azure SDK usage.

---

## Data model: `DataLocation`

Implement (or add) a minimal immutable dataclass:

```python
@dataclass(frozen=True)
class DataLocation:
    role: StorageRole               # HOT | COOL
    backend: StorageBackend         # AZURE_FILESHARE | AZURE_BLOB
    project_slug: str
    uri: str                        # canonical URI for project root
```

Notes:

* Use `uri` (lowercase) for Python style.
* `uri` must point to the **project root folder/prefix** (not to a file, not to a run subfolder).
* `StorageBackend` enum values must be:

  * `AZURE_FILESHARE`
  * `AZURE_BLOB`

---

## Environment configuration

### Backend selection (per role)

* `DATA_BACKEND=azure_fileshare|azure_blob`

  * Used for **HOT** data
  * Required

* `DATA_BACKEND_COOL=azure_fileshare|azure_blob`

  * Used for **COOL** data
  * **Optional**
  * If **absent**, cooling is considered **disabled** and COOL resolution must not be allowed

If `DATA_BACKEND_COOL` is set, **all required COOL-specific configuration must be present**; otherwise startup or resolution must fail with a clear configuration error.

---

### Backend-specific configuration

#### Azure Fileshare

* `AZURE_STORAGE_FILESHARE`

  * Fileshare name for HOT data (existing config; keep as-is)

* `AZURE_STORAGE_FILESHARE_COOL`

  * Fileshare name for COOL data (required if `DATA_BACKEND_COOL=azure_fileshare`)

#### Azure Blob

* `AZURE_STORAGE_DATA_CONTAINER`

  * Blob container for HOT data (existing config; keep as-is)

* `AZURE_STORAGE_DATA_CONTAINER_COOL`

  * Blob container for COOL data (required if `DATA_BACKEND_COOL=azure_blob`)

---

### Project prefix configuration

Project prefix must be **backend-agnostic**.

* `DATA_PROJECTS_LOCATION_PREFIX`

  * Base prefix for HOT data

* `DATA_PROJECTS_LOCATION_PREFIX_COOL`

  * Base prefix for COOL data

**Backward compatibility rule**:
* No backward compatibility rule. Replace `AZURE_STORAGE_PROJECTS_LOCATION_PREFIX`with `DATA_PROJECTS_LOCATION_PREFIX`


---

## Resolver behavior

### Resolver API

Expose role-based resolver functions (names indicative):

* `resolve_hot_location(project_slug: str) -> DataLocation`
* `resolve_cool_location(project_slug: str) -> DataLocation`

Optionally expose:

* `resolve_location(role: StorageRole, project_slug: str) -> DataLocation`

The resolver must:

* validate `project_slug`
* determine backend from env configuration
* build a **canonical URI**
* return `DataLocation(role, backend, project_slug, uri)`

If `resolve_cool_location` is called while `DATA_BACKEND_COOL` is **unset**, raise a clear error indicating that cooling is disabled.

---

## Canonical URI formats

Use **no trailing slash** policy.

### Azure Fileshare

```
https://{account}.file.core.windows.net/{share}/{prefix}/{project_slug}
```

### Azure Blob

```
https://{account}.blob.core.windows.net/{container}/{prefix}/{project_slug}
```

### Prefix normalization rules

* Strip leading/trailing `/` from prefixes before joining
* Avoid double slashes in the resulting path
* Empty prefix must be handled cleanly

---

## Validation requirements

Reject invalid `project_slug` values:

* empty string
* leading or trailing whitespace
* contains `/` or `\`
* contains `..`
* contains `//`

Raise a clear, FastAPI-compatible error (HTTP 400–class).

---

## Determinism & stability requirements

* Same inputs + same env config → **exact same `uri` string**
* Must not depend on:

  * timestamps
  * randomness
  * operation_id
  * mutable metadata
* All project storage URIs must be produced via this resolver; no ad-hoc concatenation elsewhere in the codebase.

---

## Unit tests

Add unit tests covering:

1. **Determinism**

   * same slug + same config → identical `DataLocation` (including `uri`)

2. **Golden snapshots**

   * exact URI assertion for:

     * HOT (fileshare)
     * COOL (blob)

3. **Prefix joining**

   * empty prefix
   * non-empty prefix (no double slashes)

4. **Validation**

   * invalid slugs rejected:

     * `""`
     * `"../x"`
     * `"a/b"`
     * `"a\\b"`
     * `"a..b"`
     * `" a "`

5. **Role backend selection**

   * HOT backend driven by `DATA_BACKEND`
   * COOL backend driven by `DATA_BACKEND_COOL`
   * COOL resolution fails when `DATA_BACKEND_COOL` is unset

---

## Acceptance criteria

* `DataLocation` dataclass exists with:

  * `role`, `backend`, `project_slug`, `uri`
* HOT and COOL resolver functions exist and are the **single source of truth** for project storage URIs
* HOT and COOL backends are configurable independently
* Cooling is **disabled by default** when `DATA_BACKEND_COOL` is absent
* URIs are canonical, stable, and validated
* Unit tests validate mapping, validation, and backend selection

---

## Notes

* This task is **resolution only**:

  * no Azure API calls
  * no AzCopy
  * no authentication or SAS logic
* Keep implementation minimal, explicit, and well-documented.
* This resolver defines a long-lived contract; correctness and stability matter more than flexibility.

