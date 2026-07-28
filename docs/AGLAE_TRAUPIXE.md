# Aglaé TRAUPIXE normalization

The `aglae.traupixe` package recognizes the stable data interface shared by
TRAUPIXE workbooks for the Albert BETA, validates it without using a language
model, and exports deterministic CSV and JSON files for Python analysis.

## Public workflow

1. Discover compatible `.xlsx` files in a project run with
   `detect_traupixe_workbooks`.
2. Send only the returned opaque `file_id` to the client.
3. Resolve the identifier in the same project and run with
   `resolve_traupixe_workbook`.
4. Load the resolved stream with `load_traupixe_workbook`, passing the resolved
   file name, and close the stream in a `finally` block.
5. Normalize the resulting `LoadedTraupixeWorkbook` with `normalize_traupixe`.
6. Use `temporary_analysis_dataset` while uploading the normalized files to
   the execution environment.

Both discovery and resolution receive `validate_traupixe_workbook` as their
validator. The resolved stream remains owned by the caller:

```python
resolved = resolve_traupixe_workbook(...)
try:
    loaded = load_traupixe_workbook(
        resolved.source,
        source_name=resolved.name,
    )
finally:
    resolved.source.close()

dataset = normalize_traupixe(loaded)
```

The package does not know about JWTs, HTTP routes, Albert, or Azure Dynamic
Sessions. Authorization is performed by the caller before discovery or
resolution.

## V1 data contract

`TRAUPIXE_FORMAT` is the single source of truth for:

- the four worksheets consumed by the loader;
- the two selected concentration worksheets and their units;
- the header and data row positions;
- the 100 MiB source limit.

The required worksheets are `S_Conc. & Unc %`, `S_Conc. & Unc ppm`,
`S_Best Det.`, and `Exp. data`. Additional worksheets are accepted and ignored.
The two concentration worksheets must declare alternating `analyte` / `Unc%`
columns from column C. `S_Best Det.` must declare the same analytes, in the same
order, with one detector column per analyte. Analyte names and detector labels
are discovered from the workbook rather than selected from a fixed allow-list.
A detector may be empty; non-empty values must be text and are preserved
without interpretation.

The three analyte sequences must match after trimming surrounding whitespace.
At least one analyte is required, analyte names must be unique, and formulas
are rejected in the four source worksheets. Formulas in additional,
non-consumed worksheets do not affect compatibility.

`analysis_id` is opaque. The V1 does not infer a point number, object, project,
or standard from its segments or from the file name. Rows are aligned by the
source identifier and its occurrence order. A unique source identifier is
preserved; repeated identifiers receive a deterministic occurrence suffix in
the normalized dataset.

Values below a detection limit are exported with `value` empty,
`qualifier=below_lod`, and the threshold in `detection_limit`. Empty values,
`n.d.`, and `999999` are never converted to zero. Detector labels are preserved
and never recalculated.

## Exported files

- `analyses.csv`: `analysis_id`, `description`
- `measurements.csv`: one row per analysis, analyte, and unit
- `dataset_metadata.json`: source fingerprint, schemas, counts, analytes,
  units, detectors, aliases, conventions, and aggregated exclusions

Exports use a stable order and representation so the same input produces the
same bytes.

## Supporting another TRAUPIXE variant

A workbook variant is supported automatically when it preserves the minimal
four-sheet interface above, even if its analyte list, detector labels, and
additional worksheets differ. A variant that changes this interface requires
an explicit parser change and an anonymized regression fixture; file names must
not be used to select parsing behavior.

The current fixture is derived from the workbook identified by SHA-256
`bf7861fb9bc2d4ee43951fffa02281b01c2676c8c04d5fecdd246b27ae1a56b0`.
Its identifiers, descriptions, project metadata, and workbook author metadata
are anonymized before it is committed.
