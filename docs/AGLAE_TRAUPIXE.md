# Aglaé TRAUPIXE normalization

The `aglae.traupixe` package recognizes one workbook layout for the Albert
BETA, validates it without using a language model, and exports deterministic
CSV and JSON files for Python analysis.

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

- the 17 required worksheet names, structural rows, and exact headers;
- the two selected concentration worksheets and their units;
- the 36 analytes, including the source headers with trailing spaces;
- the allowed `X0` and `X10` detector values;
- ignored `RED` worksheets;
- the 100 MiB source limit.

`analysis_id` is opaque. The V1 does not infer a point number, object, project,
or standard from its segments. The identifier is preserved and used only for
joins and traceability.

Values below a detection limit are exported with `value` empty,
`qualifier=below_lod`, and the threshold in `detection_limit`. Empty values,
`n.d.`, and `999999` are never converted to zero. X10 is preserved and never
recalculated.

## Exported files

- `analyses.csv`: `analysis_id`, `description`
- `measurements.csv`: one row per analysis, analyte, and unit
- `dataset_metadata.json`: source fingerprint, schemas, counts, analytes,
  units, detectors, aliases, conventions, and aggregated exclusions

Exports use a stable order and representation so the same input produces the
same bytes.

## Supporting another TRAUPIXE variant

Do not loosen `TRAUPIXE_FORMAT` or infer a layout dynamically. Introduce a
separate immutable format definition, add a detector that selects it
unambiguously, and run the complete validation and normalization suite against
an anonymized fixture for that variant. Keep variant-specific parsing behind
the same normalized models and export contract.

The current fixture is derived from the workbook identified by SHA-256
`bf7861fb9bc2d4ee43951fffa02281b01c2676c8c04d5fecdd246b27ae1a56b0`.
Its identifiers, descriptions, project metadata, and workbook author metadata
are anonymized before it is committed.
