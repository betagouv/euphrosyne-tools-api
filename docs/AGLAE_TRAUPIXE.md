# TRAUPIXE workbook selection

`aglae.traupixe` selects a raw Excel workbook for the Albert proof of concept.
It deliberately does not parse or normalize scientific data.

## Public flow

1. Call `detect_traupixe_workbooks` in an authorized project and run.
2. Display every returned candidate and preselect `default_file_id`.
3. Send the selected opaque `file_id` back to Tools API.
4. Call `resolve_traupixe_workbook` in the same project and run.
5. Copy the returned raw `source` stream to the isolated Albert session.
6. Close `source` after the upload.

The browser must never send a storage path.

## Candidate criteria

A candidate:

- is stored in `raw_data` or `processed_data`;
- has an `.xlsx` extension;
- is no larger than 100 MiB;
- is a readable XLSX container;
- declares these worksheets in `xl/workbook.xml`:
  - `S_Conc. & Unc %`;
  - `S_Conc. & Unc ppm`;
  - `S_Best Det.`.

This is a minimal signature, not a scientific data contract. The selector does
not inspect cell values, headers, analytes, detectors, units, or file-name
segments. Additional worksheets and internal layout variations are accepted.

## Integrity and scope

`FileIdCodec` produces an encrypted and authenticated opaque identifier bound
to:

- project and run;
- run data type and storage path;
- listed size and modification time;
- SHA-256 content fingerprint.

Resolution scans only the authorized run and verifies the metadata,
fingerprint, size limit, and minimal signature again. A replaced file raises
`TraupixeSourceChangedError`; an unknown or out-of-scope identifier raises
`TraupixeWorkbookNotFoundError`.

The codec secret must contain at least 32 bytes and must remain server-side.

## Intentional omissions

The former loader, normalized measurement models, LOD rules, detector mapping,
and CSV/JSON exports were removed. Albert will inspect the raw workbook and
execute its analysis in the isolated sandbox during M2.
