# Anonymized TRAUPIXE fixture

`traupixe_reference_anonymized.xlsx` is derived from the workbook documented by
the M1 scopes. The source workbook itself is not committed.

The generator accepts only the source whose SHA-256 is:

```text
bf7861fb9bc2d4ee43951fffa02281b01c2676c8c04d5fecdd246b27ae1a56b0
```

It replaces the 48 analysis identifiers and descriptions consistently across
all worksheets, clears project-specific experimental metadata, and removes
author metadata. Measurement values, worksheet names, headers, detector values,
and the malformed worksheet dimensions are kept so the fixture still proves
the format and normalization rules.

The expected normalized output contains 48 analyses, 36 analytes, 3,456
measurements, and 299 values below the detection limit for each unit.
