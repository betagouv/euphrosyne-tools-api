from __future__ import annotations

import csv
import json
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterator

from .models import DatasetExport, NormalizedDataset

ANALYSES_FIELDS = ("analysis_id", "description")
MEASUREMENT_FIELDS = (
    "analysis_id",
    "analyte",
    "value",
    "unit",
    "qualifier",
    "detection_limit",
    "uncertainty",
    "detector",
)


def _decimal_text(value: Decimal | None) -> str:
    return "" if value is None else format(value, "f")


def _write_analyses(dataset: NormalizedDataset, path: Path) -> None:
    with path.open("w", encoding="utf-8", newline="") as destination:
        writer = csv.writer(destination, lineterminator="\n")
        writer.writerow(ANALYSES_FIELDS)
        for analysis in dataset.analyses:
            writer.writerow((analysis.analysis_id, analysis.description))


def _write_measurements(dataset: NormalizedDataset, path: Path) -> None:
    with path.open("w", encoding="utf-8", newline="") as destination:
        writer = csv.writer(destination, lineterminator="\n")
        writer.writerow(MEASUREMENT_FIELDS)
        for measurement in dataset.measurements:
            writer.writerow(
                (
                    measurement.analysis_id,
                    measurement.analyte,
                    _decimal_text(measurement.value),
                    measurement.unit.value,
                    measurement.qualifier.value,
                    _decimal_text(measurement.detection_limit),
                    _decimal_text(measurement.uncertainty),
                    (
                        ""
                        if measurement.detector is None
                        else measurement.detector.value
                    ),
                )
            )


def _write_metadata(dataset: NormalizedDataset, path: Path) -> None:
    metadata = dataset.metadata
    content = {
        "aliases": dict(metadata.aliases),
        "analyses_schema": list(ANALYSES_FIELDS),
        "analysis_count": metadata.analysis_count,
        "analytes": list(metadata.analytes),
        "conventions": list(metadata.conventions),
        "detectors": [detector.value for detector in metadata.detectors],
        "exclusions": [
            {
                "count": exclusion.count,
                "reason": exclusion.reason.value,
            }
            for exclusion in metadata.exclusions
        ],
        "measurement_count": metadata.measurement_count,
        "measurements_schema": list(MEASUREMENT_FIELDS),
        "source": {
            "name": metadata.source_name,
            "sha256": metadata.source_sha256,
        },
        "units": [unit.value for unit in metadata.units],
    }
    with path.open("w", encoding="utf-8", newline="\n") as destination:
        json.dump(
            content,
            destination,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        destination.write("\n")


def export_analysis_dataset(
    dataset: NormalizedDataset, destination: Path
) -> DatasetExport:
    destination.mkdir(parents=True, exist_ok=True)
    export = DatasetExport(
        analyses_csv=destination / "analyses.csv",
        measurements_csv=destination / "measurements.csv",
        metadata_json=destination / "dataset_metadata.json",
    )
    _write_analyses(dataset, export.analyses_csv)
    _write_measurements(dataset, export.measurements_csv)
    _write_metadata(dataset, export.metadata_json)
    return export


@contextmanager
def temporary_analysis_dataset(
    dataset: NormalizedDataset,
) -> Iterator[DatasetExport]:
    with TemporaryDirectory(prefix="aglae-traupixe-") as temporary:
        yield export_analysis_dataset(dataset, Path(temporary))
