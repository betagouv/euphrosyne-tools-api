from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

MAX_VISUALIZATIONS = 8
MAX_GENERATED_OPTION_BYTES = 200_000


class DataVisualization(BaseModel):
    """A self-contained ECharts visualization."""

    model_config = ConfigDict(extra="forbid", strict=True)

    title: str = Field(min_length=1, max_length=200)
    option: dict[str, Any]

    @model_validator(mode="after")
    def validate_option(self) -> "DataVisualization":
        series = self.option.get("series")
        if isinstance(series, dict):
            series_count = 1
        elif isinstance(series, list):
            series_count = len(series)
        else:
            series_count = 0
        if series_count == 0:
            raise ValueError("an ECharts option requires at least one series")
        if series_count > 100:
            raise ValueError("an ECharts option contains too many series")
        try:
            encoded = json.dumps(self.option, allow_nan=False)
        except (TypeError, ValueError) as error:
            raise ValueError("the ECharts option must be finite JSON") from error
        if len(encoded.encode("utf-8")) > MAX_GENERATED_OPTION_BYTES:
            raise ValueError("the ECharts option is too large")
        _reject_external_resources(self.option)
        return self


class GeneratedVisualizationResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    answer: str = Field(min_length=1, max_length=10_000)
    visualizations: list[DataVisualization] = Field(
        min_length=1,
        max_length=MAX_VISUALIZATIONS,
    )


def _reject_external_resources(value: Any) -> None:
    if isinstance(value, dict):
        for item in value.values():
            _reject_external_resources(item)
        return
    if isinstance(value, list):
        for item in value:
            _reject_external_resources(item)
        return
    if isinstance(value, str) and value.lstrip().casefold().startswith(
        ("http://", "https://", "data:", "image://")
    ):
        raise ValueError("the ECharts option references an external resource")
