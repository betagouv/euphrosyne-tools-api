from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

MAX_VISUALIZATIONS = 8
MAX_GENERATED_OPTION_BYTES = 200_000


@dataclass(frozen=True)
class PreparedDataVisualization:
    """Format-independent data prepared for the visualization workflow."""

    filename: str
    content: bytes
    descriptor: dict[str, Any]
    calculation_instructions: str = ""
    visualization_instructions: str = ""


class DataVisualization(BaseModel):
    """A self-contained ECharts visualization."""

    model_config = ConfigDict(extra="forbid", strict=True)

    title: str = Field(min_length=1, max_length=200)
    option: dict[str, Any]

    @model_validator(mode="after")
    def validate_option(self) -> DataVisualization:
        try:
            encoded = json.dumps(self.option, allow_nan=False)
        except (TypeError, ValueError) as error:
            raise ValueError("the ECharts option must be finite JSON") from error
        if len(encoded.encode("utf-8")) > MAX_GENERATED_OPTION_BYTES:
            raise ValueError("the ECharts option is too large")
        _validate_safe_echarts_option(self.option)
        return self


class GeneratedVisualizationResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    answer: str = Field(min_length=1, max_length=10_000)
    visualizations: list[DataVisualization] = Field(
        min_length=1,
        max_length=MAX_VISUALIZATIONS,
    )


def _validate_safe_echarts_option(option: dict[str, Any]) -> None:
    _reject_unsafe_tooltips(option)

    for toolbox in _mappings(option.get("toolbox")):
        feature = toolbox.get("feature")
        if not isinstance(feature, dict):
            continue
        for data_view in _mappings(feature.get("dataView")):
            _reject_keys(data_view, {"optionToContent", "title", "lang"})
        for save_as_image in _mappings(feature.get("saveAsImage")):
            _reject_keys(save_as_image, {"name", "type"})

    for title in _mappings(option.get("title")):
        _reject_keys(title, {"link", "sublink"})

    for series in _mappings(option.get("series")):
        _reject_key(series.get("data"), "link")

    for dataset in _mappings(option.get("dataset")):
        for transform in _mappings(dataset.get("transform")):
            for config in _mappings(transform.get("config")):
                _reject_key(config, "reg")

    _reject_external_images(option)


def _mappings(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, dict):
        return [value]
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    return []


def _reject_unsafe_tooltips(value: Any) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            if key == "tooltip":
                for tooltip in _mappings(item):
                    _reject_keys(tooltip, {"extraCssText", "formatter"})
            _reject_unsafe_tooltips(item)
        return
    if isinstance(value, list):
        for item in value:
            _reject_unsafe_tooltips(item)


def _reject_keys(value: dict[str, Any], keys: set[str]) -> None:
    for key in keys:
        if key in value:
            raise ValueError(f"the ECharts option contains unsafe field {key!r}")


def _reject_key(value: Any, key: str) -> None:
    if isinstance(value, dict):
        if key in value:
            raise ValueError(f"the ECharts option contains unsafe field {key!r}")
        for item in value.values():
            _reject_key(item, key)
        return
    if isinstance(value, list):
        for item in value:
            _reject_key(item, key)


def _reject_external_images(value: Any) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            if key in {"image", "symbol"} and _is_external_resource(item):
                raise ValueError("the ECharts option references an external resource")
            _reject_external_images(item)
        return
    if isinstance(value, list):
        for item in value:
            _reject_external_images(item)


def _is_external_resource(value: Any) -> bool:
    return isinstance(value, str) and value.lstrip().casefold().startswith(
        ("http://", "https://", "data:", "image://")
    )
