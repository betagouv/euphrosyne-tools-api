from typing import Any

import pytest
from pydantic import ValidationError

from data_visualization.models import DataVisualization


def _visualization(option: dict[str, Any]) -> DataVisualization:
    return DataVisualization(title="Graphique", option=option)


def test_accepts_an_option_without_series_and_benign_url_text() -> None:
    option = {
        "dataset": {"source": [["Site", "URL"], ["A", "https://example.test"]]},
        "graphic": {"type": "text", "style": {"text": "Sans série"}},
    }

    assert _visualization(option).option == option


@pytest.mark.parametrize(
    "option",
    [
        {"tooltip": {"extraCssText": "position: fixed"}},
        {"tooltip": {"formatter": "<img src=x onerror=alert(1)>"}},
        {
            "series": {
                "type": "bar",
                "data": [1],
                "tooltip": {"formatter": "<img src=x onerror=alert(1)>"},
            }
        },
        {"toolbox": {"feature": {"dataView": {"optionToContent": "code"}}}},
        {"toolbox": {"feature": {"dataView": {"title": "<img>"}}}},
        {"toolbox": {"feature": {"dataView": {"lang": ["<img>"]}}}},
        {"toolbox": {"feature": {"saveAsImage": {"name": "download"}}}},
        {"toolbox": {"feature": {"saveAsImage": {"type": "svg"}}}},
        {"title": {"link": "javascript:alert(1)"}},
        {"title": [{"text": "A"}, {"sublink": "https://example.test"}]},
        {
            "series": {
                "type": "treemap",
                "data": [{"name": "A", "children": [{"link": "javascript:x"}]}],
            }
        },
        {
            "dataset": {
                "transform": {"config": {"reg": "(a+)+$"}},
            }
        },
        {"graphic": {"type": "image", "style": {"image": "https://x.test"}}},
        {"series": {"type": "scatter", "symbol": "image://https://x.test"}},
    ],
)
def test_rejects_unsafe_echarts_fields(option: dict[str, Any]) -> None:
    with pytest.raises(ValidationError, match="unsafe|external resource"):
        _visualization(option)


def test_rejects_non_finite_values() -> None:
    with pytest.raises(ValidationError, match="finite JSON"):
        _visualization({"series": [{"data": [float("nan")]}]})
