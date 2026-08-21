from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True)
class DataVisualizationCompletion:
    message: dict[str, Any]
    usage: dict[str, Any]
    model: str
    finish_reason: str | None = None


class DataVisualizationLlmClient(Protocol):
    def complete(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        *,
        tool_choice: str | dict[str, Any] = "auto",
        response_format: dict[str, Any] | None = None,
    ) -> DataVisualizationCompletion: ...
