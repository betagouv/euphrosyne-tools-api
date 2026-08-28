from __future__ import annotations

import os
from typing import Any, NotRequired, TypedDict, cast

import httpx

from data_visualization.llm import DataVisualizationCompletion

DEFAULT_ALBERT_BASE_URL = "https://albert.api.etalab.gouv.fr"
MAX_ERROR_DETAIL_LENGTH = 1_000


class ChatCompletionChoice(TypedDict):
    message: dict[str, Any]
    finish_reason: NotRequired[str | None]


class ChatCompletionPayload(TypedDict):
    choices: list[ChatCompletionChoice]
    usage: NotRequired[dict[str, Any]]
    model: NotRequired[str]


class AlbertAPIError(RuntimeError):
    """Raised when Albert cannot provide a usable chat completion."""


class AlbertClient:
    """Minimal client for Albert's OpenAI-compatible chat endpoint."""

    def __init__(
        self,
        api_key: str | None = None,
        model: str | None = None,
        *,
        base_url: str = DEFAULT_ALBERT_BASE_URL,
        timeout_seconds: float = 90,
        http_client: httpx.Client | None = None,
    ) -> None:
        api_key = api_key if api_key is not None else os.environ["ALBERT_API_KEY"]
        model = model if model is not None else os.environ["ALBERT_MODEL"]
        self.model = model
        self._url = f"{base_url.rstrip('/')}/v1/chat/completions"
        self._http_client = http_client or httpx.Client(timeout=timeout_seconds)
        self._headers = {"Authorization": f"Bearer {api_key}"}

    def complete(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        *,
        tool_choice: str | dict[str, Any] = "auto",
        response_format: dict[str, Any] | None = None,
    ) -> DataVisualizationCompletion:
        request_body: dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "temperature": 1,
            "stream": False,
        }
        if tools is not None:
            request_body.update(
                {
                    "tools": tools,
                    "tool_choice": tool_choice,
                    "parallel_tool_calls": False,
                }
            )
        if response_format is not None:
            request_body["response_format"] = response_format
        response = self._http_client.post(
            self._url,
            headers=self._headers,
            json=request_body,
        )
        if response.is_error:
            detail = _error_detail(response)
            suffix = f": {detail}" if detail else ""
            raise AlbertAPIError(f"Albert returned HTTP {response.status_code}{suffix}")
        try:
            payload = cast(ChatCompletionPayload, response.json())
            choice = payload["choices"][0]
            message = choice["message"]
        except (ValueError, KeyError, IndexError, TypeError) as error:
            raise AlbertAPIError("Albert returned an invalid completion") from error
        if not isinstance(message, dict):
            raise AlbertAPIError("Albert returned an invalid assistant message")
        usage = payload.get("usage") or {}
        return DataVisualizationCompletion(
            message=message,
            usage=usage if isinstance(usage, dict) else {},
            model=str(payload.get("model") or self.model),
            finish_reason=choice.get("finish_reason"),
        )


def _error_detail(response: httpx.Response) -> str:
    try:
        payload = response.json()
        error = payload.get("error") if isinstance(payload, dict) else None
        if isinstance(error, dict) and isinstance(error.get("message"), str):
            return error["message"][:MAX_ERROR_DETAIL_LENGTH]
    except ValueError:
        pass
    return response.text.strip()[:MAX_ERROR_DETAIL_LENGTH]
