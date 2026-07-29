import json

import httpx
import pytest

from clients.albert import AlbertAPIError, AlbertClient


def test_complete_calls_albert_with_tools() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url == "https://albert.test/v1/chat/completions"
        assert request.headers["Authorization"] == "Bearer secret"
        body = json.loads(request.content)
        assert body["model"] == "model-id"
        assert body["messages"] == [{"role": "user", "content": "question"}]
        assert body["tools"] == [{"type": "function"}]
        assert body["tool_choice"] == "auto"
        assert body["parallel_tool_calls"] is False
        assert body["temperature"] == 1
        return httpx.Response(
            200,
            json={
                "model": "model-id",
                "choices": [
                    {
                        "finish_reason": "stop",
                        "message": {
                            "role": "assistant",
                            "content": "réponse",
                        },
                    }
                ],
                "usage": {"total_tokens": 42},
            },
        )

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = AlbertClient(
        "secret",
        "model-id",
        base_url="https://albert.test",
        http_client=http_client,
    )

    completion = client.complete(
        [{"role": "user", "content": "question"}],
        [{"type": "function"}],
    )

    assert completion.message["content"] == "réponse"
    assert completion.usage == {"total_tokens": 42}
    assert completion.finish_reason == "stop"


def test_complete_calls_albert_with_a_json_schema_without_tools() -> None:
    response_format = {
        "type": "json_schema",
        "json_schema": {
            "name": "result",
            "strict": True,
            "schema": {"type": "object"},
        },
    }

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        assert body["response_format"] == response_format
        assert "tools" not in body
        assert "tool_choice" not in body
        assert "parallel_tool_calls" not in body
        return httpx.Response(
            200,
            json={
                "model": "model-id",
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": '{"answer":"ok"}',
                        }
                    }
                ],
            },
        )

    client = AlbertClient(
        "secret",
        "model-id",
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )

    completion = client.complete([], response_format=response_format)

    assert completion.message["content"] == '{"answer":"ok"}'


@pytest.mark.parametrize(
    ("response", "message"),
    [
        (httpx.Response(503), "Albert returned HTTP 503"),
        (httpx.Response(200, json={"choices": []}), "invalid completion"),
    ],
)
def test_complete_rejects_api_and_schema_errors(
    response: httpx.Response,
    message: str,
) -> None:
    http_client = httpx.Client(transport=httpx.MockTransport(lambda _request: response))
    client = AlbertClient("secret", "model", http_client=http_client)

    with pytest.raises(AlbertAPIError, match=message):
        client.complete([], [])
