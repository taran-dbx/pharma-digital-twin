"""Foundation Model call through the serving-endpoint / AI Gateway path.

Uses the app service principal (``WorkspaceClient()`` ambient auth) and
``serving_endpoints.query`` on a pay-per-token chat endpoint. The endpoint
name comes from ``GENAI_ENDPOINT`` (injected from the ``genai-endpoint`` app
resource, CAN_QUERY) and defaults to ``databricks-claude-sonnet-5-5``. Usage
tracking, rate limits and guardrails configured on the endpoint's AI Gateway
apply to every call.

Each attempt is bounded by ``GENAI_TIMEOUT_S``; transient failures (timeouts,
429, 5xx) are retried up to ``GENAI_MAX_RETRIES`` times with backoff. Permission,
not-found and bad-request errors are not retried.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk import errors as sdk_errors
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

DEFAULT_ENDPOINT = "databricks-claude-sonnet-5-5"
ENDPOINT_ENV = "GENAI_ENDPOINT"
TIMEOUT_ENV = "GENAI_TIMEOUT_S"
RETRIES_ENV = "GENAI_MAX_RETRIES"
DEFAULT_TIMEOUT_S = 60.0
DEFAULT_RETRIES = 2
MAX_TOKENS = 2500
BACKOFF_S = 1.0

_TRANSIENT = (
    TimeoutError,
    sdk_errors.TooManyRequests,
    sdk_errors.RequestLimitExceeded,
    sdk_errors.TemporarilyUnavailable,
    sdk_errors.InternalError,
    sdk_errors.DeadlineExceeded,
    ConnectionError,
)


class ModelCallError(RuntimeError):
    """The endpoint did not return an answer. ``kind`` is ``timeout`` or ``endpoint``."""

    def __init__(self, message: str, *, kind: str, attempts: int) -> None:
        super().__init__(message)
        self.kind = kind
        self.attempts = attempts


@dataclass(frozen=True)
class ModelReply:
    text: str
    model: str | None
    served_model_name: str | None
    usage: dict[str, int | None] = field(default_factory=dict)
    attempts: int = 1


def endpoint_name() -> str:
    return os.getenv(ENDPOINT_ENV, "").strip() or DEFAULT_ENDPOINT


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, "") or default)
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return max(0, int(os.getenv(name, "") or default))
    except ValueError:
        return default


def _message_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = [p.get("text", "") if isinstance(p, dict) else str(p) for p in content]
        return "".join(parts)
    return "" if content is None else str(content)


def _reply(response: Any, attempts: int) -> ModelReply:
    choices = getattr(response, "choices", None) or []
    message = getattr(choices[0], "message", None) if choices else None
    text = _message_text(getattr(message, "content", None))
    usage = getattr(response, "usage", None)
    return ModelReply(
        text=text,
        model=getattr(response, "model", None),
        served_model_name=getattr(response, "served_model_name", None),
        usage={
            "prompt_tokens": getattr(usage, "prompt_tokens", None),
            "completion_tokens": getattr(usage, "completion_tokens", None),
            "total_tokens": getattr(usage, "total_tokens", None),
        },
        attempts=attempts,
    )


def _sdk_messages(messages: tuple[dict[str, str], ...]) -> list[ChatMessage]:
    return [ChatMessage(role=ChatMessageRole(m["role"]), content=m["content"]) for m in messages]


async def query_chat(
    messages: tuple[dict[str, str], ...],
    *,
    endpoint: str | None = None,
    client: Any | None = None,
    timeout_s: float | None = None,
    retries: int | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> ModelReply:
    """Query a chat endpoint with a per-attempt timeout and bounded retries."""
    name = endpoint or endpoint_name()
    api = client or WorkspaceClient()
    limit = timeout_s if timeout_s is not None else _env_float(TIMEOUT_ENV, DEFAULT_TIMEOUT_S)
    max_retries = retries if retries is not None else _env_int(RETRIES_ENV, DEFAULT_RETRIES)
    payload = _sdk_messages(messages)
    last: BaseException | None = None
    for attempt in range(1, max_retries + 2):
        try:
            response = await asyncio.wait_for(
                asyncio.to_thread(
                    api.serving_endpoints.query,
                    name=name,
                    messages=payload,
                    max_tokens=MAX_TOKENS,
                    temperature=0.0,
                ),
                timeout=limit,
            )
            return _reply(response, attempt)
        except _TRANSIENT as exc:
            last = exc
            if attempt <= max_retries:
                await sleep(BACKOFF_S * 2 ** (attempt - 1))
        except Exception as exc:
            raise ModelCallError(
                f"{name}: {type(exc).__name__}: {exc}", kind="endpoint", attempts=attempt
            ) from exc
    kind = "timeout" if isinstance(last, TimeoutError) else "endpoint"
    detail = (
        f"timed out after {limit:g}s" if kind == "timeout" else f"{type(last).__name__}: {last}"
    )
    raise ModelCallError(f"{name}: {detail}", kind=kind, attempts=max_retries + 1) from last
