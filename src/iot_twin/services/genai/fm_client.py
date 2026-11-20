"""Foundation Model call through the serving-endpoint / AI Gateway path.

Uses the app service principal (ambient auth) and ``serving_endpoints.query``
on a pay-per-token chat endpoint. The endpoint name comes from
``GENAI_ENDPOINT`` (injected from the ``genai-endpoint`` app resource,
CAN_QUERY) and defaults to ``databricks-claude-sonnet-5-5``. Usage tracking,
rate limits and guardrails configured on the endpoint's AI Gateway apply to
every call.

Time bounds. The whole call has a budget (``GENAI_REQUEST_BUDGET_S``, default
90 s). Each attempt is bounded by ``GENAI_TIMEOUT_S`` (default 40 s), and the
number of attempts is capped so that ``timeout x attempts`` plus backoff fits
the budget. The SDK client is built for this: its HTTP timeout is shorter than
the attempt limit and its own retry window is no longer than the attempt
limit, so a worker thread never outlives its attempt by more than one HTTP
timeout. Calls run on a small dedicated executor, never the shared default
one. Transient failures (timeouts, 429, 5xx) are retried by the app loop;
permission, not-found and bad-request errors are not.

No ``temperature`` is sent: Claude 5.x endpoints reject it. Determinism comes
from the prompt, the schema and the grounding check.
"""

from __future__ import annotations

import asyncio
import functools
import os
import time
from collections.abc import Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk import errors as sdk_errors
from databricks.sdk.config import Config
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

DEFAULT_ENDPOINT = "databricks-claude-sonnet-5-5"
ENDPOINT_ENV = "GENAI_ENDPOINT"
TIMEOUT_ENV = "GENAI_TIMEOUT_S"
RETRIES_ENV = "GENAI_MAX_RETRIES"
BUDGET_ENV = "GENAI_REQUEST_BUDGET_S"
DEFAULT_TIMEOUT_S = 40.0
DEFAULT_RETRIES = 1
DEFAULT_BUDGET_S = 90.0
MAX_TOKENS = 6000
BACKOFF_S = 1.0
EXECUTOR_WORKERS = 4
DESCRIBE_TIMEOUT_S = 5.0
DESCRIBE_FAILURE_TTL_S = 60.0

_TRANSIENT = (
    TimeoutError,
    sdk_errors.TooManyRequests,
    sdk_errors.RequestLimitExceeded,
    sdk_errors.TemporarilyUnavailable,
    sdk_errors.InternalError,
    sdk_errors.DeadlineExceeded,
    ConnectionError,
)

# Dedicated pool: a slow endpoint can tie up these threads, never the event
# loop's default executor (used by DBSQL, Genie and Lakebase helpers).
EXECUTOR = ThreadPoolExecutor(max_workers=EXECUTOR_WORKERS, thread_name_prefix="genai-fm")


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


@dataclass(frozen=True)
class CallLimits:
    """Per-attempt timeout and attempt count that fit the request budget."""

    timeout_s: float
    attempts: int
    budget_s: float


def endpoint_name() -> str:
    return os.getenv(ENDPOINT_ENV, "").strip() or DEFAULT_ENDPOINT


def _env_float(name: str, default: float) -> float:
    try:
        value = float(os.getenv(name, "") or default)
    except ValueError:
        return default
    return value if value > 0 else default


def _env_int(name: str, default: int) -> int:
    try:
        return max(0, int(os.getenv(name, "") or default))
    except ValueError:
        return default


def call_limits(timeout_s: float | None = None, retries: int | None = None) -> CallLimits:
    """Clamp the attempt count so ``timeout x attempts + backoff <= budget``."""
    budget = _env_float(BUDGET_ENV, DEFAULT_BUDGET_S)
    limit = timeout_s if timeout_s is not None else _env_float(TIMEOUT_ENV, DEFAULT_TIMEOUT_S)
    limit = min(limit, budget)
    wanted = 1 + (retries if retries is not None else _env_int(RETRIES_ENV, DEFAULT_RETRIES))
    attempts = 1
    while attempts < wanted:
        backoff = sum(BACKOFF_S * 2**i for i in range(attempts))
        if limit * (attempts + 1) + backoff > budget:
            break
        attempts += 1
    return CallLimits(timeout_s=limit, attempts=attempts, budget_s=budget)


def client_config_kwargs(limit_s: float) -> dict[str, int]:
    """SDK HTTP timeout shorter than the attempt; SDK retry window no longer than it."""
    return {
        "http_timeout_seconds": max(1, int(limit_s) - 2),
        "retry_timeout_seconds": max(1, int(limit_s)),
    }


@functools.lru_cache(maxsize=4)
def build_client(limit_s: float = DEFAULT_TIMEOUT_S, profile: str | None = None) -> WorkspaceClient:
    """A WorkspaceClient whose timeouts are bounded by ``limit_s`` (cached)."""
    return WorkspaceClient(config=Config(profile=profile, **client_config_kwargs(limit_s)))


async def _in_executor(fn: Callable[..., Any], /, **kwargs: Any) -> Any:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(EXECUTOR, functools.partial(fn, **kwargs))


_described: dict[str, tuple[str | None, str | None]] = {}
_describe_failed_at: dict[str, float] = {}


async def describe_endpoint(name: str, client: Any | None = None) -> tuple[str | None, str | None]:
    """``(served model, version)`` from the endpoint config; best effort, cached.

    Pay-per-token endpoints report e.g. ``system.ai.databricks-claude-sonnet-5-5``
    and the display name ``Claude Sonnet 5.5``. A failure is cached for
    ``DESCRIBE_FAILURE_TTL_S`` so a broken metadata call is not repeated per plan.
    """
    if name in _described:
        return _described[name]
    failed = _describe_failed_at.get(name)
    if failed is not None and time.monotonic() - failed < DESCRIBE_FAILURE_TTL_S:
        return None, None
    try:
        api = client or build_client(DESCRIBE_TIMEOUT_S)
        endpoint = await asyncio.wait_for(
            _in_executor(api.serving_endpoints.get, name=name), timeout=DESCRIBE_TIMEOUT_S
        )
        entity = (getattr(getattr(endpoint, "config", None), "served_entities", None) or [None])[0]
        foundation = getattr(entity, "foundation_model", None)
        model = getattr(foundation, "name", None) or getattr(entity, "entity_name", None)
        version = getattr(entity, "entity_version", None) or getattr(
            foundation, "display_name", None
        )
    except Exception:  # noqa: BLE001 - metadata only; never blocks a plan
        _describe_failed_at[name] = time.monotonic()
        return None, None
    _described[name] = (model, version)
    return model, version


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
    """Query a chat endpoint within the request budget (bounded attempts and timeout)."""
    name = endpoint or endpoint_name()
    limits = call_limits(timeout_s, retries)
    try:
        api = client or build_client(limits.timeout_s)
    except Exception as exc:  # noqa: BLE001 - e.g. no credentials in this runtime
        raise ModelCallError(
            f"{name}: client setup failed: {exc}", kind="endpoint", attempts=0
        ) from exc
    payload = _sdk_messages(messages)
    last: BaseException | None = None
    for attempt in range(1, limits.attempts + 1):
        try:
            response = await asyncio.wait_for(
                _in_executor(
                    api.serving_endpoints.query,
                    name=name,
                    messages=payload,
                    max_tokens=MAX_TOKENS,
                ),
                timeout=limits.timeout_s,
            )
            return _reply(response, attempt)
        except _TRANSIENT as exc:
            last = exc
            if attempt < limits.attempts:
                await sleep(BACKOFF_S * 2 ** (attempt - 1))
        except Exception as exc:
            raise ModelCallError(
                f"{name}: {type(exc).__name__}: {exc}", kind="endpoint", attempts=attempt
            ) from exc
    kind = "timeout" if isinstance(last, TimeoutError) else "endpoint"
    detail = (
        f"timed out after {limits.timeout_s:g}s"
        if kind == "timeout"
        else f"{type(last).__name__}: {last}"
    )
    raise ModelCallError(f"{name}: {detail}", kind=kind, attempts=limits.attempts) from last
