"""Admission control for AI action-plan generation.

A plan costs ~8k tokens and ~20 s of endpoint time, so ``POST
/api/alerts/{id}/action-plan`` is bounded three ways (each refusal is a 429
with ``Retry-After``):

* a process-wide concurrency cap (``GENAI_MAX_CONCURRENT``, default 2),
* one generation in flight per alert,
* a per-user cooldown between requests (``GENAI_USER_COOLDOWN_S``, default 20 s).

The AI Gateway rate limits on the endpoint still apply on top of this.
"""

from __future__ import annotations

import asyncio
import math
import os
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any

CONCURRENCY_ENV = "GENAI_MAX_CONCURRENT"
COOLDOWN_ENV = "GENAI_USER_COOLDOWN_S"
DEFAULT_CONCURRENCY = 2
DEFAULT_COOLDOWN_S = 20.0
BUSY_RETRY_S = 10


class RateLimited(RuntimeError):
    def __init__(self, message: str, retry_after_s: int) -> None:
        super().__init__(message)
        self.retry_after_s = max(1, retry_after_s)


def _env_number(name: str, default: float) -> float:
    try:
        value = float(os.getenv(name, "") or default)
    except ValueError:
        return default
    return value if value >= 0 else default


class GenaiLimiter:
    """Concurrency cap, per-alert single flight and per-user cooldown."""

    def __init__(
        self,
        max_concurrent: int | None = None,
        cooldown_s: float | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.max_concurrent = max(
            1, int(max_concurrent or _env_number(CONCURRENCY_ENV, DEFAULT_CONCURRENCY))
        )
        self.cooldown_s = (
            cooldown_s if cooldown_s is not None else _env_number(COOLDOWN_ENV, DEFAULT_COOLDOWN_S)
        )
        self._clock = clock
        self._lock = asyncio.Lock()
        self._running = 0
        self._alerts: frozenset[str] = frozenset()
        self._last_by_user: dict[str, float] = {}

    def _check(self, user: str, alert_id: str, now: float) -> None:
        if alert_id in self._alerts:
            raise RateLimited(
                "an action plan for this alert is already being generated", BUSY_RETRY_S
            )
        last = self._last_by_user.get(user)
        if last is not None and now - last < self.cooldown_s:
            wait = math.ceil(self.cooldown_s - (now - last))
            raise RateLimited(f"wait {wait} s before requesting another action plan", wait)
        if self._running >= self.max_concurrent:
            raise RateLimited("the AI action plan service is busy", BUSY_RETRY_S)

    @asynccontextmanager
    async def admit(self, user: str, alert_id: str) -> AsyncIterator[None]:
        """Reserve a slot or raise :class:`RateLimited`; released on exit."""
        async with self._lock:
            now = self._clock()
            self._check(user, alert_id, now)
            self._running += 1
            self._alerts = self._alerts | {alert_id}
            self._last_by_user = {**self._last_by_user, user: now}
        try:
            yield
        finally:
            async with self._lock:
                self._running -= 1
                self._alerts = self._alerts - {alert_id}


def limiter_for(state: Any) -> GenaiLimiter:
    limiter = getattr(state, "genai_limiter", None)
    if limiter is None:
        limiter = GenaiLimiter()
        state.genai_limiter = limiter
    return limiter
