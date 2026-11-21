"""Minimal circuit breaker for calls to a model serving endpoint."""

from __future__ import annotations

import time
from collections.abc import Callable

CLOSED = "closed"
OPEN = "open"
HALF_OPEN = "half_open"


class CircuitBreaker:
    """Opens after ``failure_threshold`` consecutive failures.

    While open, calls are refused for ``cooldown_s``; then one trial call is
    allowed (half-open). Success closes the breaker; failure re-opens it.
    """

    def __init__(
        self,
        failure_threshold: int,
        cooldown_s: float,
        now_fn: Callable[[], float] = time.monotonic,
    ) -> None:
        self.failure_threshold = max(1, int(failure_threshold))
        self.cooldown_s = max(0.0, float(cooldown_s))
        self._now = now_fn
        self.state = CLOSED
        self.consecutive_failures = 0
        self._opened_at = 0.0

    def allow(self) -> bool:
        if self.state == OPEN and self._now() - self._opened_at >= self.cooldown_s:
            self.state = HALF_OPEN
            return True
        return self.state == CLOSED

    def record_success(self) -> None:
        self.state = CLOSED
        self.consecutive_failures = 0

    def record_failure(self) -> None:
        self.consecutive_failures += 1
        if self.state == HALF_OPEN or self.consecutive_failures >= self.failure_threshold:
            self.state = OPEN
            self._opened_at = self._now()
