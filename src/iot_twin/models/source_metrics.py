"""Rolling metrics for each data source (WebSocket, Lakebase, Zerobus, SSS)."""

from __future__ import annotations

import collections
import time
from typing import Any


class SourceMetrics:
    """Rolling metrics for a single data source.

    Optionally holds a live reference to the sink's ``SinkConfig`` and
    ``SinkRunner`` so the REST API can read/mutate throughput knobs and
    report buffer depth at runtime.
    """

    def __init__(self, name: str, display_name: str) -> None:
        self.name = name
        self.display_name = display_name
        self.status: str = "initializing"
        self.error: str | None = None
        self.total_records: int = 0
        self.latency_samples: collections.deque[float] = collections.deque(maxlen=200)
        self.e2e_latency_samples: collections.deque[float] = collections.deque(maxlen=200)
        self.write_timestamps: collections.deque[float] = collections.deque(maxlen=200)
        self._last_write: float = 0.0
        self.sink_config: Any = None
        self._sink_runner: Any = None

    def record_write(
        self,
        record_count: int,
        write_latency_ms: float,
        e2e_latency_ms: float | None = None,
    ) -> None:
        now = time.time()
        self.total_records += record_count
        self.latency_samples.append(write_latency_ms)
        if e2e_latency_ms is not None:
            self.e2e_latency_samples.append(e2e_latency_ms)
        self.write_timestamps.append(now)
        self._last_write = now

    @property
    def records_per_sec(self) -> float:
        if len(self.write_timestamps) < 2:
            return 0.0
        window = self.write_timestamps[-1] - self.write_timestamps[0]
        if window < 0.1:
            return 0.0
        return len(self.write_timestamps) / window

    @property
    def avg_latency_ms(self) -> float:
        if not self.latency_samples:
            return 0.0
        return sum(self.latency_samples) / len(self.latency_samples)

    @property
    def p95_latency_ms(self) -> float:
        if not self.latency_samples:
            return 0.0
        s = sorted(self.latency_samples)
        idx = int(len(s) * 0.95)
        return s[min(idx, len(s) - 1)]

    @property
    def avg_e2e_latency_ms(self) -> float:
        if not self.e2e_latency_samples:
            return 0.0
        return sum(self.e2e_latency_samples) / len(self.e2e_latency_samples)

    @property
    def p95_e2e_latency_ms(self) -> float:
        if not self.e2e_latency_samples:
            return 0.0
        s = sorted(self.e2e_latency_samples)
        idx = int(len(s) * 0.95)
        return s[min(idx, len(s) - 1)]

    @property
    def seconds_since_last_write(self) -> float:
        if self._last_write == 0:
            return -1
        return time.time() - self._last_write

    def to_dict(self) -> dict:
        d: dict[str, Any] = {
            "name": self.name,
            "display_name": self.display_name,
            "status": self.status,
            "error": self.error,
            "total_records": self.total_records,
            "records_per_sec": round(self.records_per_sec, 1),
            "avg_latency_ms": round(self.avg_latency_ms, 1),
            "p95_latency_ms": round(self.p95_latency_ms, 1),
            "avg_e2e_latency_ms": round(self.avg_e2e_latency_ms, 1),
            "p95_e2e_latency_ms": round(self.p95_e2e_latency_ms, 1),
            "seconds_since_last_write": round(self.seconds_since_last_write, 1),
        }
        if self.sink_config is not None:
            cfg = self.sink_config
            d["config"] = {
                "rate_hz": cfg.rate_hz,
                "batch_size": cfg.batch_size,
                "max_buffer_size": cfg.max_buffer_size,
                "backpressure": cfg.backpressure,
                "retry_count": cfg.retry_count,
                "retry_delay_s": cfg.retry_delay_s,
            }
        if self._sink_runner is not None:
            try:
                d["buffer_len"] = len(self._sink_runner._buffer)
            except Exception:
                d["buffer_len"] = 0
        return d


def create_default_sources() -> dict[str, SourceMetrics]:
    """Factory for the two built-in data sources.

    Neither Lakebase nor the legacy in-app WebSocket simulator are listed here
    anymore — data arrives exclusively from per-pipeline jobs:
      * ``sss`` — Spark Structured Streaming consumer (/api/sss/ingest)
      * ``zerobus`` — Zerobus Delta ingest (optional)
    """
    return {
        "zerobus": SourceMetrics("zerobus", "Zerobus (Delta Ingest)"),
        "sss": SourceMetrics("sss", "Spark Structured Streaming"),
    }
