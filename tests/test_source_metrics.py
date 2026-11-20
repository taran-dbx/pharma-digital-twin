"""Tests for iot_twin.models.source_metrics — SourceMetrics rolling metrics."""

from __future__ import annotations

import time

import pytest

from iot_twin.models.source_metrics import SourceMetrics, create_default_sources


class TestSourceMetricsInit:
    """Verify that a freshly created SourceMetrics has correct defaults."""

    def test_name_and_display_name(self) -> None:
        m = SourceMetrics("ws", "WebSocket")
        assert m.name == "ws"
        assert m.display_name == "WebSocket"

    def test_initial_status_is_initializing(self) -> None:
        m = SourceMetrics("ws", "WebSocket")
        assert m.status == "initializing"

    def test_initial_total_records_is_zero(self) -> None:
        m = SourceMetrics("ws", "WebSocket")
        assert m.total_records == 0

    def test_initial_error_is_none(self) -> None:
        m = SourceMetrics("ws", "WebSocket")
        assert m.error is None

    def test_initial_latency_samples_empty(self) -> None:
        m = SourceMetrics("ws", "WebSocket")
        assert len(m.latency_samples) == 0

    def test_initial_e2e_latency_samples_empty(self) -> None:
        m = SourceMetrics("ws", "WebSocket")
        assert len(m.e2e_latency_samples) == 0


class TestRecordWrite:
    """Verify record_write updates counters and samples."""

    def test_updates_total_records(self) -> None:
        m = SourceMetrics("ws", "WS")
        m.record_write(10, 5.0)
        assert m.total_records == 10

    def test_accumulates_total_records(self) -> None:
        m = SourceMetrics("ws", "WS")
        m.record_write(10, 5.0)
        m.record_write(20, 3.0)
        assert m.total_records == 30

    def test_appends_latency_sample(self) -> None:
        m = SourceMetrics("ws", "WS")
        m.record_write(1, 12.5)
        assert list(m.latency_samples) == [12.5]

    def test_appends_write_timestamp(self) -> None:
        m = SourceMetrics("ws", "WS")
        m.record_write(1, 1.0)
        assert len(m.write_timestamps) == 1

    def test_e2e_latency_recorded_when_provided(self) -> None:
        m = SourceMetrics("ws", "WS")
        m.record_write(1, 1.0, e2e_latency_ms=25.0)
        assert list(m.e2e_latency_samples) == [25.0]

    def test_e2e_latency_not_recorded_when_none(self) -> None:
        m = SourceMetrics("ws", "WS")
        m.record_write(1, 1.0, e2e_latency_ms=None)
        assert len(m.e2e_latency_samples) == 0

    def test_updates_last_write_timestamp(self) -> None:
        m = SourceMetrics("ws", "WS")
        before = time.time()
        m.record_write(1, 1.0)
        after = time.time()
        assert before <= m._last_write <= after


class TestRecordsPerSec:
    """Verify records_per_sec calculation."""

    def test_zero_with_no_writes(self) -> None:
        m = SourceMetrics("ws", "WS")
        assert m.records_per_sec == 0.0

    def test_zero_with_single_write(self) -> None:
        m = SourceMetrics("ws", "WS")
        m.record_write(1, 1.0)
        assert m.records_per_sec == 0.0

    def test_positive_with_multiple_writes_over_time(self) -> None:
        m = SourceMetrics("ws", "WS")
        # Manually set timestamps to simulate time passing
        m.write_timestamps.append(100.0)
        m.write_timestamps.append(100.5)
        m.write_timestamps.append(101.0)
        # 3 events over 1 second = 3.0/sec
        assert m.records_per_sec == pytest.approx(3.0, abs=0.1)

    def test_zero_when_window_too_small(self) -> None:
        m = SourceMetrics("ws", "WS")
        m.write_timestamps.append(100.0)
        m.write_timestamps.append(100.0)  # same timestamp
        assert m.records_per_sec == 0.0


class TestLatencyProperties:
    """Verify avg and p95 latency calculations."""

    def test_avg_latency_empty(self) -> None:
        m = SourceMetrics("ws", "WS")
        assert m.avg_latency_ms == 0.0

    def test_avg_latency_single(self) -> None:
        m = SourceMetrics("ws", "WS")
        m.record_write(1, 10.0)
        assert m.avg_latency_ms == 10.0

    def test_avg_latency_multiple(self) -> None:
        m = SourceMetrics("ws", "WS")
        for lat in [10.0, 20.0, 30.0]:
            m.record_write(1, lat)
        assert m.avg_latency_ms == pytest.approx(20.0)

    def test_p95_latency_empty(self) -> None:
        m = SourceMetrics("ws", "WS")
        assert m.p95_latency_ms == 0.0

    def test_p95_latency_picks_high_value(self) -> None:
        m = SourceMetrics("ws", "WS")
        # 20 samples: 1..19 plus one outlier at 100
        for i in range(1, 20):
            m.record_write(1, float(i))
        m.record_write(1, 100.0)
        # p95 should be near the top of the distribution
        assert m.p95_latency_ms >= 18.0


class TestE2ELatencyProperties:
    """Verify e2e latency average and p95."""

    def test_avg_e2e_latency_empty(self) -> None:
        m = SourceMetrics("ws", "WS")
        assert m.avg_e2e_latency_ms == 0.0

    def test_avg_e2e_latency_with_data(self) -> None:
        m = SourceMetrics("ws", "WS")
        m.record_write(1, 1.0, e2e_latency_ms=50.0)
        m.record_write(1, 1.0, e2e_latency_ms=100.0)
        assert m.avg_e2e_latency_ms == pytest.approx(75.0)

    def test_p95_e2e_latency_empty(self) -> None:
        m = SourceMetrics("ws", "WS")
        assert m.p95_e2e_latency_ms == 0.0

    def test_p95_e2e_latency_with_data(self) -> None:
        m = SourceMetrics("ws", "WS")
        for i in range(1, 21):
            m.record_write(1, 1.0, e2e_latency_ms=float(i))
        assert m.p95_e2e_latency_ms >= 19.0


class TestSecondsSinceLastWrite:
    """Verify seconds_since_last_write."""

    def test_returns_negative_one_before_any_write(self) -> None:
        m = SourceMetrics("ws", "WS")
        assert m.seconds_since_last_write == -1

    def test_returns_small_positive_after_write(self) -> None:
        m = SourceMetrics("ws", "WS")
        m.record_write(1, 1.0)
        elapsed = m.seconds_since_last_write
        assert 0 <= elapsed < 1.0


class TestToDict:
    """Verify to_dict output shape and values."""

    def test_contains_all_expected_keys(self) -> None:
        m = SourceMetrics("ws", "WebSocket")
        d = m.to_dict()
        expected_keys = {
            "name",
            "display_name",
            "status",
            "error",
            "total_records",
            "records_per_sec",
            "avg_latency_ms",
            "p95_latency_ms",
            "avg_e2e_latency_ms",
            "p95_e2e_latency_ms",
            "seconds_since_last_write",
        }
        assert expected_keys.issubset(d.keys())

    def test_name_and_display_name_in_dict(self) -> None:
        m = SourceMetrics("lb", "Lakebase")
        d = m.to_dict()
        assert d["name"] == "lb"
        assert d["display_name"] == "Lakebase"

    def test_values_are_rounded(self) -> None:
        m = SourceMetrics("ws", "WS")
        m.record_write(1, 12.3456)
        d = m.to_dict()
        assert d["avg_latency_ms"] == 12.3

    def test_includes_config_when_sink_config_present(self) -> None:
        m = SourceMetrics("ws", "WS")
        m.sink_config = type(
            "C",
            (),
            {
                "rate_hz": 10,
                "batch_size": 50,
                "max_buffer_size": 1000,
                "backpressure": True,
                "retry_count": 3,
                "retry_delay_s": 1.0,
            },
        )()
        d = m.to_dict()
        assert "config" in d
        assert d["config"]["rate_hz"] == 10

    def test_excludes_config_when_no_sink_config(self) -> None:
        m = SourceMetrics("ws", "WS")
        d = m.to_dict()
        assert "config" not in d


class TestCreateDefaultSources:
    """Verify the factory function for built-in sources."""

    def test_returns_two_sources(self) -> None:
        sources = create_default_sources()
        assert len(sources) == 2

    def test_keys_match_expected_names(self) -> None:
        sources = create_default_sources()
        assert set(sources.keys()) == {"zerobus", "sss"}

    def test_each_value_is_source_metrics(self) -> None:
        sources = create_default_sources()
        for v in sources.values():
            assert isinstance(v, SourceMetrics)
