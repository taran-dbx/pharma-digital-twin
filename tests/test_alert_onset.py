"""Excursion onset recorded on the alert ``opened`` event [qa]."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from iot_twin.domain.catalog import list_alert_rules
from iot_twin.services.alerts.alert_engine import AlertEngine, onset_fields, reading_time

START = datetime(2026, 11, 1, 8, 0, tzinfo=UTC)


class Clock:
    def __init__(self) -> None:
        self.instant = START

    def __call__(self) -> datetime:
        return self.instant


def _rule(duration_s: float):
    base = next(
        r for r in list_alert_rules("manufacturing") if r.rule_id == "mfg-alert-spindle-temp"
    )
    return replace(base, duration_s=duration_s)


def _reading(value: float, timestamp=None) -> dict:
    reading = {
        "sensor_id": "mfg-sns-cnc_1_spindle_temp",
        "sensor_name": "cnc_1_spindle_temp",
        "value": value,
    }
    if timestamp is not None:
        reading["timestamp"] = timestamp
    return reading


class TestReadingTime:
    @pytest.mark.parametrize(
        "raw",
        [START.timestamp(), START.timestamp() * 1000, START.isoformat(), "2026-11-01T08:00:00Z"],
    )
    def test_parses_epoch_and_iso_c7955_smoke(self, raw) -> None:
        """[C7955][smoke] Epoch seconds, epoch ms and ISO-8601 all parse to UTC."""
        assert reading_time({"timestamp": raw}) == START

    @pytest.mark.parametrize("raw", [None, "garbage", True, float("inf")])
    def test_unknown_is_none_c7956_regression(self, raw) -> None:
        """[C7956][regression] Missing or unparseable timestamps give no onset."""
        assert reading_time({"timestamp": raw}) is None

    def test_fallback_is_opened_minus_duration_c7957_smoke(self) -> None:
        """[C7957][smoke] Unknown onset falls back to opened_at − rule.duration_s."""
        out = onset_fields(None, START, 2.0)
        assert out == {
            "onset_at": (START - timedelta(seconds=2)).isoformat(),
            "onset_source": "rule_duration",
        }


class TestEngineOnset:
    @pytest.mark.asyncio
    async def test_first_over_limit_reading_is_onset_c7958_smoke(self) -> None:
        """[C7958][smoke] The opened event carries the first over-limit reading's time."""
        clock = Clock()
        engine = AlertEngine(now_fn=clock)
        rule = _rule(10.0)
        first_ts = START - timedelta(seconds=1)
        await engine.evaluate(
            [_reading(90, first_ts.isoformat())], [rule], industry="manufacturing"
        )
        clock.instant = START + timedelta(seconds=10)
        opened = await engine.evaluate(
            [_reading(91, (START + timedelta(seconds=9)).isoformat())],
            [rule],
            industry="manufacturing",
        )
        assert opened[0].extra["onset_at"] == first_ts.isoformat()
        assert opened[0].extra["onset_source"] == "first_reading"

    @pytest.mark.asyncio
    async def test_no_timestamp_uses_rule_duration_c7959_regression(self) -> None:
        """[C7959][regression] Readings without a timestamp record opened_at − duration."""
        clock = Clock()
        engine = AlertEngine(now_fn=clock)
        opened = await engine.evaluate([_reading(95)], [_rule(0.0)], industry="manufacturing")
        assert opened[0].extra["onset_source"] == "rule_duration"
        assert opened[0].extra["onset_at"] == START.isoformat()

    @pytest.mark.asyncio
    async def test_in_limit_reading_resets_onset_c7960_regression(self) -> None:
        """[C7960][regression] Dropping back inside the limit starts a fresh onset."""
        clock = Clock()
        engine = AlertEngine(now_fn=clock)
        rule = _rule(5.0)
        await engine.evaluate([_reading(90, START.isoformat())], [rule])
        clock.instant = START + timedelta(seconds=2)
        await engine.evaluate([_reading(40, clock.instant.isoformat())], [rule])
        later = START + timedelta(seconds=3)
        clock.instant = later
        await engine.evaluate([_reading(90, later.isoformat())], [rule])
        clock.instant = later + timedelta(seconds=5)
        opened = await engine.evaluate([_reading(90, clock.instant.isoformat())], [rule])
        assert opened[0].extra["onset_at"] == later.isoformat()
