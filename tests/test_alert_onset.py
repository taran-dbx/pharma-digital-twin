"""Excursion onset copied onto the alert ``opened`` event [qa].

The alert row's ``metadata.excursion_started_at`` holds the first over-limit
reading of the *current* episode and is replaced on re-open; the opened event
keeps a per-episode copy for the outcome views.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from iot_twin.domain.catalog import list_alert_rules
from iot_twin.services.alerts.alert_engine import EXCURSION_STARTED_KEY, AlertEngine

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


def _reading(value: float, timestamp: str | None = None) -> dict:
    reading = {
        "sensor_id": "mfg-sns-cnc_1_spindle_temp",
        "sensor_name": "cnc_1_spindle_temp",
        "value": value,
    }
    if timestamp is not None:
        reading["timestamp"] = timestamp
    return reading


class TestOpenedEventOnset:
    @pytest.mark.asyncio
    async def test_first_over_limit_reading_on_event_c7955_smoke(self) -> None:
        """[C7955][smoke] The opened event and the row carry the same first over-limit time."""
        clock = Clock()
        engine = AlertEngine(now_fn=clock)
        rule = _rule(10.0)
        await engine.evaluate([_reading(90)], [rule], industry="manufacturing")
        clock.instant = START + timedelta(seconds=10)
        opened = await engine.evaluate([_reading(91)], [rule], industry="manufacturing")
        assert opened[0].event_type == "alert_opened"
        assert opened[0].extra[EXCURSION_STARTED_KEY] == START.isoformat()
        assert opened[0].alert.metadata[EXCURSION_STARTED_KEY] == START.isoformat()

    @pytest.mark.asyncio
    async def test_device_time_copied_to_event_c7958_smoke(self) -> None:
        """[C7958][smoke] The event copy is the same bounded device-time stamp as the row."""
        clock = Clock()
        engine = AlertEngine(now_fn=clock)
        rule = _rule(10.0)
        device = START - timedelta(seconds=3)
        await engine.evaluate([_reading(90, device.isoformat())], [rule], industry="manufacturing")
        clock.instant = START + timedelta(seconds=10)
        opened = await engine.evaluate([_reading(91)], [rule], industry="manufacturing")
        stamp = opened[0].alert.metadata[EXCURSION_STARTED_KEY]
        assert stamp == device.isoformat()
        assert opened[0].extra[EXCURSION_STARTED_KEY] == stamp

    @pytest.mark.asyncio
    async def test_in_limit_reading_restarts_onset_c7956_regression(self) -> None:
        """[C7956][regression] Dropping back inside the limit starts a fresh onset."""
        clock = Clock()
        engine = AlertEngine(now_fn=clock)
        rule = _rule(5.0)
        await engine.evaluate([_reading(90)], [rule])
        clock.instant = START + timedelta(seconds=2)
        await engine.evaluate([_reading(40)], [rule])
        later = START + timedelta(seconds=3)
        clock.instant = later
        await engine.evaluate([_reading(90)], [rule])
        clock.instant = later + timedelta(seconds=5)
        opened = await engine.evaluate([_reading(90)], [rule])
        assert opened[0].extra[EXCURSION_STARTED_KEY] == later.isoformat()

    @pytest.mark.asyncio
    async def test_updates_do_not_restamp_c7957_regression(self) -> None:
        """[C7957][regression] Only the opened transition carries the onset."""
        engine = AlertEngine()
        rule = _rule(0.0)
        await engine.evaluate([_reading(90)], [rule])
        updated = await engine.evaluate([_reading(95)], [rule])
        assert updated[0].event_type == "alert_updated"
        assert EXCURSION_STARTED_KEY not in updated[0].extra
