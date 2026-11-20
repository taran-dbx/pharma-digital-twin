"""SSE lifecycle payload helper tests [qa]."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from iot_twin.core.state import AppState
from iot_twin.services.alerts.alert_engine import AlertRecord
from iot_twin.services.alerts.alert_sse import (
    EVENT_ACKNOWLEDGED,
    EVENT_CLEARED,
    EVENT_OPENED,
    EVENT_UPDATED,
    LIFECYCLE_EVENT_TYPES,
    build_lifecycle_payload,
    enqueue_alert_events,
)


def _alert(**overrides) -> AlertRecord:
    now = datetime(2026, 10, 13, 12, 0, tzinfo=UTC)
    payload = {
        "id": "alert-1",
        "fingerprint": "abc123",
        "industry": "manufacturing",
        "pipeline_id": "industry:manufacturing",
        "rule_id": "mfg-alert-spindle-temp",
        "sensor_id": "mfg-sns-cnc_1_spindle_temp",
        "sensor_ids": ["mfg-sns-cnc_1_spindle_temp"],
        "severity": "crit",
        "status": "open",
        "fault_code": "M-SPINDLE-OVERTEMP",
        "value": 85.0,
        "threshold": 80.0,
        "opened_at": now,
        "updated_at": now,
        "acknowledged_at": None,
        "acknowledged_by": None,
        "cleared_at": None,
    }
    payload.update(overrides)
    return AlertRecord(**payload)


class TestAlertSsePayloads:
    def test_lifecycle_types_are_exhaustive_c7411_smoke(self) -> None:
        """[C7411][smoke] Helper covers opened/updated/acknowledged/cleared."""
        assert LIFECYCLE_EVENT_TYPES == (
            EVENT_OPENED,
            EVENT_UPDATED,
            EVENT_ACKNOWLEDGED,
            EVENT_CLEARED,
        )
        assert LIFECYCLE_EVENT_TYPES == (
            "alert_opened",
            "alert_updated",
            "alert_acknowledged",
            "alert_cleared",
        )

    def test_payload_includes_full_lifecycle_fields_c7411_regression(self) -> None:
        """[C7411][regression] Each lifecycle payload includes the full alert snapshot."""
        required = {
            "type",
            "alert_id",
            "fingerprint",
            "industry",
            "pipeline_id",
            "rule_id",
            "sensor_id",
            "sensor_ids",
            "severity",
            "status",
            "fault_code",
            "value",
            "threshold",
            "opened_at",
            "updated_at",
            "acknowledged_at",
            "acknowledged_by",
            "cleared_at",
        }
        for event_type in LIFECYCLE_EVENT_TYPES:
            payload = build_lifecycle_payload(event_type, _alert())
            assert required <= set(payload)
            assert payload["type"] == event_type
            assert payload["alert_id"] == "alert-1"

    def test_enqueue_puts_on_all_sse_clients_c7411_smoke(self) -> None:
        """[C7411][smoke] SSE helper fans payloads to every registered client."""
        state = AppState()
        q1: asyncio.Queue = asyncio.Queue()
        q2: asyncio.Queue = asyncio.Queue()
        state.sse_clients.extend([q1, q2])
        payload = build_lifecycle_payload(EVENT_OPENED, _alert())
        enqueue_alert_events(state, [payload])
        assert q1.get_nowait()["type"] == "alert_opened"
        assert q2.get_nowait()["type"] == "alert_opened"

    def test_industry_filter_skips_other_lifecycle_c7411_defect(self) -> None:
        """[C7411][defect] Server-side SSE filter keeps other-industry alerts off the queue."""
        state = AppState()
        matching: asyncio.Queue = asyncio.Queue()
        matching.filter_industry = "manufacturing"
        other: asyncio.Queue = asyncio.Queue()
        other.filter_industry = "energy"
        raw: asyncio.Queue = asyncio.Queue()
        state.sse_clients.extend([matching, other, raw])
        payload = build_lifecycle_payload(EVENT_OPENED, _alert())
        enqueue_alert_events(state, [payload])
        assert matching.get_nowait()["industry"] == "manufacturing"
        assert other.empty()
        assert raw.get_nowait()["type"] == "alert_opened"
