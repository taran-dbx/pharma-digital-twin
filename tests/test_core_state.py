"""Tests for iot_twin.core.state — AppState dataclass and get_app_state dependency."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

from iot_twin.core.state import AppState, get_app_state


class TestAppStateDefaults:
    """Verify that a freshly created AppState has sensible defaults."""

    def test_db_defaults_to_none(self) -> None:
        state = AppState()
        assert state.db is None

    def test_db_available_defaults_to_false(self) -> None:
        state = AppState()
        assert state.db_available is False

    def test_db_error_defaults_to_none(self) -> None:
        state = AppState()
        assert state.db_error is None

    def test_connected_clients_defaults_to_empty_list(self) -> None:
        state = AppState()
        assert state.connected_clients == []

    def test_simulator_task_defaults_to_none(self) -> None:
        state = AppState()
        assert state.simulator_task is None

    def test_alert_snapshot_sync_defaults_c7505_smoke(self) -> None:
        """[C7505][smoke] Snapshot synchronizer is unset until lifespan starts it."""
        state = AppState()
        assert state.alert_snapshot_sync_task is None
        assert state.alert_snapshot_synchronizer is None
        assert state.genie_conversations == {}

    def test_refresh_task_defaults_to_none(self) -> None:
        state = AppState()
        assert state.refresh_task is None

    def test_shutdown_event_is_asyncio_event(self) -> None:
        state = AppState()
        assert isinstance(state.shutdown_event, asyncio.Event)
        assert not state.shutdown_event.is_set()

    def test_producer_states_defaults_to_empty_dict(self) -> None:
        state = AppState()
        assert state.producer_states == {}

    def test_industries_defaults_to_empty_string(self) -> None:
        state = AppState()
        assert state.industries == ""

    def test_update_rate_hz_defaults_to_2(self) -> None:
        state = AppState()
        assert state.update_rate_hz == 2.0


class TestAppStateSources:
    """Verify the default sources dict is populated correctly."""

    def test_sources_has_two_entries(self) -> None:
        state = AppState()
        assert len(state.sources) == 2

    def test_sources_omits_in_app_sinks(self) -> None:
        # Neither the legacy in-app WebSocket simulator nor Lakebase are
        # listed — both only populate via per-pipeline SSS/producer jobs.
        state = AppState()
        assert "websocket" not in state.sources
        assert "lakebase" not in state.sources

    def test_sources_contains_zerobus(self) -> None:
        state = AppState()
        assert "zerobus" in state.sources

    def test_sources_contains_sss(self) -> None:
        state = AppState()
        assert "sss" in state.sources

    def test_each_source_is_source_metrics(self) -> None:
        from iot_twin.models.source_metrics import SourceMetrics

        state = AppState()
        for src in state.sources.values():
            assert isinstance(src, SourceMetrics)


class TestGetAppState:
    """Verify the FastAPI dependency returns the correct object."""

    def test_returns_app_state_from_request(self) -> None:
        state = AppState()
        mock_request = MagicMock()
        mock_request.app.state.app_state = state
        result = get_app_state(mock_request)
        assert result is state

    def test_different_states_are_isolated(self) -> None:
        state_a = AppState(industries="automotive")
        state_b = AppState(industries="pharmaceutical")

        request_a = MagicMock()
        request_a.app.state.app_state = state_a
        request_b = MagicMock()
        request_b.app.state.app_state = state_b

        assert get_app_state(request_a).industries == "automotive"
        assert get_app_state(request_b).industries == "pharmaceutical"
