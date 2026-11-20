"""Tests: single Lakebase OAuth connection at boot; no demo loops.

Asserts that after lifespan startup:
  - Exactly ONE AppBackendConnectionManager is created (OAuth, iot-app-backend).
  - That manager is assigned to both state.db and state.app_backend_db (same
    object identity — callers that read either field get the same pool).
  - The four schema ensure_* calls are made: ensure_app_schema,
    ensure_pipelines_table, ensure_user_settings_table,
    ensure_source_bindings_table.
  - state.refresh_task is None  (matview-refresh loop not started).
  - state.retention_task is None (retention loop not started).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_manager() -> MagicMock:
    """Return a fully mocked AppBackendConnectionManager."""
    mgr = MagicMock()
    mgr.initialize = AsyncMock()
    mgr.ensure_pipelines_table = AsyncMock()
    mgr.ensure_user_settings_table = AsyncMock()
    mgr.ensure_source_bindings_table = AsyncMock()
    mgr.close = AsyncMock()
    return mgr


async def _run_lifespan(monkeypatch, mock_mgr: MagicMock):
    """Boot the app lifespan with all external I/O mocked.

    Patches:
    - iot_twin.services.db.app_backend_db.build_app_backend_manager → mock_mgr
    - iot_twin.models.app_schema.ensure_app_schema              → AsyncMock
    - app.run_simulator                                → AsyncMock (no loop)
    - app.install_sigterm_handler                      → no-op
    - app.shutdown                                     → AsyncMock (no-op cleanup)

    Returns (AppState, ensure_app_schema_mock).
    """
    monkeypatch.setenv("PGHOST", "example-pg.database.cloud.databricks.com")
    monkeypatch.setenv("INDUSTRIES", "pharmaceutical")

    mock_ensure_schema = AsyncMock()

    with (
        patch(
            "iot_twin.services.db.app_backend_db.build_app_backend_manager",
            return_value=mock_mgr,
        ),
        patch("iot_twin.models.app_schema.ensure_app_schema", mock_ensure_schema),
        patch("iot_twin.app.run_simulator", AsyncMock()),
        patch("iot_twin.app.install_sigterm_handler"),
        patch("iot_twin.app.shutdown", AsyncMock()),
    ):
        from fastapi import FastAPI

        import iot_twin.app as app_module

        fa = FastAPI()
        async with app_module.lifespan(fa):
            state = fa.state.app_state

        return state, mock_ensure_schema


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_db_and_app_backend_db_are_same_manager(monkeypatch: pytest.MonkeyPatch) -> None:
    """state.db and state.app_backend_db must be the identical object."""
    mock_mgr = _make_mock_manager()
    state, _ = await _run_lifespan(monkeypatch, mock_mgr)
    assert state.db is mock_mgr
    assert state.app_backend_db is mock_mgr
    assert state.db is state.app_backend_db


async def test_single_manager_initialized_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """The one manager must be initialized exactly once."""
    mock_mgr = _make_mock_manager()
    await _run_lifespan(monkeypatch, mock_mgr)
    mock_mgr.initialize.assert_called_once()


async def test_ensure_app_schema_called(monkeypatch: pytest.MonkeyPatch) -> None:
    """ensure_app_schema must be called with the single manager."""
    mock_mgr = _make_mock_manager()
    _, mock_ensure_schema = await _run_lifespan(monkeypatch, mock_mgr)
    mock_ensure_schema.assert_called_once_with(mock_mgr)


async def test_ensure_pipelines_table_called(monkeypatch: pytest.MonkeyPatch) -> None:
    """ensure_pipelines_table must be called on the single manager."""
    mock_mgr = _make_mock_manager()
    await _run_lifespan(monkeypatch, mock_mgr)
    mock_mgr.ensure_pipelines_table.assert_called_once()


async def test_ensure_user_settings_table_called(monkeypatch: pytest.MonkeyPatch) -> None:
    """ensure_user_settings_table must be called on the single manager."""
    mock_mgr = _make_mock_manager()
    await _run_lifespan(monkeypatch, mock_mgr)
    mock_mgr.ensure_user_settings_table.assert_called_once()


async def test_ensure_source_bindings_table_called(monkeypatch: pytest.MonkeyPatch) -> None:
    """ensure_source_bindings_table must be called on the single manager."""
    mock_mgr = _make_mock_manager()
    await _run_lifespan(monkeypatch, mock_mgr)
    mock_mgr.ensure_source_bindings_table.assert_called_once()


async def test_no_matview_refresh_task(monkeypatch: pytest.MonkeyPatch) -> None:
    """The materialized-view refresh loop must NOT be started."""
    mock_mgr = _make_mock_manager()
    state, _ = await _run_lifespan(monkeypatch, mock_mgr)
    assert state.refresh_task is None


async def test_no_retention_task(monkeypatch: pytest.MonkeyPatch) -> None:
    """The data-retention loop must NOT be started."""
    mock_mgr = _make_mock_manager()
    state, _ = await _run_lifespan(monkeypatch, mock_mgr)
    assert state.retention_task is None
