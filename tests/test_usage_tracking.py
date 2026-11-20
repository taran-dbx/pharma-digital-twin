"""Tests for the fail-safe dbdemos-tracker integration (services/usage_tracking)."""

from __future__ import annotations

import asyncio
import sys
import types
from unittest.mock import MagicMock

import pytest

from iot_twin.services import usage_tracking as ut
from iot_twin.services.usage_tracking import (
    APP_START_PATH,
    DEFAULT_APP_NAME,
    UsageTracker,
    UsageTrackingConfig,
    UsageTrackingMiddleware,
    event_label,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


def test_config_defaults_when_env_empty() -> None:
    cfg = UsageTrackingConfig.from_env({})
    assert cfg == UsageTrackingConfig(enabled=True, app_name=DEFAULT_APP_NAME, demo_catalog_id=None)


@pytest.mark.parametrize("value", ["false", "FALSE", "0", "no", "off", " Off "])
def test_config_opt_out_values(value: str) -> None:
    assert UsageTrackingConfig.from_env({"USAGE_TRACKING_ENABLED": value}).enabled is False


@pytest.mark.parametrize("value", ["true", "1", "yes", "on", ""])
def test_config_enabled_values(value: str) -> None:
    assert UsageTrackingConfig.from_env({"USAGE_TRACKING_ENABLED": value}).enabled is True


def test_config_reads_app_name_and_catalog_id() -> None:
    cfg = UsageTrackingConfig.from_env(
        {"USAGE_TRACKING_APP_NAME": " my-app ", "USAGE_TRACKING_DEMO_CATALOG_ID": " cat-1 "}
    )
    assert cfg.app_name == "my-app"
    assert cfg.demo_catalog_id == "cat-1"


def test_config_blank_values_fall_back() -> None:
    cfg = UsageTrackingConfig.from_env(
        {"USAGE_TRACKING_APP_NAME": "  ", "USAGE_TRACKING_DEMO_CATALOG_ID": ""}
    )
    assert cfg.app_name == DEFAULT_APP_NAME
    assert cfg.demo_catalog_id is None


def test_config_reads_process_env_by_default(monkeypatch) -> None:
    monkeypatch.setenv("USAGE_TRACKING_ENABLED", "false")
    assert UsageTrackingConfig.from_env().enabled is False


# ---------------------------------------------------------------------------
# Event labels
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("method", "path", "query", "expected"),
    [
        ("GET", "/", "", "/"),
        ("GET", "/industries", "", "/industries"),
        ("GET", "/industries/", "", "/industries"),
        ("GET", "/i/pharmaceutical", "", "/i/pharmaceutical"),
        ("GET", "/i/pharmaceutical/realtime/dashboard", "", "/i/pharmaceutical/realtime/dashboard"),
        ("GET", "/setup-validate", "", "/setup-validate"),
        ("GET", "/pipelines", "", "/pipelines"),
        ("GET", "/pipelines/abc-123/health", "", "/pipelines/{id}/health"),
        ("GET", "/settings", "", "/settings"),
        (
            "POST",
            "/api/admin/industries/manufacturing/setup",
            "",
            "/api/admin/industries/manufacturing/setup",
        ),
        (
            "POST",
            "/api/admin/industries/manufacturing/validate/start",
            "",
            "/api/admin/industries/manufacturing/validate/start",
        ),
        (
            "POST",
            "/api/admin/pipelines/9f1c/deploy/zerobus",
            "",
            "/api/admin/pipelines/{id}/deploy/zerobus",
        ),
        ("POST", "/api/pipelines", "", "/api/pipelines"),
        ("GET", "/api/events", "industry=pharmaceutical", "/dashboard/pharmaceutical/stream"),
        ("GET", "/api/events", "industry=bad%20value", "/dashboard/stream"),
        ("GET", "/api/events", "", "/dashboard/stream"),
    ],
)
def test_event_label_tracked(method: str, path: str, query: str, expected: str) -> None:
    assert event_label(method, path, query) == expected


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("GET", "/api/pipelines"),  # polled list: not a key event
        ("POST", "/"),  # wrong method
        ("GET", "/api/admin/industries/x/setup"),  # status poll, not the setup action
        ("GET", "/assets/index-abc.js"),
        ("GET", "/static/logo.svg"),
        ("GET", "/i/pharmaceutical/notamode/dashboard"),
        ("GET", "/i/" + "x" * 100),  # over-long segment rejected
        ("GET", "/api/health"),
    ],
)
def test_event_label_untracked(method: str, path: str) -> None:
    assert event_label(method, path, "") is None


def test_event_label_never_raises_on_garbage() -> None:
    assert event_label("GET", None, None) is None  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# UsageTracker
# ---------------------------------------------------------------------------


def _enabled(**overrides) -> UsageTrackingConfig:
    return UsageTrackingConfig(
        enabled=True, app_name="app-x", demo_catalog_id=overrides.get("catalog")
    )


def test_tracker_inactive_before_start() -> None:
    factory = MagicMock()
    tracker = UsageTracker(_enabled(), tracker_factory=factory, workspace_id_provider=lambda: "1")
    assert tracker.active is False
    tracker.track("/", "a@databricks.com")  # no-op, no error
    factory.assert_not_called()


def test_start_builds_tracker_with_config() -> None:
    inner = MagicMock()
    factory = MagicMock(return_value=inner)
    tracker = UsageTracker(
        _enabled(catalog="cat-9"), tracker_factory=factory, workspace_id_provider=lambda: 42
    )

    assert asyncio.run(tracker.start()) is True
    assert tracker.active is True
    factory.assert_called_once_with(org_id="42", demo_name="app-x", demo_catalog_id="cat-9")


def test_track_forwards_to_inner_tracker() -> None:
    inner = MagicMock()
    tracker = UsageTracker(
        _enabled(), tracker_factory=MagicMock(return_value=inner), workspace_id_provider=lambda: 1
    )
    asyncio.run(tracker.start())

    tracker.track("/industries", "a@databricks.com")
    inner.track_app_view.assert_called_once_with(
        user_email="a@databricks.com", app_path="/industries"
    )


def test_track_app_start_sends_without_user() -> None:
    inner = MagicMock()
    tracker = UsageTracker(
        _enabled(), tracker_factory=MagicMock(return_value=inner), workspace_id_provider=lambda: 1
    )
    asyncio.run(tracker.start())

    tracker.track_app_start()
    inner.track_app_view.assert_called_once_with(user_email=None, app_path=APP_START_PATH)


def test_track_swallows_inner_errors() -> None:
    inner = MagicMock()
    inner.track_app_view.side_effect = RuntimeError("network down")
    tracker = UsageTracker(
        _enabled(), tracker_factory=MagicMock(return_value=inner), workspace_id_provider=lambda: 1
    )
    asyncio.run(tracker.start())

    tracker.track("/", "a@databricks.com")  # must not raise
    tracker.track_app_start()


def test_start_disabled_never_touches_workspace() -> None:
    provider = MagicMock()
    factory = MagicMock()
    tracker = UsageTracker(
        UsageTrackingConfig(enabled=False, app_name="a", demo_catalog_id=None),
        tracker_factory=factory,
        workspace_id_provider=provider,
    )
    assert asyncio.run(tracker.start()) is False
    provider.assert_not_called()
    factory.assert_not_called()
    assert tracker.active is False


def test_start_fails_safe_when_workspace_lookup_raises() -> None:
    def boom():
        raise RuntimeError("no auth")

    tracker = UsageTracker(_enabled(), tracker_factory=MagicMock(), workspace_id_provider=boom)
    assert asyncio.run(tracker.start()) is False
    assert tracker.active is False


def test_start_fails_safe_when_workspace_id_empty() -> None:
    tracker = UsageTracker(
        _enabled(), tracker_factory=MagicMock(), workspace_id_provider=lambda: None
    )
    assert asyncio.run(tracker.start()) is False


def test_start_fails_safe_on_timeout() -> None:
    import time

    tracker = UsageTracker(
        _enabled(),
        tracker_factory=MagicMock(),
        workspace_id_provider=lambda: time.sleep(0.5) or 1,
        startup_timeout_s=0.05,
    )
    assert asyncio.run(tracker.start()) is False


def test_start_fails_safe_when_factory_raises() -> None:
    tracker = UsageTracker(
        _enabled(),
        tracker_factory=MagicMock(side_effect=ValueError("bad")),
        workspace_id_provider=lambda: 1,
    )
    assert asyncio.run(tracker.start()) is False


def test_start_fails_safe_when_package_missing(monkeypatch) -> None:
    monkeypatch.setitem(sys.modules, "dbdemos_tracker", None)  # import -> ImportError
    tracker = UsageTracker(_enabled(), workspace_id_provider=lambda: 1)
    assert asyncio.run(tracker.start()) is False


def test_default_factory_uses_dbdemos_tracker_package(monkeypatch) -> None:
    fake_cls = MagicMock()
    monkeypatch.setitem(sys.modules, "dbdemos_tracker", types.SimpleNamespace(Tracker=fake_cls))
    tracker = UsageTracker(_enabled(), workspace_id_provider=lambda: 7)
    assert asyncio.run(tracker.start()) is True
    fake_cls.assert_called_once_with(org_id="7", demo_name="app-x", demo_catalog_id=None)


def test_default_workspace_id_provider_uses_sdk(monkeypatch) -> None:
    client = MagicMock()
    client.get_workspace_id.return_value = 123
    fake_sdk = types.SimpleNamespace(WorkspaceClient=MagicMock(return_value=client))
    monkeypatch.setitem(sys.modules, "databricks.sdk", fake_sdk)
    assert ut._default_workspace_id() == 123


def test_from_env_builds_tracker(monkeypatch) -> None:
    monkeypatch.setenv("USAGE_TRACKING_ENABLED", "off")
    assert UsageTracker.from_env().config.enabled is False


# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------


class _RecordingTracker:
    def __init__(self, fail: bool = False) -> None:
        self.calls: list[tuple[str, str | None]] = []
        self.fail = fail

    def track(self, app_path: str, user_email: str | None) -> None:
        if self.fail:
            raise RuntimeError("tracker exploded")
        self.calls.append((app_path, user_email))


def _http_scope(method: str, path: str, query: bytes = b"", email: bytes | None = None) -> dict:
    headers = [(b"host", b"x")]
    if email is not None:
        headers.append((b"x-forwarded-email", email))
    return {
        "type": "http",
        "method": method,
        "path": path,
        "query_string": query,
        "headers": headers,
    }


def _run_middleware(scope: dict, tracker) -> list:
    downstream_calls: list = []

    async def downstream(s, receive, send):
        downstream_calls.append(s["path"] if "path" in s else s["type"])

    mw = UsageTrackingMiddleware(downstream, tracker)
    asyncio.run(mw(scope, None, None))
    return downstream_calls


def test_middleware_tracks_key_event_and_passes_through() -> None:
    rec = _RecordingTracker()
    calls = _run_middleware(
        _http_scope("POST", "/api/admin/pipelines/s1/deploy/realtime", email=b"a@databricks.com"),
        rec,
    )
    assert rec.calls == [("/api/admin/pipelines/{id}/deploy/realtime", "a@databricks.com")]
    assert calls == ["/api/admin/pipelines/s1/deploy/realtime"]


def test_middleware_passes_query_to_label() -> None:
    rec = _RecordingTracker()
    _run_middleware(_http_scope("GET", "/api/events", query=b"industry=manufacturing"), rec)
    assert rec.calls == [("/dashboard/manufacturing/stream", None)]


def test_middleware_ignores_untracked_paths() -> None:
    rec = _RecordingTracker()
    calls = _run_middleware(_http_scope("GET", "/api/health"), rec)
    assert rec.calls == []
    assert calls == ["/api/health"]


def test_middleware_ignores_non_http_scopes() -> None:
    rec = _RecordingTracker()
    calls = _run_middleware({"type": "websocket", "path": "/"}, rec)
    assert rec.calls == []
    assert calls == ["/"]


def test_middleware_never_breaks_request_when_tracker_fails() -> None:
    calls = _run_middleware(_http_scope("GET", "/"), _RecordingTracker(fail=True))
    assert calls == ["/"]


def test_middleware_tolerates_undecodable_header() -> None:
    rec = _RecordingTracker()
    _run_middleware(_http_scope("GET", "/", email=b"\xff\xfe"), rec)
    assert rec.calls == [("/", None)]


# ---------------------------------------------------------------------------
# App wiring
# ---------------------------------------------------------------------------


def test_app_registers_tracking_middleware() -> None:
    from iot_twin import app as app_module

    registered = [m for m in app_module.app.user_middleware if m.cls is UsageTrackingMiddleware]
    assert len(registered) == 1
    assert registered[0].kwargs["tracker"] is app_module.usage_tracker


def test_app_tracking_disabled_in_test_env() -> None:
    from iot_twin import app as app_module

    assert app_module.usage_tracker.config.enabled is False


@pytest.mark.parametrize(("started", "expected_calls"), [(True, 1), (False, 0)])
def test_start_usage_tracking_records_app_start(monkeypatch, started, expected_calls) -> None:
    from iot_twin import app as app_module

    fake = MagicMock()

    async def fake_start() -> bool:
        return started

    fake.start = fake_start
    monkeypatch.setattr(app_module, "usage_tracker", fake)
    asyncio.run(app_module.start_usage_tracking())
    assert fake.track_app_start.call_count == expected_calls
