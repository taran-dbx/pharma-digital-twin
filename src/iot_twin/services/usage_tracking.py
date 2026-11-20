"""Fail-safe usage tracking for the app, built on the ``dbdemos-tracker`` package.

``dbdemos-tracker`` posts fire-and-forget analytics events (org id, app name,
event path and, for ``@databricks.com`` users only, a SHA-256 hash of the email)
to the dbdemos analytics endpoint. This module wraps it so that tracking can
never affect the app:

* configured from env vars only (``USAGE_TRACKING_ENABLED`` opt-out flag,
  ``USAGE_TRACKING_APP_NAME``, ``USAGE_TRACKING_DEMO_CATALOG_ID``);
* the package is imported lazily, so a missing install just disables tracking;
* the workspace id lookup runs off the event loop with a timeout at startup;
* every call is wrapped and errors are logged at debug level, never raised;
* only a small allow-list of key events is tracked, with ids collapsed to
  ``{id}`` so no free-form request data leaves the app.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs

logger = logging.getLogger(__name__)

ENV_ENABLED = "USAGE_TRACKING_ENABLED"
ENV_APP_NAME = "USAGE_TRACKING_APP_NAME"
ENV_DEMO_CATALOG_ID = "USAGE_TRACKING_DEMO_CATALOG_ID"

# The package asks for the repository name as the app name.
DEFAULT_APP_NAME = "iot-digital-twin"
APP_START_PATH = "/_app/start"
DEFAULT_STARTUP_TIMEOUT_S = 10.0

_FALSY = frozenset({"0", "false", "no", "off"})
_EMAIL_HEADER = b"x-forwarded-email"

# Low-cardinality path segments we are willing to report verbatim.
_SLUG = r"[A-Za-z0-9_-]{1,64}"
_MODE = r"realtime|microbatch|zerobus"
_SLUG_RE = re.compile(rf"^{_SLUG}$")

# (method, path regex, label template). Named groups are substituted into the
# label; anything else (pipeline / setup ids) is collapsed to ``{id}``.
_EVENT_RULES: tuple[tuple[str, re.Pattern[str], str], ...] = tuple(
    (method, re.compile(rf"^{pattern}/?$"), label)
    for method, pattern, label in (
        # SPA page loads (full loads and deep links reach the backend).
        ("GET", r"", "/"),
        ("GET", r"/industries", "/industries"),
        ("GET", rf"/i/(?P<industry>{_SLUG})", "/i/{industry}"),
        (
            "GET",
            rf"/i/(?P<industry>{_SLUG})/(?P<mode>{_MODE})/dashboard",
            "/i/{industry}/{mode}/dashboard",
        ),
        ("GET", r"/setup-validate", "/setup-validate"),
        ("GET", r"/pipelines", "/pipelines"),
        ("GET", rf"/pipelines/{_SLUG}/health", "/pipelines/{{id}}/health"),
        ("GET", r"/settings", "/settings"),
        # Pipeline setup / validation / deploy actions.
        (
            "POST",
            rf"/api/admin/industries/(?P<industry>{_SLUG})/setup",
            "/api/admin/industries/{industry}/setup",
        ),
        (
            "POST",
            rf"/api/admin/industries/(?P<industry>{_SLUG})/validate/start",
            "/api/admin/industries/{industry}/validate/start",
        ),
        (
            "POST",
            rf"/api/admin/pipelines/{_SLUG}/deploy/(?P<mode>{_MODE})",
            "/api/admin/pipelines/{{id}}/deploy/{mode}",
        ),
        ("POST", r"/api/pipelines", "/api/pipelines"),
    )
)

# The live dashboard opens one SSE stream per view: the dashboard-view signal.
_DASHBOARD_STREAM_PATH = "/api/events"


@dataclass(frozen=True)
class UsageTrackingConfig:
    enabled: bool = True
    app_name: str = DEFAULT_APP_NAME
    demo_catalog_id: str | None = None

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> UsageTrackingConfig:
        source = os.environ if env is None else env
        enabled = source.get(ENV_ENABLED, "").strip().lower() not in _FALSY
        app_name = source.get(ENV_APP_NAME, "").strip() or DEFAULT_APP_NAME
        catalog_id = source.get(ENV_DEMO_CATALOG_ID, "").strip() or None
        return cls(enabled=enabled, app_name=app_name, demo_catalog_id=catalog_id)


def _dashboard_stream_label(query_string: str) -> str:
    values = parse_qs(query_string or "").get("industry", [])
    industry = values[0] if values else ""
    if _SLUG_RE.match(industry):
        return f"/dashboard/{industry}/stream"
    return "/dashboard/stream"


def event_label(method: str, path: str, query_string: str = "") -> str | None:
    """Map a request to a stable tracking label, or ``None`` if not a key event."""
    try:
        if method == "GET" and path.rstrip("/") == _DASHBOARD_STREAM_PATH:
            return _dashboard_stream_label(query_string)
        trimmed = path.rstrip("/")
        for rule_method, pattern, label in _EVENT_RULES:
            if rule_method != method:
                continue
            match = pattern.match(trimmed)
            if match:
                return label.format(**match.groupdict())
    except Exception:
        return None
    return None


def _default_tracker_factory(**kwargs: Any) -> Any:
    from dbdemos_tracker import Tracker

    return Tracker(**kwargs)


def _default_workspace_id() -> Any:
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient().get_workspace_id()


class UsageTracker:
    """App-level tracker; inactive (a no-op) until :meth:`start` succeeds."""

    def __init__(
        self,
        config: UsageTrackingConfig,
        *,
        tracker_factory: Callable[..., Any] = _default_tracker_factory,
        workspace_id_provider: Callable[[], Any] = _default_workspace_id,
        startup_timeout_s: float = DEFAULT_STARTUP_TIMEOUT_S,
    ) -> None:
        self.config = config
        self._factory = tracker_factory
        self._workspace_id_provider = workspace_id_provider
        self._timeout_s = startup_timeout_s
        self._tracker: Any | None = None

    @classmethod
    def from_env(cls) -> UsageTracker:
        return cls(UsageTrackingConfig.from_env())

    @property
    def active(self) -> bool:
        return self._tracker is not None

    async def start(self) -> bool:
        """Resolve the workspace id and build the tracker. Never raises."""
        if not self.config.enabled:
            logger.info("Usage tracking disabled (%s)", ENV_ENABLED)
            return False
        try:
            workspace_id = await asyncio.wait_for(
                asyncio.to_thread(self._workspace_id_provider), timeout=self._timeout_s
            )
            if not workspace_id:
                raise ValueError("empty workspace id")
            self._tracker = self._factory(
                org_id=str(workspace_id),
                demo_name=self.config.app_name,
                demo_catalog_id=self.config.demo_catalog_id,
            )
        except Exception as exc:
            logger.info("Usage tracking unavailable, continuing without it: %r", exc)
            self._tracker = None
            return False
        logger.info("Usage tracking enabled (app=%s)", self.config.app_name)
        return True

    def track(self, app_path: str, user_email: str | None) -> None:
        """Send one app-view event. Fire-and-forget; never raises."""
        tracker = self._tracker
        if tracker is None:
            return
        try:
            tracker.track_app_view(user_email=user_email, app_path=app_path)
        except Exception as exc:
            logger.debug("Usage tracking event dropped: %r", exc)

    def track_app_start(self) -> None:
        self.track(APP_START_PATH, None)


def _header(scope: Mapping[str, Any], name: bytes) -> str | None:
    for key, value in scope.get("headers") or ():
        if key.lower() == name:
            try:
                return value.decode("ascii") or None
            except UnicodeDecodeError:
                return None
    return None


class UsageTrackingMiddleware:
    """Pure ASGI middleware: tracks allow-listed key events, then passes through.

    Pure ASGI (not ``BaseHTTPMiddleware``) so SSE and WebSocket streams are
    untouched. Tracking happens before the request is handled and any error in
    it is swallowed.
    """

    def __init__(self, app: Any, tracker: Any) -> None:
        self.app = app
        self.tracker = tracker

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope.get("type") == "http":
            self._maybe_track(scope)
        await self.app(scope, receive, send)

    def _maybe_track(self, scope: Mapping[str, Any]) -> None:
        try:
            query = (scope.get("query_string") or b"").decode("latin-1")
            label = event_label(scope.get("method", ""), scope.get("path", ""), query)
            if label is not None:
                self.tracker.track(label, _header(scope, _EMAIL_HEADER))
        except Exception as exc:
            logger.debug("Usage tracking middleware error: %r", exc)
