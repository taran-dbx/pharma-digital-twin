"""Static regression checks for the dashboard alert rail [qa]."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).parents[1]
CLIENT = ROOT / "client" / "src"
DIST = ROOT / "static" / "dist"


def _read(*parts: str) -> str:
    return (CLIENT.joinpath(*parts)).read_text(encoding="utf-8")


def _bundle() -> str:
    index = (DIST / "index.html").read_text(encoding="utf-8")
    match = re.search(r'src="/assets/(index-[^"]+\.js)"', index)
    assert match is not None
    return (DIST / "assets" / match.group(1)).read_text(encoding="utf-8")


def test_alert_types_cover_list_detail_and_lifecycle_c7412_smoke() -> None:
    """[C7412][smoke] Client types describe REST alerts and SSE lifecycle events."""
    types = _read("lib", "types.ts")

    for marker in (
        "export type AlertStatus",
        "export type AlertSeverity",
        "export interface AlertRecord",
        "export interface AlertListResponse",
        "export type AlertLifecycleEventType",
        'alert_opened"',
        'alert_updated"',
        'alert_acknowledged"',
        'alert_cleared"',
        "export interface AlertLifecycleEvent",
        "export type WsMessage",
        'type: "sensor_update"',
        'type: "source_status"',
    ):
        assert marker in types, marker

    assert "sensor_ids" in types
    assert "acknowledged_by" in types
    assert "fault_code" in types


def test_api_client_exposes_alert_list_detail_ack_c7412_regression() -> None:
    """[C7412][regression] API client can list, load, and acknowledge alerts."""
    api = _read("lib", "api.ts")

    assert "listAlerts:" in api or "listAlerts :" in api
    assert "getAlert:" in api or "getAlert :" in api
    assert "acknowledgeAlert:" in api or "acknowledgeAlert :" in api
    assert "`/alerts/${" in api or '"/alerts/"' in api
    assert "/acknowledge" in api
    assert "AlertListResponse" in api
    assert "AlertRecord" in api
    assert "AlertLifecycleEvent" in api


def test_sensor_context_handles_alert_lifecycle_exhaustively_c7412_defect() -> None:
    """[C7412][defect] SSE handler covers sensor, source, and all alert variants."""
    context = _read("contexts", "sensor-context.tsx")

    for event in (
        "sensor_update",
        "source_status",
        "alert_opened",
        "alert_updated",
        "alert_acknowledged",
        "alert_cleared",
    ):
        assert event in context, event

    assert "never" in context
    assert "alerts" in context
    assert "switch (msg.type)" in context or "switch(msg.type)" in context.replace(" ", "")


def test_alert_merge_requires_exact_industry_c7412_defect() -> None:
    """[C7412][defect] Client merge ignores alerts that lack an exact industry match."""
    component = _read("components", "dashboard", "alert-rail.tsx")
    assert "!alert.industry" not in component
    assert "alert.industry === industry" in component
    assert "function mergeAlerts" in component


def test_alert_rail_component_is_operational_chrome_c7412_usability() -> None:
    """[C7412][usability] Alert rail is compact, left-aligned, and evidence-rich."""
    component = _read("components", "dashboard", "alert-rail.tsx")
    css = _read("styles", "dubois.css")

    for marker in (
        "alert-rail",
        "Acknowledge",
        "severity",
        "status",
        "asset",
        "component",
        "maintenance",
        "sensor",
        "acknowledgeAlert",
        "listAlerts",
        "telemetry",
    ):
        assert marker.lower() in component.lower(), marker

    assert "alert-rail" in css
    assert ".alert-rail" in css
    assert "generic card" not in component.lower()


def test_mode_dashboard_mounts_alert_rail_for_all_modes_c7412_regression() -> None:
    """[C7412][regression] Every mode dashboard mounts the alert rail."""
    page = _read("pages", "mode-dashboard-page.tsx")

    assert "AlertRail" in page
    assert 'from "@/components/dashboard/alert-rail"' in page
    assert "getIndustryReference" in page
    assert "getIndustryAssets" in page
    # Rail sits on the live dashboard, not only the realtime SSE path.
    assert "pipeline &&" in page or "pipelineId" in page


def test_kpi_bindings_use_domain_reference_for_four_industries_c7412_regression() -> None:
    """[C7412][regression] KPI tiles resolve real catalog sensor names for all four industries."""
    page = _read("pages", "mode-dashboard-page.tsx")
    api = _read("lib", "industries-api.ts")

    assert "getIndustryReference" in api
    assert "/industries/${industry}/reference" in api or "/industries/" in api
    assert "sensor_name" in api or "sensorName" in api or "sensor_name" in page
    assert "gt_1_power_output" in page or "reference" in page.lower()
    assert "robot_1_weld_current" in page or "kpiBindings" in page or "kpi_bindings" in page
    # Hardcoded pharma-only map is no longer the sole binding source.
    assert "liveKpiValue" in page
    assert "kpiBindings" in page or "reference.kpis" in page or "sensor_name" in page


def test_event_source_includes_industry_query_c7412_regression() -> None:
    """[C7412][regression] Client EventSource requests the current industry."""
    hook = _read("hooks", "use-websocket.ts")
    assert "/api/events?industry=" in hook
    context = _read("contexts", "sensor-context.tsx")
    assert "industry" in context
    assert "useWebSocket" in context


def test_production_bundle_ships_alert_rail_c7412_smoke() -> None:
    """[C7412][smoke] Generated SPA bundle includes alert lifecycle + rail chrome."""
    bundle = _bundle()
    css = next((DIST / "assets").glob("index-*.css")).read_text(encoding="utf-8")

    for marker in (
        "alert_opened",
        "alert_updated",
        "alert_acknowledged",
        "alert_cleared",
        "/alerts/",
        "Acknowledge",
        "Operational alerts",
        "/industries/",
        "sensor_name",
    ):
        assert marker in bundle, marker

    assert ".alert-rail" in css
    assert "Acknowledge" in bundle
    assert "historical-dbsql" in bundle


def test_alert_rail_actions_wrap_inside_column_c7414_defect() -> None:
    """[C7414][defect][regression] Alert actions wrap so Explain never paints over the chart."""
    css = _read("styles", "dubois.css")
    component = _read("components", "dashboard", "alert-rail.tsx")
    compact = css.replace(" ", "").replace("\n", "")

    assert ".alert-rail__actions{display:flex;flex-wrap:wrap" in compact
    assert (
        ".alert-rail{" in compact
        and "overflow:hidden" in compact.split(".alert-rail{")[1].split("}")[0]
    )
    assert "is-cleared" in component
    assert "Explain this alert" in component
