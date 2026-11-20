"""Static regression coverage for the vendored manufacturing dashboard."""

from __future__ import annotations

import re
from pathlib import Path

DIST = Path(__file__).parents[1] / "static" / "dist"


def _bundle() -> str:
    index = (DIST / "index.html").read_text(encoding="utf-8")
    match = re.search(r'src="/assets/(index-[^"]+\.js)"', index)
    assert match is not None
    return (DIST / "assets" / match.group(1)).read_text(encoding="utf-8")


def test_realtime_lakebase_writer_retries_recovery_with_exponential_backoff():
    """[C7231][defect][regression] RTM survives transient Postgres recovery."""
    bundle = _bundle()
    start = bundle.index("class LakebaseWriter:")
    end = bundle.index("class AppSseWriter:", start)
    writer = bundle[start:end]

    assert "MAX_RECONNECT_ATTEMPTS = 12" in writer
    assert "BACKOFF_BASE_S = 1.0" in writer
    assert "BACKOFF_MAX_S = 30.0" in writer
    assert "def _connect_with_backoff(self):" in writer
    assert "def _flush_with_backoff(self):" in writer
    assert "_lb_time.sleep(delay)" in writer
    assert "min(self.BACKOFF_BASE_S * (2 ** (attempt - 1))" in writer
    assert (
        writer.count("execute_values(self.cur, self.sql, self.buffer, template=_INSERT_TEMPLATE)")
        == 2
    )


CLIENT_SRC = Path(__file__).parents[1] / "client" / "src"


def _source(*parts: str) -> str:
    return CLIENT_SRC.joinpath(*parts).read_text(encoding="utf-8")


def test_manufacturing_view_has_no_invented_metrics_c7230():
    """[C7230][defect][regression] The operator twin shows only measured or catalog values."""
    bundle = _bundle()
    kpis = _source("components", "cell", "cell-kpis.tsx")
    model = _source("components", "cell", "use-cell-model.ts")

    for invented in (
        'OEE:"oee"',
        'Utilization:"utilization"',
        'Production:"throughput"',
        "/1250)*100",
    ):
        assert invented not in bundle
    for label in (
        "Spindle 3 temperature",
        "Operating limit",
        "Spindle 3 status",
        "Open alerts",
        "Last device event",
    ):
        assert f'case "{label}":' in kpis
    assert "spindleLimitFrom(rules)" in model
    for source in (kpis, model):
        assert "input_rows_per_second" not in source
        assert "processed_rows_per_second" not in source


def test_manufacturing_realtime_data_is_industry_scoped_c7232():
    """[C7232][defect][regression] Mixed SSE data cannot contaminate the machining cell."""
    section = _source("components", "cell", "use-cell-model.ts")

    assert section.count('r.industry === "manufacturing"') >= 2
    assert 'useIndustryAlerts("manufacturing")' in section
    assert 'inspect: { health: "no-feed" as const, readout: "no sensor feed" }' in section


def test_machining_cell_twin_is_accessible_c7231():
    """[C7231][defect][smoke][regression] MILL -> INSPECT -> PACK twin is labelled and selectable."""
    bundle = _bundle()
    story = _source("lib", "manufacturing-story.ts")

    assert "Machining cell twin: mill, inspect, pack" in bundle
    assert 'aria-label":"Cell stations"' in bundle
    assert "Interactive 3D manufacturing factory" not in bundle
    assert "Reference · MES not connected" in story


def test_alert_action_comes_from_catalog_rule_c7417():
    """[C7417][regression] "Inspection required" is catalog data, not hard-coded UI copy."""
    rail = _source("components", "dashboard", "alert-rail.tsx")
    card = _source("components", "dashboard", "cell-station-card.tsx")

    assert "r.rule_id === selected.rule_id)?.action" in rail
    assert "limit.action" in card
    for source in (rail, card, _source("lib", "manufacturing-story.ts")):
        assert "Inspection required" not in source


def test_tier_summary_uses_latest_timestamps_and_delay_c7265():
    """[C7265][defect][regression] Tier summary avoids expensive row counts."""
    component = (
        Path(__file__).parents[1]
        / "client"
        / "src"
        / "components"
        / "dashboard"
        / "historical-dbsql.tsx"
    ).read_text(encoding="utf-8")

    assert "Latest Hot Event" in component
    assert "Latest Cold Event" in component
    assert "Replication Delay" in component
    assert "hot_latest_at" in component
    assert "latest_event_at" in component
    assert "hot_rows" not in component
    assert "total_rows" not in component
    assert "Checking Lakebase Sync status" in component


def test_historical_chart_keeps_axis_readable_c7268_defect() -> None:
    """[C7268][defect][regression] DBSQL chart sorts points, compacts ticks, and avoids mixed scales."""
    component = (
        Path(__file__).parents[1]
        / "client"
        / "src"
        / "components"
        / "dashboard"
        / "historical-dbsql.tsx"
    ).read_text(encoding="utf-8")
    assert "formatAxisTick" in component
    assert "pickCompatibleSeries" in component
    assert ".sort(" in component
    assert "width={44}" in component or "width={40}" in component or "width={48}" in component
    assert "margin={{" in component


def test_streaming_summary_tiles_use_pipeline_scoped_spark_metrics():
    """[C7247][defect][regression] Summary tiles cannot use global app counters."""
    bundle = _bundle()
    component = (
        Path(__file__).parents[1]
        / "client"
        / "src"
        / "components"
        / "dashboard"
        / "streaming-source-metrics.tsx"
    ).read_text(encoding="utf-8")

    for label in (
        "Records/s",
        "Processed rows/s",
        "Ingest Latency (avg)",
        "Processing p50",
        "Latency p95",
        "Processing p99",
        "Buffer Len",
        "Spark E2E p99",
    ):
        assert label in bundle
    assert "const sparkProcessedRps = latency?.processed_rps" in component
    assert "const sparkProcessingP50 = latency?.processing_p50" in component
    assert "const sparkProcessingP99 = latency?.processing_p99" in component
    assert "const sparkE2eP99 = latency?.e2e_p99" in component

    # Process-global source values remain only for Zerobus; Spark tiles resolve
    # from the industry/mode-scoped latency response.
    assert re.search(r"isZerobus\s*\?\s*rolling", component)
    assert "sparkProcessedRps != null" in component
    assert "sparkProcessingP50 != null" in component
    assert "sparkProcessingP99 != null" in component
    assert "industry" in component and "mode" in component


def test_event_to_insert_latency_is_hidden_for_all_pipelines():
    """[C7248][defect][regression] Untrusted insert-lag values remain blank."""
    bundle = _bundle()
    streaming_source = (
        Path(__file__).parents[1]
        / "client"
        / "src"
        / "components"
        / "dashboard"
        / "streaming-source-metrics.tsx"
    ).read_text(encoding="utf-8")
    assert 'label:"Insert Lag p50",value:"---",sub:void 0' in bundle
    assert 'label:"Insert Lag p95",value:"---",sub:void 0' in bundle

    e2e_label = bundle.index('"Spark E2E p99"')
    e2e_widget = bundle[e2e_label : e2e_label + 300]
    assert "avg_e2e_latency_ms" not in e2e_widget
    assert '"---"' in e2e_widget
    assert "const SHOW_STREAMING_E2E_LATENCY = false" in streaming_source
    assert "SHOW_STREAMING_E2E_LATENCY && sparkE2eP99 != null" in streaming_source
    assert '"Ingestion Latency"' not in bundle


def test_industry_pages_wait_for_parallel_runtime_state():
    """[C7250][defect][regression] Runtime loading never flashes Start pipeline."""
    bundle = _bundle()

    assert "Loading industries…" in bundle
    assert "Loading industry…" in bundle
    assert '("/industries/runtime")' in bundle
    assert 'new Set(["starting","running","degraded","stopped"])' in bundle
    assert 'case"starting":return{label:"Starting",color:"#3b82f6"' in bundle


def test_gallery_renders_from_catalog_without_admin_fallback_c7262():
    """[C7262][performance][regression] Industries page never awaits /admin/pipelines.

    Catalog cards must appear from industry-first ``deploymentId`` without waiting
    for the Jobs/Lakebase runtime refresh. The public runtime endpoint is the only
    live-status source — falling back to ``/admin/pipelines`` on 503 re-runs the
    expensive path and is what made ``/industries`` hang.
    """
    bundle = _bundle()
    admin_api = (Path(__file__).parents[1] / "client" / "src" / "lib" / "admin-api.ts").read_text(
        encoding="utf-8"
    )
    gallery = (
        Path(__file__).parents[1] / "client" / "src" / "pages" / "industry-gallery-page.tsx"
    ).read_text(encoding="utf-8")
    dashboard = (
        Path(__file__).parents[1] / "client" / "src" / "pages" / "mode-dashboard-page.tsx"
    ).read_text(encoding="utf-8")

    assert "listPipelineRuntime" in gallery
    assert "deploymentId" in gallery
    assert "galleryReady = !!catalog" in gallery or "galleryReady=!!catalog" in bundle.replace(
        " ", ""
    )
    assert "return request(`/admin/pipelines`)" not in admin_api
    assert "listAdminPipelines" not in dashboard
    assert "listPipelineRuntime" in dashboard


def test_failure_stopped_pipeline_offers_restart_c7260():
    """[C7260][defect][regression] Fail-stop recovery is actionable in the UI."""
    bundle = _bundle()

    assert "Pipeline stopped after task failure" in bundle
    assert "Restart pipeline" in bundle
    assert "restartable" in bundle


def test_legacy_pipeline_pages_are_unreachable_c7263():
    """[C7263][defect][regression] Industry UI never opens legacy pipeline pages."""
    root = Path(__file__).parents[1]
    bundle = _bundle()
    app = (root / "client" / "src" / "App.tsx").read_text(encoding="utf-8")
    pipeline_card = (
        root / "client" / "src" / "components" / "industry" / "pipeline-card.tsx"
    ).read_text(encoding="utf-8")
    setup_page = (root / "client" / "src" / "pages" / "industry-setup-page.tsx").read_text(
        encoding="utf-8"
    )
    gallery = (root / "client" / "src" / "pages" / "industry-gallery-page.tsx").read_text(
        encoding="utf-8"
    )

    retired_modules = (
        "pipeline-detail-page",
        "dashboard-page",
        "pipeline-wizard-page",
        "home-page",
    )
    for module in retired_modules:
        assert not re.search(rf'from\s+["\']@/pages/{module}["\']', app)

    source_redirects = {
        "/legacy": "/industries",
        "/pipelines/new": "/setup-validate",
        "/pipelines/:id": "/industries",
        "/pipelines/:id/dashboard": "/industries",
    }
    for path, target in source_redirects.items():
        assert re.search(
            rf'path="{re.escape(path)}".{{0,180}}'
            rf'<Navigate\s+to="{re.escape(target)}"\s+replace',
            app,
            re.DOTALL,
        )
        assert re.search(
            rf'path:"{re.escape(path)}".{{0,140}}'
            rf'to:"{re.escape(target)}".{{0,40}}replace:!0',
            bundle,
            re.DOTALL,
        )

    for legacy_copy in (
        "View Dashboard",
        "View Pipeline Dashboard",
        "Go to Dashboard",
        "NEURAL OBSERVATORY",
    ):
        assert legacy_copy not in bundle

    assert "/pipelines/" not in pipeline_card
    assert "navigate(`/i/${resolvedIndustryId}/${mode.id}/dashboard`)" in setup_page
    assert "/pipelines/${industryId}/dashboard" not in gallery
    assert (
        "navigate(`/setup-validate?industry=${encodeURIComponent(industry.id)}`)" in pipeline_card
    )


def test_mode_dashboard_passes_industry_only_for_industry_first_c7263() -> None:
    """Industry query params follow industry-first ids, not leftover legacy pipeline ids."""
    page = (
        Path(__file__).resolve().parents[1] / "client" / "src" / "pages" / "mode-dashboard-page.tsx"
    ).read_text(encoding="utf-8")
    assert "isIndustryFirst ? industryId : undefined" in page


def test_runtime_is_loaded_once_then_polled_only_while_starting_c7264():
    """[C7264][defect][regression] Settled pipeline cards do not keep polling."""
    root = Path(__file__).parents[1]
    setup_page = (root / "client" / "src" / "pages" / "industry-setup-page.tsx").read_text(
        encoding="utf-8"
    )
    pipeline_card = (
        root / "client" / "src" / "components" / "industry" / "pipeline-card.tsx"
    ).read_text(encoding="utf-8")

    assert "void refreshAdmin();" in setup_page
    assert 'if (effective !== "starting") return;' in pipeline_card
    assert "timer = setTimeout(tick, POLL_FAST_MS)" in pipeline_card
    assert 'realStatus === "stopped" || realStatus === "draft"' in pipeline_card
    assert "requestAge >= STARTING_SETTLE_GRACE_MS" in pipeline_card
    assert "setJustStarted(false)" in pipeline_card
    assert "return () => clearTimeout(timer)" in pipeline_card
