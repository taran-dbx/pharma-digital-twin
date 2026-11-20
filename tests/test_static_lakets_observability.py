"""Source-level checks for the standalone LakeTS observability module."""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DIST_ROOT = REPO_ROOT / "static" / "dist"
APP_PY = REPO_ROOT / "src" / "iot_twin" / "app.py"
INDEX_HTML = DIST_ROOT / "index.html"
OBSERVABILITY_JS = DIST_ROOT / "lakets-observability.js"
OBSERVABILITY_CSS = DIST_ROOT / "lakets-observability.css"


def _source() -> str:
    return OBSERVABILITY_JS.read_text(encoding="utf-8")


def test_index_loads_observer_before_main_bundle_c7226_smoke_regression() -> None:
    """[C7226][smoke][regression] Load observer before the hashed React bundle."""
    index = INDEX_HTML.read_text(encoding="utf-8")
    app_source = APP_PY.read_text(encoding="utf-8")

    observer_script = index.index('src="/static/dist/lakets-observability.js"')
    module_bundle = re.search(
        r'<script[^>]*\btype="module"[^>]*\bsrc="(/assets/index-[^"]+\.js)"',
        index,
    )

    assert module_bundle is not None
    assert observer_script < module_bundle.start()
    assert 'href="/static/dist/lakets-observability.css"' in index
    assert 'app.mount("/static", StaticFiles(directory=STATIC_DIR)' in app_source
    assert OBSERVABILITY_JS.is_file()
    assert OBSERVABILITY_CSS.is_file()


def test_fetch_interception_targets_only_lakets_calls_c7226_smoke_regression() -> None:
    """[C7226][smoke][regression] Observe successful rate and gapfill requests."""
    source = _source()

    assert r"lakets\/(rate|gapfill)" in source
    assert "if (response.ok) {" in source
    assert "const observed = observedTarget(input, init)" in source
    assert "input instanceof Request" in source
    assert "originalFetch.apply(this, arguments)" in source
    assert "originalFetch.call(window, endpoint" in source


def test_observer_uses_consolidated_dashboard_endpoint_c7226_smoke_regression() -> None:
    """[C7226][smoke][regression] Fetch one industry-aware dashboard endpoint."""
    source = _source()

    assert "/lakets/dashboard${suffix}" in source
    assert 'query.set("industry", requestedTarget.industry)' in source
    assert "encodeURIComponent(requestedTarget.pipelineId)" in source
    assert "REFRESH_INTERVAL_MS = 30000" in source
    assert "clearRefreshTimer()" in source


def test_dashboard_exposes_required_sections_c7226_smoke_regression() -> None:
    """[C7226][smoke][regression] Keep metrics available across drawer tabs."""
    source = _source()
    section_ids = {
        "partition-timeline",
        "current-partition",
        "rollup-value-series",
        "rollup-refresh",
        "tiering",
        "query-comparison",
        "data-completeness",
        "cdf-lag",
        "lvc",
    }
    labels = {
        "Partition timeline",
        "Current partition",
        "Latest stddev",
        "Samples in range",
        "Watermark",
        "Pending invalidations",
        "Age-eligible",
        "Durable-eligible",
        "Raw timing",
        "Event-to-rollup reduction",
        "Missing bucket percentage",
        "Gapfilled count",
        "Missing sample percentage",
        "Hot-to-shadow event lag",
        "WAL lag",
        "Series count",
        "Cache update age",
        "Section unavailable",
    }

    for section_id in section_ids:
        assert f'"{section_id}"' in source
    for label in labels:
        assert f'"{label}"' in source
    for phase in ("past", "current", "future"):
        assert phase in source.lower()
    assert 'const TAB_NAMES = ["overview", "storage", "rollup", "quality"]' in source
    assert 'panel.setAttribute("role", "tabpanel")' in source


def test_dynamic_content_uses_safe_dom_apis_c7226_smoke_regression() -> None:
    """[C7226][smoke][regression] Keep API-provided strings out of HTML sinks."""
    source = _source()

    assert "textContent" in source
    assert "createTextNode" in source
    assert "innerHTML" not in source
    assert re.search(r"\beval\s*\(", source) is None


def test_null_formatter_never_coerces_missing_values_c7226_smoke_regression() -> None:
    """[C7226][smoke][regression] Format null as unavailable, never numeric zero."""
    source = _source()
    match = re.search(
        r"function formatValue\(value, formatter\) \{(?P<body>.*?)\n  \}",
        source,
        flags=re.DOTALL,
    )

    assert match is not None
    body = match.group("body")
    assert "value === null" in body
    assert "value === undefined" in body
    assert 'return "Unavailable"' in body
    assert "|| 0" not in body


def test_observer_requires_same_origin_get_c7226_smoke_regression() -> None:
    """[C7226][smoke][regression] Ignore cross-origin and non-GET requests."""
    source = _source()

    assert "url.origin !== window.location.origin" in source
    assert 'requestMethod(input, init) !== "GET"' in source
    assert "input.method.toUpperCase()" in source
    assert "init && init.method" in source


def test_dashboard_fetch_is_lazy_until_open_c7226_smoke_regression() -> None:
    """[C7226][smoke][regression] Do not load telemetry behind a hidden drawer."""
    source = _source()
    observe_body = source.split("function observeLakeTs", 1)[1].split("function element", 1)[0]
    open_body = source.split("function openDrawer", 1)[1].split("function closeDrawer", 1)[0]

    assert "if (!drawer.hidden)" in observe_body
    assert "requestDashboard(false)" in observe_body
    assert "renderLoading()" in open_body
    assert "requestDashboard(false)" in open_body


def test_manual_refresh_bypasses_timer_throttle_c7226_smoke_regression() -> None:
    """[C7226][smoke][regression] Force clicks while throttling timer refreshes."""
    source = _source()

    assert "requestDashboard(true)" in source
    assert "requestDashboard(false)" in source
    assert "!forceRefresh && lastRequestStartedAt && elapsed < REFRESH_INTERVAL_MS" in source


def test_drawer_is_modal_and_traps_focus_c7226_smoke_regression() -> None:
    """[C7226][smoke][regression] Isolate app content and contain keyboard focus."""
    source = _source()

    assert 'drawer.setAttribute("role", "dialog")' in source
    assert 'drawer.setAttribute("aria-modal", "true")' in source
    assert 'appRoot.setAttribute("inert", "")' in source
    assert 'appRoot.removeAttribute("inert")' in source
    assert "launcher.hidden = true" in source
    assert "launcher.hidden = false" in source
    assert 'event.key !== "Tab"' in source
    assert "document.activeElement === first" in source
    assert "document.activeElement === last" in source
    assert "launcher.focus()" in source


def test_fetch_wrapper_guards_capture_and_returns_original_c7228_usability_regression() -> None:
    """[C7228][usability][regression] Never let capture/clone disturb React fetch."""
    source = _source()

    wrapper = source.split("window.fetch = function", 1)[1].split("function isCurrentTarget", 1)[0]
    assert "try {" in wrapper
    assert "observeLakeTs(observed)" in wrapper
    assert "captureChartRows(response, observed)" in wrapper
    assert "} catch (_error) {" in wrapper
    assert wrapper.index("} catch (_error) {") < wrapper.index("return response;")

    capture = source.split("function captureChartRows", 1)[1].split("function observeLakeTs", 1)[0]
    assert "cloned = response.clone();" in capture
    assert "} catch (_error) {" in capture
    assert "return;" in capture
    assert "Array.isArray(payload.rows)" in capture
    assert "chartRows[observed.chartType] = payload.rows.slice()" in capture


def test_focused_charts_share_sensor_and_replace_only_valid_data_c7228_usability_regression() -> (
    None
):
    """[C7228][usability][regression] Synchronize one focused sensor safely."""
    source = _source()

    assert 'gapfill: "time_bucket_gapfill"' in source
    assert 'rate: "lakets.rate"' in source
    assert "let selectedSensor = null" in source
    assert "/temperature|temp/i.test(sensor)" in source
    assert "Focused view · applies to both charts" in source
    assert "1 of ${sensors.length} sensors" in source
    assert "rescheduleAllCharts()" in source
    assert "Parsing failure intentionally leaves the original React chart visible" in source
    # Overlays are only rebuilt when the sensor or the data version changes.
    assert "rendered.version === chartVersion[chartType]" in source
    assert "rendered.sensor === selectedSensor" in source
    assert "restoreSelectFocus(focusedId)" in source


def test_focused_overlay_appends_before_hiding_native_c7228_usability_regression() -> None:
    """[C7228][usability][regression] Hide native only after a valid overlay mounts."""
    source = _source()

    enhance = source.split("function enhanceCard", 1)[1].split("function restoreSelectFocus", 1)[0]
    build_index = enhance.index("buildFocusedOverlay(chartType, rows)")
    hide_index = enhance.index('classList.add("lakets-focused-react-chart")')
    append_index = enhance.index("card.append(overlay)")

    assert build_index < append_index < hide_index
    # On any failure the overlay is removed and the native chart is unhidden.
    assert "if (!overlay) {" in enhance
    assert 'nativeChart.classList.remove("lakets-focused-react-chart")' in enhance
    assert "overlay = null" in enhance

    build = source.split("function buildFocusedOverlay", 1)[1].split(
        "function renderSensorControl", 1
    )[0]
    assert "if (!points.length) {" in build
    assert "return null;" in build


def test_enhancement_uses_no_global_observer_c7228_usability_regression() -> None:
    """[C7228][usability][regression] Schedule via frames/retries, not a body observer."""
    source = _source()

    assert "MutationObserver" not in source
    assert "function afterFrames(count, callback)" in source
    assert "afterFrames(2, function () {" in source
    assert "attempt < MAX_ENHANCE_RETRIES" in source
    assert "window.setTimeout(" in source
    assert "if (token !== enhanceToken[chartType])" in source


def test_numeric_rows_reject_missing_values_strictly_c7228_usability_regression() -> None:
    """[C7228][usability][regression] Exclude null/empty values before plotting."""
    source = _source()

    numeric = source.split("function numericChartRows", 1)[1].split("function createFocusedSvg", 1)[
        0
    ]
    assert "String(row.sensor_name) !== selectedSensor" in numeric
    assert 'rawValue === null || rawValue === undefined || rawValue === ""' in numeric
    assert "if (!Number.isFinite(numericValue)) {" in numeric
    # No coercion of missing values to zero anywhere in the reader.
    assert "|| 0" not in numeric


def test_custom_svg_charts_handle_single_point_and_scale_c7228_usability_regression() -> None:
    """[C7228][usability][regression] Render readable trend/rate with 1..N points."""
    source = _source()

    assert "function createFocusedSvg(points, chartType)" in source
    assert 'role: "img"' in source
    assert 'svgNode("title")' in source
    assert "lakets-focused-gapfilled" in source
    assert "lakets-focused-rate-area" in source
    assert "lakets-focused-zero" in source
    assert '"Above zero: increasing · Below zero: decreasing"' in source
    for label in ("Min", "Current", "Max"):
        assert f'metric("{label}"' in source
    # Single-point safety: centered marker, one tick, no degenerate area polygon.
    assert "const singlePoint = points.length === 1" in source
    assert "return padding.left + plotWidth / 2" in source
    assert "singlePoint ? 1 : Math.min(6, Math.max(4, points.length))" in source
    assert 'if (chartType === "rate") {\n      if (!singlePoint) {' in source


def test_drawer_renders_active_tab_only_c7228_usability_regression() -> None:
    """[C7228][usability][regression] Mount one tabpanel, not four hidden panels."""
    source = _source()

    assert 'tablist.setAttribute("role", "tablist")' in source
    assert 'tab.setAttribute("role", "tab")' in source
    assert 'panel.setAttribute("role", "tabpanel")' in source
    assert 'tab.setAttribute("aria-controls", PANEL_ID)' in source
    assert "panel.append(renderTab(activeTab))" in source
    assert "panel.hidden" not in source
    assert 'event.key !== "ArrowLeft" && event.key !== "ArrowRight"' in source
    assert "function selectTab(tabName)" in source


def test_partition_strip_is_bounded_and_inventory_is_lazy_c7228_usability_regression() -> None:
    """[C7228][usability][regression] Cap the strip and defer the 240-row table."""
    source = _source()

    assert "PHASE_STRIP_SEGMENTS = 48" in source
    assert "const segments = Math.min(total, PHASE_STRIP_SEGMENTS)" in source
    assert 'linear-gradient(to right, ${stops.join(", ")})' in source
    assert '"Current partition ±3"' in source
    assert 'String(partition.phase).toLowerCase() === "current"' in source
    assert 'String(partition.status).toLowerCase() === "active"' not in source
    assert "String(partition.name) === String(currentPartition.name)" in source
    assert "Math.max(anchor - 3, 0)" in source
    assert "Math.min(anchor + 4, partitions.length)" in source
    # Full inventory rows only build when the <details> first opens.
    inventory = source.split("function buildInventory", 1)[1].split("function partitionRow", 1)[0]
    assert 'inventory.addEventListener("toggle"' in inventory
    assert "if (!inventory.open || built) {" in inventory
    assert "built = true;" in inventory
    assert "Full partition inventory (${partitions.length})" in inventory


def test_status_region_and_focus_trap_are_accessible_c7228_usability_regression() -> None:
    """[C7228][usability][regression] Compact live region and complete focus trap."""
    source = _source()

    # A dedicated status node carries loading/error/refresh, not the whole content.
    assert 'content.setAttribute("aria-live"' not in source
    assert 'statusRegion.setAttribute("role", "status")' in source
    assert 'statusRegion.setAttribute("aria-live", "polite")' in source
    assert "function setStatus(message)" in source

    # Focus trap includes summary and form fields.
    trap = source.split("function handleKeyDown", 1)[1].split("function clearRefreshTimer", 1)[0]
    assert "summary" in trap
    assert "input:not([disabled])" in trap
    assert "textarea:not([disabled])" in trap

    # inert has a fallback when unsupported.
    assert "supportsInert" in source
    assert 'appRoot.setAttribute("inert", "")' in source
    assert 'appRoot.setAttribute("aria-hidden", "true")' in source
    assert 'appRoot.style.pointerEvents = "none"' in source


def test_refined_styles_cap_scrolling_and_keep_labels_readable_c7228_usability_regression() -> None:
    """[C7228][usability][regression] Preserve readable industrial-console styles."""
    css = OBSERVABILITY_CSS.read_text(encoding="utf-8")

    assert ".lakets-obs-timeline-full" in css
    assert "max-height: 22rem" in css
    assert "overflow-y: auto" in css
    assert ".lakets-focused-react-chart" in css
    assert "display: none !important" in css
    assert ".lakets-focused-axis" in css
    assert "font-size: 12px" in css
    # Bounded phase strip is a gradient, not 240 flexed spans.
    assert ".lakets-obs-phase-segment" not in css
    assert "background-repeat: no-repeat" in css
    # Tabs scroll horizontally on narrow screens rather than clipping.
    assert "overflow-x: auto" in css
    assert re.search(r"font-size:\s*0\.[0-6]\d*rem", css) is None


def test_c7228_dynamic_content_avoids_unsafe_sinks_usability_regression() -> None:
    """[C7228][usability][regression] Keep chart and drawer data out of HTML sinks."""
    source = _source()

    assert "textContent" in source
    assert "innerHTML" not in source
    assert re.search(r"\beval\s*\(", source) is None


def test_health_entry_is_pipeline_scoped_c7229_defect_regression() -> None:
    """[C7229][defect][regression] Mount health inside an eligible pipeline view."""
    source = _source()
    css = OBSERVABILITY_CSS.read_text(encoding="utf-8")

    assert '"data-lakets-obs-context"' in source
    assert '"Operations / LakeTS health"' in source
    assert "`Pipeline ${target.pipelineId}${industry}`" in source
    assert "anchor.parentElement.insertBefore(contextSection, anchor)" in source
    assert "document.body.append(drawer)" in source
    assert "document.body.append(launcher" not in source
    assert "if (!target || !contextSection.isConnected)" in source
    assert "position: fixed" not in css.split(".lakets-obs-launcher {", 1)[1].split("}", 1)[0]
    assert ".lakets-obs-context {" in css
    assert "border-left: 4px solid var(--lakets-obs-accent)" in css


def test_health_context_is_removed_on_navigation_c7229_defect_regression() -> None:
    """[C7229][defect][regression] Remove stale pipeline health on navigation."""
    source = _source()

    assert '["pushState", "replaceState"]' in source
    assert 'window.addEventListener("popstate", deactivateContext)' in source
    assert 'window.addEventListener("hashchange", deactivateContext)' in source
    assert "observed.pageLocation === currentPageLocation()" in source
    assert "contextSection.remove()" in source
    assert "target = null" in source
    assert "resetChartEnhancements()" in source
    assert "CONTEXT_CHECK_INTERVAL_MS = 1000" in source
    assert "if (!contextAnchor()) {" in source
    assert "candidate.getClientRects().length > 0" in source
    assert "const targetChangedWhileLoading = !isCurrentTarget(requestedTarget)" in source


def test_operations_metrics_are_semantically_separate_c7229_usability_regression() -> None:
    """[C7229][usability][regression] Keep LakeTS operations outside business KPIs."""
    source = _source()

    context = source.split("function mount()", 1)[1].split("function openDrawer", 1)[0]
    assert '"Operations / LakeTS health"' in context
    assert '"Operational freshness, storage, rollup, and data-quality telemetry."' in context
    assert '"aria-labelledby"' in context
    assert "contextSection.append(contextCopy, launcher)" in context
    assert "drawer.append(header, statusRegion, content, note)" in context
    assert "contextSection.append(content)" not in context


def test_pipeline_identity_uses_safe_accessible_text_c7229_accessibility_regression() -> None:
    """[C7229][accessibility][regression] Name controls for the selected pipeline."""
    source = _source()

    identity = source.split("function updateContextIdentity", 1)[1].split(
        "function detachContextSection", 1
    )[0]
    assert "contextLabel.textContent" in identity
    assert '"aria-label"' in identity
    assert "target.pipelineId" in identity
    assert "innerHTML" not in identity
    assert "restoreFocus !== false && launcher.isConnected" in source
