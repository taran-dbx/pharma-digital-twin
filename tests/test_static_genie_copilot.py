"""Static regression checks for the Genie investigation drawer [qa]."""

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


def test_genie_types_and_exhaustive_status_c7505_smoke() -> None:
    """[C7505][smoke] Client types cover conversation polling and status unions."""
    types = _read("lib", "types.ts")
    for marker in (
        "export type GenieMessageStatus",
        "IN_PROGRESS",
        "EXECUTING_QUERY",
        "COMPLETED",
        "FAILED",
        "CANCELLED",
        "export interface GenieStatusResponse",
        "export type GenieAllowlistStatus",
        "authorization_enforced",
        "table_allowlist",
        "export interface GenieConversationIds",
        "export interface GenieMessageResponse",
        "can_mutate_alerts",
    ):
        assert marker in types, marker


def test_api_client_exposes_genie_endpoints_c7505_regression() -> None:
    """[C7505][regression] API client can status, start, follow up, and poll Genie."""
    api = _read("lib", "api.ts")
    assert "getGenieStatus" in api
    assert "startGenieConversation" in api
    assert "addGenieMessage" in api
    assert "getGenieMessage" in api
    assert "/genie/status" in api
    assert "/genie/conversations" in api


def test_copilot_is_investigation_drawer_c7505_usability() -> None:
    """[C7505][usability] Copilot is a right-side drawer, not a floating bubble."""
    # Drawer chrome lives in genie-drawer.tsx, shared by the workspace-wide and
    # per-pipeline copilots; genie-copilot.tsx dispatches between them.
    component = _read("components", "dashboard", "genie-copilot.tsx") + _read(
        "components", "dashboard", "genie-drawer.tsx"
    )
    css = _read("styles", "dubois.css")
    assert 'data-testid="genie-copilot"' in component
    assert 'data-testid="genie-copilot-drawer"' in component
    assert 'data-testid="genie-explain-alert"' in component or "Explain this alert" in component
    assert 'data-testid="genie-copilot-transcript"' in component
    assert "floating" not in component.lower()
    assert "chat-bubble" not in component.lower()
    assert "genie-drawer" in css
    assert "position:fixed" not in component.lower() or "drawer" in component.lower()


def test_mode_dashboard_docks_copilot_c7505_regression() -> None:
    """[C7505][regression] Every mode dashboard mounts the Genie copilot drawer."""
    page = _read("pages", "mode-dashboard-page.tsx")
    assert "GenieCopilot" in page
    assert 'from "@/components/dashboard/genie-copilot"' in page
    rail = _read("components", "dashboard", "alert-rail.tsx")
    assert "Explain this alert" in rail
    assert "onExplainAlert" in rail


def test_disabled_genie_copy_is_operator_facing_c7509_usability() -> None:
    """[C7509][usability] Disabled Genie copy never dumps env-var / valueFrom internals."""
    component = _read("components", "dashboard", "genie-copilot.tsx")
    assert "Genie isn’t connected" in component or "Genie isn't connected" in component
    assert "Ask an admin to attach a Genie space" in component
    assert (
        'return "Genie isn’t connected' in component
        or "return \"Genie isn't connected" in component
    )
    css = _read("styles", "dubois.css")
    assert "text-overflow:ellipsis" in css.replace(" ", "") or "text-overflow: ellipsis" in css


def test_copilot_covers_loading_error_disabled_c7505_defect() -> None:
    """[C7505][defect] Loading, error, and disabled states are explicit."""
    component = _read("components", "dashboard", "genie-copilot.tsx")
    assert "disabled" in component.lower()
    assert "loading" in component.lower() or "aria-busy" in component
    assert "error" in component.lower()
    assert "switch (" in component
    assert "never" in component


def test_production_bundle_ships_genie_copilot_c7505_smoke() -> None:
    """[C7505][smoke] Generated SPA bundle includes Genie drawer chrome and selectors."""
    bundle = _bundle()
    css = next((DIST / "assets").glob("index-*.css")).read_text(encoding="utf-8")
    for marker in (
        "genie-copilot",
        "Explain this alert",
        "/genie/conversations",
        "genie-copilot-drawer",
    ):
        assert marker in bundle, marker
    assert ".genie-drawer" in css


def test_every_industry_uses_per_pipeline_genie_regression() -> None:
    """[regression] Pipeline context (not the industry) selects the per-pipeline copilot."""
    cell = _read("pages", "manufacturing-cell-page.tsx")
    assert "setupId={setupId ?? pipelineId}" in cell
    mode_page = _read("pages", "mode-dashboard-page.tsx")
    assert "setupId={catalogMode?.setupId ?? pipelineId}" in mode_page
    copilot = _read("components", "dashboard", "genie-copilot.tsx")
    assert "GeniePipelineCopilot" in copilot
    assert "PER_PIPELINE_GENIE_INDUSTRIES" not in copilot
    for name in ("genie-pipeline-copilot.tsx", "genie-drawer.tsx", "genie-query-attachment.tsx"):
        assert "manufacturing" not in _read("components", "dashboard", name).lower(), name
    assert "manufacturing" not in _read("hooks", "use-pipeline-genie.ts").lower()
    pipeline = _read("components", "dashboard", "genie-pipeline-copilot.tsx")
    assert "Genie space not provisioned yet" in pipeline
    assert "Create Genie space" in pipeline
    assert "provisionPipelineGenieSpace" in _read("hooks", "use-pipeline-genie.ts")
    api = _read("lib", "api.ts")
    for fn in (
        "getPipelineGenieStatus",
        "getPipelineGenieStarters",
        "startPipelineGenieConversation",
        "addPipelineGenieMessage",
        "getPipelineGenieMessage",
        "getPipelineGenieAttachmentResult",
    ):
        assert fn in api, fn
    assert "/genie/pipelines/" in api
    assert "/admin/pipelines/" in api


def test_production_bundle_ships_pipeline_genie_smoke() -> None:
    """[smoke] Bundle includes per-pipeline Genie routes and result chart chrome."""
    bundle = _bundle()
    for marker in (
        "/genie/pipelines/",
        "Genie space not provisioned yet",
        "genie-result",
        "Show SQL",
        "truncated to",
    ):
        assert marker in bundle, marker


def test_genie_polling_is_shared_and_status_aware_regression() -> None:
    """[regression] Both copilots poll via the shared helper; only terminal statuses stop it."""
    types = _read("lib", "types.ts")
    for status in ("SUBMITTED", "ASKING_AI", "PENDING_WAREHOUSE", "QUERY_RESULT_EXPIRED"):
        assert status in types, status
    poll = _read("lib", "genie-poll.ts")
    assert "setInterval" not in poll
    assert "This conversation expired - ask again." in poll
    for name in ("genie-copilot.tsx",):
        assert "startGeniePoll" in _read("components", "dashboard", name)
    assert "startGeniePoll" in _read("hooks", "use-pipeline-genie.ts")
    assert "class ApiError" in _read("lib", "api.ts")


def test_pipeline_genie_distinguishes_unresolved_and_unsupported_regression() -> None:
    """[regression] A 404 is 'could not be resolved' (no Create); supported:false has no Create."""
    hook = _read("hooks", "use-pipeline-genie.ts")
    assert "unknown_pipeline" in hook
    assert "unsupported" in hook
    assert "supported?: boolean" in _read("lib", "types.ts")
    pipeline = _read("components", "dashboard", "genie-pipeline-copilot.tsx")
    assert "This pipeline could not be resolved" in pipeline
    assert "Genie is not available for this industry" in pipeline
    assert 'event.key === "Escape"' in _read("components", "dashboard", "genie-drawer.tsx")
