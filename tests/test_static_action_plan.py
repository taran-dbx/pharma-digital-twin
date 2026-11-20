"""Static checks for the AI action-plan card in the incident panel [qa]."""

from __future__ import annotations

from tests.test_static_alert_ui import _bundle, _read


def test_card_is_in_the_incident_panel_and_the_shipped_bundle() -> None:
    panel = _read("components", "cell", "incident-panel.tsx")
    assert "<ActionPlanCard" in panel
    assert "prefill={prefill}" in panel
    bundle = _bundle()
    for marker in ("AI action plan", "Apply via response workflow", "/genai/audit/", "action-plan"):
        assert marker in bundle


def test_apply_prefills_the_signed_form_and_never_records_directly() -> None:
    card = _read("components", "cell", "action-plan-card.tsx")
    assert "recordAlertResponse" not in card  # the card cannot sign a decision
    assert "onApply(action, prefillFor(plan, step))" in card
    form = _read("components", "cell", "decision-signoff.tsx")
    assert "action_plan_id: prefill.planId" in form
    assert "useState(false)" in form  # the signature intent is never pre-ticked
