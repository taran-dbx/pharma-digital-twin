"""Savings model maths: projection, actuals and assumption validation [qa]."""

from __future__ import annotations

import json
import math

import pytest

from iot_twin.services.value.episodes import WindowTotals
from iot_twin.services.value.value_model import (
    DEFAULTS_PATH,
    AssumptionError,
    actuals,
    default_assumptions,
    per_event_breakdown,
    project,
    resolve_assumptions,
)


def _assumptions(**overrides):
    base = {
        "lines_in_scope": 2,
        "excursion_events_per_line_per_year": 10,
        "cost_per_lot_lost": 50_000,
        "capture_rate": 0.5,
        "inspection_cost": 2_000,
        "lot_release_delay_cost_per_day": 0,
        "release_delay_days_per_event": 0,
    }
    return resolve_assumptions({**base, **overrides})


class TestDefaults:
    def test_defaults_file_is_valid_and_sums_components_c7900_smoke(self) -> None:
        """[C7900][smoke] Defaults load, validate, and cost per lot = sum of components."""
        raw = json.loads(DEFAULTS_PATH.read_text())
        a = default_assumptions()
        assert a.cost_per_lot_lost == sum(raw["lot_cost_components"].values())
        assert dict(a.lot_cost_components) == raw["lot_cost_components"]
        assert 0 <= a.capture_rate <= 1

    def test_invalid_defaults_payload_raises_c7901_regression(self) -> None:
        """[C7901][regression] A broken defaults payload is rejected, not silently used."""
        with pytest.raises(AssumptionError):
            default_assumptions({"lot_cost_components": {}, "capture_rate": 2})


class TestProjection:
    def test_formula_numbers_c7902_smoke(self) -> None:
        """[C7902][smoke] exposure = events × loss; avoided = caught × (loss − inspection)."""
        out = project(_assumptions())
        assert out["annual_events"] == 20
        assert out["annual_exposure"] == 1_000_000
        assert out["caught_events"] == 10
        assert out["cost_avoided"] == 10 * 48_000
        assert out["exposure_remaining"] == 1_000_000 - 480_000
        # identity: remaining = missed × loss + caught × inspection
        assert out["exposure_remaining"] == 10 * 50_000 + 10 * 2_000
        assert "= $1,000,000" in out["formula"]["annual_exposure"]

    def test_zero_events_c7903_regression(self) -> None:
        """[C7903][regression] Zero excursions → zero exposure, nothing avoided or remaining."""
        out = project(_assumptions(excursion_events_per_line_per_year=0))
        assert out["annual_exposure"] == 0
        assert out["cost_avoided"] == 0
        assert out["exposure_remaining"] == 0

    def test_capture_zero_and_one_c7904_regression(self) -> None:
        """[C7904][regression] capture 0 avoids nothing; capture 1 leaves only inspection cost."""
        none = project(_assumptions(capture_rate=0))
        assert none["cost_avoided"] == 0
        assert none["exposure_remaining"] == none["annual_exposure"]
        full = project(_assumptions(capture_rate=1))
        assert full["missed_events"] == 0
        assert full["exposure_remaining"] == 20 * 2_000

    def test_release_delay_adds_to_loss_c7905_regression(self) -> None:
        """[C7905][regression] Optional lot-release delay cost is part of the loss per event."""
        out = project(
            _assumptions(lot_release_delay_cost_per_day=1_000, release_delay_days_per_event=3)
        )
        assert out["loss_per_event"] == 53_000
        items = {line["item"] for line in out["per_event"]["if_missed"]}
        assert "lot_release_delay" in items

    def test_inspection_above_loss_warns_c7906_regression(self) -> None:
        """[C7906][regression] An inspection dearer than the lot gives negative avoidance + a warning."""
        out = project(_assumptions(cost_per_lot_lost=1_000, inspection_cost=2_000))
        assert out["cost_avoided"] < 0
        assert out["warnings"]

    def test_breakdown_sums_to_loss_c7907_smoke(self) -> None:
        """[C7907][smoke] The per-event breakdown sums to the loss used in the projection."""
        a = default_assumptions()
        breakdown = per_event_breakdown(a)
        total = sum(line["usd"] for line in breakdown["if_missed"])
        assert math.isclose(total, breakdown["loss_per_event"])
        assert breakdown["net_avoided_per_caught_event"] == a.loss_per_event - a.inspection_cost

    def test_cost_override_replaces_components_c7908_regression(self) -> None:
        """[C7908][regression] Overriding cost per lot collapses the breakdown to one line."""
        a = resolve_assumptions({"cost_per_lot_lost": "12345"})
        assert a.lot_cost_components == (("override", 12345.0),)


class TestValidation:
    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("capture_rate", "1.5"),
            ("capture_rate", "-0.1"),
            ("capture_rate", "nan"),
            ("lines_in_scope", "0"),
            ("lines_in_scope", "2.5"),
            ("cost_per_lot_lost", "abc"),
            ("inspection_cost", "inf"),
            ("release_delay_days_per_event", "400"),
            ("not_a_field", "1"),
        ],
    )
    def test_invalid_inputs_rejected_c7909_regression(self, field: str, value: str) -> None:
        """[C7909][regression] Out-of-range, non-numeric and unknown assumptions are rejected."""
        with pytest.raises(AssumptionError) as info:
            resolve_assumptions({field: value})
        assert info.value.errors[0]["field"] == field

    def test_boolean_is_not_a_number_c7910_regression(self) -> None:
        """[C7910][regression] JSON booleans never pass as 0/1."""
        with pytest.raises(AssumptionError):
            resolve_assumptions({"capture_rate": True})

    def test_all_errors_reported_together_c7911_regression(self) -> None:
        """[C7911][regression] Every invalid field is reported, not just the first."""
        with pytest.raises(AssumptionError) as info:
            resolve_assumptions({"capture_rate": "2", "lines_in_scope": "-1"})
        assert {e["field"] for e in info.value.errors} == {"capture_rate", "lines_in_scope"}


class TestActuals:
    def test_no_episodes_c7912_smoke(self) -> None:
        """[C7912][smoke] An empty window reports zeros and no observed capture rate."""
        out = actuals(WindowTotals(), _assumptions())
        assert out["excursion_events_detected"] == 0
        assert out["cost_avoided_to_date"] == 0
        assert out["capture_rate_observed"] is None

    def test_cost_avoided_to_date_c7913_smoke(self) -> None:
        """[C7913][smoke] cost avoided = caught × (loss − inspection); missed priced if scrapped."""
        totals = WindowTotals(
            episodes=3, caught_in_time=1, lots_protected=1, deadline_before_alert=1
        )
        out = actuals(totals, _assumptions())
        assert out["excursion_events_detected"] == 3
        assert out["not_caught_in_time"] == 2
        assert out["cost_avoided_to_date"] == 48_000
        assert out["exposure_if_scrapped"] == 100_000
        assert out["deadline_before_alert"] == 1

    def test_release_delay_priced_on_both_sides_c7914_regression(self) -> None:
        """[C7914][regression] To-date figures use loss_per_event, like the projection."""
        a = _assumptions(lot_release_delay_cost_per_day=1_000, release_delay_days_per_event=3)
        out = actuals(WindowTotals(episodes=2, caught_in_time=1), a)
        assert out["cost_avoided_to_date"] == 53_000 - 2_000
        assert out["exposure_if_scrapped"] == 53_000
        assert project(a)["loss_per_event"] == 53_000

    def test_parts_released_from_quarantined_lots_c7915_regression(self) -> None:
        """[C7915][regression] Released = parts of the held lots that were not at risk."""
        totals = WindowTotals(episodes=1, lots_quarantined=2, parts_quarantined=7)
        out = actuals(totals, _assumptions())
        assert out["parts_quarantined"] == 7
        assert out["parts_released"] == 2 * 15 - 7
