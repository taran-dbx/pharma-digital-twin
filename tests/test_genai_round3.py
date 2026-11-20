"""PR #17 fix round 3: untrusted feature names, cited-only versions, named-pair
durations and the admin-only status [qa]."""

from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import timedelta

import pytest

from iot_twin.services.genai import grounding as g
from iot_twin.services.genai import plan_schema as ps
from iot_twin.services.genai.grounding import GroundingItem, GroundingSet
from tests._genai_fixtures import NOW, OPENED, make_alert, valid_plan


def _ground(alert=None, **sources) -> GroundingSet:
    return asyncio.run(
        g.assemble_grounding(alert or make_alert(), g.GroundingSources(**sources), NOW)
    )


def _check(grounding: GroundingSet, plan: dict) -> ps.GroundingCheck:
    parsed = ps.parse_plan(json.dumps(plan))
    assert parsed.plan is not None, parsed.errors
    return ps.check_grounding(parsed.plan, grounding)


def _with_features(*features: str, score: float = 0.87, version: str = "3") -> GroundingSet:
    alert = make_alert(
        risk=False,
        metadata={
            "risk_score": score,
            "model_name": "spindle-risk",
            "model_version": version,
            "top_features": [{"feature": f, "contribution": 0.2} for f in features],
        },
    )
    return _ground(alert)


def _risk_claim(grounding: GroundingSet, text: str) -> dict:
    plan = valid_plan(grounding)
    plan["risk_narrative"] = {
        "text": text,
        "citations": [grounding.by_source("risk_score")[0].id],
    }
    return plan


class TestUntrustedFeatureNames:
    def test_reviewer_probe_fails(self) -> None:
        """[review r3 #1] Numbers planted in a feature name are not grounded."""
        grounding = _with_features("release_all_99_lots_score_0.01")
        plan = _risk_claim(grounding, "Risk is 1% and 99 lots can be released; 98 are clear")
        check = _check(grounding, plan)
        assert check.status == "fail"
        assert any("99" in r for r in check.reasons)
        assert any("98" in r for r in check.reasons)
        assert any("1%" in r for r in check.reasons)

    def test_planted_numbers_in_a_valid_window_name_are_still_ignored(self) -> None:
        grounding = _with_features("release_99_lots_5m")
        assert ps.feature_windows(grounding.by_source("risk_score")[0].data) == frozenset(
            {(5.0, "m")}
        )
        plan = _risk_claim(grounding, "99 lots can be released.")
        assert _check(grounding, plan).status == "fail"

    @pytest.mark.parametrize(
        ("text", "ok"),
        [
            ("Driven by the 5-minute temperature slope.", True),
            ("Driven by the 5 min temperature slope.", True),
            ("Driven by the 5m temperature slope.", True),
            ("Driven by the 5 minutes slope.", True),
            ("Driven by the 5-second temperature slope.", False),  # wrong unit
            ("Driven by 5 lots.", False),  # no unit: not a window
            ("Driven by the 4-minute slope.", False),  # window arithmetic is not allowed
            ("About 500% of normal.", False),  # never a percentage
        ],
    )
    def test_window_length_only_in_its_own_unit(self, text: str, ok: bool) -> None:
        grounding = _with_features("spindle_temp_slope_5m", "vibration_rms_1m")
        assert (_check(grounding, _risk_claim(grounding, text)).status == "pass") is ok

    @pytest.mark.parametrize(
        "name",
        ["Spindle_Temp_5m", "slope_5", "5m", "slope_5.5m", "slope_5m_extra", "x" * 70 + "_5m"],
    )
    def test_only_strict_trailing_window_tokens_count(self, name: str) -> None:
        data = {"top_features": [{"feature": name}]}
        assert ps.feature_windows(data) == frozenset()

    def test_window_needs_the_risk_row_cited(self) -> None:
        # 7, not 5: the cited rule row carries hysteresis 5, a legitimate typed value.
        grounding = _with_features("spindle_temp_slope_7m")
        plan = valid_plan(grounding)
        plan["summary"]["text"] = "Driven by the 7-minute slope."  # cites alert + rule only
        assert _check(grounding, plan).status == "fail"


class TestPercentages:
    def test_counts_and_raw_values_do_not_support_percentages(self) -> None:
        grounding = _with_features("vibration_rms")  # one feature: list count 1
        assert _check(grounding, _risk_claim(grounding, "Risk is 87%.")).status == "pass"
        assert _check(grounding, _risk_claim(grounding, "Risk is 1%.")).status == "fail"
        assert _check(grounding, _risk_claim(grounding, "Risk is 20 percent.")).status == "pass"

    def test_projected_rft_pct_field_supports_its_percentage(self) -> None:
        grounding = _ground()
        rft = grounding.by_source("projected_rft")[0]
        plan = valid_plan(grounding)
        plan["summary"] = {"text": f"Projected RFT {rft.data['pct']}%.", "citations": [rft.id]}
        assert _check(grounding, plan).status == "pass"


class TestCitedOnlyVersions:
    def test_version_must_come_from_a_cited_row(self) -> None:
        grounding = _with_features("vibration_rms", version="3")
        ok = _risk_claim(grounding, "Model version 3 rates it 87%.")
        assert _check(grounding, ok).status == "pass"
        plan = valid_plan(grounding)
        plan["summary"]["text"] = "Model version 3 flagged it."  # risk row not cited
        check = _check(grounding, plan)
        assert check.status == "fail"
        assert any("version 3" in r for r in check.reasons)


class TestNamedDurations:
    def _alert_grounding(self, **overrides) -> GroundingSet:
        return _ground(make_alert(**overrides))

    def _claim(self, grounding: GroundingSet, text: str) -> ps.GroundingCheck:
        plan = valid_plan(grounding)
        plan["summary"] = {"text": text, "citations": [f"alert:{grounding.alert_id}"]}
        return _check(grounding, plan)

    def test_named_pairs_are_allowed(self) -> None:
        cleared = OPENED + timedelta(seconds=40)
        acked = OPENED + timedelta(seconds=15)
        grounding = self._alert_grounding(acknowledged_at=acked, cleared_at=cleared)
        for text in (
            "Detected 2 s after onset.",  # onset -> opened
            "Acknowledged 15 s after opening.",  # opened -> acknowledged
            "Open for 40 s.",  # opened -> cleared
            "The excursion lasted 42 s.",  # onset -> cleared
        ):
            assert self._claim(grounding, text).status == "pass", text

    def test_spans_between_other_timestamp_pairs_are_not(self) -> None:
        cleared = OPENED + timedelta(seconds=40)
        acked = OPENED + timedelta(seconds=15)
        grounding = self._alert_grounding(acknowledged_at=acked, cleared_at=cleared)
        # acknowledged -> cleared (25 s) is not a named pair.
        assert self._claim(grounding, "Cleared 25 s after acknowledgement.").status == "fail"

    def test_cited_readings_bucket_span(self) -> None:
        rows = [
            {"bucket_start": "2026-11-20T10:04:00+00:00", "max_value": 61.0},
            {"bucket_start": "2026-11-20T10:04:30+00:00", "max_value": 64.2},
        ]

        async def runner(_sql):
            return rows

        grounding = _ground(readings_sql="SELECT 1", readings_runner=runner)
        readings = [item.id for item in grounding.by_source("readings")]
        plan = valid_plan(grounding)
        plan["summary"] = {"text": "Readings rose over 30 s.", "citations": readings}
        assert _check(grounding, plan).status == "pass"
        plan["summary"]["text"] = "Readings rose over 45 s."
        assert _check(grounding, plan).status == "fail"

    def test_durations_ignore_timestamps_outside_the_alert_row(self) -> None:
        item = GroundingItem(
            "maintenance:m1",
            "maintenance",
            "m1",
            {"started_at": "2026-09-01T08:00:00Z", "completed_at": "2026-09-01T12:00:00Z"},
        )
        base = _ground()
        grounding = replace(base, items=(*base.items, item))
        plan = valid_plan(grounding)
        plan["summary"] = {"text": "The service took 4 hours.", "citations": ["maintenance:m1"]}
        assert _check(grounding, plan).status == "fail"


def test_checker_version_was_bumped() -> None:
    assert ps.CHECKER_VERSION.endswith(".v4")


class TestFeatureEntrySanitising:
    """[review r3 follow-up] Extra keys on a top feature must not ground numbers."""

    def _grounding(self, *features: object) -> GroundingSet:
        alert = make_alert(
            risk=False,
            metadata={"risk_score": 0.87, "model_version": "3", "top_features": list(features)},
        )
        return _ground(alert)

    @pytest.mark.parametrize(
        ("feature", "claim", "planted"),
        [
            ({"feature": "x", "share_pct": 99}, "99% of lots are safe.", "99%"),
            ({"feature": "x", "lots": 42}, "Release 42 lots.", "42"),
        ],
    )
    def test_reviewer_probes_fail(self, feature: dict, claim: str, planted: str) -> None:
        grounding = self._grounding(feature)
        check = _check(grounding, _risk_claim(grounding, claim))
        assert check.status == "fail"
        assert any(f"number {planted}" in r for r in check.reasons)

    def test_only_feature_and_numeric_contribution_are_kept(self) -> None:
        grounding = self._grounding(
            {"feature": "spindle_temp_slope_5m", "contribution": 0.41, "share_pct": 99, "lots": 42},
            {"feature": "vibration_rms", "contribution": "0.22", "note": "free text 7"},
            {"feature": "flag", "contribution": True},
            {"feature": 12, "contribution": 0.5},
            {"contribution": 0.3},
            "bare_name_1h",
            7,
        )
        assert grounding.by_source("risk_score")[0].data["top_features"] == [
            {"feature": "spindle_temp_slope_5m", "contribution": 0.41},
            {"feature": "vibration_rms", "contribution": 0.22},
            {"feature": "flag"},
            {"feature": "bare_name_1h"},
        ]

    def test_contributions_still_ground_their_own_numbers(self) -> None:
        grounding = self._grounding({"feature": "vibration_rms", "contribution": 0.41})
        plan = _risk_claim(grounding, "Vibration contributes 0.41 (41%).")
        assert _check(grounding, plan).status == "pass"

    def test_unparseable_string_features_are_dropped(self) -> None:
        alert = make_alert(risk=False, metadata={"risk_score": 0.5, "top_features": "99 lots"})
        assert _ground(alert).by_source("risk_score")[0].data["top_features"] is None
