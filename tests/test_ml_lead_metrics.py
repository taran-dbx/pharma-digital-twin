"""Per-breach early-warning metrics, event-based threshold choice and the promotion rule."""

from __future__ import annotations

import pytest

from iot_twin.ml import lead_metrics as lm
from iot_twin.ml.evaluation import challenger_beats_champion
from iot_twin.ml.spindle_features import SpindleSeries
from iot_twin.ml.training_set import (
    LabelledRow,
    TrainingSet,
    breach_labels,
    choose_event_threshold,
    period_event_report,
)


class TestBreachEpisodes:
    def test_needs_duration_over_the_limit_and_clears_with_hysteresis(self) -> None:
        times = [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]
        values = [50, 61, 59, 61, 62, 63, 57, 54]
        # The 1 s spike resets; the run from t=3 lasts 2 s; clears at <= 55 (t=7).
        assert lm.breach_episodes(times, values, limit_c=60, hysteresis_c=5, duration_s=2) == [
            (3.0, 7.0)
        ]

    def test_open_breach_at_the_end_has_no_clear_time(self) -> None:
        assert lm.breach_episodes(
            [0, 1, 2, 3], [61, 61, 61, 61], limit_c=60, hysteresis_c=5, duration_s=2
        ) == [(0, None)]

    def test_noise_inside_the_clear_band_does_not_reopen(self) -> None:
        values = [61, 61, 61, 58, 61, 59, 61, 55, 61, 61, 61]
        eps = lm.breach_episodes(range(11), values, limit_c=60, hysteresis_c=5, duration_s=2)
        assert eps == [(0, 7), (8, None)]


class TestAlertEpisodes:
    def test_open_and_clear_hysteresis(self) -> None:
        times = [0, 5, 10, 15, 20, 25]
        scores = [0.1, 0.6, 0.45, 0.55, 0.39, 0.7]
        assert lm.alert_episodes(times, scores, open_at=0.5, clear_at=0.4) == [(5, 20), (25, None)]

    def test_clear_above_open_is_rejected(self) -> None:
        with pytest.raises(ValueError):
            lm.alert_episodes([0], [0.1], open_at=0.4, clear_at=0.5)


class TestLeadsAndFalseAlarms:
    def test_lead_counts_only_an_alert_open_at_the_onset(self) -> None:
        alerts = [(100.0, 150.0), (160.0, 260.0)]
        assert lm.breach_leads([200.0], alerts) == [40.0]
        assert lm.breach_leads([155.0], alerts) == [None]  # cleared before the onset

    def test_lookback_caps_stuck_alerts(self) -> None:
        assert lm.breach_leads([1000.0], [(0.0, None)], lookback_s=240) == [None]
        assert lm.breach_leads([200.0], [(0.0, None)], lookback_s=240) == [200.0]

    def test_alert_opened_after_the_onset_is_a_miss(self) -> None:
        assert lm.breach_leads([100.0], [(100.0, 200.0)]) == [None]

    def test_false_alarm_definition(self) -> None:
        breaches = [(500.0, 560.0)]
        alerts = [
            (300.0, 520.0),  # followed by a breach within 240 s: a warning
            (510.0, 600.0),  # opened inside the breach: not a false alarm
            (900.0, 960.0),  # nothing follows: false alarm
        ]
        assert lm.false_alarms(alerts, breaches) == [(900.0, 960.0)]

    def test_shifted_wraps_inside_the_span(self) -> None:
        assert lm.shifted([(90.0, 95.0)], 20.0, 0.0, 100.0) == [(10.0, 15.0)]


class TestEventReport:
    def _cycle_data(self, cycles: int, lead: float):
        breaches = [(c * 300.0 + 230.0, c * 300.0 + 280.0) for c in range(cycles)]
        alerts = [(onset - lead, onset + 30.0) for onset, _ in breaches]
        return breaches, alerts

    def test_perfect_warning_beats_the_chance_baseline(self) -> None:
        breaches, alerts = self._cycle_data(12, 60.0)
        report = lm.event_report(breaches, alerts, start=0.0, end=3600.0, rule_delay_s=2.0)
        assert report.breaches == 12 and report.warned_30s == 12
        assert report.warned_30s_share == 1.0
        assert report.median_lead_s == pytest.approx(60.0)
        assert report.false_alarms == 0 and report.false_alarms_per_hour == 0.0
        assert report.hours == pytest.approx(1.0)
        assert report.rule_lead_s == -2.0
        assert report.chance_warned_30s_mean < 0.35
        assert 0.0 <= report.chance_warned_30s_p95 <= 1.0

    def test_metrics_names_are_prefixed(self) -> None:
        breaches, alerts = self._cycle_data(2, 10.0)
        report = lm.event_report(breaches, alerts, start=0.0, end=600.0, rule_delay_s=2.0)
        metrics = report.as_metrics("test_event_")
        assert metrics["test_event_breaches_warned"] == 2.0
        assert metrics["test_event_breaches_warned_30s"] == 0.0
        assert set(metrics) >= {
            "test_event_false_alarms_per_hour",
            "test_event_chance_warned_30s_mean",
        }

    def test_combine_pools_leads_and_chance_offset_by_offset(self) -> None:
        a = lm.event_report(*self._cycle_data(4, 60.0), start=0.0, end=1200.0, rule_delay_s=2.0)
        b = lm.event_report([], [(10.0, 20.0)], start=0.0, end=1200.0, rule_delay_s=2.0)
        pooled = lm.combine([a, b])
        assert pooled.breaches == 4 and pooled.false_alarms == 1
        assert pooled.hours == pytest.approx(a.hours + b.hours)
        assert len(pooled.chance_counts) == lm.CHANCE_OFFSETS
        assert pooled.chance_warned_30s_mean == pytest.approx(a.chance_warned_30s_mean)


def _training_set(
    cycles: int, precursor_lead: float
) -> tuple[TrainingSet, list[LabelledRow], list[float]]:
    """One run with a breach at 230 s of every cycle and scores that rise ``precursor_lead`` before."""
    breaches = tuple((c * 300.0 + 230.0, c * 300.0 + 280.0) for c in range(cycles))
    rows, scores = [], []
    for c in range(cycles):
        for t in range(0, 300, 5):
            at = c * 300.0 + t
            inside = 230 <= t <= 280
            if inside:
                continue
            rows.append(LabelledRow("r", at, {}, int(230 - 120 <= t < 230)))
            scores.append(0.8 if 230 - precursor_lead <= t < 230 else 0.1)
    ts = TrainingSet(
        rows=tuple(rows),
        series_by_run={"r": SpindleSeries(points={"temp": ((), ())})},
        data_origin="test",
        breaches_by_run={"r": breaches},
    )
    return ts, rows, scores


class TestPeriodReportAndThreshold:
    def test_period_report_replays_scores_per_run(self) -> None:
        ts, rows, scores = _training_set(6, 90.0)
        report = period_event_report(
            ts, rows, scores, open_at=0.5, clear_at=0.3, limit_c=60, hysteresis_c=5, rule_delay_s=2
        )
        assert report.breaches == 6 and report.warned_30s == 6
        assert report.median_lead_s == pytest.approx(90.0)

    def test_event_threshold_prefers_early_warning_within_budget(self) -> None:
        ts, rows, scores = _training_set(12, 90.0)
        chosen = choose_event_threshold(
            ts, rows, scores, limit_c=60, hysteresis_c=5, rule_delay_s=2, grid=[0.05, 0.5, 0.9]
        )
        assert chosen.threshold == 0.5
        assert chosen.warned_30s_share == 1.0
        assert chosen.rule.startswith("max_warned_30s")

    def test_event_threshold_falls_back_to_fewest_false_alarms(self) -> None:
        ts, rows, _ = _training_set(4, 90.0)
        noisy = [0.8 if i % 2 else 0.1 for i in range(len(rows))]
        chosen = choose_event_threshold(
            ts,
            rows,
            noisy,
            limit_c=60,
            hysteresis_c=5,
            rule_delay_s=2,
            false_alarm_budget_per_h=-1.0,
            grid=[0.5, 0.9],
        )
        assert chosen.rule == "min_false_alarms_per_h"
        assert chosen.threshold == 0.9


class TestBreachLabels:
    def test_labels_follow_breach_onsets_and_drop_in_breach_rows(self) -> None:
        breaches = [(230.0, 280.0)]
        labels = breach_labels([100.0, 115.0, 229.0, 240.0, 290.0], breaches, horizon_s=120.0)
        assert labels == [0, 1, 1, None, 0]


class TestPromotionRule:
    base = {
        "warned_30s_share": 0.3,
        "chance_warned_30s_mean": 0.1,
        "false_alarms_per_hour": 2.0,
        "pr_auc": 0.5,
    }

    def test_no_champion_promotes(self) -> None:
        assert challenger_beats_champion(self.base, None)[0] is True

    def test_must_warn_more_without_extra_false_alarms_or_worse_ranking(self) -> None:
        better = {
            "warned_30s_share": 0.6,
            "chance_warned_30s_mean": 0.2,
            "false_alarms_per_hour": 2.5,
            "pr_auc": 0.6,
        }
        assert challenger_beats_champion(better, self.base)[0] is True
        noisy = {**better, "false_alarms_per_hour": 3.5}
        assert challenger_beats_champion(noisy, self.base)[0] is False
        same = {**better, "warned_30s_share": 0.3}
        assert challenger_beats_champion(same, self.base)[0] is False
        worse_rank = {**better, "pr_auc": 0.4}
        beats, reason = challenger_beats_champion(worse_rank, self.base)
        assert beats is False and "PR-AUC" in reason

    def test_an_alert_that_is_simply_open_more_cannot_win(self) -> None:
        lucky = {
            "warned_30s_share": 0.6,
            "chance_warned_30s_mean": 0.45,  # skill 0.15 < champion's 0.2
            "false_alarms_per_hour": 2.0,
            "pr_auc": 0.6,
        }
        beats, reason = challenger_beats_champion(lucky, self.base)
        assert beats is False and "skill over chance" in reason


class TestCalmAndDuty:
    def test_open_fraction_counts_stuck_alerts_to_the_end(self) -> None:
        assert lm.open_fraction([(0.0, 30.0), (90.0, None)], start=0.0, end=100.0) == 0.4

    def test_report_exposes_alert_duty(self) -> None:
        report = lm.event_report([], [(0.0, 900.0)], start=0.0, end=3600.0, rule_delay_s=2.0)
        assert report.alert_open_fraction == pytest.approx(0.25)
        assert report.as_metrics()["alert_open_fraction"] == pytest.approx(0.25)

    def test_calm_mask_excludes_the_run_up_and_the_settle_after_a_breach(self) -> None:
        from iot_twin.ml.training_set import calm_mask

        rows = [LabelledRow("r", float(at), {}, 0) for at in (0, 100, 285, 345, 700, 900)]
        ts = TrainingSet(
            rows=tuple(rows),
            series_by_run={},
            data_origin="test",
            breaches_by_run={"r": ((230.0, 280.0), (1100.0, 1150.0))},
        )
        assert calm_mask(ts, rows) == [False, False, False, True, True, False]
        # 0/100: run-up to 230; 285: settling; 345/700: calm; 900: run-up to 1100.

    def test_min_clear_rejects_bands_that_would_not_clear_in_calm(self) -> None:
        ts, rows, scores = _training_set(12, 90.0)
        chosen = choose_event_threshold(
            ts,
            rows,
            scores,
            limit_c=60,
            hysteresis_c=5,
            rule_delay_s=2,
            grid=[0.5],
            clear_bands=(0.05, 0.45),
            min_clear=0.3,
        )
        assert chosen.clear_band == 0.05
