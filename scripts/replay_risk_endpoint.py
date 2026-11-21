#!/usr/bin/env python3
"""Replay held-out live-format telemetry through real serving endpoints.

Generates Spindle 3 readings with the live producer's own code path
(``iot_twin.ml.live_format_telemetry``: simulator, calibration, ~25 Hz,
shared scenario) on cycles no training run used, builds the scorer's feature
rows every 5 s, queries each endpoint with ``dataframe_records`` and reports
per-breach lead times, false alarms per hour, the chance baseline and the
threshold rule. Output is redacted JSON (no host, user or catalog).

    python scripts/replay_risk_endpoint.py --profile <cli-profile> \\
        --endpoint dev_<user>_pharma-twin-spindle-risk --endpoint pharma-twin-spindle-risk \\
        --hours 4 --output docs/evidence/ml/replay_v4_vs_v5.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(REPO_ROOT / "src"))
from _profile import resolve_profile  # noqa: E402

BATCH = 200


def _args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", action="append", required=True)
    parser.add_argument("--hours", type=float, default=4.0)
    parser.add_argument("--seed", type=int, default=9001)
    parser.add_argument("--start-epoch", type=float, default=0.0, help="default: replay epoch")
    parser.add_argument("--output", default="")
    return parser.parse_args(argv)


def _query(workspace: Any, endpoint: str, rows: list[Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for lo in range(0, len(rows), BATCH):
        records = [dict(row.features) for row in rows[lo : lo + BATCH]]
        for attempt in range(5):
            try:
                response = workspace.serving_endpoints.query(
                    name=endpoint, dataframe_records=records
                )
                break
            except Exception as exc:  # cold start / transient: back off, then give up
                if attempt == 4:
                    raise
                print(f"{endpoint}: {type(exc).__name__}, retrying", file=sys.stderr)
                time.sleep(30)
        out.extend(dict(item) for item in response.predictions)
    return out


def _redacted(endpoint: str) -> str:
    """``dev_<user>_pharma-twin-…`` for dev-mode names; others are unchanged."""
    idx = endpoint.find("pharma-twin")
    return f"dev_<user>_{endpoint[idx:]}" if endpoint.startswith("dev_") and idx > 0 else endpoint


def _served(workspace: Any, endpoint: str) -> dict[str, Any]:
    found = workspace.serving_endpoints.get(endpoint)
    entity = found.config.served_entities[0]
    return {
        "endpoint": _redacted(endpoint),
        "model_version": entity.entity_version,
        "ready": getattr(found.state.ready, "value", str(found.state.ready)),
    }


def _false_alarm_plans(report_alerts: list[Any], breaches: list[Any]) -> dict[str, int]:
    from iot_twin.ml.lead_metrics import false_alarms
    from iot_twin.scenario_engine.spindle_scenario import SPINDLE_CYCLE_S, spindle_cycle_plan

    counts: Counter[str] = Counter()
    for opened, _ in false_alarms(report_alerts, breaches):
        plan = spindle_cycle_plan(int(opened // SPINDLE_CYCLE_S))
        counts[f"{plan.kind}/{plan.precursor}"] += 1
    return dict(sorted(counts.items()))


def main(argv: list[str]) -> int:
    from databricks.sdk import WorkspaceClient

    from iot_twin.ml.lead_metrics import EARLY_WARNING_S, alert_episodes, breach_episodes
    from iot_twin.ml.live_format_telemetry import (
        DATA_ORIGIN,
        LIVE_RATE_HZ,
        REPLAY_START_EPOCH,
        live_format_readings,
    )
    from iot_twin.ml.replay import replay_report, replay_rows, warned_share_by_precursor
    from iot_twin.ml.spindle_features import (
        SpindleSeries,
        last_spindle_maintenance_epoch,
        threshold_rule_delay_s,
        threshold_rule_hysteresis_c,
        validated_limit_c,
    )
    from iot_twin.scenario_engine.spindle_scenario import SPINDLE_CYCLE_S, spindle_cycle_plan

    profile, rest = resolve_profile(argv, dict(os.environ))
    if not profile:
        print("pass --profile <cli-profile> (or set DATABRICKS_CONFIG_PROFILE)", file=sys.stderr)
        return 2
    args = _args(rest)
    workspace = WorkspaceClient(profile=profile)
    start = args.start_epoch or REPLAY_START_EPOCH
    duration = args.hours * 3600.0
    limit, hysteresis, delay = (
        validated_limit_c(),
        threshold_rule_hysteresis_c(),
        threshold_rule_delay_s(),
    )
    readings = live_format_readings(start=start, duration_s=duration, seed=args.seed)
    rows = replay_rows(
        readings, limit_c=limit, last_maintenance_at=last_spindle_maintenance_epoch()
    )
    times = [row.at for row in rows]
    cycles = range(int(start // SPINDLE_CYCLE_S), int((start + duration) // SPINDLE_CYCLE_S))
    plan_mix = Counter(
        f"{spindle_cycle_plan(i).kind}/{spindle_cycle_plan(i).precursor}" for i in cycles
    )
    temp = SpindleSeries.from_readings(readings).points["temp"]
    breaches = breach_episodes(*temp, limit_c=limit, hysteresis_c=hysteresis, duration_s=delay)
    result: dict[str, Any] = {
        "data_origin": DATA_ORIGIN,
        "note": "SIMULATED scenario physics; held-out cycles replayed through the serving path",
        "replay": {
            "hours": args.hours,
            "sample_rate_hz": LIVE_RATE_HZ,
            "scoring_interval_s": 5.0,
            "scoring_rounds": len(rows),
            "seed": args.seed,
            "cycles": len(cycles),
            "cycle_plan_mix": dict(sorted(plan_mix.items())),
            "breaches": len(breaches),
        },
        "threshold_rule": {
            "lead_s": -delay,
            "warned_30s_share": 0.0,
            "note": "opens duration_s after each onset",
        },
        "models": [],
    }
    for endpoint in args.endpoint:
        predictions = _query(workspace, endpoint, rows)
        scores = [float(p["risk_score"]) for p in predictions]
        open_at = float(predictions[0]["decision_threshold"])
        clear_at = float(predictions[0]["clear_threshold"])
        report, details = replay_report(
            readings,
            times,
            scores,
            open_at=open_at,
            clear_at=clear_at,
            limit_c=limit,
            hysteresis_c=hysteresis,
            rule_delay_s=delay,
        )
        alerts = alert_episodes(times, scores, open_at=open_at, clear_at=clear_at)
        result["models"].append(
            {
                **_served(workspace, endpoint),
                "decision_threshold": open_at,
                "clear_threshold": clear_at,
                "metrics": {k: round(v, 4) for k, v in report.as_metrics().items()},
                "warned_by_precursor": warned_share_by_precursor(details, EARLY_WARNING_S),
                "false_alarms_by_cycle_plan": _false_alarm_plans(alerts, breaches),
                "breaches": details,
            }
        )
    text = json.dumps(result, indent=2, sort_keys=True)
    if args.output:
        Path(args.output).write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
