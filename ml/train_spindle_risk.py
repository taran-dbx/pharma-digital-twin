"""Train, evaluate and register the Spindle 3 thermal-risk model (serverless job task).

Run by the ``pharma-twin-risk-model-train`` job (resources/risk_model.yml) as a
``spark_python_task``. Steps:

1. Load labelled rows. ``--data-source auto`` trains on real
   ``manufacturing_realtime`` telemetry when it has enough labelled rows and
   excursions, and otherwise on LIVE-FORMAT runs: the live producer's own
   simulator, calibration, sample rate and shared Spindle 3 scenario
   (``iot_twin.ml.live_format_telemetry``), on a virtual clock. ``synthetic``
   is the older 1 Hz generator (``iot_twin.ml.synthetic_telemetry``). The
   choice is logged as the ``data_origin`` param and tag.
2. Features and labels come from ``iot_twin.ml.spindle_features``, the same
   code the app's scorer runs, so training and serving cannot drift.
3. Time-based split (train → validation → test, in time order), LightGBM,
   cut-off chosen on validation, metrics on test.
4. Log to MLflow (params, metrics, signature, input example) and register in
   Unity Catalog with the ``challenger`` alias.
5. ``--promote if_better`` (default) selects ``champion`` neutrally: the new
   version, the current champion and ``--compare-versions`` are scored on the
   same pooled held-out cycles (test split plus a fresh replay), each must pass
   ``evaluation.qualifies`` (including input invariance), and the best by
   standardised discrimination wins (``evaluation.neutral_winner``). Outcomes
   are logged and tagged on every version. ``always`` moves champion to the
   new version (if its clear level is above calm running); ``never`` reports
   only. The gate is defined for simulated live-format data; any other
   ``data_origin`` forces ``never``.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

logger = logging.getLogger("pharma_twin.train_spindle_risk")

MIN_REAL_ROWS = 2000
MIN_REAL_EXCURSIONS = 20
REAL_MAX_ROWS = 2_000_000
VALIDATION_FRACTION = 0.2
ENDPOINT_WAIT_S = 1200
ENDPOINT_POLL_S = 30
PIP_PINNED = ("mlflow", "lightgbm", "pandas", "numpy")


def _args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--src-root", required=True, help="Path to the bundle's src/ folder")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", default="pharma_twin")
    parser.add_argument("--model-name", default="spindle_thermal_risk")
    parser.add_argument("--telemetry-table", default="", help="catalog.schema.table or empty")
    parser.add_argument(
        "--data-source", choices=("auto", "live_format", "synthetic", "real"), default="auto"
    )
    parser.add_argument("--real-days", type=int, default=14)
    parser.add_argument("--horizon-s", type=float, default=120.0)
    parser.add_argument("--step-s", type=float, default=5.0)
    parser.add_argument("--test-fraction", type=float, default=0.25)
    # 0.9: at 0.8 the cut-off (~0.26) sat inside the calm-score range of a worn
    # spindle (0.20-0.27 at >= 1950 h), so calm running could open alerts.
    parser.add_argument("--min-precision", type=float, default=0.9)
    parser.add_argument(
        "--clear-hysteresis",
        type=float,
        default=0.05,
        help="clear band (cut-off minus this) for --threshold-rule precision; the event rule chooses its own band",
    )
    parser.add_argument("--synthetic-runs", type=int, default=24)
    parser.add_argument("--synthetic-run-s", type=int, default=7200)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--live-runs", type=int, default=36)
    parser.add_argument("--live-run-s", type=float, default=3600.0)
    parser.add_argument("--live-rate-hz", type=float, default=25.0)
    parser.add_argument(
        "--threshold-rule",
        choices=("event", "precision"),
        default="event",
        help="event: most breaches warned >= 30 s ahead within --false-alarm-budget "
        "(live-format data only); precision: highest recall at --min-precision",
    )
    parser.add_argument("--false-alarm-budget", type=float, default=3.0, help="per hour")
    parser.add_argument("--promote", choices=("if_better", "always", "never"), default="if_better")
    parser.add_argument("--challenger-alias", default="challenger")
    parser.add_argument(
        "--compare-versions",
        default="",
        help="comma-separated extra versions scored on the same held-out rows (fallbacks)",
    )
    parser.add_argument("--code-sha", default="unknown", help="git SHA the run was submitted from")
    parser.add_argument("--code-dirty", default="unknown", help="true if the tree had local edits")
    parser.add_argument("--source-hash", default="unknown", help="sha256 of the uploaded source")
    parser.add_argument("--replay-hours", type=float, default=12.0)
    parser.add_argument("--replay-seed", type=int, default=21)
    parser.add_argument(
        "--replay-start-epoch",
        type=float,
        default=1_806_000_000.0,
        help="fresh replay cycles pooled into the gate; use a range no run has seen",
    )
    parser.add_argument(
        "--live-start-epoch",
        type=float,
        default=0.0,
        help="first live-format run start (0: the module default); use a fresh range per run",
    )
    parser.add_argument("--experiment", default="", help="MLflow experiment path")
    parser.add_argument("--alias", default="champion")
    parser.add_argument("--endpoint-name", default="", help="Serving endpoint to repoint")
    parser.add_argument("--create-endpoint", default="false")
    return parser.parse_args(argv)


def _import_shared(src_root: str) -> None:
    root = str(Path(src_root).resolve())
    if root not in sys.path:
        sys.path.insert(0, root)


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------
def _real_readings(table: str, days: int) -> list[dict[str, Any]]:
    from pyspark.sql import SparkSession

    from iot_twin.ml.spindle_features import SPINDLE_SENSOR_NAMES

    spark = SparkSession.builder.getOrCreate()
    names = ", ".join(f"'{name}'" for name in SPINDLE_SENSOR_NAMES)
    columns = "sensor_name, value, event_time"
    if "metadata" in spark.table(table).columns:
        columns += ", metadata"
    frame = spark.sql(
        f"SELECT {columns} FROM {table} "  # noqa: S608 - identifiers from job params
        f"WHERE sensor_name IN ({names}) "
        f"AND event_time >= current_timestamp() - INTERVAL {int(days)} DAYS "
        f"ORDER BY event_time LIMIT {REAL_MAX_ROWS}"
    )
    return [row.asDict() for row in frame.collect()]


def _quoted(table: str) -> str:
    return ".".join(f"`{part.strip('`')}`" for part in table.split("."))


def load_training_set(args: argparse.Namespace) -> Any:
    from dataclasses import replace

    from iot_twin.ml.live_format_telemetry import pinned_maintenance
    from iot_twin.ml.spindle_features import (
        excursion_onsets,
        last_spindle_maintenance_epoch,
        threshold_rule_hysteresis_c,
        validated_limit_c,
    )
    from iot_twin.ml.synthetic_telemetry import DATA_ORIGIN, generate_runs
    from iot_twin.ml.training_set import RunInput, build_training_set, is_scenario_telemetry

    limit = validated_limit_c()
    common = {"limit_c": limit, "horizon_s": args.horizon_s, "step_s": args.step_s}
    if args.data_source in {"auto", "real"} and args.telemetry_table:
        try:
            readings = _real_readings(_quoted(args.telemetry_table), args.real_days)
        except Exception as exc:  # table missing / no access: fall back unless forced
            if args.data_source == "real":
                raise
            logger.warning("real telemetry unavailable (%s); using synthetic", exc)
            readings = []
        if readings and is_scenario_telemetry(readings):
            # The live producer tags simulated readings with metadata.scenario_id.
            message = (
                "telemetry carries metadata.scenario_id: it is simulated producer "
                "output, not a real machine"
            )
            if args.data_source == "real":
                raise ValueError(message)
            logger.warning("%s; using live_format", message)
            readings = []
        if readings:
            real = build_training_set(
                [RunInput("real", readings, last_spindle_maintenance_epoch())],
                data_origin="real",
                **common,
            )
            # The catalog maintenance date is a fixture, so its age is not a
            # measurement either: pin it as for live-format rows.
            real = replace(real, rows=tuple(pinned_maintenance(row) for row in real.rows))
            events = len(
                excursion_onsets(
                    real.series_by_run["real"],
                    limit_c=limit,
                    hysteresis_c=threshold_rule_hysteresis_c(),
                )
            )
            enough = len(real.rows) >= MIN_REAL_ROWS and events >= MIN_REAL_EXCURSIONS
            logger.info("real telemetry: %d rows, %d excursions", len(real.rows), events)
            if enough or args.data_source == "real":
                return real
    if args.data_source in {"auto", "live_format"}:
        from iot_twin.ml.live_format_telemetry import build_live_format_training_set
        from iot_twin.ml.spindle_features import threshold_rule_delay_s

        return build_live_format_training_set(
            n_runs=args.live_runs,
            duration_s=args.live_run_s,
            seed=args.seed,
            limit_c=limit,
            hysteresis_c=threshold_rule_hysteresis_c(),
            rule_delay_s=threshold_rule_delay_s(),
            horizon_s=args.horizon_s,
            step_s=args.step_s,
            rate_hz=args.live_rate_hz,
            **({"start_epoch": args.live_start_epoch} if args.live_start_epoch else {}),
        )
    runs = generate_runs(
        n_runs=args.synthetic_runs, duration_s=args.synthetic_run_s, seed=args.seed
    )
    return build_training_set(
        [RunInput(run.run_id, run.readings, run.last_maintenance_at) for run in runs],
        data_origin=DATA_ORIGIN,
        **common,
    )


def _frame(rows: list[Any]) -> tuple[Any, Any]:
    import pandas as pd

    from iot_twin.ml.spindle_features import FEATURE_NAMES

    features = pd.DataFrame([dict(row.features) for row in rows], columns=list(FEATURE_NAMES))
    labels = pd.Series([row.label for row in rows], name="label")
    return features.astype(float), labels


# ---------------------------------------------------------------------------
# Train + evaluate
# ---------------------------------------------------------------------------
LGBM_PARAMS: dict[str, Any] = {
    "n_estimators": 400,
    "learning_rate": 0.05,
    "num_leaves": 31,
    "min_child_samples": 40,
    "subsample": 0.8,
    "subsample_freq": 1,
    "colsample_bytree": 0.8,
    "reg_lambda": 1.0,
    "random_state": 7,
    "verbose": -1,
}


def _feature_gain(model: Any, feature: str) -> float:
    """Total split gain of ``feature`` in the trained booster (0.0: never split on)."""
    booster = model.booster_
    gains = dict(
        zip(booster.feature_name(), booster.feature_importance(importance_type="gain"), strict=True)
    )
    return float(gains.get(feature, 0.0))


def train_and_evaluate(training_set: Any, args: argparse.Namespace) -> dict[str, Any]:
    import lightgbm as lgb
    from sklearn.metrics import average_precision_score, roc_auc_score

    from iot_twin.ml.evaluation import (
        calm_score_check,
        choose_threshold,
        clear_threshold_for,
        is_calm,
        precision_recall_at,
        quantile,
        two_classes,
    )
    from iot_twin.ml.spindle_features import (
        WINDOW_S,
        lead_time_report,
        threshold_rule_delay_s,
        threshold_rule_hysteresis_c,
        validated_limit_c,
    )
    from iot_twin.ml.training_set import (
        SEARCH_CLEAR_MARGIN,
        choose_event_threshold,
        onsets_in_period,
        period_event_report,
        steady_clear_floor,
        time_split,
    )

    # Rows whose window or horizon straddles a boundary would leak across it.
    embargo = WINDOW_S + args.horizon_s
    train_rows, test_rows = time_split(list(training_set.rows), args.test_fraction, embargo)
    fit_rows, val_rows = time_split(train_rows, VALIDATION_FRACTION, embargo)
    x_fit, y_fit = _frame(fit_rows)
    x_val, y_val = _frame(val_rows)
    x_test, y_test = _frame(test_rows)
    if not two_classes(list(y_fit)) or not two_classes(list(y_val)):
        raise ValueError(
            "training or validation split has a single class; use more data or another split"
        )

    model = lgb.LGBMClassifier(**LGBM_PARAMS)
    model.fit(
        x_fit,
        y_fit,
        eval_set=[(x_val, y_val)],
        # Log-loss, not average precision: AP on a few hours of validation is
        # noisy enough to stop at a handful of trees (v8 stopped at 5, its
        # scores compressed below any usable cut-off).
        eval_metric="binary_logloss",
        callbacks=[lgb.early_stopping(40, first_metric_only=True, verbose=False)],
    )
    val_scores = model.predict_proba(x_val)[:, 1]
    band = args.clear_hysteresis
    # Steady-running p90: the clear level must not sit below it (blocking at promotion).
    min_clear = (
        steady_clear_floor(training_set, val_rows, [float(s) for s in val_scores])
        + SEARCH_CLEAR_MARGIN
    )
    if args.threshold_rule == "event" and training_set.breaches_by_run:
        chosen = choose_event_threshold(
            training_set,
            val_rows,
            [float(score) for score in val_scores],
            min_clear=min_clear,
            limit_c=validated_limit_c(),
            hysteresis_c=threshold_rule_hysteresis_c(),
            rule_delay_s=threshold_rule_delay_s(),
            false_alarm_budget_per_h=args.false_alarm_budget,
            step_s=args.step_s,
        )
        threshold, band, rule = chosen.threshold, chosen.clear_band, chosen.rule
    else:
        threshold, rule = choose_threshold(
            list(y_val), list(val_scores), min_precision=args.min_precision
        )
    clear = clear_threshold_for(threshold, band)
    legacy_calm = [
        is_calm(int(y), float(margin), float(slope))
        for y, margin, slope in zip(y_val, x_val["temp_margin_c"], x_val["temp_slope"], strict=True)
    ]
    calm_metrics, calm_problems = calm_score_check(
        [float(s) for s in val_scores],
        legacy_calm,
        [float(h) for h in x_val["hours_since_maintenance"]],
        cutoff=threshold,
        clear=clear,
    )
    negatives = [float(s) for s, y in zip(val_scores, y_val, strict=True) if y == 0]
    calm_metrics["val_negative_score_p99"] = quantile(negatives, 0.99)
    for problem in calm_problems:
        logger.warning("calm-score check: %s", problem)
    test_scores = model.predict_proba(x_test)[:, 1]
    at_cut = precision_recall_at(list(y_test), list(test_scores), threshold)

    limit = validated_limit_c()
    if training_set.breaches_by_run:
        start = min(row.at for row in test_rows)
        onsets = sorted(
            onset
            for episodes in training_set.breaches_by_run.values()
            for onset, _ in episodes
            if onset >= start
        )
    else:
        onsets = onsets_in_period(
            training_set, test_rows, limit_c=limit, hysteresis_c=threshold_rule_hysteresis_c()
        )
    lead = lead_time_report(
        [row.at for row in test_rows],
        list(test_scores),
        onsets,
        threshold=threshold,
        horizon_s=args.horizon_s,
        rule_delay_s=threshold_rule_delay_s(),
    )
    ranking: dict[str, float] = {}
    if two_classes(list(y_test)):
        ranking = {
            "test_roc_auc": float(roc_auc_score(y_test, test_scores)),
            "test_pr_auc": float(average_precision_score(y_test, test_scores)),
        }
    else:
        logger.warning("test split has a single class; ROC-AUC / PR-AUC not logged")
    events = period_event_report(
        training_set,
        test_rows,
        [float(score) for score in test_scores],
        open_at=threshold,
        clear_at=clear,
        limit_c=limit,
        hysteresis_c=threshold_rule_hysteresis_c(),
        rule_delay_s=threshold_rule_delay_s(),
        step_s=args.step_s,
    )
    metrics = {
        **ranking,
        **events.as_metrics("test_event_"),
        "test_precision_at_threshold": at_cut["precision"],
        "test_recall_at_threshold": at_cut["recall"],
        "test_f1_at_threshold": at_cut["f1"],
        "decision_threshold": float(threshold),
        "clear_threshold": float(clear),
        "clear_band": float(band),
        "val_calm_steady_clear_floor": float(min_clear),
        **calm_metrics,
        "calm_check_problems": float(len(calm_problems)),
        "embargo_s": float(embargo),
        "val_pr_auc": float(average_precision_score(y_val, val_scores)),
        "best_iteration": float(model.best_iteration_ or LGBM_PARAMS["n_estimators"]),
        "hours_since_maintenance_split_gain": _feature_gain(model, "hours_since_maintenance"),
        "rows_train": float(len(fit_rows)),
        "rows_validation": float(len(val_rows)),
        "rows_test": float(len(test_rows)),
        "positive_rate_train": float(y_fit.mean()),
        "positive_rate_test": float(y_test.mean()),
        **{f"test_{key}": float(value) for key, value in lead.items()},
    }
    return {
        "model": model,
        "metrics": metrics,
        "threshold_rule": rule,
        "x_test": x_test,
        "y_test": y_test,
        "test_scores": test_scores,
        "test_rows": test_rows,
        "x_val": x_val,
        "val_rows": val_rows,
        "val_scores": val_scores,
    }


# ---------------------------------------------------------------------------
# MLflow + Unity Catalog
# ---------------------------------------------------------------------------
def _pip_requirements() -> list[str]:
    from importlib.metadata import version

    return [f"{name}=={version(name)}" for name in PIP_PINNED]


def _experiment_path(args: argparse.Namespace, workspace: Any) -> str:
    if args.experiment:
        path = args.experiment
    else:
        user = workspace.current_user.me().user_name
        path = f"/Users/{user}/pharma-twin/spindle-thermal-risk"
    workspace.workspace.mkdirs(str(Path(path).parent))
    return path


def log_and_register(
    result: dict[str, Any], training_set: Any, args: argparse.Namespace, workspace: Any
) -> dict[str, Any]:
    import mlflow
    import pandas as pd
    from mlflow.models import infer_signature
    from mlflow.tracking import MlflowClient

    from iot_twin.ml.provenance import source_hash
    from iot_twin.ml.spindle_features import FEATURE_NAMES, WINDOW_S, validated_limit_c

    mlflow.set_registry_uri("databricks-uc")
    mlflow.set_experiment(_experiment_path(args, workspace))
    full_name = f"{args.catalog}.{args.schema}.{args.model_name}"
    metrics = result["metrics"]
    model = result["model"]
    example = result["x_test"].head(3)
    # __file__ is not reliable in serverless python tasks; ml/ sits next to src/.
    model_code = str(Path(args.src_root).resolve().parent / "ml" / "spindle_risk_model.py")

    with (
        mlflow.start_run(run_name="spindle-thermal-risk") as run,
        tempfile.TemporaryDirectory() as tmp,
    ):
        booster_path = os.path.join(tmp, "booster.txt")
        model.booster_.save_model(booster_path, num_iteration=model.best_iteration_ or None)
        decision_path = os.path.join(tmp, "decision.json")
        with open(decision_path, "w", encoding="utf-8") as handle:
            json.dump(
                {
                    "decision_threshold": metrics["decision_threshold"],
                    "clear_threshold": metrics["clear_threshold"],
                    "threshold_rule": result["threshold_rule"],
                },
                handle,
            )
        mlflow.log_params(
            {
                **{f"lgbm_{key}": value for key, value in LGBM_PARAMS.items()},
                "data_origin": training_set.data_origin,
                "horizon_s": args.horizon_s,
                "window_s": WINDOW_S,
                "step_s": args.step_s,
                "validated_limit_c": validated_limit_c(),
                "split": (
                    f"time-ordered train/val/test, test_fraction={args.test_fraction}, "
                    f"embargo_s={metrics['embargo_s']}"
                ),
                "clear_hysteresis": args.clear_hysteresis,
                "threshold_rule": result["threshold_rule"],
                "min_precision": args.min_precision,
                "feature_names": ",".join(FEATURE_NAMES),
                "early_stopping_metric": "binary_logloss",
                "maintenance_pinned": str(training_set.data_origin != "synthetic").lower(),
                "code_sha": args.code_sha,
                "code_dirty": args.code_dirty,
                "source_hash": args.source_hash,
                "source_hash_runtime": source_hash(Path(args.src_root).resolve().parent),
            }
        )
        mlflow.log_metrics(metrics)
        mlflow.set_tags({"data_origin": training_set.data_origin, "project": "pharma-twin"})
        output = pd.DataFrame(
            {
                "risk_score": [0.5] * len(example),
                "top_features": ["[]"] * len(example),
                "decision_threshold": [metrics["decision_threshold"]] * len(example),
                "clear_threshold": [metrics["clear_threshold"]] * len(example),
            }
        )
        info = mlflow.pyfunc.log_model(
            name="model",
            python_model=model_code,
            artifacts={"booster": booster_path, "decision": decision_path},
            signature=infer_signature(example, output),
            input_example=example,
            pip_requirements=_pip_requirements(),
            registered_model_name=full_name,
        )
        run_id = run.info.run_id
    version = str(info.registered_model_version)
    client = MlflowClient(registry_uri="databricks-uc")
    client.set_registered_model_alias(full_name, args.challenger_alias, version)
    for key, value in {
        "data_origin": training_set.data_origin,
        "decision_threshold": f"{metrics['decision_threshold']:.2f}",
        "clear_threshold": f"{metrics['clear_threshold']:.2f}",
        "horizon_s": str(args.horizon_s),
        "run_id": run_id,
    }.items():
        client.set_model_version_tag(full_name, version, key, value)
    return {"model_name": full_name, "model_version": version, "run_id": run_id}


def _plan_group(index: int) -> str:
    from iot_twin.scenario_engine.spindle_scenario import spindle_cycle_plan

    plan = spindle_cycle_plan(index)
    return "quiet" if plan.kind == "quiet" else plan.precursor


def fresh_replay(args: argparse.Namespace) -> list[Any]:
    """Fresh replay hours (scorer path, catalog maintenance age) pooled into the gate."""
    from iot_twin.ml.live_format_telemetry import replay_segments
    from iot_twin.ml.spindle_features import (
        threshold_rule_delay_s,
        threshold_rule_hysteresis_c,
        validated_limit_c,
    )

    if args.replay_hours <= 0:
        return []
    return list(
        replay_segments(
            hours=args.replay_hours,
            seed=args.replay_seed,
            start_epoch=args.replay_start_epoch,
            limit_c=validated_limit_c(),
            hysteresis_c=threshold_rule_hysteresis_c(),
            rule_delay_s=threshold_rule_delay_s(),
            rate_hz=args.live_rate_hz,
        )
    )


def _replay_frame(segment: Any) -> Any:
    import pandas as pd

    from iot_twin.ml.spindle_features import FEATURE_NAMES

    return pd.DataFrame(list(segment.features), columns=list(FEATURE_NAMES)).astype(float)


def _with_hours(frame: Any, hours: float | None) -> Any:
    """``frame`` with ``hours_since_maintenance`` set to ``hours`` (unchanged if None)."""
    if hours is None:
        return frame
    return frame.assign(hours_since_maintenance=float(hours))


def _scores(
    predict: Any, result: dict[str, Any], replay: list[Any], hours: float | None = None
) -> dict[str, Any]:
    return {
        "val": [float(s) for s in predict(_with_hours(result["x_val"], hours))],
        "test": [float(s) for s in predict(_with_hours(result["x_test"], hours))],
        "replay": [
            [float(s) for s in predict(_with_hours(_replay_frame(seg), hours))] for seg in replay
        ],
    }


def _periods(
    result: dict[str, Any],
    training_set: Any,
    scores: dict[str, Any],
    open_at: float,
    clear_at: float,
    replay: list[Any],
) -> list[tuple[list[Any], list[Any], float, float]]:
    """(breaches, model alerts, start, end) per held-out period: test runs, then replay hours."""
    from iot_twin.ml.lead_metrics import alert_episodes
    from iot_twin.ml.training_set import period_alerts, run_breaches

    periods = []
    for run_id, (start, end, alerts) in sorted(
        period_alerts(
            result["test_rows"], scores["test"], open_at=open_at, clear_at=clear_at
        ).items()
    ):
        breaches = [b for b in run_breaches(training_set, run_id) if start <= b[0] < end]
        periods.append((breaches, alerts, start, end))
    for segment, segment_scores in zip(replay, scores["replay"], strict=True):
        alerts = alert_episodes(
            list(segment.times), list(segment_scores), open_at=open_at, clear_at=clear_at
        )
        breaches = [b for b in segment.breaches if segment.start <= b[0] < segment.end]
        periods.append((breaches, alerts, segment.start, segment.end))
    return periods


def _records(periods: list[Any]) -> list[Any]:
    from iot_twin.ml.gate_stats import cycle_outcomes

    records = []
    for index, (breaches, alerts, start, end) in enumerate(periods):
        records.extend(
            cycle_outcomes(
                breaches, alerts, start=start, end=end, group_of=_plan_group, period=index
            )
        )
    return records


def invariance(
    result: dict[str, Any],
    training_set: Any,
    predict: Any,
    open_at: float,
    clear_at: float,
    replay: list[Any],
) -> dict[str, float]:
    """Gate quantities re-scored with hours_since_maintenance at each INVARIANCE_HOURS."""
    from iot_twin.ml.evaluation import INVARIANCE_HOURS
    from iot_twin.ml.gate_stats import summarise

    out: dict[str, float] = {}
    discs, alarms = [], []
    for hours in INVARIANCE_HOURS:
        scores = _scores(predict, result, replay, hours)
        stats = summarise(
            _records(_periods(result, training_set, scores, open_at, clear_at, replay))
        )
        discs.append(stats["discrimination_std"])
        alarms.append(stats["false_alarms_per_hour"])
        out[f"invariance_{int(hours)}h_discrimination_std"] = stats["discrimination_std"]
        out[f"invariance_{int(hours)}h_false_alarms_per_hour"] = stats["false_alarms_per_hour"]
        out[f"invariance_{int(hours)}h_warned_30s_share"] = stats["warned_30s_share"]
    out["invariance_discrimination_std_range"] = max(discs) - min(discs)
    out["invariance_false_alarms_per_hour_range"] = max(alarms) - min(alarms)
    return out


def model_metrics(
    result: dict[str, Any],
    training_set: Any,
    args: argparse.Namespace,
    predict: Any,
    open_at: float,
    clear_at: float,
    replay: list[Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Gate metrics of one model at its own cut-off / clear level.

    Held-out test cycles and fresh replay cycles are pooled; the clock window
    and the steady-running floor come from validation; the invariance check
    re-scores the same cycles with the maintenance age changed. Returns
    ``(metrics, null counts and cycle records)``.
    """
    from sklearn.metrics import average_precision_score

    from iot_twin.ml.gate_stats import as_rows, block_bootstrap, bootstrap, group_counts
    from iot_twin.ml.lead_metrics import clock_alerts, combine, event_report
    from iot_twin.ml.spindle_features import threshold_rule_delay_s
    from iot_twin.ml.training_set import (
        choose_clock_window,
        period_event_report,
        steady_clear_floor,
    )

    delay = threshold_rule_delay_s()
    scores = _scores(predict, result, replay)
    val_events = period_event_report(
        training_set,
        result["val_rows"],
        scores["val"],
        open_at=open_at,
        clear_at=clear_at,
        limit_c=_limit(),
        hysteresis_c=_hysteresis(),
        rule_delay_s=delay,
        step_s=args.step_s,
        significance=False,
    )
    window = choose_clock_window(
        training_set,
        result["val_rows"],
        max_duty=val_events.alert_open_fraction,
        rule_delay_s=delay,
    )
    any_window = choose_clock_window(
        training_set, result["val_rows"], max_duty=1.0, rule_delay_s=delay
    )
    periods = _periods(result, training_set, scores, open_at, clear_at, replay)

    def pooled(alerts_of: Any) -> tuple[Any, list[Any]]:
        reports = [
            event_report(
                breaches,
                alerts_of(alerts, start, end),
                start=start,
                end=end,
                rule_delay_s=delay,
                seed=args.seed + index,
            )
            for index, (breaches, alerts, start, end) in enumerate(periods)
        ]
        chosen = [
            (breaches, alerts_of(alerts, start, end), start, end)
            for breaches, alerts, start, end in periods
        ]
        return combine(reports), _records(chosen)

    events, records = pooled(lambda alerts, start, end: alerts)
    clock, clock_records = pooled(
        lambda alerts, start, end: clock_alerts(
            start, end, open_offset_s=window.open_offset_s, duration_s=window.duration_s
        )
    )
    any_clock, any_clock_records = pooled(
        lambda alerts, start, end: clock_alerts(
            start, end, open_offset_s=any_window.open_offset_s, duration_s=any_window.duration_s
        )
    )
    metrics: dict[str, Any] = {
        "decision_threshold": float(open_at),
        "clear_threshold": float(clear_at),
        "steady_clear_floor": steady_clear_floor(training_set, result["val_rows"], scores["val"]),
        "pooled_hours": events.hours,
        "breaches": float(events.breaches),
        "median_lead_s": events.median_lead_s,
        "permutation_p": events.permutation_p,
        "permutation_warned_30s_p95": events.permutation_warned_30s_p95,
        "alert_open_fraction": events.alert_open_fraction,
        "clock_open_offset_s": window.open_offset_s,
        "clock_duration_s": window.duration_s,
        "clock_warned_30s_share": clock.warned_30s_share,
        "clock_permutation_p": clock.permutation_p,
        "clock_any_duty_open_offset_s": any_window.open_offset_s,
        "clock_any_duty_duration_s": any_window.duration_s,
        "clock_any_duty_warned_30s_share": any_clock.warned_30s_share,
        "clock_any_duty_permutation_p": any_clock.permutation_p,
        "test_pr_auc": float(average_precision_score(result["y_test"], scores["test"])),
    }
    blocks = (
        ("", bootstrap(records, seed=args.seed)),
        ("clock_", bootstrap(clock_records, seed=args.seed)),
        ("clock_any_duty_", bootstrap(any_clock_records, seed=args.seed)),
    )
    for prefix, values in blocks:
        for key, (estimate, lo, hi) in values.items():
            if prefix and key in ("cycles", "breaches"):
                continue
            metrics[f"{prefix}{key}"] = estimate
            metrics[f"{prefix}{key}_lo95"] = lo
            metrics[f"{prefix}{key}_hi95"] = hi
    for key, (_, lo, hi) in block_bootstrap(records, seed=args.seed).items():
        metrics[f"{key}_block_lo95"] = lo
        metrics[f"{key}_block_hi95"] = hi
    metrics.update(invariance(result, training_set, predict, open_at, clear_at, replay))
    metrics["cycle_groups"] = group_counts(records)
    nulls = {
        "permutation_counts": list(events.permutation_counts),
        "shift_counts": list(events.chance_counts),
        "cycle_records": as_rows(records),
    }
    return metrics, nulls


def _limit() -> float:
    from iot_twin.ml.spindle_features import validated_limit_c

    return validated_limit_c()


def _hysteresis() -> float:
    from iot_twin.ml.spindle_features import threshold_rule_hysteresis_c

    return threshold_rule_hysteresis_c()


def version_metrics(
    result: dict[str, Any],
    training_set: Any,
    args: argparse.Namespace,
    full_name: str,
    version: str,
    replay: list[Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """A registered version scored on the challenger's validation, test and replay rows."""
    import mlflow

    model = mlflow.pyfunc.load_model(f"models:/{full_name}/{version}")
    levels = model.predict(result["x_test"].head(1))
    return model_metrics(
        result,
        training_set,
        args,
        lambda frame: model.predict(frame)["risk_score"],
        float(levels["decision_threshold"].iloc[0]),
        float(levels["clear_threshold"].iloc[0]),
        replay,
    )


def _alias_version(full_name: str, alias: str) -> str | None:
    from mlflow.exceptions import MlflowException
    from mlflow.tracking import MlflowClient

    try:
        found = MlflowClient(registry_uri="databricks-uc").get_model_version_by_alias(
            full_name, alias
        )
    except MlflowException:
        return None
    return str(found.version)


SIMULATED_ORIGIN = "live_format_simulated"


def decide_promotion(
    result: dict[str, Any],
    training_set: Any,
    args: argparse.Namespace,
    full_name: str,
    new_version: str,
) -> dict[str, Any]:
    """Neutral champion selection over every compared version on the same pooled cycles.

    * Every version (the new one and ``--compare-versions`` plus the current
      champion) is scored on the same held-out test and fresh replay cycles and
      checked with ``qualifies`` (including input invariance).
    * The winner is ``neutral_winner``: the best standardised discrimination
      among qualifying versions; incumbency plays no part. Every other
      qualifying version gets a paired-bootstrap difference against it.
    * ``--promote if_better`` (default) moves champion to the winner;
      ``always`` moves it to the new version if its clear level is above calm
      running; ``never`` reports only. If nothing qualifies, champion moves to
      the best version by the gate metric, tagged ``status=unqualified``.
    * The gate is defined for simulated live-format data only (plan groups,
      replay pooling). Any other ``data_origin`` forces ``--promote never``.
    """
    from iot_twin.ml.evaluation import best_by_gate, neutral_winner, qualifies
    from iot_twin.ml.gate_stats import from_rows, paired_difference

    champion_version = _alias_version(full_name, args.alias)
    if training_set.data_origin != SIMULATED_ORIGIN:
        return {
            "champion_version_before": champion_version,
            "champion_version_after": champion_version,
            "promoted": False,
            "comparison": (
                f"data_origin={training_set.data_origin}: the promotion gate is defined for "
                "simulated live-format data only; --promote never forced"
            ),
            "winner": None,
            "fallback": None,
            "unqualified_champion": None,
            "paired_vs_winner": {},
            "replay": None,
            "versions": {},
            "nulls": {},
        }
    replay = fresh_replay(args)
    model = result["model"]
    metrics = result["metrics"]
    challenger, nulls = model_metrics(
        result,
        training_set,
        args,
        lambda frame: model.predict_proba(frame)[:, 1],
        metrics["decision_threshold"],
        metrics["clear_threshold"],
        replay,
    )
    compared = sorted(
        {v for v in [champion_version, *args.compare_versions.split(",")] if v and v.strip()},
        key=int,
    )
    table: dict[str, dict[str, Any]] = {}
    all_nulls = {new_version: nulls}
    for version in compared:
        values, version_nulls = version_metrics(
            result, training_set, args, full_name, version, replay
        )
        ok, problems = qualifies(values)
        table[version] = {"metrics": values, "qualifies": ok, "problems": problems}
        all_nulls[version] = version_nulls
    ok, problems = qualifies(challenger)
    table[new_version] = {"metrics": challenger, "qualifies": ok, "problems": problems}
    flat = {v: row["metrics"] for v, row in table.items()}
    winner = neutral_winner(flat)
    paired: dict[str, list[float]] = {}
    if winner:
        for version, row in table.items():
            if version != winner and row["qualifies"]:
                paired[version] = list(
                    paired_difference(
                        from_rows(all_nulls[version]["cycle_records"]),
                        from_rows(all_nulls[winner]["cycle_records"]),
                        seed=args.seed,
                    )
                )
    unqualified = None
    if args.promote == "never":
        champion_after = champion_version
    elif args.promote == "always":
        floor_ok = challenger["clear_threshold"] >= challenger["steady_clear_floor"]
        champion_after = new_version if floor_ok else champion_version
    elif winner:
        champion_after = winner
    else:
        champion_after = best_by_gate(flat)
        unqualified = champion_after
    reason = (
        (f"neutral winner v{winner}" if winner else "no version qualifies")
        + "; paired vs winner: "
        + ", ".join(
            f"v{v} {d[0]:+.3f} [{d[1]:+.3f}, {d[2]:+.3f}]" for v, d in sorted(paired.items())
        )
    )
    return {
        "champion_version_before": champion_version,
        "champion_version_after": champion_after,
        "promoted": champion_after == new_version and champion_after != champion_version,
        "comparison": reason,
        "winner": winner,
        "fallback": None,
        "unqualified_champion": unqualified,
        "paired_vs_winner": paired,
        "replay": {
            "hours": args.replay_hours,
            "seed": args.replay_seed,
            "start_epoch": args.replay_start_epoch,
        },
        "versions": table,
        "nulls": all_nulls,
    }


def ensure_schema(catalog: str, schema: str) -> None:
    """Create the model schema if it is missing (needs CREATE SCHEMA on the catalog)."""
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_quoted(f'{catalog}.{schema}')}")


def _wait_until_idle(workspace: Any, name: str, sleep: Any = None) -> bool:
    """Wait while a config update is in progress; False if it never settles."""
    import time

    pause = sleep or time.sleep
    waited = 0
    while waited <= ENDPOINT_WAIT_S:
        state = workspace.serving_endpoints.get(name).state
        update = getattr(getattr(state, "config_update", None), "value", None)
        if update != "IN_PROGRESS":
            return True
        logger.info("endpoint %s is updating; waiting %ss", name, ENDPOINT_POLL_S)
        pause(ENDPOINT_POLL_S)
        waited += ENDPOINT_POLL_S
    return False


def deploy_champion(
    workspace: Any, args: argparse.Namespace, model_name: str, version: str, sleep: Any = None
) -> str:
    """Point the serving endpoint at the new champion version (no wait for READY).

    A ``bundle deploy`` resets the endpoint to ``var.risk_model_version``; the
    job logs the override that keeps it on the champion.
    """
    if not args.endpoint_name:
        return "skipped"
    from databricks.sdk.errors import NotFound
    from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

    entity = ServedEntityInput(
        entity_name=model_name,
        entity_version=version,
        workload_size="Small",
        scale_to_zero_enabled=True,
    )
    try:
        workspace.serving_endpoints.get(args.endpoint_name)
    except NotFound:
        if str(args.create_endpoint).lower() != "true":
            return "missing"
        workspace.serving_endpoints.create(
            name=args.endpoint_name,
            config=EndpointCoreConfigInput(name=args.endpoint_name, served_entities=[entity]),
        )
        return "creating"
    if not _wait_until_idle(workspace, args.endpoint_name, sleep):
        logger.warning("endpoint %s still updating; not repointed", args.endpoint_name)
        return "busy"
    workspace.serving_endpoints.update_config(name=args.endpoint_name, served_entities=[entity])
    logger.warning(
        "endpoint %s now serves version %s. A later `bundle deploy` resets it to "
        "var.risk_model_version: deploy with --var risk_model_version=%s",
        args.endpoint_name,
        version,
        version,
    )
    return "updating"


def selection_outcome(version: str, decision: dict[str, Any]) -> str:
    """``winner`` / ``tied_with_winner`` / ``worse_than_winner`` / ``not_qualified``."""
    row = decision["versions"][version]
    if not row["qualifies"]:
        return "not_qualified"
    if version == decision["winner"]:
        return "winner"
    _, lo, hi = decision["paired_vs_winner"][version]
    return "worse_than_winner" if hi < 0.0 else "tied_with_winner"


def _tag_outcomes(model_name: str, run_id: str, decision: dict[str, Any]) -> None:
    from mlflow.tracking import MlflowClient

    client = MlflowClient(registry_uri="databricks-uc")
    for version, row in decision["versions"].items():
        outcome = selection_outcome(version, decision)
        paired = decision["paired_vs_winner"].get(version)
        detail = (
            f"run {run_id}: {outcome}; discrimination_std "
            f"{row['metrics']['discrimination_std']:.3f} "
            f"[{row['metrics']['discrimination_std_lo95']:.3f}, "
            f"{row['metrics']['discrimination_std_hi95']:.3f}]"
        )
        if paired:
            detail += f"; vs winner {paired[0]:+.3f} [{paired[1]:+.3f}, {paired[2]:+.3f}]"
        if row["problems"]:
            detail += "; " + "; ".join(row["problems"])
        client.set_model_version_tag(model_name, version, "selection_outcome", outcome)
        client.set_model_version_tag(model_name, version, "selection_detail", detail[:4900])


def _log_decision(run_id: str, decision: dict[str, Any]) -> None:
    import mlflow

    values: dict[str, float] = {}
    for version, row in decision["versions"].items():
        values.update(
            {
                f"v{version}_{k}": float(v)
                for k, v in row["metrics"].items()
                if isinstance(v, (int, float))
            }
        )
        values[f"v{version}_qualifies"] = float(row["qualifies"])
    values["promoted"] = float(decision["promoted"])
    items = sorted(values.items())
    with mlflow.start_run(run_id=run_id):
        for start in range(0, len(items), 500):  # log_metrics batches are capped
            mlflow.log_metrics(dict(items[start : start + 500]))
        mlflow.log_dict(decision["nulls"], "null_counts.json")
        mlflow.log_dict(
            {k: v for k, v in decision.items() if k != "nulls"}, "promotion_decision.json"
        )
        mlflow.set_tags(
            {
                "comparison": decision["comparison"][:5000],
                "champion_version_before": str(decision["champion_version_before"]),
                "champion_version_after": str(decision["champion_version_after"]),
            }
        )


def main(argv: list[str] | None = None) -> dict[str, Any]:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = _args(list(sys.argv[1:] if argv is None else argv))
    _import_shared(args.src_root)
    from databricks.sdk import WorkspaceClient

    workspace = WorkspaceClient()
    ensure_schema(args.catalog, args.schema)
    training_set = load_training_set(args)
    logger.info(
        "training set: %d rows, %d positive, origin=%s",
        len(training_set.rows),
        training_set.positives,
        training_set.data_origin,
    )
    result = train_and_evaluate(training_set, args)
    registered = log_and_register(result, training_set, args, workspace)
    decision = decide_promotion(
        result, training_set, args, registered["model_name"], registered["model_version"]
    )
    _log_decision(registered["run_id"], decision)
    endpoint = "unchanged"
    after = decision["champion_version_after"]
    if after and after != decision["champion_version_before"]:
        from mlflow.tracking import MlflowClient

        MlflowClient(registry_uri="databricks-uc").set_registered_model_alias(
            registered["model_name"], args.alias, after
        )
        endpoint = deploy_champion(workspace, args, registered["model_name"], after)
    _tag_outcomes(registered["model_name"], registered["run_id"], decision)
    if decision["unqualified_champion"]:
        from mlflow.tracking import MlflowClient

        row = decision["versions"][decision["unqualified_champion"]]
        client = MlflowClient(registry_uri="databricks-uc")
        client.set_model_version_tag(
            registered["model_name"], decision["unqualified_champion"], "status", "unqualified"
        )
        client.set_model_version_tag(
            registered["model_name"],
            decision["unqualified_champion"],
            "unqualified_reason",
            "; ".join(row["problems"])[:4000],
        )
    summary = {
        **registered,
        **{k: v for k, v in decision.items() if k not in ("versions", "nulls")},
        "alias": args.alias if decision["promoted"] else args.challenger_alias,
        "versions": {
            v: {"qualifies": row["qualifies"], "problems": row["problems"], **row["metrics"]}
            for v, row in decision["versions"].items()
        },
        "data_origin": training_set.data_origin,
        "endpoint": args.endpoint_name or None,
        "endpoint_action": endpoint,
        "bundle_override": f"--var risk_model_version={after}" if after else None,
        "metrics": result["metrics"],
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return summary


if __name__ == "__main__":
    main()
