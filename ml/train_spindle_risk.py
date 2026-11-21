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
5. ``--promote if_better`` (default) scores the current ``champion`` on the
   same held-out test rows and moves ``champion`` (and the serving endpoint)
   only when the new version beats it (``challenger_beats_champion``).
   ``--promote always`` moves it unconditionally; ``never`` never does.
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
    frame = spark.sql(
        f"SELECT sensor_name, value, event_time FROM {table} "  # noqa: S608 - identifiers from job params
        f"WHERE sensor_name IN ({names}) "
        f"AND event_time >= current_timestamp() - INTERVAL {int(days)} DAYS "
        f"ORDER BY event_time LIMIT {REAL_MAX_ROWS}"
    )
    return [row.asDict() for row in frame.collect()]


def _quoted(table: str) -> str:
    return ".".join(f"`{part.strip('`')}`" for part in table.split("."))


def load_training_set(args: argparse.Namespace) -> Any:
    from iot_twin.ml.spindle_features import (
        excursion_onsets,
        last_spindle_maintenance_epoch,
        threshold_rule_hysteresis_c,
        validated_limit_c,
    )
    from iot_twin.ml.synthetic_telemetry import DATA_ORIGIN, generate_runs
    from iot_twin.ml.training_set import RunInput, build_training_set

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
        if readings:
            real = build_training_set(
                [RunInput("real", readings, last_spindle_maintenance_epoch())],
                data_origin="real",
                **common,
            )
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
        eval_metric="average_precision",
        callbacks=[lgb.early_stopping(40, verbose=False)],
    )
    val_scores = model.predict_proba(x_val)[:, 1]
    band = args.clear_hysteresis
    # Steady-running p90: the clear level must not sit below it (blocking at promotion).
    min_clear = steady_clear_floor(training_set, val_rows, [float(s) for s in val_scores])
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


def model_metrics(
    result: dict[str, Any],
    training_set: Any,
    args: argparse.Namespace,
    val_scores: list[float],
    test_scores: list[float],
    open_at: float,
    clear_at: float,
) -> tuple[dict[str, float], dict[str, list[int]]]:
    """Qualification and comparison metrics of one model at its own cut-off / clear level.

    Everything is measured on the challenger's held-out test rows; the clock
    baseline and the steady-running floor are chosen on its validation rows.
    Returns ``(metrics, null counts)``.
    """
    from sklearn.metrics import average_precision_score

    from iot_twin.ml.spindle_features import (
        threshold_rule_delay_s,
        threshold_rule_hysteresis_c,
        validated_limit_c,
    )
    from iot_twin.ml.training_set import (
        choose_clock_window,
        period_clock_report,
        period_event_report,
        steady_clear_floor,
    )

    common = {
        "limit_c": validated_limit_c(),
        "hysteresis_c": threshold_rule_hysteresis_c(),
        "rule_delay_s": threshold_rule_delay_s(),
        "step_s": args.step_s,
    }
    val_events = period_event_report(
        training_set,
        result["val_rows"],
        val_scores,
        open_at=open_at,
        clear_at=clear_at,
        significance=False,
        **common,
    )
    window = choose_clock_window(
        training_set,
        result["val_rows"],
        max_duty=val_events.alert_open_fraction,
        rule_delay_s=common["rule_delay_s"],
        step_s=args.step_s,
    )
    clock = period_clock_report(
        training_set, result["test_rows"], window, rule_delay_s=common["rule_delay_s"]
    )
    any_window = choose_clock_window(
        training_set,
        result["val_rows"],
        max_duty=1.0,
        rule_delay_s=common["rule_delay_s"],
        step_s=args.step_s,
    )
    any_clock = period_clock_report(
        training_set, result["test_rows"], any_window, rule_delay_s=common["rule_delay_s"]
    )
    events = period_event_report(
        training_set, result["test_rows"], test_scores, open_at=open_at, clear_at=clear_at, **common
    )
    metrics = {
        "decision_threshold": float(open_at),
        "clear_threshold": float(clear_at),
        "steady_clear_floor": steady_clear_floor(training_set, result["val_rows"], val_scores),
        "warned_30s_share": events.warned_30s_share,
        "median_lead_s": events.median_lead_s,
        "false_alarms_per_hour": events.false_alarms_per_hour,
        "false_alarm_cycle_share": events.false_alarm_cycle_share,
        "discrimination": events.discrimination,
        "permutation_p": events.permutation_p,
        "permutation_warned_30s_p95": events.permutation_warned_30s_p95,
        "shift_warned_30s_mean": events.chance_warned_30s_mean,
        "alert_open_fraction": events.alert_open_fraction,
        "clock_open_offset_s": window.open_offset_s,
        "clock_duration_s": window.duration_s,
        "clock_warned_30s_share": clock.warned_30s_share,
        "clock_false_alarms_per_hour": clock.false_alarms_per_hour,
        "clock_discrimination": clock.discrimination,
        "clock_any_duty_open_offset_s": any_window.open_offset_s,
        "clock_any_duty_duration_s": any_window.duration_s,
        "clock_any_duty_warned_30s_share": any_clock.warned_30s_share,
        "clock_any_duty_false_alarms_per_hour": any_clock.false_alarms_per_hour,
        "clock_any_duty_discrimination": any_clock.discrimination,
        "pr_auc": float(average_precision_score(result["y_test"], test_scores)),
        "breaches": float(events.breaches),
    }
    nulls = {
        "permutation_counts": list(events.permutation_counts),
        "shift_counts": list(events.chance_counts),
    }
    return metrics, nulls


def version_metrics(
    result: dict[str, Any],
    training_set: Any,
    args: argparse.Namespace,
    full_name: str,
    version: str,
) -> tuple[dict[str, float], dict[str, list[int]]]:
    """A registered version scored on the challenger's validation and test rows."""
    import mlflow

    model = mlflow.pyfunc.load_model(f"models:/{full_name}/{version}")
    val_out = model.predict(result["x_val"])
    test_out = model.predict(result["x_test"])
    return model_metrics(
        result,
        training_set,
        args,
        [float(score) for score in val_out["risk_score"]],
        [float(score) for score in test_out["risk_score"]],
        float(test_out["decision_threshold"].iloc[0]),
        float(test_out["clear_threshold"].iloc[0]),
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


def decide_promotion(
    result: dict[str, Any],
    training_set: Any,
    args: argparse.Namespace,
    full_name: str,
    new_version: str,
) -> dict[str, Any]:
    """Qualify every compared version on the same held-out rows and decide ``champion``.

    * the challenger becomes champion when it passes ``challenger_beats_champion``
      (``--promote if_better``) or, with ``--promote always``, when its clear level
      is at or above steady running (that check is never skipped);
    * otherwise, if the current champion fails ``qualifies``, champion falls back
      to the newest compared version that qualifies (none: unchanged, flagged).
    """
    from iot_twin.ml.evaluation import challenger_beats_champion, qualifies

    metrics = result["metrics"]
    challenger, nulls = model_metrics(
        result,
        training_set,
        args,
        [float(s) for s in result["val_scores"]],
        [float(s) for s in result["test_scores"]],
        metrics["decision_threshold"],
        metrics["clear_threshold"],
    )
    champion_version = _alias_version(full_name, args.alias)
    compared = sorted(
        {v for v in [champion_version, *args.compare_versions.split(",")] if v and v.strip()},
        key=int,
    )
    table: dict[str, dict[str, Any]] = {}
    all_nulls = {new_version: nulls}
    for version in compared:
        values, version_nulls = version_metrics(result, training_set, args, full_name, version)
        ok, problems = qualifies(values)
        table[version] = {"metrics": values, "qualifies": ok, "problems": problems}
        all_nulls[version] = version_nulls
    ok, problems = qualifies(challenger)
    table[new_version] = {"metrics": challenger, "qualifies": ok, "problems": problems}
    champion_values = table[champion_version]["metrics"] if champion_version else None
    beats, reason = challenger_beats_champion(challenger, champion_values)
    floor_ok = challenger["clear_threshold"] >= challenger["steady_clear_floor"]
    promote = (args.promote == "if_better" and beats) or (args.promote == "always" and floor_ok)
    champion_after = new_version if promote else champion_version
    fallback = None
    if (
        not promote
        and args.promote != "never"
        and champion_version
        and not table[champion_version]["qualifies"]
    ):
        passing = [v for v in reversed(compared) if table[v]["qualifies"]]
        fallback = passing[0] if passing else "none"
        champion_after = passing[0] if passing else champion_version
    return {
        "champion_version_before": champion_version,
        "champion_version_after": champion_after,
        "challenger_beats_champion": beats,
        "comparison": reason,
        "promoted": promote,
        "fallback": fallback,
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


def _log_decision(run_id: str, decision: dict[str, Any]) -> None:
    import mlflow

    values: dict[str, float] = {}
    for version, row in decision["versions"].items():
        values.update({f"v{version}_{k}": float(v) for k, v in row["metrics"].items()})
        values[f"v{version}_qualifies"] = float(row["qualifies"])
    values["challenger_beats_champion"] = float(decision["challenger_beats_champion"])
    values["promoted"] = float(decision["promoted"])
    with mlflow.start_run(run_id=run_id):
        mlflow.log_metrics(values)
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
