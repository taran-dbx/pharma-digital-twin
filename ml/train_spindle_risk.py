"""Train, evaluate and register the Spindle 3 thermal-risk model (serverless job task).

Run by the ``pharma-twin-risk-model-train`` job (resources/risk_model.yml) as a
``spark_python_task``. Steps:

1. Load labelled rows. ``--data-source auto`` trains on real
   ``manufacturing_realtime`` telemetry when it has enough labelled rows and
   excursions, and otherwise on SYNTHETIC runs from the repo's simulator and
   scenario engine (``iot_twin.ml.synthetic_telemetry``). The choice is logged
   as the ``data_origin`` param and tag.
2. Features and labels come from ``iot_twin.ml.spindle_features``, the same
   code the app's scorer runs, so training and serving cannot drift.
3. Time-based split (train → validation → test, in time order), LightGBM,
   cut-off chosen on validation, metrics on test.
4. Log to MLflow (params, metrics, signature, input example), register in Unity
   Catalog, move the ``champion`` alias, and optionally point the serving
   endpoint at the new version.
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
PIP_PINNED = ("mlflow", "lightgbm", "pandas", "numpy")


def _args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--src-root", required=True, help="Path to the bundle's src/ folder")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", default="pharma_twin")
    parser.add_argument("--model-name", default="spindle_thermal_risk")
    parser.add_argument("--telemetry-table", default="", help="catalog.schema.table or empty")
    parser.add_argument("--data-source", choices=("auto", "synthetic", "real"), default="auto")
    parser.add_argument("--real-days", type=int, default=14)
    parser.add_argument("--horizon-s", type=float, default=120.0)
    parser.add_argument("--step-s", type=float, default=5.0)
    parser.add_argument("--test-fraction", type=float, default=0.25)
    parser.add_argument("--min-precision", type=float, default=0.8)
    parser.add_argument("--synthetic-runs", type=int, default=24)
    parser.add_argument("--synthetic-run-s", type=int, default=7200)
    parser.add_argument("--seed", type=int, default=7)
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

    from iot_twin.ml.evaluation import choose_threshold, precision_recall_at
    from iot_twin.ml.spindle_features import (
        lead_time_report,
        threshold_rule_delay_s,
        threshold_rule_hysteresis_c,
        validated_limit_c,
    )
    from iot_twin.ml.training_set import onsets_in_period, time_split

    train_rows, test_rows = time_split(list(training_set.rows), args.test_fraction)
    fit_rows, val_rows = time_split(train_rows, VALIDATION_FRACTION)
    x_fit, y_fit = _frame(fit_rows)
    x_val, y_val = _frame(val_rows)
    x_test, y_test = _frame(test_rows)

    model = lgb.LGBMClassifier(**LGBM_PARAMS)
    model.fit(
        x_fit,
        y_fit,
        eval_set=[(x_val, y_val)],
        eval_metric="average_precision",
        callbacks=[lgb.early_stopping(40, verbose=False)],
    )
    val_scores = model.predict_proba(x_val)[:, 1]
    threshold, rule = choose_threshold(
        list(y_val), list(val_scores), min_precision=args.min_precision
    )
    test_scores = model.predict_proba(x_test)[:, 1]
    at_cut = precision_recall_at(list(y_test), list(test_scores), threshold)

    limit = validated_limit_c()
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
    metrics = {
        "test_roc_auc": float(roc_auc_score(y_test, test_scores)),
        "test_pr_auc": float(average_precision_score(y_test, test_scores)),
        "test_precision_at_threshold": at_cut["precision"],
        "test_recall_at_threshold": at_cut["recall"],
        "test_f1_at_threshold": at_cut["f1"],
        "decision_threshold": float(threshold),
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
        mlflow.log_params(
            {
                **{f"lgbm_{key}": value for key, value in LGBM_PARAMS.items()},
                "data_origin": training_set.data_origin,
                "horizon_s": args.horizon_s,
                "window_s": WINDOW_S,
                "step_s": args.step_s,
                "validated_limit_c": validated_limit_c(),
                "split": f"time-ordered train/val/test, test_fraction={args.test_fraction}",
                "threshold_rule": result["threshold_rule"],
                "min_precision": args.min_precision,
                "feature_names": ",".join(FEATURE_NAMES),
            }
        )
        mlflow.log_metrics(metrics)
        mlflow.set_tags({"data_origin": training_set.data_origin, "project": "pharma-twin"})
        output = pd.DataFrame(
            {"risk_score": [0.5] * len(example), "top_features": ["[]"] * len(example)}
        )
        info = mlflow.pyfunc.log_model(
            name="model",
            python_model=model_code,
            artifacts={"booster": booster_path},
            signature=infer_signature(example, output),
            input_example=example,
            pip_requirements=_pip_requirements(),
            registered_model_name=full_name,
        )
        run_id = run.info.run_id
    version = str(info.registered_model_version)
    client = MlflowClient(registry_uri="databricks-uc")
    client.set_registered_model_alias(full_name, args.alias, version)
    for key, value in {
        "data_origin": training_set.data_origin,
        "decision_threshold": f"{metrics['decision_threshold']:.2f}",
        "horizon_s": str(args.horizon_s),
        "run_id": run_id,
    }.items():
        client.set_model_version_tag(full_name, version, key, value)
    return {"model_name": full_name, "model_version": version, "run_id": run_id}


def deploy_champion(workspace: Any, args: argparse.Namespace, model_name: str, version: str) -> str:
    """Point the serving endpoint at the new champion version (no wait)."""
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
    workspace.serving_endpoints.update_config(name=args.endpoint_name, served_entities=[entity])
    return "updating"


def main(argv: list[str] | None = None) -> dict[str, Any]:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = _args(list(sys.argv[1:] if argv is None else argv))
    _import_shared(args.src_root)
    from databricks.sdk import WorkspaceClient

    workspace = WorkspaceClient()
    training_set = load_training_set(args)
    logger.info(
        "training set: %d rows, %d positive, origin=%s",
        len(training_set.rows),
        training_set.positives,
        training_set.data_origin,
    )
    result = train_and_evaluate(training_set, args)
    registered = log_and_register(result, training_set, args, workspace)
    endpoint = deploy_champion(
        workspace, args, registered["model_name"], registered["model_version"]
    )
    summary = {
        **registered,
        "alias": args.alias,
        "data_origin": training_set.data_origin,
        "endpoint": args.endpoint_name or None,
        "endpoint_action": endpoint,
        "metrics": result["metrics"],
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return summary


if __name__ == "__main__":
    main()
