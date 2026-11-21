"""Snapshot DDL / rows carry the model output; the bundle wires job, endpoint and app."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from iot_twin.ml.risk_contract import (
    DEFAULT_MODEL_SCHEMA,
    DEFAULT_MODEL_SHORT_NAME,
    MODEL_RULE_ID,
)
from iot_twin.services import intelligence_catalog_service as ics

ROOT = Path(__file__).resolve().parents[1]
CATALOG = "example-catalog"
SCHEMA = "pharma_twin_test_schema"


@pytest.fixture(autouse=True)
def _uc_env(monkeypatch):
    monkeypatch.setenv("UC_CATALOG", CATALOG)
    monkeypatch.setenv("UC_SCHEMA", SCHEMA)


PREDICTED = {
    "id": "a1",
    "industry": "manufacturing",
    "rule_id": MODEL_RULE_ID,
    "sensor_id": "mfg-sns-cnc_1_spindle_temp",
    "status": "open",
    "severity": "warn",
    "fault_code": "PRED-SPINDLE3-THERMAL",
    "value": 0.81,
    "threshold": 0.6,
    "opened_at": "2026-11-20T12:00:00+00:00",
    "updated_at": "2026-11-20T12:00:05+00:00",
    "metadata": {
        "alert_kind": "predicted",
        "risk_score": 0.8123,
        "model_name": "cat.pharma_twin.spindle_thermal_risk",
        "model_version": "3",
        "top_features": [{"feature": "temp_slope", "contribution": 1.2, "value": 9.5}],
        "lot_id": "TM-HS12-261120-001",
    },
}


class TestSnapshotDdl:
    def test_ddl_has_the_model_columns(self) -> None:
        ddl = ics._TABLE_DDL["intelligence_alert_snapshot"]
        for column, sql_type in (
            ("alert_kind", "STRING"),
            ("risk_score", "DOUBLE"),
            ("model_name", "STRING"),
            ("model_version", "STRING"),
            ("top_features", "STRING"),
            ("metadata", "STRING"),
        ):
            assert f"{column} {sql_type}" in ddl
        # The original contract columns keep their order at the front.
        assert ics.ALERT_SNAPSHOT_COLUMN_LIST.startswith(
            "alert_id, industry, rule_id, sensor_id, status, severity, fault_code, value, "
            "threshold, opened_at, updated_at"
        )

    def test_rows_match_the_column_list(self) -> None:
        [row] = ics._alert_snapshot_rows([PREDICTED])
        assert row.count("'") % 2 == 0
        assert "'predicted', 0.8123, 'cat.pharma_twin.spindle_thermal_risk', '3'" in row
        assert '"feature": "temp_slope"' in row
        assert "TM-HS12-261120-001" in row

    def test_threshold_alert_row_is_breached_with_null_score(self) -> None:
        [row] = ics._alert_snapshot_rows(
            [{**PREDICTED, "rule_id": "mfg-alert-spindle-temp", "metadata": {}}]
        )
        assert "'breached', NULL, NULL, NULL, NULL, NULL)" in row

    def test_quotes_in_metadata_are_escaped(self) -> None:
        alert = {**PREDICTED, "metadata": {**PREDICTED["metadata"], "note": "it's hot"}}
        [row] = ics._alert_snapshot_rows([alert])
        # Databricks SQL string literals escape quotes (and backslashes) with a backslash.
        assert "it\\'s hot" in row
        assert "it''s hot" not in row

    @pytest.mark.asyncio
    async def test_provision_migrates_an_old_snapshot_table(self) -> None:
        executed: list[str] = []
        old_columns = [
            {"col_name": name, "data_type": t}
            for name, t in ics.ALERT_SNAPSHOT_COLUMNS
            if name not in ics.MODEL_SNAPSHOT_COLUMNS
        ]

        async def fake_sql(sql: str, warehouse_id: str) -> dict:
            executed.append(sql)
            if sql.startswith("DESCRIBE TABLE"):
                return {"columns": [], "data": old_columns, "row_count": len(old_columns)}
            return {"columns": [], "data": [], "row_count": 0}

        with patch.object(ics, "execute_dbsql", side_effect=fake_sql):
            await ics.provision_reference_tables(warehouse_id="wh-1")
        [alter] = [sql for sql in executed if sql.startswith("ALTER TABLE")]
        assert "intelligence_alert_snapshot" in alter
        assert "ADD COLUMNS (alert_kind STRING, risk_score DOUBLE" in alter
        assert "metadata STRING)" in alter

    @pytest.mark.asyncio
    async def test_provision_leaves_a_current_table_alone(self) -> None:
        executed: list[str] = []
        columns = [{"col_name": name} for name, _ in ics.ALERT_SNAPSHOT_COLUMNS]

        async def fake_sql(sql: str, warehouse_id: str) -> dict:
            executed.append(sql)
            data = columns if sql.startswith("DESCRIBE") else []
            return {"columns": [], "data": data, "row_count": len(data)}

        with patch.object(ics, "execute_dbsql", side_effect=fake_sql):
            await ics.provision_reference_tables(warehouse_id="wh-1")
        assert not [sql for sql in executed if sql.startswith("ALTER TABLE")]

    @pytest.mark.asyncio
    async def test_background_sync_writes_the_model_columns(self) -> None:
        from iot_twin.services.alerts import alert_snapshot_sync as sync

        executed: list[str] = []

        async def fake_sql(sql: str, warehouse_id: str) -> dict:
            executed.append(sql)
            return {"columns": [], "data": [], "row_count": 0}

        with patch.object(ics, "execute_dbsql", side_effect=fake_sql):
            await sync.sync_alert_snapshot(alerts=[PREDICTED], warehouse_id="wh-1")
        [insert] = executed
        assert f"({ics.ALERT_SNAPSHOT_COLUMN_LIST})" in insert
        assert "'predicted', 0.8123" in insert


def _yaml(path: str) -> dict:
    return yaml.safe_load((ROOT / path).read_text(encoding="utf-8"))


class TestBundle:
    def test_training_job_is_serverless_python_with_the_shared_src(self) -> None:
        job = _yaml("resources/risk_model.yml")["resources"]["jobs"]["risk_model_train"]
        assert job["name"] == "${var.risk_job_name}"
        [task] = job["tasks"]
        assert "new_cluster" not in task and "existing_cluster_id" not in task
        assert "job_clusters" not in job
        assert task["spark_python_task"]["python_file"] == "../ml/train_spindle_risk.py"
        params = task["spark_python_task"]["parameters"]
        assert params[params.index("--src-root") + 1] == "${workspace.file_path}/src"
        assert params[params.index("--alias") + 1] == "champion"
        assert (ROOT / "ml" / "train_spindle_risk.py").is_file()
        [env] = job["environments"]
        assert env["environment_key"] == task["environment_key"]
        deps = env["spec"]["dependencies"]
        wheel = next(d for d in deps if d.endswith(".whl"))
        assert (ROOT / "resources" / wheel).resolve().is_file()

    def test_endpoint_is_small_scale_to_zero_on_the_uc_model(self) -> None:
        endpoint = _yaml("resources/risk_model.yml")["resources"]["model_serving_endpoints"][
            "spindle_risk"
        ]
        assert endpoint["name"] == "${var.risk_endpoint_name}"
        [entity] = endpoint["config"]["served_entities"]
        assert entity["workload_size"] == "Small"
        assert entity["scale_to_zero_enabled"] is True
        assert entity["entity_name"] == (
            "${var.uc_catalog}.${var.risk_model_schema}.${var.risk_model_short_name}"
        )

    def test_names_default_to_the_resource_prefix(self) -> None:
        variables = _yaml("databricks.yml")["variables"]
        assert variables["risk_endpoint_name"]["default"] == "pharma-twin-spindle-risk"
        assert variables["risk_job_name"]["default"] == "pharma-twin-risk-model-train"
        assert variables["risk_model_schema"]["default"] == DEFAULT_MODEL_SCHEMA
        assert variables["risk_model_short_name"]["default"] == DEFAULT_MODEL_SHORT_NAME

    def test_app_queries_the_endpoint_through_a_resource(self) -> None:
        app = _yaml("resources/app.yml")["resources"]["apps"]["pharma_twin"]
        resource = next(r for r in app["resources"] if "serving_endpoint" in r)
        assert resource["serving_endpoint"] == {
            "name": "${resources.model_serving_endpoints.spindle_risk.name}",
            "permission": "CAN_QUERY",
        }
        env = {e["name"]: e for e in app["config"]["env"]}
        assert env["RISK_ENDPOINT_NAME"] == {
            "name": "RISK_ENDPOINT_NAME",
            "value_from": resource["name"],
        }
        assert "RISK_MODEL_NAME" in env and "RISK_ALERT_THRESHOLD" in env

    def test_submit_script_reuses_the_job_environment(self) -> None:
        import importlib.util

        spec = importlib.util.spec_from_file_location(
            "submit_risk", ROOT / "scripts" / "submit_risk_model_training.py"
        )
        assert spec and spec.loader
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        client, deps = module.job_environment("/Workspace/x/sim.whl")
        assert client == "4"
        assert "/Workspace/x/sim.whl" in deps and not [d for d in deps if d.startswith("..")]
        uploaded = {p.relative_to(ROOT).as_posix() for p in module.files_to_upload()}
        assert "ml/train_spindle_risk.py" in uploaded
        assert "src/iot_twin/ml/spindle_features.py" in uploaded
        assert not [p for p in uploaded if p.startswith("src/iot_twin/services/")]


class TestDocsContract:
    def test_integration_contract_documents_every_metadata_key(self) -> None:
        from iot_twin.ml.risk_contract import RISK_METADATA_KEYS

        doc = (ROOT / "docs" / "ml-risk-model.md").read_text(encoding="utf-8")
        for key in RISK_METADATA_KEYS:
            assert re.search(rf"`{key}`", doc), key
        assert MODEL_RULE_ID in doc

    def test_model_card_documents_every_feature(self) -> None:
        from iot_twin.ml.spindle_features import FEATURE_NAMES

        doc = (ROOT / "docs" / "ml-risk-model.md").read_text(encoding="utf-8")
        assert [name for name in FEATURE_NAMES if f"`{name}`" not in doc] == []


@pytest.mark.skipif(
    os.getenv("PHARMA_TWIN_LIVE_TESTS") != "1",
    reason="live endpoint test: set PHARMA_TWIN_LIVE_TESTS=1 and a CLI profile",
)
def test_live_endpoint_returns_a_risk_score() -> None:  # pragma: no cover - live only
    from databricks.sdk import WorkspaceClient

    from iot_twin.ml.spindle_features import FEATURE_NAMES
    from iot_twin.services.ml.risk_scorer import parse_prediction

    endpoint = os.getenv("RISK_ENDPOINT_NAME", "pharma-twin-spindle-risk")
    record = {name: 0.0 for name in FEATURE_NAMES} | {"temp_last": 50.0, "temp_margin_c": 10.0}
    response = WorkspaceClient().serving_endpoints.query(name=endpoint, dataframe_records=[record])
    score, top = parse_prediction(response.predictions)
    assert 0.0 <= score <= 1.0
    assert json.dumps(list(top))
