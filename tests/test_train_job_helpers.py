"""Training-job helpers that need no Spark / MLflow / LightGBM (offline)."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def train():
    return _module("train_spindle_risk", ROOT / "ml" / "train_spindle_risk.py")


class FakeEndpoints:
    def __init__(self, updates: list[str] | None = None, missing: bool = False) -> None:
        self.updates = list(updates or ["NOT_UPDATING"])
        self.missing = missing
        self.updated: list[dict] = []
        self.created: list[str] = []

    def get(self, name: str):
        if self.missing:
            from databricks.sdk.errors import NotFound

            raise NotFound("missing")
        state = self.updates.pop(0) if len(self.updates) > 1 else self.updates[0]
        return SimpleNamespace(state=SimpleNamespace(config_update=SimpleNamespace(value=state)))

    def update_config(self, name: str, served_entities) -> None:
        self.updated.append({"name": name, "version": served_entities[0].entity_version})

    def create(self, name: str, config) -> None:
        self.created.append(name)


def _args(train, **overrides):
    args = train._args(["--src-root", "src", "--catalog", "cat"])
    for key, value in overrides.items():
        setattr(args, key, value)
    return args


def test_deploy_waits_for_an_in_progress_update(train) -> None:
    # The first get() is the existence check; then two polls see IN_PROGRESS.
    endpoints = FakeEndpoints(["IN_PROGRESS", "IN_PROGRESS", "IN_PROGRESS", "NOT_UPDATING"])
    sleeps: list[float] = []
    workspace = SimpleNamespace(serving_endpoints=endpoints)
    args = _args(train, endpoint_name="pharma-twin-spindle-risk")
    action = train.deploy_champion(workspace, args, "cat.pharma_twin.m", "4", sleeps.append)
    assert action == "updating"
    assert endpoints.updated == [{"name": "pharma-twin-spindle-risk", "version": "4"}]
    assert sleeps == [train.ENDPOINT_POLL_S, train.ENDPOINT_POLL_S]


def test_deploy_gives_up_when_the_endpoint_never_settles(train) -> None:
    endpoints = FakeEndpoints(["IN_PROGRESS"])
    workspace = SimpleNamespace(serving_endpoints=endpoints)
    args = _args(train, endpoint_name="pharma-twin-spindle-risk")
    assert train.deploy_champion(workspace, args, "m", "4", lambda s: None) == "busy"
    assert endpoints.updated == []


def test_deploy_creates_only_when_asked(train) -> None:
    workspace = SimpleNamespace(serving_endpoints=FakeEndpoints(missing=True))
    args = _args(train, endpoint_name="pharma-twin-spindle-risk")
    assert train.deploy_champion(workspace, args, "m", "1") == "missing"
    args.create_endpoint = "true"
    assert train.deploy_champion(workspace, args, "m", "1") == "creating"
    assert train.deploy_champion(workspace, _args(train), "m", "1") == "skipped"


def test_quoted_table_and_pyfunc_output_contract(train) -> None:
    assert train._quoted("a-b.pharma_twin.t") == "`a-b`.`pharma_twin`.`t`"
    source = (ROOT / "ml" / "spindle_risk_model.py").read_text(encoding="utf-8")
    assert (
        'OUTPUT_COLUMNS = ["risk_score", "top_features", "decision_threshold", "clear_threshold"]'
        in source
    )
    assert 'context.artifacts["decision"]' in source


def test_job_defaults_match_the_contract(train) -> None:
    from iot_twin.ml.risk_contract import DEFAULT_HYSTERESIS

    args = _args(train)
    assert args.clear_hysteresis == DEFAULT_HYSTERESIS
    assert json.dumps(train.LGBM_PARAMS)
