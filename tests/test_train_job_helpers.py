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


class TestSelectionAndRealData:
    def _decision(self):
        return {
            "winner": "11",
            "versions": {
                "11": {"qualifies": True, "problems": [], "metrics": {}},
                "12": {"qualifies": True, "problems": [], "metrics": {}},
                "4": {"qualifies": True, "problems": [], "metrics": {}},
                "7": {"qualifies": False, "problems": ["not invariant"], "metrics": {}},
            },
            "paired_vs_winner": {"12": [-0.03, -0.09, 0.02], "4": [-0.1, -0.17, -0.04]},
        }

    def test_selection_outcomes(self) -> None:
        train = _module("train_spindle_risk", ROOT / "ml" / "train_spindle_risk.py")
        decision = self._decision()
        assert train.selection_outcome("11", decision) == "winner"
        assert train.selection_outcome("12", decision) == "tied_with_winner"
        assert train.selection_outcome("4", decision) == "worse_than_winner"
        assert train.selection_outcome("7", decision) == "not_qualified"

    def test_non_simulated_data_never_promotes(self, monkeypatch) -> None:
        train = _module("train_spindle_risk", ROOT / "ml" / "train_spindle_risk.py")
        monkeypatch.setattr(train, "_alias_version", lambda name, alias: "7")
        monkeypatch.setattr(
            train, "fresh_replay", lambda args: pytest.fail("no simulated replay for real data")
        )
        decision = train.decide_promotion(
            {}, SimpleNamespace(data_origin="real"), SimpleNamespace(alias="champion"), "m", "13"
        )
        assert decision["promoted"] is False
        assert decision["champion_version_after"] == "7"
        assert "--promote never forced" in decision["comparison"]
