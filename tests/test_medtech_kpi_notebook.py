"""Tests for usecases/medtech/medtech_kpis.ipynb (medical-device executive + operations KPIs).

* structure: valid nbformat 4, unique cell ids, shared helpers copied from the manufacturing
  notebook, pharma-twin Lakebase defaults (never the upstream iot-* projects);
* formulas: the value model, episode pairing, time-to-detect matching, and parity of the lot
  clock / projected right-first-time with ``iot_twin.domain.lot_context`` (one definition);
* a fresh offline run in sample mode renders both persona sections and the placeholders;
* the committed notebook keeps readable outputs and passes the identifier scan.
"""

from __future__ import annotations

import importlib.util
import os
import re
import sys
import tempfile
import types
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

nbformat = pytest.importorskip("nbformat")

ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK = ROOT / "usecases" / "medtech" / "medtech_kpis.ipynb"
TEMPLATE = ROOT / "usecases" / "manufacturing" / "manufacturing_kpis.ipynb"
SHARED_CELLS = ("redaction", "sql-guard", "render")
FORMULA_CELLS = ("params", "config", "redaction", "sql-guard", "render", "formulas")
SECTIONS = (
    "Pipeline at a glance",
    "EXECUTIVE: cost of poor quality and lot exposure",
    "OPERATIONS: detect, respond, recover",
    "MODEL RISK SCORE: Spindle 3 thermal-excursion risk",
    "AI ACTION PLANS: grounded, advisory, audited",
    "Summary",
)
RICH_TYPES = ("display_data", "execute_result")
SECRET_PATTERNS = (
    r"eyJ[A-Za-z0-9_-]{8,}\.",
    r"\bdapi[a-z0-9]{16,}",
    r"(?i)\barn:aws[a-z-]*:",
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
    r"[\w.+-]+@[\w-]+\.[\w.-]+",
    r"https?://(?!<host>)[\w.-]+\.(?:com|net|io)",
)


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def committed():
    return nbformat.read(NOTEBOOK, as_version=4)


def _cell(notebook, cell_id: str) -> str:
    return next(c.source for c in notebook.cells if c.get("id") == cell_id)


@pytest.fixture()
def ns(committed, monkeypatch):
    """Execute the formula cells (in order) into one module namespace, offline."""
    for key in [k for k in os.environ if k.startswith("MEDTECH_")]:
        monkeypatch.delenv(key)
    monkeypatch.setenv("MEDTECH_SAMPLE", "true")
    module = types.ModuleType("medtech_kpi_notebook")
    monkeypatch.setitem(sys.modules, module.__name__, module)
    namespace = module.__dict__
    for cell_id in FORMULA_CELLS:
        exec(compile(_cell(committed, cell_id), f"<cell:{cell_id}>", "exec"), namespace)
    return namespace


# ── structure ────────────────────────────────────────────────────────────────
def test_notebook_is_valid_with_unique_ids(committed):
    nbformat.validate(committed)
    ids = [c.id for c in committed.cells]
    assert len(ids) == len(set(ids))
    assert {"params", "queries", "live-backend", "sample-backend", "executive", "operations",
            "model-risk", "ai-plans", "summary"} <= set(ids)  # fmt: skip


def test_shared_helpers_match_the_manufacturing_template(committed):
    template = nbformat.read(TEMPLATE, as_version=4)
    for cell_id in SHARED_CELLS:
        assert _cell(committed, cell_id) == _cell(template, cell_id), cell_id


def test_lakebase_projects_default_to_this_fork_and_are_explicit(committed, ns):
    assert ns["PARAMS"].app_backend_project == "pharma-twin-backend"
    assert ns["PARAMS"].pipelines_project == "pharma-twin-pipelines"
    live = _cell(committed, "live-backend")
    flat = re.sub(r"\s+", " ", live)
    assert "self.find_project( PARAMS.app_backend_project," in flat or (
        "self.find_project(PARAMS.app_backend_project," in flat
    )
    assert "explicit or PARAMS.pipelines_project" in flat
    source = "\n".join(c.source for c in committed.cells if c.cell_type == "code")
    isolation = _load(
        "resource_isolation_for_medtech", ROOT / "tests" / "test_resource_isolation.py"
    )
    assert not isolation.LEGACY_PATTERNS.search(source)


def test_no_first_pass_yield_naming(committed):
    text = "\n".join(c.source for c in committed.cells).lower()
    assert "projected right-first-time" in text
    assert "first-pass yield" not in text and "fpy" not in text


# ── formulas ─────────────────────────────────────────────────────────────────
def test_copq_value_model(ns):
    assert ns["copq_avoided"](10, 0.8, 24_000, 1_500) == pytest.approx(180_000)
    assert ns["copq_avoided"](0, 0.8, 24_000, 1_500) == 0
    assert ns["copq_avoided"](5, 1.0, 1_000, 2_000) == 0  # never negative
    assert ns["exposure_remaining"](3, 24_000) == 72_000


def test_lot_clock_and_projected_rft_match_the_app(ns):
    lot_context = _load(
        "lot_context_for_test", ROOT / "src" / "iot_twin" / "domain" / "lot_context.py"
    )
    start = datetime(2026, 11, 20, 14, 2, 3, tzinfo=UTC)
    for offset in (0, 37, 299, 1234, 86_399):
        at = start + timedelta(seconds=offset)
        assert ns["lot_id_at"](at.timestamp()) == lot_context.lot_id_at(at)
    end = start + timedelta(seconds=95)
    for now in (end, end + timedelta(seconds=400)):
        mine = ns["projected_rft"](start, end, now)
        theirs = lot_context.projected_rft(start, end, now)
        assert mine["parts_started"] == theirs["parts_started"]
        assert mine["parts_at_risk"] == theirs["parts_at_risk"]
        assert mine["good"] == theirs["good"]
    assert ns["PROJECTED_RFT_DEFINITION"] == lot_context.PROJECTED_RFT_DEFINITION
    expected = [(p["lot_id"], p["serial"]) for p in lot_context.parts_in_window(start, end)]
    assert ns["parts_in_window"](start, end) == expected


def test_episodes_pair_open_ack_clear_per_reopen(ns):
    t0 = datetime(2026, 11, 20, 15, 0, tzinfo=UTC)

    def ev(kind: str, s: int) -> dict:
        return {"alert_id": "a", "event_type": kind, "created_at": t0 + timedelta(seconds=s),
                "rule_id": ns["SPINDLE_RULE_ID"]}  # fmt: skip

    episodes = ns["build_episodes"]([ev("opened", 0), ev("acknowledged", 30), ev("cleared", 80),
                                     ev("opened", 300), ev("cleared", 370)])  # fmt: skip
    assert len(episodes) == 2
    assert ns["seconds_between"](episodes[0]["opened_at"], episodes[0]["acknowledged_at"]) == 30
    assert episodes[1]["acknowledged_at"] is None
    assert ns["seconds_between"](episodes[1]["opened_at"], episodes[1]["cleared_at"]) == 70


def test_time_to_detect_uses_the_latest_onset_inside_the_window(ns):
    t0 = datetime(2026, 11, 20, 15, 0, tzinfo=UTC)
    episodes = [{"opened_at": t0 + timedelta(seconds=3)}, {"opened_at": t0 + timedelta(hours=1)}]
    onsets = [t0 - timedelta(minutes=30), t0]
    assert ns["match_onsets"](episodes, onsets) == [3.0, None]


def test_latency_histogram_buckets(ns):
    hist = ns["latency_histogram"]([10, 30, 60, 6000], (0, 25, 50, 5000))
    assert [count for _, count in hist] == [1, 1, 1, 1]


# ── execution (offline sample mode) ──────────────────────────────────────────
def _execute():
    pytest.importorskip("nbclient")
    pytest.importorskip("ipykernel")
    from nbclient import NotebookClient

    notebook = nbformat.read(NOTEBOOK, as_version=4)
    previous = dict(os.environ)
    env = {k: v for k, v in os.environ.items() if not k.startswith("MEDTECH_")}
    env["MEDTECH_SAMPLE"] = "true"
    os.environ.clear()
    os.environ.update(env)
    try:
        with tempfile.TemporaryDirectory() as workdir:
            NotebookClient(notebook, timeout=300, kernel_name="python3",
                           resources={"metadata": {"path": workdir}}).execute()  # fmt: skip
    finally:
        os.environ.clear()
        os.environ.update(previous)
    return notebook


def _plain(notebook) -> str:
    return "\n".join(
        "".join(o["data"].get("text/plain", ""))
        for c in notebook.cells
        for o in c.get("outputs", [])
        if o["output_type"] in RICH_TYPES
    )


def test_sample_run_renders_both_personas_and_placeholders():
    notebook = _execute()
    errors = [
        o for c in notebook.cells for o in c.get("outputs", []) if o["output_type"] == "error"
    ]
    assert not errors
    text = _plain(notebook)
    for section in SECTIONS:
        assert section in text, section
    for kpi in ("Cost of poor quality avoided", "Lots quarantined", "Exposure remaining",
                "Time to detect", "Time to acknowledge", "NCRs / CMM inspections",
                "Projected right-first-time", "MTTR", "Streaming latency distribution",
                "Predicted alerts", "Breaches warned >= 30 s ahead", "Scoring lag (median)",
                "Plans generated", "Grounding check passed", "Applied by a person"):  # fmt: skip
        assert kpi in text, kpi
    assert "SAMPLE DATA" in text


# ── committed outputs ────────────────────────────────────────────────────────
def test_committed_outputs_are_readable_and_redacted(committed):
    outputs = [
        o for c in committed.cells for o in c.get("outputs", []) if o["output_type"] in RICH_TYPES
    ]
    assert outputs, "the committed notebook must keep its outputs"
    for o in outputs:
        assert {"text/html", "text/plain"} <= set(o["data"]), "every output needs a text fallback"
    blob = "\n".join(str(o["data"]) for o in outputs)
    for pattern in SECRET_PATTERNS:
        assert not re.search(pattern, blob), pattern
    checker = _load("check_identifiers_medtech", ROOT / "scripts" / "check_identifiers.py")
    assert checker.main([str(NOTEBOOK)]) == 0


def test_lead_times_count_only_predictions_at_or_before_the_breach(ns, committed):
    helpers = _cell(committed, "model-risk").split("\ndef model_risk_section", 1)[0]
    exec(compile(helpers, "<cell:model-risk>", "exec"), ns)
    t0 = datetime(2026, 11, 20, 15, 0, tzinfo=UTC)
    predicted = [
        {"opened_at": t0 - timedelta(seconds=45)},
        {"opened_at": t0 + timedelta(seconds=300)},
    ]
    breaches = [{"opened_at": t0}, {"opened_at": t0 + timedelta(seconds=301)},
                {"opened_at": t0 + timedelta(hours=2)}]  # fmt: skip
    assert ns["lead_times"](predicted, breaches) == [45.0, 1.0, None]
    # One long-open prediction is credited to the first breach only, not to the next one.
    long_open = [{"opened_at": t0 - timedelta(seconds=1)}]
    two = [{"opened_at": t0}, {"opened_at": t0 + timedelta(seconds=300)}]
    assert ns["lead_times"](long_open, two) == [1.0, None]


def test_ai_plans_query_never_reads_the_requester(committed):
    queries = _cell(committed, "queries")
    sql = queries.split('"ai_plans": (', 1)[1].split("),\n", 1)[0]
    assert "genai_action_plan_audit" in sql
    assert "requested_by" not in sql


def test_event_queries_keep_the_newest_rows_under_their_limit(committed):
    """[regression] 'updated' events and oldest-first ordering truncated newer episodes."""
    queries = _cell(committed, "queries")
    alert_events = queries.split('"alert_events": (', 1)[1].split("),\n", 1)[0]
    assert "event_type IN ('opened', 'acknowledged', 'cleared')" in alert_events
    assert "ORDER BY e.created_at DESC" in alert_events
    risk_events = queries.split('"risk_events": (', 1)[1].split("),\n", 1)[0]
    assert "ORDER BY e.created_at DESC" in risk_events


def test_use_case_notebook_stays_runnable(committed):
    """The exporter adds a snapshot guard for stored ops runs; this notebook must not keep it."""
    assert committed.cells[0].source.startswith("**LIVE RUN, redacted, ")
    assert all(c.get("id") != "snapshot-guard" for c in committed.cells)
