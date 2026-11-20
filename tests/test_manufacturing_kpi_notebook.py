"""Tests for usecases/manufacturing/manufacturing_kpis.ipynb.

* structure: valid nbformat 4, unique cell ids, self-contained copies of the shared helpers;
* KPI formula helpers (OEE = A x P x Q, MTBA, stop time, drift, counter check) against small
  hand-computed fixtures, executed straight from the notebook cells;
* a fresh offline run in sample mode renders every KPI section and its data-source note;
* graceful 'not deployed' / 'not applicable' states;
* the *committed* notebook (a redacted live run) keeps readable outputs and passes the
  identifier scan.

Execution tests need the dev extras (``nbformat``, ``nbclient``, ``ipykernel``).
"""

from __future__ import annotations

import importlib.util
import json
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
NOTEBOOK = ROOT / "usecases" / "manufacturing" / "manufacturing_kpis.ipynb"
OPS_NOTEBOOK = ROOT / "ops" / "notebooks" / "pipeline_health_check.ipynb"
GENIE_CONTENT = ROOT / "src" / "iot_twin" / "services" / "genie" / "genie_space_content.py"
SECTIONS = (
    "Pipeline at a glance",
    "1. Overall Equipment Effectiveness (OEE)",
    "2. Downtime and alarms",
    "3. Sensor health trends",
    "4. Throughput per hour versus target",
    "Summary",
)
KPI_CELLS = ("glance", "oee", "alarms", "sensors", "throughput")
FORMULA_CELLS = ("params", "config", "redaction", "sql-guard", "render", "formulas")
RICH_TYPES = ("display_data", "execute_result")
SECRET_PATTERNS = (
    r"eyJ[A-Za-z0-9_-]{8,}\.",
    r"\bdapi[a-z0-9]{16,}",
    r"(?i)password\s*[=:]\s*(?!<redacted>)\S",
    r"(?i)\barn:aws[a-z-]*:",
    r"(?i)\bs3a?://",
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
    r"[\w.+-]+@[\w-]+\.[\w.-]+",
    r"https?://(?!<host>)[\w.-]+\.(?:com|net|io)",
    r"(?<![\d,.])\d{10,}(?![\d,]|\.\d)",
)


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _text(value) -> str:
    return "".join(value) if isinstance(value, list) else str(value or "")


@pytest.fixture(scope="module")
def committed():
    return nbformat.read(NOTEBOOK, as_version=4)


@pytest.fixture(scope="module")
def checker():
    return _load("check_identifiers", ROOT / "scripts" / "check_identifiers.py")


def _cell(notebook, cell_id: str) -> str:
    return next(c.source for c in notebook.cells if c.get("id") == cell_id)


def _namespace(notebook, cell_ids, monkeypatch) -> dict:
    """Execute selected notebook cells (in order) into one module namespace."""
    for key in [k for k in os.environ if k.startswith("KPI_")]:
        monkeypatch.delenv(key)
    monkeypatch.setenv("KPI_SAMPLE", "true")
    module = types.ModuleType("mfg_kpi_notebook")  # dataclasses resolve via sys.modules
    monkeypatch.setitem(sys.modules, module.__name__, module)
    namespace = module.__dict__
    for cell_id in cell_ids:
        exec(compile(_cell(notebook, cell_id), f"<cell:{cell_id}>", "exec"), namespace)
    return namespace


def _execute(overrides: dict[str, str] | None = None):
    """Fresh-kernel run of the notebook in sample mode (offline)."""
    pytest.importorskip("nbclient")
    pytest.importorskip("ipykernel")
    from nbclient import NotebookClient

    notebook = nbformat.read(NOTEBOOK, as_version=4)
    previous = dict(os.environ)
    env = {k: v for k, v in os.environ.items() if not k.startswith("KPI_")}
    env.update(
        {"KPI_SAMPLE": "true", **{f"KPI_{k.upper()}": v for k, v in (overrides or {}).items()}}
    )
    os.environ.clear()
    os.environ.update(env)
    try:
        with tempfile.TemporaryDirectory() as workdir:
            NotebookClient(
                notebook,
                timeout=300,
                kernel_name="python3",
                resources={"metadata": {"path": workdir}},
            ).execute()
    finally:
        os.environ.clear()
        os.environ.update(previous)
    return notebook


def _rich(notebook) -> list[dict]:
    return [
        o for c in notebook.cells for o in c.get("outputs", []) if o["output_type"] in RICH_TYPES
    ]


def _plain(notebook, cell_id: str | None = None) -> str:
    cells = [c for c in notebook.cells if cell_id is None or c.get("id") == cell_id]
    return "\n".join(
        _text(o["data"].get("text/plain"))
        for c in cells
        for o in c.get("outputs", [])
        if o["output_type"] in RICH_TYPES
    )


# ── structure ────────────────────────────────────────────────────────────────
def test_notebook_is_valid_and_self_contained(committed):
    nbformat.validate(committed)
    assert committed.metadata["kernelspec"]["name"] == "python3"
    ids = [c.id for c in committed.cells]
    assert len(ids) == len(set(ids))
    assert set(FORMULA_CELLS + KPI_CELLS + ("queries", "live-backend", "sample-backend")) <= set(
        ids
    )
    for cell in committed.cells:
        if cell.cell_type == "code":
            assert not re.search(r"^\s*[%!]", cell.source, re.M), f"magic in {cell.id}"
            assert "import iot_twin" not in cell.source and "from iot_twin" not in cell.source


def test_shared_helpers_are_verbatim_copies(committed):
    source = (ROOT / "ops" / "html_fallback.py").read_text(encoding="utf-8")
    assert source[source.index("_BLOCK_TAGS = ") :].rstrip() in _cell(committed, "render")
    ops = nbformat.read(OPS_NOTEBOOK, as_version=4)
    for cell_id in ("redaction", "sql-guard"):
        assert _cell(committed, cell_id) == _cell(ops, cell_id), cell_id


def test_every_query_is_read_only(committed, monkeypatch):
    ns = _namespace(committed, (*FORMULA_CELLS, "queries"), monkeypatch)
    for _target, sql in ns["LAKEBASE_SQL"].values():
        ns["assert_read_only"](sql)
    fields = {"table": "`c`.`s`.`t`", "row_filter": ns["CDC_ROW_FILTER"] + " AND ", "hours": 24}
    for name, sql in ns["UC_SQL"].items():
        ns["assert_read_only"](sql.format(mode="realtime", **fields)), name


def test_kpis_reuse_the_app_oee_definition(committed, monkeypatch):
    """The SQL carries the same factor expressions as the app's Genie OEE example."""
    ns = _namespace(committed, (*FORMULA_CELLS, "queries"), monkeypatch)
    genie = GENIE_CONTENT.read_text("utf-8")
    oee_sql = ns["UC_SQL"]["oee"]
    assert "least(value / nominal_value, 1)" in oee_sql.lower()
    assert "LEAST(value / nominal_value, 1)" in genie
    assert "WHEN value BETWEEN min_value AND max_value THEN 1.0 ELSE 0.0 END" in oee_sql
    assert "WHEN value BETWEEN min_value AND max_value THEN 1.0 ELSE 0.0 END" in genie


def test_plant_model_matches_the_domain_catalog(committed, monkeypatch):
    ns = _namespace(committed, (*FORMULA_CELLS,), monkeypatch)
    catalog = pytest.importorskip("iot_twin.domain.catalog")
    rules = {(r.sensor_id.split("-sns-")[1], r.operator, r.threshold, r.severity, r.fault_code)
             for r in catalog.list_alert_rules("manufacturing")}  # fmt: skip
    mirrored = {(r.sensor_name, r.operator, r.threshold, r.severity, r.fault_code)
                for r in ns["ALERT_RULES"]}  # fmt: skip
    assert mirrored == rules
    for sensor in catalog.list_sensors("manufacturing"):
        asset = next(
            a for a in catalog.list_assets("manufacturing") if a.asset_id == sensor.asset_id
        )
        assert ns["asset_of"](sensor.name) == asset.name, sensor.name


# ── KPI formula helpers (hand-computed fixtures) ─────────────────────────────
@pytest.fixture
def f(committed, monkeypatch):
    return _namespace(committed, FORMULA_CELLS, monkeypatch)


def test_oee_is_availability_times_performance_times_quality(f):
    # 60 observed minutes, 6 fault minutes -> A = 0.9; P = 0.8; Q = 0.95 -> OEE = 0.684
    row = {"observed_min": 60, "fault_min": 6, "performance": 0.8, "quality": 0.95}
    out = f["oee_factors"](row)
    assert out["availability"] == pytest.approx(0.9)
    assert out["oee"] == pytest.approx(0.9 * 0.8 * 0.95)
    assert f["oee"](0.5, None, None) == 0.5  # missing P/Q count as 1 (app's COALESCE)
    assert f["oee"](None, 0.9, 0.9) is None
    assert f["availability"](0, 0) is None
    assert f["availability"](10, None) == 1.0


def test_mtba_equals_mean_gap_between_openings(f):
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    stamps = [t0, t0 + timedelta(minutes=10), t0 + timedelta(minutes=40)]
    # gaps 10 and 30 minutes -> mean 20 minutes = 1200 s
    assert f["mean_gap_seconds"](stamps) == 1200
    assert f["mtba_seconds"](3, stamps[0], stamps[-1]) == 1200
    assert f["mtba_seconds"](1, t0, t0) is None
    assert f["mtba_seconds"](0, None, None) is None


def test_stopped_seconds_counts_only_stopped_intervals(f):
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    end = t0 + timedelta(hours=2)
    events = [
        {"state": "stopped", "created_at": t0 + timedelta(minutes=30)},
        {"state": "running", "created_at": t0 + timedelta(minutes=50)},
        {"state": "stopped", "created_at": t0 + timedelta(minutes=110)},
    ]
    # 20 min + the last 10 min stopped = 30 min
    assert f["stopped_seconds"](events, t0, end, "running") == 30 * 60
    # already stopped at the window start, never resumed -> whole window
    assert f["stopped_seconds"]([], t0, end, "stopped") == 2 * 3600


def test_drift_percentile_and_weighted_mean(f):
    assert f["drift_pct"](52.5, 50.0, 50.0) == pytest.approx(5.0)
    assert f["drift_pct"](1.0, 2.0, 0) is None
    assert f["percentile"]([1, 2, 3, 4, 5], 0.5) == 3
    assert f["percentile"]([10, 20], 0.95) == pytest.approx(19.5)
    assert f["percentile"]([], 0.5) is None
    assert f["weighted_mean"]([(10, 1), (20, 3), (None, 5)]) == pytest.approx(17.5)


def test_counter_check_rejects_fluctuating_levels(f):
    hours = [datetime(2026, 1, 1, h, tzinfo=UTC) for h in range(3)]
    counting = [
        {"hour": hours[0], "first_value": 100, "last_value": 160},
        {"hour": hours[1], "first_value": 161, "last_value": 220},
        {"hour": hours[2], "first_value": 221, "last_value": 300},
    ]
    assert f["counter_production"](counting) == (True, 200)
    noisy = [
        {"hour": hours[0], "first_value": 123400, "last_value": 123500},
        {"hour": hours[1], "first_value": 123480, "last_value": 123410},
    ]
    assert f["counter_production"](noisy) == (False, None)


def test_asset_mapping_and_threshold_breach(f):
    assert f["asset_of"]("cnc_1_spindle_temp") == "CNC-1"
    assert f["asset_of"]("assy_line_1_speed") == "Assembly line 1"
    assert f["asset_of"]("hvac_zone_1_temp") == "Facility (HVAC)"
    assert f["asset_of_sensor_id"]("mfg-sns-press_1_pressure") == "Hydraulic press 1"
    assert f["breach"]("cnc_1_spindle_temp", 61.0) is True
    assert f["breach"]("press_1_pressure", 119.0) is True
    assert f["breach"]("press_1_pressure", 121.0) is False
    assert f["breach"]("cnc_1_coolant_flow", 99.0) is None


# ── offline sample run ───────────────────────────────────────────────────────
@pytest.fixture(scope="module")
def executed():
    return _execute()


def test_sample_run_renders_every_section_and_data_source(executed):
    text = _plain(executed)
    for title in SECTIONS:
        assert f"## {title}" in text, title
    assert "SAMPLE DATA" in text
    for cell_id in KPI_CELLS:
        assert "#### Data source" in _plain(executed, cell_id), cell_id
        assert "| time window | last 24 h" in _plain(executed, cell_id), cell_id
    assert "Production counts (units per hour): not available from this pipeline" in text
    assert "Plant OEE" in text and "Mean time between alerts" in text
    errors = [o for c in executed.cells for o in c.get("outputs", []) if o.output_type == "error"]
    assert not errors
    streams = [o for c in executed.cells for o in c.get("outputs", []) if o.output_type == "stream"]
    assert not streams, [o.text[:200] for o in streams]


def test_sample_outputs_are_readable_without_html(executed):
    outputs = _rich(executed)
    assert len(outputs) >= 15
    for output in outputs:
        data = output["data"]
        assert {"text/html", "text/markdown", "text/plain"} <= set(data), sorted(data)
        assert len(_text(data["text/plain"]).strip()) >= 8
        assert "<IPython.core.display." not in _text(data["text/plain"])
    charts = [o for o in outputs if "image/svg+xml" in o["data"]]
    assert len(charts) >= 7
    for chart in charts:
        assert _text(chart["data"]["text/plain"]).startswith("[chart] ")


@pytest.mark.parametrize("mode", ["microbatch", "zerobus"])
def test_other_modes_degrade_gracefully(mode):
    notebook = _execute({"mode": mode})
    text = _plain(notebook)
    assert "NOT APPLICABLE" in text and "has no Lakebase hot tier" in text
    if mode == "zerobus":
        assert "no Spark stream" in text
    assert not [o for c in notebook.cells for o in c.get("outputs", []) if o.output_type == "error"]


def test_not_deployed_pipeline_shows_clear_state(committed, monkeypatch):
    cells = (*FORMULA_CELLS, "queries", "live-backend", "sample-backend")
    ns = _namespace(committed, cells, monkeypatch)
    ns["install_output_guards"] = lambda: None  # keep pytest's stdout untouched
    header = _cell(committed, "header").split("BACKEND, PIPE = connect()")[0]
    exec(compile(header, "<cell:header>", "exec"), ns)
    shown: list[str] = []
    pipe = ns["Pipeline"](reason="the manufacturing setup has no microbatch deployment")
    ns.update(emit_html=shown.append, PIPE=pipe, BACKEND=None, SUMMARY=[])
    ns["DATA"] = ns["Data"](None, pipe)
    for cell_id in KPI_CELLS:
        source = _cell(committed, cell_id).rstrip()
        body, call = source.rsplit("\n", 1)
        exec(compile(body, f"<cell:{cell_id}>", "exec"), ns)
        ns[call.strip().removesuffix("()")]()
    html = "".join(shown)
    assert html.count("not available from this pipeline") >= 4
    assert "no microbatch deployment" in html


# ── committed notebook (redacted live run) ───────────────────────────────────
def test_committed_notebook_is_a_labelled_live_run(committed):
    first = committed.cells[0]
    assert first.cell_type == "markdown"
    assert re.fullmatch(r"\*\*LIVE RUN, redacted, \d{4}-\d{2}-\d{2}\*\*", first.source)
    code = [c for c in committed.cells if c.cell_type == "code"]
    assert all(o.output_type != "error" for c in code for o in c.outputs)
    text = _plain(committed)
    for title in SECTIONS:
        assert f"## {title}" in text, title
    assert text.count("#### Data source") >= len(KPI_CELLS)
    assert "SAMPLE DATA" not in text
    assert "| redaction | on |" in text


def test_committed_outputs_are_readable_and_clean(committed, checker):
    outputs = _rich(committed)
    assert len(outputs) >= 10
    for output in outputs:
        data = output["data"]
        plain = _text(data.get("text/plain"))
        assert len(plain.strip()) >= 8, sorted(data)
        assert "<IPython.core.display." not in plain
        assert not (plain.strip().startswith("<") and " object" in plain[:60])
        if "text/html" in data:
            assert "text/markdown" in data
    assert not checker.scan_file(NOTEBOOK, str(NOTEBOOK)), "identifier scan must pass"
    blob = json.dumps(outputs)
    for pattern in SECRET_PATTERNS:
        assert not re.search(pattern, blob), pattern


def test_exporter_replaces_an_existing_label(tmp_path):
    exporter = _load("export_run_notebook", ROOT / "ops" / "export_run_notebook.py")
    source = nbformat.v4.new_notebook()
    source.cells = [
        nbformat.v4.new_markdown_cell("**LIVE RUN, redacted, 2026-01-01**", id="live-run-label"),
        nbformat.v4.new_code_cell("x = 1", id="code"),
    ]
    model = {"commands": [{"position": i, "state": "finished", "results": None} for i in range(2)]}
    notebook = exporter.build_notebook(
        model, _write(tmp_path, source), "LIVE RUN, redacted, 2026-02-02"
    )
    assert [c.source for c in notebook.cells][0] == "**LIVE RUN, redacted, 2026-02-02**"
    assert len(notebook.cells) == 2


def _write(tmp_path: Path, notebook) -> Path:
    path = tmp_path / "source.ipynb"
    nbformat.write(notebook, path)
    return path
