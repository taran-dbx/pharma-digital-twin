"""Tests for the dated live-run snapshots in ops/notebooks/runs/ (never re-executed here).

Each ``runs/<industry>_<mode>_<YYYY-MM-DD>.ipynb`` must be valid nbformat 4, start with a
``LIVE RUN, redacted, <date>`` label, keep its outputs, and pass the identifier scan plus
secret-shape checks. Also covers ``ops/export_run_notebook.py`` with a synthetic export.
"""

from __future__ import annotations

import base64
import importlib.util
import json
import re
import sys
import urllib.parse
from pathlib import Path

import pytest

nbformat = pytest.importorskip("nbformat")

ROOT = Path(__file__).resolve().parents[1]
RUNS = sorted((ROOT / "ops" / "notebooks" / "runs").glob("*.ipynb"))
NAME_RE = re.compile(
    r"^(?P<industry>[a-z]+)_(?P<mode>realtime|microbatch|zerobus)_(?P<date>\d{4}-\d{2}-\d{2})\.ipynb$"
)
SECRET_PATTERNS = (
    r"eyJ[A-Za-z0-9_-]{8,}\.",
    r"\bdapi[a-z0-9]{16,}",
    r"(?i)password\s*[=:]\s*(?!<redacted>)\S",
    r"(?i)\barn:aws[a-z-]*:",
    r"(?i)\bs3a?://",
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
    r"[\w.+-]+@[\w-]+\.[\w.-]+",
)


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def checker():
    return _load("check_identifiers", ROOT / "scripts" / "check_identifiers.py")


@pytest.fixture(scope="module")
def exporter():
    return _load("export_run_notebook", ROOT / "ops" / "export_run_notebook.py")


def _outputs_blob(notebook) -> str:
    return json.dumps([o for c in notebook.cells for o in c.get("outputs", [])])


@pytest.mark.skipif(not RUNS, reason="no live-run snapshots committed")
@pytest.mark.parametrize("path", RUNS, ids=lambda p: p.name)
def test_snapshot_is_valid_labelled_and_clean(path, checker):
    match = NAME_RE.match(path.name)
    assert match, "name must be <industry>_<mode>_<YYYY-MM-DD>.ipynb"
    notebook = nbformat.read(path, as_version=4)
    nbformat.validate(notebook)
    assert notebook.nbformat == 4
    first = notebook.cells[0]
    assert first.cell_type == "markdown"
    assert first.source == f"**LIVE RUN, redacted, {match['date']}**"
    code = [c for c in notebook.cells if c.cell_type == "code"]
    assert sum(1 for c in code if c.outputs) >= 8, "outputs must be kept"
    for cell in code:
        assert all(o.output_type != "error" for o in cell.outputs), cell.id
    html = "".join(
        "".join(o["data"].get("text/html", "")) for c in code for o in c.outputs if "data" in o
    )
    assert "Scorecard" in html and "redaction: ON" in html
    assert "SAMPLE DATA" not in html
    assert not checker.scan_file(path, str(path)), "identifier scan must pass"
    blob = _outputs_blob(notebook)
    for pattern in SECRET_PATTERNS:
        assert not re.search(pattern, blob), pattern


def _fake_export(tmp_path: Path, states: list[str]) -> Path:
    """Export JSON shaped like ``jobs export-run`` for a notebook with ``len(states)`` cells."""
    commands = [
        {
            "position": i,
            "state": state,
            "results": {
                "type": "listResults",
                "data": [{"type": "mimeBundle", "data": {"text/html": f"<b>out{i}</b>"}}],
            },
        }
        for i, state in enumerate(states)
    ]
    model = urllib.parse.quote(json.dumps({"commands": commands}))
    page = f"<script>__DATABRICKS_NOTEBOOK_MODEL = '{base64.b64encode(model.encode()).decode()}'"
    path = tmp_path / "export.json"
    path.write_text(json.dumps({"views": [{"type": "NOTEBOOK", "content": page}]}))
    return path


def _source_cell_count(exporter) -> int:
    return len(nbformat.read(exporter.SOURCE_NOTEBOOK, as_version=4).cells)


def test_export_keeps_outputs_and_adds_label(exporter, tmp_path):
    n = _source_cell_count(exporter)
    out = tmp_path / "x_realtime_2026-01-02.ipynb"
    export = _fake_export(tmp_path, ["finished"] * n)
    assert exporter.main([str(export), str(out), "--date", "2026-01-02"]) == 0
    notebook = nbformat.read(out, as_version=4)
    assert notebook.cells[0].source == "**LIVE RUN, redacted, 2026-01-02**"
    code = [c for c in notebook.cells if c.cell_type == "code"]
    assert all(c.outputs for c in code)
    assert code[0].outputs[0]["data"]["text/html"].startswith("<b>out")
    assert code[0].outputs[0]["data"]["text/plain"].startswith("out")
    assert code[0].outputs[0]["data"]["text/markdown"].startswith("out")


def test_export_refuses_partial_runs(exporter, tmp_path, capsys):
    n = _source_cell_count(exporter)
    states = ["finished"] * (n - 1) + ["error"]
    out = tmp_path / "partial.ipynb"
    export = _fake_export(tmp_path, states)
    assert exporter.main([str(export), str(out), "--date", "2026-01-02"]) == 1
    assert not out.exists()
    assert "partial run" in capsys.readouterr().err
