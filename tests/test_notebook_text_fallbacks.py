"""Every committed health notebook output must be readable without an HTML renderer.

Covers the sample notebook and the live-run snapshots: no ``<IPython.core.display...>`` reprs,
a non-trivial ``text/plain`` / ``text/markdown`` on every rich output, the identifier check,
and the converter in ``ops/html_fallback.py`` (plus its copy inside the notebook).
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
NOTEBOOKS_DIR = ROOT / "ops" / "notebooks"
SAMPLE = NOTEBOOKS_DIR / "pipeline_health_check.ipynb"
NOTEBOOKS = [SAMPLE, *sorted((NOTEBOOKS_DIR / "runs").glob("*.ipynb"))]
HTML_COPIES = sorted((NOTEBOOKS_DIR / "runs").glob("*.html"))
RICH_TYPES = ("display_data", "execute_result")
MIN_TEXT_CHARS = 8


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def fallback():
    return _load("html_fallback", ROOT / "ops" / "html_fallback.py")


@pytest.fixture(scope="module")
def checker():
    return _load("check_identifiers", ROOT / "scripts" / "check_identifiers.py")


def _text(value) -> str:
    return "".join(value) if isinstance(value, list) else str(value or "")


def _rich_outputs(path: Path) -> list[dict]:
    notebook = json.loads(path.read_text(encoding="utf-8"))
    return [
        output
        for cell in notebook["cells"]
        for output in cell.get("outputs", [])
        if output["output_type"] in RICH_TYPES
    ]


@pytest.mark.parametrize("path", NOTEBOOKS, ids=lambda p: p.name)
def test_no_output_is_an_ipython_object_repr(path):
    for output in _rich_outputs(path):
        for mime in ("text/plain", "text/markdown"):
            text = _text(output["data"].get(mime))
            assert "<IPython.core.display." not in text, f"{path.name}: {text[:80]}"
            assert not (text.strip().startswith("<") and " object" in text[:60]), text[:80]


@pytest.mark.parametrize("path", NOTEBOOKS, ids=lambda p: p.name)
def test_every_rich_output_has_readable_text(path):
    outputs = _rich_outputs(path)
    assert outputs, f"{path.name} has no rich outputs"
    for output in outputs:
        data = output["data"]
        plain, markdown = _text(data.get("text/plain")), _text(data.get("text/markdown"))
        assert len(plain.strip()) >= MIN_TEXT_CHARS or len(markdown.strip()) >= MIN_TEXT_CHARS, (
            f"{path.name}: output without a text fallback: {sorted(data)}"
        )
        if "text/html" in data:
            assert "text/plain" in data and "text/markdown" in data, sorted(data)


@pytest.mark.parametrize("path", NOTEBOOKS, ids=lambda p: p.name)
def test_scorecard_is_readable_as_text(path):
    text = "\n".join(_text(o["data"].get("text/plain")) for o in _rich_outputs(path))
    assert "## Scorecard" in text
    assert "| section | check | status | value | threshold | explanation |" in text
    assert "Overall: " in text
    assert any(f"| {status} |" in text for status in ("PASS", "WARN", "FAIL"))


@pytest.mark.parametrize("path", NOTEBOOKS, ids=lambda p: p.name)
def test_charts_carry_an_svg_image_and_a_summary(path):
    charts = [o for o in _rich_outputs(path) if "image/svg+xml" in o["data"]]
    for output in charts:
        svg = _text(output["data"]["image/svg+xml"])
        assert svg.startswith('<svg xmlns="http://www.w3.org/2000/svg"'), svg[:60]
        assert _text(output["data"]["text/plain"]).startswith("[chart] ")


@pytest.mark.parametrize("path", NOTEBOOKS + HTML_COPIES, ids=lambda p: p.name)
def test_identifier_check_passes(path, checker):
    assert not checker.scan_file(path, str(path))


@pytest.mark.parametrize("path", HTML_COPIES, ids=lambda p: p.name)
def test_static_html_copy_has_no_object_reprs(path):
    html = path.read_text(encoding="utf-8")
    assert "IPython.core.display" not in html
    assert "LIVE RUN, redacted," in html


# ── converter ────────────────────────────────────────────────────────────────
def test_tables_headings_badges_and_placeholders(fallback):
    markup = (
        "<h2 style='m'>Scorecard</h2><div>Overall: <span style='x'>WARN</span></div>"
        "<table><thead><tr><th>check</th><th>status</th></tr></thead><tbody>"
        "<tr><td>a|b</td><td><span>PASS</span></td></tr><tr><td><host></td><td>x</td></tr>"
        "</tbody></table>"
    )
    bundle = fallback.mime_bundle(markup)
    assert bundle["text/plain"] == (
        "## Scorecard\n\nOverall: WARN\n\n| check | status |\n|---|---|\n"
        "| a\\|b | PASS |\n| <host> | x |"
    )
    assert "\\<host\\>" in bundle["text/markdown"]
    assert "&lt;host&gt;" in bundle["text/html"] and "<host>" not in bundle["text/html"]
    assert "image/svg+xml" not in bundle


def test_pure_svg_chart_becomes_image_and_summary(fallback):
    markup = '<svg width="10"><title>Rate</title><desc>3 points</desc><text>9</text></svg>'
    bundle = fallback.mime_bundle(markup)
    assert bundle["text/plain"] == "[chart] Rate: 3 points"
    assert bundle["image/svg+xml"].startswith('<svg xmlns="http://www.w3.org/2000/svg" width')


def test_empty_markup_still_has_text(fallback):
    assert fallback.html_to_text("<div></div>") == "(empty output)"


def test_notebook_carries_an_identical_converter(fallback):
    """The notebook is self-contained, so it holds a verbatim copy of the converter."""
    source = (ROOT / "ops" / "html_fallback.py").read_text(encoding="utf-8")
    converter = source[source.index("_BLOCK_TAGS = ") :].rstrip()
    notebook = json.loads(SAMPLE.read_text(encoding="utf-8"))
    render = next(c for c in notebook["cells"] if c.get("id") == "render")
    assert converter in _text(render["source"])


def test_exporter_replaces_object_reprs(tmp_path):
    pytest.importorskip("nbformat")
    exporter = _load("export_run_notebook", ROOT / "ops" / "export_run_notebook.py")
    data = exporter.with_text_fallbacks(
        {"text/html": "<h4>Jobs</h4>", "text/plain": "<IPython.core.display.HTML object>"}
    )
    assert data["text/plain"] == "#### Jobs"
    assert data["text/markdown"] == "#### Jobs"
    assert exporter.with_text_fallbacks({"text/plain": "hello"}) == {"text/plain": "hello"}
