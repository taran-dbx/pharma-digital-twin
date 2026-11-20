#!/usr/bin/env python3
"""Turn a Databricks job-run export into an ipynb that keeps the cell outputs.

``databricks jobs export-run <task_run_id> --views-to-export ALL -o json`` returns the executed
notebook as an HTML page that embeds the notebook model (base64 + URL-encoded JSON). This
script reads that JSON, copies the rendered outputs onto the *source* notebook's cells and
writes a valid nbformat 4 file whose first cell is labelled ``LIVE RUN, redacted, <date>``.

Databricks ``displayHTML`` outputs carry only ``text/html``; each one gets ``text/markdown`` and
``text/plain`` versions derived from the HTML (tables as pipe tables, badges as words, charts
as a one-line summary plus ``image/svg+xml``) so the snapshot is readable in viewers that do not
render HTML. An IPython object repr (``<IPython.core.display.HTML object>``) is never kept.

Usage::

    python ops/export_run_notebook.py export.json OUT.ipynb --date 2026-11-19

Cells that did not finish (``state != "finished"``) make the script exit non-zero: a partial
run must not be stored as a snapshot.
"""

from __future__ import annotations

import argparse
import base64
import json
import re
import sys
import urllib.parse
from pathlib import Path
from typing import Any

import nbformat

sys.path.insert(0, str(Path(__file__).resolve().parent))
from html_fallback import mime_bundle  # noqa: E402  (sibling module, not a package)

SOURCE_NOTEBOOK = Path(__file__).resolve().parent / "notebooks" / "pipeline_health_check.ipynb"
_MODEL_RE = re.compile(r"__DATABRICKS_NOTEBOOK_MODEL\s*=\s*'([^']+)'")
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
_OBJECT_REPR = re.compile(r"^<[\w.]+ object(?: at 0x[0-9a-f]+)?>$")


class IncompleteRunError(RuntimeError):
    """The exported run has cells that errored or were skipped."""


def load_model(export_json: Path) -> dict[str, Any]:
    """Decode the embedded notebook model from a ``jobs export-run`` JSON file."""
    views = json.loads(export_json.read_text(encoding="utf-8"))["views"]
    html = next(v["content"] for v in views if v.get("type") == "NOTEBOOK")
    match = _MODEL_RE.search(html)
    if match is None:
        raise ValueError("no embedded notebook model found in the export")
    return json.loads(urllib.parse.unquote(base64.b64decode(match.group(1)).decode("utf-8")))


def _as_text(value: Any) -> str:
    return "".join(value) if isinstance(value, list) else str(value)


def with_text_fallbacks(data: dict[str, Any]) -> dict[str, Any]:
    """Add Markdown/plain-text versions to an HTML bundle; drop useless object reprs."""
    plain = data.get("text/plain")
    if plain is not None and _OBJECT_REPR.match(_as_text(plain).strip()):
        data = {k: v for k, v in data.items() if k != "text/plain"}
    if "text/html" not in data:
        return dict(data)
    return {**data, **mime_bundle(_as_text(data["text/html"]))}


def _outputs(command: dict[str, Any]) -> list[nbformat.NotebookNode]:
    """Map one command's results to nbformat outputs (HTML/text bundles and plain text)."""
    results = command.get("results") or {}
    data = results.get("data")
    if results.get("type") == "listResults":
        items = data or []
    elif results.get("type") in {"html", "text"} and data:
        mime = "text/html" if results["type"] == "html" else "text/plain"
        items = [{"type": "mimeBundle", "data": {mime: data}}]
    else:
        items = []
    return [
        nbformat.v4.new_output("display_data", data=with_text_fallbacks(item["data"]), metadata={})
        for item in items
        if item.get("type") == "mimeBundle" and isinstance(item.get("data"), dict)
    ]


def build_notebook(model: dict[str, Any], source: Path, label: str) -> nbformat.NotebookNode:
    """Source notebook + outputs from ``model`` (cells matched by position)."""
    commands = sorted(model["commands"], key=lambda c: c.get("position", 0))
    bad = [i for i, c in enumerate(commands) if c.get("state") != "finished"]
    if bad:
        raise IncompleteRunError(f"cells not finished: {bad}")
    notebook = nbformat.read(source, as_version=4)
    if len(commands) != len(notebook.cells):
        raise ValueError(f"{len(commands)} executed cells vs {len(notebook.cells)} source cells")
    for cell, command in zip(notebook.cells, commands, strict=True):
        if cell.cell_type == "code":
            cell.outputs = _outputs(command)
            cell.execution_count = None
    banner = nbformat.v4.new_markdown_cell(f"**{label}**", id="live-run-label")
    if notebook.cells and notebook.cells[0].get("id") == banner.id:
        notebook.cells[0] = banner  # source is itself a stored snapshot: replace its label
    else:
        notebook.cells.insert(0, banner)
    nbformat.validate(notebook)
    return notebook


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("export_json", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--date", required=True, help="run date, YYYY-MM-DD")
    parser.add_argument("--source", type=Path, default=SOURCE_NOTEBOOK)
    args = parser.parse_args(argv)
    label = f"LIVE RUN, redacted, {args.date}"
    try:
        notebook = build_notebook(load_model(args.export_json), args.source, label)
    except IncompleteRunError as exc:
        print(f"refusing to write a partial run: {exc}", file=sys.stderr)
        return 1
    args.output.parent.mkdir(parents=True, exist_ok=True)
    nbformat.write(notebook, args.output)
    print(f"wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
