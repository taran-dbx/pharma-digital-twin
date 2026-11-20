#!/usr/bin/env python3
"""Execute the pipeline health-check notebook in sample mode and save its outputs in place.

Usage::

    python ops/refresh_notebook_outputs.py                      # refresh the committed outputs
    python ops/refresh_notebook_outputs.py --check              # execute only; non-zero exit on error

The run is offline: ``HEALTH_SAMPLE=true`` makes the notebook render its synthetic dataset, and
every other ``HEALTH_*`` variable is cleared so a developer's shell cannot change the output.
Volatile metadata (cell timings, interpreter version) is dropped so diffs stay small.
Requires the dev extras (``nbformat``, ``nbclient``, ``ipykernel``).
"""

from __future__ import annotations

import argparse
import os
import tempfile
from collections.abc import Mapping
from pathlib import Path

import nbformat
from nbclient import NotebookClient

NOTEBOOK = Path(__file__).resolve().parent / "notebooks" / "pipeline_health_check.ipynb"
DEFAULT_TIMEOUT_S = 300


def execute_sample(
    path: Path = NOTEBOOK,
    overrides: Mapping[str, str] | None = None,
    timeout: int = DEFAULT_TIMEOUT_S,
) -> nbformat.NotebookNode:
    """Run ``path`` in a fresh kernel (sample mode) and return the executed notebook."""
    notebook = nbformat.read(path, as_version=4)
    env = {k: v for k, v in os.environ.items() if not k.startswith("HEALTH_")}
    env["HEALTH_SAMPLE"] = "true"
    env.update({f"HEALTH_{k.upper()}": v for k, v in (overrides or {}).items()})
    previous = dict(os.environ)
    os.environ.clear()
    os.environ.update(env)
    try:
        with tempfile.TemporaryDirectory() as workdir:
            client = NotebookClient(
                notebook,
                timeout=timeout,
                kernel_name="python3",
                resources={"metadata": {"path": workdir}},
            )
            client.execute()
    finally:
        os.environ.clear()
        os.environ.update(previous)
    return notebook


def strip_volatile_metadata(notebook: nbformat.NotebookNode) -> nbformat.NotebookNode:
    """Remove per-run timing and interpreter details that would churn every diff."""
    for cell in notebook.cells:
        cell.metadata.pop("execution", None)
    notebook.metadata["language_info"] = {"name": "python"}
    return notebook


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", help="execute only; do not write")
    parser.add_argument("--notebook", type=Path, default=NOTEBOOK)
    args = parser.parse_args()
    executed = strip_volatile_metadata(execute_sample(args.notebook))
    nbformat.validate(executed)
    if args.check:
        print("notebook executed without errors")
        return 0
    nbformat.write(executed, args.notebook)
    print(f"saved outputs to {args.notebook}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
