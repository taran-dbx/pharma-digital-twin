#!/usr/bin/env python3
"""Fail when a credential-named variable, kwarg or key is assigned a string literal.

External "hardcoded credential" scanners flag ``password="..."``, ``token: "..."``,
``"client_secret": "..."`` and similar shapes regardless of whether the value is a
real secret, so the tracked tree must not contain any. Tests use
``tests/_fakes.fake_credential()``; configuration that names a secret (scope or key
names) uses constants whose own name is not credential-shaped.

Scans the given files (pre-commit passes the staged ones) or, with no arguments,
every tracked file. Notebooks are scanned cell by cell, source and saved outputs.
Binary files, lockfiles and this script are skipped.

Narrow allowlist (see ``_is_allowed``):
- the value starts with an interpolation (``"${cfg.x}"``, ``"{name}"``): a template
  slot in generated code, not a literal;
- the fetch ``RequestInit.credentials`` mode (``credentials: "include"``,
  ``"same-origin"``, ``"omit"``);
- the ``UC_SERVICE_CREDENTIAL`` / ``uc_service_credential`` key in the JSON deploy
  templates (``deploy/**/*.example``): it holds the *name* of a Unity Catalog
  service credential, the key is a fixed env/bundle variable name and JSON has no
  constants to hoist the example value into.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SELF = Path(__file__).resolve()
MAX_BYTES = 5_000_000
SKIPPED_NAMES = frozenset({"package-lock.json"})

_CREDENTIAL_WORDS = r"secret|password|passwd|pwd|token|api[_-]?key|credential|private[_-]?key"
# <credential-ish name>[closing quote of a key] (= or :) <quoted literal>
ASSIGNMENT = re.compile(
    rf"(?i)(?P<name>[A-Za-z0-9_]*(?:{_CREDENTIAL_WORDS})\w*)['\"]?\s*[:=]\s*"
    r"['\"](?P<value>[^'\"\n]+)['\"]"
)
_FETCH_CREDENTIAL_MODES = frozenset({"include", "same-origin", "omit"})
_DEPLOY_TEMPLATE_NAME_KEYS = frozenset({"uc_service_credential"})


def _is_deploy_template(path: str) -> bool:
    return path.startswith("deploy/") and path.endswith(".example")


@dataclass(frozen=True)
class Hit:
    path: str
    line: int
    name: str

    def __str__(self) -> str:
        return f"{self.path}:{self.line}: credential-shaped name `{self.name}` assigned a literal"


def _is_allowed(name: str, value: str, path: str) -> bool:
    if value.startswith(("${", "{")):
        return True
    if name.lower() == "credentials" and value in _FETCH_CREDENTIAL_MODES:
        return True
    return _is_deploy_template(path) and name.lower() in _DEPLOY_TEMPLATE_NAME_KEYS


def scan_text(text: str, path: str = "<text>", first_line: int = 1) -> list[Hit]:
    hits: list[Hit] = []
    for number, line in enumerate(text.splitlines(), start=first_line):
        for match in ASSIGNMENT.finditer(line):
            if not _is_allowed(match.group("name"), match.group("value"), path):
                hits.append(Hit(path, number, match.group("name")))
    return hits


def _as_text(value: object) -> str:
    return "".join(value) if isinstance(value, list) else str(value)


def _notebook_chunks(raw: str) -> Iterator[tuple[str, str]]:
    """Yield ``(location, text)`` for every cell source and saved text output."""
    notebook = json.loads(raw)
    for index, cell in enumerate(notebook.get("cells", [])):
        yield f"cell {index} source", _as_text(cell.get("source", ""))
        for output in cell.get("outputs", []) or []:
            if "text" in output:
                yield f"cell {index} output", _as_text(output["text"])
            for mime, data in (output.get("data") or {}).items():
                if mime.startswith("text/") or mime.endswith("json"):
                    payload = data if isinstance(data, (str, list)) else json.dumps(data)
                    yield f"cell {index} output {mime}", _as_text(payload)


def scan_notebook(raw: str, display: str) -> list[Hit]:
    try:
        chunks = list(_notebook_chunks(raw))
    except (ValueError, AttributeError):
        return scan_text(raw, display)
    hits: list[Hit] = []
    for location, text in chunks:
        hits.extend(scan_text(text, f"{display} [{location}]"))
    return hits


def scan_file(path: Path, display: str) -> list[Hit]:
    try:
        if path.stat().st_size > MAX_BYTES:
            return []
        data = path.read_bytes()
    except OSError:
        return []
    if b"\0" in data:
        return []
    text = data.decode("utf-8", errors="replace")
    if path.suffix == ".ipynb":
        return scan_notebook(text, display)
    return scan_text(text, display)


def tracked_files() -> list[str]:
    out = subprocess.check_output(["git", "ls-files", "-z"], cwd=REPO_ROOT)
    return [p for p in out.decode().split("\0") if p]


def run(files: Iterable[str]) -> list[Hit]:
    hits: list[Hit] = []
    for name in files:
        path = (REPO_ROOT / name) if not Path(name).is_absolute() else Path(name)
        if not path.is_file() or path.resolve() == SELF or path.name in SKIPPED_NAMES:
            continue
        hits.extend(scan_file(path, name))
    return hits


def main(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    hits = run(args or tracked_files())
    for hit in hits:
        print(hit)
    if hits:
        print(
            f"\n{len(hits)} literal credential assignment(s) found. In tests use "
            "tests/_fakes.fake_credential(); for secret scope/key names use a constant "
            "whose name is not credential-shaped.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
