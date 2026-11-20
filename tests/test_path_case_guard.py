"""Guard: repo paths referenced from tests must match on-disk case exactly.

macOS is case-insensitive, so a wrong-case path passes locally and fails on
Linux CI. This resolves every literal ``ROOT / "a" / "b"`` chain in tests and
checks each segment against the directory listing.
"""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _is_repo_root(node: ast.expr) -> bool:
    """True for ``ROOT`` or ``Path(__file__)...parent.parent`` / ``.parents[1]``."""
    text = ast.unparse(node)
    return text in {"ROOT", "root", "REPO_ROOT"} or (
        "__file__" in text and text.endswith(("parent.parent", "parents[1]"))
    )


def _segments(node: ast.expr) -> list[str] | None:
    """String segments of a repo-root-anchored `X / "a" / "b"` chain, else None."""
    parts: list[str] = []
    while isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div):
        if not (isinstance(node.right, ast.Constant) and isinstance(node.right.value, str)):
            return None
        parts.append(node.right.value)
        node = node.left
    return list(reversed(parts)) if parts and _is_repo_root(node) else None


def _exact_case_exists(segments: list[str]) -> bool:
    current = ROOT
    for segment in segments:
        if segment in ("", ".", ".."):
            return True
        if not current.is_dir() or segment not in {p.name for p in current.iterdir()}:
            return False
        current = current / segment
    return True


def _chains():
    for test_file in sorted(Path(__file__).parent.glob("test_*.py")):
        tree = ast.parse(test_file.read_text(encoding="utf-8"))
        inner = {id(n.left) for n in ast.walk(tree) if isinstance(n, ast.BinOp)}
        for node in ast.walk(tree):
            if id(node) in inner:
                continue  # only the outermost node of each chain
            segments = _segments(node)
            if segments:
                yield test_file.name, node.lineno, segments


def test_repo_paths_in_tests_use_exact_case():
    bad = [
        f"{name}:{line}: {'/'.join(segs)}"
        for name, line, segs in _chains()
        if not _exact_case_exists(segs)
    ]
    assert not bad, "wrong-case or missing repo paths:\n" + "\n".join(bad)
