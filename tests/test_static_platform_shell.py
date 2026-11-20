"""Static regression checks for platform-shell UI defects."""

from __future__ import annotations

from pathlib import Path

DIST = Path(__file__).parents[1] / "static" / "dist"


def _index() -> str:
    return (DIST / "index.html").read_text(encoding="utf-8")


def test_production_shell_uses_a_real_favicon_c7303() -> None:
    """[C7303][defect][smoke] The production page does not request Vite's missing icon."""
    index = _index()

    assert 'href="/brand/data-streaming-icon-full-color.svg"' in index
    assert 'href="/vite.svg"' not in index
    assert (DIST / "brand" / "data-streaming-icon-full-color.svg").is_file()
