"""Readable text fallbacks for the health-check notebook's HTML outputs.

Every report block is HTML (tables, badges, headings, inline SVG charts). Viewers that do not
render HTML (untrusted notebooks, sanitising previews, many IDE/file viewers) fall back to
``text/plain`` / ``text/markdown``, so each output must carry a meaningful text version.

``html_to_markdown`` converts the report's HTML subset to Markdown: headings become ``#``
lines, tables become pipe tables, badges become their status word, and an ``<svg>`` chart
becomes ``[chart] <title>: <desc>``. ``mime_bundle`` builds the full output bundle.

The notebook is self-contained and carries an identical copy of these functions in its
``render`` cell (``tests/test_notebook_text_fallbacks.py`` keeps the two in sync).
"""

from __future__ import annotations

import re
from html.parser import HTMLParser

_BLOCK_TAGS = frozenset({"div", "p", "ul", "ol", "li", "section", "pre", "thead", "tbody"})
_SKIP_TAGS = frozenset({"script", "style"})
_SVG_NS = 'xmlns="http://www.w3.org/2000/svg"'
_PURE_SVG = re.compile(r"^\s*<svg\b[^>]*>.*</svg>\s*$", re.DOTALL)
_SPACES = re.compile(r"[ \t\r\f\v]+")
_BLANKS = re.compile(r"\n{3,}")
# Characters that would otherwise turn text such as ``<host>`` into (hidden) inline HTML.
_MD_SPECIAL = re.compile(r"[\\`*<>]")
# Element names the report can emit; any other bare ``<name>`` is a redaction placeholder.
_HTML_TAGS = frozenset(
    [
        "a",
        "abbr",
        "b",
        "blockquote",
        "br",
        "caption",
        "code",
        "col",
        "colgroup",
        "dd",
        "del",
        "desc",
        "details",
        "div",
        "dl",
        "dt",
        "em",
        "g",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "hr",
        "i",
        "img",
        "li",
        "line",
        "ol",
        "p",
        "path",
        "polyline",
        "pre",
        "rect",
        "s",
        "small",
        "span",
        "strong",
        "sub",
        "summary",
        "sup",
        "svg",
        "table",
        "tbody",
        "td",
        "text",
        "tfoot",
        "th",
        "thead",
        "title",
        "tr",
        "tspan",
        "u",
        "ul",
    ]
)
_BARE_TAG = re.compile(r"<([a-z][a-z0-9-]*)>")


class _MarkdownBuilder(HTMLParser):
    """Collects text from the report's HTML subset and lays it out as Markdown."""

    def __init__(self, markdown: bool) -> None:
        super().__init__(convert_charrefs=True)
        self._markdown = markdown
        self.out: list[str] = []
        self._skip = 0
        self._heading = 0
        self._table: list[list[str]] | None = None
        self._cell: list[str] | None = None
        self._svg: dict[str, list[str]] | None = None
        self._svg_part = ""

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in _SKIP_TAGS:
            self._skip += 1
        elif self._svg is not None:
            self._svg_part = tag if tag in ("title", "desc") else ""
        elif tag == "svg":
            self._svg = {"title": [], "desc": []}
        elif tag == "table":
            self._table = []
        elif tag == "tr" and self._table is not None:
            self._table.append([])
        elif tag in ("td", "th") and self._table is not None:
            self._cell = []
        elif re.fullmatch(r"h[1-6]", tag):
            self._heading = int(tag[1])
            self.out.append("\n\n" + "#" * self._heading + " ")
        elif tag == "br":
            self._write(" " if self._cell is not None else "\n")
        elif tag in _BLOCK_TAGS:
            self.out.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag in _SKIP_TAGS:
            self._skip = max(0, self._skip - 1)
        elif tag == "svg" and self._svg is not None:
            self._flush_svg()
        elif self._svg is not None:
            self._svg_part = ""
        elif tag in ("td", "th") and self._cell is not None and self._table:
            self._table[-1].append(" ".join("".join(self._cell).split()))
            self._cell = None
        elif tag == "table" and self._table is not None:
            self.out.append("\n\n" + _pipe_table(self._table) + "\n\n")
            self._table = None
        elif re.fullmatch(r"h[1-6]", tag) and self._heading:
            self._heading = 0
            self.out.append("\n\n")
        elif tag in _BLOCK_TAGS:
            self.out.append("\n")

    def handle_data(self, data: str) -> None:
        if self._skip:
            return
        if self._svg is not None:
            if self._svg_part:
                self._svg[self._svg_part].append(data)
            return
        self._write(_MD_SPECIAL.sub(r"\\\g<0>", data) if self._markdown else data)

    def _write(self, text: str) -> None:
        if self._cell is not None:
            self._cell.append(text)
        elif self._table is None:
            self.out.append(text)

    def _flush_svg(self) -> None:
        svg, self._svg = self._svg or {}, None
        title = " ".join("".join(svg.get("title", [])).split()) or "chart"
        desc = " ".join("".join(svg.get("desc", [])).split())
        self._write(f"\n[chart] {title}" + (f": {desc}" if desc else "") + "\n")


def escape_placeholders(markup: str) -> str:
    """Escape redaction placeholders (``<host>``) that would otherwise render as empty tags."""
    return _BARE_TAG.sub(
        lambda m: m.group(0) if m.group(1) in _HTML_TAGS else f"&lt;{m.group(1)}&gt;", markup
    )


def _pipe_table(rows: list[list[str]]) -> str:
    rows = [r for r in rows if r]
    if not rows:
        return ""
    width = max(len(r) for r in rows)
    padded = [[c.replace("|", "\\|") for c in r] + [""] * (width - len(r)) for r in rows]
    lines = ["| " + " | ".join(padded[0]) + " |", "|" + "---|" * width]
    lines += ["| " + " | ".join(r) + " |" for r in padded[1:]]
    return "\n".join(lines)


def _convert(markup: str, markdown: bool) -> str:
    builder = _MarkdownBuilder(markdown)
    builder.feed(markup)
    builder.close()
    lines = [_SPACES.sub(" ", line).strip() for line in "".join(builder.out).split("\n")]
    text = _BLANKS.sub("\n\n", "\n".join(lines)).strip()
    return text or "(empty output)"


def html_to_markdown(markup: str) -> str:
    """Markdown rendering of the report's HTML (text is escaped so nothing is hidden)."""
    return _convert(markup, markdown=True)


def html_to_text(markup: str) -> str:
    """Plain-text rendering: same layout as the Markdown, without escapes."""
    return _convert(markup, markdown=False)


def standalone_svg(markup: str) -> str | None:
    """The markup as a standalone SVG document when it is exactly one ``<svg>`` element."""
    if not _PURE_SVG.match(markup) or markup.count("<svg") != 1:
        return None
    svg = markup.strip()
    return svg if _SVG_NS in svg else svg.replace("<svg", f"<svg {_SVG_NS}", 1)


def mime_bundle(markup: str) -> dict[str, str]:
    """``text/html`` plus Markdown/plain-text fallbacks (and ``image/svg+xml`` for charts)."""
    markup = escape_placeholders(markup)
    bundle = {
        "text/html": markup,
        "text/markdown": html_to_markdown(markup),
        "text/plain": html_to_text(markup),
    }
    svg = standalone_svg(markup)
    if svg is not None:
        bundle["image/svg+xml"] = svg
    return bundle
