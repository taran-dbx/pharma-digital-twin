#!/usr/bin/env python3
"""Fail when workspace-specific identifiers are committed.

Scans the given files (pre-commit passes the staged ones) or, with no arguments,
every tracked file. This file defines the patterns, so it is excluded from its own scan.

Rules (see RULES): Lakebase endpoint hosts, Databricks Apps hosts, real
``*.cloud.databricks.com`` hosts, AWS account ids (in ARNs, in context, or bare
12-digit numbers), 15-16 digit workspace ids next to ``zerobus`` / ``workspace`` /
``/o=`` or used as a hostname label, MSK broker hosts with a real cluster id,
``npm-proxy*`` hosts (lockfiles included), the owner's personal email / handle and
known real workspace names. Placeholders such as ``example-*``, ``<workspace-host>``,
``123456789012``, ``000000000000`` and ``abc123`` pass.

Suppress a single deliberate line with a trailing ``check-identifiers: allow`` comment.
"""

from __future__ import annotations

import re
import subprocess
import sys
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SELF = Path(__file__).resolve()
ALLOW_MARKER = "check-identifiers: allow"
MAX_BYTES = 5_000_000

# First DNS label of a *.cloud.databricks.com host that is a known placeholder.
_PLACEHOLDER_LABEL = re.compile(
    r"^(?:example[a-z0-9-]*|x|ws|workspace|myws|your-host|dbc-12345|123|e2"
    r"|1234567890123456|(\d)\1{9,})$"
)
_PLACEHOLDER_ACCOUNT_IDS = frozenset({"123456789012", "000000000000"})
# 15-16 digit workspace ids that are obviously fake (sequences, repeated digits, zeros).
_PLACEHOLDER_WORKSPACE_ID = re.compile(r"^(?:1234567890123456|1357924680135792|(\d)\1{9,})$")
# Cluster-id label of an MSK broker host that is a known placeholder.
_PLACEHOLDER_MSK_ID = re.compile(r"^(?:example[a-z0-9-]*|abc123|x{3,}|0{3,}|1{3,}|123456)$")

_CLOUD_HOST = re.compile(
    r"(?<![A-Za-z0-9<>_.-])([A-Za-z0-9][A-Za-z0-9.-]*)\.cloud\.databricks\.com"
)
_ARN_ACCOUNT = re.compile(r"arn:aws[a-z-]*:[a-z0-9-]+:[a-z0-9-]*:(\d{12})(?!\d)")
_ACCOUNT_CONTEXT = re.compile(
    r"(?i)(?:aws[_ -]?account(?:[_ -]?id)?|account[_ -]?id)\W{0,6}(\d{12})(?!\d)"
)


_MSK_BROKER = re.compile(
    r"(?i)(?<![A-Za-z0-9-])[a-z0-9-]+\.[a-z0-9-]+\.([a-z0-9]+)\.c\d+\.kafka\.[a-z0-9-]+\.amazonaws\.com"
)
# Three shapes: id after zerobus/workspace/o= context, id as a hostname label.
_WORKSPACE_ID = re.compile(
    r"(?i)(?:(?:zerobus(?:[_ -]?[a-z]+)?|workspace(?:[_ -]?id)?|[/?&]o)\W{0,6}(?<![0-9])(\d{15,16})(?![0-9])"
    r"|(?<![0-9A-Za-z_-])(\d{15,16})(?=\.[a-z]))"
)
_BARE_ACCOUNT_ID = re.compile(r"(?<![0-9A-Za-z_.-])(\d{12})(?![0-9A-Za-z_])")


@dataclass(frozen=True)
class Hit:
    path: str
    line: int
    rule: str
    text: str


@dataclass(frozen=True)
class Rule:
    name: str
    pattern: re.Pattern[str]
    # Optional filter on the regex match; returns True when the match is a violation.
    is_violation: Callable[[re.Match[str]], bool] = lambda _m: True


def _host_violation(match: re.Match[str]) -> bool:
    first_label = match.group(1).split(".")[0]
    return _PLACEHOLDER_LABEL.match(first_label) is None


def _arn_violation(match: re.Match[str]) -> bool:
    return match.group(1) not in _PLACEHOLDER_ACCOUNT_IDS


def _msk_violation(match: re.Match[str]) -> bool:
    return _PLACEHOLDER_MSK_ID.match(match.group(1)) is None


def _workspace_id_violation(match: re.Match[str]) -> bool:
    digits = match.group(1) or match.group(2)
    return _PLACEHOLDER_WORKSPACE_ID.match(digits) is None


RULES: tuple[Rule, ...] = (
    Rule("lakebase-endpoint-host", re.compile(r"(?<![A-Za-z0-9_-])ep-[a-z]+-[a-z]+-[a-z0-9]{4,}")),
    Rule(
        "databricksapps-host",
        re.compile(
            r"(?<![A-Za-z0-9<>_.-])[A-Za-z0-9][A-Za-z0-9-]*\.(?:[a-z]+\.)?databricksapps\.com"
        ),
    ),
    Rule("databricks-cloud-host", _CLOUD_HOST, _host_violation),
    Rule("aws-account-id-in-arn", _ARN_ACCOUNT, _arn_violation),
    Rule("aws-account-id-in-context", _ACCOUNT_CONTEXT, _arn_violation),
    Rule("aws-account-id-bare", _BARE_ACCOUNT_ID, _arn_violation),
    Rule("workspace-id", _WORKSPACE_ID, _workspace_id_violation),
    Rule("msk-broker-host", _MSK_BROKER, _msk_violation),
    Rule("private-npm-registry", re.compile(r"(?i)npm-proxy[a-z0-9.-]*")),
    Rule(
        "owner-identity", re.compile(r"(?i)taran(?:preet)?(?:singh)?[a-z._-]*(?:@databricks\.com)?")
    ),
    Rule(
        "real-workspace-name",
        re.compile(r"(?<![A-Za-z0-9])(?:fe-vm-[a-z-]+|fe-sandbox-[a-z-]+|e2-demo-[a-z-]+)"),
    ),
)


def scan_text(text: str, path: str = "<text>") -> list[Hit]:
    hits: list[Hit] = []
    for number, line in enumerate(text.splitlines(), start=1):
        if ALLOW_MARKER in line:
            continue
        seen: set[tuple[int, int]] = set()
        for rule in RULES:
            for match in rule.pattern.finditer(line):
                if not rule.is_violation(match):
                    continue
                # Overlapping rules (e.g. ARN account vs bare account id) report once.
                span = match.span()
                if any(span[0] < end and start < span[1] for start, end in seen):
                    continue
                seen.add(span)
                hits.append(Hit(path, number, rule.name, match.group(0)))
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
    return scan_text(data.decode("utf-8", errors="replace"), display)


def tracked_files() -> list[str]:
    out = subprocess.check_output(["git", "ls-files", "-z"], cwd=REPO_ROOT)
    return [p for p in out.decode().split("\0") if p]


def run(files: Iterable[str]) -> list[Hit]:
    hits: list[Hit] = []
    for name in files:
        path = (REPO_ROOT / name) if not Path(name).is_absolute() else Path(name)
        if not path.is_file() or path.resolve() == SELF:
            continue
        hits.extend(scan_file(path, name))
    return hits


def main(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    hits = run(args or tracked_files())
    for hit in hits:
        print(f"{hit.path}:{hit.line}: [{hit.rule}] {hit.text}")
    if hits:
        print(
            f"\n{len(hits)} workspace-specific identifier(s) found. Replace them with placeholders "
            "(example.com, <workspace-host>, 123456789012, ...) and keep real values in the "
            "gitignored deploy/workspaces/*.profile.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
