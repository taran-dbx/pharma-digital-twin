"""Shared helper: require an explicit Databricks CLI profile for dev scripts.

Scripts must never fall back to a hardcoded (real) profile name. The profile is
taken from ``--profile <name>`` (removed from ``argv`` so a script's own argparse
never sees it) or the ``DATABRICKS_CONFIG_PROFILE`` environment variable.
"""

from __future__ import annotations

import os
import sys

ENV_VAR = "DATABRICKS_CONFIG_PROFILE"


def resolve_profile(argv: list[str], environ: dict[str, str]) -> tuple[str | None, list[str]]:
    """Return ``(profile, remaining_argv)``; ``--profile`` wins over the env var."""
    remaining: list[str] = []
    profile: str | None = None
    items = iter(argv)
    for arg in items:
        if arg == "--profile":
            profile = next(items, None)
        elif arg.startswith("--profile="):
            profile = arg.split("=", 1)[1]
        else:
            remaining.append(arg)
    return (profile or environ.get(ENV_VAR) or None), remaining


def require_profile() -> str:
    """Resolve the profile, export it, strip ``--profile`` from ``sys.argv``, or exit(2)."""
    profile, remaining = resolve_profile(sys.argv[1:], dict(os.environ))
    if not profile:
        sys.stderr.write(
            f"error: no Databricks CLI profile given. Pass --profile <name> or set {ENV_VAR}.\n"
            "       (This script does not default to any workspace.)\n"
        )
        raise SystemExit(2)
    os.environ[ENV_VAR] = profile
    sys.argv[1:] = remaining
    return profile
