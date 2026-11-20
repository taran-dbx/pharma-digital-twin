#!/usr/bin/env python3
"""Render app.yaml from a named workspace deployment profile.

Profiles live in deploy/workspaces/<name>.profile (JSON, no secrets). The real
``*.profile`` files are gitignored; only ``*.profile.example`` templates with
placeholder values are committed. To create a profile::

    cp deploy/workspaces/example-us-east.profile.example deploy/workspaces/<name>.profile
    $EDITOR deploy/workspaces/<name>.profile
    python scripts/render_app_yaml.py <name>

The committed app.yaml is rendered from an example profile (placeholders). Render
your real profile before ``databricks sync``/deploy, and do NOT commit the result
(the identifier check in .pre-commit-config.yaml and a unit test guard this).
Admin access is group-based (ADMIN_GROUP); ``ADMIN_EMAILS`` is an optional profile
key that is only emitted when set. ``USAGE_TRACKING_ENABLED`` is an optional profile
key that defaults to ``"true"`` (set ``"false"`` to opt out of usage tracking).
This script is dev/deploy-time only — scripts/ is excluded from the app bundle.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PROFILES_DIR = REPO_ROOT / "deploy" / "workspaces"
DEFAULT_OUTPUT = REPO_ROOT / "app.yaml"

REQUIRED_PROFILE_KEYS: tuple[str, ...] = (
    "UC_CATALOG",
    "UC_SCHEMA",
    "PGHOST",
    "LAKEBASE_PROJECT",
    "LAKEBASE_ENDPOINT",
    "DATABRICKS_SQL_WAREHOUSE_ID",
    "ZEROBUS_SERVER_ENDPOINT",
    "ZEROBUS_CLIENT_ID",
    "ZEROBUS_SECRET_SCOPE",
    "ZEROBUS_SECRET_KEY",
    "UC_STORAGE_BASE",
    "UC_EXTERNAL_LOCATION",
    "UC_SERVICE_CREDENTIAL",
    "PIPELINES_LAKEBASE_PROJECT",
    "MSK_BOOTSTRAP_SERVERS",
    "INDUSTRIES",
    "ADMIN_GROUP",
)


OPTIONAL_PROFILE_KEYS: tuple[str, ...] = (
    "ADMIN_EMAILS",
    "LAKETS_SQL_PATH",
    "USAGE_TRACKING_ENABLED",
)
DEFAULT_USAGE_TRACKING_ENABLED = "true"
EXAMPLE_SUFFIX = ".profile.example"


class ProfileError(Exception):
    """Raised when a profile cannot be loaded or validated."""


def _yaml_value(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _env_entry(name: str, value: str) -> str:
    return f"  - name: {name}\n    value: {_yaml_value(value)}"


def _env_from_resource(name: str, resource_key: str) -> str:
    """Bind an env var from a Databricks App resource; never a hardcoded id.

    Genie uses resource key ``genie-space`` (permission CAN_VIEW) so
    ``GENIE_SPACE_ID`` is injected by the Apps runtime.
    """
    return f"  - name: {name}\n    valueFrom: {resource_key}"


def load_profile(
    name: str,
    profiles_dir: Path = DEFAULT_PROFILES_DIR,
    *,
    use_example: bool = False,
) -> dict[str, str]:
    suffix = EXAMPLE_SUFFIX if use_example else ".profile"
    path = profiles_dir / f"{name}{suffix}"
    if not path.is_file():
        available = sorted(p.name[: -len(suffix)] for p in profiles_dir.glob(f"*{suffix}"))
        choices = ", ".join(available) if available else "(none)"
        hint = (
            ""
            if use_example
            else (
                f"; copy a *{EXAMPLE_SUFFIX} template to {name}.profile and fill in "
                "your workspace values (or pass --example to render the placeholders)"
            )
        )
        raise ProfileError(f"unknown profile {name!r}; available: {choices}{hint}")

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ProfileError(f"invalid JSON in profile {name!r}: {exc}") from exc

    if not isinstance(raw, dict):
        raise ProfileError(f"profile {name!r} must be a JSON object")

    missing = [key for key in REQUIRED_PROFILE_KEYS if key not in raw]
    if missing:
        raise ProfileError(f"profile {name!r} missing required key(s): {', '.join(missing)}")

    profile: dict[str, str] = {}
    for key in REQUIRED_PROFILE_KEYS:
        value = raw[key]
        if not isinstance(value, str):
            raise ProfileError(
                f"profile {name!r} key {key!r} must be a string, got {type(value).__name__}"
            )
        profile[key] = value

    for key in OPTIONAL_PROFILE_KEYS:
        value = raw.get(key)
        if value is None:
            continue
        if not isinstance(value, str):
            raise ProfileError(
                f"profile {name!r} key {key!r} must be a string, got {type(value).__name__}"
            )
        profile[key] = value

    return profile


def render_app_yaml(profile: dict[str, str]) -> str:
    env_blocks = [
        _env_entry("UC_CATALOG", profile["UC_CATALOG"]),
        _env_entry("UC_SCHEMA", profile["UC_SCHEMA"]),
        _env_entry("PGHOST", profile["PGHOST"]),
        _env_entry("PGDATABASE", "databricks_postgres"),
        _env_entry("PGPORT", "5432"),
        _env_entry("LAKEBASE_PROJECT", profile["LAKEBASE_PROJECT"]),
        _env_entry("LAKEBASE_ENDPOINT", profile["LAKEBASE_ENDPOINT"]),
        # Injected from the ``sql-warehouse`` app resource (CAN_USE) declared in
        # resources/app.yml; the profile's warehouse id feeds the bundle variable.
        _env_from_resource("DATABRICKS_SQL_WAREHOUSE_ID", "sql-warehouse"),
        _env_entry("ZEROBUS_SERVER_ENDPOINT", profile["ZEROBUS_SERVER_ENDPOINT"]),
        _env_entry("ZEROBUS_TABLE_NAME", ""),
        _env_entry("ZEROBUS_CLIENT_ID", profile["ZEROBUS_CLIENT_ID"]),
        _env_entry("ZEROBUS_SECRET_SCOPE", profile["ZEROBUS_SECRET_SCOPE"]),
        _env_entry("ZEROBUS_SECRET_KEY", profile["ZEROBUS_SECRET_KEY"]),
        _env_entry("UC_STORAGE_BASE", profile["UC_STORAGE_BASE"]),
        _env_entry("UC_EXTERNAL_LOCATION", profile["UC_EXTERNAL_LOCATION"]),
        _env_entry("UC_SERVICE_CREDENTIAL", profile["UC_SERVICE_CREDENTIAL"]),
        _env_entry("PIPELINES_LAKEBASE_PROJECT", profile["PIPELINES_LAKEBASE_PROJECT"]),
        _env_entry("MSK_BOOTSTRAP_SERVERS", profile["MSK_BOOTSTRAP_SERVERS"]),
        _env_entry("INDUSTRIES", profile["INDUSTRIES"]),
        _env_entry("ADMIN_GROUP", profile["ADMIN_GROUP"]),
        *(
            [_env_entry("ADMIN_EMAILS", profile["ADMIN_EMAILS"])]
            if profile.get("ADMIN_EMAILS")
            else []
        ),
        # Optional: by default lakets.sql is resolved relative to the package.
        *(
            [_env_entry("LAKETS_SQL_PATH", profile["LAKETS_SQL_PATH"])]
            if profile.get("LAKETS_SQL_PATH")
            else []
        ),
        # Injected by the Apps runtime from the genie-space resource (CAN_VIEW).
        # Never hardcode a Genie space id in profiles or source.
        _env_from_resource("GENIE_SPACE_ID", "genie-space"),
        # Usage tracking (dbdemos-tracker) opt-out flag; on unless the profile says otherwise.
        _env_entry(
            "USAGE_TRACKING_ENABLED",
            profile.get("USAGE_TRACKING_ENABLED") or DEFAULT_USAGE_TRACKING_ENABLED,
        ),
    ]

    body = "\n\n".join(env_blocks)
    return (
        "command:\n"
        '  - "/bin/sh"\n'
        '  - "-c"\n'
        '  - "uvicorn --app-dir src iot_twin.app:app --host 0.0.0.0 --port ${DATABRICKS_APP_PORT:-8000}"\n'
        "\n"
        "env:\n"
        f"{body}\n"
    )


def _normalize(text: str) -> str:
    lines = [line.rstrip() for line in text.splitlines()]
    return "\n".join(lines) + "\n"


def check_app_yaml(path: Path, profile: dict[str, str]) -> bool:
    if not path.is_file():
        return False
    expected = _normalize(render_app_yaml(profile))
    actual = _normalize(path.read_text(encoding="utf-8"))
    return actual == expected


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Render app.yaml from a workspace deployment profile."
    )
    parser.add_argument(
        "profile",
        help="Profile name (deploy/workspaces/<name>.profile)",
    )
    parser.add_argument(
        "--example",
        action="store_true",
        help="Load deploy/workspaces/<name>.profile.example (placeholder values)",
    )
    parser.add_argument(
        "--profiles-dir",
        type=Path,
        default=DEFAULT_PROFILES_DIR,
        help=f"Directory containing *.profile files (default: {DEFAULT_PROFILES_DIR})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Output app.yaml path (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Verify --output matches the profile without writing",
    )
    args = parser.parse_args(argv)

    try:
        profile = load_profile(args.profile, args.profiles_dir, use_example=args.example)
    except ProfileError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.check:
        if check_app_yaml(args.output, profile):
            print(f"{args.output} matches profile {args.profile!r}")
            return 0
        print(
            f"{args.output} does not match profile {args.profile!r}",
            file=sys.stderr,
        )
        return 2

    rendered = render_app_yaml(profile)
    args.output.write_text(rendered, encoding="utf-8")
    print(f"wrote {args.output} from profile {args.profile!r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
