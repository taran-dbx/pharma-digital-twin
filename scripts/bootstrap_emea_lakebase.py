"""Bootstrap iot-digital-twin + iot-lakets-demo Lakebase projects on example-demo-emea."""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
# Repo root (for ``scripts.*``) and src/ (for ``iot_twin``) must be importable.
for _path in (_ROOT / "src", _ROOT):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from scripts._profile import require_profile

from iot_twin.services.provisioning_service import provision_lakebase_project_and_branch


async def main() -> int:
    targets = [
        ("iot-digital-twin", "production"),
        ("iot-lakets-demo", "production"),
    ]
    for project_id, branch_id in targets:
        print(f"\n── {project_id}/{branch_id} ──")
        results = await provision_lakebase_project_and_branch(project_id, branch_id)
        for r in results:
            mark = {"created": "+", "already_exists": "=", "failed": "!"}.get(
                r.get("status", "?"),
                "?",
            )
            print(f"  [{mark}] {r.get('resource')}: {r.get('status')}")
            if r.get("error"):
                print(f"      error: {r['error']}")

    print("\n── endpoint discovery ──")
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    hosts: dict = {}
    for project_id, branch_id in targets:
        parent = f"projects/{project_id}/branches/{branch_id}"
        try:
            endpoints = list(w.postgres.list_endpoints(parent=parent))
            if endpoints:
                ep = w.postgres.get_endpoint(name=endpoints[0].name)
                hosts[project_id] = {
                    "endpoint_name": endpoints[0].name,
                    "host": ep.status.hosts.host if ep.status and ep.status.hosts else None,
                    "state": ep.status.current_state if ep.status else None,
                }
            else:
                hosts[project_id] = {"error": "no endpoints yet"}
        except Exception as exc:
            hosts[project_id] = {"error": str(exc)[:200]}

    print(json.dumps(hosts, indent=2, default=str))
    return 0


if __name__ == "__main__":
    require_profile()
    sys.exit(asyncio.run(main()))
