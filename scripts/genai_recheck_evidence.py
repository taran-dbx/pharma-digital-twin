#!/usr/bin/env python3
"""Re-run the CURRENT grounding check over every saved live-evidence plan.

Reads ``docs/evidence/genai/*-audit.json`` (the exact grounding rows and raw
answer of each run) and prints, per run, the check recorded at the time and the
result of today's checker — offline, no workspace access::

    python scripts/genai_recheck_evidence.py [--markdown]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from iot_twin.services.genai.grounding import GroundingItem, GroundingSet  # noqa: E402
from iot_twin.services.genai.plan_schema import CHECKER_VERSION, check_grounding, parse_plan  # noqa: E402

EVIDENCE = ROOT / "docs" / "evidence" / "genai"


def recheck(path: Path) -> dict:
    trace = json.loads(path.read_text(encoding="utf-8"))
    items = tuple(
        # ``ref`` (current) or ``key`` (records written before the rename).
        GroundingItem(r["id"], r["source"], r.get("ref", r.get("key")), r["data"])
        for r in trace["grounding_rows"]
    )
    grounding = GroundingSet(alert_id=trace["alert_id"], items=items)
    parsed = parse_plan(trace["response"].get("raw_text") or "")
    if parsed.plan is None:
        current, reasons = "rejected", list(parsed.errors)
    else:
        check = check_grounding(parsed.plan, grounding)
        current, reasons = check.status, list(check.reasons)
    return {
        "run": path.name.removesuffix("-audit.json"),
        "template": trace["prompt_template_version"].rsplit(".", 1)[-1],
        "recorded": f"{trace['status']} / {trace['grounding_check']}",
        "current": current,
        "reasons": reasons,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--markdown", action="store_true")
    args = parser.parse_args()
    results = [recheck(p) for p in sorted(EVIDENCE.glob("*-audit.json"))]
    if not args.markdown:
        print(json.dumps({"checker_version": CHECKER_VERSION, "runs": results}, indent=2))
        return 0
    print(f"Current checker: `{CHECKER_VERSION}`\n")
    print("| Run | Template | Recorded at the time | Current checker | Current reasons |")
    print("| --- | --- | --- | --- | --- |")
    for r in results:
        why = "; ".join(r["reasons"]) if r["reasons"] else "none"
        print(f"| `{r['run']}` | {r['template']} | {r['recorded']} | **{r['current']}** | {why} |")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
