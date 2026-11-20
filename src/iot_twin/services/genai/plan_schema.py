"""Structured action-plan schema and the grounding check run on every model answer.

The model must answer with one JSON object matching :class:`ActionPlan`. Every
claim carries ``citations`` — ids from the grounding set. After schema
validation, :func:`check_grounding` fails the plan when it:

* cites an id that is not in the grounding set (fabricated citation),
* states a number that does not match any grounded value (after rounding to
  the precision written, and allowing simple derived values such as
  ``value - threshold``, list counts and a 0–1 score written as a percentage),
* names a lot or part serial that is not in the grounding set,
* narrates a risk score the grounding set does not hold, or ignores one it does.

A failed check never hides the plan; it is shown with a ``fail`` badge and the
reasons so a reviewer can see exactly what was not supported.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Any, Literal, get_args

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from iot_twin.models.app_schema import ALERT_RESPONSE_ACTIONS
from iot_twin.services.genai.grounding import GroundingSet

OwnerRole = Literal["Operator", "QA", "Maintenance", "Manufacturing Eng"]
OWNER_ROLES: tuple[str, ...] = ("Operator", "QA", "Maintenance", "Manufacturing Eng")
ResponseAction = Literal[
    "quarantine_lot", "open_ncr", "request_cmm_inspection", "request_maintenance"
]
if set(get_args(ResponseAction)) != set(ALERT_RESPONSE_ACTIONS):  # pragma: no cover - drift guard
    raise RuntimeError("ResponseAction must list exactly ALERT_RESPONSE_ACTIONS")

MAX_TEXT = 1200
MAX_CITATIONS = 20
Citations = list[str]


class _Model(BaseModel):
    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)


class Claim(_Model):
    text: str = Field(min_length=1, max_length=MAX_TEXT)
    citations: Citations = Field(min_length=1, max_length=MAX_CITATIONS)


class Quarantine(_Model):
    lots: list[str] = Field(default_factory=list, max_length=20)
    serials: list[str] = Field(default_factory=list, max_length=200)
    rationale: str = Field(min_length=1, max_length=MAX_TEXT)
    citations: Citations = Field(min_length=1, max_length=MAX_CITATIONS)


class CmmInspection(_Model):
    scope: str = Field(min_length=1, max_length=MAX_TEXT)
    serials: list[str] = Field(default_factory=list, max_length=200)
    citations: Citations = Field(min_length=1, max_length=MAX_CITATIONS)


class Decision(_Model):
    required: bool
    rationale: str = Field(min_length=1, max_length=MAX_TEXT)
    citations: Citations = Field(min_length=1, max_length=MAX_CITATIONS)


class Disposition(_Model):
    quarantine: Quarantine
    cmm_inspection: CmmInspection
    ncr: Decision
    maintenance_request: Decision


class Step(_Model):
    order: int = Field(ge=1, le=50)
    owner_role: OwnerRole
    action: str = Field(min_length=1, max_length=MAX_TEXT)
    response_action: ResponseAction | None = None
    citations: Citations = Field(min_length=1, max_length=MAX_CITATIONS)


class ActionPlan(_Model):
    summary: Claim
    disposition: Disposition
    risk_narrative: Claim
    steps: list[Step] = Field(min_length=1, max_length=15)


def output_schema() -> dict[str, Any]:
    """JSON schema shown to the model (trusted) and documented in the API."""
    return ActionPlan.model_json_schema()


@dataclass(frozen=True)
class ParseResult:
    plan: ActionPlan | None
    raw: Any
    errors: tuple[str, ...]


def _json_text(text: str) -> str:
    stripped = text.strip()
    fence = re.match(r"^```(?:json)?\s*(.*?)\s*```$", stripped, re.S)
    if fence:
        stripped = fence.group(1)
    start, end = stripped.find("{"), stripped.rfind("}")
    if start == -1 or end < start:
        raise ValueError("no JSON object in the model answer")
    return stripped[start : end + 1]


def parse_plan(text: str) -> ParseResult:
    """Parse and schema-validate the model's answer; never raises."""
    try:
        raw = json.loads(_json_text(text or ""))
    except (ValueError, json.JSONDecodeError) as exc:
        return ParseResult(None, None, (f"malformed JSON: {exc}",))
    if not isinstance(raw, dict):
        return ParseResult(None, raw, ("the answer must be a JSON object",))
    try:
        return ParseResult(ActionPlan.model_validate(raw), raw, ())
    except ValidationError as exc:
        errors = tuple(
            f"schema: {'.'.join(str(p) for p in err['loc'])}: {err['msg']}" for err in exc.errors()
        )
        return ParseResult(None, raw, errors[:20])


# --------------------------------------------------------------------------- grounding check

# A number not glued to a letter, hyphen, slash, colon or dot (so ids like
# OQ-TM-CNC1-014, HS-TI64-12, dates and clock times are not read as claims).
_CLAIM_NUMBER = re.compile(r"(?<![\w.\-/:])[-+]?\d+(?:\.\d+)?(?![\w\-/:]|\.\d)")
_PERCENT_AFTER = re.compile(r"^\s*(%|percent)")


@dataclass(frozen=True)
class GroundingCheck:
    status: Literal["pass", "fail"]
    reasons: tuple[str, ...]
    cited_ids: tuple[str, ...]
    numbers_checked: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "reasons": list(self.reasons),
            "cited_ids": list(self.cited_ids),
            "numbers_checked": self.numbers_checked,
        }


def failed_check(reasons: Iterable[str]) -> GroundingCheck:
    return GroundingCheck("fail", tuple(reasons), (), 0)


def _leaves(value: Any) -> Iterator[Any]:
    if isinstance(value, dict):
        for item in value.values():
            yield from _leaves(item)
    elif isinstance(value, list | tuple):
        for item in value:
            yield from _leaves(item)
    else:
        yield value


def _numeric(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int | float):
        return float(value)
    return None


def grounded_numbers(grounding: GroundingSet, trusted_text: str = "") -> list[float]:
    """Every number the plan may state: grounded values, numbers in grounded text,
    the trusted context's numbers and a few simple derived values."""
    values: set[float] = set()
    for item in grounding.items:
        nums = []
        for leaf in _leaves(item.data):
            num = _numeric(leaf)
            if num is not None:
                nums.append(num)
            elif isinstance(leaf, str):
                values.update(float(m.group(0)) for m in _CLAIM_NUMBER.finditer(leaf))
        for value in item.data.values():
            if isinstance(value, list):
                values.add(float(len(value)))
        values.update(nums)
        values.update(n * 100 for n in nums if 0 < abs(n) <= 1)
        if item.source in {"alert", "risk_score", "alert_rule", "readings", "lot"}:
            values.update(abs(a - b) for a in nums for b in nums)
    values.update(float(m.group(0)) for m in _CLAIM_NUMBER.finditer(trusted_text))
    values.add(float(len(grounding.by_source("lot"))))
    values.add(
        float(sum(len(i.data.get("parts_at_risk") or []) for i in grounding.by_source("lot")))
    )
    return sorted(values)


def _matches(claim: str, allowed: list[float]) -> bool:
    number = abs(float(claim))
    decimals = len(claim.split(".", 1)[1]) if "." in claim else 0
    tolerance = 0.5 * 10 ** (-decimals) + 1e-9
    return any(abs(number - abs(value)) <= tolerance for value in allowed)


def _claims(plan: ActionPlan) -> Iterator[tuple[str, str, list[str]]]:
    """``(path, text, citations)`` for every claim in the plan."""
    yield "summary", plan.summary.text, plan.summary.citations
    yield "risk_narrative", plan.risk_narrative.text, plan.risk_narrative.citations
    d = plan.disposition
    yield "disposition.quarantine", d.quarantine.rationale, d.quarantine.citations
    yield "disposition.cmm_inspection", d.cmm_inspection.scope, d.cmm_inspection.citations
    yield "disposition.ncr", d.ncr.rationale, d.ncr.citations
    yield (
        "disposition.maintenance_request",
        d.maintenance_request.rationale,
        d.maintenance_request.citations,
    )
    for index, step in enumerate(plan.steps):
        yield f"steps[{index}]", step.action, step.citations


def _identifier_reasons(plan: ActionPlan, grounding: GroundingSet) -> list[str]:
    corpus = json.dumps(grounding.rows(), default=str)
    reasons = []
    d = plan.disposition
    named = [("lot", v) for v in d.quarantine.lots] + [
        ("serial", v) for v in (*d.quarantine.serials, *d.cmm_inspection.serials)
    ]
    for kind, value in named:
        if f'"{value}"' not in corpus:
            reasons.append(f"disposition names {kind} '{value}' that is not in the grounding set")
    return reasons


def _number_reasons(path: str, text: str, allowed: list[float]) -> tuple[list[str], int]:
    reasons, checked = [], 0
    for match in _CLAIM_NUMBER.finditer(text):
        checked += 1
        claim = match.group(0).lstrip("+-")
        percent = bool(_PERCENT_AFTER.match(text[match.end() :]))
        if not _matches(claim, allowed) and not (
            percent and _matches(str(float(claim) / 100), allowed)
        ):
            reasons.append(f"{path}: number {match.group(0)} does not match any grounded value")
    return reasons, checked


def _risk_reasons(plan: ActionPlan, grounding: GroundingSet) -> list[str]:
    risk_ids = {item.id for item in grounding.by_source("risk_score")}
    cited = set(plan.risk_narrative.citations)
    if risk_ids and not cited & risk_ids:
        return ["risk_narrative does not cite the model risk score in the grounding set"]
    if not risk_ids and re.search(r"\brisk score\b.*\d", plan.risk_narrative.text, re.I):
        return ["risk_narrative states a risk score but the grounding set holds none"]
    return []


def check_grounding(
    plan: ActionPlan, grounding: GroundingSet, trusted_text: str = ""
) -> GroundingCheck:
    """Verify citations, numbers and identifiers against the grounding set."""
    ids = grounding.ids
    allowed = grounded_numbers(grounding, trusted_text)
    reasons: list[str] = []
    cited: list[str] = []
    checked = 0
    for path, text, citations in _claims(plan):
        for citation in citations:
            if citation not in ids:
                reasons.append(f"{path}: cites '{citation}', which is not in the grounding set")
            elif citation not in cited:
                cited.append(citation)
        found, count = _number_reasons(path, text, allowed)
        reasons.extend(found)
        checked += count
    reasons.extend(_identifier_reasons(plan, grounding))
    reasons.extend(_risk_reasons(plan, grounding))
    return GroundingCheck("fail" if reasons else "pass", tuple(reasons), tuple(cited), checked)
