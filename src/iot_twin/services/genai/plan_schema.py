"""Structured action-plan schema and the grounding check run on every model answer.

The model must answer with one JSON object matching :class:`ActionPlan`. Every
claim carries ``citations`` — ids from the grounding set. After schema
validation, :func:`check_grounding` fails the plan when it:

* cites an id that is not in the grounding set (fabricated citation),
* states a number (units included: 0.05mm, 5min, 87%) that matches no value in
  the rows THAT CLAIM cites, after rounding to the precision written and
  allowing simple derivations (``value - threshold``, list counts, a 0–1 score
  as a percentage). Numbers inside untrusted free text (notes, reasons, model
  names) and in the system prompt are never allowed values,
* names a lot or part serial — in the disposition lists or anywhere in the
  text — that is not in the lot rows,
* narrates a risk score the grounding set does not hold, or ignores one it does.

A failed check never hides the plan; it is shown with a ``fail`` badge and the
reasons so a reviewer can see exactly what was not supported.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, get_args

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from iot_twin.domain.lot_context import LOT_PREFIX
from iot_twin.models.app_schema import ALERT_RESPONSE_ACTIONS
from iot_twin.services.genai.grounding import GroundingItem, GroundingSet

OwnerRole = Literal["Operator", "QA", "Maintenance", "Manufacturing Eng"]
OWNER_ROLES: tuple[str, ...] = ("Operator", "QA", "Maintenance", "Manufacturing Eng")
ResponseAction = Literal[
    "quarantine_lot", "open_ncr", "request_cmm_inspection", "request_maintenance"
]
if set(get_args(ResponseAction)) != set(ALERT_RESPONSE_ACTIONS):  # pragma: no cover - drift guard
    raise RuntimeError("ResponseAction must list exactly ALERT_RESPONSE_ACTIONS")

MAX_TEXT = 1200
MAX_CITATIONS = 120
MAX_LOTS = 100
MAX_SERIALS = 1500
Citations = list[str]


class _Model(BaseModel):
    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)


class Claim(_Model):
    text: str = Field(min_length=1, max_length=MAX_TEXT)
    citations: Citations = Field(min_length=1, max_length=MAX_CITATIONS)


class Quarantine(_Model):
    lots: list[str] = Field(default_factory=list, max_length=MAX_LOTS)
    serials: list[str] = Field(default_factory=list, max_length=MAX_SERIALS)
    rationale: str = Field(min_length=1, max_length=MAX_TEXT)
    citations: Citations = Field(min_length=1, max_length=MAX_CITATIONS)


class CmmInspection(_Model):
    scope: str = Field(min_length=1, max_length=MAX_TEXT)
    serials: list[str] = Field(default_factory=list, max_length=MAX_SERIALS)
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

# Bumped whenever the rules below change; part of the plan-reuse key.
CHECKER_VERSION = "grounding-check/2026-11-20.v3"

# A number, optionally with a unit glued or hyphenated on (0.05mm, 5min,
# 5-minute, 64.2°C, 87%), that is not part of an identifier, date or clock time
# (OQ-TM-CNC1-014, 2026-11-20, 10:42).
_CLAIM_NUMBER = re.compile(
    r"(?<![\w.\-/:])([-+]?\d+(?:\.\d+)?)(\s?%|-?[a-zA-Zµ°]{1,8})?(?![\w\-/:]|\.\d)"
)
_PERCENT_WORD = re.compile(r"^\s*percent\b", re.I)
# Fixed names and regulatory references that contain digits but are not quantities.
_NON_QUANTITIES = re.compile(
    r"\bSpindle\s+\d+\b|\bCNC-?\d+\b|\b21\s*CFR\s*(?:Part\s*)?\d+(?:\.\d+)?\b"
    r"|\bISO\s*\d+(?::\d{4})?\b|\bPart\s+11\b|\brev\.?\s+[A-Z]\b|\b\d+-axis\b",
    re.I,
)
# References that ARE checked, against their own source rather than quantities:
# "step 2" against the plan's step orders, "version 3" / "v3" against the cited
# rows and the model metadata (risk model_version).
_STEP_REF = re.compile(r"\bsteps?\s+\d+(?:\s*(?:-|to|and|,)\s*\d+)*", re.I)
_VERSION_REF = re.compile(
    r"\b(?:model\s+)?(?:version|ver\.)\s*v?(\d+(?:\.\d+)*)\b|\bv(\d+(?:\.\d+)*)\b", re.I
)
_LOT_OR_SERIAL = re.compile(rf"\b{re.escape(LOT_PREFIX)}-\d{{6}}-\d{{3}}(?:-\d{{2}})?\b")
# Rows whose text fields are structured (catalog, SQL output) and may carry
# numbers in text ("60 °C", bucket labels). Every other string (notes, reasons,
# model names) is untrusted and contributes none.
_TEXT_NUMBER_SOURCES = frozenset({"alert_rule", "readings"})
_DERIVABLE_SOURCES = frozenset({"alert", "alert_rule", "risk_score", "projected_rft"})
_DURATION_TOLERANCE_S = 1.0


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


def _model_metadata_numbers(data: dict[str, Any]) -> list[float]:
    """Numbers inside the risk model's feature identifiers (``spindle_temp_slope_5m``).

    Feature names are structured model metadata, not free text, so "the
    5-minute slope" is grounded when the risk row is cited.
    """
    features = data.get("top_features") or []
    names = [f.get("feature") for f in features if isinstance(f, dict)]
    names += [f for f in features if isinstance(f, str)]
    return [float(n) for name in names if name for n in re.findall(r"\d+(?:\.\d+)?", str(name))]


def _item_numbers(item: GroundingItem) -> set[float]:
    """Typed numbers in one row, list counts, 0-1 values as percentages, intra-row differences."""
    nums = [n for n in (_numeric(leaf) for leaf in _leaves(item.data)) if n is not None]
    if item.source in _TEXT_NUMBER_SOURCES:
        for leaf in _leaves(item.data):
            if isinstance(leaf, str):
                nums.extend(float(m.group(1)) for m in _CLAIM_NUMBER.finditer(leaf))
    if item.source == "risk_score":
        nums.extend(_model_metadata_numbers(item.data))
    values = set(nums)
    values.update(float(len(v)) for v in item.data.values() if isinstance(v, list))
    values.update(n * 100 for n in nums if 0 < abs(n) <= 1)
    if len(nums) <= 40:
        values.update(abs(a - b) for a in nums for b in nums)
    return values


def allowed_numbers(grounding: GroundingSet, citations: Iterable[str]) -> list[float]:
    """Numbers a claim may state: values from the rows IT cites, plus simple derivations."""
    by_id = {item.id: item for item in grounding.items}
    cited = [by_id[c] for c in citations if c in by_id]
    values: set[float] = set()
    for item in cited:
        values |= _item_numbers(item)
    derivable = [
        n
        for item in cited
        if item.source in _DERIVABLE_SOURCES
        for n in (_numeric(leaf) for leaf in _leaves(item.data))
        if n is not None
    ]
    values.update(abs(a - b) for a in derivable for b in derivable)
    values |= _durations(cited)
    lots = [item for item in cited if item.source == "lot"]
    values.add(float(len(lots)))
    values.add(float(sum(len(i.data.get("parts_at_risk") or []) for i in lots)))
    return sorted(values)


def _timestamp(value: Any) -> float | None:
    if not isinstance(value, str) or len(value) < 16 or value[4:5] != "-":
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return (parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)).timestamp()


def _durations(cited: list[GroundingItem]) -> set[float]:
    """Durations between cited timestamps (onset → opened, opened → cleared, …).

    Seconds with a ±1 s tolerance, and the same span in minutes and hours.
    """
    stamps = sorted(
        {t for item in cited for t in (_timestamp(leaf) for leaf in _leaves(item.data)) if t}
    )[:60]
    values: set[float] = set()
    for i, a in enumerate(stamps):
        for b in stamps[i + 1 :]:
            span = b - a
            values.update({span - _DURATION_TOLERANCE_S, span, span + _DURATION_TOLERANCE_S})
            values.update({span / 60, span / 3600})
    return values


def _matches(number: float, decimals: int, allowed: list[float]) -> bool:
    tolerance = 0.5 * 10 ** (-decimals) + 1e-9
    return any(abs(abs(number) - abs(value)) <= tolerance for value in allowed)


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


def _lot_identifiers(grounding: GroundingSet) -> frozenset[str]:
    ids: set[str] = set()
    for item in grounding.by_source("lot"):
        ids.add(str(item.data.get("lot_id")))
        ids.update(str(s) for s in item.data.get("parts_at_risk") or [])
        if item.data.get("triggering_part_serial"):
            ids.add(str(item.data["triggering_part_serial"]))
    return frozenset(ids)


def _identifier_reasons(plan: ActionPlan, grounding: GroundingSet) -> list[str]:
    """Every lot id / serial — in disposition lists AND in free text — must be a lot row's."""
    known = _lot_identifiers(grounding)
    d = plan.disposition
    named = [("lot", "disposition.quarantine.lots", v) for v in d.quarantine.lots]
    named += [("serial", "disposition.quarantine.serials", v) for v in d.quarantine.serials]
    named += [("serial", "disposition.cmm_inspection.serials", v) for v in d.cmm_inspection.serials]
    for path, text, _citations in _claims(plan):
        named += [("lot/serial", path, m.group(0)) for m in _LOT_OR_SERIAL.finditer(text)]
    return [
        f"{path} names {kind} '{value}' that is not in the lot rows"
        for kind, path, value in named
        if value not in known
    ]


def _versions(grounding: GroundingSet, citations: Iterable[str]) -> set[str]:
    """Version strings a claim may name: cited rows' and the model metadata's."""
    cited = set(citations)
    found: set[str] = set()
    for item in grounding.items:
        if item.id in cited or item.source == "risk_score":
            version = item.data.get("model_version")
            if version is not None:
                found.update(re.findall(r"\d+(?:\.\d+)*", str(version)))
    return found


def _reference_reasons(
    path: str, text: str, versions: set[str], step_orders: set[int]
) -> tuple[list[str], int]:
    reasons, checked = [], 0
    for match in _STEP_REF.finditer(text):
        for number in re.findall(r"\d+", match.group(0)):
            checked += 1
            if int(number) not in step_orders:
                reasons.append(f"{path}: refers to step {number}, which the plan does not have")
    for match in _VERSION_REF.finditer(text):
        checked += 1
        version = match.group(1) or match.group(2)
        if version not in versions:
            reasons.append(f"{path}: version {version} does not match the cited model metadata")
    return reasons, checked


def _number_reasons(path: str, text: str, allowed: list[float]) -> tuple[list[str], int]:
    reasons, checked = [], 0
    # Replace with a word character so neighbouring digits stay "glued" (ids stay ids).
    scrubbed = _NON_QUANTITIES.sub("X", text)
    for pattern in (_LOT_OR_SERIAL, _STEP_REF, _VERSION_REF):
        scrubbed = pattern.sub("X", scrubbed)
    for match in _CLAIM_NUMBER.finditer(scrubbed):
        checked += 1
        raw, unit = match.group(1), (match.group(2) or "").strip()
        number = float(raw)
        decimals = len(raw.split(".", 1)[1]) if "." in raw else 0
        percent = unit == "%" or bool(_PERCENT_WORD.match(scrubbed[match.end() :]))
        ok = _matches(number, decimals, allowed) or (
            percent and _matches(number / 100, decimals + 2, allowed)
        )
        if not ok:
            reasons.append(
                f"{path}: number {match.group(0).strip()} does not match a value in its cited rows"
            )
    return reasons, checked


def _risk_reasons(plan: ActionPlan, grounding: GroundingSet) -> list[str]:
    risk_ids = {item.id for item in grounding.by_source("risk_score")}
    cited = set(plan.risk_narrative.citations)
    if risk_ids and not cited & risk_ids:
        return ["risk_narrative does not cite the model risk score in the grounding set"]
    if not risk_ids and re.search(r"\brisk score\b.*\d", plan.risk_narrative.text, re.I):
        return ["risk_narrative states a risk score but the grounding set holds none"]
    return []


def check_grounding(plan: ActionPlan, grounding: GroundingSet) -> GroundingCheck:
    """Verify citations, per-claim numbers and lot/serial ids against the grounding set."""
    ids = grounding.ids
    reasons: list[str] = []
    cited: list[str] = []
    checked = 0
    step_orders = {step.order for step in plan.steps}
    for path, text, citations in _claims(plan):
        for citation in citations:
            if citation not in ids:
                reasons.append(f"{path}: cites '{citation}', which is not in the grounding set")
            elif citation not in cited:
                cited.append(citation)
        found, count = _number_reasons(path, text, allowed_numbers(grounding, citations))
        refs, ref_count = _reference_reasons(
            path, text, _versions(grounding, citations), step_orders
        )
        reasons.extend([*found, *refs])
        checked += count + ref_count
    reasons.extend(_identifier_reasons(plan, grounding))
    reasons.extend(_risk_reasons(plan, grounding))
    return GroundingCheck("fail" if reasons else "pass", tuple(reasons), tuple(cited), checked)
