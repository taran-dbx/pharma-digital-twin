"""Savings model for the Trinity Medical implant cell (pure, no I/O).

Two halves:

* :func:`project` — the annual projection a funding executive reads:
  annual exposure, cost avoided and exposure remaining from a handful of
  assumptions (defaults in ``value_defaults.json``, each overridable per request).
* :func:`actuals` — the same arithmetic applied to what the app actually
  recorded over a window: excursions detected, how many were caught in time,
  lots and parts held or released, NCRs and CMM inspections opened.

Formulas (documented with sources and limitations in docs/value-model.md)::

    loss_per_event   = cost_per_lot_lost + lot_release_delay_cost_per_day * release_delay_days_per_event
    annual_events    = lines_in_scope * excursion_events_per_line_per_year
    annual_exposure  = annual_events * loss_per_event
    caught_events    = annual_events * capture_rate
    cost_avoided     = caught_events * (loss_per_event - inspection_cost)
    exposure_remaining = annual_exposure - cost_avoided
                       = missed_events * loss_per_event + caught_events * inspection_cost

    cost_avoided_to_date = caught_in_time * (loss_per_event - inspection_cost)
    exposure_if_scrapped = not_caught_in_time * loss_per_event
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import asdict, dataclass, replace
from typing import Any

from iot_twin.services.value.config import DEFAULTS_PATH, defaults_payload
from iot_twin.services.value.episodes import WindowTotals

__all__ = ["DEFAULTS_PATH"]
MONEY_MAX = 1e9


class AssumptionError(ValueError):
    """One or more assumptions failed validation; ``errors`` lists them."""

    def __init__(self, errors: list[dict[str, str]]) -> None:
        super().__init__("; ".join(f"{e['field']}: {e['message']}" for e in errors))
        self.errors = errors


@dataclass(frozen=True)
class FieldSpec:
    minimum: float
    maximum: float
    integer: bool = False
    label: str = ""


FIELDS: dict[str, FieldSpec] = {
    "lines_in_scope": FieldSpec(1, 1000, integer=True, label="Lines in scope"),
    "excursion_events_per_line_per_year": FieldSpec(0, 10000, label="Excursions per line per year"),
    "cost_per_lot_lost": FieldSpec(0, MONEY_MAX, label="Cost per lot lost"),
    "capture_rate": FieldSpec(0, 1, label="Capture rate"),
    "inspection_cost": FieldSpec(0, MONEY_MAX, label="CMM inspection + NCR cost"),
    "lot_release_delay_cost_per_day": FieldSpec(
        0, MONEY_MAX, label="Lot-release delay cost per day"
    ),
    "release_delay_days_per_event": FieldSpec(0, 365, label="Release delay days per event"),
}


@dataclass(frozen=True)
class Assumptions:
    lines_in_scope: int
    excursion_events_per_line_per_year: float
    cost_per_lot_lost: float
    capture_rate: float
    inspection_cost: float
    lot_release_delay_cost_per_day: float
    release_delay_days_per_event: float
    lot_cost_components: tuple[tuple[str, float], ...]

    @property
    def loss_per_event(self) -> float:
        delay = self.lot_release_delay_cost_per_day * self.release_delay_days_per_event
        return self.cost_per_lot_lost + delay

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["lot_cost_components"] = dict(self.lot_cost_components)
        return data


def _coerce(name: str, raw: Any) -> tuple[float | None, str | None]:
    spec = FIELDS[name]
    if isinstance(raw, bool):
        return None, "must be a number"
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None, "must be a number"
    if not math.isfinite(value):
        return None, "must be a finite number"
    if spec.integer and not value.is_integer():
        return None, "must be a whole number"
    if value < spec.minimum or value > spec.maximum:
        return None, f"must be between {spec.minimum:g} and {spec.maximum:g}"
    return value, None


def _build(values: Mapping[str, float], components: tuple[tuple[str, float], ...]) -> Assumptions:
    return Assumptions(
        lines_in_scope=int(values["lines_in_scope"]),
        excursion_events_per_line_per_year=float(values["excursion_events_per_line_per_year"]),
        cost_per_lot_lost=float(values["cost_per_lot_lost"]),
        capture_rate=float(values["capture_rate"]),
        inspection_cost=float(values["inspection_cost"]),
        lot_release_delay_cost_per_day=float(values["lot_release_delay_cost_per_day"]),
        release_delay_days_per_event=float(values["release_delay_days_per_event"]),
        lot_cost_components=components,
    )


def default_assumptions(payload: Mapping[str, Any] | None = None) -> Assumptions:
    """Validated defaults; ``cost_per_lot_lost`` is the sum of the lot cost components."""
    data = dict(payload if payload is not None else defaults_payload())
    raw_components = data.get("lot_cost_components") or {}
    errors: list[dict[str, str]] = []
    components: list[tuple[str, float]] = []
    if not isinstance(raw_components, Mapping) or not raw_components:
        errors.append({"field": "lot_cost_components", "message": "must be a non-empty object"})
    else:
        for key, raw in raw_components.items():
            value, problem = _coerce("cost_per_lot_lost", raw)
            if problem or value is None:
                errors.append({"field": f"lot_cost_components.{key}", "message": problem or ""})
            else:
                components.append((str(key), value))
    data["cost_per_lot_lost"] = sum(v for _, v in components)
    values: dict[str, float] = {}
    for name in FIELDS:
        value, problem = _coerce(name, data.get(name))
        if problem or value is None:
            errors.append({"field": name, "message": problem or "is required"})
        else:
            values[name] = value
    if errors:
        raise AssumptionError(errors)
    return _build(values, tuple(components))


def resolve_assumptions(
    overrides: Mapping[str, Any] | None, base: Assumptions | None = None
) -> Assumptions:
    """Apply per-request overrides to the defaults, rejecting unknown or invalid keys.

    Overriding ``cost_per_lot_lost`` replaces the component breakdown with a
    single ``override`` line, so the breakdown always sums to the cost used.
    """
    resolved = base or default_assumptions()
    errors: list[dict[str, str]] = []
    changes: dict[str, Any] = {}
    for name, raw in (overrides or {}).items():
        if name not in FIELDS:
            errors.append({"field": name, "message": "is not a recognised assumption"})
            continue
        value, problem = _coerce(name, raw)
        if problem or value is None:
            errors.append({"field": name, "message": problem or "is invalid"})
            continue
        changes[name] = int(value) if FIELDS[name].integer else value
    if errors:
        raise AssumptionError(errors)
    if "cost_per_lot_lost" in changes:
        changes["lot_cost_components"] = (("override", float(changes["cost_per_lot_lost"])),)
    return replace(resolved, **changes)


def _money(value: float) -> float:
    return round(value, 2)


def _usd(value: float) -> str:
    sign = "-" if value < 0 else ""
    return f"{sign}${abs(value):,.0f}"


def _num(value: float) -> str:
    return f"{value:,.2f}".rstrip("0").rstrip(".")


def per_event_breakdown(a: Assumptions) -> dict[str, Any]:
    """What one excursion costs if the lot is lost, and what catching it costs instead."""
    lines = [{"item": name, "usd": _money(v)} for name, v in a.lot_cost_components]
    delay = a.lot_release_delay_cost_per_day * a.release_delay_days_per_event
    if delay:
        lines.append({"item": "lot_release_delay", "usd": _money(delay)})
    return {
        "if_missed": lines,
        "loss_per_event": _money(a.loss_per_event),
        "if_caught": [{"item": "cmm_inspection_and_ncr", "usd": _money(a.inspection_cost)}],
        "net_avoided_per_caught_event": _money(a.loss_per_event - a.inspection_cost),
    }


def project(a: Assumptions) -> dict[str, Any]:
    """Annual exposure, cost avoided and exposure remaining, with the formula shown."""
    annual_events = a.lines_in_scope * a.excursion_events_per_line_per_year
    caught = annual_events * a.capture_rate
    missed = annual_events - caught
    loss = a.loss_per_event
    exposure = annual_events * loss
    avoided = caught * (loss - a.inspection_cost)
    remaining = exposure - avoided
    warnings = []
    if a.inspection_cost > loss:
        warnings.append("inspection costs more than the loss it prevents; cost avoided is negative")
    return {
        "annual_events": round(annual_events, 4),
        "caught_events": round(caught, 4),
        "missed_events": round(missed, 4),
        "loss_per_event": _money(loss),
        "annual_exposure": _money(exposure),
        "cost_avoided": _money(avoided),
        "exposure_remaining": _money(remaining),
        "per_event": per_event_breakdown(a),
        "formula": {
            "loss_per_event": (
                "cost_per_lot_lost + lot_release_delay_cost_per_day × release_delay_days_per_event"
                f" = {_usd(a.cost_per_lot_lost)} + {_usd(a.lot_release_delay_cost_per_day)} × "
                f"{_num(a.release_delay_days_per_event)} = {_usd(loss)}"
            ),
            "annual_exposure": (
                "lines_in_scope × excursion_events_per_line_per_year × loss_per_event"
                f" = {a.lines_in_scope} × {_num(a.excursion_events_per_line_per_year)} × "
                f"{_usd(loss)} = {_usd(exposure)}"
            ),
            "cost_avoided": (
                "annual_events × capture_rate × (loss_per_event − inspection_cost)"
                f" = {_num(annual_events)} × {_num(a.capture_rate)} × "
                f"({_usd(loss)} − {_usd(a.inspection_cost)}) = {_usd(avoided)}"
            ),
            "exposure_remaining": (
                "annual_exposure − cost_avoided"
                f" = {_usd(exposure)} − {_usd(avoided)} = {_usd(remaining)}"
            ),
        },
        "warnings": warnings,
    }


def actuals(totals: WindowTotals, a: Assumptions) -> dict[str, Any]:
    """Recorded outcomes over the whole window, priced like the projection.

    Uses ``loss_per_event`` (lot cost plus any release-delay cost) on both
    sides, so to-date and annual figures are comparable.
    """
    detected = totals.episodes
    caught = totals.caught_in_time
    missed = detected - caught
    net = a.loss_per_event - a.inspection_cost
    return {
        "excursion_events_detected": detected,
        "caught_in_time": caught,
        "not_caught_in_time": missed,
        "deadline_before_alert": totals.deadline_before_alert,
        "capture_rate_observed": round(caught / detected, 4) if detected else None,
        "lots_protected": totals.lots_protected,
        "lots_quarantined": totals.lots_quarantined,
        "parts_quarantined": totals.parts_quarantined,
        "parts_released": totals.parts_released,
        "ncrs_opened": totals.ncrs_opened,
        "cmm_inspections": totals.cmm_inspections,
        "cost_avoided_to_date": _money(caught * net),
        "exposure_if_scrapped": _money(missed * a.loss_per_event),
        "formula": {
            "cost_avoided_to_date": (
                "caught_in_time × (loss_per_event − inspection_cost)"
                f" = {caught} × ({_usd(a.loss_per_event)} − {_usd(a.inspection_cost)})"
                f" = {_usd(caught * net)}"
            ),
            "exposure_if_scrapped": (
                "not_caught_in_time × loss_per_event"
                f" = {missed} × {_usd(a.loss_per_event)} = {_usd(missed * a.loss_per_event)}"
            ),
        },
    }


def assumption_fields() -> list[dict[str, Any]]:
    """Field metadata for the editable assumptions panel."""
    return [
        {
            "name": name,
            "label": spec.label,
            "min": spec.minimum,
            "max": spec.maximum,
            "integer": spec.integer,
        }
        for name, spec in FIELDS.items()
    ]
