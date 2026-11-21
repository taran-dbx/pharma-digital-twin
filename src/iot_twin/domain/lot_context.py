"""Lot / device-history context for the Trinity Medical machining cell.

Spindle 3 on CNC-1 machines Ti-6Al-4V femoral hip stems. There is no MES in
the demo, so the lot clock is deterministic on the wall clock and mirrors
``client/src/lib/manufacturing-lot.ts`` (the producer stamps the same values
on every ``cnc_1_*`` reading):

* one lot per scenario cycle (``LOT_CYCLE_S`` = the 300 s heat-event cycle),
  numbered within its UTC day, e.g. ``TM-HS12-261120-142`` (cycles align with
  days because 86 400 s is a whole number of cycles, so ids never wrap),
* one part every ``PART_CYCLE_S`` seconds, ``PARTS_PER_LOT`` parts per lot.

Projected right-first-time (the app's single first-pass-yield proxy) is
``(parts started - parts at risk) / parts started`` over the lots an
excursion touched. It is *projected* because no CMM results exist yet.

Every value here is a labelled reference fixture (work order, part number,
UDI-DI, DHR reference), never a record of a real device.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

LOT_CYCLE_S = 300
DAY_S = 86_400
PART_CYCLE_S = 20
PARTS_PER_LOT = LOT_CYCLE_S // PART_CYCLE_S
LOT_PREFIX = "TM-HS12"
WORK_ORDER = "WO-TM-24817"
PART_NUMBER = "HS-TI64-12"
PART_DESCRIPTION = "Femoral hip stem, size 12, Ti-6Al-4V"
# Fictitious GTIN-14 with a valid check digit; a UDI-DI identifies the model.
UDI_DI = "00850012345671"
# Sensors on the instrumented mill carry lot context.
LOT_SENSOR_PREFIX = "cnc_1_"

LOT_METADATA_KEYS: tuple[str, ...] = (
    "lot_id",
    "part_serial",
    "work_order",
    "part_number",
    "udi_di",
    "dhr_ref",
)


def _epoch(at: float | datetime) -> float:
    return at.timestamp() if isinstance(at, datetime) else float(at)


PROJECTED_RFT_LABEL = "Projected right-first-time"
PROJECTED_RFT_DEFINITION = (
    "Projected right-first-time = (parts started - parts at risk) / parts started, over the "
    "lots an excursion touched. Parts at risk are the parts whose cut overlapped the time "
    "Spindle 3 was outside its validated limit. Projected: no CMM results exist yet."
)


def lot_id_at(at: float | datetime) -> str:
    """Lot being machined at ``at``: ``<prefix>-<YYMMDD>-<cycle of the UTC day>``."""
    epoch = _epoch(at)
    day = datetime.fromtimestamp(epoch, UTC).strftime("%y%m%d")
    return f"{LOT_PREFIX}-{day}-{int((epoch % DAY_S) // LOT_CYCLE_S):03d}"


def lot_start_at(at: float | datetime) -> float:
    """Epoch second at which the lot being machined at ``at`` started."""
    epoch = _epoch(at)
    return float(epoch - (epoch % LOT_CYCLE_S))


def part_index_at(at: float | datetime) -> int:
    """1-based position of the part being cut at ``at`` within its lot."""
    return int((_epoch(at) % LOT_CYCLE_S) // PART_CYCLE_S) + 1


def part_serial(lot_id: str, index: int) -> str:
    return f"{lot_id}-{index:02d}"


def dhr_ref(lot_id: str) -> str:
    return f"DHR-{lot_id}"


def lot_context_at(at: float | datetime) -> dict[str, str]:
    """Lot fields for the part being machined at ``at``."""
    lot_id = lot_id_at(at)
    return {
        "lot_id": lot_id,
        "part_serial": part_serial(lot_id, part_index_at(at)),
        "work_order": WORK_ORDER,
        "part_number": PART_NUMBER,
        "udi_di": UDI_DI,
        "dhr_ref": dhr_ref(lot_id),
    }


def carries_lot_context(sensor_name: str) -> bool:
    return sensor_name.startswith(LOT_SENSOR_PREFIX)


def lot_fields(metadata: Mapping[str, Any] | None) -> dict[str, str]:
    """The lot keys present in a reading's ``metadata``."""
    if not metadata:
        return {}
    return {key: str(metadata[key]) for key in LOT_METADATA_KEYS if metadata.get(key)}


def parts_in_window(start: float | datetime, end: float | datetime) -> list[dict[str, Any]]:
    """Parts whose cut overlaps ``[start, end]``, oldest first.

    A part cut from ``t0`` to ``t0 + PART_CYCLE_S`` is at risk if any of its
    cut happened inside the window.
    """
    lo, hi = _epoch(start), _epoch(end)
    if hi < lo:
        lo, hi = hi, lo
    first = int(lo // PART_CYCLE_S)
    last = int(hi // PART_CYCLE_S)
    parts: list[dict[str, Any]] = []
    for slot in range(first, last + 1):
        t0 = slot * PART_CYCLE_S
        lot_id = lot_id_at(t0)
        parts.append(
            {
                "serial": part_serial(lot_id, part_index_at(t0)),
                "lot_id": lot_id,
                "index": part_index_at(t0),
                "cut_start": float(t0),
                "cut_end": float(t0 + PART_CYCLE_S),
            }
        )
    return parts


def projected_rft(
    start: float | datetime, end: float | datetime, now: float | datetime
) -> dict[str, Any] | None:
    """The canonical first-pass-yield proxy; see ``PROJECTED_RFT_DEFINITION``."""
    at_risk = parts_in_window(start, end)
    if not at_risk:
        return None
    lot_starts = sorted({lot_start_at(part["cut_start"]) for part in at_risk})
    current = _epoch(now)
    started = 0
    for lot_start in lot_starts:
        if current >= lot_start:
            started += min(PARTS_PER_LOT, int((current - lot_start) // PART_CYCLE_S) + 1)
    started = max(started, len(at_risk))
    good = started - len(at_risk)
    return {
        "pct": round(100.0 * good / started, 2),
        "good": good,
        "parts_started": started,
        "parts_at_risk": len(at_risk),
    }


def lot_disposition(
    start: float | datetime, end: float | datetime, now: float | datetime | None = None
) -> dict[str, Any]:
    """Which parts to quarantine and how many of each affected lot can be released."""
    at_risk = parts_in_window(start, end)
    lots: dict[str, int] = {}
    for part in at_risk:
        lots[part["lot_id"]] = lots.get(part["lot_id"], 0) + 1
    return {
        "projected_rft": projected_rft(start, end, end if now is None else now),
        "lot_ids": list(lots),
        "parts_at_risk": [part["serial"] for part in at_risk],
        "parts_released": sum(PARTS_PER_LOT - count for count in lots.values()),
        "parts_per_lot": PARTS_PER_LOT,
        "dhr_refs": [dhr_ref(lot_id) for lot_id in lots],
        "part_number": PART_NUMBER,
        "udi_di": UDI_DI,
        "work_order": WORK_ORDER,
    }
