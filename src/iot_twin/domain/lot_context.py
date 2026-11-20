"""Lot / device-history context for the Trinity Medical machining cell.

Spindle 3 on CNC-1 machines Ti-6Al-4V femoral hip stems. There is no MES in
the demo, so the lot clock is deterministic on the wall clock and mirrors
``client/src/lib/manufacturing-lot.ts`` (the producer stamps the same values
on every ``cnc_1_*`` reading):

* one lot per scenario cycle (``LOT_CYCLE_S`` = the 300 s heat-event cycle),
* one part every ``PART_CYCLE_S`` seconds, ``PARTS_PER_LOT`` parts per lot.

Every value here is a labelled reference fixture (work order, part number,
UDI-DI, DHR reference), never a record of a real device.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

LOT_CYCLE_S = 300
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


def lot_seq_at(at: float | datetime) -> int:
    return int(_epoch(at) // LOT_CYCLE_S) % 10000


def lot_id_for(seq: int) -> str:
    return f"{LOT_PREFIX}-{seq % 10000:04d}"


def lot_id_at(at: float | datetime) -> str:
    return lot_id_for(lot_seq_at(at))


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


def lot_disposition(start: float | datetime, end: float | datetime) -> dict[str, Any]:
    """Which parts to quarantine and how many of each affected lot can be released."""
    at_risk = parts_in_window(start, end)
    lots: dict[str, int] = {}
    for part in at_risk:
        lots[part["lot_id"]] = lots.get(part["lot_id"], 0) + 1
    return {
        "lot_ids": list(lots),
        "parts_at_risk": [part["serial"] for part in at_risk],
        "parts_released": sum(PARTS_PER_LOT - count for count in lots.values()),
        "parts_per_lot": PARTS_PER_LOT,
        "dhr_refs": [dhr_ref(lot_id) for lot_id in lots],
        "part_number": PART_NUMBER,
        "udi_di": UDI_DI,
        "work_order": WORK_ORDER,
    }
