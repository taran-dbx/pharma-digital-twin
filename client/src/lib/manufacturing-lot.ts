/**
 * Lot / device-history context for Trinity Medical's hip-stem cell.
 *
 * Mirrors `src/iot_twin/domain/lot_context.py`: one lot per 300 s heat-event
 * cycle, numbered within its UTC day (`TM-HS12-261120-142`, so ids never
 * wrap), one part every 20 s. The producer stamps these values on every
 * `cnc_1_*` reading, the alert engine copies them onto the alert, and the cell
 * page uses the same clock to name the parts cut during an excursion. All
 * values are labelled reference fixtures (no MES is connected).
 */

/** One lot per heat-event cycle; equals `SPINDLE_HEAT_SCENARIO.cycleSeconds` (tested). */
export const LOT_CYCLE_S = 300;
const DAY_S = 86_400;
export const PART_CYCLE_S = 20;
export const PARTS_PER_LOT = LOT_CYCLE_S / PART_CYCLE_S;

export const LOT_REFERENCE = {
  label: "Reference · MES not connected",
  lotPrefix: "TM-HS12",
  workOrder: "WO-TM-24817",
  partNumber: "HS-TI64-12",
  partDescription: "Femoral hip stem, size 12, Ti-6Al-4V",
  /** Fictitious GTIN-14 with a valid check digit. */
  udiDi: "00850012345671",
  /** Reference cost of one scrapped stem, for the cost-of-poor-quality estimate. */
  unitCostUsd: 1850,
} as const;

/** Sensors on the instrumented mill carry lot context. */
export const LOT_SENSOR_PREFIX = "cnc_1_";

export interface LotContext {
  lot_id: string;
  part_serial: string;
  work_order: string;
  part_number: string;
  udi_di: string;
  dhr_ref: string;
}

export interface PartAtRisk {
  serial: string;
  lotId: string;
  index: number;
  cutStart: number;
  cutEnd: number;
}

export interface LotDisposition {
  lotIds: string[];
  partsAtRisk: PartAtRisk[];
  partsReleased: number;
  dhrRefs: string[];
}

const mod = (n: number, m: number) => ((n % m) + m) % m;

/** Lot being machined at `epochSeconds`: `<prefix>-<YYMMDD>-<cycle of the UTC day>`. */
export function lotIdAt(epochSeconds: number): string {
  const day = new Date(Math.floor(epochSeconds) * 1000).toISOString().slice(2, 10).replace(/-/g, "");
  const seq = Math.floor(mod(epochSeconds, DAY_S) / LOT_CYCLE_S);
  return `${LOT_REFERENCE.lotPrefix}-${day}-${String(seq).padStart(3, "0")}`;
}

/** Epoch second at which the lot being machined at `epochSeconds` started. */
export function lotStartAt(epochSeconds: number): number {
  return epochSeconds - mod(epochSeconds, LOT_CYCLE_S);
}

export function partIndexAt(epochSeconds: number): number {
  return Math.floor(mod(epochSeconds, LOT_CYCLE_S) / PART_CYCLE_S) + 1;
}

export function partSerial(lotId: string, index: number): string {
  return `${lotId}-${String(index).padStart(2, "0")}`;
}

export function dhrRef(lotId: string): string {
  return `DHR-${lotId}`;
}

export function lotContextAt(epochSeconds: number): LotContext {
  const lotId = lotIdAt(epochSeconds);
  return {
    lot_id: lotId,
    part_serial: partSerial(lotId, partIndexAt(epochSeconds)),
    work_order: LOT_REFERENCE.workOrder,
    part_number: LOT_REFERENCE.partNumber,
    udi_di: LOT_REFERENCE.udiDi,
    dhr_ref: dhrRef(lotId),
  };
}

/** Parts whose cut overlaps `[start, end]` (epoch seconds), oldest first. */
export function partsInWindow(start: number, end: number): PartAtRisk[] {
  const lo = Math.min(start, end);
  const hi = Math.max(start, end);
  const parts: PartAtRisk[] = [];
  for (let slot = Math.floor(lo / PART_CYCLE_S); slot <= Math.floor(hi / PART_CYCLE_S); slot += 1) {
    const t0 = slot * PART_CYCLE_S;
    const lotId = lotIdAt(t0);
    const index = partIndexAt(t0);
    parts.push({ serial: partSerial(lotId, index), lotId, index, cutStart: t0, cutEnd: t0 + PART_CYCLE_S });
  }
  return parts;
}

/** "Quarantine these parts, release the rest" for an excursion window. */
export function lotDisposition(start: number, end: number): LotDisposition {
  const partsAtRisk = partsInWindow(start, end);
  const perLot = new Map<string, number>();
  for (const part of partsAtRisk) perLot.set(part.lotId, (perLot.get(part.lotId) ?? 0) + 1);
  const lotIds = [...perLot.keys()];
  return {
    lotIds,
    partsAtRisk,
    partsReleased: [...perLot.values()].reduce((sum, n) => sum + (PARTS_PER_LOT - n), 0),
    dhrRefs: lotIds.map(dhrRef),
  };
}

/** Python constants the producer notebook needs to stamp the same lot context. */
export function lotContextPython(): string {
  return `
# Lot / DHR context for the implant cell (mirror of lot_context.py). Every
# cnc_1_* reading carries the lot and part being machined at its device time.
LOT_CYCLE_S = ${LOT_CYCLE_S}
PART_CYCLE_S = ${PART_CYCLE_S}
LOT_PREFIX = "${LOT_REFERENCE.lotPrefix}"
LOT_WORK_ORDER = "${LOT_REFERENCE.workOrder}"
LOT_PART_NUMBER = "${LOT_REFERENCE.partNumber}"
LOT_UDI_DI = "${LOT_REFERENCE.udiDi}"
LOT_SENSOR_PREFIX = "${LOT_SENSOR_PREFIX}"


def _lot_context(now):
    day = time.strftime("%y%m%d", time.gmtime(now))
    lot_id = f"{LOT_PREFIX}-{day}-{int((now % 86400) // LOT_CYCLE_S):03d}"
    part = int((now % LOT_CYCLE_S) // PART_CYCLE_S) + 1
    return {
        "lot_id": lot_id,
        "part_serial": f"{lot_id}-{part:02d}",
        "work_order": LOT_WORK_ORDER,
        "part_number": LOT_PART_NUMBER,
        "udi_di": LOT_UDI_DI,
        "dhr_ref": f"DHR-{lot_id}",
    }`.trim();
}
