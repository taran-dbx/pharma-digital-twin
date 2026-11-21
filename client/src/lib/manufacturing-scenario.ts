import { SPINDLE_SCENARIO_PYTHON } from "./generated/spindle-scenario-python";
import { lotContextPython } from "./manufacturing-lot";

/**
 * Scheduled Spindle 3 thermal episodes for the implant cell's realtime producer.
 *
 * The episodes are SIMULATED physics. The generator is the Python module
 * `src/iot_twin/scenario_engine/spindle_scenario.py`, which the producer notebook
 * embeds verbatim and the risk model's training data imports, so live and
 * training data come from one generator. Each 300 s lot cycle draws a plan from
 * a deterministic per-cycle PRNG: most cycles have an excursion whose ramp
 * starts at a random point in the cycle (often after a noisy vibration / power
 * precursor of random lead), some are false alarms and some are quiet. The
 * dashboard ports only the PRNG and its first two draws (excursion or not, ramp
 * start) to show when the next excursion is due; a Python test checks that the
 * two agree. It never synthesizes readings itself.
 */

export const SPINDLE_HEAT_SCENARIO = {
  id: "spindle-3-heat",
  sensorName: "cnc_1_spindle_temp",
  cycleSeconds: 300,
  /** The ramp starts at a random point in this range of each excursion cycle. */
  rampStartRangeSeconds: [30, 210],
  rampSeconds: 30,
  holdSeconds: 30,
  coolSeconds: 30,
  peakDeltaC: 25,
  planSeed: 20261121,
  excursionShare: 0.7,
} as const;

export type SpindleHeatPhase = "normal" | "heating" | "overheat" | "cooling";

export interface SpindleHeatState {
  phase: SpindleHeatPhase;
  /** Seconds until the next excursion starts (0 while one is in progress). */
  secondsToNextEvent: number;
  /** Seconds until the current phase ends. */
  secondsLeftInPhase: number;
}

/** Cycles searched ahead for the next excursion (the share makes a long gap unlikely). */
const MAX_LOOKAHEAD_CYCLES = 64;

/** The first `count` mulberry32 draws of cycle `index`; mirrors `spindle_cycle_draws` in Python. */
export function spindleCycleDraws(index: number, count: number): number[] {
  let state = (SPINDLE_HEAT_SCENARIO.planSeed ^ Math.imul(index | 0, 0x9e3779b1)) >>> 0;
  const draws: number[] = [];
  for (let i = 0; i < count; i += 1) {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = Math.imul(state ^ (state >>> 15), state | 1);
    t = (t + Math.imul(t ^ (t >>> 7), t | 61)) ^ t;
    draws.push(((t ^ (t >>> 14)) >>> 0) / 4294967296);
  }
  return draws;
}

/** First draw of cycle `index`. */
export function spindleCycleDraw(index: number): number {
  return spindleCycleDraws(index, 1)[0];
}

/** True when cycle `index` (`floor(epoch / cycleSeconds)`) has a heat excursion. */
export function spindleCycleHasExcursion(index: number): boolean {
  return spindleCycleDraw(index) < SPINDLE_HEAT_SCENARIO.excursionShare;
}

/** Seconds into cycle `index` where its ramp starts; mirrors `spindle_ramp_start`. */
export function spindleRampStart(index: number): number {
  const [lo, hi] = SPINDLE_HEAT_SCENARIO.rampStartRangeSeconds;
  return lo + (hi - lo) * spindleCycleDraws(index, 2)[1];
}

function secondsToNextExcursion(cycle: number, t: number): number {
  const { cycleSeconds } = SPINDLE_HEAT_SCENARIO;
  if (spindleCycleHasExcursion(cycle) && t < spindleRampStart(cycle)) {
    return spindleRampStart(cycle) - t;
  }
  for (let ahead = 1; ahead <= MAX_LOOKAHEAD_CYCLES; ahead += 1) {
    if (spindleCycleHasExcursion(cycle + ahead)) {
      return ahead * cycleSeconds + spindleRampStart(cycle + ahead) - t;
    }
  }
  return MAX_LOOKAHEAD_CYCLES * cycleSeconds - t;
}

export function spindleHeatStateAt(epochSeconds: number): SpindleHeatState {
  const { cycleSeconds, rampSeconds, holdSeconds, coolSeconds } = SPINDLE_HEAT_SCENARIO;
  const cycle = Math.floor(epochSeconds / cycleSeconds);
  const t = epochSeconds - cycle * cycleSeconds;
  const into = spindleCycleHasExcursion(cycle) ? t - spindleRampStart(cycle) : -1;
  if (into < 0 || into >= rampSeconds + holdSeconds + coolSeconds) {
    const next = secondsToNextExcursion(cycle, t);
    return { phase: "normal", secondsToNextEvent: next, secondsLeftInPhase: next };
  }
  if (into < rampSeconds) {
    return { phase: "heating", secondsToNextEvent: 0, secondsLeftInPhase: rampSeconds - into };
  }
  if (into < rampSeconds + holdSeconds) {
    return {
      phase: "overheat",
      secondsToNextEvent: 0,
      secondsLeftInPhase: rampSeconds + holdSeconds - into,
    };
  }
  return {
    phase: "cooling",
    secondsToNextEvent: 0,
    secondsLeftInPhase: rampSeconds + holdSeconds + coolSeconds - into,
  };
}

/** Replaced server-side at upload with the workspace path the app writes. */
export const PRODUCTION_CONTROL_PLACEHOLDER = "__IOT_PRODUCTION_CONTROL_FILE__";

const PRODUCTION_CONTROL_PYTHON = `
# Cell production control. The app rewrites this workspace file when an
# operator stops or resumes production; sensors of stopped assets stop
# emitting. It is read through the Workspace API because /Workspace FUSE keeps
# serving the content a process first saw. A missing or corrupt file means the
# cell is running; a failed read keeps the last known state.
from databricks.sdk import WorkspaceClient as _ControlWorkspaceClient
from databricks.sdk.errors import NotFound as _ControlNotFound

PRODUCTION_CONTROL_FILE = "${PRODUCTION_CONTROL_PLACEHOLDER}"
PRODUCTION_POLL_S = 2.0
_production_stopped = ()
_control_client = None


def _download_production_control():
    global _control_client
    if _control_client is None:
        _control_client = _ControlWorkspaceClient()
    with _control_client.workspace.download(PRODUCTION_CONTROL_FILE) as handle:
        return handle.read()


def _read_production_control():
    try:
        control = json.loads(_download_production_control())
    except (_ControlNotFound, ValueError):
        return ()
    if not isinstance(control, dict) or control.get("state") != "stopped":
        return ()
    return tuple(f"{asset}_" for asset in control.get("assets") or ())


def _watch_production_control():
    global _production_stopped
    while True:
        time.sleep(PRODUCTION_POLL_S)
        try:
            stopped = _read_production_control()
        except Exception as exc:
            logger.warning(f"[production control] read failed, keeping last state: {exc}")
            continue
        if stopped != _production_stopped:
            names = ", ".join(prefix[:-1] for prefix in stopped) or "none"
            logger.info(f"[production control] stopped assets: {names}")
            _production_stopped = stopped


def _production_allows(payload):
    return not str(payload.get("sensor_name") or "").startswith(_production_stopped)


threading.Thread(target=_watch_production_control, daemon=True).start()`.trim();

/**
 * Python cell fragment for the producer: calibrates the simulator's noise and,
 * for manufacturing, applies the scheduled heat event and the cell production
 * control before each Kafka send. Defines `_calibrate_simulator(sim)`,
 * `_apply_scenario(payload)` and `_production_allows(payload)`.
 */
export function producerSignalPython(industry: string): string {
  const calibrate = `
# The wheel's sensor catalog states noise_std in sensor units, but
# SensorSimulator multiplies it by nominal_value (0.01 is the wheel's
# fractional default). Rescale so the emitted sigma matches the catalog.
def _calibrate_simulator(simulator, disable_random_faults=False):
    for _label, sensor in simulator._generator._simulators.values():
        cfg = sensor.config
        update = {}
        if cfg.nominal_value and cfg.noise_std != 0.01:
            update["noise_std"] = cfg.noise_std / abs(cfg.nominal_value)
        if disable_random_faults:
            update["anomaly_probability"] = 0.0
        if update:
            sensor.config = cfg.model_copy(update=update)
`.trim();

  if (industry !== "manufacturing") {
    return `${calibrate}

SCENARIO_ENABLED = False


def _apply_scenario(payload):
    return payload


def _production_allows(payload):
    return True`;
  }

  return `${calibrate}

# Scheduled Spindle 3 thermal episodes (SIMULATED physics): most cycles drift
# past the validated limit, putting the stems cut meanwhile at risk of a
# dimensional nonconformance, often after a noisy vibration / power precursor.
# The generator below is src/iot_twin/scenario_engine/spindle_scenario.py,
# embedded verbatim; the risk model trains on the same code. Random simulator
# faults are disabled so these are the only faults the dashboard narrates.
SCENARIO_ENABLED = True

${SPINDLE_SCENARIO_PYTHON}

# Per-sample precursor noise; the per-cycle plan itself is deterministic.
_scenario_rng = random.Random()


${lotContextPython()}


def _apply_scenario(payload):
    name = str(payload.get("sensor_name") or "")
    if name.startswith(LOT_SENSOR_PREFIX):
        payload = {
            **payload,
            "metadata": {**(payload.get("metadata") or {}), **_lot_context(payload["timestamp"])},
        }
    return apply_spindle_scenario(payload, _scenario_rng)


${PRODUCTION_CONTROL_PYTHON}`;
}
