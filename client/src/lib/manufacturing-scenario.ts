/**
 * Scheduled Spindle 3 heat event for the manufacturing realtime producer.
 *
 * The producer notebook applies this schedule to `cnc_1_spindle_temp` on the
 * wall clock, so every consumer (Lakebase, SSE, alert engine) sees the same
 * device readings. The dashboard reads the same constants to show when the
 * next prepared event fires; it never synthesizes readings itself.
 */

export const SPINDLE_HEAT_SCENARIO = {
  id: "spindle-3-heat",
  sensorName: "cnc_1_spindle_temp",
  cycleSeconds: 300,
  normalSeconds: 210,
  rampSeconds: 30,
  holdSeconds: 30,
  peakDeltaC: 25,
} as const;

export type SpindleHeatPhase = "normal" | "heating" | "overheat" | "cooling";

export interface SpindleHeatState {
  phase: SpindleHeatPhase;
  /** Seconds until the next heat event starts (0 while one is in progress). */
  secondsToNextEvent: number;
  /** Seconds until the current phase ends. */
  secondsLeftInPhase: number;
}

const COOL_SECONDS =
  SPINDLE_HEAT_SCENARIO.cycleSeconds -
  SPINDLE_HEAT_SCENARIO.normalSeconds -
  SPINDLE_HEAT_SCENARIO.rampSeconds -
  SPINDLE_HEAT_SCENARIO.holdSeconds;

export function spindleHeatStateAt(epochSeconds: number): SpindleHeatState {
  const { cycleSeconds, normalSeconds, rampSeconds, holdSeconds } = SPINDLE_HEAT_SCENARIO;
  const t = ((epochSeconds % cycleSeconds) + cycleSeconds) % cycleSeconds;
  if (t < normalSeconds) {
    const left = normalSeconds - t;
    return { phase: "normal", secondsToNextEvent: left, secondsLeftInPhase: left };
  }
  const heatingEnd = normalSeconds + rampSeconds;
  if (t < heatingEnd) {
    return { phase: "heating", secondsToNextEvent: 0, secondsLeftInPhase: heatingEnd - t };
  }
  const overheatEnd = heatingEnd + holdSeconds;
  if (t < overheatEnd) {
    return { phase: "overheat", secondsToNextEvent: 0, secondsLeftInPhase: overheatEnd - t };
  }
  const left = overheatEnd + COOL_SECONDS - t;
  return { phase: "cooling", secondsToNextEvent: 0, secondsLeftInPhase: left };
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

  const s = SPINDLE_HEAT_SCENARIO;
  return `${calibrate}

# Scheduled Spindle 3 heat event on the wall clock. Random simulator faults are
# disabled so this is the only fault the dashboard narrates.
SCENARIO_ENABLED = True
SCENARIO_ID = "${s.id}"
SCENARIO_SENSOR = "${s.sensorName}"
SCENARIO_CYCLE_S = ${s.cycleSeconds}
SCENARIO_NORMAL_S = ${s.normalSeconds}
SCENARIO_RAMP_S = ${s.rampSeconds}
SCENARIO_HOLD_S = ${s.holdSeconds}
SCENARIO_COOL_S = ${COOL_SECONDS}
SCENARIO_PEAK_DELTA = ${s.peakDeltaC.toFixed(1)}


def _scenario_phase(now):
    t = now % SCENARIO_CYCLE_S
    if t < SCENARIO_NORMAL_S:
        return "normal", 0.0
    t -= SCENARIO_NORMAL_S
    if t < SCENARIO_RAMP_S:
        return "heating", SCENARIO_PEAK_DELTA * t / SCENARIO_RAMP_S
    t -= SCENARIO_RAMP_S
    if t < SCENARIO_HOLD_S:
        return "overheat", SCENARIO_PEAK_DELTA
    t -= SCENARIO_HOLD_S
    return "cooling", SCENARIO_PEAK_DELTA * max(0.0, 1.0 - t / SCENARIO_COOL_S)


def _apply_scenario(payload):
    if payload.get("sensor_name") != SCENARIO_SENSOR:
        return payload
    phase, delta = _scenario_phase(payload["timestamp"])
    value = payload["value"] + delta
    ceiling = payload.get("max_value")
    payload["value"] = min(ceiling, value) if ceiling is not None else value
    payload["fault_active"] = phase != "normal"
    payload["metadata"] = {
        **(payload.get("metadata") or {}),
        "scenario_id": SCENARIO_ID,
        "phase": phase,
    }
    return payload


${PRODUCTION_CONTROL_PYTHON}`;
}
