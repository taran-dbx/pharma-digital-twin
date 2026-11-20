/**
 * A single streaming-mode "flavor" card on the industry setup screen.
 *
 * Ports `renderFlavors()`/`provBlock()`/`provSteps()` from the design
 * reference (the original design reference's
 * `<script type="text/x-dc">` block) into React, driven by the real
 * `IndustryMode.status` from the backend
 * (`services/industry_service.py::get_catalog`) instead of the mockup's
 * in-memory `IND[cur].flavors` state.
 *
 * Note: the design's `progressBlock()`/`compactProgress()`/`healthDots()`
 * helpers and the `.fc-health` styling they'd feed are dead code in the
 * mockup — `renderFlavors()` never calls them, it only ever renders
 * `provBlock()` (the `.fc-prov` checklist) regardless of state. There is
 * deliberately no equivalent of those unused helpers here.
 *
 * States, resolved from the REAL per-mode runtime (see `resolveEffectiveStatus`):
 * - `draft`/`idle`/`stopped` — a single "▶ Start pipeline" button (admin);
 *   `.fc-prov` empty. The click always regenerates the consumer + producer
 *   notebooks and (re)deploys the mode's job(s) via the admin deploy endpoint
 *   (idempotent reset by job name), so code/notebook fixes always reach the
 *   running job — never a stale re-run. See `handleStart`.
 * - `starting` — no foot buttons; `.fc-prov` shows an indeterminate sweep
 *   ("provisioning…") — NOT a faked climbing percentage — while polling
 *   `onChanged()` for the real status; the bar reflects the real background
 *   state so it no longer resets to 0% on every page refresh.
 * - `running`/`degraded` — "Stop" + "Open dashboard →"; `.fc-prov` filled.
 */
import { useEffect, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import { toast } from "sonner";

import {
  startMode,
  stopMode,
  type Industry,
  type IndustryMode,
  type ModeStatus,
  type PipelineMode,
} from "@/lib/industries-api";
import {
  DEFAULT_MODE_RATE_HZ,
  deployPipeline,
  listAdminPipelines,
  startPipelineMode,
  stopPipelineMode,
  updatePipelineModeConfig,
  type ModeRuntimeStatus,
} from "@/lib/admin-api";
import { buildIndustryDeployPayload } from "@/lib/industry-deploy-config";
import { DataFlowModalContent } from "./data-flow-modal-content";
import { RuntimeHealthCheck } from "./runtime-health-check";
import { SampleModalContent } from "./sample-modal-content";
import { useTwinModal } from "./twin-modal";

const BRAND = {
  sss: "/brand/data-streaming-icon-full-color.svg",
  zerobus: "/brand/lakeflow-connect-icon-full-color.svg",
};

// Ported from MICON in the design reference.
const MODE_ICON: Record<PipelineMode, string> = {
  realtime: BRAND.sss,
  microbatch: BRAND.sss,
  zerobus: BRAND.zerobus,
};

// Ported from FNAME in the design reference.
const MODE_NAME: Record<PipelineMode, string> = {
  realtime: "Real-time mode",
  microbatch: "Micro-batch mode",
  zerobus: "Zerobus (Delta Ingest)",
};

// Ported from FTO in the design reference (the technical arrow-chain shown
// in `.fc-to` — distinct from the backend's `IndustryMode.desc`/`.to`, which
// feed the "View data flow" modal instead).
const MODE_FLOW: Record<PipelineMode, string> = {
  realtime: "Kafka → SSS → Lakebase + LakeTS",
  microbatch: "Kafka → SSS → Delta table",
  zerobus: "Zerobus SDK → Delta table",
};

// While a mode is "starting" we refetch the real per-mode status: fast at
// first (deploy is quick), then a slower cadence for the longer cluster
// warm-up, and we never hard-stop while it's still starting (a continuous
// job's cluster can take several minutes to come up before it reports
// `running`). See `handleStart`.
const POLL_FAST_MS = 3000;
const POLL_SLOW_MS = 12_000;
const STARTING_FAST_WINDOW_MS = 60_000;
const STARTING_SETTLE_GRACE_MS = 60_000;

function getErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : "Something went wrong";
}

/**
 * The card's true rendered state — resolved from the REAL per-mode runtime
 * status (`ModeRuntimeStatus` from `listAdminPipelines()`), which is
 * AUTHORITATIVE whenever it's present. The curated `IndustryMode.status` is
 * fabricated from a legacy table and can read "idle" for a mode that's
 * actually deployed-and-running, "running" for one that was never really
 * deployed, or get stranded at "starting" — so it must never override a
 * definitive real status (running/degraded/stopped/draft). Curated status is
 * only a fallback when there's no real per-mode view at all (a non-admin
 * viewer, or the admin fetch failed).
 *
 * Note the card doesn't need a curated-"starting" override for its own Start
 * click: `start_mode` is synchronous, so `handleStart` holds `busy` (button
 * disabled) for the whole call and only refetches once it returns with the
 * settled real status — there's no gap where a stale curated "starting" is
 * the only truth available.
 */
type EffectiveStatus =
  | "unconfigured"
  | "starting"
  | "running"
  | "degraded"
  | "stopped"
  | "draft"
  | "idle";

function resolveEffectiveStatus(
  curatedStatus: ModeStatus,
  realStatus: ModeRuntimeStatus | undefined,
  isConfigured: boolean | undefined,
): EffectiveStatus {
  if (isConfigured === false) return "unconfigured";
  switch (realStatus) {
    case "starting":
      return "starting";
    case "running":
      return "running";
    case "degraded":
      return "degraded";
    case "stopped":
      return "stopped";
    case "draft":
      return "draft";
    default:
      // No real per-mode data available (e.g. a non-admin viewer, or the
      // admin view failed to load) — fall back to the curated flow's own
      // status rather than guessing.
      if (curatedStatus === "starting") return "starting";
      return curatedStatus === "running" ? "running" : "idle";
  }
}

function StatusBadge({
  effective,
  isAdmin,
  runtimeDetail,
}: {
  effective: EffectiveStatus;
  isAdmin: boolean;
  runtimeDetail?: string | null;
}) {
  switch (effective) {
    case "unconfigured":
      return (
        <span className="fc-badge idle">
          <span className="dot stop" /> not configured
        </span>
      );
    case "starting":
      return (
        <span className="fc-badge starting">
          <span className="spin" /> starting
        </span>
      );
    case "running":
      return (
        <span className="fc-badge run">
          <span className="dot run" /> running
        </span>
      );
    case "degraded":
      return (
        <span
          className="fc-badge"
          title={runtimeDetail ?? "A task in this pipeline's job crashed — data may not be flowing"}
          style={{ background: "var(--db-background-warning)", color: "var(--db-warning)" }}
        >
          <span className="dot" style={{ background: "var(--db-warning)" }} /> degraded
        </span>
      );
    case "stopped":
      return (
        <span className="fc-badge idle">
          <span className="dot stop" /> stopped
        </span>
      );
    case "draft":
      return (
        <span className="fc-badge idle">
          <span className="dot stop" /> {isAdmin ? "not deployed" : "coming soon"}
        </span>
      );
    case "idle":
      return (
        <span className="fc-badge idle">
          <span className="dot stop" /> idle
        </span>
      );
    default: {
      const exhaustive: never = effective;
      return exhaustive;
    }
  }
}

/** Compact "already running & healthy" verdict for the end-user view —
 * shown once a mode is live (there's nothing to pre-flight anymore). */
function RunningHealth() {
  return (
    <div className="fc-verdict ok">
      <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth={1.6} aria-hidden="true">
        <circle cx="8" cy="8" r="6.5" />
        <path d="M5.2 8.2 7.1 10l3.7-4" strokeLinecap="round" strokeLinejoin="round" />
      </svg>
      <div className="fc-verdict-txt">
        <strong>Healthy · streaming</strong>
        <span>Live data is flowing to your dashboard.</span>
      </div>
    </div>
  );
}

/** Shown in place of `RunningHealth` when the real per-mode status is
 * `degraded` — the job is up but a task crashed, so data is NOT actually
 * flowing even though the pipeline "looks" live. Names the failure
 * (`runtime_detail`) when the backend supplies one. */
function DegradedHealth({ detail }: { detail?: string | null }) {
  return (
    <div className="fc-verdict bad">
      <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth={1.6} aria-hidden="true">
        <path d="M8 1.7 15 14H1z" strokeLinejoin="round" />
        <path d="M8 6.4v3.1" strokeLinecap="round" />
        <circle cx="8" cy="11.6" r=".5" fill="currentColor" />
      </svg>
      <div className="fc-verdict-txt">
        <strong>Degraded · data may not be flowing</strong>
        <span>{detail ?? "A task in this pipeline's job crashed. Contact your admin."}</span>
      </div>
    </div>
  );
}

function FailureStoppedHealth({ detail }: { detail?: string | null }) {
  return (
    <div className="fc-verdict bad">
      <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth={1.6} aria-hidden="true">
        <path d="M8 1.7 15 14H1z" strokeLinejoin="round" />
        <path d="M8 6.4v3.1" strokeLinecap="round" />
        <circle cx="8" cy="11.6" r=".5" fill="currentColor" />
      </svg>
      <div className="fc-verdict-txt">
        <strong>Pipeline stopped after task failure</strong>
        <span>{detail ?? "All remaining tasks were stopped. Restart when ready."}</span>
      </div>
    </div>
  );
}

export interface PipelineCardProps {
  mode: IndustryMode;
  industry: Industry;
  /** Gates the "Edit configuration →" admin affordance (see `handleEditConfig`). */
  isAdmin: boolean;
  onOpenDashboard: (mode: IndustryMode) => void;
  onChanged: () => void | Promise<unknown>;
  /**
   * Real per-mode runtime status from `listAdminPipelines()`
   * (`draft`/`running`/`degraded`/`stopped`) — the source of truth over the
   * curated `mode.status` (idle/starting/running) passed in `mode`, which is
   * fabricated from a legacy table. Undefined when no real per-mode view is
   * available (e.g. a non-admin viewer, or the admin fetch failed): the card
   * then falls back to the curated `mode.status` exactly as before.
   */
  realStatus?: ModeRuntimeStatus;
  setupId?: string;
  modeRateHz?: number;
  /** False when this mode has no successfully created deployment job. */
  isConfigured?: boolean;
  /** Why a `degraded` mode is degraded (e.g. "data_producer: driver
   * stopped"), surfaced on the badge tooltip and the degraded verdict. */
  realRuntimeDetail?: string | null;
  /** The last run failed and fail-stop cancelled every remaining task. */
  realRestartable?: boolean;
}

export function PipelineCard({
  mode,
  industry,
  isAdmin,
  onOpenDashboard,
  onChanged,
  realStatus,
  setupId,
  modeRateHz = DEFAULT_MODE_RATE_HZ,
  isConfigured,
  realRuntimeDetail,
  realRestartable = false,
}: PipelineCardProps) {
  const [busy, setBusy] = useState(false);
  const [frequencySaving, setFrequencySaving] = useState(false);
  const [rateHz, setRateHz] = useState(modeRateHz);
  const lastPersistedRateRef = useRef(modeRateHz);
  const frequencySaveRef = useRef<Promise<boolean> | null>(null);
  const startRequestedAtRef = useRef<number | null>(null);
  // Set the instant an admin fires a deploy/start so the card flips to
  // "starting" immediately, before `listAdminPipelines()` has caught up. It's
  // cleared once the REAL per-mode status settles to `running`/`degraded`.
  const [justStarted, setJustStarted] = useState(false);
  const { open: openModal } = useTwinModal();
  const navigate = useNavigate();

  // `onChanged` is a parent-owned callback that may not be memoized; keep it in
  // a ref so the poll effect below doesn't reset its timer on every render.
  const onChangedRef = useRef(onChanged);
  useEffect(() => {
    onChangedRef.current = onChanged;
  });

  // The settled status from the REAL per-mode runtime (authoritative). While
  // `justStarted` holds and that real status hasn't yet reached a live state,
  // we show "starting" — the deploy/start is in flight or the job is warming
  // up. `running`/`degraded` always win (data is/ isn't flowing for real).
  const settledStatus = resolveEffectiveStatus(mode.status, realStatus, isConfigured);
  const effective: EffectiveStatus =
    justStarted && settledStatus !== "running" && settledStatus !== "degraded"
      ? "starting"
      : settledStatus;
  const failureStopped = effective === "stopped" && realRestartable;
  const canEditFrequency = isAdmin && effective === "stopped";

  useEffect(() => {
    setRateHz(modeRateHz);
    lastPersistedRateRef.current = modeRateHz;
  }, [modeRateHz]);

  // Clear the local "just started" latch once the backend reports a real,
  // definitive live state — from here on `realStatus` alone drives the card.
  useEffect(() => {
    if (justStarted && (settledStatus === "running" || settledStatus === "degraded")) {
      setJustStarted(false);
      startRequestedAtRef.current = null;
    }
  }, [justStarted, settledStatus]);

  // While starting, poll the real per-mode status (via the parent's refetch)
  // so the card flips to "ready"/"degraded" as soon as the backend reports it.
  useEffect(() => {
    if (effective !== "starting") return;
    const startedAt = Date.now();
    let timer: ReturnType<typeof setTimeout>;
    const tick = () => {
      const requestAge = Date.now() - (startRequestedAtRef.current ?? startedAt);
      const definitivelySettled =
        realStatus === "stopped" || realStatus === "draft";
      if (justStarted && definitivelySettled && requestAge >= STARTING_SETTLE_GRACE_MS) {
        setJustStarted(false);
        startRequestedAtRef.current = null;
        return;
      }
      onChangedRef.current();
      const fast = Date.now() - startedAt < STARTING_FAST_WINDOW_MS;
      timer = setTimeout(tick, fast ? POLL_FAST_MS : POLL_SLOW_MS);
    };
    timer = setTimeout(tick, POLL_FAST_MS);
    return () => clearTimeout(timer);
  }, [effective, justStarted, realStatus]);

  /** Resolve the real pipeline setup for this industry from the admin list,
   * plus its configured Kafka bootstrap (falling back to the public MSK
   * endpoint). `null` when no setup exists yet — the caller then routes to the
   * setup/validate flow, since there's nothing to deploy against. */
  async function resolveSetup(): Promise<{
    setupId: string;
    bootstrapServers: string;
  } | null> {
    const { pipelines } = await listAdminPipelines();
    const match = pipelines.find((p) => p.setup.industry === industry.id);
    if (!match) return null;
    return {
      setupId: match.setup.id,
      // Empty string = no setup-row bootstrap; buildIndustryDeployPayload will
      // fall back to appDefaults.msk_bootstrap (MSK_BOOTSTRAP_SERVERS in app.yaml).
      bootstrapServers: match.setup.bootstrap_servers || "",
    };
  }

  function commitFrequency(): Promise<boolean> {
    if (rateHz === lastPersistedRateRef.current) return Promise.resolve(true);
    if (!setupId || !canEditFrequency) return Promise.resolve(false);
    if (frequencySaveRef.current) return frequencySaveRef.current;

    const previous = lastPersistedRateRef.current;
    setFrequencySaving(true);
    const save = updatePipelineModeConfig(setupId, mode.id, rateHz)
      .then(() => {
        lastPersistedRateRef.current = rateHz;
        toast.success(`${mode.name} frequency saved for the next start`);
        onChanged();
        return true;
      })
      .catch((error: unknown) => {
        setRateHz(previous);
        toast.error(`Couldn't update ${mode.name} frequency: ${getErrorMessage(error)}`);
        return false;
      })
      .finally(() => {
        setFrequencySaving(false);
        frequencySaveRef.current = null;
      });
    frequencySaveRef.current = save;
    return save;
  }

  /**
   * One-click start for this mode.
   *
   * Non-admin: the curated per-mode start (`startMode`) — a pre-flighted run of
   * an already-deployed pipeline (admin deploy endpoints 403 for non-admins).
   *
   * Admin:
   *   - never deployed (`draft`, incl. the ambiguous no-real-status fallback):
   *     generate the mode's consumer + producer notebooks and DEPLOY them
   *     (`deployPipeline`, gate overridden). realtime/microbatch deploy as an
   *     UNPAUSED continuous job that self-starts; zerobus is a run-once
   *     serverless job, so we trigger a run with `startPipelineMode`.
   *   - deployed but stopped: just (re)run the existing job — never redeploy.
   * Either way we latch `justStarted` so the card shows "starting" and polls
   * until the backend reports the mode `running` (data flowing) or `degraded`.
   */
  async function handleStart() {
    setBusy(true);
    try {
      if (!isAdmin) {
        await startMode(industry.id, mode.id);
        startRequestedAtRef.current = Date.now();
        setJustStarted(true);
        return;
      }

      if (canEditFrequency && !(await commitFrequency())) return;
      const target = await resolveSetup();
      if (!target) {
        // No setup to deploy against — send the admin to the setup/validate flow.
        handleSetupDeploy();
        return;
      }

      // ALWAYS regenerate the notebooks and (re)deploy the job(s) on Start —
      // `deployPipeline` resets the job idempotently (by name) with the latest
      // generated notebook. This is what makes notebook/config FIXES actually
      // reach an already-deployed job: a plain re-run would keep running the
      // previously-uploaded (possibly buggy) notebook. `_create_or_reset_*_job`
      // is idempotent, so this is safe for both first deploy and redeploy.
      const payload = await buildIndustryDeployPayload(
        industry.id,
        mode.id,
        target.bootstrapServers,
        rateHz,
      );
      await deployPipeline(target.setupId, mode.id, payload);
      // realtime/microbatch self-start (continuous, UNPAUSED); zerobus's
      // run-once serverless job needs an explicit run trigger.
      if (mode.id === "zerobus") {
        await startPipelineMode(target.setupId, mode.id);
      }
      startRequestedAtRef.current = Date.now();
      setJustStarted(true);
    } catch (error: unknown) {
      toast.error(`Couldn't start ${mode.name}: ${getErrorMessage(error)}`);
    } finally {
      setBusy(false);
      onChanged();
    }
  }

  async function handleStop() {
    setBusy(true);
    try {
      if (isAdmin) {
        const resolvedSetupId = setupId ?? (await resolveSetup())?.setupId;
        if (!resolvedSetupId) {
          throw new Error(`No pipeline setup exists for ${industry.name}`);
        }
        await stopPipelineMode(resolvedSetupId, mode.id);
      } else {
        await stopMode(industry.id, mode.id);
      }
    } catch (error: unknown) {
      toast.error(`Couldn't stop ${mode.name}: ${getErrorMessage(error)}`);
    } finally {
      await onChanged();
      setBusy(false);
    }
  }

  /**
   * Route every setup/settings action through the industry-first flow. Legacy
   * per-pipeline detail pages are intentionally unreachable, even when the
   * catalog still carries a reconciled legacy `pipelineId`.
   */
  function handleSetupDeploy() {
    navigate(`/setup-validate?industry=${encodeURIComponent(industry.id)}`);
  }

  const cardClass = [
    "fcard",
    effective === "running" ? "on" : "",
    effective === "unconfigured" ? "unconfigured" : "",
  ]
    .filter(Boolean)
    .join(" ");

  return (
    <div className={cardClass}>
      <div className="fc-head">
        <img className="fc-ic" src={MODE_ICON[mode.id]} alt="" />
        <div className="fc-hd-txt">
          <div className="fc-name">{MODE_NAME[mode.id]}</div>
          {mode.id === "realtime" && <div className="fc-sub">◷ Time-series · LakeTS</div>}
        </div>
        <StatusBadge effective={effective} isAdmin={isAdmin} runtimeDetail={realRuntimeDetail} />
        {isAdmin && (
          <button
            className="fc-info"
            title="Configure & deploy this industry pipeline"
            aria-label="Pipeline settings"
            onClick={handleSetupDeploy}
          >
            <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth={1.3} aria-hidden="true">
              <circle cx="8" cy="8" r="2.1" />
              <path d="M8 1.4v1.7M8 12.9v1.7M1.4 8h1.7M12.9 8h1.7M3.3 3.3l1.2 1.2M11.5 11.5l1.2 1.2M12.7 3.3l-1.2 1.2M4.5 11.5l-1.2 1.2" />
            </svg>
          </button>
        )}
        <button
          className="fc-info"
          title="View data flow"
          onClick={() => openModal(`${industry.name} · ${mode.name} data flow`, <DataFlowModalContent industry={industry} mode={mode} />)}
        >
          <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth={1.4} aria-hidden="true">
            <circle cx="4" cy="8" r="1.6" />
            <circle cx="12" cy="4" r="1.6" />
            <circle cx="12" cy="12" r="1.6" />
            <path d="M5.5 7.2 10.5 4.6M5.5 8.8l5 2.6" />
          </svg>
        </button>
      </div>
      <div className="fc-to">{MODE_FLOW[mode.id]}</div>
      <div className="fc-frequency">
        <label htmlFor={`frequency-${industry.id}-${mode.id}`}>
          <span>Data frequency</span>
          <strong>{rateHz.toLocaleString()} rec/s</strong>
        </label>
        <div className="fc-frequency-control">
          <input
            id={`frequency-${industry.id}-${mode.id}`}
            type="range"
            aria-label={`${mode.name} data frequency`}
            min={100}
            max={5000}
            step={100}
            value={rateHz}
            disabled={!canEditFrequency || frequencySaving}
            onChange={(event) => setRateHz(Number(event.target.value))}
            onMouseUp={() => void commitFrequency()}
            onTouchEnd={() => void commitFrequency()}
            onKeyUp={() => void commitFrequency()}
          />
          <button
            className="btn ghost sm"
            onClick={() =>
              openModal(
                `${industry.name} · ${mode.name} sample record`,
                <SampleModalContent
                  industryId={industry.id}
                  industryName={industry.name}
                  hz={rateHz}
                  mode={mode.id}
                />,
              )
            }
          >
            View sample →
          </button>
        </div>
        {!canEditFrequency && (
          <span className="fc-frequency-note">
            {effective === "running" || effective === "starting" || effective === "degraded"
              ? "Stop this pipeline to change its frequency"
              : isAdmin
                ? "Set frequency in pipeline configuration"
                : "Frequency is managed by your admin"}
          </span>
        )}
      </div>
      {mode.id !== "zerobus" && (
        <div className="fc-topic">
          Topic <b>iot.{industry.id}.{mode.id}</b>
        </div>
      )}

      {/* Streaming metrics are always on by admin default — surfaced as a
          read-only status, never a user-facing on/off toggle. Zerobus has
          no Spark job, so no streaming metrics apply. */}
      {mode.id !== "zerobus" && (
        <div className="fc-metrics ro">
          <span className="metrics-pill">
            <span className="dot run" aria-hidden="true" />
            Streaming metrics: on
          </span>
        </div>
      )}

      {isAdmin ? (
        /* Admin: manual start/stop controls, driven by the REAL effective
           status — not the curated `mode.status`, which can read "idle" for
           a mode that's actually deployed-and-running via the admin flow. The
           per-resource provisioning checklist lives on the Resource-Health
           view now, so it's intentionally not duplicated on the card. */
        <>
          <div className="fc-spacer" />
          {effective === "unconfigured" ? (
            <div className="fc-foot">
              <button className="btn primary sm" onClick={handleSetupDeploy}>
                Configure &amp; deploy →
              </button>
            </div>
          ) : failureStopped ? (
            <>
              <FailureStoppedHealth detail={realRuntimeDetail} />
              <div className="fc-foot">
                <button
                  className="btn primary sm"
                  disabled={busy || frequencySaving}
                  onClick={handleStart}
                >
                  {busy ? "Restarting…" : "↻ Restart pipeline"}
                </button>
              </div>
            </>
          ) : effective === "starting" ? (
            /* A starting mode still needs a Stop — a job whose cluster is
               warming up (or a run that's stuck failing to reach `running`)
               must be cancellable from the card, not only via the API. */
            <div className="fc-foot">
              <button className="btn ghost sm" disabled={busy} onClick={handleStop}>
                Stop
              </button>
            </div>
          ) : effective === "running" || effective === "degraded" ? (
            <div className="fc-foot">
              <button className="btn ghost sm" disabled={busy} onClick={handleStop}>
                Stop
              </button>
              <button className="btn primary sm" onClick={() => onOpenDashboard(mode)}>
                Open dashboard →
              </button>
            </div>
          ) : (
            /* Undeployed (`draft`) OR deployed-but-stopped: one button, one
               click. `handleStart` DEPLOYS this mode's job(s) when nothing is
               deployed yet — it generates the consumer + producer notebooks and
               calls the admin deploy endpoint (the backend can't produce those
               notebooks on its own) — or simply (re)runs the existing job when
               one is already deployed but idle. */
            <div className="fc-foot">
              <button
                className="btn primary sm"
                disabled={busy || frequencySaving}
                onClick={handleStart}
              >
                ▶ Start pipeline
              </button>
            </div>
          )}
        </>
      ) : failureStopped ? (
        <>
          <FailureStoppedHealth detail={realRuntimeDetail} />
          <div className="fc-spacer" />
          <div className="fc-foot">
            <button className="btn primary sm" disabled={busy} onClick={handleStart}>
              {busy ? "Restarting…" : "↻ Restart pipeline"}
            </button>
          </div>
        </>
      ) : effective === "running" || effective === "degraded" ? (
        /* End user, genuinely live (running or degraded): a compact verdict
           — healthy or "data may not be flowing" — plus operate controls.
           A degraded mode's job is still deployed, so the dashboard (even
           if showing stale data) stays reachable. */
        <>
          {effective === "degraded" ? <DegradedHealth detail={realRuntimeDetail} /> : <RunningHealth />}
          <div className="fc-spacer" />
          <div className="fc-foot">
            <button className="btn ghost sm" disabled={busy} onClick={handleStop}>
              Stop
            </button>
            <button className="btn primary sm" onClick={() => onOpenDashboard(mode)}>
              Open dashboard →
            </button>
          </div>
        </>
      ) : effective === "draft" ? (
        /* End user, not deployed at all: no job exists for this mode, so
           there's nothing to start or open — never offer a live dashboard
           for a mode with no job. */
        <div className="fc-verdict" style={{ borderColor: "var(--db-border)" }}>
          <div className="fc-verdict-txt">
            <strong>Coming soon</strong>
            <span>Your admin hasn&apos;t deployed this pipeline mode yet.</span>
          </div>
        </div>
      ) : (
        /* End user, idle/starting/stopped: lightweight runtime health gate.
           Start runs the checks as a pre-flight and only proceeds if all pass. */
        <RuntimeHealthCheck
          mode={mode.id}
          starting={busy}
          modeStarting={effective === "starting"}
          onStart={handleStart}
        />
      )}
    </div>
  );
}
