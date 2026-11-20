/**
 * Drives the admin pipeline validation flow against the real backend
 * (`routes/validation_admin.py` + `routes/pipeline_deploy.py` +
 * `routes/industries_admin.py`), replacing the prototype's local timer.
 *
 * Responsibilities:
 *  - ensure the `pipeline_setup` exists (save-draft) + run the provisioning
 *    engine before starting validation;
 *  - kick the persisted state machine (`validate/start`) and POLL
 *    `validate/state` every {@link POLL_MS} until the backend driver settles
 *    (waiting_admin / failed / complete);
 *  - map the backend flat step keys onto the frontend step ids the UI renders;
 *  - release the `cdf_manual` park, retry a failed step, gate + fire deploy,
 *    and tear the pipeline down.
 *
 * Every call degrades gracefully: without an admin session / app-backend DB the
 * endpoints 403/503, surfaced here as `error` (never a thrown crash).
 */
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import {
  backendKeyToFeStep,
  DEFAULT_MODE_RATE_HZ,
  deployPipeline,
  driverPhase,
  feStepToBackendKey,
  getCanDeploy,
  getIndustrySetup,
  getValidationState,
  listAdminPipelines,
  markCdfEnabled,
  provisionIndustry,
  retryValidationStep,
  saveSetup,
  startValidation,
  deletePipeline as apiDeletePipeline,
  updatePipelineModes,
  type CanDeployResult,
  type ClusterState,
  type ModeGate,
  type SetupPayload,
  type ValidationState,
} from "@/lib/admin-api";
import { buildIndustryDeployPayload } from "@/lib/industry-deploy-config";
import type { PipelineMode } from "@/lib/industries-api";
import type { StepStatus } from "@/pages/setup-validation/types";

export const POLL_MS = 2000;

type Phase = "idle" | "running" | "waiting_admin" | "failed" | "complete" | "error";

export interface ValidationFlow {
  /** Backend-derived per-step statuses, keyed by frontend step id. */
  statuses: Record<string, StepStatus>;
  /** Backend failure reasons, keyed by frontend step id. */
  stepErrors: Record<string, string>;
  /** True while the backend driver is actively working (poller live). */
  running: boolean;
  /** True once a validation run has been kicked off (or resumed from reload). */
  started: boolean;
  /** True when the realtime cdf_manual step is parked awaiting the admin. */
  cdfWaiting: boolean;
  /** Coarse driver phase, for headline messaging. */
  phase: Phase;
  /** Last error message from any admin call, or null. */
  error: string | null;
  /** True while a mutating action (start/retry/cdf/deploy/delete) is in flight. */
  busy: boolean;
  /** Resolved setup id, or null until save/provision/resolve completes. */
  setupId: string | null;
  /** Live interactive-cluster lifecycle (PENDING/RUNNING/TERMINATED), or null
   * until the backend reports one — drives the provisioning indicator. */
  clusterState: ClusterState | null;
  /** Interactive-cluster id once provisioned, else null. */
  clusterId: string | null;
  /** Frontend step id of the step currently executing (or the next one to run
   * when nothing is mid-flight), so the panel can highlight it. Null when
   * idle / complete. */
  currentStepId: string | null;
  /** Epoch-ms of the earliest step transition in this run (for elapsed time),
   * or null before anything has moved off `pending`. */
  runStartedAt: number | null;
  /** Aggregate deploy-gate verdict from `validate/can-deploy` (all enabled modes open). */
  canDeploy: boolean;
  /** Outstanding (not-yet-passed) backend step keys, unioned across enabled modes. */
  outstanding: string[];
  /** Per-mode deploy-gate verdicts, so the UI can enable Deploy per lane. */
  modeGates: Partial<Record<PipelineMode, ModeGate>>;
  run: () => Promise<void>;
  confirmCdf: () => Promise<void>;
  retry: (feStepId: string) => Promise<void>;
  deploy: () => Promise<{ ok: boolean; message: string }>;
  saveDraft: () => Promise<{ ok: boolean; message: string }>;
  remove: () => Promise<{ ok: boolean; message: string }>;
}

function errMsg(e: unknown): string {
  return e instanceof Error ? e.message : "Request failed";
}

function toFeStatuses(state: ValidationState | null): Record<string, StepStatus> {
  if (!state?.steps) return {};
  const out: Record<string, StepStatus> = {};
  for (const [key, rec] of Object.entries(state.steps)) {
    out[backendKeyToFeStep(key)] = rec.status as StepStatus;
  }
  return out;
}

function toFeStepErrors(state: ValidationState | null): Record<string, string> {
  if (!state?.steps) return {};
  const out: Record<string, string> = {};
  for (const [key, record] of Object.entries(state.steps)) {
    if (record.status === "failed") {
      out[backendKeyToFeStep(key)] =
        record.error?.trim() || "No failure reason was returned by the validation service.";
    }
  }
  return out;
}

interface Options {
  industry: string;
  enabledModes: PipelineMode[];
  buildPayload: () => SetupPayload;
}

export function useValidationFlow({
  industry,
  enabledModes,
  buildPayload,
}: Options): ValidationFlow {
  const [state, setState] = useState<ValidationState | null>(null);
  const [setupId, setSetupId] = useState<string | null>(null);
  const [gate, setGate] = useState<CanDeployResult | null>(null);
  const [phase, setPhase] = useState<Phase>("idle");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [polling, setPolling] = useState(false);

  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const mountedRef = useRef(true);

  const clearTimer = useCallback(() => {
    if (timerRef.current) {
      clearTimeout(timerRef.current);
      timerRef.current = null;
    }
  }, []);

  // Read a persisted state after the driver settles, and refresh the gate.
  const refreshGate = useCallback(async () => {
    try {
      const g = await getCanDeploy(industry);
      if (mountedRef.current) setGate(g);
    } catch {
      /* gate is best-effort; leave prior value */
    }
  }, [industry]);

  // One poll tick: fetch state, map phase, decide whether to keep polling.
  const tick = useCallback(async () => {
    try {
      const { validation_state } = await getValidationState(industry);
      if (!mountedRef.current) return;
      setState(validation_state);
      const dp = driverPhase(validation_state);
      if (dp === "running") {
        timerRef.current = setTimeout(tick, POLL_MS);
        return;
      }
      // Settled — stop polling, reflect terminal phase, refresh the gate.
      setPolling(false);
      setPhase(dp === "idle" ? "idle" : dp);
      void refreshGate();
    } catch (e) {
      if (!mountedRef.current) return;
      setPolling(false);
      setError(errMsg(e));
      setPhase("error");
    }
  }, [industry, refreshGate]);

  const startPolling = useCallback(() => {
    clearTimer();
    setPolling(true);
    setPhase("running");
    timerRef.current = setTimeout(tick, POLL_MS);
  }, [clearTimer, tick]);

  // Resume-on-reload: pull any persisted state + resolve the setup id once.
  useEffect(() => {
    mountedRef.current = true;
    let cancelled = false;

    (async () => {
      // Resolve the setup id from the management list (admin-gated; best-effort).
      try {
        const { pipelines } = await listAdminPipelines();
        const match = pipelines.find((p) => p.setup.industry === industry);
        if (!cancelled && match) {
          setSetupId(match.setup.id);
        }
      } catch {
        /* not admin / no DB — setup id stays null until save/provision */
      }

      try {
        const { validation_state } = await getValidationState(industry);
        if (cancelled) return;
        if (validation_state) {
          setState(validation_state);
          const dp = driverPhase(validation_state);
          setPhase(dp === "idle" ? "idle" : dp);
          void refreshGate();
          if (dp === "running") startPolling();
        }
      } catch {
        /* no setup yet / not admin — remain idle */
      }
    })();

    return () => {
      cancelled = true;
      mountedRef.current = false;
      clearTimer();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [industry]);

  // Ensure the setup exists and synchronize incremental mode additions/removals.
  const ensureSetup = useCallback(async (): Promise<{ id: string; created: boolean }> => {
    const payload = buildPayload();
    let resolvedId = setupId;
    if (!resolvedId) {
      try {
        const existing = await getIndustrySetup(industry);
        resolvedId = existing.setup.id;
        if (mountedRef.current) {
          setSetupId(resolvedId);
        }
      } catch {
        // No setup yet; saveSetup below creates the first draft.
      }
    }
    if (resolvedId && payload.enabled_modes?.length) {
      const modeUpdate = await updatePipelineModes(resolvedId, payload.enabled_modes);
      if (mountedRef.current) {
        setState(modeUpdate.setup.validation_state ?? null);
      }
    }
    const { setup, created } = await saveSetup(industry, payload);
    if (mountedRef.current) setSetupId(setup.id);
    return { id: setup.id, created };
  }, [buildPayload, industry, setupId]);

  const run = useCallback(async () => {
    setBusy(true);
    setError(null);
    try {
      const { id } = await ensureSetup();
      await provisionIndustry(industry, id);
      const { validation_state } = await startValidation(industry);
      if (!mountedRef.current) return;
      setState(validation_state);
      setGate(null);
      startPolling();
    } catch (e) {
      if (mountedRef.current) {
        setError(errMsg(e));
        setPhase("error");
      }
    } finally {
      if (mountedRef.current) setBusy(false);
    }
  }, [ensureSetup, industry, startPolling]);

  const confirmCdf = useCallback(async () => {
    setBusy(true);
    setError(null);
    try {
      const { validation_state } = await markCdfEnabled(industry, "realtime");
      if (!mountedRef.current) return;
      setState(validation_state);
      startPolling();
    } catch (e) {
      if (mountedRef.current) setError(errMsg(e));
    } finally {
      if (mountedRef.current) setBusy(false);
    }
  }, [industry, startPolling]);

  const retry = useCallback(
    async (feStepId: string) => {
      setBusy(true);
      setError(null);
      try {
        const backendKey = feStepToBackendKey(feStepId);
        const { validation_state } = await retryValidationStep(industry, backendKey);
        if (!mountedRef.current) return;
        setState(validation_state);
        startPolling();
      } catch (e) {
        if (mountedRef.current) setError(errMsg(e));
      } finally {
        if (mountedRef.current) setBusy(false);
      }
    },
    [industry, startPolling],
  );

  const deploy = useCallback(async (): Promise<{ ok: boolean; message: string }> => {
    setBusy(true);
    setError(null);
    try {
      const { id } = await ensureSetup();
      const currentSetup = await getIndustrySetup(industry);
      const currentlyDeployed = new Set(
        currentSetup.deployments
          .filter((deployment) => !!deployment.job_id)
          .map((deployment) => deployment.mode as PipelineMode),
      );
      const selectedModes = enabledModes.length
        ? enabledModes
        : (["realtime"] as PipelineMode[]);
      const modes = selectedModes.filter((mode) => !currentlyDeployed.has(mode));
      if (modes.length === 0) {
        return { ok: true, message: "Configuration updated; existing mode jobs were left untouched" };
      }
      const results = await Promise.allSettled(
        modes.map(async (mode) => {
          const payload = await buildIndustryDeployPayload(
            industry,
            mode,
            currentSetup.setup.bootstrap_servers || "",
            currentSetup.setup.mode_configs?.[mode]?.rate_hz ?? DEFAULT_MODE_RATE_HZ,
          );
          return deployPipeline(id as string, mode, payload);
        }),
      );
      const failed = results.filter((r) => r.status === "rejected");
      if (failed.length) {
        const reason = failed
          .map((r) => (r as PromiseRejectedResult).reason)
          .map(errMsg)
          .join("; ");
        setError(reason);
        return { ok: false, message: `Deploy failed: ${reason}` };
      }
      return { ok: true, message: `Deployed ${modes.length} mode(s)` };
    } catch (e) {
      const message = errMsg(e);
      if (mountedRef.current) setError(message);
      return { ok: false, message };
    } finally {
      if (mountedRef.current) setBusy(false);
    }
  }, [enabledModes, ensureSetup, industry]);

  const saveDraft = useCallback(async (): Promise<{ ok: boolean; message: string }> => {
    setBusy(true);
    setError(null);
    try {
      const { created } = await ensureSetup();
      return {
        ok: true,
        message: created ? "Saved as draft" : "Configuration updated",
      };
    } catch (e) {
      const message = errMsg(e);
      if (mountedRef.current) setError(message);
      return { ok: false, message };
    } finally {
      if (mountedRef.current) setBusy(false);
    }
  }, [ensureSetup]);

  const remove = useCallback(async (): Promise<{ ok: boolean; message: string }> => {
    setBusy(true);
    setError(null);
    try {
      let id = setupId;
      if (!id) ({ id } = await ensureSetup());
      await apiDeletePipeline(id);
      return { ok: true, message: "Pipeline deleted — resources torn down" };
    } catch (e) {
      const message = errMsg(e);
      if (mountedRef.current) setError(message);
      return { ok: false, message };
    } finally {
      if (mountedRef.current) setBusy(false);
    }
  }, [ensureSetup, setupId]);

  const statuses = useMemo(() => toFeStatuses(state), [state]);
  const stepErrors = useMemo(() => toFeStepErrors(state), [state]);
  const cdfWaiting = statuses["rt_cdf_manual"] === "waiting_admin";

  const clusterState = state?.cluster_state ?? null;
  const clusterId = state?.cluster_id ?? null;

  // The step currently executing — the first `running` step in walk order, or
  // (when nothing is mid-flight) the first not-yet-passed step, so the panel
  // can still point at "what's next". Mapped to the frontend step id.
  const currentStepId = useMemo(() => {
    if (!state?.sequence) return null;
    for (const key of state.sequence) {
      if (state.steps[key]?.status === "running") return backendKeyToFeStep(key);
    }
    for (const key of state.sequence) {
      if ((state.steps[key]?.status ?? "pending") !== "passed") {
        return backendKeyToFeStep(key);
      }
    }
    return null;
  }, [state]);

  // Earliest transition off `pending` across all steps — a stable run-start
  // anchor that survives reload (unlike a local timer), so elapsed time is
  // correct even when the poller resumes an in-flight run.
  const runStartedAt = useMemo(() => {
    if (!state?.steps) return null;
    let min: number | null = null;
    for (const rec of Object.values(state.steps)) {
      if (rec.status === "pending" || !rec.updated_at) continue;
      const t = Date.parse(rec.updated_at);
      if (!Number.isNaN(t) && (min === null || t < min)) min = t;
    }
    return min;
  }, [state]);

  return {
    statuses,
    stepErrors,
    running: polling,
    started: state !== null,
    cdfWaiting,
    phase,
    error,
    busy,
    setupId,
    clusterState,
    clusterId,
    currentStepId,
    runStartedAt,
    canDeploy: gate?.can_deploy ?? false,
    outstanding: gate?.outstanding ?? [],
    modeGates: gate?.modes ?? {},
    run,
    confirmCdf,
    retry,
    deploy,
    saveDraft,
    remove,
  };
}
