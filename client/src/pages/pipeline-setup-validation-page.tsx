/**
 * Admin "Classic Pipeline Creation & Validation" page — implements Section 1
 * of docs/superpowers/specs/2026-07-05-admin-pipeline-setup-design.md as a
 * multi-step stepper that reuses the pipeline wizard's layout (stepper +
 * left content / right Configuration Summary + Architect's Tip) and the
 * DuBois product brand marks.
 *
 * Wired to the real admin API (Phase 6): the flavor/industry + source steps
 * persist a `pipeline_setup` draft; entering the Validation step provisions the
 * industry and starts the persisted state machine, which this page then POLLS
 * (via `useValidationFlow`) to render live per-step status. Retry / CDF-confirm
 * / Deploy / Save-draft / Delete all hit the backend and degrade gracefully
 * (403/503 surface as an inline error, never a crash).
 *
 * Route: /setup-validate (+ optional ?industry=<id>&step=<n>), wired in App.tsx
 * inside the industry-first TwinShell.
 */
import { useEffect, useRef, useState } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import { toast } from "sonner";
import { AlertCircle, ArrowLeft, ArrowRight, Rocket, Save, Trash2 } from "lucide-react";

import { Button } from "@/components/ui/button";
import { ConfirmDialog } from "@/components/confirm-dialog";
import { Stepper } from "@/components/stepper";
import { useValidationFlow } from "@/hooks/use-validation-flow";
import { api } from "@/lib/api";
import {
  backendKeyToFeStep,
  DEFAULT_MODE_RATE_HZ,
  listAdminPipelines,
  type ModeConfigs,
  type ModeRuntimeStatus,
  type SetupPayload,
} from "@/lib/admin-api";
import type { PipelineMode } from "@/lib/industries-api";

import { BuiltOnRow, Eyebrow } from "./setup-validation/components";
import { SummaryCard } from "./setup-validation/summary-card";
import { StepFlavorIndustry } from "./setup-validation/step-flavor-industry";
import { StepSource } from "./setup-validation/step-source";
import { StepPreview } from "./setup-validation/step-preview";
import { StepValidation } from "./setup-validation/step-validation";
import { StepMetrics } from "./setup-validation/step-metrics";
import { StepNotebook } from "./setup-validation/step-notebook";
import {
  BRAND,
  formatModes,
  hasKafkaModes,
  INDUSTRIES,
  STEP_LABEL,
  STEP_LABELS,
  type Flavor,
  type SourceType,
} from "./setup-validation/types";

type StepId = "flavor" | "source" | "preview" | "validation" | "metrics" | "notebook";

function tipFor(stepId: StepId, enabledModes: PipelineMode[]): string {
  const zerobusOnly = enabledModes.length === 1 && enabledModes[0] === "zerobus";
  if (stepId === "flavor" && zerobusOnly) {
    return "Zerobus is a serverless Delta Ingest path. It authenticates with the app service principal and writes directly to the managed Delta target — no Kafka source or Spark cluster required.";
  }
  if (stepId === "flavor") {
    return "Classic runs the selected Kafka consumers on a job cluster. The interactive validation harness mirrors that deploy-time compute; Zerobus remains serverless when selected alongside them.";
  }
  if (stepId === "source") {
    return "You only provide bootstrap_servers. Region, MSK ARN, replication factor and the UC service credential are derived in code — fewer inputs, no drift.";
  }
  return `This view is scoped to ${formatModes(enabledModes)}. Only resources required by those modes are shown and validated.`;
}

function sameModes(left: PipelineMode[], right: PipelineMode[]): boolean {
  return left.length === right.length && left.every((mode) => right.includes(mode));
}

export function PipelineSetupValidationPage() {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();

  const initialStep = (() => {
    const raw = Number(searchParams.get("step"));
    return Number.isInteger(raw) && raw >= 0 && raw < STEP_LABELS.length ? raw : 0;
  })();
  const initialIndustry = searchParams.get("industry") ?? "pharmaceutical";

  const [step, setStep] = useState(initialStep);
  const [flavor, setFlavor] = useState<Flavor>("classic");
  const [industryId, setIndustryId] = useState(initialIndustry);
  const [sourceType, setSourceType] = useState<SourceType>("kafka");
  const [bootstrap, setBootstrap] = useState<string>(
    "b-1.example.abc123.c9.kafka.us-east-1.amazonaws.com:9098",
  );
  const [enabledModes, setEnabledModes] = useState<PipelineMode[]>(["realtime", "microbatch", "zerobus"]);
  const [modeConfigs, setModeConfigs] = useState<ModeConfigs>({
    realtime: { rate_hz: DEFAULT_MODE_RATE_HZ },
    microbatch: { rate_hz: DEFAULT_MODE_RATE_HZ },
    zerobus: { rate_hz: DEFAULT_MODE_RATE_HZ },
  });
  const [lockedFrequencyModes, setLockedFrequencyModes] = useState<Set<PipelineMode>>(
    new Set(),
  );
  const [persistedModes, setPersistedModes] = useState<PipelineMode[]>([]);
  const [warehouseId, setWarehouseId] = useState("");
  const [setupHydrated, setSetupHydrated] = useState(false);
  const [showDelete, setShowDelete] = useState(false);

  // Derived: Source step (Kafka/Kinesis bootstrap) is only relevant when
  // realtime or microbatch is enabled. Zerobus uses direct SDK ingest — its
  // endpoint comes from app-defaults config, not user input.
  const needsSourceStep = hasKafkaModes(enabledModes);

  const activeSteps: { id: StepId; label: string }[] = [
    { id: "flavor", label: "Flavor & Industry" },
    ...(needsSourceStep ? [{ id: "source" as StepId, label: "Source" }] : []),
    { id: "preview", label: "Setup Preview" },
    { id: "validation", label: "Validation" },
    { id: "metrics", label: "Metrics" },
    { id: "notebook", label: "Notebook" },
  ];
  const visibleStepLabels = activeSteps.map((s) => s.label);
  const currentStepId: StepId = activeSteps[step]?.id ?? "flavor";
  const validationStepIdx = activeSteps.findIndex((s) => s.id === "validation");
  const modesChanged = !sameModes(enabledModes, persistedModes);
  const modeRunKey = `${industryId}:${[...enabledModes].sort().join(",")}`;
  // Industries that already have a setup — one-per-industry uniqueness rule
  // (§1.2). Derived from the real admin pipeline list; `null` until resolved,
  // in which case Step 1 falls back to the static `exists` flags. 403/503
  // (not admin / no backend) leaves it null — the fallback still applies.
  const [existingIds, setExistingIds] = useState<Set<string> | null>(null);

  useEffect(() => {
    let cancelled = false;
    Promise.all([
      listAdminPipelines().catch(() => null),
      api.getAppDefaults().catch(() => null),
    ]).then(([pipelineResult, appDefaults]) => {
      if (cancelled) return;
      const pipelines = pipelineResult?.pipelines ?? [];
      if (pipelineResult) {
        setExistingIds(new Set(pipelines.map((p) => p.setup.industry)));
      }
      const existing = pipelines.find((pipeline) => pipeline.setup.industry === initialIndustry);
      setWarehouseId(existing?.setup.warehouse_id || appDefaults?.warehouse_id || "");
      setSetupHydrated(true);
      if (!existing) return;
      const setup = existing.setup;
      const activeStatuses = new Set<ModeRuntimeStatus>([
        "running",
        "degraded",
        "starting",
      ]);
      setLockedFrequencyModes(
        new Set(
          (existing.modes ?? [])
            .filter((mode) => activeStatuses.has(mode.status))
            .map((mode) => mode.mode as PipelineMode),
        ),
      );
      if (setup.flavor === "classic" || setup.flavor === "serverless") {
        setFlavor(setup.flavor);
      }
      if (setup.source_type === "kafka" || setup.source_type === "kinesis") {
        setSourceType(setup.source_type);
      }
      if (setup.bootstrap_servers) setBootstrap(setup.bootstrap_servers);
      if (setup.enabled_modes?.length) {
        setEnabledModes(setup.enabled_modes);
        setPersistedModes(setup.enabled_modes);
      }
      if (setup.mode_configs) {
        setModeConfigs((current) => ({ ...current, ...setup.mode_configs }));
      }
    });
    return () => {
      cancelled = true;
    };
  }, []);

  const industry = INDUSTRIES.find((i) => i.id === industryId) ?? INDUSTRIES[1];
  const partitions = 6;

  // Build the setup-draft payload from the current form state.
  const buildPayload = useRef<() => SetupPayload>(() => ({}));
  buildPayload.current = () => ({
    flavor,
    // Only include Kafka/Kinesis source config when a Kafka-based mode is active.
    // Zerobus-only setups derive their endpoint from app-defaults — no user input.
    ...(needsSourceStep
      ? { source_type: sourceType, bootstrap_servers: bootstrap }
      : {}),
    ...(warehouseId ? { warehouse_id: warehouseId } : {}),
    enabled_modes: enabledModes,
    mode_configs: Object.fromEntries(
      enabledModes.map((mode) => [
        mode,
        modeConfigs[mode] ?? { rate_hz: DEFAULT_MODE_RATE_HZ },
      ]),
    ) as ModeConfigs,
  });

  const flow = useValidationFlow({
    industry: industryId,
    enabledModes,
    buildPayload: () => buildPayload.current(),
  });

  // Auto-provision + start the state machine the first time the admin lands on
  // the Validation step (spec §Phase 6). Guarded so it fires once per industry.
  const autoRanFor = useRef<string | null>(null);
  useEffect(() => {
    if (!setupHydrated) return;
    if (step !== validationStepIdx) return;
    if ((flow.started && !modesChanged) || flow.busy) return;
    if (autoRanFor.current === modeRunKey) return;
    autoRanFor.current = modeRunKey;
    setPersistedModes(enabledModes);
    void flow.run();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    setupHydrated,
    step,
    industryId,
    flow.started,
    flow.busy,
    modesChanged,
    enabledModes,
    modeRunKey,
  ]);

  // Explicit re-run entry point: the health page's "Re-run validation" routes
  // here with `?step=3&run=1`, which kicks a fresh run (re-provision + restart
  // the state machine) so the admin lands directly on the live progress view —
  // even when a prior run has already settled (`flow.started === true`).
  const forceRun = searchParams.get("run") === "1";
  const forceRanFor = useRef<string | null>(null);
  useEffect(() => {
    if (!setupHydrated || !forceRun || flow.busy) return;
    if (forceRanFor.current === industryId) return;
    forceRanFor.current = industryId;
    autoRanFor.current = modeRunKey; // suppress the plain auto-run for this selection
    setStep(validationStepIdx);
    void flow.run();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    setupHydrated,
    forceRun,
    industryId,
    flow.busy,
    modeRunKey,
    validationStepIdx,
  ]);

  const showSummary = currentStepId === "flavor" || currentStepId === "source";
  const isLast = step === activeSteps.length - 1;
  const builtOn = [
    ...(needsSourceStep ? [BRAND.streaming] : []),
    ...(enabledModes.includes("zerobus") ? [BRAND.zerobus] : []),
    ...(enabledModes.includes("realtime") ? [BRAND.lakebase] : []),
    BRAND.unity,
  ];

  const allPassed = flow.canDeploy;
  const outstandingLabels = flow.outstanding.map(
    (key) => STEP_LABEL[backendKeyToFeStep(key)] ?? key,
  );
  const outstandingSummary =
    outstandingLabels.slice(0, 3).join(", ") + (outstandingLabels.length > 3 ? "…" : "");

  async function handleDeploy() {
    if (!allPassed) return;
    const res = await flow.deploy();
    if (res.ok) {
      toast.success(res.message);
      navigate("/pipelines");
    } else {
      toast.error(res.message);
    }
  }

  async function handleSaveDraft() {
    if (!setupHydrated) return;
    const res = await flow.saveDraft();
    if (res.ok) {
      setPersistedModes(enabledModes);
      toast.success(`${res.message} — resume anytime from the pipelines list`);
    }
    else toast.error(res.message);
  }

  async function handleDelete() {
    setShowDelete(false);
    const res = await flow.remove();
    if (res.ok) {
      toast.success(res.message);
      navigate("/industries");
    } else {
      toast.error(res.message);
    }
  }

  function handleRun() {
    if (!setupHydrated) return;
    setPersistedModes(enabledModes);
    void flow.run();
  }

  return (
    <div className="mx-auto max-w-5xl space-y-6 pb-16">
      <div className="space-y-3">
        <div className="flex items-start justify-between gap-4">
          <div>
            <Eyebrow>Admin · Pipeline Setup</Eyebrow>
            <h1 className="mt-1 font-display text-xl font-semibold tracking-tight">
              Pipeline Creation &amp; Validation
            </h1>
          </div>
          <Button
            variant="ghost"
            size="sm"
            className="gap-1.5 text-destructive hover:bg-destructive/10 hover:text-destructive"
            onClick={() => setShowDelete(true)}
            disabled={flow.busy}
          >
            <Trash2 className="h-3.5 w-3.5" />
            Delete pipeline
          </Button>
        </div>
        <BuiltOnRow items={builtOn} />
      </div>

      {flow.error && (
        <div className="flex items-start gap-2 rounded-lg border border-destructive/40 bg-destructive/[0.06] p-3 text-xs text-destructive">
          <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
          <span>{flow.error}</span>
        </div>
      )}

      <Stepper steps={visibleStepLabels} currentStep={step} />

      <div className={showSummary ? "flex min-w-0 flex-col items-stretch gap-6 lg:flex-row lg:items-start" : ""}>
        <div className={showSummary ? "min-w-0 flex-[3] space-y-6" : ""}>
          {currentStepId === "flavor" && (
            <StepFlavorIndustry
              flavor={flavor}
              onFlavorChange={setFlavor}
              industryId={industryId}
              onIndustryChange={setIndustryId}
              enabledModes={enabledModes}
              onEnabledModesChange={setEnabledModes}
              modeConfigs={modeConfigs}
              lockedFrequencyModes={lockedFrequencyModes}
              onModeRateChange={(mode, rateHz) =>
                setModeConfigs((current) => ({
                  ...current,
                  [mode]: { rate_hz: rateHz },
                }))
              }
              existingIds={existingIds}
              onEditExisting={(id) => navigate(`/i/${id}`)}
            />
          )}
          {currentStepId === "source" && (
            <StepSource
              sourceType={sourceType}
              onSourceTypeChange={setSourceType}
              bootstrap={bootstrap}
              onBootstrapChange={setBootstrap}
              partitions={partitions}
            />
          )}
          {currentStepId === "preview" && (
            <StepPreview
              industry={industry}
              partitions={partitions}
              enabledModes={enabledModes}
            />
          )}
          {currentStepId === "validation" && (
            <StepValidation
              statuses={flow.statuses}
              stepErrors={flow.stepErrors}
              running={flow.running || flow.busy}
              started={flow.started}
              onRun={handleRun}
              onConfirmCdf={() => void flow.confirmCdf()}
              onRetry={(id) => void flow.retry(id)}
              clusterState={flow.clusterState}
              clusterId={flow.clusterId}
              currentStepId={flow.currentStepId}
              runStartedAt={flow.runStartedAt}
              enabledModes={enabledModes}
            />
          )}
          {currentStepId === "metrics" && <StepMetrics enabledModes={enabledModes} />}
          {currentStepId === "notebook" && <StepNotebook enabledModes={enabledModes} />}
        </div>

        {showSummary && (
          <div className="min-w-0 flex-[2] lg:sticky lg:top-6">
            <SummaryCard
              stepId={currentStepId}
              flavor={flavor}
              industry={industry}
              partitions={partitions}
              enabledModes={enabledModes}
              tip={tipFor(currentStepId, enabledModes)}
            />
          </div>
        )}
      </div>

      <div className="flex flex-wrap items-center justify-between gap-2">
        <Button
          variant="outline"
          className="gap-2"
          onClick={() => (step === 0 ? navigate("/industries") : setStep((s) => s - 1))}
        >
          <ArrowLeft className="h-4 w-4" />
          {step === 0 ? "Industries" : "Back"}
        </Button>
        {/* Save-as-draft is available at every step so an admin can stop and
            resume a partial config later. */}
        <Button
          variant="ghost"
          className="gap-2"
          onClick={handleSaveDraft}
          disabled={flow.busy || !setupHydrated}
        >
          <Save className="h-4 w-4" />
          Save configuration
        </Button>
        {isLast ? (
          <div className="flex items-center gap-3">
            {!allPassed && outstandingLabels.length > 0 && (
              <span className="hidden text-xs text-muted-foreground sm:inline">
                {outstandingLabels.length} step{outstandingLabels.length === 1 ? "" : "s"} remaining:{" "}
                {outstandingSummary}
              </span>
            )}
            <Button
              className="gap-2 hover:glow-primary transition-all"
              onClick={handleDeploy}
              disabled={!allPassed || flow.busy}
            >
              <Rocket className="h-4 w-4" />
              Deploy
            </Button>
          </div>
        ) : (
          <Button
            className="gap-2 hover:glow-primary transition-all"
            onClick={() => setStep((s) => s + 1)}
          >
            Next: {visibleStepLabels[step + 1]}
            <ArrowRight className="h-4 w-4" />
          </Button>
        )}
      </div>

      <ConfirmDialog
        open={showDelete}
        title="Delete pipeline"
        description={`This permanently deletes the "${industry.name}" pipeline and tears down all of its resources in reverse provisioning order — jobs, Kafka topics, LakeTS ChronoTable + CDF sync, Lakebase endpoints/branch, and Unity Catalog objects. This cannot be undone.`}
        confirmLabel="Delete pipeline"
        destructive
        onConfirm={handleDelete}
        onCancel={() => setShowDelete(false)}
      />
    </div>
  );
}
