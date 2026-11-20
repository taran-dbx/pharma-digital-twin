import { useState, useEffect, useRef, useCallback } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import { toast } from "sonner";
import {
  ArrowLeft,
  ArrowRight,
  Check,
  Rocket,
  CheckCircle2,
  Info,
  Save,
  Loader2,
  ExternalLink,
  LayoutDashboard,
  Database,
  Square,
  Play,
  RefreshCw,
  AlertCircle,
  Activity,
  Radio,
  Lightbulb,
} from "lucide-react";
import type { PipelineStatus, PipelineType, FrequencyMode, PipelineResources, SssSource, SssProcessingMode, SssRtmSinkType, DataProducerState, CatalogIsolationMode, TieringConfig } from "@/lib/types";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Stepper } from "@/components/stepper";
import { usePipelines } from "@/contexts/pipeline-context";
import { api } from "@/lib/api";
// Data producer notebook generation removed — producer runs as Task 2 of the deployed job
import { DataHubStep } from "@/pages/steps/data-hub-step";
import { SourceConfigStep } from "@/pages/steps/source-config-step";
import { SinkConfigStep } from "@/pages/steps/sink-config-step";
import { ArchitectureCanvas } from "@/components/architecture-canvas";

const SOURCE_LABELS: Record<SssSource, string> = {
  kafka: "Kafka",
  kinesis: "Kinesis",
  event_hub: "Event Hub",
  rabbitmq: "RabbitMQ",
  pubsub: "Pub/Sub",
  zerobus: "Zerobus",
};

const STEP_LABELS = ["Data Hub", "Pipeline Config", "Sink Config", "Deploy & Monitor"];

type JobRunState = "idle" | "polling" | "running" | "stopped" | "error";

export function PipelineWizardPage() {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const { pipelines, createPipeline, updatePipeline } = usePipelines();

  const [step, setStep] = useState(0);
  const [pipelineId, setPipelineId] = useState<string | null>(
    searchParams.get("id"),
  );
  const [pipelineName, setPipelineName] = useState("");
  const [industry, setIndustry] = useState<string>("pharmaceutical");
  const [sensorConfig, setSensorConfig] = useState<Record<string, unknown>[]>(
    [],
  );
  const [updateRateHz, setUpdateRateHz] = useState(2.0);
  const [frequencyMode, setFrequencyMode] = useState<FrequencyMode>("global");
  const [step1Valid, setStep1Valid] = useState(false);
  const [pipelineResources, setPipelineResources] = useState<PipelineResources>({});
  const [catalogProvisioning, setCatalogProvisioning] = useState(false);
  const [pipelineLoading, setPipelineLoading] = useState(!!searchParams.get("id"));
  const [originalStatus, setOriginalStatus] = useState<PipelineStatus | null>(null);
  const [catalogIsolation, setCatalogIsolation] = useState<CatalogIsolationMode>("per-pipeline");

  const [jobRunState, setJobRunState] = useState<JobRunState>("idle");
  const [pipelineType, setPipelineType] = useState<PipelineType | null>(null);

  // Step 2: live polling state
  const [step2JobState, setStep2JobState] = useState<string>("UNKNOWN");
  const [step2JobMessage, setStep2JobMessage] = useState("");
  const [step2SimRunning, setStep2SimRunning] = useState(false);
  const [step2ActionLoading, setStep2ActionLoading] = useState(false);
  const step2PollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Step 2: Per-pipeline data producer state
  const [producerState, setProducerState] = useState<DataProducerState>({
    running: false, source_type: null, source_label: null, destination: null,
    run_id: null, state: "IDLE", error: null,
  });
  // producerAutoStartedRef removed — producer runs as Task 2 of the deployed job

  // Pipeline source config (read from DB for canvas on step 2)
  const [pipelineSource, setPipelineSource] = useState<SssSource | null>(null);
  const [pipelineProcessingMode, setPipelineProcessingMode] = useState<SssProcessingMode>("realtime");
  const [pipelineSinkTable, setPipelineSinkTable] = useState("");
  const [pipelineMetricsEnabled, setPipelineMetricsEnabled] = useState(false);
  const [pipelineMetricsTable, setPipelineMetricsTable] = useState("");
  const [pipelineRtmSink, setPipelineRtmSink] = useState<SssRtmSinkType | undefined>(undefined);

  // Sink config step (Phase 3b): LakeTS tiering + SSE metric selection.
  const [tiering, setTiering] = useState<TieringConfig | undefined>(undefined);
  const [sseMetrics, setSseMetrics] = useState<string[]>([]);
  const [sinkStepValid, setSinkStepValid] = useState(true);
  // Preserve other sink_config keys written by sibling features so we don't
  // clobber them when persisting tiering/sse_metrics.
  const sinkConfigExtrasRef = useRef<Record<string, unknown>>({});

  useEffect(() => {
    const resumeStep = searchParams.get("step");
    if (resumeStep) setStep(Number(resumeStep));
  }, [searchParams]);

  // Load default catalog isolation mode from user settings
  useEffect(() => {
    api.getUserSettings().then((s) => {
      if (s.uc?.catalog_isolation_mode) {
        setCatalogIsolation(s.uc.catalog_isolation_mode);
      }
    }).catch(() => {});
  }, []);

  useEffect(() => {
    if (!pipelineId) return;
    api.getPipeline(pipelineId).then((p) => {
      if (p.name) setPipelineName(p.name);
      if (p.industry) setIndustry(p.industry);
      if (p.status) setOriginalStatus(p.status);
      if (p.sensor_config?.length) setSensorConfig(p.sensor_config);
      const cfg = (p.source_config ?? {}) as Record<string, unknown>;
      const rate = cfg.update_rate_hz;
      if (typeof rate === "number") setUpdateRateHz(rate);
      if (p.resources) setPipelineResources(p.resources);
      if (p.source_type) setPipelineType(p.source_type as PipelineType);

      const src = cfg.source || cfg.sss_source;
      if (src) setPipelineSource(src as SssSource);
      if (cfg.processing_mode) setPipelineProcessingMode(cfg.processing_mode as SssProcessingMode);
      if (cfg.sink_table) setPipelineSinkTable(cfg.sink_table as string);
      if (typeof cfg.metrics_enabled === "boolean") setPipelineMetricsEnabled(cfg.metrics_enabled as boolean);
      if (cfg.metrics_table) setPipelineMetricsTable(cfg.metrics_table as string);
      if (cfg.rtm_sink) setPipelineRtmSink(cfg.rtm_sink as SssRtmSinkType);

      // Hydrate sink_config (Phase 3b tiering + sse_metrics).
      const sinkCfg = (p.sink_config ?? {}) as Record<string, unknown>;
      const t = sinkCfg.tiering as Record<string, unknown> | undefined;
      if (
        t &&
        typeof t.hot === "string" &&
        typeof t.drop === "string" &&
        typeof t.chunk === "string"
      ) {
        setTiering({ hot: t.hot, drop: t.drop, chunk: t.chunk });
      }
      const rawSse = sinkCfg.sse_metrics;
      if (Array.isArray(rawSse)) {
        setSseMetrics(rawSse.filter((m): m is string => typeof m === "string"));
      }
      // Preserve other sink_config keys so we round-trip them unchanged.
      const { tiering: _t, sse_metrics: _s, ...rest } = sinkCfg;
      void _t; void _s;
      sinkConfigExtrasRef.current = rest;
    }).catch(() => {}).finally(() => setPipelineLoading(false));
  }, [pipelineId]);

  const prevStep = useRef(step);
  useEffect(() => {
    if (step === 1 && prevStep.current !== 1 && pipelineId) {
      api.getPipeline(pipelineId).then((p) => {
        if (p.resources) setPipelineResources(p.resources);
        if (p.source_type) setPipelineType(p.source_type as PipelineType);
      }).catch(() => {});
    }
    prevStep.current = step;
  }, [step, pipelineId]);

  // Deploy & Monitor polling (step 3): job status + simulator status + producer status
  useEffect(() => {
    if (step !== 3) {
      if (step2PollRef.current) { clearInterval(step2PollRef.current); step2PollRef.current = null; }
      return;
    }
    const jobId = pipelineResources.job_id ? Number(pipelineResources.job_id) : null;

    const poll = async () => {
      try {
        const sim = await api.getSimulatorStatus();
        setStep2SimRunning(sim.running);
      } catch { /* ignore */ }

      if (jobId) {
        try {
          const s = await api.getJobStatus(jobId);
          setStep2JobState(s.state);
          setStep2JobMessage(s.state_message || "");
          // Derive producer state from job's Task 2 if available
          if (s.tasks?.data_producer) {
            const taskState = s.tasks.data_producer.state;
            if (taskState === "RUNNING") {
              setProducerState((prev) => ({ ...prev, running: true, state: "RUNNING" }));
            } else if (taskState === "FAILED" || taskState === "INTERNAL_ERROR") {
              setProducerState((prev) => ({
                ...prev, running: false, state: "FAILED",
                error: s.tasks!.data_producer.state_message || "Task failed",
              }));
            } else if (taskState === "PENDING" || taskState === "QUEUED" || taskState === "BLOCKED") {
              setProducerState((prev) => ({ ...prev, state: "STARTING" }));
            }
          }
        } catch { /* ignore */ }
      }
    };
    poll();
    step2PollRef.current = setInterval(poll, 5000);
    return () => { if (step2PollRef.current) clearInterval(step2PollRef.current); };
  }, [step, pipelineResources.job_id]);

  // Re-fetch pipeline to get source_config for canvas when entering Deploy step
  useEffect(() => {
    if (step === 3 && pipelineId) {
      api.getPipeline(pipelineId).then((p) => {
        if (p.resources) setPipelineResources(p.resources);
        const cfg = (p.source_config ?? {}) as Record<string, unknown>;
        const src2 = cfg.source || cfg.sss_source;
        if (src2) setPipelineSource(src2 as SssSource);
        if (cfg.processing_mode) setPipelineProcessingMode(cfg.processing_mode as SssProcessingMode);
        if (cfg.sink_table) setPipelineSinkTable(cfg.sink_table as string);
        if (typeof cfg.metrics_enabled === "boolean") setPipelineMetricsEnabled(cfg.metrics_enabled as boolean);
        if (cfg.metrics_table) setPipelineMetricsTable(cfg.metrics_table as string);
        if (cfg.rtm_sink) setPipelineRtmSink(cfg.rtm_sink as SssRtmSinkType);
      }).catch(() => {});
    }
  }, [step, pipelineId]);

  const handleJobRunStateChange = useCallback((state: JobRunState) => {
    setJobRunState(state);
  }, []);

  async function handleStep0Next() {
    if (!pipelineName.trim()) {
      toast.error("Pipeline name is required");
      return;
    }
    const duplicate = pipelines.find(
      (p) => p.name.toLowerCase() === pipelineName.trim().toLowerCase() && p.id !== pipelineId && p.status !== "deleted",
    );
    if (duplicate) {
      toast.error(`A pipeline named "${pipelineName.trim()}" already exists`);
      return;
    }
    try {
      const nextStatus = originalStatus === "active" ? "active" : "step1_complete";
      let id = pipelineId;
      if (!id) {
        const p = await createPipeline(pipelineName, industry);
        id = p.id;
        setPipelineId(id);
        await updatePipeline(id, {
          status: "step1_complete",
          industry,
          sensor_config: sensorConfig,
          source_config: { update_rate_hz: updateRateHz },
        });
      } else {
        await updatePipeline(id, {
          name: pipelineName,
          status: nextStatus,
          industry,
          sensor_config: sensorConfig,
          source_config: { update_rate_hz: updateRateHz },
        });
      }

      if (!pipelineResources.uc_catalog) {
        setCatalogProvisioning(true);
        try {
          const cat = await api.provisionPipelineCatalog(id, pipelineName, catalogIsolation);
          setPipelineResources((prev) => ({
            ...prev,
            uc_catalog: cat.catalog,
            checkpoint_volume: cat.volume,
          }));
          if (cat.status === "exists") {
            toast.info(`Using existing catalog ${cat.catalog}`);
          } else {
            toast.success(`Catalog ${cat.catalog} provisioned`);
          }
        } catch (catErr) {
          toast.error(`Catalog provisioning failed: ${catErr instanceof Error ? catErr.message : String(catErr)}`);
          setCatalogProvisioning(false);
          return;
        } finally {
          setCatalogProvisioning(false);
        }
      }

      setStep(1);
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Failed to save");
    }
  }

  const isZerobusPipeline = pipelineType === "zerobus";
  const canProceedToStep2 = step1Valid && (isZerobusPipeline || jobRunState === "running");

  async function handleStep1Next() {
    if (pipelineId) {
      try {
        const nextStatus = originalStatus === "active" ? "active" : "step2_complete";
        await updatePipeline(pipelineId, { status: nextStatus });
      } catch {
        // non-blocking
      }
    }
    setStep(2);
  }

  // Persist sink_config (tiering + sse_metrics) and advance to Deploy & Monitor.
  async function handleStep2Next() {
    if (!sinkStepValid) {
      toast.error("Fix sink configuration errors before continuing");
      return;
    }
    if (pipelineId) {
      try {
        const nextStatus = originalStatus === "active" ? "active" : "step3_complete";
        // Build sink_config, preserving untouched keys.  When tiering is
        // undefined we omit the key entirely (JSONB field absent).
        const nextSinkConfig: Record<string, unknown> = {
          ...sinkConfigExtrasRef.current,
          sse_metrics: sseMetrics,
        };
        if (tiering) {
          nextSinkConfig.tiering = tiering;
        } else {
          delete nextSinkConfig.tiering;
        }
        await updatePipeline(pipelineId, {
          status: nextStatus,
          sink_config: nextSinkConfig,
        });
      } catch (err) {
        toast.error(err instanceof Error ? err.message : "Failed to save sink config");
        return;
      }
    }
    setStep(3);
  }

  async function handleFinish() {
    if (pipelineId) {
      try {
        await updatePipeline(pipelineId, { status: "active" });
      } catch {
        // non-blocking
      }
    }
    navigate(pipelineId ? `/pipelines/${pipelineId}/dashboard` : "/");
  }

  async function handleSaveAndExit() {
    if (pipelineId) {
      try {
        const status: PipelineStatus = originalStatus === "active"
          ? "active"
          : step === 0 ? "draft" : `step${step}_complete` as PipelineStatus;
        await updatePipeline(pipelineId, { status });
        toast.success("Pipeline saved");
      } catch {
        // non-blocking
      }
      navigate(`/pipelines/${pipelineId}`);
    } else {
      navigate("/industries");
    }
  }

  async function handleStep2Stop() {
    const jobId = pipelineResources.job_id ? Number(pipelineResources.job_id) : null;
    if (!jobId) return;
    setStep2ActionLoading(true);
    try {
      await api.stopJob(jobId);
      setStep2JobState("TERMINATED");
      setStep2JobMessage("Job paused by user");
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Failed to stop job");
    } finally {
      setStep2ActionLoading(false);
    }
  }

  async function handleStep2Restart() {
    const jobId = pipelineResources.job_id ? Number(pipelineResources.job_id) : null;
    if (!jobId) return;
    setStep2ActionLoading(true);
    try {
      await api.restartJob(jobId);
      setStep2JobState("PENDING");
      setStep2JobMessage("Restarting...");
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Failed to restart job");
    } finally {
      setStep2ActionLoading(false);
    }
  }

  // Producer runs as Task 2 of the deployed job — no separate start/stop needed.
  // Status is derived from the job's task state in the polling useEffect above.

  const isStep2Active = step2SimRunning && step2JobState === "RUNNING";
  const dbHost = typeof window !== "undefined" ? window.location.origin : "";

  return (
    <div className="mx-auto max-w-5xl">
      <Stepper steps={STEP_LABELS} currentStep={step} />

      {step === 0 && (
        <div className="space-y-6">
          {/* Two-column layout: 60% form, 40% summary */}
          <div className="flex gap-6 items-start">
            {/* Left column — form (60%) */}
            <div className="flex-[3] min-w-0 space-y-6">
              <div className="glass rounded-lg p-5">
                <label className="mb-1.5 block text-xs font-ui font-medium uppercase tracking-wider text-muted-foreground">
                  Pipeline Name
                </label>
                <input
                  type="text"
                  value={pipelineName}
                  onChange={(e) => setPipelineName(e.target.value)}
                  placeholder="My IoT Pipeline"
                  className="w-full rounded-md border border-border/50 bg-input px-3 py-2 text-sm placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-primary"
                />
              </div>

              {/* Catalog isolation mode */}
              <div className="glass rounded-lg p-5 space-y-3">
                <label className="block text-xs font-ui font-medium uppercase tracking-wider text-muted-foreground">
                  Catalog Mode
                </label>
                <div className="flex gap-4">
                  {(["per-pipeline", "shared"] as const).map((mode) => (
                    <label key={mode} className="flex items-center gap-2 text-sm cursor-pointer">
                      <input
                        type="radio"
                        name="catalog-isolation-wizard"
                        checked={catalogIsolation === mode}
                        onChange={() => setCatalogIsolation(mode)}
                        className="h-4 w-4"
                      />
                      {mode === "per-pipeline"
                        ? "Dedicated catalog for this pipeline"
                        : "Use shared catalog"}
                    </label>
                  ))}
                </div>
                <p className="text-xs text-muted-foreground">
                  {catalogIsolation === "per-pipeline"
                    ? "A new catalog will be created using the naming pattern from global settings."
                    : "This pipeline will use the shared catalog configured in global settings."}
                </p>
              </div>

              <DataHubStep
                selectedIndustry={industry}
                onIndustryChange={setIndustry}
                sensorConfigs={sensorConfig}
                onSensorConfigsChange={setSensorConfig}
                updateRateHz={updateRateHz}
                onUpdateRateHzChange={setUpdateRateHz}
                frequencyMode={frequencyMode}
                onFrequencyModeChange={setFrequencyMode}
              />

              {pipelineResources.uc_catalog && (
                <div className="glass rounded-lg border-primary/30 p-4 flex items-start gap-3">
                  <Database className="mt-0.5 h-4 w-4 shrink-0 text-primary" />
                  <div className="space-y-1 text-sm">
                    <p className="font-medium">Unity Catalog provisioned</p>
                    <div className="grid grid-cols-[auto_1fr] gap-x-3 gap-y-0.5 text-xs text-muted-foreground">
                      <span className="font-medium">Catalog</span>
                      <span className="font-mono">{pipelineResources.uc_catalog}</span>
                      <span className="font-medium">Schemas</span>
                      <span className="font-mono">raw, metrics, checkpoints</span>
                      <span className="font-medium">Volume</span>
                      <span className="font-mono truncate">{pipelineResources.checkpoint_volume || "—"}</span>
                    </div>
                  </div>
                </div>
              )}
            </div>

            {/* Right column — Configuration Summary (40%) */}
            <div className="flex-[2] min-w-0 sticky top-6">
              <div className="glass rounded-lg p-5 space-y-4">
                {/* Header */}
                <div>
                  <h3 className="font-display text-sm font-semibold">Configuration Summary</h3>
                  <p className="mt-0.5 text-[10px] font-ui font-medium uppercase tracking-wider text-muted-foreground">
                    Sensor Rate Topology ({sensorConfig.length} Units)
                  </p>
                </div>

                {/* Dot grid — one dot per sensor node */}
                <div className="rounded-lg bg-surface-highest p-3">
                  {sensorConfig.length > 0 ? (
                    <div className="flex flex-wrap gap-1">
                      {sensorConfig.map((_, i) => (
                        <div
                          key={i}
                          className="w-3 h-3 rounded-full bg-primary opacity-90"
                        />
                      ))}
                    </div>
                  ) : (
                    <div className="flex h-16 items-center justify-center text-xs text-muted-foreground">
                      Select an industry to visualise sensor nodes
                    </div>
                  )}
                </div>

                {/* Summary details */}
                <div className="space-y-2">
                  <div className="flex items-center justify-between">
                    <span className="text-xs font-ui text-muted-foreground">Mode</span>
                    <span className="text-xs font-medium capitalize">
                      {frequencyMode === "global" ? "Global Rate" : "Per-Sensor"}
                    </span>
                  </div>
                  <div className="flex items-center justify-between">
                    <span className="text-xs font-ui text-muted-foreground">Isolation</span>
                    <span className="text-xs font-medium capitalize">
                      {catalogIsolation === "per-pipeline" ? "Per-Pipeline" : "Shared"}
                    </span>
                  </div>
                  <div className="flex items-center justify-between">
                    <span className="text-xs font-ui text-muted-foreground">Node Count</span>
                    <span className="text-xs font-medium tabular-nums">
                      {sensorConfig.length} sensors
                    </span>
                  </div>
                  <div className="flex items-center justify-between">
                    <span className="text-xs font-ui text-muted-foreground">Update Rate</span>
                    <span className="text-xs font-medium tabular-nums">
                      {updateRateHz.toFixed(1)} Hz
                    </span>
                  </div>
                  {industry && (
                    <div className="flex items-center justify-between">
                      <span className="text-xs font-ui text-muted-foreground">Industry</span>
                      <span className="text-xs font-medium capitalize">
                        {industry.replace(/_/g, " ")}
                      </span>
                    </div>
                  )}
                </div>

                {/* Architect's Tip */}
                <div className="glass border border-primary/30 p-3 rounded-lg space-y-1.5">
                  <div className="flex items-center gap-1.5">
                    <Lightbulb className="h-3.5 w-3.5 shrink-0 text-primary" />
                    <span className="text-[10px] font-ui font-semibold uppercase tracking-wider text-primary">
                      Architect's Tip
                    </span>
                  </div>
                  <p className="text-xs text-muted-foreground leading-relaxed">
                    {sensorConfig.length >= 42
                      ? `${industry ? industry.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase()) : "Manufacturing"} configurations with ${sensorConfig.length}+ sensors perform best with 'Per-Pipeline' cataloging to ensure isolation during high-frequency data bursts.`
                      : "Select an industry and configure sensors. Larger sensor counts benefit from dedicated catalog isolation to prevent schema contention."}
                  </p>
                </div>
              </div>
            </div>
          </div>

          <div className="flex justify-end">
            <Button className="gap-2 hover:glow-primary transition-all" onClick={handleStep0Next} disabled={pipelineLoading || catalogProvisioning}>
              {pipelineLoading ? (
                <>
                  <Loader2 className="h-4 w-4 animate-spin" />
                  Loading...
                </>
              ) : catalogProvisioning ? (
                <>
                  <Loader2 className="h-4 w-4 animate-spin" />
                  Provisioning Catalog...
                </>
              ) : (
                <>
                  Next: Pipeline Config
                  <ArrowRight className="h-4 w-4" />
                </>
              )}
            </Button>
          </div>
        </div>
      )}

      {step === 1 && (
        <div className="space-y-6">
          <SourceConfigStep
            pipelineId={pipelineId}
            pipelineResources={pipelineResources}
            onValidityChange={setStep1Valid}
            onJobRunStateChange={handleJobRunStateChange}
            onPipelineTypeChange={setPipelineType}
          />
          <div className="flex justify-between">
            <Button variant="outline" className="gap-2" onClick={() => setStep(0)}>
              <ArrowLeft className="h-4 w-4" />
              Back
            </Button>
            <div className="flex gap-2">
              <Button variant="outline" className="gap-2" onClick={handleSaveAndExit}>
                <Save className="h-4 w-4" />
                Save & Exit
              </Button>
              <Button className="gap-2 hover:glow-primary transition-all" onClick={handleStep1Next} disabled={!canProceedToStep2}>
                Next: Sink Config
                <ArrowRight className="h-4 w-4" />
              </Button>
            </div>
            {!canProceedToStep2 && (
              <p className="text-xs text-muted-foreground flex items-center gap-1">
                <Info className="h-3 w-3" />
                {!step1Valid
                  ? "Validate or run your notebook on a cluster before proceeding."
                  : "Deploy your job and wait for it to reach RUNNING state."}
              </p>
            )}
          </div>
        </div>
      )}

      {step === 2 && (
        <div className="space-y-6">
          <SinkConfigStep
            sensorConfigs={sensorConfig}
            tiering={tiering}
            onTieringChange={setTiering}
            sseMetrics={sseMetrics}
            onSseMetricsChange={setSseMetrics}
            onValidityChange={setSinkStepValid}
          />
          <div className="flex justify-between">
            <Button variant="outline" className="gap-2" onClick={() => setStep(1)}>
              <ArrowLeft className="h-4 w-4" />
              Back: Pipeline Config
            </Button>
            <div className="flex gap-2">
              <Button variant="outline" className="gap-2" onClick={handleSaveAndExit}>
                <Save className="h-4 w-4" />
                Save & Exit
              </Button>
              <Button
                className="gap-2 hover:glow-primary transition-all"
                onClick={handleStep2Next}
                disabled={!sinkStepValid}
              >
                Next: Deploy & Monitor
                <ArrowRight className="h-4 w-4" />
              </Button>
            </div>
            {!sinkStepValid && (
              <p className="text-xs text-muted-foreground flex items-center gap-1">
                <Info className="h-3 w-3" />
                Fix the validation errors above before continuing.
              </p>
            )}
          </div>
        </div>
      )}

      {step === 3 && (
        <div className="space-y-6">
          {/* Live Architecture Canvas */}
          <ArchitectureCanvas
            pipeline={pipelineType || "sss"}
            sssSource={pipelineSource}
            processingMode={pipelineProcessingMode}
            sinkTable={pipelineSinkTable}
            metricsEnabled={pipelineMetricsEnabled}
            metricsTable={pipelineMetricsTable}
            rtmSink={pipelineRtmSink}
            active={isStep2Active}
          />

          {/* E2E Flow Status Strip */}
          <div className="glass flex items-center justify-center gap-2 rounded-lg p-3">
            <FlowStage label="Simulator" active={step2SimRunning} />
            <FlowArrow />
            <FlowStage
              label={pipelineSource ? SOURCE_LABELS[pipelineSource] : "Source"}
              active={producerState.state === "RUNNING"}
            />
            <FlowArrow />
            <FlowStage label="SSS Job" active={step2JobState === "RUNNING"} />
            <FlowArrow />
            <FlowStage label="Sink(s)" active={step2JobState === "RUNNING"} />
          </div>

          <Card className="glass border-border/50">
            <CardHeader>
              <CardTitle className="font-display flex items-center gap-2">
                <Rocket className="h-5 w-5 text-primary" />
                Deploy & Monitor
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-6">
              {/* Simulator Status */}
              <div className="flex items-center gap-3 rounded-lg border border-border/50 bg-surface-high p-3">
                <Activity className="h-4 w-4 shrink-0 text-muted-foreground" />
                {step2SimRunning ? (
                  <span className="flex items-center gap-2 text-sm text-success">
                    <span className="relative flex h-2 w-2">
                      <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-success opacity-75" />
                      <span className="relative inline-flex h-2 w-2 rounded-full bg-success" />
                    </span>
                    IoT Simulator active — generating <strong className="ml-1">{industry.replace(/_/g, " ")}</strong> data
                  </span>
                ) : (
                  <span className="flex items-center gap-2 text-sm text-warning">
                    <span className="h-2 w-2 rounded-full bg-warning" />
                    Simulator idle
                  </span>
                )}
              </div>

              {/* Data Producer Status (read-only — runs as Task 2 of the deployed job) */}
              {pipelineSource && (
                <div className="flex items-center gap-3 rounded-lg border border-border/50 bg-surface-high p-3">
                  <Radio className="h-4 w-4 shrink-0 text-muted-foreground" />
                  {producerState.state === "RUNNING" ? (
                    <span className="flex items-center gap-2 text-sm text-success">
                      <span className="relative flex h-2 w-2">
                        <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-success opacity-75" />
                        <span className="relative inline-flex h-2 w-2 rounded-full bg-success" />
                      </span>
                      {SOURCE_LABELS[pipelineSource]} Producer active (Job Task 2)
                    </span>
                  ) : producerState.state === "STARTING" ? (
                    <span className="flex items-center gap-2 text-sm text-blue-600">
                      <Loader2 className="h-3.5 w-3.5 animate-spin" />
                      {SOURCE_LABELS[pipelineSource]} producer starting (Job Task 2)...
                    </span>
                  ) : producerState.state === "FAILED" ? (
                    <span className="flex items-center gap-2 text-sm text-destructive">
                      <AlertCircle className="h-4 w-4" />
                      {SOURCE_LABELS[pipelineSource]} producer failed{producerState.error ? `: ${producerState.error}` : ""} (Job Task 2)
                    </span>
                  ) : (
                    <span className="flex items-center gap-2 text-sm text-muted-foreground">
                      <span className="h-2 w-2 rounded-full bg-muted-foreground/50" />
                      {SOURCE_LABELS[pipelineSource]} producer — managed by Job Task 2
                    </span>
                  )}
                </div>
              )}

              {/* Job Status & Controls */}
              {pipelineResources.job_id && (
                <div className="flex items-center gap-3 rounded-lg border border-border/50 bg-surface-high p-3">
                  <Rocket className="h-4 w-4 shrink-0 text-muted-foreground" />
                  {step2JobState === "RUNNING" && (
                    <>
                      <span className="flex items-center gap-2 text-sm text-success">
                        <span className="relative flex h-2 w-2">
                          <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-success opacity-75" />
                          <span className="relative inline-flex h-2 w-2 rounded-full bg-success" />
                        </span>
                        Streaming job running
                      </span>
                      <Button
                        size="sm"
                        variant="outline"
                        className="ml-auto gap-1.5 text-xs"
                        disabled={step2ActionLoading}
                        onClick={handleStep2Stop}
                      >
                        {step2ActionLoading ? <Loader2 className="h-3 w-3 animate-spin" /> : <Square className="h-3 w-3" />}
                        Stop
                      </Button>
                    </>
                  )}
                  {(step2JobState === "PENDING" || step2JobState === "QUEUED" || step2JobState === "BLOCKED") && (
                    <span className="flex items-center gap-2 text-sm text-muted-foreground">
                      <Loader2 className="h-4 w-4 animate-spin" />
                      Job starting... ({step2JobState})
                    </span>
                  )}
                  {step2JobState === "TERMINATED" && (
                    <>
                      <span className="flex items-center gap-2 text-sm text-warning">
                        <span className="h-2 w-2 rounded-full bg-warning" />
                        Job paused
                        {step2JobMessage && <span className="text-xs text-muted-foreground">— {step2JobMessage}</span>}
                      </span>
                      <Button
                        size="sm"
                        variant="outline"
                        className="ml-auto gap-1.5 text-xs"
                        disabled={step2ActionLoading}
                        onClick={handleStep2Restart}
                      >
                        {step2ActionLoading ? <Loader2 className="h-3 w-3 animate-spin" /> : <Play className="h-3 w-3" />}
                        Restart
                      </Button>
                    </>
                  )}
                  {(step2JobState === "INTERNAL_ERROR" || step2JobState === "SKIPPED") && (
                    <>
                      <span className="flex items-center gap-2 text-sm text-destructive">
                        <AlertCircle className="h-4 w-4" />
                        Job error: {step2JobMessage || step2JobState}
                      </span>
                      <Button
                        size="sm"
                        variant="outline"
                        className="ml-auto gap-1.5 text-xs"
                        disabled={step2ActionLoading}
                        onClick={handleStep2Restart}
                      >
                        {step2ActionLoading ? <Loader2 className="h-3 w-3 animate-spin" /> : <RefreshCw className="h-3 w-3" />}
                        Retry
                      </Button>
                    </>
                  )}
                  {step2JobState === "NO_RUNS" && (
                    <span className="text-sm text-muted-foreground">No runs found for this job.</span>
                  )}
                </div>
              )}

              {/* Pipeline summary */}
              <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                <div className="rounded-lg border border-border/50 bg-surface-high p-4 text-center">
                  <p className="text-xs font-ui font-medium uppercase tracking-wider text-muted-foreground">Pipeline</p>
                  <p className="mt-1 text-sm font-semibold">{pipelineName || "Untitled"}</p>
                </div>
                <div className="rounded-lg border border-border/50 bg-surface-high p-4 text-center">
                  <p className="text-xs font-ui font-medium uppercase tracking-wider text-muted-foreground">Industry</p>
                  <p className="mt-1 text-sm font-semibold capitalize">{industry.replace(/_/g, " ")}</p>
                </div>
              </div>

              {pipelineResources.uc_catalog && (
                <div className="glass rounded-lg border-primary/30 p-4 flex items-start gap-3">
                  <Database className="mt-0.5 h-4 w-4 shrink-0 text-primary" />
                  <div className="space-y-1 text-sm">
                    <p className="font-medium">Unity Catalog</p>
                    <div className="grid grid-cols-[auto_1fr] gap-x-3 gap-y-0.5 text-xs text-muted-foreground">
                      <span className="font-medium">Catalog</span>
                      <span className="font-mono">{pipelineResources.uc_catalog}</span>
                      <span className="font-medium">Schemas</span>
                      <span className="font-mono">raw, metrics, checkpoints</span>
                      <span className="font-medium">Volume</span>
                      <span className="font-mono truncate">{pipelineResources.checkpoint_volume || "—"}</span>
                    </div>
                  </div>
                </div>
              )}

              <div className="flex items-center gap-3">
                <Button
                  className="gap-2 hover:glow-primary transition-all"
                  onClick={() => navigate(pipelineId ? `/pipelines/${pipelineId}/dashboard` : "/")}
                >
                  <LayoutDashboard className="h-4 w-4" />
                  View Pipeline Dashboard
                </Button>
                {pipelineResources.job_id && (
                  <a
                    href={`${dbHost}/#job/${pipelineResources.job_id}`}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="flex items-center gap-1 text-sm text-primary underline-offset-4 hover:underline"
                  >
                    <ExternalLink className="h-3.5 w-3.5" />
                    View Job in Databricks
                  </a>
                )}
              </div>
            </CardContent>
          </Card>

          <div className="flex justify-between">
            <Button variant="outline" className="gap-2" onClick={() => setStep(2)}>
              <ArrowLeft className="h-4 w-4" />
              Back: Sink Config
            </Button>
            <div className="flex gap-2">
              <Button variant="outline" className="gap-2" onClick={handleSaveAndExit}>
                <Save className="h-4 w-4" />
                Save & Exit
              </Button>
              <Button className="gap-2 hover:glow-primary transition-all" onClick={handleFinish}>
                <Check className="h-4 w-4" />
                Finish
              </Button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

function FlowStage({ label, active }: { label: string; active: boolean }) {
  return (
    <div className="flex items-center gap-1.5">
      {active ? (
        <CheckCircle2 className="h-4 w-4 text-success" />
      ) : (
        <span className="h-4 w-4 rounded-full border-2 border-muted-foreground/30" />
      )}
      <span className={`text-xs font-ui font-medium ${active ? "text-success" : "text-muted-foreground"}`}>
        {label}
      </span>
    </div>
  );
}

function FlowArrow() {
  return <ArrowRight className="h-3.5 w-3.5 text-muted-foreground/40" />;
}
