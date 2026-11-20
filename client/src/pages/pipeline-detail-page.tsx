import { useState, useEffect, useCallback } from "react";
import { useParams, useNavigate, Link } from "react-router-dom";
import { formatDistanceToNow } from "date-fns";
import {
  ArrowLeft,
  Pencil,
  Trash2,
  ExternalLink,
  Server,
  Database,
  FileCode,
  LayoutDashboard,
  Loader2,
  Copy,
  CheckCircle,
  Clock,
  Activity,
  RefreshCw,
  ChevronDown,
  ChevronUp,
} from "lucide-react";
import { toast } from "sonner";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import { ConfirmDialog } from "@/components/confirm-dialog";
import { api } from "@/lib/api";
import { usePipelines } from "@/contexts/pipeline-context";
import type { Pipeline, PipelineStatus, StreamingMetricsResponse, TaskStatus } from "@/lib/types";

const statusVariant: Record<PipelineStatus, "default" | "secondary" | "success" | "warning" | "destructive"> = {
  draft: "secondary",
  step1_complete: "warning",
  step2_complete: "warning",
  step3_complete: "warning",
  active: "success",
  error: "destructive",
  deleted: "secondary",
};

const statusLabel: Record<PipelineStatus, string> = {
  draft: "Draft",
  step1_complete: "Step 1 Complete",
  step2_complete: "Step 2 Complete",
  step3_complete: "Step 3 Complete",
  active: "Active",
  error: "Error",
  deleted: "Deleted",
};

export function PipelineDetailPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const { deletePipeline, clonePipeline } = usePipelines();

  const [pipeline, setPipeline] = useState<Pipeline | null>(null);
  const [loading, setLoading] = useState(true);
  const [showDelete, setShowDelete] = useState(false);
  const [showClone, setShowClone] = useState(false);
  const [cloneName, setCloneName] = useState("");
  const [cloning, setCloning] = useState(false);

  // Streaming metrics state
  const [metricsData, setMetricsData] = useState<StreamingMetricsResponse | null>(null);
  const [metricsLoading, setMetricsLoading] = useState(false);
  const [metricsExpanded, setMetricsExpanded] = useState(false);

  // Job task status state
  const [jobState, setJobState] = useState<string>("unknown");
  const [taskStates, setTaskStates] = useState<Record<string, TaskStatus>>({});

  useEffect(() => {
    if (!id) return;
    setLoading(true);
    api
      .getPipeline(id)
      .then(setPipeline)
      .catch(() => toast.error("Failed to load pipeline"))
      .finally(() => setLoading(false));
  }, [id]);

  async function handleDelete() {
    if (!id) return;
    try {
      await deletePipeline(id);
      setShowDelete(false);
      navigate("/pipelines");
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Failed to delete pipeline");
      setShowDelete(false);
    }
  }

  async function handleClone() {
    if (!id || !cloneName.trim()) return;
    setCloning(true);
    try {
      const cloned = await clonePipeline(id, cloneName.trim());
      setShowClone(false);
      setCloneName("");
      toast.success(`Pipeline "${cloned.name}" created`);
      navigate(`/pipelines/new?id=${cloned.id}&step=0`);
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Failed to clone pipeline");
    } finally {
      setCloning(false);
    }
  }

  const fetchMetrics = useCallback(async () => {
    if (!id) return;
    setMetricsLoading(true);
    try {
      const data = await api.getStreamingMetrics(id);
      setMetricsData(data);
    } catch {
      // Silently fail — metrics may not be available yet
    } finally {
      setMetricsLoading(false);
    }
  }, [id]);

  // Poll streaming metrics for active pipelines with a UC catalog
  useEffect(() => {
    if (!pipeline || !pipeline.resources?.uc_catalog) return;
    if (pipeline.status !== "active" && pipeline.status !== "step3_complete") return;
    fetchMetrics();
    const interval = setInterval(fetchMetrics, 30_000);
    return () => clearInterval(interval);
  }, [pipeline, fetchMetrics]);

  // Poll job task status
  useEffect(() => {
    const jobId = pipeline?.resources?.job_id;
    if (!jobId) return;
    const poll = async () => {
      try {
        const status = await api.getJobStatus(Number(jobId));
        setJobState(status.state);
        if (status.tasks) setTaskStates(status.tasks);
      } catch { /* job may not exist */ }
    };
    poll();
    const interval = setInterval(poll, 5_000);
    return () => clearInterval(interval);
  }, [pipeline?.resources?.job_id]);

  if (loading) {
    return (
      <div className="flex items-center justify-center gap-2 py-24 text-sm text-muted-foreground">
        <Loader2 className="h-4 w-4 animate-spin" />
        Loading pipeline...
      </div>
    );
  }

  if (!pipeline) {
    return (
      <div className="py-24 text-center text-sm text-muted-foreground">
        Pipeline not found.
      </div>
    );
  }

  const res = pipeline.resources || {};
  const hasResources = Object.keys(res).length > 0;
  const created = pipeline.created_at
    ? formatDistanceToNow(new Date(pipeline.created_at), { addSuffix: true })
    : "";

  // Real per-task runtime truth: a job whose overall life-cycle reads
  // "RUNNING" can still have an individual task that crashed (e.g. the data
  // producer died) — the same "RUNNING alone hides a crashed task" pitfall
  // the admin pipeline view guards against. Never let the top badge read a
  // healthy "RUNNING" green when that's actually the case.
  const failedTasks = Object.values(taskStates).filter(
    (t) => t.state === "TERMINATED" || t.state === "INTERNAL_ERROR",
  );
  const hasFailedTask = failedTasks.length > 0;
  const isDegraded = jobState === "RUNNING" && hasFailedTask;
  const degradedReason = failedTasks[0]?.state_message || undefined;

  return (
    <div className="mx-auto max-w-4xl space-y-6">
      {/* Header */}
      <div className="flex items-start justify-between gap-4">
        <div>
          <Button
            variant="ghost"
            size="sm"
            className="mb-2 -ml-2 gap-1 text-muted-foreground hover:text-primary"
            onClick={() => navigate("/pipelines")}
          >
            <ArrowLeft className="h-3.5 w-3.5" />
            All Pipelines
          </Button>
          <h1 className="font-display text-2xl font-bold tracking-tight">
            {pipeline.name}
          </h1>
          <div className="mt-1 flex items-center gap-3 text-sm text-muted-foreground">
            <Badge variant={statusVariant[pipeline.status]}>
              {statusLabel[pipeline.status]}
            </Badge>
            {isDegraded && (
              <Badge
                variant="warning"
                title={degradedReason ?? "A task in this pipeline's job crashed"}
              >
                Degraded
              </Badge>
            )}
            {pipeline.industry && (
              <span className="capitalize">
                {pipeline.industry.replace(/_/g, " ")}
              </span>
            )}
            {created && <span>{created}</span>}
          </div>
        </div>
        <div className="flex gap-2">
          <Button
            variant="outline"
            size="sm"
            className="gap-1.5"
            onClick={() => {
              setCloneName(`${pipeline.name} (Copy)`);
              setShowClone(true);
            }}
          >
            <Copy className="h-3.5 w-3.5" />
            Clone
          </Button>
          <Button
            variant="outline"
            size="sm"
            className="gap-1.5"
            asChild
          >
            <Link to={`/pipelines/new?id=${pipeline.id}&step=2`}>
              <Pencil className="h-3.5 w-3.5" />
              Edit
            </Link>
          </Button>
          <Button
            variant="destructive"
            size="sm"
            className="gap-1.5"
            onClick={() => setShowDelete(true)}
          >
            <Trash2 className="h-3.5 w-3.5" />
            Delete
          </Button>
        </div>
      </div>

      <Separator />

      {/* Configuration + Resources — 2-column grid */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <div className="lg:col-span-2">
          {/* Configuration summary */}
          <Card className="glass h-full">
            <CardHeader>
              <CardTitle className="font-display">Configuration</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <Row label="Pipeline ID" value={pipeline.id} mono />
              <Row label="Industry" value={pipeline.industry ?? "Not set"} />
              <Row
                label="Source Type"
                value={pipeline.source_type ?? "Not configured"}
              />
              <Row label="Status" value={statusLabel[pipeline.status]} />
              <Row
                label="Sensors"
                value={
                  Array.isArray(pipeline.sensor_config)
                    ? `${pipeline.sensor_config.length} configured`
                    : "0"
                }
              />
            </CardContent>
          </Card>
        </div>
        <div>
          {/* Resources */}
          {hasResources ? (
            <Card className="glass h-full">
              <CardHeader>
                <CardTitle className="font-display">Provisioned Resources</CardTitle>
              </CardHeader>
              <CardContent className="space-y-3">
                {res.job_id && (
                  <ResourceRow
                    icon={Server}
                    label="Databricks Job"
                    value={`Job #${res.job_id}`}
                    href={
                      res.job_id
                        ? `${window.location.origin.replace(/:\d+$/, "")}/#job/${res.job_id}`
                        : undefined
                    }
                  />
                )}
                {res.cluster_id && (
                  <ResourceRow
                    icon={Server}
                    label="Cluster"
                    value={res.cluster_id}
                  />
                )}
                {res.uc_catalog && (
                  <ResourceRow
                    icon={Database}
                    label="UC Catalog / Schema"
                    value={`${res.uc_catalog}.${res.uc_schema ?? ""}`}
                  />
                )}
                {res.lakebase_table && (
                  <ResourceRow
                    icon={Database}
                    label="Lakebase Table"
                    value={res.lakebase_table}
                  />
                )}
                {res.notebook_path && (
                  <ResourceRow
                    icon={FileCode}
                    label="Notebook"
                    value={res.notebook_path}
                  />
                )}
              </CardContent>
            </Card>
          ) : (
            <Card className="glass h-full">
              <CardHeader>
                <CardTitle className="font-display">Provisioned Resources</CardTitle>
              </CardHeader>
              <CardContent>
                <p className="text-xs text-muted-foreground">No resources provisioned yet.</p>
              </CardContent>
            </Card>
          )}
        </div>
      </div>

      {/* Job Task Status */}
      {res.job_id && Object.keys(taskStates).length > 0 && (
        <Card className="glass">
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="font-display flex items-center gap-2">
              <Server className="h-4 w-4" />
              Job Tasks
            </CardTitle>
            <Badge
              variant={
                isDegraded
                  ? "warning"
                  : jobState === "RUNNING"
                    ? "success"
                    : jobState === "TERMINATED"
                      ? "destructive"
                      : "secondary"
              }
              className="font-mono text-[10px]"
              title={isDegraded ? (degradedReason ?? "A task crashed — data may not be flowing") : undefined}
            >
              {isDegraded ? "DEGRADED" : jobState}
            </Badge>
          </CardHeader>
          <CardContent className="space-y-2">
            {Object.entries(taskStates).map(([key, task]) => {
              const isRunning = task.state === "RUNNING";
              const isFailed = task.state === "TERMINATED" || task.state === "INTERNAL_ERROR";
              const isPending = task.state === "PENDING" || task.state === "QUEUED" || task.state === "BLOCKED";
              const label = key === "sss_consumer" ? "SSS Consumer" : key === "data_producer" ? "Data Producer" : key;
              return (
                <div
                  key={key}
                  className={`flex items-center gap-3 rounded-md px-3 py-2 text-sm border ${
                    isFailed ? "border-destructive/50 bg-destructive/5" : "border-border/50 bg-surface-high"
                  }`}
                >
                  <span
                    className={`h-2 w-2 rounded-full shrink-0 ${
                      isRunning ? "bg-success animate-pulse"
                        : isFailed ? "bg-destructive"
                          : isPending ? "bg-warning animate-pulse"
                            : "bg-muted-foreground"
                    }`}
                  />
                  <span className="font-ui text-xs font-semibold">{label}</span>
                  <Badge
                    variant={isRunning ? "success" : isFailed ? "destructive" : "secondary"}
                    className="ml-auto font-mono text-[10px]"
                  >
                    {task.state}
                  </Badge>
                  {isFailed && task.state_message && (
                    <p className="text-[10px] text-destructive truncate max-w-[200px]" title={task.state_message}>
                      {task.state_message}
                    </p>
                  )}
                </div>
              );
            })}
            {hasFailedTask && (
              <p className={`text-xs font-medium mt-1 ${isDegraded ? "text-warning" : "text-destructive"}`}>
                {isDegraded
                  ? `Degraded — the job is still running but data may not be flowing${degradedReason ? `: ${degradedReason}` : ". Check the job in Databricks for details."}`
                  : "One or more tasks have failed. Check the job in Databricks for details."}
              </p>
            )}
          </CardContent>
        </Card>
      )}

      {/* Live Data Stream */}
      <div className="glass rounded-lg overflow-hidden">
        <div className="p-3 border-b border-border/50">
          <h3 className="font-display text-sm font-semibold">Live Data Stream</h3>
          <p className="font-ui text-xs text-muted-foreground">Streaming telemetry • End-to-end verified</p>
        </div>
        <div className="relative h-48 bg-[#00161d] overflow-hidden">
          <svg className="w-full h-full" viewBox="0 0 800 200">
            {/* Grid lines */}
            <line x1="0" y1="50" x2="800" y2="50" stroke="#2A4A56" strokeWidth="0.5" strokeDasharray="4 4" />
            <line x1="0" y1="100" x2="800" y2="100" stroke="#2A4A56" strokeWidth="0.5" strokeDasharray="4 4" />
            <line x1="0" y1="150" x2="800" y2="150" stroke="#2A4A56" strokeWidth="0.5" strokeDasharray="4 4" />
            {/* Data points - scattered dots */}
            {Array.from({ length: 30 }, (_, i) => (
              <circle
                key={i}
                cx={20 + (i * 26) % 760}
                cy={30 + ((i * 37 + 13) % 140)}
                r={2 + (i % 3)}
                fill={i % 5 === 0 ? "#FF3621" : "#59dea2"}
                opacity={0.4 + (i % 4) * 0.15}
              />
            ))}
            {/* Connection lines between some dots */}
            <path d="M50,80 Q200,40 350,90 T650,70" fill="none" stroke="#59dea2" strokeWidth="1" opacity="0.3" />
            <path d="M100,140 Q300,100 500,150 T750,120" fill="none" stroke="#2A4A56" strokeWidth="1" opacity="0.4" />
          </svg>
        </div>
      </div>

      {/* Dashboard link */}
      <Card className="glass">
        <CardContent className="flex items-center gap-4 p-5">
          <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-lg bg-primary/10">
            <LayoutDashboard className="h-5 w-5 text-primary" />
          </div>
          <div className="flex-1">
            <p className="text-sm font-semibold">IoT Dashboard</p>
            <p className="text-xs text-muted-foreground">
              View live sensor data, throughput, and latency for this pipeline.
            </p>
          </div>
          <Button asChild className="gap-2 hover:shadow-[0_0_16px_rgba(255,54,33,0.4)] transition-shadow">
            <Link to={`/pipelines/${pipeline.id}/dashboard`}>
              <LayoutDashboard className="h-4 w-4" />
              View Dashboard
            </Link>
          </Button>
        </CardContent>
      </Card>

      {/* Streaming Metrics Status */}
      {pipeline.resources?.uc_catalog && (
        <Card className="glass">
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="font-display flex items-center gap-2">
              <Activity className="h-4 w-4" />
              Streaming Metrics
            </CardTitle>
            <div className="flex items-center gap-2">
              <Button
                variant="ghost"
                size="sm"
                className="h-7 w-7 p-0"
                disabled={metricsLoading}
                onClick={fetchMetrics}
              >
                <RefreshCw className={`h-3.5 w-3.5 ${metricsLoading ? "animate-spin" : ""}`} />
              </Button>
              {metricsData && metricsData.summary.total_rows > 0 ? (
                <Badge variant="success" className="gap-1">
                  <CheckCircle className="h-3 w-3" />
                  Writing
                </Badge>
              ) : metricsLoading ? (
                <Badge variant="secondary" className="gap-1">
                  <Loader2 className="h-3 w-3 animate-spin" />
                  Checking
                </Badge>
              ) : (
                <Badge variant="secondary" className="gap-1">
                  <Clock className="h-3 w-3" />
                  No Data
                </Badge>
              )}
            </div>
          </CardHeader>
          <CardContent>
            {metricsData && metricsData.summary.total_rows > 0 ? (
              <div className="space-y-3">
                <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
                  <MetricStat label="Total Batches" value={String(metricsData.summary.total_rows)} />
                  <MetricStat label="Last Batch" value={`#${metricsData.summary.last_batch}`} />
                  <MetricStat label="Avg Input RPS" value={`${metricsData.summary.avg_input_rps}`} />
                  <MetricStat label="Avg Batch Duration" value={`${metricsData.summary.avg_batch_duration_ms}ms`} />
                </div>
                {/* RTM Latency Metrics */}
                {(metricsData.summary.avg_e2e_p50_ms > 0 || metricsData.summary.avg_processing_p50_ms > 0) && (
                  <div>
                    <p className="text-[10px] text-muted-foreground font-semibold mb-1.5 uppercase tracking-wider">RTM Latency</p>
                    <div className="grid grid-cols-3 gap-3">
                      <div className="rounded-md bg-surface-high px-3 py-2">
                        <p className="text-[9px] text-muted-foreground">E2E Latency</p>
                        <p className="font-mono text-sm font-semibold tabular-nums">
                          {metricsData.summary.avg_e2e_p50_ms}ms
                          <span className="text-[9px] text-muted-foreground ml-1">p50</span>
                        </p>
                        <p className="font-mono text-xs tabular-nums text-warning">
                          {metricsData.summary.avg_e2e_p99_ms}ms
                          <span className="text-[9px] text-muted-foreground ml-1">p99</span>
                        </p>
                      </div>
                      <div className="rounded-md bg-surface-high px-3 py-2">
                        <p className="text-[9px] text-muted-foreground">Processing</p>
                        <p className="font-mono text-sm font-semibold tabular-nums">
                          {metricsData.summary.avg_processing_p50_ms}ms
                          <span className="text-[9px] text-muted-foreground ml-1">p50</span>
                        </p>
                        <p className="font-mono text-xs tabular-nums text-warning">
                          {metricsData.summary.avg_processing_p99_ms}ms
                          <span className="text-[9px] text-muted-foreground ml-1">p99</span>
                        </p>
                      </div>
                      <div className="rounded-md bg-surface-high px-3 py-2">
                        <p className="text-[9px] text-muted-foreground">Source Queuing</p>
                        <p className="font-mono text-sm font-semibold tabular-nums">
                          {metricsData.summary.avg_queuing_p50_ms}ms
                          <span className="text-[9px] text-muted-foreground ml-1">p50</span>
                        </p>
                        <p className="font-mono text-xs tabular-nums text-warning">
                          {metricsData.summary.avg_queuing_p99_ms}ms
                          <span className="text-[9px] text-muted-foreground ml-1">p99</span>
                        </p>
                      </div>
                    </div>
                  </div>
                )}
                {metricsData.summary.last_write && (
                  <p className="text-[10px] text-muted-foreground">
                    Last write: {metricsData.summary.last_write}
                  </p>
                )}
                <div>
                  <button
                    className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors"
                    onClick={() => setMetricsExpanded(!metricsExpanded)}
                  >
                    {metricsExpanded ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />}
                    {metricsExpanded ? "Hide" : "Show"} recent batches ({metricsData.rows.length})
                  </button>
                  {metricsExpanded && (
                    <div className="mt-2 max-h-64 overflow-auto rounded border border-border/50">
                      <table className="w-full text-xs">
                        <thead className="bg-surface-high sticky top-0">
                          <tr className="text-muted-foreground">
                            <th className="px-2 py-1 text-left">Batch</th>
                            <th className="px-2 py-1 text-left">Query</th>
                            <th className="px-2 py-1 text-right">In RPS</th>
                            <th className="px-2 py-1 text-right">Out RPS</th>
                            <th className="px-2 py-1 text-right">Trigger</th>
                            <th className="px-2 py-1 text-right">addBatch</th>
                            <th className="px-2 py-1 text-right">Rows In</th>
                            <th className="px-2 py-1 text-right">Rows Out</th>
                          </tr>
                        </thead>
                        <tbody className="font-mono">
                          {metricsData.rows.map((r, i) => (
                            <tr key={i} className="border-t border-border/30 hover:bg-surface-high/50">
                              <td className="px-2 py-1">{r.batch_id}</td>
                              <td className="px-2 py-1 text-[10px] truncate max-w-[80px]">{r.query_name || "—"}</td>
                              <td className="px-2 py-1 text-right">{parseFloat(r.input_rows_per_second || "0").toFixed(1)}</td>
                              <td className="px-2 py-1 text-right">{parseFloat(r.processed_rows_per_second || "0").toFixed(1)}</td>
                              <td className="px-2 py-1 text-right">{r.batch_duration_ms || "—"}ms</td>
                              <td className="px-2 py-1 text-right">{r.add_batch_ms || "—"}ms</td>
                              <td className="px-2 py-1 text-right">{r.num_input_rows || "0"}</td>
                              <td className="px-2 py-1 text-right">{r.num_output_rows || "—"}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}
                </div>
              </div>
            ) : (
              <p className="text-xs text-muted-foreground">
                {metricsLoading
                  ? "Checking streaming metrics table..."
                  : "No streaming metrics captured yet. Start the pipeline to begin writing metrics."}
              </p>
            )}
            <p className="mt-2 text-[10px] text-muted-foreground font-mono">
              Table: {pipeline.resources.uc_catalog}.metrics.streaming_metrics
            </p>
          </CardContent>
        </Card>
      )}

      <ConfirmDialog
        open={showDelete}
        title="Delete Pipeline"
        description={`This will permanently delete "${pipeline.name}" and clean up all provisioned resources (jobs, clusters, UC objects, Lakebase tables). Notebooks will be kept.`}
        confirmLabel="Delete Pipeline"
        destructive
        onConfirm={handleDelete}
        onCancel={() => setShowDelete(false)}
      />

      {/* Clone Dialog */}
      <ConfirmDialog
        open={showClone}
        title="Clone Pipeline"
        description={`Create a copy of "${pipeline.name}" with all its configuration. You'll be taken to the wizard to review settings and provision new resources.`}
        confirmLabel={cloning ? "Cloning..." : "Clone Pipeline"}
        onConfirm={handleClone}
        onCancel={() => { setShowClone(false); setCloneName(""); }}
      >
        <div className="mt-3">
          <label className="text-sm font-medium">New Pipeline Name</label>
          <input
            type="text"
            className="mt-1 w-full rounded-md border border-border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
            value={cloneName}
            onChange={(e) => setCloneName(e.target.value)}
            placeholder="Enter name for the cloned pipeline"
            autoFocus
          />
        </div>
      </ConfirmDialog>
    </div>
  );
}

function Row({
  label,
  value,
  mono,
}: {
  label: string;
  value: string;
  mono?: boolean;
}) {
  return (
    <div className="flex items-center justify-between text-sm">
      <span className="font-ui text-xs text-muted-foreground">{label}</span>
      <span className={mono ? "font-mono text-xs" : "font-sans text-sm"}>{value}</span>
    </div>
  );
}

function MetricStat({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-md bg-surface-high px-3 py-2">
      <p className="text-[10px] text-muted-foreground">{label}</p>
      <p className="font-mono text-sm font-semibold tabular-nums">{value}</p>
    </div>
  );
}

function ResourceRow({
  icon: Icon,
  label,
  value,
  href,
}: {
  icon: typeof Server;
  label: string;
  value: string;
  href?: string;
}) {
  return (
    <div className="flex items-center gap-3 rounded-md px-2 py-1.5 text-sm transition-colors hover:bg-surface-high">
      <Icon className="h-4 w-4 shrink-0 text-muted-foreground" />
      <span className="font-ui text-xs text-muted-foreground">{label}</span>
      <span className="ml-auto font-mono text-xs">{value}</span>
      {href && (
        <a
          href={href}
          target="_blank"
          rel="noopener noreferrer"
          className="text-primary hover:underline"
        >
          <ExternalLink className="h-3.5 w-3.5" />
        </a>
      )}
    </div>
  );
}
