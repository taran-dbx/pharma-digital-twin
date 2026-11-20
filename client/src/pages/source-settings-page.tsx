import { useState, useMemo, useCallback, useEffect, useRef } from "react";
import { Link } from "react-router-dom";
import {
  Zap,
  Database,
  Radio,
  Rabbit,
  Waves,
  Cloud,
  Globe,
  Server,
  HardDrive,
  Plus,
  Minus,
  CheckCircle2,
  Loader2,
  Circle,
  ArrowLeft,
  ArrowRight,
  Timer,
  Activity,
  Code2,
  Pencil,
  Eye,
  BarChart3,
  Layers,
  AlertCircle,
  AlertTriangle,
  TableProperties,
  Play,
  Shield,
  Rocket,
  ExternalLink,
  ChevronDown,
  Upload,
  Cpu,
  FileCheck,
  RefreshCw,
  Lightbulb,
  XCircle,
  Lock,
  Square,
} from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import { CollapsibleSection } from "@/components/collapsible-section";
import { cn } from "@/lib/utils";
import { ArchitectureCanvas } from "@/components/architecture-canvas";
import { KeyValueEditor, type KeyValuePair } from "@/components/key-value-editor";
import { LaketsSetup } from "@/components/lakets-setup";
import { generateNotebook } from "@/lib/notebook-template";
import { generateConnectivityTestNotebook } from "@/lib/connectivity-test-notebook";
import { generateDataProducerNotebook } from "@/lib/data-producer-notebook";
import { api, type AppDefaults } from "@/lib/api";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
  DialogDescription,
} from "@/components/ui/dialog";
import { Settings2 } from "lucide-react";
import { toast } from "sonner";
import type {
  PipelineType,
  SssProcessingMode,
  SssRtmSinkType,
  SssMicrobatchSink,
  SssSource,
  SssConfig,
  ClusterState,
  LakebaseConnectionMode,
  ProvisionStatus,
  NodeType,
  SparkVersion,
  CloudProvider,
  PipelineResources,
  TaskStatus,
  UserSettings,
  TieringConfig,
} from "@/lib/types";

interface SourceAdvancedOption {
  key: string;
  label: string;
  defaultValue: string;
  type: "text" | "select";
  options?: string[];
}

const SOURCE_ADVANCED_OPTIONS: Record<SssSource, SourceAdvancedOption[]> = {
  kafka: [
    { key: "startingOffsets", label: "Starting Offsets", defaultValue: "latest", type: "select", options: ["latest", "earliest"] },
    { key: "failOnDataLoss", label: "Fail on Data Loss", defaultValue: "true", type: "select", options: ["true", "false"] },
    { key: "maxOffsetsPerTrigger", label: "Max Offsets/Trigger", defaultValue: "", type: "text" },
    { key: "includeHeaders", label: "Include Headers", defaultValue: "false", type: "select", options: ["true", "false"] },
    { key: "minPartitions", label: "Min Partitions", defaultValue: "", type: "text" },
  ],
  kinesis: [
    { key: "initialPosition", label: "Initial Position", defaultValue: "latest", type: "select", options: ["latest", "trim_horizon", "earliest"] },
    { key: "consumerMode", label: "Consumer Mode", defaultValue: "polling", type: "select", options: ["polling", "EFO"] },
    { key: "maxFetchTime", label: "Max Fetch Time", defaultValue: "", type: "text" },
    { key: "maxFetchRecordsPerShard", label: "Max Records/Shard", defaultValue: "", type: "text" },
  ],
  event_hub: [
    { key: "startingPosition", label: "Starting Position", defaultValue: "latest", type: "select", options: ["latest", "earliest"] },
    { key: "maxEventsPerTrigger", label: "Max Events/Trigger", defaultValue: "", type: "text" },
    { key: "receiverTimeout", label: "Receiver Timeout", defaultValue: "", type: "text" },
    { key: "operationTimeout", label: "Operation Timeout", defaultValue: "", type: "text" },
  ],
  rabbitmq: [
    { key: "prefetchCount", label: "Prefetch Count", defaultValue: "", type: "text" },
    { key: "durable", label: "Durable", defaultValue: "true", type: "select", options: ["true", "false"] },
    { key: "autoAck", label: "Auto Ack", defaultValue: "false", type: "select", options: ["true", "false"] },
  ],
  pubsub: [
    { key: "maxBytesPerTrigger", label: "Max Bytes/Trigger", defaultValue: "", type: "text" },
    { key: "maxMessagesPerBatch", label: "Max Messages/Batch", defaultValue: "", type: "text" },
  ],
  zerobus: [],
};

const pipelines: Array<{
  id: PipelineType;
  label: string;
  description: string;
  icon: typeof Zap;
}> = [
  {
    id: "sss",
    label: "Spark Structured Streaming",
    description: "Consume from external sources via Databricks clusters",
    icon: Zap,
  },
  {
    id: "zerobus",
    label: "Zerobus Delta Ingest",
    description: "Direct gRPC ingestion into Delta tables",
    icon: Database,
  },
  {
    id: "websocket_demo",
    label: "WebSocket Demo",
    description: "Live preview in browser, no infrastructure needed",
    icon: Radio,
  },
];

const sssSources: Array<{
  id: SssSource;
  label: string;
  description: string;
  icon: typeof Radio;
}> = [
  { id: "kafka", label: "Apache Kafka", description: "Confluent Cloud or self-managed", icon: Radio },
  { id: "kinesis", label: "Amazon Kinesis", description: "AWS Kinesis Data Streams", icon: Waves },
  { id: "event_hub", label: "Azure Event Hub", description: "Azure Event Hubs", icon: Cloud },
  { id: "rabbitmq", label: "RabbitMQ", description: "AMQP message broker", icon: Rabbit },
  { id: "pubsub", label: "Google Pub/Sub", description: "GCP Pub/Sub", icon: Globe },
];

const provisioningSteps = [
  { key: "creating", label: "Creating Cluster" },
  { key: "starting", label: "Starting Cluster" },
  { key: "running", label: "Cluster Ready" },
];

const PREDEFINED_METRICS = [
  "input_rows_per_second",
  "processed_rows_per_second",
  "batch_duration_ms",
  "num_input_rows",
  "batch_id",
];

function StepIndicator({
  step,
  currentState,
  index,
}: {
  step: { key: string; label: string };
  currentState: ClusterState;
  index: number;
}) {
  const stateOrder = provisioningSteps.map((s) => s.key);
  const currentIndex = stateOrder.indexOf(currentState);
  const isCompleted = currentIndex > index || (currentState === "running" && currentIndex === index);
  const isActive = currentIndex === index && currentState !== "running";

  return (
    <div className="flex items-center gap-3">
      {isCompleted ? (
        <CheckCircle2 className="h-6 w-6 text-success" />
      ) : isActive ? (
        <Loader2 className="h-6 w-6 animate-spin text-primary" />
      ) : (
        <Circle className="h-6 w-6 text-muted-foreground/30" />
      )}
      <span
        className={cn(
          "text-sm font-medium",
          isCompleted ? "text-success" : isActive ? "text-foreground" : "text-muted-foreground/50"
        )}
      >
        {step.label}
      </span>
    </div>
  );
}

function InputField({
  label,
  placeholder,
  value,
  onChange,
  colSpan,
  type = "text",
}: {
  label: string;
  placeholder: string;
  value: string;
  onChange: (v: string) => void;
  colSpan?: number;
  type?: string;
}) {
  return (
    <div className={colSpan === 2 ? "sm:col-span-2" : ""}>
      <label className="mb-1.5 block text-xs font-medium uppercase tracking-wider text-muted-foreground">
        {label}
      </label>
      <input
        type={type}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring"
      />
    </div>
  );
}

function SelectField({
  label,
  value,
  onChange,
  options,
  richOptions,
  disabled,
  loading,
}: {
  label: string;
  value: string;
  onChange: (v: string) => void;
  options?: string[];
  richOptions?: Array<{ value: string; label: string }>;
  disabled?: boolean;
  loading?: boolean;
}) {
  const items = richOptions ?? (options || []).map((o) => ({ value: o, label: o }));
  return (
    <div>
      <label className="mb-1.5 block text-xs font-medium uppercase tracking-wider text-muted-foreground">
        {label}
      </label>
      {loading ? (
        <div className="flex h-[38px] items-center gap-2 rounded-md border border-input bg-background px-3 text-sm text-muted-foreground">
          <Loader2 className="h-3.5 w-3.5 animate-spin" />
          Loading...
        </div>
      ) : (
        <select
          value={value}
          onChange={(e) => onChange(e.target.value)}
          disabled={disabled}
          className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm focus:outline-none focus:ring-1 focus:ring-ring disabled:cursor-not-allowed disabled:opacity-50"
        >
          {items.map((o) => (
            <option key={o.value} value={o.value}>{o.label}</option>
          ))}
        </select>
      )}
    </div>
  );
}

export interface SourceSettingsPageProps {
  pipelineId?: string | null;
  pipelineResources?: PipelineResources;
  onValidityChange?: (valid: boolean) => void;
  onJobRunStateChange?: (state: "idle" | "polling" | "running" | "stopped" | "error") => void;
  onPipelineTypeChange?: (pipelineType: PipelineType) => void;
}

const CONFIG_SUB_STEPS = ["Source", "Sink", "Metrics", "Notebook & Deploy"] as const;

export function SourceSettingsPage({ pipelineId, pipelineResources, onValidityChange, onJobRunStateChange, onPipelineTypeChange }: SourceSettingsPageProps = {}) {
  const [configStep, setConfigStep] = useState(0);
  const [appDefaults, setAppDefaults] = useState<AppDefaults | null>(null);
  const [globalSettings, setGlobalSettings] = useState<Partial<UserSettings> | null>(null);

  useEffect(() => {
    api.getAppDefaults().then(setAppDefaults).catch(() => {});
    api.getUserSettings().then(setGlobalSettings).catch(() => {});
  }, []);

  const [selectedPipeline, setSelectedPipeline] = useState<PipelineType | null>(null);
  const [processingMode, setProcessingMode] = useState<SssProcessingMode>("realtime");
  const [selectedSource, setSelectedSource] = useState<SssSource | null>(null);

  // Connection state
  const [kafkaBootstrap, setKafkaBootstrap] = useState("");
  const [kafkaTopic, setKafkaTopic] = useState("");
  const [kafkaGroup, setKafkaGroup] = useState("");
  const [kafkaSecurity, setKafkaSecurity] = useState("SASL_SSL");
  const [kinesisStream, setKinesisStream] = useState("");
  const [kinesisRegion, setKinesisRegion] = useState("us-east-1");
  const [ehConnString, setEhConnString] = useState("");
  const [ehName, setEhName] = useState("");
  const [rmqHost, setRmqHost] = useState("");
  const [rmqPort, setRmqPort] = useState("5672");
  const [rmqQueue, setRmqQueue] = useState("");
  const [rmqUser, setRmqUser] = useState("");
  const [psProjectId, setPsProjectId] = useState("");
  const [psSubscription, setPsSubscription] = useState("");
  const [psTopicId, setPsTopicId] = useState("");

  // Source parallelism (partitions/shards/consumers/streams)
  const [kafkaPartitions, setKafkaPartitions] = useState(6);
  const [kinesisShards, setKinesisShards] = useState(2);
  const [ehPartitions, setEhPartitions] = useState(4);
  const [rmqConsumers, setRmqConsumers] = useState(1);
  const [psMaxStreams, setPsMaxStreams] = useState(1);

  // UC service credentials per source
  const [sourceCredentials, setSourceCredentials] = useState<Record<string, string>>({});
  const setSourceCred = (src: string, val: string) =>
    setSourceCredentials((prev) => ({ ...prev, [src]: val }));

  // Source advanced settings
  const [sourceAdvancedConfig, setSourceAdvancedConfig] = useState<Record<string, Record<string, string>>>({});
  const [advancedDialogOpen, setAdvancedDialogOpen] = useState(false);
  const [advancedDialogDraft, setAdvancedDialogDraft] = useState<Record<string, string>>({});

  // Sink
  const [sinkTable, setSinkTable] = useState("");
  const [checkpointLocation, setCheckpointLocation] = useState("");

  // RTM sink
  const [rtmSink, setRtmSink] = useState<SssRtmSinkType>("lakebase");
  const [microbatchSinks, setMicrobatchSinks] = useState<Set<SssMicrobatchSink>>(new Set(["delta"]));
  const [lakebaseBufferSize, setLakebaseBufferSize] = useState(50);
  const [wsBufferSize, setWsBufferSize] = useState(20);
  const [lakebaseMode, setLakebaseMode] = useState<LakebaseConnectionMode>("autoscale");
  const [lbHost, setLbHost] = useState("");
  const [lbPort, setLbPort] = useState("5432");
  const [lbDatabase, setLbDatabase] = useState("databricks_postgres");
  const [lbUser, setLbUser] = useState("");
  const [lbPassword] = useState("");  // kept for sssConfig/provision calls
  const [lbProjectId, setLbProjectId] = useState("");
  const [lbBranchId, setLbBranchId] = useState("production");
  const [lbPgTable, setLbPgTable] = useState("sensor_readings");
  // LakeTS tier policy edited in the one-click Lakebase setup; null → no tiering.
  const [lbTiering, setLbTiering] = useState<TieringConfig | null>(null);
  // Owning pipeline's industry (for the one-click setup payload).
  const [pipelineIndustry, setPipelineIndustry] = useState<string | null>(null);
  const [wsAppUrl, setWsAppUrl] = useState("");

  // Provisioning status
  const [sinkUcStatus, setSinkUcStatus] = useState<ProvisionStatus>("idle");
  const [sinkUcError, setSinkUcError] = useState<string | null>(null);
  const [metricsUcStatus, setMetricsUcStatus] = useState<ProvisionStatus>("idle");
  const [metricsUcError, setMetricsUcError] = useState<string | null>(null);
  const [pgTableStatus, setPgTableStatus] = useState<ProvisionStatus>("idle");
  const [pgTableError, setPgTableError] = useState<string | null>(null);

  // Metrics — always on by admin default (no user-facing on/off toggle).
  const [metricsEnabled, setMetricsEnabled] = useState(true);
  const [metricsTable, setMetricsTable] = useState("");
  const [customMetrics, setCustomMetrics] = useState<KeyValuePair[]>([]);

  // Cluster
  const [workers, setWorkers] = useState(2);
  const [workerNodeType, setWorkerNodeType] = useState("");
  const [dbrVersion, setDbrVersion] = useState("");
  const [autoTerminate, setAutoTerminate] = useState(30);
  const [clusterState, setClusterState] = useState<ClusterState>("idle");
  const [clusterId, setClusterId] = useState<string | null>(null);
  const [clusterError, setClusterError] = useState<string | null>(null);
  const clusterReady = clusterState === "running" || clusterState === "submitting_job" || clusterState === "job_running";
  const pollingRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const sourceFieldsFilled = useMemo(() => {
    if (!selectedSource) return false;
    switch (selectedSource) {
      case "kafka": return !!(kafkaBootstrap.trim() && kafkaTopic.trim() && kafkaGroup.trim() && kafkaSecurity);
      case "kinesis": return !!(kinesisStream.trim() && kinesisRegion.trim());
      case "event_hub": return !!(ehConnString.trim() && ehName.trim());
      case "rabbitmq": return !!(rmqHost.trim() && rmqPort.trim() && rmqQueue.trim() && rmqUser.trim());
      case "pubsub": return !!(psProjectId.trim() && psSubscription.trim());
      default: return false;
    }
  }, [selectedSource, kafkaBootstrap, kafkaTopic, kafkaGroup, kafkaSecurity, kinesisStream, kinesisRegion, ehConnString, ehName, rmqHost, rmqPort, rmqQueue, rmqUser, psProjectId, psSubscription]);

  const sinkFieldsFilled = useMemo(() => {
    if (processingMode === "realtime") return !!rtmSink;
    return microbatchSinks.size > 0 && (!microbatchSinks.has("delta") || !!sinkTable.trim());
  }, [processingMode, rtmSink, microbatchSinks, sinkTable]);

  const subStep0Valid = useMemo(() => !!processingMode && sourceFieldsFilled, [processingMode, sourceFieldsFilled]);
  const subStep1Valid = useMemo(() => sinkFieldsFilled, [sinkFieldsFilled]);
  const subStep2Valid = useMemo(() => !metricsEnabled || !!metricsTable.trim(), [metricsEnabled, metricsTable]);
  const subStepValid = [subStep0Valid, subStep1Valid, subStep2Valid, true];
  const maxReachableStep = useMemo(() => {
    for (let i = 0; i < subStepValid.length; i++) {
      if (!subStepValid[i]) return i;
    }
    return subStepValid.length - 1;
  }, [subStep0Valid, subStep1Valid, subStep2Valid]); // eslint-disable-line react-hooks/exhaustive-deps

  const canValidate = clusterReady && sourceFieldsFilled && sinkFieldsFilled;

  // Per-task job status
  const [jobTaskStates, setJobTaskStates] = useState<Record<string, TaskStatus> | null>(null);

  // Load existing cluster from pipeline resources
  useEffect(() => {
    if (!pipelineResources?.cluster_id || clusterId) return;
    const cid = pipelineResources.cluster_id;
    setClusterId(cid);
    setClusterState("starting");
    api.getClusterStatus(cid).then((resp) => {
      const s = resp.state;
      if (s === "RUNNING") {
        setClusterState("running");
      } else if (s === "TERMINATED" || s === "TERMINATING") {
        setClusterState("terminated");
      } else if (s === "ERROR") {
        setClusterState("error");
        setClusterError(resp.state_message || "Cluster is in error state");
      } else {
        setClusterState("starting");
        startClusterPolling(cid);
      }
    }).catch(() => {
      setClusterState("idle");
      setClusterId(null);
    });
  }, [pipelineResources?.cluster_id]); // eslint-disable-line react-hooks/exhaustive-deps

  // Dynamic node types and spark versions
  const [nodeTypes, setNodeTypes] = useState<NodeType[]>([]);
  const [nodeTypesLoading, setNodeTypesLoading] = useState(false);
  const [sparkVersions, setSparkVersions] = useState<SparkVersion[]>([]);
  const [sparkVersionsLoading, setSparkVersionsLoading] = useState(false);
  const [cloudProvider, setCloudProvider] = useState<CloudProvider>("unknown");

  // Source parallelism helper
  const getSourceParallelism = useCallback((): number => {
    switch (selectedSource) {
      case "kafka": return kafkaPartitions;
      case "kinesis": return kinesisShards;
      case "event_hub": return ehPartitions;
      case "rabbitmq": return rmqConsumers;
      case "pubsub": return psMaxStreams;
      default: return 2;
    }
  }, [selectedSource, kafkaPartitions, kinesisShards, ehPartitions, rmqConsumers, psMaxStreams]);

  const getRecommendedWorkers = useCallback((parallelism: number, coresPerNode: number): number => {
    return Math.max(1, Math.ceil(parallelism / coresPerNode));
  }, []);

  // Auto-calculate workers when source parallelism or node type changes
  const [workersManualOverride, setWorkersManualOverride] = useState(false);
  useEffect(() => {
    if (workersManualOverride || !selectedSource || !workerNodeType) return;
    const nodeType = nodeTypes.find((nt) => nt.node_type_id === workerNodeType);
    if (!nodeType) return;
    const parallelism = getSourceParallelism();
    const recommended = getRecommendedWorkers(parallelism, nodeType.num_cores);
    setWorkers(recommended);
  }, [selectedSource, workerNodeType, nodeTypes, getSourceParallelism, getRecommendedWorkers, workersManualOverride]);

  const recommendedWorkers = useMemo(() => {
    if (!selectedSource || !workerNodeType) return null;
    const nodeType = nodeTypes.find((nt) => nt.node_type_id === workerNodeType);
    if (!nodeType) return null;
    const parallelism = getSourceParallelism();
    return {
      workers: getRecommendedWorkers(parallelism, nodeType.num_cores),
      parallelism,
      coresPerNode: nodeType.num_cores,
    };
  }, [selectedSource, workerNodeType, nodeTypes, getSourceParallelism, getRecommendedWorkers]);

  // Validate state
  const [validateState, setValidateState] = useState<"idle" | "running" | "success" | "error" | "timeout">("idle");
  const [validatePhase, setValidatePhase] = useState(0);
  const [validateOutput, setValidateOutput] = useState<string | null>(null);
  const [validateError, setValidateError] = useState<string | null>(null);
  const [validateOutputCollapsed, setValidateOutputCollapsed] = useState(true);

  // Run state (longer timeout, reuses similar feedback UI)
  const [runState, setRunState] = useState<"idle" | "running" | "success" | "error">("idle");
  const [runPhase, setRunPhase] = useState(0);
  const [runOutput, setRunOutput] = useState<string | null>(null);
  const [runError, setRunError] = useState<string | null>(null);
  const [runOutputCollapsed, setRunOutputCollapsed] = useState(true);

  // Source connectivity test state (serverless)
  const [connectTestState, setConnectTestState] = useState<"idle" | "testing" | "success" | "error">("idle");
  const [connectTestMsg, setConnectTestMsg] = useState<string | null>(null);
  const [connectTestPhase, setConnectTestPhase] = useState(0);
  const [connectTestResult, setConnectTestResult] = useState<{
    success: boolean; source: string;
    write_test?: string; read_test?: string;
    message?: string; error?: string; hint?: string;
    diagnostics?: {
      dns_ok?: boolean; resolved_ips?: string[];
      is_private_ip?: boolean; tcp_reachable?: boolean;
      ssl_ok?: boolean | null;
      checks?: Array<{ name: string; ok: boolean; detail: string }>;
    };
  } | null>(null);
  const connectPollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Deploy state
  const [deployState, setDeployState] = useState<"idle" | "deploying" | "success" | "error">("idle");
  const [deployError, setDeployError] = useState<string | null>(null);
  const [deployJobUrl, setDeployJobUrl] = useState<string | null>(null);
  const [deployedJobId, setDeployedJobId] = useState<number | null>(null);

  // Job run state (polled after deploy)
  const [jobRunState, setJobRunState] = useState<"idle" | "polling" | "running" | "stopped" | "error">("idle");
  const [jobRunMessage, setJobRunMessage] = useState<string>("");
  const jobPollRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const [jobActionLoading, setJobActionLoading] = useState(false);

  // Cluster-level Spark conf (applied at cluster creation/provisioning)
  const [clusterSparkConf, setClusterSparkConf] = useState<KeyValuePair[]>([
    { key: "spark.databricks.streaming.realTimeMode.enabled", value: "true" },
  ]);

  // Notebook-level Spark conf (applied via spark.conf.set() at runtime)
  const [notebookSparkConf, setNotebookSparkConf] = useState<KeyValuePair[]>([]);

  // Trigger interval — auto-updated when processing mode changes
  const [triggerInterval, setTriggerInterval] = useState("5 minutes");

  // Report config validity to parent wizard — requires successful validation or run
  // Note: Zerobus validity is handled in a separate effect after zbEndpoint/zbTable are declared
  useEffect(() => {
    if (!onValidityChange) return;
    if (selectedPipeline === "zerobus") return; // handled below
    const hasPipeline = selectedPipeline !== null;
    const hasSource = selectedSource !== null;
    const hasSink =
      processingMode === "realtime"
        ? !!rtmSink
        : microbatchSinks.size > 0 && (!microbatchSinks.has("delta") || !!sinkTable.trim());
    const validated = validateState === "success" || validateState === "timeout" || runState === "success";
    onValidityChange(hasPipeline && hasSource && hasSink && validated);
  }, [selectedPipeline, selectedSource, processingMode, rtmSink, microbatchSinks, sinkTable, onValidityChange, validateState, runState]);

  useEffect(() => {
    onJobRunStateChange?.(jobRunState);
  }, [jobRunState, onJobRunStateChange]);

  useEffect(() => {
    if (selectedPipeline) onPipelineTypeChange?.(selectedPipeline);
  }, [selectedPipeline, onPipelineTypeChange]);

  // Auto-update trigger interval when processing mode changes
  useEffect(() => {
    setTriggerInterval(processingMode === "realtime" ? "5 minutes" : "10 seconds");
  }, [processingMode]);

  // Fetch node types and spark versions when SSS pipeline is selected
  useEffect(() => {
    if (selectedPipeline !== "sss") return;

    let cancelled = false;

    async function fetchClusterMetadata() {
      setNodeTypesLoading(true);
      setSparkVersionsLoading(true);
      try {
        const [ntResp, svResp] = await Promise.all([
          api.getNodeTypes(),
          api.getSparkVersions(),
        ]);
        if (cancelled) return;

        setCloudProvider(ntResp.cloud);
        setNodeTypes(ntResp.node_types);
        if (ntResp.node_types.length > 0 && !workerNodeType) {
          setWorkerNodeType(ntResp.node_types[0].node_type_id);
        }

        setSparkVersions(svResp.spark_versions);
        if (svResp.spark_versions.length > 0 && !dbrVersion) {
          setDbrVersion(svResp.spark_versions[0].key);
        }
      } catch {
        // Silently fail — dropdowns will stay empty with loading state cleared
      } finally {
        if (!cancelled) {
          setNodeTypesLoading(false);
          setSparkVersionsLoading(false);
        }
      }
    }

    fetchClusterMetadata();
    return () => { cancelled = true; };
  }, [selectedPipeline]); // eslint-disable-line react-hooks/exhaustive-deps

  // Cleanup polling on unmount
  useEffect(() => {
    return () => {
      if (pollingRef.current) clearInterval(pollingRef.current);
      if (connectPollRef.current) clearInterval(connectPollRef.current);
    };
  }, []);

  // Notebook
  const [notebookEditing, setNotebookEditing] = useState(false);
  const [notebookOverride, setNotebookOverride] = useState<string | null>(null);

  // Zerobus
  const [zbEndpoint, setZbEndpoint] = useState("");
  const [zbTable, setZbTable] = useState("");
  const [zbClientId, setZbClientId] = useState("");
  const [zbClientSecret, setZbClientSecret] = useState("");

  const isLakebaseActive =
    (processingMode === "realtime" && (rtmSink === "lakebase" || rtmSink === "both")) ||
    (processingMode === "microbatch" && microbatchSinks.has("lakebase"));

  const isWsActive =
    (processingMode === "realtime" && (rtmSink === "websocket" || rtmSink === "both")) ||
    (processingMode === "microbatch" && microbatchSinks.has("websocket"));

  useEffect(() => {
    if (isWsActive && !wsAppUrl) {
      setWsAppUrl(window.location.origin);
    }
  }, [isWsActive]); // eslint-disable-line react-hooks/exhaustive-deps

  const pipelineCatalog = pipelineResources?.uc_catalog;

  useEffect(() => {
    if (!pipelineCatalog) return;
    if (!sinkTable) setSinkTable(`${pipelineCatalog}.raw.sensor_data`);
    if (!checkpointLocation) setCheckpointLocation(`/Volumes/${pipelineCatalog}/checkpoints/stream_checkpoints/sss`);
  }, [pipelineCatalog]); // eslint-disable-line react-hooks/exhaustive-deps

  // Auto-populate Zerobus + Lakebase autoscale fields from appDefaults
  useEffect(() => {
    if (!appDefaults) return;
    if (!zbEndpoint && appDefaults.zerobus_endpoint) setZbEndpoint(appDefaults.zerobus_endpoint);
    if (!zbClientId && appDefaults.zerobus_client_id) setZbClientId(appDefaults.zerobus_client_id);
    if (!zbClientSecret && appDefaults.zerobus_client_secret) setZbClientSecret(appDefaults.zerobus_client_secret);
    // Pre-fill the Lakebase autoscale project from the deployed app's own
    // LAKEBASE_ENDPOINT so new pipelines aren't saddled with the
    // `<PROJECT_ID>` placeholder in the generated notebook.
    if (!lbProjectId && appDefaults.lakebase_project_id) setLbProjectId(appDefaults.lakebase_project_id);
    if (lbBranchId === "production" && appDefaults.lakebase_branch_id) setLbBranchId(appDefaults.lakebase_branch_id);
    if (!lbDatabase && appDefaults.lakebase_database) setLbDatabase(appDefaults.lakebase_database);
  }, [appDefaults]); // eslint-disable-line react-hooks/exhaustive-deps

  // Auto-populate Zerobus target table from pipeline catalog
  useEffect(() => {
    if (!zbTable && pipelineCatalog) setZbTable(`${pipelineCatalog}.raw.iot_sensor_data`);
  }, [pipelineCatalog]); // eslint-disable-line react-hooks/exhaustive-deps

  // Zerobus validity — separate effect because zbEndpoint/zbTable are declared after the main validity effect
  useEffect(() => {
    if (!onValidityChange || selectedPipeline !== "zerobus") return;
    onValidityChange(!!(zbEndpoint.trim() && zbTable.trim()));
  }, [selectedPipeline, zbEndpoint, zbTable, onValidityChange]);

  useEffect(() => {
    if (metricsEnabled && !metricsTable && pipelineCatalog) {
      setMetricsTable(`${pipelineCatalog}.metrics.streaming_metrics`);
    }
  }, [metricsEnabled, pipelineCatalog]); // eslint-disable-line react-hooks/exhaustive-deps

  const savePipelineConfig = useCallback(async () => {
    if (!pipelineId) return;
    try {
      await api.updatePipeline(pipelineId, {
        source_type: selectedPipeline ?? undefined,
        source_config: {
          processing_mode: processingMode,
          source: selectedSource,
          kafka: { bootstrap_servers: kafkaBootstrap, topic: kafkaTopic, consumer_group: kafkaGroup, security_protocol: kafkaSecurity, num_partitions: kafkaPartitions },
          kinesis: { stream_name: kinesisStream, region: kinesisRegion, num_shards: kinesisShards },
          event_hub: { connection_string: ehConnString, event_hub_name: ehName, num_partitions: ehPartitions },
          rabbitmq: { host: rmqHost, port: rmqPort, queue: rmqQueue, username: rmqUser, num_consumers: rmqConsumers },
          pubsub: { project_id: psProjectId, subscription: psSubscription, topic_id: psTopicId, max_streams: psMaxStreams },
          source_credentials: sourceCredentials,
          source_advanced: sourceAdvancedConfig,
          sink_table: sinkTable,
          checkpoint_location: checkpointLocation,
          rtm_sink: rtmSink,
          microbatch_sinks: Array.from(microbatchSinks),
          lakebase_mode: lakebaseMode,
          lb_host: lbHost, lb_port: lbPort, lb_database: lbDatabase, lb_user: lbUser,
          lb_project_id: lbProjectId, lb_branch_id: lbBranchId,
          lb_pg_table: lbPgTable,
          ws_app_url: wsAppUrl, lakebase_buffer_size: lakebaseBufferSize, ws_buffer_size: wsBufferSize,
          metrics_enabled: metricsEnabled, metrics_table: metricsTable,
          custom_metrics: customMetrics.map((p) => ({ name: p.key, expression: p.value })),
          workers, worker_node_type: workerNodeType, dbr_version: dbrVersion, auto_terminate: autoTerminate,
          notebook_spark_conf: notebookSparkConf.filter((p) => p.key).map((p) => ({ key: p.key, value: p.value })),
          cluster_spark_conf: clusterSparkConf.filter((p) => p.key).map((p) => ({ key: p.key, value: p.value })),
          notebook_override: notebookOverride !== generatedNotebook ? notebookOverride : null,
          trigger_interval: triggerInterval,
          config_step: configStep,
        } as Record<string, unknown>,
        resources: {
          secret_scope: globalSettings?.secrets?.scope || "iot-secrets",
        },
      });

      if (lakebaseMode === "direct" && lbPassword) {
        const secrets: Record<string, string> = { "lb-password": lbPassword };
        if (lbUser) secrets["lb-user"] = lbUser;
        api.putPipelineSecrets(pipelineId, secrets).catch(() => {});
      }

      // If pipeline had a running job, reset it (stop job/cluster, require re-deploy)
      if (pipelineResources?.job_id) {
        try {
          await api.resetPipeline(pipelineId);
          toast.success("Config saved. Previous job stopped — re-deploy to apply new settings.");
          // Reset local deploy state
          setDeployedJobId(null);
          setDeployState("idle");
          setJobRunState("idle");
          setJobTaskStates({});
        } catch {
          toast.info("Config saved.");
        }
      } else {
        toast.success("Config saved.");
      }
    } catch {
      toast.error("Failed to save config.");
    }
  }, [pipelineId, selectedPipeline, processingMode, selectedSource, kafkaBootstrap, kafkaTopic, kafkaGroup, kafkaSecurity, kinesisStream, kinesisRegion, ehConnString, ehName, rmqHost, rmqPort, rmqQueue, rmqUser, psProjectId, psSubscription, sourceCredentials, sourceAdvancedConfig, sinkTable, checkpointLocation, rtmSink, microbatchSinks, lakebaseMode, lbHost, lbPort, lbDatabase, lbUser, lbPassword, lbProjectId, lbBranchId, lbPgTable, wsAppUrl, lakebaseBufferSize, wsBufferSize, metricsEnabled, metricsTable, customMetrics, workers, workerNodeType, dbrVersion, autoTerminate, notebookSparkConf, clusterSparkConf, notebookOverride, configStep, globalSettings?.secrets?.scope]);

  const configRestored = useRef(false);
  useEffect(() => {
    if (!pipelineId || configRestored.current) return;
    configRestored.current = true;
    api.getPipeline(pipelineId).then((p) => {
      if (p.industry) setPipelineIndustry(p.industry);
      const sinkCfg = p.sink_config as Record<string, unknown> | null;
      const t = sinkCfg?.tiering as Record<string, string> | undefined;
      if (t && typeof t.hot === "string" && typeof t.drop === "string" && typeof t.chunk === "string") {
        setLbTiering({ hot: t.hot, drop: t.drop, chunk: t.chunk });
      }
      const cfg = p.source_config as Record<string, unknown> | null;
      if (!cfg) return;
      if (p.source_type) setSelectedPipeline(p.source_type as PipelineType);
      if (cfg.processing_mode) setProcessingMode(cfg.processing_mode as SssProcessingMode);
      if (cfg.source) setSelectedSource(cfg.source as SssSource);
      const k = cfg.kafka as Record<string, string> | undefined;
      if (k?.bootstrap_servers) { setKafkaBootstrap(k.bootstrap_servers); setKafkaTopic(k.topic || ""); setKafkaGroup(k.consumer_group || ""); setKafkaSecurity(k.security_protocol || "SASL_SSL"); }
      if (k?.num_partitions) setKafkaPartitions(Number(k.num_partitions));
      const kin = cfg.kinesis as Record<string, string> | undefined;
      if (kin?.stream_name) { setKinesisStream(kin.stream_name); setKinesisRegion(kin.region || "us-east-1"); }
      if (kin?.num_shards) setKinesisShards(Number(kin.num_shards));
      const eh = cfg.event_hub as Record<string, string> | undefined;
      if (eh?.connection_string) { setEhConnString(eh.connection_string); setEhName(eh.event_hub_name || ""); }
      if (eh?.num_partitions) setEhPartitions(Number(eh.num_partitions));
      const rmq = cfg.rabbitmq as Record<string, string> | undefined;
      if (rmq?.host) { setRmqHost(rmq.host); setRmqPort(rmq.port || "5672"); setRmqQueue(rmq.queue || ""); setRmqUser(rmq.username || ""); }
      if (rmq?.num_consumers) setRmqConsumers(Number(rmq.num_consumers));
      const ps = cfg.pubsub as Record<string, string> | undefined;
      if (ps?.project_id) { setPsProjectId(ps.project_id); setPsSubscription(ps.subscription || ""); setPsTopicId(ps.topic_id || ""); }
      if (ps?.max_streams) setPsMaxStreams(Number(ps.max_streams));
      if (cfg.source_credentials) setSourceCredentials(cfg.source_credentials as Record<string, string>);
      if (cfg.source_advanced) setSourceAdvancedConfig(cfg.source_advanced as Record<string, Record<string, string>>);
      if (cfg.sink_table) setSinkTable(cfg.sink_table as string);
      if (cfg.checkpoint_location) setCheckpointLocation(cfg.checkpoint_location as string);
      if (cfg.rtm_sink) setRtmSink(cfg.rtm_sink as SssRtmSinkType);
      if (cfg.microbatch_sinks) setMicrobatchSinks(new Set(cfg.microbatch_sinks as SssMicrobatchSink[]));
      if (cfg.lakebase_mode) setLakebaseMode(cfg.lakebase_mode as LakebaseConnectionMode);
      if (cfg.lb_host) setLbHost(cfg.lb_host as string);
      if (cfg.lb_port) setLbPort(cfg.lb_port as string);
      if (cfg.lb_database) setLbDatabase(cfg.lb_database as string);
      if (cfg.lb_user) setLbUser(cfg.lb_user as string);
      if (cfg.lb_project_id) setLbProjectId(cfg.lb_project_id as string);
      if (cfg.lb_branch_id) setLbBranchId(cfg.lb_branch_id as string);
      if (cfg.lb_pg_table) setLbPgTable(cfg.lb_pg_table as string);
      if (cfg.ws_app_url) setWsAppUrl(cfg.ws_app_url as string);
      if (typeof cfg.lakebase_buffer_size === "number") setLakebaseBufferSize(cfg.lakebase_buffer_size);
      if (typeof cfg.ws_buffer_size === "number") setWsBufferSize(cfg.ws_buffer_size);
      if (typeof cfg.metrics_enabled === "boolean") setMetricsEnabled(cfg.metrics_enabled);
      if (cfg.metrics_table) setMetricsTable(cfg.metrics_table as string);
      if (cfg.custom_metrics) setCustomMetrics((cfg.custom_metrics as Array<{ name: string; expression: string }>).map((m) => ({ key: m.name, value: m.expression })));
      if (typeof cfg.workers === "number") setWorkers(cfg.workers);
      if (cfg.worker_node_type) setWorkerNodeType(cfg.worker_node_type as string);
      if (cfg.dbr_version) setDbrVersion(cfg.dbr_version as string);
      if (typeof cfg.auto_terminate === "number") setAutoTerminate(cfg.auto_terminate);
      if (cfg.notebook_spark_conf) setNotebookSparkConf((cfg.notebook_spark_conf as Array<{ key: string; value: string }>));
      if (cfg.cluster_spark_conf) setClusterSparkConf((cfg.cluster_spark_conf as Array<{ key: string; value: string }>));
      if (cfg.notebook_override) setNotebookOverride(cfg.notebook_override as string);
      if (cfg.trigger_interval) setTriggerInterval(cfg.trigger_interval as string);
      if (typeof cfg.config_step === "number") setConfigStep(cfg.config_step);

      if (p.resources?.cluster_id && !clusterId) {
        const cid = p.resources.cluster_id;
        setClusterId(cid);
        setClusterState("starting");
        api.getClusterStatus(cid).then((resp) => {
          const s = resp.state;
          if (s === "RUNNING") setClusterState("running");
          else if (s === "TERMINATED" || s === "TERMINATING") setClusterState("terminated");
          else if (s === "ERROR") { setClusterState("error"); setClusterError(resp.state_message || "Cluster error"); }
          else { setClusterState("starting"); startClusterPolling(cid); }
        }).catch(() => { setClusterState("idle"); setClusterId(null); });
      }
    }).catch(() => {});

    // Secret keys now managed via UC secrets from global config
  }, [pipelineId]); // eslint-disable-line react-hooks/exhaustive-deps

  // ── Pre-fill from global settings (only if pipeline config hasn't restored values) ──
  const globalApplied = useRef(false);
  useEffect(() => {
    if (!globalSettings || globalApplied.current) return;
    // Wait until pipeline config restore has had a chance to run
    if (pipelineId && !configRestored.current) return;
    globalApplied.current = true;

    const lb = globalSettings.lakebase;
    if (lb) {
      if (lb.mode && !lakebaseMode) setLakebaseMode(lb.mode);
      if (lb.project_id && !lbProjectId) setLbProjectId(lb.project_id);
      if (lb.branch_id && lbBranchId === "production") setLbBranchId(lb.branch_id);
      if (lb.database && lbDatabase === "databricks_postgres") setLbDatabase(lb.database);
      if (lb.pg_table && lbPgTable === "sensor_readings") setLbPgTable(lb.pg_table);
      if (lb.host && !lbHost) setLbHost(lb.host);
      if (lb.port && lbPort === "5432") setLbPort(lb.port);
    }
    const cl = globalSettings.cluster;
    if (cl) {
      if (cl.workers && workers === 2) setWorkers(cl.workers);
      if (cl.worker_node_type && !workerNodeType) setWorkerNodeType(cl.worker_node_type);
      if (cl.dbr_version && !dbrVersion) setDbrVersion(cl.dbr_version);
      if (cl.auto_terminate_min && autoTerminate === 30) setAutoTerminate(cl.auto_terminate_min);
    }
  }, [globalSettings, pipelineId]); // eslint-disable-line react-hooks/exhaustive-deps

  const sssConfig = useMemo((): SssConfig => ({
    pipeline_id: pipelineId ?? undefined,
    processing_mode: processingMode,
    source: selectedSource,
    connection: {
      kafka: { bootstrap_servers: kafkaBootstrap, topic: kafkaTopic, consumer_group: kafkaGroup, security_protocol: kafkaSecurity, service_credential: sourceCredentials.kafka || undefined, advanced: sourceAdvancedConfig.kafka, num_partitions: kafkaPartitions },
      kinesis: { stream_name: kinesisStream, region: kinesisRegion, service_credential: sourceCredentials.kinesis || undefined, advanced: sourceAdvancedConfig.kinesis, num_shards: kinesisShards },
      event_hub: { connection_string: ehConnString, event_hub_name: ehName, service_credential: sourceCredentials.event_hub || undefined, advanced: sourceAdvancedConfig.event_hub, num_partitions: ehPartitions },
      rabbitmq: { host: rmqHost, port: rmqPort, queue: rmqQueue, username: rmqUser, service_credential: sourceCredentials.rabbitmq || undefined, advanced: sourceAdvancedConfig.rabbitmq, num_consumers: rmqConsumers },
      pubsub: { project_id: psProjectId, subscription: psSubscription, topic_id: psTopicId, service_credential: sourceCredentials.pubsub || undefined, advanced: sourceAdvancedConfig.pubsub, max_streams: psMaxStreams },
    },
    sink_table: sinkTable,
    checkpoint_location: checkpointLocation || `/checkpoints/${sinkTable || "bronze"}`,
    rtm_sink: processingMode === "realtime" ? rtmSink : undefined,
    microbatch_sinks: processingMode === "microbatch" ? Array.from(microbatchSinks) : undefined,
    lakebase_mode: isLakebaseActive ? lakebaseMode : undefined,
    lakebase: isLakebaseActive
      ? { host: lbHost, port: lbPort, database: lbDatabase, user: lbUser, password: lbPassword, pg_table: lbPgTable }
      : undefined,
    lakebase_autoscale: (isLakebaseActive && lakebaseMode === "autoscale")
      ? { project_id: lbProjectId, branch_id: lbBranchId, database: lbDatabase }
      : undefined,
    websocket_sink: isWsActive
      ? { app_url: wsAppUrl }
      : undefined,
    lakebase_buffer_size: lakebaseBufferSize,
    ws_buffer_size: wsBufferSize,
    tiering: (isLakebaseActive && lbTiering) ? lbTiering : undefined,
    metrics: {
      enabled: metricsEnabled,
      target_table: metricsTable,
      custom_metrics: customMetrics.map((p) => ({ name: p.key, expression: p.value })),
      zerobus_endpoint: appDefaults?.zerobus_endpoint || undefined,
      zerobus_workspace_url: appDefaults?.workspace_url || undefined,
      zerobus_client_id: appDefaults?.zerobus_client_id || undefined,
      zerobus_client_secret: appDefaults?.zerobus_client_secret || undefined,
    },
    cluster: { workers, worker_node_type: workerNodeType, driver_node_type: workerNodeType, dbr_version: dbrVersion, spot_instances: false, auto_terminate_min: autoTerminate, spark_conf: {} },
    spark_conf: {
      ...Object.fromEntries(notebookSparkConf.filter((p) => p.key).map((p) => [p.key, p.value])),
      "spark.sql.streaming.trigger.processingTime": triggerInterval,
    },
    notebook_override: notebookOverride,
    secret_scope: pipelineResources?.secret_scope || globalSettings?.secrets?.scope,
  }), [processingMode, selectedSource, kafkaBootstrap, kafkaTopic, kafkaGroup, kafkaSecurity, kinesisStream, kinesisRegion, ehConnString, ehName, rmqHost, rmqPort, rmqQueue, rmqUser, psProjectId, psSubscription, sinkTable, checkpointLocation, rtmSink, microbatchSinks, lakebaseMode, lbHost, lbPort, lbDatabase, lbUser, lbPassword, lbPgTable, lbProjectId, lbBranchId, lbTiering, wsAppUrl, lakebaseBufferSize, wsBufferSize, metricsEnabled, metricsTable, customMetrics, appDefaults, workers, workerNodeType, dbrVersion, autoTerminate, notebookSparkConf, notebookOverride, isLakebaseActive, isWsActive, sourceAdvancedConfig, sourceCredentials, pipelineResources?.secret_scope, globalSettings?.secrets?.scope, triggerInterval]);

  const generatedNotebook = useMemo(() => generateNotebook(sssConfig), [sssConfig]);

  const displayedNotebook = notebookOverride ?? generatedNotebook;

  // Auto-clear override when generated notebook changes (unless user is actively editing)
  useEffect(() => {
    if (!notebookEditing) {
      setNotebookOverride(null);
    }
  }, [generatedNotebook]); // eslint-disable-line react-hooks/exhaustive-deps

  const handleResetNotebook = useCallback(() => {
    setNotebookOverride(null);
    setNotebookEditing(false);
  }, []);

  function parseCatalogSchema(tableFqn: string): { catalog: string; schema: string } | null {
    const parts = tableFqn.split(".");
    if (parts.length >= 2) return { catalog: parts[0], schema: parts[1] };
    return null;
  }

  async function handleProvisionUc(
    tableFqn: string,
    setStatus: (s: ProvisionStatus) => void,
    setError: (e: string | null) => void,
  ) {
    const parsed = parseCatalogSchema(tableFqn);
    if (!parsed) {
      setError("Enter a fully qualified table name: catalog.schema.table");
      setStatus("error");
      return;
    }
    setStatus("loading");
    setError(null);
    try {
      const result = await api.provisionUc(parsed.catalog, parsed.schema, pipelineId ?? undefined);
      // Both "created" and "already_exists" are success states
      setStatus("success");
      if (result.status === "already_exists") {
        setError(null); // Clear any previous error — already exists is fine
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      // Treat "already exists" errors as success
      if (msg.toLowerCase().includes("already exists")) {
        setStatus("success");
        setError(null);
      } else {
        setError(msg);
        setStatus("error");
      }
    }
  }

  async function handleProvisionPgTable() {
    if (!lbPgTable.trim()) {
      setPgTableError("PG table name is required");
      setPgTableStatus("error");
      return;
    }
    setPgTableStatus("loading");
    setPgTableError(null);
    try {
      await api.provisionLakebaseTable(
        lakebaseMode === "autoscale"
          ? { mode: "autoscale", table_name: lbPgTable, project_id: lbProjectId, branch_id: lbBranchId, database: lbDatabase, pipeline_id: pipelineId ?? undefined }
          : { mode: "direct", table_name: lbPgTable, host: lbHost, port: Number(lbPort), database: lbDatabase, user: lbUser, password: lbPassword, pipeline_id: pipelineId ?? undefined },
      );
      setPgTableStatus("success");
    } catch (err) {
      setPgTableError(err instanceof Error ? err.message : String(err));
      setPgTableStatus("error");
    }
  }

  function startClusterPolling(cid: string) {
    if (pollingRef.current) clearInterval(pollingRef.current);
    pollingRef.current = setInterval(async () => {
      try {
        const status = await api.getClusterStatus(cid);
        if (status.state === "RUNNING") {
          setClusterState("running");
          if (pollingRef.current) clearInterval(pollingRef.current);
          pollingRef.current = null;
        } else if (status.state === "ERROR" || status.state === "TERMINATED") {
          setClusterState("error");
          setClusterError(status.state_message || "Cluster failed to start");
          if (pollingRef.current) clearInterval(pollingRef.current);
          pollingRef.current = null;
        }
      } catch {
        // Keep polling on transient errors
      }
    }, 5000);
  }

  async function handleStartCluster() {
    if (!clusterId) return;
    setClusterState("starting");
    setClusterError(null);
    try {
      await api.startCluster(clusterId);
      startClusterPolling(clusterId);
    } catch (err) {
      setClusterError(err instanceof Error ? err.message : String(err));
      setClusterState("error");
    }
  }

  async function handleProvisionCluster() {
    if (!workerNodeType || !dbrVersion) {
      setClusterError("Select a node type and runtime version first.");
      return;
    }
    setClusterState("creating");
    setClusterError(null);
    try {
      const resp = await api.provisionCluster({
        num_workers: workers,
        worker_node_type: workerNodeType,
        spark_version: dbrVersion,
        auto_terminate_min: autoTerminate,
        spark_conf: Object.fromEntries(clusterSparkConf.filter((p) => p.key).map((p) => [p.key, p.value])),
        pipeline_id: pipelineId ?? undefined,
      });
      setClusterId(resp.cluster_id);
      setClusterState("starting");
      startClusterPolling(resp.cluster_id);
    } catch (err) {
      setClusterError(err instanceof Error ? err.message : String(err));
      setClusterState("error");
    }
  }

  async function handleTestConnectivity() {
    if (!selectedSource) return;
    setConnectTestState("testing");
    setConnectTestMsg(null);
    setConnectTestPhase(0);
    setConnectTestResult(null);
    if (connectPollRef.current) { clearInterval(connectPollRef.current); connectPollRef.current = null; }

    try {
      // Phase 0: Upload wheel
      setConnectTestPhase(0);
      const { wheel_path } = await api.uploadSimulatorWheel();

      // Phase 1: Generate and submit test notebook
      setConnectTestPhase(1);
      const notebook = generateConnectivityTestNotebook(selectedSource, sssConfig.connection, wheel_path);
      const { run_id } = await api.submitConnectivityTest({ notebook, wheel_path });

      // Phase 2-3: Poll for result
      setConnectTestPhase(2);

      const poll = async () => {
        try {
          const status = await api.getConnectivityTestStatus(run_id);
          if (status.state === "RUNNING") {
            setConnectTestPhase(3);
          } else if (status.state === "COMPLETED") {
            if (connectPollRef.current) { clearInterval(connectPollRef.current); connectPollRef.current = null; }
            const r = status.result;
            setConnectTestResult(r ?? null);
            if (r?.success) {
              setConnectTestState("success");
              setConnectTestMsg(r.message || "Connectivity verified.");
              toast.success(r.message || "Connectivity verified on serverless.");
            } else {
              setConnectTestState("error");
              setConnectTestMsg(r?.error || "Connectivity test failed.");
              toast.error(r?.error || "Connectivity test failed.");
            }
          } else if (status.state === "FAILED") {
            if (connectPollRef.current) { clearInterval(connectPollRef.current); connectPollRef.current = null; }
            setConnectTestState("error");
            setConnectTestMsg(status.error || "Serverless run failed.");
            toast.error(status.error || "Serverless run failed.");
          }
        } catch (pollErr) {
          if (connectPollRef.current) { clearInterval(connectPollRef.current); connectPollRef.current = null; }
          setConnectTestState("error");
          const msg = pollErr instanceof Error ? pollErr.message : String(pollErr);
          setConnectTestMsg(msg);
          toast.error(msg);
        }
      };

      connectPollRef.current = setInterval(poll, 3000);
      // Initial poll immediately
      await poll();
    } catch (err) {
      if (connectPollRef.current) { clearInterval(connectPollRef.current); connectPollRef.current = null; }
      setConnectTestState("error");
      const msg = err instanceof Error ? err.message : String(err);
      setConnectTestMsg(msg);
      toast.error(msg);
    }
  }

  async function handleValidate() {
    if (!clusterId) return;

    // Check if Zerobus credentials are needed and available
    const needsOAuth = rtmSink === "websocket" || rtmSink === "both" || microbatchSinks.has("websocket");
    if (needsOAuth) {
      try {
        const status = await api.getSettingsStatus();
        const keys = status?.secrets?.keys || [];
        const idKey = globalSettings?.secrets?.zb_client_id_key || "zerobus-client-id";
        const secretKey = globalSettings?.secrets?.zb_client_secret_key || "zerobus-client-secret";
        if (!keys.includes(idKey) || !keys.includes(secretKey)) {
          toast.error("Zerobus credentials not found in secret scope. Go to Settings > Secrets and click 'Create Secret Scope' to provision them first.");
          return;
        }
      } catch {
        // If status check fails, proceed anyway — the notebook will show the actual error
      }
    }

    setValidateState("running");
    setValidatePhase(0);
    setValidateOutput(null);
    setValidateError(null);
    setValidateOutputCollapsed(true);

    const phaseTimer1 = setTimeout(() => setValidatePhase(1), 800);
    const phaseTimer2 = setTimeout(() => setValidatePhase(2), 2500);

    try {
      const resp = await api.validateOnCluster(clusterId, displayedNotebook);
      clearTimeout(phaseTimer1);
      clearTimeout(phaseTimer2);
      setValidatePhase(3);
      if (resp.success) {
        setValidateState("success");
        setValidateOutput(resp.output || "Validation passed.");
      } else {
        setValidateState("error");
        setValidateError(resp.error || "Validation failed.");
        setValidateOutput(resp.output || null);
      }
    } catch (err) {
      clearTimeout(phaseTimer1);
      clearTimeout(phaseTimer2);
      setValidatePhase(3);
      const errMsg = err instanceof Error ? err.message : String(err);
      const is504 = errMsg.includes("504") || errMsg.includes("Gateway Timeout") || errMsg.includes("gateway timeout");
      if (is504) {
        setValidateState("timeout");
        setValidateError("Validation timed out — this is expected for streaming notebooks. You can proceed to deploy.");
      } else {
        setValidateState("error");
        setValidateError(errMsg);
      }
    }
  }

  async function handleRunOnCluster() {
    if (!clusterId) return;
    setRunState("running");
    setRunPhase(0);
    setRunOutput(null);
    setRunError(null);
    setRunOutputCollapsed(true);

    const phaseTimer1 = setTimeout(() => setRunPhase(1), 800);
    const phaseTimer2 = setTimeout(() => setRunPhase(2), 2500);

    try {
      const resp = await api.runOnCluster(clusterId, displayedNotebook);
      clearTimeout(phaseTimer1);
      clearTimeout(phaseTimer2);
      setRunPhase(3);
      if (resp.success) {
        setRunState("success");
        setRunOutput(resp.output || "Run completed successfully.");
      } else {
        setRunState("error");
        setRunError(resp.error || "Run failed.");
        setRunOutput(resp.output || null);
      }
    } catch (err) {
      clearTimeout(phaseTimer1);
      clearTimeout(phaseTimer2);
      setRunPhase(3);
      setRunState("error");
      setRunError(err instanceof Error ? err.message : String(err));
    }
  }

  function startJobPolling(jobId: number) {
    if (jobPollRef.current) clearInterval(jobPollRef.current);
    setJobRunState("polling");
    setJobRunMessage("Provisioning cluster — this may take up to 5 minutes...");
    setJobTaskStates(null);

    let allTasksRunning = false;
    const poll = async () => {
      try {
        const s = await api.getJobStatus(jobId);

        // Update per-task states
        if (s.tasks) setJobTaskStates(s.tasks);

        // Check for any task failure — if any task fails, cancel the whole job
        if (s.tasks) {
          const failedTask = Object.entries(s.tasks).find(
            ([, t]) => t.state === "TERMINATED" || t.state === "INTERNAL_ERROR" || t.state === "FAILED",
          );
          if (failedTask) {
            const [taskKey, taskState] = failedTask;
            // Cancel the entire job
            try { await api.stopJob(jobId); } catch { /* best effort */ }
            setJobRunState("error");
            setJobRunMessage(`Task "${taskKey}" failed: ${taskState.state_message || "unknown error"}. Job cancelled.`);
            if (jobPollRef.current) { clearInterval(jobPollRef.current); jobPollRef.current = null; }
            return;
          }
        }

        if (s.state === "RUNNING") {
          if (s.tasks) {
            const taskEntries = Object.entries(s.tasks);
            const allRunning = taskEntries.length > 0 && taskEntries.every(([, t]) => t.state === "RUNNING");
            const runningTasks = taskEntries.filter(([, t]) => t.state === "RUNNING").map(([k]) => k);
            const pendingTasks = taskEntries.filter(([, t]) => t.state !== "RUNNING").map(([k]) => k);

            if (allRunning) {
              if (!allTasksRunning) {
                allTasksRunning = true;
                // Slow down polling once all tasks confirmed running
                if (jobPollRef.current) clearInterval(jobPollRef.current);
                jobPollRef.current = setInterval(poll, 60000);
              }
              setJobRunState("running");
              setJobRunMessage("All tasks running — pipeline streaming data");
            } else {
              setJobRunState("polling");
              setJobRunMessage(
                `Waiting for tasks: ${pendingTasks.join(", ")} (${runningTasks.length}/${taskEntries.length} running)`
              );
            }
          } else {
            setJobRunState("running");
            setJobRunMessage("Job is running");
          }
        } else if (s.state === "TERMINATED" || s.state === "INTERNAL_ERROR" || s.state === "SKIPPED") {
          const paused = s.result_state === "CANCELED" || s.state === "TERMINATED";
          setJobRunState(paused ? "stopped" : "error");
          setJobRunMessage(s.state_message || s.state);
          if (jobPollRef.current) { clearInterval(jobPollRef.current); jobPollRef.current = null; }
        } else {
          setJobRunState("polling");
          setJobRunMessage(
            s.state === "PENDING" || s.state === "QUEUED"
              ? "Provisioning cluster — this may take up to 5 minutes..."
              : `Job state: ${s.state}`
          );
        }
      } catch {
        // keep polling on transient errors
      }
    };
    poll();
    // Poll every 60s — job cluster takes ~5 min to provision
    jobPollRef.current = setInterval(poll, 60000);
  }

  useEffect(() => {
    return () => { if (jobPollRef.current) clearInterval(jobPollRef.current); };
  }, []);

  useEffect(() => {
    const existingJobId = pipelineResources?.job_id;
    if (existingJobId && deployState === "idle" && jobRunState === "idle") {
      setDeployedJobId(Number(existingJobId));
      setDeployState("success");
      startJobPolling(Number(existingJobId));
    }
  }, [pipelineResources?.job_id]);

  async function handleDeployJob() {
    // --- Zerobus pipeline: deploy only the producer notebook (serverless) ---
    if (selectedPipeline === "zerobus") {
      if (!pipelineId) {
        toast.error("Save the pipeline first before deploying.");
        return;
      }
      setDeployState("deploying");
      setDeployError(null);
      setDeployJobUrl(null);
      try {
        const { wheel_path } = await api.uploadSimulatorWheel();
        const pipeline = await api.getPipeline(pipelineId);
        const secretScope = globalSettings?.secrets?.scope || "iot-secrets";

        const producerNotebook = generateDataProducerNotebook({
          wheelPath: wheel_path,
          industry: pipeline.industry || "pharmaceutical",
          updateRateHz: 2,
          sensorConfig: pipeline.sensor_config || [],
          source: "zerobus",
          connection: {
            zerobus: {
              endpoint: zbEndpoint,
              table_name: zbTable,
              client_id: zbClientId,
              client_secret: zbClientSecret,
              workspace_url: appDefaults?.workspace_url || "",
              secret_scope: secretScope,
            },
          },
        });

        await api.startProducer(pipelineId, producerNotebook, "zerobus", zbTable);
        setDeployState("success");
        // Save the pipeline config so the wizard knows it's deployed
        await savePipelineConfig();
      } catch (err) {
        setDeployState("error");
        setDeployError(err instanceof Error ? err.message : String(err));
      }
      return;
    }

    // --- SSS pipeline: deploy consumer + optional producer as continuous job ---
    // Check if Zerobus credentials are needed and available
    const needsOAuth = rtmSink === "websocket" || rtmSink === "both" || microbatchSinks.has("websocket");
    if (needsOAuth) {
      try {
        const status = await api.getSettingsStatus();
        const keys = status?.secrets?.keys || [];
        const idKey = globalSettings?.secrets?.zb_client_id_key || "zerobus-client-id";
        const secretKey = globalSettings?.secrets?.zb_client_secret_key || "zerobus-client-secret";
        if (!keys.includes(idKey) || !keys.includes(secretKey)) {
          toast.error("Zerobus credentials not found in secret scope. Go to Settings > Secrets and click 'Create Secret Scope' to provision them first.");
          return;
        }
      } catch {
        // If status check fails, proceed anyway
      }
    }

    setDeployState("deploying");
    setDeployError(null);
    setDeployJobUrl(null);
    try {
      // Generate producer notebook if a source is selected
      let producerNotebook: string | undefined;
      if (selectedSource && pipelineId) {
        try {
          const { wheel_path } = await api.uploadSimulatorWheel();
          producerNotebook = generateDataProducerNotebook({
            wheelPath: wheel_path,
            industry: (await api.getPipeline(pipelineId)).industry || "pharmaceutical",
            updateRateHz: parseFloat(
              sssConfig.spark_conf["spark.sql.streaming.trigger.processingTime"] || "2",
            ) || 2,
            sensorConfig: [],
            source: selectedSource,
            connection: sssConfig.connection,
          });
        } catch (prodErr) {
          // Non-blocking: deploy without producer if generation fails
          console.warn("Producer notebook generation failed:", prodErr);
        }
      }

      const resp = await api.deployJob({
        notebook: displayedNotebook,
        producer_notebook: producerNotebook,
        source_type: selectedSource ?? undefined,
        cluster_config: {
          spark_version: dbrVersion,
          worker_node_type: workerNodeType,
          num_workers: workers,
          spark_conf: Object.fromEntries(clusterSparkConf.filter((p) => p.key).map((p) => [p.key, p.value])),
        },
        pipeline_id: pipelineId ?? undefined,
        job_id: pipelineResources?.job_id ? Number(pipelineResources.job_id) : undefined,
      });
      setDeployState("success");
      setDeployJobUrl(resp.job_url || null);
      setDeployedJobId(resp.job_id);
      startJobPolling(resp.job_id);
    } catch (err) {
      setDeployState("error");
      setDeployError(err instanceof Error ? err.message : String(err));
    }
  }

  async function handleStopJob() {
    if (!deployedJobId) return;
    setJobActionLoading(true);
    try {
      await api.stopJob(deployedJobId);
      setJobRunState("stopped");
      setJobRunMessage("Job paused");
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Failed to stop job");
    } finally {
      setJobActionLoading(false);
    }
  }

  async function handleRestartJob() {
    if (!deployedJobId) return;
    setJobActionLoading(true);
    try {
      await api.restartJob(deployedJobId);
      startJobPolling(deployedJobId);
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Failed to restart job");
    } finally {
      setJobActionLoading(false);
    }
  }

  const nodeTypeOptions = useMemo(() =>
    nodeTypes.map((nt) => ({
      value: nt.node_type_id,
      label: `${nt.node_type_id} (${nt.num_cores} cores, ${(nt.memory_mb / 1024).toFixed(1)} GB, ${nt.storage_label})`,
    })),
    [nodeTypes],
  );

  const sparkVersionOptions = useMemo(() =>
    sparkVersions.map((sv) => ({
      value: sv.key,
      label: sv.name,
    })),
    [sparkVersions],
  );

  return (
    <div className="space-y-6">
      {/* Architecture Canvas */}
      <ArchitectureCanvas
        pipeline={selectedPipeline}
        sssSource={selectedSource}
        processingMode={processingMode}
        sinkTable={sinkTable}
        metricsEnabled={metricsEnabled}
        metricsTable={metricsTable}
        rtmSink={processingMode === "realtime" ? rtmSink : undefined}
      />

      {/* Pipeline type selector */}
      <CollapsibleSection
        title="Pipeline Type"
        icon={Zap}
        complete={!!selectedPipeline}
        defaultOpen
      >
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
          {pipelines.map((p) => (
            <Card
              key={p.id}
              onClick={() => setSelectedPipeline(p.id)}
              className={cn(
                "cursor-pointer transition-all hover:border-primary/50 hover:shadow-md",
                selectedPipeline === p.id && "border-primary bg-primary/5 shadow-md"
              )}
            >
              <CardContent className="flex items-center gap-4 p-4">
                <div className={cn("flex h-11 w-11 shrink-0 items-center justify-center rounded-lg", selectedPipeline === p.id ? "bg-primary/15 text-primary" : "bg-muted text-muted-foreground")}>
                  <p.icon className="h-5 w-5" />
                </div>
                <div className="min-w-0">
                  <p className="text-sm font-semibold">{p.label}</p>
                  <p className="text-xs text-muted-foreground">{p.description}</p>
                </div>
                {selectedPipeline === p.id && <CheckCircle2 className="ml-auto h-4 w-4 shrink-0 text-primary" />}
              </CardContent>
            </Card>
          ))}
        </div>
      </CollapsibleSection>

      {/* ================================================================ */}
      {/* SSS Configuration — Sub-step wizard */}
      {/* ================================================================ */}
      {selectedPipeline === "sss" && (
        <>
          {/* Mini stepper */}
          <nav className="flex items-center gap-1 rounded-lg border border-border bg-card p-1">
            {CONFIG_SUB_STEPS.map((label, i) => {
              const reachable = i <= maxReachableStep + 1;
              return (
                <button
                  key={label}
                  type="button"
                  onClick={() => reachable && setConfigStep(i)}
                  disabled={!reachable}
                  className={cn(
                    "flex-1 rounded-md px-3 py-2 text-xs font-medium transition-colors",
                    configStep === i
                      ? "bg-primary text-primary-foreground shadow-sm"
                      : reachable
                        ? "text-muted-foreground hover:bg-muted hover:text-foreground"
                        : "text-muted-foreground/40 cursor-not-allowed",
                  )}
                >
                  <span className="mr-1.5 inline-flex h-4 w-4 items-center justify-center rounded-full text-[10px] font-bold border border-current/30">
                    {i + 1}
                  </span>
                  {label}
                </button>
              );
            })}
          </nav>

          {/* ---- Sub-step 1: Sink (Cluster & Infrastructure + Sinks) ---- */}
          {configStep === 1 && (
          <>
          {/* ---- Cluster & Infrastructure ---- */}
          <CollapsibleSection
            title="Cluster & Infrastructure"
            icon={Server}
            complete={clusterState === "running"}
            defaultOpen
          >
            <div className="space-y-6">
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Server className="h-4 w-4" />
                    Cluster Configuration
                    {cloudProvider !== "unknown" && (
                      <Badge variant="secondary" className="text-[10px] uppercase">
                        {cloudProvider}
                      </Badge>
                    )}
                  </CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                  {/* Source parallelism input */}
                  {selectedSource && (
                    <div className="mb-4 flex items-center gap-4 rounded-lg border border-border bg-muted/30 p-3">
                      <Cpu className="h-4 w-4 shrink-0 text-muted-foreground" />
                      <div className="flex items-center gap-3">
                        <label className="text-xs font-medium text-muted-foreground whitespace-nowrap">
                          {selectedSource === "kafka" && "Kafka Partitions"}
                          {selectedSource === "kinesis" && "Kinesis Shards"}
                          {selectedSource === "event_hub" && "Event Hub Partitions"}
                          {selectedSource === "rabbitmq" && "Parallel Consumers"}
                          {selectedSource === "pubsub" && "Pull Subscriber Streams"}
                        </label>
                        <input
                          type="number"
                          min={1}
                          max={256}
                          value={
                            selectedSource === "kafka" ? kafkaPartitions :
                            selectedSource === "kinesis" ? kinesisShards :
                            selectedSource === "event_hub" ? ehPartitions :
                            selectedSource === "rabbitmq" ? rmqConsumers :
                            psMaxStreams
                          }
                          onChange={(e) => {
                            const v = Math.max(1, parseInt(e.target.value) || 1);
                            setWorkersManualOverride(false);
                            if (selectedSource === "kafka") setKafkaPartitions(v);
                            else if (selectedSource === "kinesis") setKinesisShards(v);
                            else if (selectedSource === "event_hub") setEhPartitions(v);
                            else if (selectedSource === "rabbitmq") setRmqConsumers(v);
                            else if (selectedSource === "pubsub") setPsMaxStreams(v);
                          }}
                          className="w-20 rounded-md border border-input bg-background px-2 py-1 text-center text-sm focus:outline-none focus:ring-1 focus:ring-ring"
                        />
                      </div>
                      <span className="text-xs text-muted-foreground">
                        {selectedSource === "kafka" && "(1 Spark core per partition)"}
                        {selectedSource === "kinesis" && "(1 Spark core per shard)"}
                        {selectedSource === "event_hub" && "(1 Spark core per partition)"}
                        {selectedSource === "rabbitmq" && "(typically 1)"}
                        {selectedSource === "pubsub" && "(typically 1–4)"}
                      </span>
                    </div>
                  )}
                  <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
                    <div>
                      <label className="mb-1.5 block text-xs font-medium uppercase tracking-wider text-muted-foreground">
                        Workers
                      </label>
                      <div className="flex items-center gap-2">
                        <Button variant="outline" size="icon" className="h-8 w-8" onClick={() => { setWorkers(Math.max(1, workers - 1)); setWorkersManualOverride(true); }}>
                          <Minus className="h-3 w-3" />
                        </Button>
                        <span className="w-10 text-center text-sm font-semibold tabular-nums">{workers}</span>
                        <Button variant="outline" size="icon" className="h-8 w-8" onClick={() => { setWorkers(Math.min(100, workers + 1)); setWorkersManualOverride(true); }}>
                          <Plus className="h-3 w-3" />
                        </Button>
                      </div>
                      {recommendedWorkers && (
                        <p className="mt-1.5 text-[10px] text-muted-foreground">
                          Recommended: {recommendedWorkers.workers} ({recommendedWorkers.parallelism} parallelism / {recommendedWorkers.coresPerNode} cores)
                        </p>
                      )}
                      {recommendedWorkers && workers < recommendedWorkers.workers && (
                        <p className="mt-1 flex items-center gap-1 text-[10px] text-amber-600">
                          <AlertTriangle className="h-3 w-3" />
                          {workers * (recommendedWorkers.coresPerNode)} cores may be insufficient for {recommendedWorkers.parallelism} parallelism
                        </p>
                      )}
                    </div>
                    <SelectField
                      label="Worker Node Type"
                      value={workerNodeType}
                      onChange={setWorkerNodeType}
                      richOptions={nodeTypeOptions}
                      loading={nodeTypesLoading}
                      disabled={clusterState !== "idle"}
                    />
                    <SelectField
                      label="Databricks Runtime"
                      value={dbrVersion}
                      onChange={setDbrVersion}
                      richOptions={sparkVersionOptions}
                      loading={sparkVersionsLoading}
                      disabled={clusterState !== "idle"}
                    />
                  </div>

                  <div className="flex items-center gap-6">
                    <div className="flex items-center gap-2">
                      <Badge variant="secondary" className="text-[10px]">Non-Photon</Badge>
                      <Badge variant="secondary" className="text-[10px]">No Spot Instances</Badge>
                    </div>
                    <label className="flex items-center gap-2 text-sm">
                      <HardDrive className="h-3.5 w-3.5 text-muted-foreground" />
                      <span>Auto-terminate after</span>
                      <input
                        type="number"
                        value={autoTerminate}
                        min={10}
                        max={120}
                        onChange={(e) => setAutoTerminate(Number(e.target.value))}
                        className="w-16 rounded-md border border-input bg-background px-2 py-1 text-center text-sm focus:outline-none focus:ring-1 focus:ring-ring"
                      />
                      <span className="text-muted-foreground">min</span>
                    </label>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle>Cluster Spark Configuration</CardTitle>
                  <p className="text-xs text-muted-foreground">
                    Applied at cluster creation time. Changes require re-provisioning the cluster.
                  </p>
                </CardHeader>
                <CardContent>
                  <KeyValueEditor
                    pairs={clusterSparkConf}
                    onChange={setClusterSparkConf}
                    keyPlaceholder="spark.conf.key"
                    valuePlaceholder="value"
                    keyLabel="Property"
                    valueLabel="Value"
                    addLabel="Add Property"
                  />
                </CardContent>
              </Card>

              {clusterState === "idle" ? (
                <div className="flex items-center justify-end gap-3">
                  {clusterError && (
                    <p className="flex items-center gap-1 text-sm text-destructive">
                      <AlertCircle className="h-3.5 w-3.5" /> {clusterError}
                    </p>
                  )}
                  <Button
                    size="lg"
                    className="gap-2"
                    onClick={handleProvisionCluster}
                    disabled={!workerNodeType || !dbrVersion || nodeTypesLoading || sparkVersionsLoading}
                  >
                    <Play className="h-4 w-4" />
                    Provision & Start
                  </Button>
                </div>
              ) : clusterState === "terminated" ? (
                <Card className="border-amber-500/50 bg-amber-500/5">
                  <CardContent className="flex items-center justify-between p-4">
                    <div className="flex items-center gap-3">
                      <Badge variant="secondary" className="bg-amber-500/15 text-amber-600">Terminated</Badge>
                      {clusterId && (
                        <span className="font-mono text-xs text-muted-foreground">{clusterId}</span>
                      )}
                    </div>
                    <div className="flex items-center gap-2">
                      <Button variant="outline" className="gap-2" onClick={handleStartCluster}>
                        <RefreshCw className="h-3.5 w-3.5" />
                        Restart Cluster
                      </Button>
                      <Button variant="ghost" className="gap-2 text-muted-foreground" onClick={() => { setClusterState("idle"); setClusterId(null); setClusterError(null); }}>
                        <Plus className="h-3.5 w-3.5" />
                        Provision New
                      </Button>
                    </div>
                  </CardContent>
                </Card>
              ) : clusterState === "error" ? (
                <Card className="border-destructive/50">
                  <CardHeader>
                    <CardTitle className="text-destructive">Cluster Error</CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-3">
                    <p className="text-sm text-destructive">{clusterError || "Unknown error"}</p>
                    {clusterId && (
                      <p className="font-mono text-xs text-muted-foreground">{clusterId}</p>
                    )}
                    <div className="flex items-center gap-2">
                      <Button variant="outline" onClick={handleStartCluster}>
                        <RefreshCw className="mr-2 h-3.5 w-3.5" />
                        Restart Cluster
                      </Button>
                      <Button variant="ghost" onClick={() => { setClusterState("idle"); setClusterError(null); setClusterId(null); }}>
                        Provision New
                      </Button>
                    </div>
                  </CardContent>
                </Card>
              ) : (
                <Card>
                  <CardHeader>
                    <CardTitle>Provisioning Progress</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-4">
                      {provisioningSteps.map((step, i) => (
                        <StepIndicator key={step.key} step={step} currentState={clusterState} index={i} />
                      ))}
                      {(clusterState === "creating" || clusterState === "starting") && (
                        <p className="mt-2 flex items-center gap-1 text-sm text-muted-foreground">
                          <Timer className="h-4 w-4" />
                          This may take up to 5 minutes while the cluster provisions and starts.
                        </p>
                      )}
                      {clusterState === "running" && (
                        <div className="mt-2 flex items-center justify-between">
                          <p className="flex items-center gap-1 text-sm text-green-600">
                            <CheckCircle2 className="h-4 w-4" />
                            Cluster is ready. You can now validate and deploy your pipeline.
                          </p>
                          <div className="flex items-center gap-2">
                            {clusterId && (
                              <span className="font-mono text-xs text-muted-foreground">{clusterId}</span>
                            )}
                            <Badge className="bg-green-500/15 text-green-600 border-green-500/30">Running</Badge>
                          </div>
                        </div>
                      )}
                    </div>
                  </CardContent>
                </Card>
              )}
            </div>
          </CollapsibleSection>
          </>
          )}

          {/* ---- Sub-step 0: Source (Processing Mode + Data Source & Connection) ---- */}
          {configStep === 0 && (
          <>
          {/* ---- Processing Mode (shown first) ---- */}
          <CollapsibleSection
            title="Processing Mode"
            icon={Activity}
            complete={!!processingMode}
            defaultOpen
          >
            <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
              {([
                { id: "realtime" as const, label: "Real-Time Mode", description: "Continuous processing with sub-second latency", icon: Activity },
                { id: "microbatch" as const, label: "Microbatch", description: "Trigger-based processing at configurable intervals", icon: Timer },
              ]).map((m) => (
                <Card
                  key={m.id}
                  onClick={() => setProcessingMode(m.id)}
                  className={cn(
                    "cursor-pointer transition-all hover:border-primary/50",
                    processingMode === m.id && "border-primary bg-primary/5 shadow-md"
                  )}
                >
                  <CardContent className="flex items-center gap-4 p-4">
                    <div className={cn("flex h-10 w-10 shrink-0 items-center justify-center rounded-lg", processingMode === m.id ? "bg-primary/15 text-primary" : "bg-muted text-muted-foreground")}>
                      <m.icon className="h-5 w-5" />
                    </div>
                    <div>
                      <p className="text-sm font-semibold">{m.label}</p>
                      <p className="text-xs text-muted-foreground">{m.description}</p>
                    </div>
                    {processingMode === m.id && <CheckCircle2 className="ml-auto h-4 w-4 shrink-0 text-primary" />}
                  </CardContent>
                </Card>
              ))}
            </div>

            {/* Trigger Interval */}
            <div className="mt-4 space-y-2">
              <label className="text-sm font-medium text-foreground">Trigger Interval</label>
              {processingMode === "realtime" ? (
                <div className="flex items-center gap-2">
                  <span className="inline-flex h-8 items-center rounded-md border bg-muted px-3 text-xs font-medium text-muted-foreground">
                    5 minutes (fixed)
                  </span>
                </div>
              ) : (
                <div className="flex flex-wrap items-center gap-2">
                  {["5 seconds", "10 seconds", "30 seconds", "1 minute"].map((preset) => (
                    <Button
                      key={preset}
                      type="button"
                      variant={triggerInterval === preset ? "default" : "outline"}
                      size="sm"
                      className="text-xs"
                      onClick={() => setTriggerInterval(preset)}
                    >
                      {preset}
                    </Button>
                  ))}
                  <input
                    type="text"
                    value={triggerInterval}
                    onChange={(e) => setTriggerInterval(e.target.value)}
                    className="h-8 w-28 rounded-md border border-input bg-background px-2 text-xs focus:outline-none focus:ring-1 focus:ring-ring"
                    placeholder="e.g. 30 seconds"
                  />
                </div>
              )}
              <p className="text-[11px] text-muted-foreground">
                {processingMode === "realtime"
                  ? "Real-Time Mode uses a fixed 5-minute trigger interval."
                  : "How often the microbatch trigger fires to process new data."}
              </p>
            </div>
          </CollapsibleSection>

          <CollapsibleSection
            title="Data Source & Connection"
            icon={Globe}
            complete={!!selectedSource}
            defaultOpen
          >
            <div className="space-y-6">
              <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
                {sssSources.map((src) => (
                  <Card
                    key={src.id}
                    onClick={() => setSelectedSource(src.id)}
                    className={cn(
                      "cursor-pointer transition-all hover:border-primary/50",
                      selectedSource === src.id && "border-primary bg-primary/5 shadow-md"
                    )}
                  >
                    <CardContent className="flex items-center gap-4 p-4">
                      <div className={cn("flex h-10 w-10 shrink-0 items-center justify-center rounded-lg", selectedSource === src.id ? "bg-primary/15 text-primary" : "bg-muted text-muted-foreground")}>
                        <src.icon className="h-5 w-5" />
                      </div>
                      <div className="min-w-0 flex-1">
                        <p className="text-sm font-semibold">{src.label}</p>
                        <p className="text-xs text-muted-foreground">{src.description}</p>
                      </div>
                      {selectedSource === src.id && (
                        <div className="ml-auto flex items-center gap-1.5 shrink-0">
                          <button
                            onClick={(e) => {
                              e.stopPropagation();
                              const defaults: Record<string, string> = {};
                              SOURCE_ADVANCED_OPTIONS[src.id].forEach((opt) => {
                                defaults[opt.key] = sourceAdvancedConfig[src.id]?.[opt.key] ?? opt.defaultValue;
                              });
                              setAdvancedDialogDraft(defaults);
                              setAdvancedDialogOpen(true);
                            }}
                            className="rounded p-1 hover:bg-muted transition-colors"
                            title="Advanced settings"
                          >
                            <Settings2 className="h-3.5 w-3.5 text-muted-foreground" />
                          </button>
                          <CheckCircle2 className="h-4 w-4 text-primary" />
                        </div>
                      )}
                    </CardContent>
                  </Card>
                ))}
              </div>

              {/* Advanced source settings dialog */}
              {selectedSource && (
                <Dialog open={advancedDialogOpen} onOpenChange={setAdvancedDialogOpen}>
                  <DialogContent>
                    <DialogHeader>
                      <DialogTitle>
                        {sssSources.find((s) => s.id === selectedSource)?.label} — Advanced Options
                      </DialogTitle>
                      <DialogDescription>
                        Spark Structured Streaming readStream options. Empty values use Spark defaults.
                      </DialogDescription>
                    </DialogHeader>
                    <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                      {SOURCE_ADVANCED_OPTIONS[selectedSource].map((opt) => (
                        <div key={opt.key}>
                          <label className="mb-1 block text-xs font-medium text-muted-foreground">
                            {opt.label}
                          </label>
                          {opt.type === "select" && opt.options ? (
                            <select
                              value={advancedDialogDraft[opt.key] ?? opt.defaultValue}
                              onChange={(e) => setAdvancedDialogDraft((d) => ({ ...d, [opt.key]: e.target.value }))}
                              className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm focus:outline-none focus:ring-1 focus:ring-ring"
                            >
                              {opt.options.map((o) => (
                                <option key={o} value={o}>{o}</option>
                              ))}
                            </select>
                          ) : (
                            <input
                              type="text"
                              value={advancedDialogDraft[opt.key] ?? ""}
                              onChange={(e) => setAdvancedDialogDraft((d) => ({ ...d, [opt.key]: e.target.value }))}
                              placeholder={opt.defaultValue || "Spark default"}
                              className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring"
                            />
                          )}
                        </div>
                      ))}
                    </div>
                    <DialogFooter>
                      <Button
                        variant="outline"
                        onClick={() => setAdvancedDialogOpen(false)}
                      >
                        Cancel
                      </Button>
                      <Button
                        onClick={() => {
                          const nonEmpty = Object.fromEntries(
                            Object.entries(advancedDialogDraft).filter(([, v]) => v !== ""),
                          );
                          setSourceAdvancedConfig((prev) => ({
                            ...prev,
                            [selectedSource]: nonEmpty,
                          }));
                          setAdvancedDialogOpen(false);
                        }}
                      >
                        Save
                      </Button>
                    </DialogFooter>
                  </DialogContent>
                </Dialog>
              )}

              {selectedSource && (
                <Card>
                  <CardHeader>
                    <CardTitle>Connection Configuration</CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-4">
                    <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
                      {selectedSource === "kafka" && (
                        <>
                          <InputField label="Bootstrap Servers" placeholder="broker1:9092,broker2:9092" value={kafkaBootstrap} onChange={setKafkaBootstrap} />
                          <InputField label="Topic" placeholder="iot-sensor-data" value={kafkaTopic} onChange={setKafkaTopic} />
                          <InputField label="Consumer Group" placeholder="iot-twin-consumer" value={kafkaGroup} onChange={setKafkaGroup} />
                          <SelectField label="Security Protocol" value={kafkaSecurity} onChange={setKafkaSecurity} options={["SASL_SSL", "PLAINTEXT", "SSL", "SASL_PLAINTEXT"]} />
                        </>
                      )}
                      {selectedSource === "kinesis" && (
                        <>
                          <InputField label="Stream Name" placeholder="iot-sensor-stream" value={kinesisStream} onChange={setKinesisStream} />
                          <InputField label="AWS Region" placeholder="us-east-1" value={kinesisRegion} onChange={setKinesisRegion} />
                        </>
                      )}
                      {selectedSource === "event_hub" && (
                        <>
                          <InputField label="Connection String" placeholder="Endpoint=sb://..." value={ehConnString} onChange={setEhConnString} colSpan={2} />
                          <InputField label="Event Hub Name" placeholder="iot-events" value={ehName} onChange={setEhName} />
                        </>
                      )}
                      {selectedSource === "rabbitmq" && (
                        <>
                          <InputField label="Host" placeholder="rabbitmq.example.com" value={rmqHost} onChange={setRmqHost} />
                          <InputField label="Port" placeholder="5672" value={rmqPort} onChange={setRmqPort} />
                          <InputField label="Queue" placeholder="iot-queue" value={rmqQueue} onChange={setRmqQueue} />
                          <InputField label="Username" placeholder="guest" value={rmqUser} onChange={setRmqUser} />
                        </>
                      )}
                      {selectedSource === "pubsub" && (
                        <>
                          <InputField label="Project ID" placeholder="my-gcp-project" value={psProjectId} onChange={setPsProjectId} />
                          <InputField label="Topic ID (for producer)" placeholder="iot-sensor-data" value={psTopicId} onChange={setPsTopicId} />
                          <InputField label="Subscription (for consumer)" placeholder="iot-subscription" value={psSubscription} onChange={setPsSubscription} />
                        </>
                      )}
                    </div>

                    <div className="rounded-md border border-dashed border-border bg-muted/20 p-3 space-y-2">
                      <InputField
                        label="Service Credential (UC)"
                        placeholder="my-service-credential"
                        value={sourceCredentials[selectedSource] || ""}
                        onChange={(v) => setSourceCred(selectedSource, v)}
                        colSpan={2}
                      />
                      <p className="text-[10px] text-muted-foreground leading-relaxed">
                        Unity Catalog service credential name for authentication. Leave blank to use inline credentials.
                      </p>
                    </div>

                    <Separator />

                    <div className="space-y-3">
                      <div className="flex items-center gap-3">
                        <Button
                          variant="outline"
                          size="sm"
                          className="gap-2"
                          disabled={!sourceFieldsFilled || connectTestState === "testing"}
                          onClick={handleTestConnectivity}
                        >
                          {connectTestState === "testing" ? (
                            <Loader2 className="h-3.5 w-3.5 animate-spin" />
                          ) : (
                            <Zap className="h-3.5 w-3.5" />
                          )}
                          Test Connectivity
                        </Button>

                        {connectTestState === "success" && (
                          <Badge variant="outline" className="text-green-600 border-green-600/30 bg-green-500/5">
                            <CheckCircle2 className="mr-1 h-3 w-3" />
                            Passed
                          </Badge>
                        )}
                        {connectTestState === "error" && !connectTestResult && (
                          <Badge variant="outline" className="text-destructive border-destructive/30 bg-destructive/5 max-w-[400px] truncate">
                            <AlertCircle className="mr-1 h-3 w-3 shrink-0" />
                            {connectTestMsg}
                          </Badge>
                        )}
                        <p className="text-[10px] text-muted-foreground">
                          Runs on serverless compute with UC credentials
                        </p>
                      </div>

                      {connectTestState === "testing" && (
                        <div className="rounded-md border border-border bg-muted/20 p-3 space-y-2">
                          <div className="flex items-center gap-2 text-xs font-medium">
                            <Loader2 className="h-3.5 w-3.5 animate-spin text-primary" />
                            {connectTestPhase === 0 && "Uploading simulator wheel..."}
                            {connectTestPhase === 1 && "Generating & submitting test notebook..."}
                            {connectTestPhase === 2 && "Starting serverless compute..."}
                            {connectTestPhase === 3 && "Running connectivity test..."}
                          </div>
                          <div className="flex gap-1">
                            {[0, 1, 2, 3].map((i) => (
                              <div key={i} className={cn("h-1 flex-1 rounded-full transition-colors", i <= connectTestPhase ? "bg-primary" : "bg-muted")} />
                            ))}
                          </div>
                        </div>
                      )}

                      {connectTestResult && connectTestState !== "testing" && (
                        <div className={cn(
                          "rounded-md border p-3 space-y-2",
                          connectTestResult.success
                            ? "border-green-600/30 bg-green-500/5"
                            : "border-destructive/30 bg-destructive/5",
                        )}>
                          <div className="flex items-center gap-2 text-xs font-medium">
                            {connectTestResult.success ? (
                              <CheckCircle2 className="h-3.5 w-3.5 text-green-600" />
                            ) : (
                              <AlertCircle className="h-3.5 w-3.5 text-destructive" />
                            )}
                            {connectTestResult.success ? "Connectivity Verified" : "Connectivity Failed"}
                          </div>

                          {connectTestResult.diagnostics?.checks && (
                            <div className="flex flex-wrap gap-2">
                              {connectTestResult.diagnostics.checks.map((c) => (
                                <div key={c.name} className="flex items-center gap-1 text-[10px]">
                                  {c.ok ? (
                                    <CheckCircle2 className="h-3 w-3 text-green-600" />
                                  ) : (
                                    <XCircle className="h-3 w-3 text-destructive" />
                                  )}
                                  <span className="font-medium">{c.name}</span>
                                  <span className="text-muted-foreground">{c.detail}</span>
                                </div>
                              ))}
                            </div>
                          )}

                          {connectTestResult.write_test && (
                            <p className="text-[11px] text-muted-foreground"><span className="font-medium">Write:</span> {connectTestResult.write_test}</p>
                          )}
                          {connectTestResult.read_test && (
                            <p className="text-[11px] text-muted-foreground"><span className="font-medium">Read:</span> {connectTestResult.read_test}</p>
                          )}
                          {connectTestResult.error && (
                            <p className="text-[11px] text-destructive"><span className="font-medium">Error:</span> {connectTestResult.error}</p>
                          )}
                          {connectTestResult.hint && (
                            <div className="flex items-start gap-1.5 rounded border border-amber-500/30 bg-amber-500/5 px-2.5 py-1.5">
                              <Lightbulb className="mt-0.5 h-3 w-3 shrink-0 text-amber-600" />
                              <p className="text-[11px] text-amber-700 dark:text-amber-400">{connectTestResult.hint}</p>
                            </div>
                          )}
                        </div>
                      )}
                    </div>
                  </CardContent>
                </Card>
              )}
            </div>
          </CollapsibleSection>

          {/* Sub-step 0 navigation */}
          <div className="flex justify-end">
            <Button className="gap-2" onClick={() => { savePipelineConfig(); setConfigStep(1); }} disabled={!subStep0Valid}>
              Next: Sink
              <ArrowRight className="h-4 w-4" />
            </Button>
          </div>
          </>
          )}

          {/* ---- Sub-step 1 continued: Sinks ---- */}
          {configStep === 1 && (
          <>
          {/* ---- Real-Time Sinks (only when realtime) ---- */}
          {processingMode === "realtime" && (
            <CollapsibleSection
              title="Real-Time Sinks"
              icon={Radio}
              complete={!!rtmSink}
              defaultOpen
            >
              <div className="space-y-6">
                <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
                  {([
                    { id: "lakebase" as const, label: "Lakebase", description: "Write to PostgreSQL via buffered ForeachWriter", icon: Database },
                    { id: "websocket" as const, label: "WebSocket", description: "Stream to browser dashboard via HTTP POST", icon: Radio },
                    { id: "both" as const, label: "Both", description: "Lakebase persistence + live WebSocket broadcast", icon: Layers },
                  ]).map((s) => (
                    <Card
                      key={s.id}
                      onClick={() => setRtmSink(s.id)}
                      className={cn(
                        "cursor-pointer transition-all hover:border-primary/50",
                        rtmSink === s.id && "border-primary bg-primary/5 shadow-md"
                      )}
                    >
                      <CardContent className="flex items-center gap-4 p-4">
                        <div className={cn("flex h-10 w-10 shrink-0 items-center justify-center rounded-lg", rtmSink === s.id ? "bg-primary/15 text-primary" : "bg-muted text-muted-foreground")}>
                          <s.icon className="h-5 w-5" />
                        </div>
                        <div>
                          <p className="text-sm font-semibold">{s.label}</p>
                          <p className="text-xs text-muted-foreground">{s.description}</p>
                        </div>
                        {rtmSink === s.id && <CheckCircle2 className="ml-auto h-4 w-4 shrink-0 text-primary" />}
                      </CardContent>
                    </Card>
                  ))}
                </div>

                {(rtmSink === "lakebase" || rtmSink === "both") && (
                  <Card>
                    <CardHeader>
                      <CardTitle className="flex items-center gap-2">
                        <Database className="h-4 w-4" />
                        Lakebase Connection
                      </CardTitle>
                    </CardHeader>
                    <CardContent className="space-y-5">
                      <div className="flex gap-2">
                        {([
                          { id: "autoscale" as const, label: "Autoscale (Managed)" },
                          { id: "direct" as const, label: "Direct Connection" },
                        ]).map((m) => (
                          <button
                            key={m.id}
                            onClick={() => setLakebaseMode(m.id)}
                            className={cn(
                              "rounded-md border px-3 py-1.5 text-xs font-medium transition-colors",
                              lakebaseMode === m.id
                                ? "border-primary bg-primary/10 text-primary"
                                : "border-input text-muted-foreground hover:bg-muted",
                            )}
                          >
                            {m.label}
                          </button>
                        ))}
                      </div>

                      {/* Auto-populated from Global Config */}
                      <div className="rounded-md border border-blue-500/20 bg-blue-500/5 p-3 space-y-2">
                        <p className="text-xs font-medium text-blue-600 flex items-center gap-1.5">
                          <Settings2 className="h-3.5 w-3.5" />
                          From Global Config
                        </p>
                        <div className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-1 text-xs">
                          {lakebaseMode === "autoscale" ? (
                            <>
                              <span className="text-muted-foreground">Project</span>
                              <code>{lbProjectId || "—"}</code>
                              <span className="text-muted-foreground">Branch</span>
                              <code>{lbBranchId || "—"}</code>
                              <span className="text-muted-foreground">Database</span>
                              <code>{lbDatabase || "—"}</code>
                            </>
                          ) : (
                            <>
                              <span className="text-muted-foreground">Host</span>
                              <code>{lbHost || "—"}</code>
                              <span className="text-muted-foreground">Port</span>
                              <code>{lbPort || "—"}</code>
                              <span className="text-muted-foreground">Database</span>
                              <code>{lbDatabase || "—"}</code>
                            </>
                          )}
                          <span className="text-muted-foreground">Credentials</span>
                          <span className="text-green-600 flex items-center gap-1">
                            <Lock className="h-3 w-3" /> Stored as UC Secrets
                          </span>
                        </div>
                        <p className="text-[10px] text-muted-foreground">
                          Connection details are configured in{" "}
                          <Link to="/settings" className="text-primary underline underline-offset-2">Global Settings</Link>.
                        </p>
                      </div>

                      <Separator />

                      {lakebaseMode === "autoscale" && (
                        <LaketsSetup
                          pipelineId={pipelineId}
                          industry={pipelineIndustry}
                          secretScope={pipelineResources?.secret_scope || globalSettings?.secrets?.scope}
                          defaultProjectId={lbProjectId}
                          defaultBranchId={lbBranchId}
                          defaultDatabase={lbDatabase}
                          defaultTableName={lbPgTable}
                          onConfigChange={({ tableName, tiering }) => {
                            setLbPgTable(tableName);
                            setLbTiering(tiering);
                          }}
                          onSetupComplete={(projectId) => {
                            setLbProjectId(projectId);
                            setPgTableStatus("success");
                          }}
                        />
                      )}

                      {lakebaseMode === "direct" && (
                        <div>
                          <p className="mb-2 text-xs font-medium uppercase tracking-wider text-muted-foreground">
                            Target PostgreSQL Table
                          </p>
                          <div className="flex items-end gap-3">
                            <div className="flex-1">
                              <InputField label="Table Name" placeholder="sensor_readings" value={lbPgTable} onChange={setLbPgTable} />
                            </div>
                            <Button
                              variant="outline"
                              size="sm"
                              className="mb-0.5 gap-1.5 text-xs"
                              onClick={handleProvisionPgTable}
                              disabled={pgTableStatus === "loading"}
                            >
                              {pgTableStatus === "loading" ? (
                                <Loader2 className="h-3 w-3 animate-spin" />
                              ) : (
                                <TableProperties className="h-3 w-3" />
                              )}
                              Create Table
                            </Button>
                          </div>
                          {pgTableStatus === "success" && (
                            <p className="mt-1.5 flex items-center gap-1 text-xs text-green-600">
                              <CheckCircle2 className="h-3 w-3" /> Table created
                            </p>
                          )}
                          {pgTableStatus === "error" && pgTableError && (
                            <p className="mt-1.5 flex items-center gap-1 text-xs text-destructive">
                              <AlertCircle className="h-3 w-3" /> {pgTableError}
                            </p>
                          )}
                        </div>
                      )}

                      <Separator />

                      <div>
                        <p className="mb-2 text-xs font-medium uppercase tracking-wider text-muted-foreground">
                          Flush Settings
                        </p>
                        <div className="flex items-center gap-3">
                          <label className="text-xs text-muted-foreground">Buffer size</label>
                          <input
                            type="number"
                            min={1}
                            max={500}
                            value={lakebaseBufferSize}
                            onChange={(e) => setLakebaseBufferSize(Number(e.target.value) || 50)}
                            className="w-20 rounded-md border border-input bg-background px-2 py-1 text-center text-sm focus:outline-none focus:ring-1 focus:ring-ring"
                          />
                          <span className="text-xs text-muted-foreground">rows</span>
                        </div>
                        <p className="mt-1 text-[10px] text-muted-foreground">
                          Lower = faster flush, higher = more efficient batching
                        </p>
                      </div>
                    </CardContent>
                  </Card>
                )}

                {(rtmSink === "websocket" || rtmSink === "both") && (
                  <Card>
                    <CardHeader>
                      <CardTitle className="flex items-center gap-2">
                        <Radio className="h-4 w-4" />
                        WebSocket Sink
                      </CardTitle>
                    </CardHeader>
                    <CardContent className="space-y-5">
                      <div className="flex items-center gap-2 rounded-md border border-green-500/20 bg-green-500/5 px-3 py-2">
                        <CheckCircle2 className="h-3.5 w-3.5 shrink-0 text-green-600" />
                        <p className="text-[11px] text-green-700">
                          WebSocket will use this app&apos;s endpoint (<code className="rounded bg-green-500/10 px-1 text-[10px]">{wsAppUrl || window.location.origin}/ws</code>).
                        </p>
                      </div>
                      <Separator />
                      <div>
                        <p className="mb-2 text-xs font-medium uppercase tracking-wider text-muted-foreground">
                          Flush Settings
                        </p>
                        <div className="flex items-center gap-3">
                          <label className="text-xs text-muted-foreground">Buffer size</label>
                          <input
                            type="number"
                            min={1}
                            max={200}
                            value={wsBufferSize}
                            onChange={(e) => setWsBufferSize(Number(e.target.value) || 20)}
                            className="w-20 rounded-md border border-input bg-background px-2 py-1 text-center text-sm focus:outline-none focus:ring-1 focus:ring-ring"
                          />
                          <span className="text-xs text-muted-foreground">rows</span>
                        </div>
                        <p className="mt-1 text-[10px] text-muted-foreground">
                          Lower = faster flush, higher = more efficient batching
                        </p>
                      </div>
                    </CardContent>
                  </Card>
                )}
              </div>
            </CollapsibleSection>
          )}

          {/* ---- Sinks (microbatch -- multi-select) ---- */}
          {processingMode === "microbatch" && (
            <CollapsibleSection
              title="Sinks"
              icon={Database}
              complete={microbatchSinks.size > 0}
              defaultOpen
            >
              <div className="space-y-6">
                <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
                  {([
                    { id: "delta" as const, label: "Delta Table", description: "Write to Unity Catalog Delta table", icon: Database },
                    { id: "lakebase" as const, label: "Lakebase", description: "Write to PostgreSQL via foreachBatch", icon: Database },
                    { id: "websocket" as const, label: "WebSocket", description: "Stream to browser dashboard", icon: Radio },
                  ]).map((s) => {
                    const active = microbatchSinks.has(s.id);
                    return (
                      <Card
                        key={s.id}
                        onClick={() => {
                          setMicrobatchSinks((prev) => {
                            const next = new Set(prev);
                            if (next.has(s.id)) next.delete(s.id);
                            else next.add(s.id);
                            return next;
                          });
                        }}
                        className={cn(
                          "cursor-pointer transition-all hover:border-primary/50",
                          active && "border-primary bg-primary/5 shadow-md"
                        )}
                      >
                        <CardContent className="flex items-center gap-4 p-4">
                          <div className={cn("flex h-10 w-10 shrink-0 items-center justify-center rounded-lg", active ? "bg-primary/15 text-primary" : "bg-muted text-muted-foreground")}>
                            <s.icon className="h-5 w-5" />
                          </div>
                          <div>
                            <p className="text-sm font-semibold">{s.label}</p>
                            <p className="text-xs text-muted-foreground">{s.description}</p>
                          </div>
                          {active && <CheckCircle2 className="ml-auto h-4 w-4 shrink-0 text-primary" />}
                        </CardContent>
                      </Card>
                    );
                  })}
                </div>

                {/* Delta table config */}
                {microbatchSinks.has("delta") && (
                  <Card>
                    <CardHeader>
                      <CardTitle className="flex items-center gap-2">
                        <Database className="h-4 w-4" />
                        Delta Table Configuration
                      </CardTitle>
                    </CardHeader>
                    <CardContent className="space-y-4">
                      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
                        <InputField label="Target Table (Bronze)" placeholder="catalog.schema.iot_bronze" value={sinkTable} onChange={setSinkTable} />
                        <InputField label="Checkpoint Location" placeholder={`/checkpoints/${sinkTable || "bronze"}`} value={checkpointLocation} onChange={setCheckpointLocation} />
                      </div>

                      {sinkTable && parseCatalogSchema(sinkTable) && (
                        <div className="flex items-center gap-3">
                          <Button
                            variant="outline"
                            size="sm"
                            className="gap-1.5 text-xs"
                            onClick={() => handleProvisionUc(sinkTable, setSinkUcStatus, setSinkUcError)}
                            disabled={sinkUcStatus === "loading"}
                          >
                            {sinkUcStatus === "loading" ? (
                              <Loader2 className="h-3 w-3 animate-spin" />
                            ) : (
                              <Plus className="h-3 w-3" />
                            )}
                            Provision Catalog, Schema & Volume
                          </Button>
                          {sinkUcStatus === "success" && (
                            <span className="flex items-center gap-1 text-xs text-green-600">
                              <CheckCircle2 className="h-3 w-3" /> Created
                            </span>
                          )}
                          {sinkUcStatus === "error" && sinkUcError && (
                            <span className="flex items-center gap-1 text-xs text-destructive">
                              <AlertCircle className="h-3 w-3" /> {sinkUcError}
                            </span>
                          )}
                        </div>
                      )}
                    </CardContent>
                  </Card>
                )}

                {/* Lakebase config (reused from RTM) */}
                {microbatchSinks.has("lakebase") && (
                  <Card>
                    <CardHeader>
                      <CardTitle className="flex items-center gap-2">
                        <Database className="h-4 w-4" />
                        Lakebase Connection
                      </CardTitle>
                    </CardHeader>
                    <CardContent className="space-y-5">
                      {/* Auto-populated from Global Config */}
                      <div className="rounded-md border border-blue-500/20 bg-blue-500/5 p-3 space-y-2">
                        <p className="text-xs font-medium text-blue-600 flex items-center gap-1.5">
                          <Settings2 className="h-3.5 w-3.5" />
                          From Global Config
                        </p>
                        <div className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-1 text-xs">
                          {lakebaseMode === "autoscale" ? (
                            <>
                              <span className="text-muted-foreground">Project</span>
                              <code>{lbProjectId || "—"}</code>
                              <span className="text-muted-foreground">Branch</span>
                              <code>{lbBranchId || "—"}</code>
                              <span className="text-muted-foreground">Database</span>
                              <code>{lbDatabase || "—"}</code>
                            </>
                          ) : (
                            <>
                              <span className="text-muted-foreground">Host</span>
                              <code>{lbHost || "—"}</code>
                              <span className="text-muted-foreground">Port</span>
                              <code>{lbPort || "—"}</code>
                              <span className="text-muted-foreground">Database</span>
                              <code>{lbDatabase || "—"}</code>
                            </>
                          )}
                          <span className="text-muted-foreground">Credentials</span>
                          <span className="text-green-600 flex items-center gap-1">
                            <Lock className="h-3 w-3" /> Stored as UC Secrets
                          </span>
                        </div>
                        <p className="text-[10px] text-muted-foreground">
                          Connection details are configured in{" "}
                          <Link to="/settings" className="text-primary underline underline-offset-2">Global Settings</Link>.
                        </p>
                      </div>
                      <Separator />
                      {lakebaseMode === "autoscale" ? (
                        <LaketsSetup
                          pipelineId={pipelineId}
                          industry={pipelineIndustry}
                          secretScope={pipelineResources?.secret_scope || globalSettings?.secrets?.scope}
                          defaultProjectId={lbProjectId}
                          defaultBranchId={lbBranchId}
                          defaultDatabase={lbDatabase}
                          defaultTableName={lbPgTable}
                          onConfigChange={({ tableName, tiering }) => {
                            setLbPgTable(tableName);
                            setLbTiering(tiering);
                          }}
                          onSetupComplete={(projectId) => {
                            setLbProjectId(projectId);
                            setPgTableStatus("success");
                          }}
                        />
                      ) : (
                        <div className="flex items-end gap-3">
                          <div className="flex-1">
                            <InputField label="PG Table" placeholder="sensor_readings" value={lbPgTable} onChange={setLbPgTable} />
                          </div>
                          <Button variant="outline" size="sm" className="mb-0.5 gap-1.5 text-xs" onClick={handleProvisionPgTable} disabled={pgTableStatus === "loading"}>
                            {pgTableStatus === "loading" ? <Loader2 className="h-3 w-3 animate-spin" /> : <TableProperties className="h-3 w-3" />}
                            Create Table
                          </Button>
                        </div>
                      )}
                      <Separator />
                      <div>
                        <p className="mb-2 text-xs font-medium uppercase tracking-wider text-muted-foreground">Flush Settings</p>
                        <div className="flex items-center gap-3">
                          <label className="text-xs text-muted-foreground">Buffer size</label>
                          <input type="number" min={1} max={500} value={lakebaseBufferSize} onChange={(e) => setLakebaseBufferSize(Number(e.target.value) || 50)} className="w-20 rounded-md border border-input bg-background px-2 py-1 text-center text-sm focus:outline-none focus:ring-1 focus:ring-ring" />
                          <span className="text-xs text-muted-foreground">rows</span>
                        </div>
                      </div>
                    </CardContent>
                  </Card>
                )}

                {/* WebSocket config */}
                {microbatchSinks.has("websocket") && (
                  <Card>
                    <CardHeader>
                      <CardTitle className="flex items-center gap-2">
                        <Radio className="h-4 w-4" />
                        WebSocket Sink
                      </CardTitle>
                    </CardHeader>
                    <CardContent className="space-y-5">
                      <div className="flex items-center gap-2 rounded-md border border-green-500/20 bg-green-500/5 px-3 py-2">
                        <CheckCircle2 className="h-3.5 w-3.5 shrink-0 text-green-600" />
                        <p className="text-[11px] text-green-700">
                          WebSocket will use this app&apos;s endpoint (<code className="rounded bg-green-500/10 px-1 text-[10px]">{wsAppUrl || window.location.origin}/ws</code>).
                        </p>
                      </div>
                      <Separator />
                      <div>
                        <p className="mb-2 text-xs font-medium uppercase tracking-wider text-muted-foreground">Flush Settings</p>
                        <div className="flex items-center gap-3">
                          <label className="text-xs text-muted-foreground">Buffer size</label>
                          <input type="number" min={1} max={200} value={wsBufferSize} onChange={(e) => setWsBufferSize(Number(e.target.value) || 20)} className="w-20 rounded-md border border-input bg-background px-2 py-1 text-center text-sm focus:outline-none focus:ring-1 focus:ring-ring" />
                          <span className="text-xs text-muted-foreground">rows</span>
                        </div>
                      </div>
                    </CardContent>
                  </Card>
                )}
              </div>
            </CollapsibleSection>
          )}

          {/* Sub-step 1 navigation */}
          <div className="flex justify-between">
            <Button variant="outline" className="gap-2" onClick={() => setConfigStep(0)}>
              <ArrowLeft className="h-4 w-4" />
              Back: Source
            </Button>
            <Button className="gap-2" onClick={() => { savePipelineConfig(); setConfigStep(2); }} disabled={!subStep1Valid}>
              Next: Metrics
              <ArrowRight className="h-4 w-4" />
            </Button>
          </div>
          </>
          )}

          {/* ---- Sub-step 2: Streaming Metrics ---- */}
          {configStep === 2 && (
          <>
          <CollapsibleSection
            title="Streaming Metrics"
            icon={BarChart3}
            complete={!metricsEnabled || !!metricsTable}
            defaultOpen
          >
            <Card>
              <CardContent className="space-y-4 pt-6">
                {/* Streaming metrics are always on by admin default — shown
                    as a read-only status, not a user-facing on/off toggle. */}
                <div className="flex items-center gap-2">
                  <span className="inline-flex items-center gap-1.5 rounded-full border border-green-500/30 bg-green-500/10 px-2.5 py-1 text-xs font-medium text-green-600">
                    <CheckCircle2 className="h-3 w-3" /> Streaming metrics: on
                  </span>
                  <span className="text-xs text-muted-foreground">Always captured — no toggle.</span>
                </div>

                {metricsEnabled && (
                  <>
                    <InputField label="Metrics Table" placeholder="catalog.schema.streaming_metrics" value={metricsTable} onChange={setMetricsTable} />

                    {metricsTable && parseCatalogSchema(metricsTable) && (() => {
                      const parsed = parseCatalogSchema(metricsTable);
                      const catalogMatchesPipeline = parsed && pipelineCatalog && parsed.catalog === pipelineCatalog;
                      return catalogMatchesPipeline ? (
                        <div className="flex items-center gap-2 rounded-md border border-green-500/20 bg-green-500/5 px-3 py-2">
                          <CheckCircle2 className="h-3.5 w-3.5 shrink-0 text-green-600" />
                          <p className="text-[11px] text-green-700">
                            Catalog <strong>{parsed.catalog}</strong> and schema <strong>{parsed.schema}</strong> will be provisioned automatically with the pipeline.
                          </p>
                        </div>
                      ) : (
                        <div className="flex items-center gap-3">
                          <Button
                            variant="outline"
                            size="sm"
                            className="gap-1.5 text-xs"
                            onClick={() => handleProvisionUc(metricsTable, setMetricsUcStatus, setMetricsUcError)}
                            disabled={metricsUcStatus === "loading"}
                          >
                            {metricsUcStatus === "loading" ? (
                              <Loader2 className="h-3 w-3 animate-spin" />
                            ) : (
                              <Plus className="h-3 w-3" />
                            )}
                            Provision Catalog & Schema
                          </Button>
                          {metricsUcStatus === "success" && (
                            <span className="flex items-center gap-1 text-xs text-green-600">
                              <CheckCircle2 className="h-3 w-3" /> Ready
                            </span>
                          )}
                          {metricsUcStatus === "error" && metricsUcError && (
                            <span className="flex items-center gap-1 text-xs text-destructive">
                              <AlertCircle className="h-3 w-3" /> {metricsUcError}
                            </span>
                          )}
                        </div>
                      );
                    })()}

                    {appDefaults?.zerobus_endpoint && (
                      <div className="flex items-center gap-2 rounded-md border border-green-500/20 bg-green-500/5 px-3 py-2">
                        <CheckCircle2 className="h-3.5 w-3.5 shrink-0 text-green-600" />
                        <p className="text-[11px] text-green-700">
                          Zerobus Ingest auto-configured from app environment. Metrics will be written via gRPC instead of Spark Delta writes.
                        </p>
                      </div>
                    )}

                    <Separator />

                    <div>
                      <p className="mb-2 text-xs font-medium uppercase tracking-wider text-muted-foreground">
                        Predefined Metrics
                      </p>
                      <div className="flex flex-wrap gap-1.5">
                        {PREDEFINED_METRICS.map((m) => (
                          <Badge key={m} variant="secondary" className="font-mono text-[10px]">
                            {m}
                          </Badge>
                        ))}
                      </div>
                    </div>

                    <div>
                      <p className="mb-2 text-xs font-medium uppercase tracking-wider text-muted-foreground">
                        Custom Metrics
                      </p>
                      <KeyValueEditor
                        pairs={customMetrics}
                        onChange={setCustomMetrics}
                        keyLabel="Metric Name"
                        valueLabel="SQL Expression"
                        keyPlaceholder="avg_temperature"
                        valuePlaceholder="AVG(value) WHERE sensor_type = 'temperature'"
                        addLabel="Add Metric"
                      />
                    </div>
                  </>
                )}
              </CardContent>
            </Card>
          </CollapsibleSection>

          {/* Resource Configuration removed — these values are inherited from Global Settings */}

          {/* Sub-step 2 navigation */}
          <div className="flex justify-between">
            <Button variant="outline" className="gap-2" onClick={() => setConfigStep(1)}>
              <ArrowLeft className="h-4 w-4" />
              Back: Sink
            </Button>
            <Button className="gap-2" onClick={() => { savePipelineConfig(); setConfigStep(3); }} disabled={!subStep2Valid}>
              Next: Notebook & Deploy
              <ArrowRight className="h-4 w-4" />
            </Button>
          </div>
          </>
          )}

          {/* ---- Sub-step 3: Notebook & Deploy ---- */}
          {configStep === 3 && (
          <>
          <CollapsibleSection
            title="Generated Notebook"
            icon={Code2}
            complete
            defaultOpen
          >
            <Card>
              <CardHeader>
                <CardTitle className="text-sm">Runtime Spark Configuration</CardTitle>
                <p className="text-xs text-muted-foreground">
                  These properties are set at runtime via <code className="rounded bg-muted px-1 py-0.5 text-[10px]">spark.conf.set()</code> in the notebook.
                </p>
              </CardHeader>
              <CardContent>
                <KeyValueEditor
                  pairs={notebookSparkConf}
                  onChange={setNotebookSparkConf}
                  keyPlaceholder="spark.conf.key"
                  valuePlaceholder="value"
                  keyLabel="Property"
                  valueLabel="Value"
                  addLabel="Add Property"
                />
              </CardContent>
            </Card>
            <Card className="mt-4">
              <CardContent className="space-y-3 pt-6">
                <div className="flex items-center gap-2">
                  <Badge variant="secondary" className="text-[10px]">Python</Badge>
                  <Button
                    variant={notebookEditing ? "default" : "outline"}
                    size="sm"
                    className="gap-1.5 text-xs"
                    onClick={() => {
                      if (!notebookEditing) {
                        // Entering edit mode — capture current content for the textarea
                        setNotebookOverride(displayedNotebook);
                      } else {
                        // Leaving edit mode — if content matches generated, clear override
                        if (notebookOverride === generatedNotebook) {
                          setNotebookOverride(null);
                        }
                      }
                      setNotebookEditing(!notebookEditing);
                    }}
                  >
                    {notebookEditing ? <Eye className="h-3 w-3" /> : <Pencil className="h-3 w-3" />}
                    {notebookEditing ? "Preview" : "Edit"}
                  </Button>
                  {notebookOverride !== null && (
                    <Button variant="ghost" size="sm" className="text-xs text-muted-foreground" onClick={handleResetNotebook}>
                      Reset to Generated
                    </Button>
                  )}
                </div>
                {notebookOverride !== null && !notebookEditing && (
                  <div className="flex items-center gap-2 rounded-md border border-yellow-500/30 bg-yellow-500/5 px-3 py-2">
                    <AlertTriangle className="h-3.5 w-3.5 shrink-0 text-yellow-600 dark:text-yellow-500" />
                    <span className="text-xs text-muted-foreground">
                      Custom edits active — UI changes won&apos;t update this notebook.{" "}
                      <button type="button" onClick={handleResetNotebook} className="text-primary underline-offset-4 hover:underline">
                        Reset to Generated
                      </button>
                    </span>
                  </div>
                )}
                {notebookEditing ? (
                  <textarea
                    value={notebookOverride ?? generatedNotebook}
                    onChange={(e) => setNotebookOverride(e.target.value)}
                    className="h-96 w-full rounded-md border border-input bg-background p-3 font-mono text-xs text-foreground focus:outline-none focus:ring-1 focus:ring-ring"
                    spellCheck={false}
                  />
                ) : (
                  <pre className="max-h-96 overflow-auto rounded-md border border-border bg-muted/30 p-4 font-mono text-xs text-foreground">
                    {displayedNotebook}
                  </pre>
                )}
              </CardContent>
            </Card>
          </CollapsibleSection>

          {/* ---- Validate & Deploy ---- */}
          <CollapsibleSection
            title="Validate & Deploy"
            icon={Shield}
            complete={deployState === "success"}
            defaultOpen
          >
            <Card>
              <CardContent className="space-y-4 pt-6">
                {!canValidate && (
                  <p className="text-sm text-muted-foreground">
                    {!clusterReady
                      ? "Provision a cluster above to validate and deploy the notebook."
                      : !sourceFieldsFilled
                        ? "Fill in all required source connection fields above."
                        : !sinkFieldsFilled
                          ? "Configure at least one sink above."
                          : "Complete all required fields above."}
                  </p>
                )}

                <div className="flex items-center gap-3">
                  <Button
                    variant="outline"
                    size="lg"
                    className="gap-2"
                    disabled={!canValidate || validateState === "running" || runState === "running"}
                    onClick={handleValidate}
                  >
                    {validateState === "running" ? (
                      <Loader2 className="h-4 w-4 animate-spin" />
                    ) : (
                      <Shield className="h-4 w-4" />
                    )}
                    Validate on Cluster
                  </Button>
                  <Button
                    variant="outline"
                    size="lg"
                    className="gap-2"
                    disabled={!canValidate || runState === "running" || validateState === "running"}
                    onClick={handleRunOnCluster}
                  >
                    {runState === "running" ? (
                      <Loader2 className="h-4 w-4 animate-spin" />
                    ) : (
                      <Play className="h-4 w-4" />
                    )}
                    Run on Cluster
                  </Button>
                </div>

                {validateState === "running" && (
                  <div className="space-y-2 rounded-lg border border-border bg-muted/20 p-4">
                    {[
                      { icon: Upload, label: "Uploading notebook..." },
                      { icon: Cpu, label: "Executing on cluster..." },
                      { icon: FileCheck, label: "Checking results..." },
                    ].map((s, i) => {
                      const done = validatePhase > i;
                      const active = validatePhase === i;
                      const Icon = s.icon;
                      return (
                        <div key={i} className="flex items-center gap-2.5 text-sm">
                          {done ? (
                            <CheckCircle2 className="h-4 w-4 text-green-600 shrink-0" />
                          ) : active ? (
                            <Loader2 className="h-4 w-4 animate-spin text-primary shrink-0" />
                          ) : (
                            <Circle className="h-4 w-4 text-muted-foreground/40 shrink-0" />
                          )}
                          <Icon className={cn("h-3.5 w-3.5 shrink-0", active ? "text-primary" : done ? "text-green-600" : "text-muted-foreground/40")} />
                          <span className={cn(active ? "text-foreground font-medium" : done ? "text-muted-foreground" : "text-muted-foreground/40")}>
                            {s.label}
                          </span>
                        </div>
                      );
                    })}
                  </div>
                )}

                {validateState === "success" && (
                  <div className="rounded-lg border border-green-500/30 bg-green-500/5 p-4 space-y-3">
                    <div className="flex items-center gap-2 text-sm font-medium text-green-600">
                      <CheckCircle2 className="h-4 w-4 shrink-0" />
                      Notebook validated successfully
                    </div>
                    {validateOutput && (
                      <div>
                        <button
                          type="button"
                          onClick={() => setValidateOutputCollapsed((v) => !v)}
                          className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors"
                        >
                          <ChevronDown className={cn("h-3 w-3 transition-transform", !validateOutputCollapsed && "rotate-180")} />
                          {validateOutputCollapsed ? "Show output" : "Hide output"}
                        </button>
                        {!validateOutputCollapsed && (
                          <pre className="mt-2 max-h-48 overflow-auto rounded-md border border-border bg-muted/30 p-3 font-mono text-xs text-foreground">
                            {validateOutput}
                          </pre>
                        )}
                      </div>
                    )}
                  </div>
                )}

                {validateState === "error" && (
                  <div className="rounded-lg border border-destructive/30 bg-destructive/5 p-4 space-y-3">
                    <div className="flex items-center gap-2 text-sm font-medium text-destructive">
                      <AlertCircle className="h-4 w-4 shrink-0" />
                      Validation failed
                    </div>
                    {validateError && (
                      <pre className="max-h-64 overflow-auto rounded-md border border-destructive/20 bg-muted/30 p-3 font-mono text-xs text-foreground whitespace-pre-wrap break-words">
                        {validateError}
                      </pre>
                    )}
                    {validateOutput && (
                      <div>
                        <button
                          type="button"
                          onClick={() => setValidateOutputCollapsed((v) => !v)}
                          className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors"
                        >
                          <ChevronDown className={cn("h-3 w-3 transition-transform", !validateOutputCollapsed && "rotate-180")} />
                          {validateOutputCollapsed ? "Show raw output" : "Hide raw output"}
                        </button>
                        {!validateOutputCollapsed && (
                          <pre className="mt-2 max-h-48 overflow-auto rounded-md border border-border bg-muted/30 p-3 font-mono text-xs text-foreground">
                            {validateOutput}
                          </pre>
                        )}
                      </div>
                    )}
                    <p className="text-xs text-muted-foreground">
                      Validation is advisory — you can still deploy.{" "}
                      <button
                        type="button"
                        onClick={() => {
                          const el = document.getElementById("deploy-section");
                          el?.scrollIntoView({ behavior: "smooth" });
                        }}
                        className="text-primary underline-offset-4 hover:underline"
                      >
                        Skip validation and deploy &rarr;
                      </button>
                    </p>
                  </div>
                )}

                {validateState === "timeout" && (
                  <div className="rounded-lg border border-yellow-500/30 bg-yellow-500/5 p-4 space-y-3">
                    <div className="flex items-center gap-2 text-sm font-medium text-yellow-600 dark:text-yellow-500">
                      <AlertTriangle className="h-4 w-4 shrink-0" />
                      Validation timed out
                    </div>
                    <p className="text-sm text-muted-foreground">
                      {validateError || "This is expected for streaming notebooks. You can proceed to deploy."}
                    </p>
                  </div>
                )}

                {/* Run on Cluster feedback */}
                {runState === "running" && (
                  <div className="space-y-2 rounded-lg border border-border bg-muted/20 p-4">
                    {[
                      { icon: Upload, label: "Uploading notebook..." },
                      { icon: Cpu, label: "Running on cluster (extended timeout)..." },
                      { icon: FileCheck, label: "Checking results..." },
                    ].map((s, i) => {
                      const done = runPhase > i;
                      const active = runPhase === i;
                      const Icon = s.icon;
                      return (
                        <div key={i} className="flex items-center gap-2.5 text-sm">
                          {done ? (
                            <CheckCircle2 className="h-4 w-4 text-green-600 shrink-0" />
                          ) : active ? (
                            <Loader2 className="h-4 w-4 animate-spin text-primary shrink-0" />
                          ) : (
                            <Circle className="h-4 w-4 text-muted-foreground/40 shrink-0" />
                          )}
                          <Icon className={cn("h-3.5 w-3.5 shrink-0", active ? "text-primary" : done ? "text-green-600" : "text-muted-foreground/40")} />
                          <span className={cn(active ? "text-foreground font-medium" : done ? "text-muted-foreground" : "text-muted-foreground/40")}>
                            {s.label}
                          </span>
                        </div>
                      );
                    })}
                  </div>
                )}

                {runState === "success" && (
                  <div className="rounded-lg border border-green-500/30 bg-green-500/5 p-4 space-y-3">
                    <div className="flex items-center gap-2 text-sm font-medium text-green-600">
                      <CheckCircle2 className="h-4 w-4 shrink-0" />
                      Notebook ran successfully
                    </div>
                    {runOutput && (
                      <div>
                        <button
                          type="button"
                          onClick={() => setRunOutputCollapsed((v) => !v)}
                          className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors"
                        >
                          <ChevronDown className={cn("h-3 w-3 transition-transform", !runOutputCollapsed && "rotate-180")} />
                          {runOutputCollapsed ? "Show output" : "Hide output"}
                        </button>
                        {!runOutputCollapsed && (
                          <pre className="mt-2 max-h-48 overflow-auto rounded-md border border-border bg-muted/30 p-3 font-mono text-xs text-foreground">
                            {runOutput}
                          </pre>
                        )}
                      </div>
                    )}
                  </div>
                )}

                {runState === "error" && (
                  <div className="rounded-lg border border-destructive/30 bg-destructive/5 p-4 space-y-3">
                    <div className="flex items-center gap-2 text-sm font-medium text-destructive">
                      <AlertCircle className="h-4 w-4 shrink-0" />
                      {runError || "Run failed"}
                    </div>
                    {runOutput && (
                      <div>
                        <button
                          type="button"
                          onClick={() => setRunOutputCollapsed((v) => !v)}
                          className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors"
                        >
                          <ChevronDown className={cn("h-3 w-3 transition-transform", !runOutputCollapsed && "rotate-180")} />
                          {runOutputCollapsed ? "Show raw output" : "Hide raw output"}
                        </button>
                        {!runOutputCollapsed && (
                          <pre className="mt-2 max-h-48 overflow-auto rounded-md border border-border bg-muted/30 p-3 font-mono text-xs text-foreground">
                            {runOutput}
                          </pre>
                        )}
                      </div>
                    )}
                  </div>
                )}

                <Separator />

                <div id="deploy-section" className="space-y-3">
                  <div className="flex items-center gap-3">
                    <Button
                      size="lg"
                      className="gap-2"
                      disabled={!canValidate || deployState === "deploying"}
                      onClick={handleDeployJob}
                    >
                      {deployState === "deploying" ? (
                        <Loader2 className="h-4 w-4 animate-spin" />
                      ) : (
                        <Rocket className="h-4 w-4" />
                      )}
                      {deployState === "success" ? "Re-deploy" : "Deploy & Run on Job Cluster"}
                    </Button>
                    {deployState === "success" && deployJobUrl && (
                      <a
                        href={deployJobUrl}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="flex items-center gap-1 text-sm text-primary underline-offset-4 hover:underline"
                      >
                        <ExternalLink className="h-3.5 w-3.5" />
                        View Job
                      </a>
                    )}
                    {deployState === "error" && deployError && (
                      <span className="flex items-center gap-1 text-sm text-destructive">
                        <AlertCircle className="h-4 w-4" /> {deployError}
                      </span>
                    )}
                  </div>

                  {deployState === "success" && (
                    <div className="space-y-2">
                      <div className="flex items-center gap-3 rounded-lg border border-border bg-muted/30 p-3">
                        {jobRunState === "polling" && (
                          <span className="flex items-center gap-2 text-sm text-muted-foreground">
                            <Loader2 className="h-4 w-4 animate-spin" />
                            {jobRunMessage}
                          </span>
                        )}
                        {jobRunState === "running" && (
                          <>
                            <span className="flex items-center gap-2 text-sm text-green-600">
                              <span className="relative flex h-2.5 w-2.5">
                                <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-green-400 opacity-75" />
                                <span className="relative inline-flex h-2.5 w-2.5 rounded-full bg-green-500" />
                              </span>
                              {jobRunMessage}
                            </span>
                            <Button
                              size="sm"
                              variant="outline"
                              className="ml-auto gap-1.5 text-xs"
                              disabled={jobActionLoading}
                              onClick={handleStopJob}
                            >
                              {jobActionLoading ? <Loader2 className="h-3 w-3 animate-spin" /> : <Square className="h-3 w-3" />}
                              Stop
                            </Button>
                          </>
                        )}
                        {jobRunState === "stopped" && (
                          <>
                            <span className="flex items-center gap-2 text-sm text-amber-600">
                              <span className="h-2.5 w-2.5 rounded-full bg-amber-500" />
                              Job paused
                            </span>
                            <Button
                              size="sm"
                              variant="outline"
                              className="ml-auto gap-1.5 text-xs"
                              disabled={jobActionLoading}
                              onClick={handleRestartJob}
                            >
                              {jobActionLoading ? <Loader2 className="h-3 w-3 animate-spin" /> : <Play className="h-3 w-3" />}
                              Restart
                            </Button>
                          </>
                        )}
                        {jobRunState === "error" && (
                          <>
                            <span className="flex items-center gap-2 text-sm text-destructive">
                              <AlertCircle className="h-4 w-4" />
                              {jobRunMessage}
                            </span>
                            <Button
                              size="sm"
                              variant="outline"
                              className="ml-auto gap-1.5 text-xs"
                              disabled={jobActionLoading}
                              onClick={handleRestartJob}
                            >
                              {jobActionLoading ? <Loader2 className="h-3 w-3 animate-spin" /> : <RefreshCw className="h-3 w-3" />}
                              Retry
                            </Button>
                          </>
                        )}
                      </div>
                      {/* Per-task status breakdown */}
                      {jobTaskStates && Object.keys(jobTaskStates).length > 1 && (
                        <div className="rounded-lg border border-border bg-muted/20 p-3 space-y-1.5">
                          {Object.entries(jobTaskStates).map(([taskKey, taskState]) => {
                            const isFailed = taskState.state === "FAILED" || taskState.state === "INTERNAL_ERROR";
                            const isRunning = taskState.state === "RUNNING";
                            const isPending = taskState.state === "PENDING" || taskState.state === "QUEUED" || taskState.state === "BLOCKED";
                            const label = taskKey === "sss_consumer" ? "SSS Consumer" : taskKey === "data_producer" ? "Data Producer" : taskKey;
                            return (
                              <div key={taskKey}>
                                <div className="flex items-center gap-2">
                                  {isRunning && (
                                    <span className="relative flex h-2 w-2">
                                      <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-green-400 opacity-75" />
                                      <span className="relative inline-flex h-2 w-2 rounded-full bg-green-500" />
                                    </span>
                                  )}
                                  {isPending && <Loader2 className="h-3 w-3 animate-spin text-muted-foreground" />}
                                  {isFailed && <AlertCircle className="h-3 w-3 text-destructive" />}
                                  {!isRunning && !isPending && !isFailed && <span className="h-2 w-2 rounded-full bg-muted-foreground/30" />}
                                  <span className={cn(
                                    "text-xs font-medium",
                                    isRunning && "text-green-600",
                                    isFailed && "text-destructive",
                                    isPending && "text-muted-foreground",
                                  )}>
                                    {label}
                                  </span>
                                  <Badge variant={isFailed ? "destructive" : "secondary"} className="text-[10px]">
                                    {taskState.state}
                                  </Badge>
                                </div>
                                {isFailed && taskState.state_message && (
                                  <p className="ml-4 mt-0.5 text-[10px] text-destructive">{taskState.state_message}</p>
                                )}
                              </div>
                            );
                          })}
                        </div>
                      )}
                    </div>
                  )}
                </div>
              </CardContent>
            </Card>
          </CollapsibleSection>

          {/* Sub-step 3 navigation */}
          <div className="flex justify-start">
            <Button variant="outline" className="gap-2" onClick={() => setConfigStep(2)}>
              <ArrowLeft className="h-4 w-4" />
              Back: Metrics
            </Button>
          </div>
          </>
          )}
        </>
      )}

      {/* ================================================================ */}
      {/* Zerobus Configuration */}
      {/* ================================================================ */}
      {selectedPipeline === "zerobus" && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Database className="h-4 w-4" />
              Zerobus Configuration
            </CardTitle>
          </CardHeader>
          <CardContent>
            {appDefaults?.zerobus_endpoint && (
              <div className="flex items-center gap-2 rounded-md border border-green-500/20 bg-green-500/5 px-3 py-2 mb-3">
                <CheckCircle2 className="h-3.5 w-3.5 shrink-0 text-green-600" />
                <p className="text-[11px] text-green-700">Auto-configured from global settings.</p>
              </div>
            )}
            <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
              <InputField label="Server Endpoint" placeholder="grpc://zerobus.example.com:443" value={zbEndpoint} onChange={setZbEndpoint} />
              <InputField label="Target Table" placeholder="catalog.schema.iot_ingest" value={zbTable} onChange={setZbTable} />
              <InputField label="Client ID" placeholder="service-principal-id" value={zbClientId} onChange={setZbClientId} />
              <InputField label="Client Secret" placeholder="**************" value={zbClientSecret} onChange={setZbClientSecret} type="password" />
            </div>

            <Separator className="my-4" />

            <div className="space-y-3">
              <div className="flex items-center gap-3">
                <Button
                  size="lg"
                  className="gap-2"
                  disabled={!zbEndpoint.trim() || !zbTable.trim() || deployState === "deploying"}
                  onClick={handleDeployJob}
                >
                  {deployState === "deploying" ? (
                    <Loader2 className="h-4 w-4 animate-spin" />
                  ) : (
                    <Rocket className="h-4 w-4" />
                  )}
                  {deployState === "success" ? "Re-deploy" : "Deploy Producer"}
                </Button>
                {deployState === "success" && (
                  <span className="flex items-center gap-1 text-sm text-green-600">
                    <CheckCircle2 className="h-3.5 w-3.5" />
                    Producer deployed
                  </span>
                )}
                {deployState === "error" && deployError && (
                  <span className="text-sm text-destructive">{deployError}</span>
                )}
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {/* ================================================================ */}
      {/* WebSocket Demo */}
      {/* ================================================================ */}
      {selectedPipeline === "websocket_demo" && (
        <Card>
          <CardContent className="flex items-center gap-4 p-6">
            <Radio className="h-8 w-8 text-primary" />
            <div>
              <p className="font-semibold">WebSocket Demo Mode</p>
              <p className="text-sm text-muted-foreground">
                Sensor data streams directly to your browser via WebSocket. No
                external infrastructure needed. Configure the industry and
                payload in the Data Hub, then view live data on the Dashboard.
              </p>
            </div>
            <Button asChild className="ml-auto gap-2">
              <Link to={pipelineId ? `/pipelines/${pipelineId}/dashboard` : "/"}>
                Go to Dashboard
                <ArrowRight className="h-4 w-4" />
              </Link>
            </Button>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
