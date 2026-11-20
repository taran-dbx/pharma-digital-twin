import type { PipelineType, SssSource, SssProcessingMode, SssRtmSinkType } from "@/lib/types";

interface ArchitectureCanvasProps {
  pipeline: PipelineType | null;
  sssSource: SssSource | null;
  processingMode: SssProcessingMode;
  sinkTable: string;
  metricsEnabled: boolean;
  metricsTable: string;
  rtmSink?: SssRtmSinkType;
  active?: boolean;
}

interface NodeDef {
  id: string;
  label: string;
  sublabel?: string;
  x: number;
  y: number;
}

interface EdgeDef {
  from: string;
  to: string;
}

const SOURCE_LABELS: Record<string, string> = {
  kafka: "Apache Kafka",
  kinesis: "Amazon Kinesis",
  event_hub: "Azure Event Hub",
  rabbitmq: "RabbitMQ",
  pubsub: "Google Pub/Sub",
};

const NODE_W = 130;
const NODE_H = 46;
const CANVAS_H = 120;

function buildGraph(props: ArchitectureCanvasProps): {
  nodes: NodeDef[];
  edges: EdgeDef[];
} {
  const { pipeline, sssSource, processingMode, sinkTable, metricsEnabled, metricsTable, rtmSink } = props;

  if (!pipeline) {
    return {
      nodes: [{ id: "sim", label: "IoT Simulator", x: 400, y: CANVAS_H / 2 }],
      edges: [],
    };
  }

  const nodes: NodeDef[] = [];
  const edges: EdgeDef[] = [];

  if (pipeline === "sss") {
    const simX = 60;
    const srcX = 230;
    const sssX = 410;
    const sinkX = 590;
    const metricsX = 590;
    const cy = CANVAS_H / 2;

    nodes.push({ id: "sim", label: "IoT Simulator", x: simX, y: cy });

    const srcLabel = sssSource ? SOURCE_LABELS[sssSource] || sssSource : "Source?";
    nodes.push({ id: "source", label: srcLabel, x: srcX, y: cy });
    edges.push({ from: "sim", to: "source" });

    const modeLabel = processingMode === "realtime" ? "Real-Time" : "Microbatch";
    nodes.push({ id: "sss", label: "SSS", sublabel: modeLabel, x: sssX, y: cy });
    edges.push({ from: "source", to: "sss" });

    const isRtm = processingMode === "realtime" && rtmSink;
    const hasFanOut = isRtm && rtmSink === "both";
    const hasMetricsFanOut = metricsEnabled;
    const fanOutCount = (hasFanOut ? 2 : 1) + (hasMetricsFanOut ? 1 : 0);
    const topOffset = fanOutCount > 1 ? -(fanOutCount - 1) * 14 : 0;

    if (isRtm) {
      if (rtmSink === "both") {
        nodes.push({ id: "sink_lb", label: "Lakebase", sublabel: "PostgreSQL", x: sinkX, y: cy + topOffset });
        edges.push({ from: "sss", to: "sink_lb" });
        nodes.push({ id: "sink_ws", label: "WebSocket", sublabel: "Dashboard", x: sinkX, y: cy + topOffset + 28 });
        edges.push({ from: "sss", to: "sink_ws" });
      } else if (rtmSink === "lakebase") {
        nodes.push({ id: "sink", label: "Lakebase", sublabel: "PostgreSQL", x: sinkX, y: cy - (hasMetricsFanOut ? 14 : 0) });
        edges.push({ from: "sss", to: "sink" });
      } else {
        nodes.push({ id: "sink", label: "WebSocket", sublabel: "Dashboard", x: sinkX, y: cy - (hasMetricsFanOut ? 14 : 0) });
        edges.push({ from: "sss", to: "sink" });
      }
    } else {
      const tableLabel = sinkTable || "Delta Bronze";
      nodes.push({ id: "sink", label: tableLabel, sublabel: "Delta Table", x: sinkX, y: cy - (hasMetricsFanOut ? 20 : 0) });
      edges.push({ from: "sss", to: "sink" });
    }

    if (hasMetricsFanOut) {
      const mLabel = metricsTable || "Metrics Table";
      const metricsY = hasFanOut ? cy + topOffset + 56 : cy + 28;
      nodes.push({ id: "metrics", label: mLabel, sublabel: "Metrics", x: metricsX, y: metricsY });
      edges.push({ from: "sss", to: "metrics" });
    }
  } else if (pipeline === "zerobus") {
    const simX = 130;
    const zbX = 370;
    const sinkX = 600;
    const cy = CANVAS_H / 2;

    nodes.push({ id: "sim", label: "IoT Simulator", x: simX, y: cy });
    nodes.push({ id: "zerobus", label: "Zerobus gRPC", x: zbX, y: cy });
    nodes.push({ id: "sink", label: "Delta Table", x: sinkX, y: cy });
    edges.push({ from: "sim", to: "zerobus" });
    edges.push({ from: "zerobus", to: "sink" });
  } else {
    const simX = 170;
    const wsX = 430;
    const dashX = 640;
    const cy = CANVAS_H / 2;

    nodes.push({ id: "sim", label: "IoT Simulator", x: simX, y: cy });
    nodes.push({ id: "ws", label: "WebSocket", x: wsX, y: cy });
    nodes.push({ id: "dash", label: "Dashboard", sublabel: "Browser", x: dashX, y: cy });
    edges.push({ from: "sim", to: "ws" });
    edges.push({ from: "ws", to: "dash" });
  }

  return { nodes, edges };
}

function NodeRect({ node, active }: { node: NodeDef; active?: boolean }) {
  const x = node.x - NODE_W / 2;
  const y = node.y - NODE_H / 2;

  // Differentiate node types by id for Databricks palette
  const isSource = node.id === "sim" || node.id === "source";
  const isSink = node.id === "sink" || node.id === "sink_lb" || node.id === "sink_ws" || node.id === "metrics";
  const isProcessing = !isSource && !isSink;

  const fillColor = isSource ? "#0d2228" : isProcessing ? "#1B3139" : "#091e24";
  const strokeColor = active
    ? "#59dea2"
    : isSource
      ? "#2A4A56"
      : isProcessing
        ? "#FF3621"
        : "#59dea2";
  const strokeWidth = active ? 2 : 1.5;

  return (
    <g>
      <rect
        x={x}
        y={y}
        width={NODE_W}
        height={NODE_H}
        rx={8}
        fill={fillColor}
        stroke={strokeColor}
        strokeWidth={strokeWidth}
        filter={active ? "url(#activeGlow)" : "url(#glow)"}
      />
      <text
        x={node.x}
        y={node.sublabel ? node.y - 4 : node.y + 1}
        textAnchor="middle"
        dominantBaseline="middle"
        fill="#cfe6f1"
        fontSize="11"
        fontWeight="500"
      >
        {node.label.length > 16 ? node.label.slice(0, 15) + "..." : node.label}
      </text>
      {node.sublabel && (
        <text
          x={node.x}
          y={node.y + 12}
          textAnchor="middle"
          dominantBaseline="middle"
          fill="#7ba8be"
          fontSize="9"
        >
          {node.sublabel}
        </text>
      )}
    </g>
  );
}

function EdgeLine({ from, to, nodes, active }: { from: string; to: string; nodes: NodeDef[]; active?: boolean }) {
  const src = nodes.find((n) => n.id === from);
  const dst = nodes.find((n) => n.id === to);
  if (!src || !dst) return null;

  const x1 = src.x + NODE_W / 2;
  const y1 = src.y;
  const x2 = dst.x - NODE_W / 2;
  const y2 = dst.y;

  const edgeColor = active ? "#59dea2" : "#FF3621";

  return (
    <g>
      <line
        x1={x1}
        y1={y1}
        x2={x2}
        y2={y2}
        stroke={edgeColor}
        strokeWidth={active ? 2 : 1.5}
        strokeDasharray="6 3"
      >
        <animate
          attributeName="stroke-dashoffset"
          from="18"
          to="0"
          dur={active ? "0.6s" : "1s"}
          repeatCount="indefinite"
        />
      </line>
      <polygon
        points={`${x2},${y2} ${x2 - 6},${y2 - 4} ${x2 - 6},${y2 + 4}`}
        fill={edgeColor}
      />
    </g>
  );
}

export function ArchitectureCanvas(props: ArchitectureCanvasProps) {
  const { nodes, edges } = buildGraph(props);
  const active = props.active ?? false;

  return (
    <div className="glass relative rounded-lg p-2">
      {active && (
        <span className="absolute right-3 top-3 z-10 flex items-center gap-1.5 rounded-full bg-[#59dea2]/15 px-2.5 py-0.5 text-[10px] font-semibold uppercase tracking-wider text-[#59dea2]">
          <span className="relative flex h-2 w-2">
            <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-[#59dea2] opacity-75" />
            <span className="relative inline-flex h-2 w-2 rounded-full bg-[#59dea2]" />
          </span>
          Live
        </span>
      )}
      <svg
        viewBox={`0 0 750 ${CANVAS_H}`}
        className="h-[120px] w-full"
        preserveAspectRatio="xMidYMid meet"
      >
        <defs>
          <filter id="glow" x="-20%" y="-20%" width="140%" height="140%">
            <feGaussianBlur stdDeviation="2" result="blur" />
            <feComposite in="SourceGraphic" in2="blur" operator="over" />
          </filter>
          <filter id="activeGlow" x="-30%" y="-30%" width="160%" height="160%">
            <feGaussianBlur stdDeviation="3" result="blur" />
            <feFlood floodColor="#59dea2" floodOpacity="0.3" result="color" />
            <feComposite in="color" in2="blur" operator="in" result="glow" />
            <feComposite in="SourceGraphic" in2="glow" operator="over" />
          </filter>
        </defs>

        {edges.map((e) => (
          <EdgeLine
            key={`${e.from}-${e.to}`}
            from={e.from}
            to={e.to}
            nodes={nodes}
            active={active}
          />
        ))}
        {nodes.map((n) => (
          <NodeRect key={n.id} node={n} active={active} />
        ))}
      </svg>
    </div>
  );
}
