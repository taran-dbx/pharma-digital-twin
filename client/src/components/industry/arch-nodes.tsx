/**
 * Shared data-flow node list + icon renderer — ported from `archNodes()` /
 * `archIcon()` / `NODEIC` in the design reference
 * (the original design reference's `<script type="text/x-dc">`
 * block). Used by both `<ArchDiagram>` (the setup screen's "How your data
 * flows" panel, `.anode` markup) and `<TwinModal>`'s "View data flow" modal
 * (`.af-node` markup) so the two stay in lockstep with the design's single
 * source of truth for node lists.
 */
import type { Industry, PipelineMode } from "@/lib/industries-api";

const BRAND = {
  sss: "/brand/data-streaming-icon-full-color.svg",
  lakebase: "/brand/lakebase-icon-full-color.svg",
  uc: "/brand/unity-catalog-icon-full-color.svg",
  zerobus: "/brand/lakeflow-connect-icon-full-color.svg",
};

export type NodeIconId = "sim" | "kafka" | "dash" | "sss" | "lakebase" | "uc" | "zerobus";

export interface ArchNode {
  t: string;
  s: string;
  ic: NodeIconId;
  sink?: boolean;
}

// Ported verbatim from MODES[].name in the design reference.
export const MODE_LABEL: Record<PipelineMode, string> = {
  realtime: "Real-time",
  microbatch: "Micro-batch",
  zerobus: "Zerobus",
};

// Ported from archNodes() in the design reference.
export function archNodes(industry: Industry, mode: PipelineMode): ArchNode[] {
  const nodes: ArchNode[] = [{ t: "IoT Simulator", s: `${industry.name} sensors`, ic: "sim" }];

  if (mode === "zerobus") {
    nodes.push({ t: "Zerobus SDK", s: "direct ingest", ic: "zerobus" });
    nodes.push({ t: "Delta table", s: "Unity Catalog", ic: "uc", sink: true });
    nodes.push({ t: "Dashboard", s: "DBSQL", ic: "dash" });
    return nodes;
  }

  nodes.push({ t: "Apache Kafka", s: `iot.${industry.id}.${mode}`, ic: "kafka" });
  if (mode === "realtime") {
    nodes.push({ t: "Spark SSS", s: "real-time", ic: "sss" });
    nodes.push({ t: "Lakebase · LakeTS", s: "hot · time-series", ic: "lakebase", sink: true });
    nodes.push({ t: "CDF → Delta", s: "cold · UC", ic: "uc" });
    nodes.push({ t: "Dashboard", s: "SSE · Lakebase · DBSQL", ic: "dash" });
  } else {
    nodes.push({ t: "Spark SSS", s: "micro-batch", ic: "sss" });
    nodes.push({ t: "Delta table", s: "Unity Catalog", ic: "uc", sink: true });
    nodes.push({ t: "Dashboard", s: "DBSQL", ic: "dash" });
  }
  return nodes;
}

// Ported from archIcon()/NODEIC in the design reference — brand SVGs for
// sss/lakebase/uc/zerobus, small inline strokes for the generic nodes.
export function NodeIcon({ ic }: { ic: NodeIconId }) {
  switch (ic) {
    case "sss":
      return <img src={BRAND.sss} alt="" />;
    case "lakebase":
      return <img src={BRAND.lakebase} alt="" />;
    case "uc":
      return <img src={BRAND.uc} alt="" />;
    case "zerobus":
      return <img src={BRAND.zerobus} alt="" />;
    case "kafka":
      return (
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.6} aria-hidden="true">
          <circle cx="6" cy="12" r="2.3" />
          <circle cx="17" cy="6" r="2.3" />
          <circle cx="17" cy="18" r="2.3" />
          <path d="M8.2 11l6.6-3.6M8.2 13l6.6 3.6" />
        </svg>
      );
    case "dash":
      return (
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.6} aria-hidden="true">
          <rect x="3" y="3" width="18" height="18" rx="2" />
          <path d="M3 9h18M9 9v12" />
        </svg>
      );
    case "sim":
    default:
      return (
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.6} aria-hidden="true">
          <rect x="5" y="5" width="14" height="14" rx="2" />
          <rect x="9.5" y="9.5" width="5" height="5" />
          <path d="M9 2v3M15 2v3M9 19v3M15 19v3M2 9h3M2 15h3M19 9h3M19 15h3" />
        </svg>
      );
  }
}
