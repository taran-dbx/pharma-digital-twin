/**
 * Step 5 — mode-adaptive metrics. Spark query progress is available for
 * realtime/microbatch; Zerobus ingest telemetry comes from the Databricks
 * system table and is represented as clearly-labelled placeholders until the
 * live DBSQL query is wired.
 */
import { Activity, DatabaseZap } from "lucide-react";
import type { PipelineMode } from "@/lib/industries-api";
import { METRIC_TILES } from "./types";

const ZEROBUS_METRICS = [
  { label: "Rows ingested", unit: "rows" },
  { label: "Ingest rate", unit: "rows/s" },
  { label: "Commit latency p95", unit: "ms" },
  { label: "Last successful commit", unit: "UTC" },
] as const;

export function StepMetrics({ enabledModes }: { enabledModes: PipelineMode[] }) {
  const sparkModes = enabledModes.filter(
    (mode): mode is "realtime" | "microbatch" =>
      mode === "realtime" || mode === "microbatch",
  );
  const showZerobus = enabledModes.includes("zerobus");

  return (
    <div className="space-y-5">
      {sparkModes.length > 0 && (
        <section className="space-y-4">
          <div className="flex items-center gap-2">
            <Activity className="h-4 w-4 text-primary" />
            <h3 className="font-display text-sm font-semibold">Spark streaming metrics</h3>
            <span className="ml-auto text-xs text-muted-foreground">
              {sparkModes.join(" + ")} · managed metrics table
            </span>
          </div>

          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
            {METRIC_TILES.map((t) => (
              <div key={t.label} className="glass rounded-lg p-4">
                <p className="text-[11px] font-ui font-medium uppercase tracking-wider text-muted-foreground">
                  {t.label}
                </p>
                <p className="mt-1.5 font-display text-2xl font-semibold tabular-nums">
                  {t.value}
                  <span className="ml-1 text-sm font-medium text-muted-foreground">{t.unit}</span>
                </p>
                <p className="mt-1 text-[11px] text-muted-foreground">{t.hint}</p>
              </div>
            ))}
          </div>

          <div className="glass rounded-lg p-5 text-xs leading-relaxed text-muted-foreground">
            Spark query progress lands in the managed{" "}
            <span className="font-mono text-foreground">
              &lt;UC_CATALOG&gt;.&lt;UC_SCHEMA&gt;.&lt;industry&gt;_metrics
            </span>{" "}
            table through the Zerobus SDK. Rows are tagged by mode and query id.
          </div>
        </section>
      )}

      {showZerobus && (
        <section className="space-y-4">
          <div className="flex items-center gap-2">
            <DatabaseZap className="h-4 w-4 text-primary" />
            <h3 className="font-display text-sm font-semibold">Zerobus ingest metrics</h3>
            <span className="ml-auto font-mono text-xs text-muted-foreground">
              system.lakeflow.zerobus_ingest
            </span>
          </div>
          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
            {ZEROBUS_METRICS.map((metric) => (
              <div key={metric.label} className="glass rounded-lg border-dashed p-4">
                <p className="text-[11px] font-ui font-medium uppercase tracking-wider text-muted-foreground">
                  {metric.label}
                </p>
                <p className="mt-1.5 font-display text-2xl font-semibold text-muted-foreground">
                  —
                  <span className="ml-1 text-sm font-medium">{metric.unit}</span>
                </p>
                <p className="mt-1 text-[11px] text-muted-foreground">
                  System-table query placeholder
                </p>
              </div>
            ))}
          </div>
          <div className="rounded-lg border border-info/30 bg-info/[0.06] p-4 text-xs text-muted-foreground">
            Zerobus is not a Spark streaming query. Its operational telemetry comes from{" "}
            <span className="font-mono text-foreground">system.lakeflow.zerobus_ingest</span>;
            these tiles are intentionally placeholders until the live DBSQL query is connected.
          </div>
        </section>
      )}
    </div>
  );
}
