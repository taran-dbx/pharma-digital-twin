/**
 * Step 3 — Setup Preview (spec §1.6). Read-only preview of exactly what will
 * be built before validation runs: compute, topics, the SSOT schema, and the
 * real-time targets. Uses the Databricks product brand marks where products
 * are referenced.
 */
import type { ReactNode } from "react";
import { Server, Boxes, Table2, Target, Cloud, Database } from "lucide-react";
import type { PipelineMode } from "@/lib/industries-api";
import { BrandMark, KvRow } from "./components";
import {
  BRAND,
  COMPUTE,
  hasKafkaModes,
  INGEST_RATE,
  SOURCE_DEFAULTS,
  SSOT_FIELDS,
  computeWorkers,
  type IndustryOption,
} from "./types";

function PanelCard({
  icon: Icon,
  title,
  meta,
  children,
}: {
  icon: typeof Server;
  title: string;
  meta?: string;
  children: ReactNode;
}) {
  return (
    <div className="glass space-y-3 rounded-lg p-5">
      <div className="flex items-center gap-2">
        <Icon className="h-4 w-4 text-primary" />
        <h3 className="font-display text-sm font-semibold">{title}</h3>
        {meta && <span className="ml-auto text-xs text-muted-foreground">{meta}</span>}
      </div>
      {children}
    </div>
  );
}

interface StepPreviewProps {
  industry: IndustryOption;
  partitions: number;
  enabledModes: PipelineMode[];
}

export function StepPreview({ industry, partitions, enabledModes }: StepPreviewProps) {
  const workers = computeWorkers(partitions);
  const topicBase = `iot.${industry.id}`;
  const kafkaModes = enabledModes.filter(
    (mode): mode is "realtime" | "microbatch" =>
      mode === "realtime" || mode === "microbatch",
  );
  const kafkaEnabled = hasKafkaModes(enabledModes);

  return (
    <div className="space-y-5">
      {kafkaEnabled && (
        <PanelCard
          icon={Server}
          title="Compute — interactive validation cluster"
          meta="proxy for job cluster"
        >
          <div className="space-y-2">
            <KvRow k="Node type" v={`${COMPUTE.nodeType} · ${COMPUTE.vcpu}`} mono />
            <KvRow
              k="Workers"
              v={`${workers}  ·  clamp(ceil(${partitions}/4), 1, 8)`}
              mono
            />
            <KvRow k="Driver" v="same as worker node type" />
            <KvRow k="Runtime" v={COMPUTE.dbr} mono />
            <KvRow k="Update rate" v={INGEST_RATE.label} mono />
            <KvRow k="Auto-terminate" v={`${COMPUTE.autoTerminateMin} min (inactivity)`} />
          </div>
          <p className="text-xs leading-relaxed text-muted-foreground">
            {workers} workers × 4 cores ≥ {partitions} partitions, so every Kafka partition has a
            reader. Same IAM role, subnet and security group as the deploy-time job cluster.
          </p>
        </PanelCard>
      )}

      <div className="grid gap-5 lg:grid-cols-2">
        {kafkaEnabled && (
          <PanelCard icon={Boxes} title="Kafka topics">
            <div className="space-y-2.5">
              {kafkaModes.map((mode) => (
                <div
                  key={mode}
                  className="rounded-md border border-border/50 bg-surface-high px-3 py-2"
                >
                  <div className="font-mono text-xs text-foreground">{`${topicBase}.${mode}`}</div>
                  <div className="mt-0.5 text-[11px] text-muted-foreground">
                    {partitions} partitions · RF {SOURCE_DEFAULTS.replicationFactor}
                  </div>
                </div>
              ))}
            </div>
          </PanelCard>
        )}

        {enabledModes.includes("realtime") && (
          <PanelCard icon={Target} title="Real-time targets">
            <div className="space-y-2.5 text-sm">
              <div className="flex items-center gap-2">
                <BrandMark src={BRAND.lakebase.src} label="Lakebase + LakeTS" />
              </div>
              <KvRow k="Database" v="databricks_postgres" mono />
              <KvRow k="ChronoTable" v="15-min chunks · 24h / 96-chunk horizon" mono />
              <KvRow k="CDF" v="shadow table in lakets_cdf (manual enable)" />
              <div className="flex items-center gap-2 pt-1">
                <BrandMark src={BRAND.unity.src} label="Unity Catalog" />
              </div>
              <KvRow k="DBSQL warehouse" v="IoT Serverless SQL" />
            </div>
          </PanelCard>
        )}

        {enabledModes.includes("microbatch") && (
          <PanelCard icon={Database} title="Micro-batch target">
            <div className="space-y-2.5">
              <BrandMark src={BRAND.unity.src} label="Unity Catalog managed Delta" />
              <KvRow
                k="Target"
                v={`<UC_CATALOG>.<UC_SCHEMA>.${industry.id}_microbatch`}
                mono
              />
              <KvRow k="Trigger" v="bounded micro-batch consumer" />
            </div>
          </PanelCard>
        )}

        {enabledModes.includes("zerobus") && (
          <PanelCard icon={Cloud} title="Zerobus target" meta="serverless · no cluster">
            <div className="space-y-2.5">
              <BrandMark src={BRAND.zerobus.src} label="Zerobus Delta Ingest" />
              <KvRow
                k="Delta table"
                v={`<UC_CATALOG>.<UC_SCHEMA>.${industry.id}_zerobus`}
                mono
              />
              <KvRow k="Authentication" v="App service principal · OAuth" />
              <KvRow k="Commit path" v="SDK ingest → durable ack → managed Delta" />
            </div>
          </PanelCard>
        )}
      </div>

      <PanelCard icon={Table2} title="Schema contract — SSOT field-map" meta="industry-invariant">
        <div className="overflow-x-auto">
          <table className="w-full text-left text-xs">
            <thead>
              <tr className="border-b border-border/50 text-[10px] font-ui uppercase tracking-wider text-muted-foreground">
                <th className="py-1.5 pr-4 font-medium">SensorRecord field</th>
                <th className="py-1.5 pr-4 font-medium">Column</th>
                <th className="py-1.5 pr-4 font-medium">PG / Delta type</th>
                <th className="py-1.5 font-medium">Transform</th>
              </tr>
            </thead>
            <tbody className="font-mono">
              {SSOT_FIELDS.map((f) => (
                <tr key={f.field} className="border-b border-border/30 last:border-0">
                  <td className="py-1.5 pr-4 text-foreground">{f.field}</td>
                  <td className="py-1.5 pr-4 text-muted-foreground">{f.column}</td>
                  <td className="py-1.5 pr-4 text-muted-foreground">{f.type}</td>
                  <td className="py-1.5 text-muted-foreground">{f.transform}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <p className="text-xs text-muted-foreground">
          One declarative field-map generates only the selected modes&apos; target DDL, parse
          schemas, and records — so their contracts cannot drift.
        </p>
      </PanelCard>
    </div>
  );
}
