/**
 * Right-column "Configuration Summary" card — mirrors the layout of the
 * pipeline wizard's summary panel (dot-grid topology, key/value rows, and an
 * Architect's Tip callout). Persists across every step of the flow.
 */
import { Eyebrow, KvRow, ArchitectTip } from "./components";
import {
  computeWorkers,
  formatModes,
  hasKafkaModes,
  INGEST_RATE,
  SOURCE_DEFAULTS,
  type Flavor,
  type IndustryOption,
  type Mode,
} from "./types";

interface SummaryCardProps {
  stepId: "flavor" | "source" | "preview" | "validation" | "metrics" | "notebook";
  flavor: Flavor;
  industry: IndustryOption;
  partitions: number;
  enabledModes: Mode[];
  tip: string;
}

export function SummaryCard({
  stepId,
  flavor,
  industry,
  partitions,
  enabledModes,
  tip,
}: SummaryCardProps) {
  const workers = computeWorkers(partitions);
  const kafkaEnabled = hasKafkaModes(enabledModes);
  const zerobusOnly = enabledModes.length === 1 && enabledModes[0] === "zerobus";

  return (
    <div className="glass space-y-4 rounded-lg p-5">
      <div>
        <h3 className="font-display text-sm font-semibold">Configuration Summary</h3>
        <Eyebrow className="mt-0.5 text-[10px]">
          Sensor Rate Topology ({industry.sensors} Units)
        </Eyebrow>
      </div>

      {/* Dot grid — one dot per sensor node */}
      <div className="rounded-lg bg-surface-highest p-3">
        <div className="flex flex-wrap gap-1">
          {Array.from({ length: industry.sensors }).map((_, i) => (
            <div key={i} className="h-3 w-3 rounded-full bg-primary opacity-90" />
          ))}
        </div>
      </div>

      <div className="space-y-2">
        <KvRow
          k="Flavor"
          v={zerobusOnly ? "Serverless (Zerobus)" : flavor === "classic" ? "Classic" : "Serverless"}
        />
        <KvRow k="Industry" v={industry.name} />
        <KvRow k="Update rate" v={INGEST_RATE.label} mono />
        <KvRow k="Modes" v={formatModes(enabledModes)} />
        {stepId === "source" && kafkaEnabled && <KvRow k="Source" v="Kafka (MSK)" />}
        {stepId === "source" && kafkaEnabled && (
          <KvRow k="Partitions" v={String(partitions)} mono />
        )}
        {stepId === "source" && kafkaEnabled && (
          <KvRow k="Workers" v={`${workers} × 4 cores`} mono />
        )}
        {stepId === "source" && kafkaEnabled && (
          <KvRow k="Replication" v={`RF ${SOURCE_DEFAULTS.replicationFactor}`} mono />
        )}
      </div>

      <ArchitectTip>{tip}</ArchitectTip>
    </div>
  );
}
