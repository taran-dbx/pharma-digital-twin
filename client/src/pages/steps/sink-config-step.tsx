import { useEffect, useMemo, useState } from "react";
import {
  AlertTriangle,
  Database,
  Info,
  RadioTower,
  Layers,
} from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import { validateTiering } from "@/lib/streaming-config";
import { DEFAULT_TIERING } from "@/lib/types";
import type { TieringConfig } from "@/lib/types";

// ---------------------------------------------------------------------------
// Interval units — stored as "<n> <unit>" strings (Postgres interval compatible
// and accepted by `intervalToSeconds`).
// ---------------------------------------------------------------------------

type IntervalUnit = "minutes" | "hours" | "days";

const UNIT_OPTIONS: Array<{ value: IntervalUnit; label: string }> = [
  { value: "minutes", label: "Minutes" },
  { value: "hours", label: "Hours" },
  { value: "days", label: "Days" },
];

interface ParsedInterval {
  amount: number;
  unit: IntervalUnit;
}

function parseInterval(raw: string | undefined, fallback: ParsedInterval): ParsedInterval {
  if (!raw) return fallback;
  const parts = raw.trim().split(/\s+/);
  if (parts.length !== 2) return fallback;
  const amount = Number.parseFloat(parts[0]);
  if (!Number.isFinite(amount) || amount <= 0) return fallback;
  const rawUnit = parts[1].toLowerCase();
  let unit: IntervalUnit | null = null;
  if (rawUnit === "minute" || rawUnit === "minutes" || rawUnit === "min") {
    unit = "minutes";
  } else if (rawUnit === "hour" || rawUnit === "hours") {
    unit = "hours";
  } else if (rawUnit === "day" || rawUnit === "days") {
    unit = "days";
  }
  if (!unit) return fallback;
  return { amount, unit };
}

function formatInterval({ amount, unit }: ParsedInterval): string {
  return `${amount} ${unit}`;
}

// Default parsed values mirror DEFAULT_TIERING.
const DEFAULT_HOT: ParsedInterval = parseInterval(DEFAULT_TIERING.hot, { amount: 1, unit: "hours" });
const DEFAULT_DROP: ParsedInterval = parseInterval(DEFAULT_TIERING.drop, { amount: 7, unit: "days" });
const DEFAULT_CHUNK: ParsedInterval = parseInterval(DEFAULT_TIERING.chunk, { amount: 15, unit: "minutes" });

// ---------------------------------------------------------------------------
// SSE metric naming — mirrors dashboard filter logic which matches against
// ``sensor_type`` or ``sensor_name``.  The wizard offers one toggle per unique
// sensor_type pulled from the pipeline's sensor_config.
// ---------------------------------------------------------------------------

function uniqueSensorTypes(sensorConfigs: Record<string, unknown>[]): string[] {
  const seen = new Set<string>();
  const result: string[] = [];
  for (const cfg of sensorConfigs) {
    const t = cfg.sensor_type;
    if (typeof t === "string" && !seen.has(t)) {
      seen.add(t);
      result.push(t);
    }
  }
  return result;
}

// ---------------------------------------------------------------------------
// Props
// ---------------------------------------------------------------------------

export interface SinkConfigStepProps {
  sensorConfigs: Record<string, unknown>[];
  /** Current tiering value (undefined → Lakebase tiering disabled). */
  tiering: TieringConfig | undefined;
  onTieringChange: (next: TieringConfig | undefined) => void;
  /** Current SSE metric list (empty → SSE disabled). */
  sseMetrics: string[];
  onSseMetricsChange: (next: string[]) => void;
  /** Exposes validity so the wizard can gate "Next". */
  onValidityChange?: (valid: boolean) => void;
}

export function SinkConfigStep({
  sensorConfigs,
  tiering,
  onTieringChange,
  sseMetrics,
  onSseMetricsChange,
  onValidityChange,
}: SinkConfigStepProps) {
  // Local editor state — always present so the user can toggle Lakebase back on
  // without losing their previous values.  We only *emit* tiering upward when
  // the Lakebase checkbox is enabled.
  const [lakebaseEnabled, setLakebaseEnabled] = useState<boolean>(!!tiering);
  const [hotParsed, setHotParsed] = useState<ParsedInterval>(
    parseInterval(tiering?.hot, DEFAULT_HOT),
  );
  const [dropParsed, setDropParsed] = useState<ParsedInterval>(
    parseInterval(tiering?.drop, DEFAULT_DROP),
  );
  const [chunkParsed, setChunkParsed] = useState<ParsedInterval>(
    parseInterval(tiering?.chunk, DEFAULT_CHUNK),
  );

  const [sseEnabled, setSseEnabled] = useState<boolean>(sseMetrics.length > 0);

  // Derived tiering config (for validation + upstream payload).
  const tieringDraft: TieringConfig = useMemo(
    () => ({
      hot: formatInterval(hotParsed),
      drop: formatInterval(dropParsed),
      chunk: formatInterval(chunkParsed),
    }),
    [hotParsed, dropParsed, chunkParsed],
  );

  const tieringErrors = useMemo(
    () => (lakebaseEnabled ? validateTiering(tieringDraft) : []),
    [lakebaseEnabled, tieringDraft],
  );

  const sensorTypes = useMemo(() => uniqueSensorTypes(sensorConfigs), [sensorConfigs]);

  // Propagate tiering upward whenever draft or toggle changes.
  useEffect(() => {
    if (lakebaseEnabled) {
      onTieringChange(tieringDraft);
    } else {
      onTieringChange(undefined);
    }
    // We intentionally omit onTieringChange from deps — parent should pass a stable callback.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [lakebaseEnabled, tieringDraft]);

  // Ensure SSE metrics list mirrors the sseEnabled toggle.
  useEffect(() => {
    if (!sseEnabled && sseMetrics.length > 0) {
      onSseMetricsChange([]);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sseEnabled]);

  // Report validity.  Lakebase must have zero errors when enabled; SSE is
  // always permissible (an empty list simply means "no realtime cards").
  useEffect(() => {
    if (!onValidityChange) return;
    const valid = !lakebaseEnabled || tieringErrors.length === 0;
    onValidityChange(valid);
  }, [lakebaseEnabled, tieringErrors.length, onValidityChange]);

  function toggleSseMetric(metric: string): void {
    if (sseMetrics.includes(metric)) {
      onSseMetricsChange(sseMetrics.filter((m) => m !== metric));
    } else {
      onSseMetricsChange([...sseMetrics, metric]);
    }
  }

  return (
    <Card className="glass border-border/50">
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Layers className="h-5 w-5 text-primary" />
          Sink configuration
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-6">
        {/* ---------------- Lakebase (LakeTS) ---------------- */}
        <section className="space-y-4 rounded-lg border border-border/50 bg-surface-high p-4">
          <label className="flex cursor-pointer items-start gap-3">
            <input
              type="checkbox"
              checked={lakebaseEnabled}
              onChange={(e) => setLakebaseEnabled(e.target.checked)}
              className="mt-1 h-4 w-4 accent-primary"
            />
            <div className="flex-1">
              <div className="flex items-center gap-2">
                <Database className="h-4 w-4 text-primary" />
                <span className="font-ui text-sm font-semibold">
                  Lakebase (LakeTS)
                </span>
                {lakebaseEnabled && (
                  <Badge variant="secondary" className="text-[10px]">
                    tiered
                  </Badge>
                )}
              </div>
              <p className="mt-0.5 text-xs text-muted-foreground">
                Store hot partitions in Postgres and tier older chunks to Delta.
              </p>
            </div>
          </label>

          {lakebaseEnabled && (
            <div className="space-y-3 pl-7">
              <IntervalRow
                label="Hot tier"
                value={hotParsed}
                onChange={setHotParsed}
                hint="Data younger than this stays in Postgres."
              />
              <IntervalRow
                label="Drop after"
                value={dropParsed}
                onChange={setDropParsed}
                hint="Chunks older than this are dropped from the hot store."
              />
              <IntervalRow
                label="Chunk"
                value={chunkParsed}
                onChange={setChunkParsed}
                hint="ChronoTable partition interval — smaller = finer tiering."
              />

              {tieringErrors.length > 0 && (
                <div className="flex items-start gap-2 rounded-md border border-destructive/40 bg-destructive/10 p-3">
                  <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-destructive" />
                  <ul className="space-y-1 text-xs text-destructive">
                    {tieringErrors.map((err, i) => (
                      <li key={i}>{err}</li>
                    ))}
                  </ul>
                </div>
              )}
            </div>
          )}
        </section>

        {/* ---------------- SSE metrics ---------------- */}
        <section className="space-y-4 rounded-lg border border-border/50 bg-surface-high p-4">
          <label
            className={cn(
              "flex items-start gap-3",
              sensorTypes.length === 0 ? "cursor-not-allowed opacity-70" : "cursor-pointer",
            )}
          >
            <input
              type="checkbox"
              checked={sseEnabled}
              disabled={sensorTypes.length === 0}
              onChange={(e) => setSseEnabled(e.target.checked)}
              className="mt-1 h-4 w-4 accent-primary"
            />
            <div className="flex-1">
              <div className="flex items-center gap-2">
                <RadioTower className="h-4 w-4 text-primary" />
                <span className="font-ui text-sm font-semibold">
                  SSE (live KPIs)
                </span>
                {sseEnabled && sseMetrics.length > 0 && (
                  <Badge variant="secondary" className="text-[10px]">
                    {sseMetrics.length} metric{sseMetrics.length === 1 ? "" : "s"}
                  </Badge>
                )}
              </div>
              <p className="mt-0.5 text-xs text-muted-foreground">
                Stream selected aggregate metrics to realtime dashboard cards.
              </p>
            </div>
          </label>

          {sensorTypes.length === 0 ? (
            <div className="flex items-start gap-2 rounded-md border border-border/50 bg-surface-highest p-3 text-xs text-muted-foreground">
              <Info className="mt-0.5 h-3.5 w-3.5 shrink-0" />
              <span>
                No sensors configured yet — finish the Data Hub step to choose
                SSE metrics.
              </span>
            </div>
          ) : sseEnabled ? (
            <div className="grid grid-cols-2 gap-2 pl-7 sm:grid-cols-3">
              {sensorTypes.map((metric) => {
                const checked = sseMetrics.includes(metric);
                return (
                  <label
                    key={metric}
                    className={cn(
                      "flex cursor-pointer items-center gap-2 rounded-md border px-2.5 py-1.5 text-xs transition-colors",
                      checked
                        ? "border-primary/60 bg-primary/10 text-foreground"
                        : "border-border/50 bg-surface-highest text-muted-foreground hover:bg-surface-high",
                    )}
                  >
                    <input
                      type="checkbox"
                      checked={checked}
                      onChange={() => toggleSseMetric(metric)}
                      className="h-3.5 w-3.5 accent-primary"
                    />
                    <span className="capitalize">{metric.replace(/_/g, " ")}</span>
                  </label>
                );
              })}
            </div>
          ) : null}
        </section>
      </CardContent>
    </Card>
  );
}

// ---------------------------------------------------------------------------
// Interval row — number input + unit dropdown.
// ---------------------------------------------------------------------------

interface IntervalRowProps {
  label: string;
  value: ParsedInterval;
  onChange: (next: ParsedInterval) => void;
  hint?: string;
}

function IntervalRow({ label, value, onChange, hint }: IntervalRowProps) {
  return (
    <div className="grid grid-cols-[120px_1fr] items-center gap-3">
      <span className="text-xs font-ui font-medium uppercase tracking-wider text-muted-foreground">
        {label}
      </span>
      <div className="flex items-center gap-2">
        <input
          type="number"
          min={1}
          step={1}
          value={value.amount}
          onChange={(e) => {
            const parsed = Number.parseFloat(e.target.value);
            if (Number.isFinite(parsed) && parsed > 0) {
              onChange({ ...value, amount: parsed });
            }
          }}
          className="w-24 rounded-md border border-border/50 bg-input px-2 py-1 text-sm tabular-nums focus:outline-none focus:ring-1 focus:ring-primary"
        />
        <select
          value={value.unit}
          onChange={(e) => onChange({ ...value, unit: e.target.value as IntervalUnit })}
          className="rounded-md border border-border/50 bg-input px-2 py-1 text-sm focus:outline-none focus:ring-1 focus:ring-primary"
        >
          {UNIT_OPTIONS.map((o) => (
            <option key={o.value} value={o.value}>
              {o.label}
            </option>
          ))}
        </select>
        {hint && (
          <span className="ml-1 text-[11px] text-muted-foreground">{hint}</span>
        )}
      </div>
    </div>
  );
}
