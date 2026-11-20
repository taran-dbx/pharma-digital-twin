import { useEffect, useMemo, useState } from "react";
import {
  Database,
  Plus,
  Trash2,
  Loader2,
  CheckCircle2,
  XCircle,
  Circle,
  AlertTriangle,
  Info,
  Rocket,
  Layers,
} from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import { cn } from "@/lib/utils";
import { api } from "@/lib/api";
import { validateTiering, intervalToSeconds } from "@/lib/streaming-config";
import {
  DEFAULT_TIERING,
  DEFAULT_ROLLUP,
  ROLLUP_AGGREGATES,
  type TieringConfig,
  type RollupConfig,
  type SetupStepResult,
} from "@/lib/types";

// ---------------------------------------------------------------------------
// Interval helpers — values are stored as "<n> <unit>" Postgres interval
// strings (compatible with intervalToSeconds / validateTiering).
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
  if (rawUnit.startsWith("minute") || rawUnit === "min") return { amount, unit: "minutes" };
  if (rawUnit.startsWith("hour")) return { amount, unit: "hours" };
  if (rawUnit.startsWith("day")) return { amount, unit: "days" };
  return fallback;
}

function formatInterval({ amount, unit }: ParsedInterval): string {
  return `${amount} ${unit}`;
}

const DEFAULT_HOT = parseInterval(DEFAULT_TIERING.hot, { amount: 1, unit: "hours" });
const DEFAULT_DROP = parseInterval(DEFAULT_TIERING.drop, { amount: 7, unit: "days" });
const DEFAULT_CHUNK = parseInterval(DEFAULT_TIERING.chunk, { amount: 15, unit: "minutes" });
const DEFAULT_RETENTION = parseInterval("30 days", { amount: 30, unit: "days" });

// Postgres / DNS-safe project name: lowercase letters, digits, hyphen.
const PROJECT_NAME_RE = /^[a-z][a-z0-9-]{1,61}[a-z0-9]$/;
// Postgres identifier: letters/digits/underscore, must start with letter/underscore.
const IDENTIFIER_RE = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

export interface LaketsSetupProps {
  pipelineId?: string | null;
  industry?: string | null;
  secretScope?: string;
  /** Pre-filled existing project id (e.g. from app defaults / global config). */
  defaultProjectId?: string;
  defaultBranchId?: string;
  defaultDatabase?: string;
  defaultTableName?: string;
  /**
   * Emits the table name + tiering whenever the user edits them so the parent
   * can persist them into ``source_config`` / ``sink_config``. ``tiering`` is
   * ``null`` when tiering is disabled.
   */
  onConfigChange?: (next: { tableName: string; tiering: TieringConfig | null }) => void;
  /** Called once setup succeeds with the resolved project id. */
  onSetupComplete?: (resolvedProjectId: string) => void;
}

export function LaketsSetup({
  pipelineId,
  industry,
  secretScope,
  defaultProjectId,
  defaultBranchId,
  defaultDatabase,
  defaultTableName,
  onConfigChange,
  onSetupComplete,
}: LaketsSetupProps) {
  // Project: create new (default) vs use existing.
  const [createNewProject, setCreateNewProject] = useState(true);
  const [newProjectName, setNewProjectName] = useState("");
  const [existingProjectId, setExistingProjectId] = useState(defaultProjectId ?? "");
  const [branchId, setBranchId] = useState(defaultBranchId || "production");
  const [database, setDatabase] = useState(defaultDatabase || "databricks_postgres");
  const [tableName, setTableName] = useState(defaultTableName || "sensor_readings");

  // Tiering (LakeTS).
  const [tieringEnabled, setTieringEnabled] = useState(true);
  const [hot, setHot] = useState<ParsedInterval>(DEFAULT_HOT);
  const [drop, setDrop] = useState<ParsedInterval>(DEFAULT_DROP);
  const [chunk, setChunk] = useState<ParsedInterval>(DEFAULT_CHUNK);

  // Retention (drop-after) — separate hot-store data retention.
  const [retentionEnabled, setRetentionEnabled] = useState(true);
  const [retention, setRetention] = useState<ParsedInterval>(DEFAULT_RETENTION);

  // Rollups.
  const [rollups, setRollups] = useState<RollupConfig[]>([{ ...DEFAULT_ROLLUP }]);

  // Setup run state.
  const [running, setRunning] = useState(false);
  const [steps, setSteps] = useState<SetupStepResult[] | null>(null);
  const [setupError, setSetupError] = useState<string | null>(null);
  const [resolvedProjectId, setResolvedProjectId] = useState<string | null>(null);

  const tieringDraft: TieringConfig = useMemo(
    () => ({ hot: formatInterval(hot), drop: formatInterval(drop), chunk: formatInterval(chunk) }),
    [hot, drop, chunk],
  );

  const tieringErrors = useMemo(
    () => (tieringEnabled ? validateTiering(tieringDraft) : []),
    [tieringEnabled, tieringDraft],
  );

  const emitConfig = (nextTable: string, nextTiering: TieringConfig | null) => {
    onConfigChange?.({ tableName: nextTable, tiering: nextTiering });
  };

  // Emit the initial table + tiering so the parent captures defaults even if
  // the user never edits the form. Parent must pass a stable onConfigChange.
  useEffect(() => {
    onConfigChange?.({ tableName, tiering: tieringEnabled ? tieringDraft : null });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Validation.
  const projectNameError = useMemo(() => {
    if (!createNewProject) {
      return existingProjectId.trim() ? null : "Select or enter an existing project id.";
    }
    const name = newProjectName.trim();
    if (!name) return "Enter a name for the new project.";
    if (!PROJECT_NAME_RE.test(name)) {
      return "Use lowercase letters, digits and hyphens (3–63 chars, must start with a letter).";
    }
    return null;
  }, [createNewProject, newProjectName, existingProjectId]);

  const tableNameError = useMemo(() => {
    const t = tableName.trim();
    if (!t) return "Table name is required.";
    if (!IDENTIFIER_RE.test(t)) return "Use letters, digits and underscores (must start with a letter).";
    return null;
  }, [tableName]);

  const rollupErrors = useMemo(() => {
    const errs: string[] = [];
    const seen = new Set<string>();
    rollups.forEach((r, i) => {
      const label = r.name.trim() || `rollup ${i + 1}`;
      if (!r.name.trim()) errs.push(`Rollup ${i + 1}: name is required.`);
      else if (!IDENTIFIER_RE.test(r.name.trim())) errs.push(`Rollup "${label}": invalid name.`);
      else if (seen.has(r.name.trim())) errs.push(`Rollup "${label}": duplicate name.`);
      seen.add(r.name.trim());
      if (intervalToSeconds(r.bucket) === null) errs.push(`Rollup "${label}": invalid bucket interval.`);
      if (r.aggregates.length === 0) errs.push(`Rollup "${label}": pick at least one aggregate.`);
    });
    return errs;
  }, [rollups]);

  const canSetup =
    !!pipelineId &&
    !running &&
    !projectNameError &&
    !tableNameError &&
    tieringErrors.length === 0 &&
    rollupErrors.length === 0;

  function updateRollup(index: number, patch: Partial<RollupConfig>) {
    setRollups((prev) => prev.map((r, i) => (i === index ? { ...r, ...patch } : r)));
  }

  function toggleAggregate(index: number, agg: string) {
    setRollups((prev) =>
      prev.map((r, i) => {
        if (i !== index) return r;
        const has = r.aggregates.includes(agg);
        return { ...r, aggregates: has ? r.aggregates.filter((a) => a !== agg) : [...r.aggregates, agg] };
      }),
    );
  }

  function addRollup() {
    setRollups((prev) => [...prev, { name: `rollup_${prev.length + 1}`, bucket: "5 minutes", aggregates: ["avg"] }]);
  }

  function removeRollup(index: number) {
    setRollups((prev) => prev.filter((_, i) => i !== index));
  }

  async function handleSetup() {
    if (!pipelineId) {
      setSetupError("Save the pipeline before running setup.");
      return;
    }
    setRunning(true);
    setSteps(null);
    setSetupError(null);
    setResolvedProjectId(null);
    try {
      const resp = await api.setupPipelineLakebase({
        pipeline_id: pipelineId,
        create_new_project: createNewProject,
        lb_project_id: createNewProject ? newProjectName.trim() : existingProjectId.trim(),
        lb_branch_id: branchId.trim() || "production",
        lb_database: database.trim() || "databricks_postgres",
        table_name: tableName.trim(),
        secret_scope: secretScope,
        industry: industry ?? null,
        tiering: tieringEnabled ? tieringDraft : null,
        retention: retentionEnabled ? formatInterval(retention) : null,
        rollups: rollups.map((r) => ({
          name: r.name.trim(),
          bucket: r.bucket,
          aggregates: r.aggregates,
        })),
      });
      setSteps(resp.steps ?? []);
      if (resp.success) {
        setResolvedProjectId(resp.lb_project_id);
        if (!createNewProject) {
          // keep field in sync; for new projects show the resolved id too.
        }
        setExistingProjectId(resp.lb_project_id);
        onSetupComplete?.(resp.lb_project_id);
      } else {
        setSetupError(resp.error || "Setup failed — see steps below.");
      }
    } catch (err) {
      setSetupError(err instanceof Error ? err.message : String(err));
    } finally {
      setRunning(false);
    }
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Database className="h-4 w-4 text-primary" />
          Setup Lakebase + LakeTS
          <Badge variant="secondary" className="text-[10px]">one-click</Badge>
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-6">
        {/* ── Project ─────────────────────────────────────────────── */}
        <section className="space-y-3">
          <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
            Lakebase Project
          </p>
          <div className="flex gap-2">
            {([
              { id: true, label: "Create new project" },
              { id: false, label: "Use existing" },
            ] as const).map((opt) => (
              <button
                key={String(opt.id)}
                type="button"
                onClick={() => setCreateNewProject(opt.id)}
                className={cn(
                  "rounded-md border px-3 py-1.5 text-xs font-medium transition-colors",
                  createNewProject === opt.id
                    ? "border-primary bg-primary/10 text-primary"
                    : "border-input text-muted-foreground hover:bg-muted",
                )}
              >
                {opt.label}
              </button>
            ))}
          </div>

          {createNewProject ? (
            <LabeledInput
              label="New Project Name"
              placeholder="iot-twin-lakebase"
              value={newProjectName}
              onChange={setNewProjectName}
              error={projectNameError}
            />
          ) : (
            <LabeledInput
              label="Existing Project Id"
              placeholder="lb-proj-xxxxxxxx"
              value={existingProjectId}
              onChange={setExistingProjectId}
              error={projectNameError}
            />
          )}

          <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
            <LabeledInput label="Branch" placeholder="production" value={branchId} onChange={setBranchId} />
            <LabeledInput label="Database" placeholder="databricks_postgres" value={database} onChange={setDatabase} />
            <LabeledInput
              label="Table Name"
              placeholder="sensor_readings"
              value={tableName}
              onChange={(v) => {
                setTableName(v);
                emitConfig(v, tieringEnabled ? tieringDraft : null);
              }}
              error={tableNameError}
            />
          </div>
        </section>

        <Separator />

        {/* ── Tiering ─────────────────────────────────────────────── */}
        <section className="space-y-4">
          <label className="flex cursor-pointer items-center gap-3">
            <input
              type="checkbox"
              checked={tieringEnabled}
              onChange={(e) => {
                setTieringEnabled(e.target.checked);
                emitConfig(tableName, e.target.checked ? tieringDraft : null);
              }}
              className="h-4 w-4 accent-primary"
            />
            <span className="flex items-center gap-2">
              <Layers className="h-4 w-4 text-primary" />
              <span className="text-sm font-semibold">LakeTS tiering</span>
              {tieringEnabled && <Badge variant="secondary" className="text-[10px]">tiered</Badge>}
            </span>
          </label>
          <p className="-mt-1 pl-7 text-xs text-muted-foreground">
            Keep hot partitions in Postgres and tier older chunks to Delta.
          </p>

          {tieringEnabled && (
            <div className="space-y-3 pl-7">
              <IntervalRow
                label="Hot tier"
                value={hot}
                onChange={(v) => { setHot(v); emitConfig(tableName, { ...tieringDraft, hot: formatInterval(v) }); }}
                hint="Data younger than this stays in Postgres."
              />
              <IntervalRow
                label="Drop after"
                value={drop}
                onChange={(v) => { setDrop(v); emitConfig(tableName, { ...tieringDraft, drop: formatInterval(v) }); }}
                hint="Chunks older than this are dropped from the hot store."
              />
              <IntervalRow
                label="Chunk"
                value={chunk}
                onChange={(v) => { setChunk(v); emitConfig(tableName, { ...tieringDraft, chunk: formatInterval(v) }); }}
                hint="ChronoTable partition interval — smaller = finer tiering."
              />
              {tieringErrors.length > 0 && (
                <ErrorList errors={tieringErrors} />
              )}
            </div>
          )}
        </section>

        <Separator />

        {/* ── Retention ───────────────────────────────────────────── */}
        <section className="space-y-3">
          <label className="flex cursor-pointer items-center gap-3">
            <input
              type="checkbox"
              checked={retentionEnabled}
              onChange={(e) => setRetentionEnabled(e.target.checked)}
              className="h-4 w-4 accent-primary"
            />
            <span className="text-sm font-semibold">Retention (drop-after)</span>
          </label>
          {retentionEnabled && (
            <div className="pl-7">
              <IntervalRow
                label="Retain for"
                value={retention}
                onChange={setRetention}
                hint="Rows older than this are pruned from the hot store."
              />
            </div>
          )}
        </section>

        <Separator />

        {/* ── Rollups ─────────────────────────────────────────────── */}
        <section className="space-y-3">
          <div className="flex items-center justify-between">
            <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
              Continuous Rollups
            </p>
            <Button variant="outline" size="sm" className="gap-1.5 text-xs" onClick={addRollup} type="button">
              <Plus className="h-3 w-3" /> Add rollup
            </Button>
          </div>

          {rollups.length === 0 && (
            <p className="text-xs text-muted-foreground">No rollups — raw readings only.</p>
          )}

          <div className="space-y-3">
            {rollups.map((r, i) => {
              const bucketParsed = parseInterval(r.bucket, { amount: 1, unit: "minutes" });
              return (
                <div key={i} className="space-y-2 rounded-md border border-border/50 bg-surface-high p-3">
                  <div className="flex items-end gap-3">
                    <div className="flex-1">
                      <LabeledInput
                        label="Name"
                        placeholder="sensor_1m"
                        value={r.name}
                        onChange={(v) => updateRollup(i, { name: v })}
                      />
                    </div>
                    <div className="w-[170px]">
                      <span className="mb-1.5 block text-xs font-medium uppercase tracking-wider text-muted-foreground">
                        Bucket
                      </span>
                      <div className="flex items-center gap-2">
                        <input
                          type="number"
                          min={1}
                          step={1}
                          value={bucketParsed.amount}
                          onChange={(e) => {
                            const amount = Number.parseFloat(e.target.value);
                            if (Number.isFinite(amount) && amount > 0) {
                              updateRollup(i, { bucket: formatInterval({ ...bucketParsed, amount }) });
                            }
                          }}
                          className="w-16 rounded-md border border-input bg-background px-2 py-1.5 text-sm tabular-nums focus:outline-none focus:ring-1 focus:ring-ring"
                        />
                        <select
                          value={bucketParsed.unit}
                          onChange={(e) =>
                            updateRollup(i, { bucket: formatInterval({ ...bucketParsed, unit: e.target.value as IntervalUnit }) })
                          }
                          className="rounded-md border border-input bg-background px-2 py-1.5 text-sm focus:outline-none focus:ring-1 focus:ring-ring"
                        >
                          {UNIT_OPTIONS.map((o) => (
                            <option key={o.value} value={o.value}>{o.label}</option>
                          ))}
                        </select>
                      </div>
                    </div>
                    <Button
                      variant="ghost"
                      size="sm"
                      type="button"
                      className="mb-0.5 h-9 w-9 p-0 text-muted-foreground hover:text-destructive"
                      onClick={() => removeRollup(i)}
                      aria-label="Remove rollup"
                    >
                      <Trash2 className="h-4 w-4" />
                    </Button>
                  </div>
                  <div className="flex flex-wrap gap-2">
                    {ROLLUP_AGGREGATES.map((agg) => {
                      const checked = r.aggregates.includes(agg);
                      return (
                        <label
                          key={agg}
                          className={cn(
                            "flex cursor-pointer items-center gap-1.5 rounded-md border px-2.5 py-1 text-xs transition-colors",
                            checked
                              ? "border-primary/60 bg-primary/10 text-foreground"
                              : "border-border/50 bg-surface-highest text-muted-foreground hover:bg-surface-high",
                          )}
                        >
                          <input
                            type="checkbox"
                            checked={checked}
                            onChange={() => toggleAggregate(i, agg)}
                            className="h-3 w-3 accent-primary"
                          />
                          {agg}
                        </label>
                      );
                    })}
                  </div>
                </div>
              );
            })}
          </div>

          {rollupErrors.length > 0 && <ErrorList errors={rollupErrors} />}
        </section>

        <Separator />

        {/* ── enable_sync info ────────────────────────────────────── */}
        <div className="flex items-start gap-2 rounded-md border border-blue-500/20 bg-blue-500/5 px-3 py-2">
          <Info className="mt-0.5 h-3.5 w-3.5 shrink-0 text-blue-600" />
          <p className="text-[11px] text-muted-foreground">
            Change Data Feed (CDF) preparation for Lakehouse Sync runs automatically as part of setup —
            no separate step needed.
          </p>
        </div>

        {/* ── Setup button ────────────────────────────────────────── */}
        <div className="space-y-3">
          <Button className="w-full gap-2" onClick={handleSetup} disabled={!canSetup}>
            {running ? <Loader2 className="h-4 w-4 animate-spin" /> : <Rocket className="h-4 w-4" />}
            {running ? "Setting up Lakebase + LakeTS…" : "Setup everything"}
          </Button>
          {!pipelineId && (
            <p className="text-[11px] text-muted-foreground">Save the pipeline first to enable setup.</p>
          )}

          {steps && steps.length > 0 && (
            <div className="space-y-1.5 rounded-md border border-border/50 bg-surface-high p-3">
              {steps.map((s, i) => {
                // The backend uses a richer status vocabulary than ok/error:
                // create_rollup returns "created"/"already_exists", failures
                // are "failed". Treat all non-failure terminal states as success
                // so a healthy run renders green rather than stuck/erroring.
                const isFailure = s.status === "error" || s.status === "failed";
                const isSuccess =
                  s.status === "ok" ||
                  s.status === "created" ||
                  s.status === "already_exists";
                return (
                <div key={i} className="flex items-start gap-2 text-xs">
                  {isSuccess ? (
                    <CheckCircle2 className="mt-0.5 h-3.5 w-3.5 shrink-0 text-green-600" />
                  ) : isFailure ? (
                    <XCircle className="mt-0.5 h-3.5 w-3.5 shrink-0 text-destructive" />
                  ) : (
                    <Circle className="mt-0.5 h-3.5 w-3.5 shrink-0 text-muted-foreground/40" />
                  )}
                  <div>
                    <span className="font-medium">{s.step}</span>
                    {s.error && <span className="ml-1 text-destructive">— {s.error}</span>}
                  </div>
                </div>
                );
              })}
            </div>
          )}

          {resolvedProjectId && (
            <div className="flex items-center gap-2 rounded-md border border-green-500/20 bg-green-500/5 px-3 py-2 text-xs text-green-700">
              <CheckCircle2 className="h-3.5 w-3.5 shrink-0" />
              Lakebase ready — project <code className="rounded bg-green-500/10 px-1">{resolvedProjectId}</code>
            </div>
          )}

          {setupError && (
            <div className="flex items-start gap-2 rounded-md border border-destructive/30 bg-destructive/5 px-3 py-2 text-xs text-destructive">
              <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
              {setupError}
            </div>
          )}
        </div>
      </CardContent>
    </Card>
  );
}

// ---------------------------------------------------------------------------
// Small building blocks
// ---------------------------------------------------------------------------

interface LabeledInputProps {
  label: string;
  placeholder: string;
  value: string;
  onChange: (v: string) => void;
  error?: string | null;
}

function LabeledInput({ label, placeholder, value, onChange, error }: LabeledInputProps) {
  return (
    <div>
      <label className="mb-1.5 block text-xs font-medium uppercase tracking-wider text-muted-foreground">
        {label}
      </label>
      <input
        type="text"
        value={value}
        placeholder={placeholder}
        onChange={(e) => onChange(e.target.value)}
        className={cn(
          "w-full rounded-md border bg-background px-3 py-2 text-sm placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring",
          error ? "border-destructive/60" : "border-input",
        )}
      />
      {error && <p className="mt-1 text-[11px] text-destructive">{error}</p>}
    </div>
  );
}

interface IntervalRowProps {
  label: string;
  value: ParsedInterval;
  onChange: (next: ParsedInterval) => void;
  hint?: string;
}

function IntervalRow({ label, value, onChange, hint }: IntervalRowProps) {
  return (
    <div className="grid grid-cols-[110px_1fr] items-center gap-3">
      <span className="text-xs font-medium uppercase tracking-wider text-muted-foreground">{label}</span>
      <div className="flex items-center gap-2">
        <input
          type="number"
          min={1}
          step={1}
          value={value.amount}
          onChange={(e) => {
            const parsed = Number.parseFloat(e.target.value);
            if (Number.isFinite(parsed) && parsed > 0) onChange({ ...value, amount: parsed });
          }}
          className="w-20 rounded-md border border-input bg-background px-2 py-1.5 text-sm tabular-nums focus:outline-none focus:ring-1 focus:ring-ring"
        />
        <select
          value={value.unit}
          onChange={(e) => onChange({ ...value, unit: e.target.value as IntervalUnit })}
          className="rounded-md border border-input bg-background px-2 py-1.5 text-sm focus:outline-none focus:ring-1 focus:ring-ring"
        >
          {UNIT_OPTIONS.map((o) => (
            <option key={o.value} value={o.value}>{o.label}</option>
          ))}
        </select>
        {hint && <span className="ml-1 text-[11px] text-muted-foreground">{hint}</span>}
      </div>
    </div>
  );
}

function ErrorList({ errors }: { errors: string[] }) {
  return (
    <div className="flex items-start gap-2 rounded-md border border-destructive/40 bg-destructive/10 p-3">
      <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-destructive" />
      <ul className="space-y-1 text-xs text-destructive">
        {errors.map((err, i) => (
          <li key={i}>{err}</li>
        ))}
      </ul>
    </div>
  );
}
