import { useState } from "react";
import { Loader2, Wrench, Info, ChevronDown, ChevronRight, Database } from "lucide-react";
import { CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Separator } from "@/components/ui/separator";
import { InputField, ProvisionResultList } from "./settings-shared";
import type {
  UcSettings,
  CatalogIsolationMode,
  ProvisionedResource,
  SettingsStatus,
} from "@/lib/types";

const ALL_SCHEMAS = ["raw", "metrics", "checkpoints"] as const;

const SCHEMA_DESCRIPTIONS: Record<string, string> = {
  raw: "Raw ingested sensor readings from streaming sources",
  metrics: "Aggregated metrics and analytics tables",
  checkpoints: "Streaming checkpoint volumes for fault tolerance",
};

interface UcSettingsTabProps {
  settings: UcSettings;
  onUpdate: (patch: Partial<UcSettings>) => void;
  onProvision: () => void;
  provisioning: boolean;
  provisionResult: ProvisionedResource[] | null;
  status?: SettingsStatus["uc"] | null;
  statusLoading?: boolean;
}

export function UcSettingsTab({
  settings,
  onUpdate,
  onProvision,
  provisioning,
  provisionResult,
  status,
  statusLoading,
}: UcSettingsTabProps) {
  const toggleSchema = (schema: string) => {
    const current = settings.schemas;
    const next = current.includes(schema)
      ? current.filter((s) => s !== schema)
      : [...current, schema];
    onUpdate({ schemas: next });
  };

  return (
    <>
      <CardHeader className="p-0">
        <CardTitle className="font-display text-sm font-semibold">Unity Catalog Configuration</CardTitle>
        <p className="text-xs text-muted-foreground">
          Controls how pipeline catalogs, schemas, and checkpoint volumes are named and created.
        </p>
      </CardHeader>
      <Separator />

      {/* Current resources */}
      <CurrentUcResources status={status} loading={statusLoading} />

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Left column: Catalog Configuration + Isolation Mode */}
        <div className="space-y-4">
          {/* Catalog Configuration card */}
          <div className="glass rounded-lg p-4 space-y-4">
            <div>
              <p className="font-display text-xs font-semibold mb-3 uppercase tracking-wider">Catalog Configuration</p>
              <div className="space-y-3">
                <InputField
                  label="Catalog Name Pattern"
                  placeholder="iot_{name}"
                  value={settings.catalog_pattern}
                  onChange={(v) => onUpdate({ catalog_pattern: v })}
                />
                <InputField
                  label="Checkpoint Volume Name"
                  placeholder="stream_checkpoints"
                  value={settings.checkpoint_volume}
                  onChange={(v) => onUpdate({ checkpoint_volume: v })}
                />
                <InputField
                  label="Storage Root"
                  placeholder="s3://bucket/path or abfss://container@account..."
                  value={settings.storage_root}
                  onChange={(v) => onUpdate({ storage_root: v })}
                  description="External-location URL used as the catalog's MANAGED LOCATION. Required on workspaces whose metastore has no default storage root configured. Each pipeline's catalog will be created at <storage_root>/<catalog_name>. The app's service principal needs CREATE_MANAGED_STORAGE on this location."
                />
              </div>
            </div>

            {/* Schemas with descriptions */}
            <div>
              <label className="mb-2 block font-ui text-xs font-medium uppercase tracking-wider text-muted-foreground">
                Schemas
              </label>
              <div className="space-y-2">
                {ALL_SCHEMAS.map((s) => (
                  <label
                    key={s}
                    className="flex items-start gap-2 text-sm cursor-pointer"
                  >
                    <input
                      type="checkbox"
                      checked={settings.schemas.includes(s)}
                      onChange={() => toggleSchema(s)}
                      className="mt-0.5 h-4 w-4 rounded border-input"
                    />
                    <div>
                      <span className="font-medium">{s}</span>
                      <p className="text-xs text-muted-foreground">
                        {SCHEMA_DESCRIPTIONS[s]}
                      </p>
                    </div>
                  </label>
                ))}
              </div>
            </div>
          </div>

          {/* Isolation Mode card */}
          <div className="glass rounded-lg p-4">
            <label className="mb-3 block font-ui text-xs font-medium uppercase tracking-wider text-muted-foreground">
              Isolation Mode
            </label>
            <div className="grid grid-cols-2 gap-3">
              {(["per-pipeline", "shared"] as const).map((mode) => {
                const isSelected = settings.catalog_isolation_mode === mode;
                return (
                  <div
                    key={mode}
                    onClick={() => onUpdate({ catalog_isolation_mode: mode as CatalogIsolationMode })}
                    className={`glass p-4 rounded-lg cursor-pointer border-2 transition-all ${
                      isSelected
                        ? "border-primary glow-primary"
                        : "border-border/50 hover:border-border"
                    }`}
                  >
                    <p className="font-display text-sm font-semibold">
                      {mode === "per-pipeline" ? "Per-Pipeline" : "Shared"}
                    </p>
                    <p className="font-ui text-xs text-muted-foreground mt-1">
                      {mode === "per-pipeline"
                        ? "Each pipeline gets its own catalog."
                        : "All pipelines share a single catalog."}
                    </p>
                  </div>
                );
              })}
            </div>
          </div>
        </div>

        {/* Right column: Provisioning */}
        <div className="glass rounded-lg p-4 space-y-4">
          <p className="font-display text-xs font-semibold uppercase tracking-wider">Provisioning</p>

          {/* Permissions info */}
          <div className="rounded-md border border-border/50 bg-surface-high p-3 text-xs space-y-1">
            <div className="flex items-center gap-1.5 font-medium">
              <Info className="h-3.5 w-3.5" />
              Permissions granted on created catalogs
            </div>
            <ul className="list-disc pl-5 text-muted-foreground space-y-0.5">
              <li><code>USE_CATALOG</code> on the catalog</li>
              <li><code>USE_SCHEMA</code>, <code>SELECT</code>, <code>MODIFY</code>, <code>CREATE_TABLE</code> on each schema</li>
              <li><code>READ_VOLUME</code>, <code>WRITE_VOLUME</code> on the checkpoint volume</li>
            </ul>
          </div>

          {/* Per-tab Create button */}
          <div className="flex items-center gap-3 pt-2">
            <Button
              variant="outline"
              onClick={onProvision}
              disabled={provisioning}
              className="gap-2"
            >
              {provisioning ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <Wrench className="h-4 w-4" />
              )}
              Create Catalog & Schemas
            </Button>
          </div>

          {/* Inline provision results */}
          {provisionResult && (
            <div className="rounded-md border border-border/50 glass p-3">
              <p className="mb-2 font-display text-xs font-semibold">Provisioned Resources</p>
              <ProvisionResultList items={provisionResult} />
            </div>
          )}
        </div>
      </div>
    </>
  );
}

function CurrentUcResources({
  status,
  loading,
}: {
  status?: SettingsStatus["uc"] | null;
  loading?: boolean;
}) {
  const [expanded, setExpanded] = useState(false);

  if (loading) {
    return (
      <div className="flex items-center gap-2 rounded-md border border-border/50 glass px-3 py-2 text-xs text-muted-foreground">
        <Loader2 className="h-3.5 w-3.5 animate-spin" />
        Loading current resources...
      </div>
    );
  }

  if (!status || status.catalogs.length === 0) return null;

  return (
    <div className="rounded-md border border-border/50 glass p-3">
      <button
        onClick={() => setExpanded((p) => !p)}
        className="flex w-full items-center gap-2 text-xs font-ui font-medium"
      >
        <Database className="h-3.5 w-3.5 text-primary" />
        <span className="flex-1 text-left">
          {status.catalogs.length} existing catalog{status.catalogs.length !== 1 ? "s" : ""}
        </span>
        {expanded ? (
          <ChevronDown className="h-3.5 w-3.5 text-muted-foreground" />
        ) : (
          <ChevronRight className="h-3.5 w-3.5 text-muted-foreground" />
        )}
      </button>
      {expanded && (
        <div className="mt-2 space-y-1.5 text-xs">
          {status.catalogs.map((cat) => (
            <div key={cat.name} className="flex items-start gap-2">
              <code className="font-medium text-foreground">{cat.name}</code>
              <span className="text-muted-foreground">
                {cat.schemas.length > 0
                  ? cat.schemas.join(", ")
                  : "no schemas"}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
