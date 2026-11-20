import { CardHeader, CardTitle } from "@/components/ui/card";
import { Separator } from "@/components/ui/separator";
import { NumberField, SelectField } from "./settings-shared";
import type { ClusterSettings, NodeType, SparkVersion } from "@/lib/types";

interface ClusterSettingsTabProps {
  settings: ClusterSettings;
  onUpdate: (patch: Partial<ClusterSettings>) => void;
  nodeTypes: NodeType[];
  sparkVersions: SparkVersion[];
  nodeTypesLoading: boolean;
  sparkVersionsLoading: boolean;
}

export function ClusterSettingsTab({
  settings,
  onUpdate,
  nodeTypes,
  sparkVersions,
  nodeTypesLoading,
  sparkVersionsLoading,
}: ClusterSettingsTabProps) {
  return (
    <>
      <CardHeader className="p-0">
        <CardTitle className="font-display text-sm font-semibold">Cluster Defaults</CardTitle>
        <p className="text-xs text-muted-foreground">
          Default compute configuration for pipeline clusters.
        </p>
      </CardHeader>
      <Separator />
      <div className="grid gap-4 sm:grid-cols-2">
        <NumberField
          label="Workers"
          value={settings.workers}
          onChange={(v) => onUpdate({ workers: v })}
          min={0}
          max={100}
        />
        <NumberField
          label="Auto-terminate (minutes)"
          value={settings.auto_terminate_min}
          onChange={(v) => onUpdate({ auto_terminate_min: v })}
          min={10}
          max={1440}
        />
        <SelectField
          label="Worker Node Type"
          value={settings.worker_node_type}
          onChange={(v) => onUpdate({ worker_node_type: v })}
          options={nodeTypes.map((n) => ({
            value: n.node_type_id,
            label: `${n.node_type_id} (${n.num_cores} cores, ${Math.round(n.memory_mb / 1024)}GB)`,
          }))}
          loading={nodeTypesLoading}
        />
        <SelectField
          label="Databricks Runtime"
          value={settings.dbr_version}
          onChange={(v) => onUpdate({ dbr_version: v })}
          options={sparkVersions.map((v) => ({
            value: v.key,
            label: v.name,
          }))}
          loading={sparkVersionsLoading}
        />
      </div>
    </>
  );
}
