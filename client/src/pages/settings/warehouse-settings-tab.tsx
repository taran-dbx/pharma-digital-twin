import { useState } from "react";
import { Loader2, Plus, Zap } from "lucide-react";
import { CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Separator } from "@/components/ui/separator";
import { InputField, NumberField, SelectField } from "./settings-shared";
import { cn } from "@/lib/utils";
import { api } from "@/lib/api";
import { toast } from "sonner";
import type { WarehouseSettings, WarehouseInfo } from "@/lib/types";

const WAREHOUSE_SIZES = [
  { value: "2X-Small", label: "2X-Small" },
  { value: "X-Small", label: "X-Small" },
  { value: "Small", label: "Small" },
  { value: "Medium", label: "Medium" },
  { value: "Large", label: "Large" },
];

const STATE_STYLES: Record<string, string> = {
  RUNNING: "bg-green-500/10 text-green-600 border-green-500/20",
  STARTING: "bg-yellow-500/10 text-yellow-600 border-yellow-500/20",
  STOPPED: "bg-gray-500/10 text-gray-500 border-gray-500/20",
  STOPPING: "bg-yellow-500/10 text-yellow-600 border-yellow-500/20",
  DELETED: "bg-red-500/10 text-red-500 border-red-500/20",
  DELETING: "bg-red-500/10 text-red-500 border-red-500/20",
};

interface WarehouseSettingsTabProps {
  settings: WarehouseSettings;
  onUpdate: (patch: Partial<WarehouseSettings>) => void;
  warehouses: WarehouseInfo[];
  warehousesLoading: boolean;
  onWarehouseCreated: (wh: WarehouseInfo) => void;
}

export function WarehouseSettingsTab({
  settings,
  onUpdate,
  warehouses,
  warehousesLoading,
  onWarehouseCreated,
}: WarehouseSettingsTabProps) {
  const [creating, setCreating] = useState(false);
  const [newName, setNewName] = useState("iot-digital-twin");
  const [newSize, setNewSize] = useState("2X-Small");
  const [newAutoStop, setNewAutoStop] = useState(10);

  const handleCreate = async () => {
    setCreating(true);
    try {
      const result = await api.createWarehouse({
        name: newName,
        cluster_size: newSize,
        auto_stop_mins: newAutoStop,
      });
      onWarehouseCreated(result);
      onUpdate({ warehouse_id: result.id, warehouse_name: result.name });
      toast.success(`Warehouse "${result.name}" created`);
    } catch (err) {
      toast.error(
        `Failed to create warehouse: ${err instanceof Error ? err.message : err}`,
      );
    } finally {
      setCreating(false);
    }
  };

  return (
    <>
      <CardHeader className="p-0">
        <CardTitle className="font-display text-sm font-semibold">SQL Warehouse</CardTitle>
        <p className="text-xs text-muted-foreground">
          Select or create a Databricks SQL warehouse for running queries
          against pipeline data from the dashboard.
        </p>
      </CardHeader>
      <Separator />

      {/* Warehouse list */}
      {warehousesLoading ? (
        <div className="flex items-center gap-2 text-sm text-muted-foreground">
          <Loader2 className="h-4 w-4 animate-spin" />
          Loading warehouses...
        </div>
      ) : warehouses.length === 0 ? (
        <p className="text-sm text-muted-foreground">
          No SQL warehouses found in this workspace. Create one below.
        </p>
      ) : (
        <div className="space-y-2">
          <label className="block font-ui text-xs font-medium uppercase tracking-wider text-muted-foreground">
            Available Warehouses
          </label>
          <div className="space-y-1.5">
            {warehouses.map((wh) => (
              <label
                key={wh.id}
                className={cn(
                  "flex items-center gap-3 rounded-md border p-3 cursor-pointer transition-colors",
                  settings.warehouse_id === wh.id
                    ? "border-primary bg-primary/5"
                    : "border-border/50 hover:bg-surface-high",
                )}
              >
                <input
                  type="radio"
                  name="warehouse"
                  checked={settings.warehouse_id === wh.id}
                  onChange={() =>
                    onUpdate({
                      warehouse_id: wh.id,
                      warehouse_name: wh.name,
                    })
                  }
                  className="h-4 w-4"
                />
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2">
                    <span className="text-sm font-medium truncate">
                      {wh.name}
                    </span>
                    <span
                      className={cn(
                        "rounded-full border px-2 py-0.5 text-[10px] font-medium",
                        STATE_STYLES[wh.state] ?? STATE_STYLES.STOPPED,
                      )}
                    >
                      {wh.state}
                    </span>
                    {wh.serverless && (
                      <span className="flex items-center gap-0.5 rounded-full border border-purple-500/20 bg-purple-500/10 px-2 py-0.5 text-[10px] font-medium text-purple-600">
                        <Zap className="h-2.5 w-2.5" />
                        Serverless
                      </span>
                    )}
                  </div>
                  <p className="text-xs text-muted-foreground">
                    Size: {wh.cluster_size} · Auto-stop: {wh.auto_stop_mins}min
                  </p>
                </div>
              </label>
            ))}
          </div>
        </div>
      )}

      {/* Create new warehouse */}
      <div className="border-t border-border/50 pt-4 space-y-4">
        <div>
          <p className="font-display text-xs font-semibold text-muted-foreground">
            Create New Warehouse
          </p>
          <p className="text-xs text-muted-foreground">
            Creates a serverless PRO warehouse optimized for low-latency
            queries.
          </p>
        </div>
        <div className="grid gap-4 sm:grid-cols-3">
          <InputField
            label="Name"
            placeholder="iot-digital-twin"
            value={newName}
            onChange={setNewName}
          />
          <SelectField
            label="Size"
            value={newSize}
            onChange={setNewSize}
            options={WAREHOUSE_SIZES}
          />
          <NumberField
            label="Auto-stop (min)"
            value={newAutoStop}
            onChange={setNewAutoStop}
            min={0}
            max={120}
          />
        </div>
        <Button
          variant="outline"
          onClick={handleCreate}
          disabled={creating || !newName}
          className="gap-2"
        >
          {creating ? (
            <Loader2 className="h-4 w-4 animate-spin" />
          ) : (
            <Plus className="h-4 w-4" />
          )}
          Create Warehouse
        </Button>
      </div>
    </>
  );
}
