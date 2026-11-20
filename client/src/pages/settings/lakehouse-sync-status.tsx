import { useState } from "react";
import { AlertTriangle, CheckCircle2, Loader2, RefreshCw } from "lucide-react";
import { Button } from "@/components/ui/button";
import type { LakebaseSettings } from "@/lib/types";

interface SyncStatusResponse {
  database_compatible: boolean;
  current_database?: string;
  wal2delta_available: boolean;
  wal2delta_installed: boolean;
  wal2delta_version?: string | null;
  lakets_sync_functions_present: boolean;
  ready_to_enable: boolean;
  reason?: string | null;
  error?: string;
}

interface LakehouseSyncStatusProps {
  settings: LakebaseSettings;
}

export function LakehouseSyncStatus({ settings }: LakehouseSyncStatusProps) {
  const [status, setStatus] = useState<SyncStatusResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [fetchError, setFetchError] = useState<string | null>(null);

  async function check(): Promise<void> {
    setLoading(true);
    setFetchError(null);
    try {
      const response = await fetch("/api/lakets/sync/check", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ lakebase: settings }),
      });
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
      const body = (await response.json()) as SyncStatusResponse;
      setStatus(body);
    } catch (err: unknown) {
      setFetchError(err instanceof Error ? err.message : "Unknown error");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="rounded-md border border-border/50 glass p-3 space-y-2">
      <div className="flex items-center justify-between gap-2">
        <div>
          <p className="font-ui text-xs font-medium">Lakehouse Sync (Postgres → Delta)</p>
          <p className="text-xs text-muted-foreground">
            Probe this Lakebase for the prerequisites needed to stream
            ChronoTable partitions into Delta Lake.
          </p>
        </div>
        <Button
          size="sm"
          variant="outline"
          onClick={check}
          disabled={loading}
          className="gap-2 shrink-0"
        >
          {loading ? (
            <Loader2 className="h-3.5 w-3.5 animate-spin" />
          ) : (
            <RefreshCw className="h-3.5 w-3.5" />
          )}
          Check readiness
        </Button>
      </div>

      {fetchError && (
        <div className="flex items-start gap-2 text-xs text-destructive">
          <AlertTriangle className="h-3.5 w-3.5 mt-0.5 shrink-0" />
          <span>Request failed: {fetchError}</span>
        </div>
      )}

      {status && (
        <div
          className={`flex items-start gap-2 rounded border p-2 text-xs ${
            status.ready_to_enable
              ? "border-success/30 bg-success/10 text-success"
              : "border-warning/30 bg-warning/10 text-warning-foreground"
          }`}
        >
          {status.ready_to_enable ? (
            <CheckCircle2 className="h-4 w-4 mt-0.5 shrink-0" />
          ) : (
            <AlertTriangle className="h-4 w-4 mt-0.5 shrink-0" />
          )}
          <div className="space-y-1">
            <p className="font-medium">
              {status.ready_to_enable
                ? "Lakehouse Sync is ready to enable."
                : "Lakehouse Sync not ready."}
            </p>
            {status.reason && <p>{status.reason}</p>}
            {status.error && <p>Error: {status.error}</p>}
            <div className="grid grid-cols-[auto_1fr] gap-x-3 gap-y-0.5 pt-1 text-[10px] font-mono">
              <span>database</span>
              <span>
                {status.current_database ?? "—"}
                {status.database_compatible ? " ✓" : " ✗"}
              </span>
              <span>wal2delta</span>
              <span>
                {status.wal2delta_installed
                  ? `installed ${status.wal2delta_version ?? ""}`
                  : status.wal2delta_available
                  ? "platform-managed (not user-installed)"
                  : "platform-managed"}
              </span>
              <span>lakets.sync</span>
              <span>{status.lakets_sync_functions_present ? "loaded ✓" : "missing ✗"}</span>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
