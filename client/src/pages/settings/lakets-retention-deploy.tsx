import { useState } from "react";
import { AlertTriangle, CheckCircle2, Clock, ExternalLink, Loader2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import type { LakebaseSettings } from "@/lib/types";

interface DeployResponse {
  job_id?: number;
  job_name?: string;
  notebook_path?: string;
  schedule?: { cron: string; timezone: string };
  action?: "created" | "updated";
  url?: string | null;
  error?: string;
}

interface LakeTsRetentionDeployProps {
  settings: LakebaseSettings;
}

export function LakeTsRetentionDeploy({ settings }: LakeTsRetentionDeployProps) {
  const [result, setResult] = useState<DeployResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [fetchError, setFetchError] = useState<string | null>(null);

  async function deploy(): Promise<void> {
    setLoading(true);
    setFetchError(null);
    try {
      const response = await fetch("/api/lakets/retention/deploy", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ lakebase: settings }),
      });
      const body = (await response.json()) as DeployResponse;
      if (!response.ok) {
        throw new Error(body.error || `HTTP ${response.status}`);
      }
      setResult(body);
    } catch (err: unknown) {
      setFetchError(err instanceof Error ? err.message : "Unknown error");
    } finally {
      setLoading(false);
    }
  }

  const disabled = loading || settings.mode === "autoscale" && !settings.project_id;

  return (
    <div className="rounded-md border border-border/50 glass p-3 space-y-2">
      <div className="flex items-center justify-between gap-2">
        <div>
          <div className="flex items-center gap-2">
            <Clock className="h-3.5 w-3.5 text-primary" />
            <p className="font-ui text-xs font-medium">Scheduled Retention</p>
          </div>
          <p className="text-xs text-muted-foreground">
            Daily job at 03:00 UTC — drops Postgres chunks older than the
            configured <code>drop</code> interval for every ChronoTable with a
            retention policy.
          </p>
        </div>
        <Button
          size="sm"
          variant="outline"
          onClick={deploy}
          disabled={disabled}
          className="gap-2 shrink-0"
          title={disabled ? "Set a project_id in autoscale mode first" : undefined}
        >
          {loading ? (
            <Loader2 className="h-3.5 w-3.5 animate-spin" />
          ) : (
            <Clock className="h-3.5 w-3.5" />
          )}
          Deploy retention job
        </Button>
      </div>

      {fetchError && (
        <div className="flex items-start gap-2 text-xs text-destructive">
          <AlertTriangle className="h-3.5 w-3.5 mt-0.5 shrink-0" />
          <span>{fetchError}</span>
        </div>
      )}

      {result && result.job_id && (
        <div className="flex items-start gap-2 rounded border border-success/30 bg-success/10 p-2 text-xs text-success">
          <CheckCircle2 className="h-4 w-4 mt-0.5 shrink-0" />
          <div className="space-y-0.5">
            <p className="font-medium">
              Job {result.action} — #{result.job_id}
            </p>
            <p className="font-mono text-[10px]">
              {result.schedule?.cron} ({result.schedule?.timezone})
            </p>
            {result.url && (
              <a
                href={result.url}
                target="_blank"
                rel="noreferrer"
                className="inline-flex items-center gap-1 underline"
              >
                Open in Databricks <ExternalLink className="h-3 w-3" />
              </a>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
