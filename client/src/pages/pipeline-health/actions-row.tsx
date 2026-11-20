/**
 * Top actions row for the pipeline-health page: Modify config (links to the
 * setup/validate flow) and Restart jobs.
 *
 * Re-running validation now lives in the dedicated Tier-2 "Run end-to-end test"
 * panel (`e2e-test-panel.tsx`) — the opt-in interactive test — so it is not
 * duplicated here. The destructive "Delete pipeline" action deliberately does
 * NOT live here either: the status/health page is read-and-operate only.
 * Deletion lives on the pipelines list (`/pipelines`) and the config page.
 */
import { useState } from "react";
import { Link } from "react-router-dom";
import { toast } from "sonner";
import { Settings2, RefreshCw, Loader2 } from "lucide-react";

import { Button } from "@/components/ui/button";
import type { PipelineHealth } from "./types";

export function ActionsRow({ health }: { health: PipelineHealth }) {
  const [restarting, setRestarting] = useState(false);

  function handleRestart() {
    setRestarting(true);
    setTimeout(() => {
      setRestarting(false);
      toast.success("Restart requested for all pipeline jobs");
    }, 900);
  }

  return (
    <div className="flex flex-wrap items-center gap-2">
      <Button variant="outline" size="sm" className="gap-1.5" asChild>
        <Link to={`/setup-validate?industry=${health.industry}`}>
          <Settings2 className="h-3.5 w-3.5" />
          Modify config
        </Link>
      </Button>

      <Button
        variant="outline"
        size="sm"
        className="gap-1.5"
        onClick={handleRestart}
        disabled={restarting}
      >
        {restarting ? (
          <Loader2 className="h-3.5 w-3.5 animate-spin" />
        ) : (
          <RefreshCw className="h-3.5 w-3.5" />
        )}
        Restart jobs
      </Button>
    </div>
  );
}
