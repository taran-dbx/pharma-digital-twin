import { useState } from "react";
import { Link } from "react-router-dom";
import { toast } from "sonner";
import {
  Plus,
  Waves,
  ArrowRight,
  Activity,
  Database,
  Shield,
  Loader2,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { PipelineCard } from "@/components/pipeline-card";
import { ConfirmDialog } from "@/components/confirm-dialog";
import { usePipelines } from "@/contexts/pipeline-context";
import { useUser } from "@/contexts/user-context";

const features = [
  {
    icon: Activity,
    title: "Real-Time Monitoring",
    description: "Stream sensor data with sub-50ms latency across multiple pipelines.",
  },
  {
    icon: Database,
    title: "16 Industries",
    description: "200+ sensor types ready to configure, or bring your own JSON.",
  },
  {
    icon: Shield,
    title: "Production-Ready",
    description: "Databricks Apps, Lakebase PostgreSQL, and Unity Catalog integration.",
  },
];

export function HomePage() {
  const { email } = useUser();
  const { pipelines, loading, deletePipeline } = usePipelines();

  const [deleteTarget, setDeleteTarget] = useState<string | null>(null);
  const targetPipeline = pipelines.find((p) => p.id === deleteTarget);

  async function handleDelete() {
    if (!deleteTarget) return;
    try {
      await deletePipeline(deleteTarget);
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Failed to delete pipeline");
    }
    setDeleteTarget(null);
  }

  return (
    <div className="space-y-12 pb-16">
      {/* Hero */}
      <section className="relative overflow-hidden">
        <div className="absolute inset-0 bg-[radial-gradient(ellipse_at_top,_var(--tw-gradient-stops))] from-primary/10 via-transparent to-transparent" />
        <div className="relative mx-auto max-w-4xl px-6 py-12">
          <div className="flex flex-col gap-8 sm:flex-row sm:items-center">
            {/* Left: text + badges */}
            <div className="flex-[3] min-w-0">
              <div className="mb-4 inline-flex items-center gap-2 rounded-full border border-border bg-card px-4 py-1.5 text-xs font-medium text-muted-foreground">
                <Waves className="h-3 w-3 text-primary" />
                Powered by Databricks
              </div>
              <h1 className="mb-3 font-display text-3xl font-bold tracking-tight sm:text-4xl text-gradient-hero">
                IoT Digital Twin
              </h1>
              <p className="mb-6 max-w-md text-sm text-muted-foreground font-sans">
                Create and manage real-time IoT streaming pipelines — from sensor
                configuration to production deployment.
              </p>
              <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
                {features.map((f) => (
                  <div
                    key={f.title}
                    className="glass rounded-lg px-3 py-3 hover:border-primary/30 hover:shadow-[0_0_15px_rgba(255,54,33,0.1)] transition-all duration-300"
                  >
                    <f.icon className="mb-1.5 h-4 w-4 text-primary" />
                    <p className="font-ui text-xs font-semibold leading-snug">{f.title}</p>
                    <p className="mt-0.5 text-[11px] text-muted-foreground leading-snug">{f.description}</p>
                  </div>
                ))}
              </div>
            </div>

            {/* Right: decorative throughput chart */}
            <div className="flex-[2] min-w-0">
              <div className="bg-surface-low rounded-lg p-4 border border-border/50 shadow-[0_0_30px_rgba(255,54,33,0.06)]">
                <div className="mb-2 flex items-center justify-between">
                  <span className="hud-label text-muted-foreground">Throughput</span>
                  <span className="hud-label text-success">LIVE</span>
                </div>
                <svg
                  viewBox="0 0 240 80"
                  xmlns="http://www.w3.org/2000/svg"
                  className="w-full"
                  aria-hidden="true"
                >
                  <defs>
                    <linearGradient id="areaFill" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor="hsl(var(--primary))" stopOpacity="0.25" />
                      <stop offset="100%" stopColor="hsl(var(--primary))" stopOpacity="0.02" />
                    </linearGradient>
                  </defs>
                  {/* Area fill */}
                  <path
                    d="M0,70 C20,65 30,58 50,50 C70,42 75,55 95,40 C115,25 125,35 145,22 C165,9 175,18 195,12 C210,7 225,10 240,6 L240,80 L0,80 Z"
                    fill="url(#areaFill)"
                  />
                  {/* Stroke line */}
                  <path
                    d="M0,70 C20,65 30,58 50,50 C70,42 75,55 95,40 C115,25 125,35 145,22 C165,9 175,18 195,12 C210,7 225,10 240,6"
                    fill="none"
                    stroke="hsl(var(--success))"
                    strokeWidth="1.5"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    className="drop-shadow-[0_0_4px_hsl(var(--success)/0.6)]"
                  />
                  {/* Endpoint dot */}
                  <circle cx="240" cy="6" r="2.5" fill="hsl(var(--success))" className="drop-shadow-[0_0_6px_hsl(var(--success)/0.8)]" />
                  {/* X-axis time markers */}
                  <text x="0" y="79" fontSize="7" fill="hsl(var(--muted-foreground))" fontFamily="monospace">-5m</text>
                  <text x="55" y="79" fontSize="7" fill="hsl(var(--muted-foreground))" fontFamily="monospace">-4m</text>
                  <text x="110" y="79" fontSize="7" fill="hsl(var(--muted-foreground))" fontFamily="monospace">-3m</text>
                  <text x="165" y="79" fontSize="7" fill="hsl(var(--muted-foreground))" fontFamily="monospace">-2m</text>
                  <text x="215" y="79" fontSize="7" fill="hsl(var(--muted-foreground))" fontFamily="monospace">now</text>
                </svg>
                <div className="mt-2 flex items-center justify-between">
                  <span className="font-mono text-[11px] text-muted-foreground">msg/s</span>
                  <span className="font-mono text-xs font-semibold text-success">+12.4k</span>
                </div>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* My Pipelines */}
      <section className="mx-auto max-w-4xl px-6">
        <div className="mb-6 flex items-center justify-between">
          <div>
            <h2 className="text-xl font-bold tracking-tight">My Pipelines</h2>
            {email && email !== "anonymous" && (
              <p className="mt-0.5 text-xs text-muted-foreground">{email}</p>
            )}
          </div>
          <Button asChild className="gap-2 hover:shadow-[0_0_15px_rgba(255,54,33,0.25)]">
            <Link to="/pipelines/new">
              <Plus className="h-4 w-4" />
              Create Pipeline
            </Link>
          </Button>
        </div>

        {loading ? (
          <div className="flex items-center justify-center gap-2 py-16 text-sm text-muted-foreground">
            <Loader2 className="h-4 w-4 animate-spin" />
            Loading pipelines...
          </div>
        ) : pipelines.length === 0 ? (
          <Card>
            <CardContent className="flex flex-col items-center justify-center py-16 text-center">
              <div className="mb-4 flex h-12 w-12 items-center justify-center rounded-full bg-primary/10">
                <Activity className="h-6 w-6 text-primary" />
              </div>
              <h3 className="mb-1 text-base font-semibold">
                No pipelines yet
              </h3>
              <p className="mb-6 max-w-sm text-sm text-muted-foreground">
                Create your first IoT pipeline to start streaming sensor data
                in real time.
              </p>
              <Button asChild className="gap-2 hover:shadow-[0_0_15px_rgba(255,54,33,0.25)]">
                <Link to="/pipelines/new">
                  <Plus className="h-4 w-4" />
                  Create Your First Pipeline
                  <ArrowRight className="h-4 w-4" />
                </Link>
              </Button>
            </CardContent>
          </Card>
        ) : (
          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
            {pipelines.map((p) => (
              <PipelineCard
                key={p.id}
                pipeline={p}
                onDelete={setDeleteTarget}
              />
            ))}
          </div>
        )}
      </section>

      <ConfirmDialog
        open={deleteTarget !== null}
        title="Delete Pipeline"
        description={
          targetPipeline
            ? `This will permanently delete "${targetPipeline.name}" and clean up all provisioned resources (jobs, clusters, UC objects, Lakebase tables). Notebooks will be kept. This action cannot be undone.`
            : ""
        }
        confirmLabel="Delete Pipeline"
        destructive
        onConfirm={handleDelete}
        onCancel={() => setDeleteTarget(null)}
      />
    </div>
  );
}
