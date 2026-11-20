import { useMemo } from "react";
import { Activity, CircleDot, RadioTower } from "lucide-react";
import { Line, LineChart, ResponsiveContainer, YAxis } from "recharts";
import { useSseStream } from "@/hooks/use-sse-stream";

interface RealtimeKpiCardProps {
  /** Metric to subscribe to — matched against ``sensor_type`` or ``sensor_name``. */
  metric: string;
  /** Human-readable label for the card heading. */
  label: string;
  /** Unit shown next to the value (e.g. ``"°C"``, ``"rpm"``). */
  unit?: string;
  /** Number of sparkline points (default 60 ≈ 1 min @ 1 Hz). */
  capacity?: number;
}

export function RealtimeKpiCard({
  metric,
  label,
  unit,
  capacity = 60,
}: RealtimeKpiCardProps) {
  const { points, connected } = useSseStream({
    metrics: [metric],
    capacity,
  });

  const latest = points.at(-1);
  const series = useMemo(
    () => points.map((p, i) => ({ i, value: p.value })),
    [points],
  );

  return (
    <div className="flex flex-col gap-2 rounded-md border border-border/50 glass p-3">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <RadioTower className="h-3.5 w-3.5 text-primary" />
          <span className="font-ui text-xs font-medium text-muted-foreground uppercase tracking-wider">
            {label}
          </span>
        </div>
        <div
          className={`flex items-center gap-1 text-[10px] ${
            connected ? "text-success" : "text-muted-foreground"
          }`}
          title={connected ? "Live via SSE" : "Reconnecting…"}
        >
          <CircleDot className="h-3 w-3" />
          {connected ? "live" : "offline"}
        </div>
      </div>

      <div className="flex items-baseline gap-1">
        <span className="font-display text-2xl font-semibold tabular-nums">
          {latest ? latest.value.toFixed(2) : "—"}
        </span>
        {unit && (
          <span className="text-xs text-muted-foreground">{unit}</span>
        )}
      </div>

      <div className="h-12">
        {series.length > 1 ? (
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={series} margin={{ top: 2, right: 2, bottom: 2, left: 2 }}>
              <YAxis hide domain={["auto", "auto"]} />
              <Line
                type="monotone"
                dataKey="value"
                stroke="currentColor"
                strokeWidth={1.5}
                dot={false}
                isAnimationActive={false}
              />
            </LineChart>
          </ResponsiveContainer>
        ) : (
          <div className="flex h-full items-center gap-1.5 text-xs text-muted-foreground">
            <Activity className="h-3.5 w-3.5" />
            waiting for data…
          </div>
        )}
      </div>

      {latest?.sensor && (
        <p className="truncate text-[10px] text-muted-foreground">
          {latest.sensor}
        </p>
      )}
    </div>
  );
}
