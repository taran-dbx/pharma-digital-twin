import { useEffect, useRef, useState } from "react";
import type { SensorReading, WsMessage } from "@/lib/types";

export interface SseDataPoint {
  t: number;            // epoch seconds
  sensor: string;       // sensor_name
  value: number;
  source?: string;
}

interface UseSseStreamOptions {
  /** Metric names (sensor_type or sensor_name) to accept.  Empty → all. */
  metrics?: string[];
  /** Max points retained in the ring buffer (default 120 ≈ 2 minutes @ 1 Hz). */
  capacity?: number;
  /** Set ``false`` to short-circuit (useful when ``sse_metrics`` is empty). */
  enabled?: boolean;
  /** Current industry — forwarded so the server can filter alert events. */
  industry?: string | null;
}

/**
 * Subscribe to the app's ``/api/events`` SSE channel and feed a rolling buffer
 * of datapoints filtered to specific metrics.  Designed for realtime KPI charts
 * — latency is bounded by the Spark RTM job cadence (sub-second in practice).
 */
function eventsUrl(industry?: string | null): string {
  if (!industry) return "/api/events";
  return `/api/events?industry=${encodeURIComponent(industry)}`;
}

export function useSseStream({
  metrics = [],
  capacity = 120,
  enabled = true,
  industry,
}: UseSseStreamOptions = {}) {
  const [points, setPoints] = useState<SseDataPoint[]>([]);
  const [connected, setConnected] = useState(false);
  const esRef = useRef<EventSource | null>(null);
  const reconnectTimer = useRef<ReturnType<typeof setTimeout>>();
  const metricsRef = useRef(metrics);
  metricsRef.current = metrics;

  useEffect(() => {
    if (!enabled) {
      setConnected(false);
      return;
    }

    function openStream(): void {
      const es = new EventSource(eventsUrl(industry));
      esRef.current = es;

      es.onopen = () => setConnected(true);

      es.onmessage = (ev: MessageEvent<string>) => {
        let msg: WsMessage;
        try {
          msg = JSON.parse(ev.data) as WsMessage;
        } catch {
          return;
        }
        if (msg.type !== "sensor_update" || !Array.isArray(msg.sensors)) return;

        const filter = metricsRef.current;
        const accept = (r: SensorReading): boolean =>
          filter.length === 0
            ? true
            : filter.includes(r.sensor_type) || filter.includes(r.sensor_name);

        const newPoints = msg.sensors
          .filter(accept)
          .map<SseDataPoint>((r) => ({
            t: r.timestamp,
            sensor: r.sensor_name,
            value: r.value,
            source: r.source,
          }));

        if (newPoints.length === 0) return;

        setPoints((prev) => {
          const merged = [...prev, ...newPoints];
          return merged.length > capacity
            ? merged.slice(merged.length - capacity)
            : merged;
        });
      };

      es.onerror = () => {
        setConnected(false);
        es.close();
        esRef.current = null;
        reconnectTimer.current = setTimeout(openStream, 2000);
      };
    }

    openStream();

    return () => {
      clearTimeout(reconnectTimer.current);
      esRef.current?.close();
      esRef.current = null;
    };
  }, [capacity, enabled, industry]);

  return { points, connected };
}
