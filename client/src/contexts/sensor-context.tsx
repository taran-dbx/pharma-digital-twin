import {
  createContext,
  useContext,
  useCallback,
  useReducer,
  type ReactNode,
} from "react";
import { useLocation } from "react-router-dom";
import { useWebSocket } from "@/hooks/use-websocket";
import type {
  AlertLifecycleEvent,
  AlertRecord,
  AlertResponseRecord,
  ProductionState,
  SensorReading,
  SensorUpdateMessage,
  SourceMetrics,
  WsMessage,
} from "@/lib/types";
import { toEpochSeconds } from "@/lib/utils";

export interface SensorHistoryPoint {
  /** Device event time, epoch milliseconds. */
  t: number;
  v: number;
}

interface SensorState {
  sensors: Record<string, SensorReading>;
  history: Record<string, SensorHistoryPoint[]>;
  sources: Record<string, SourceMetrics>;
  alerts: Record<string, AlertRecord>;
  /** Operator decisions per alert id, merged from REST and SSE. */
  responses: Record<string, AlertResponseRecord[]>;
  /** Cell production state per industry, merged from REST and SSE. */
  production: Record<string, ProductionState>;
  latencyHistory: Array<{
    time: number;
    source: string;
    write_ms: number;
    e2e_ms: number;
  }>;
  wsConnected: boolean;
  totalUpdates: number;
}

type Action =
  | { type: "SENSOR_UPDATE"; payload: SensorUpdateMessage }
  | { type: "SOURCE_STATUS"; payload: Record<string, SourceMetrics> }
  | { type: "ALERT_EVENT"; payload: AlertLifecycleEvent }
  | { type: "ALERT_RESPONSES"; payload: AlertResponseRecord[] }
  | { type: "PRODUCTION_STATE"; payload: ProductionState }
  | { type: "SET_CONNECTED"; payload: boolean }
  | { type: "CLEAR_SENSORS" };

const MAX_LATENCY_POINTS = 120;
const HISTORY_WINDOW_MS = 5 * 60_000;
const HISTORY_MIN_GAP_MS = 250;

function appendHistory(
  history: Record<string, SensorHistoryPoint[]>,
  readings: SensorReading[],
): Record<string, SensorHistoryPoint[]> {
  let next = history;
  for (const reading of readings) {
    const t = reading.timestamp * 1000;
    if (!Number.isFinite(t) || !Number.isFinite(reading.value)) continue;
    const series = next[reading.sensor_name] ?? [];
    const last = series[series.length - 1];
    if (last && t - last.t < HISTORY_MIN_GAP_MS) continue;
    const cutoff = t - HISTORY_WINDOW_MS;
    const start = series.findIndex((p) => p.t >= cutoff);
    const kept = start <= 0 ? series : series.slice(start);
    if (next === history) next = { ...history };
    next[reading.sensor_name] = [...kept, { t, v: reading.value }];
  }
  return next;
}

export function alertFromLifecycle(event: AlertLifecycleEvent): AlertRecord {
  return {
    id: event.alert_id,
    fingerprint: event.fingerprint ?? "",
    industry: event.industry,
    pipeline_id: event.pipeline_id,
    rule_id: event.rule_id ?? "",
    sensor_id: event.sensor_id ?? "",
    sensor_ids: Array.isArray(event.sensor_ids) ? event.sensor_ids : [],
    severity: event.severity ?? "info",
    status: event.status ?? "open",
    fault_code: event.fault_code,
    value: event.value,
    threshold: event.threshold,
    opened_at: event.opened_at,
    updated_at: event.updated_at,
    acknowledged_at: event.acknowledged_at,
    acknowledged_by: event.acknowledged_by,
    cleared_at: event.cleared_at,
  };
}

function reducer(state: SensorState, action: Action): SensorState {
  switch (action.type) {
    case "SENSOR_UPDATE": {
      const msg = action.payload;
      const readings = msg.sensors?.map((s) => ({ ...s, timestamp: toEpochSeconds(s.timestamp) }));
      const updated = { ...state.sensors };
      if (readings) {
        for (const s of readings) {
          updated[s.sensor_name] = s;
        }
      }
      const newLatency = [...state.latencyHistory];
      if (msg.write_latency_ms !== undefined) {
        newLatency.push({
          time: Date.now(),
          source: msg.source ?? "unknown",
          write_ms: msg.write_latency_ms,
          e2e_ms: msg.e2e_latency_ms ?? 0,
        });
        if (newLatency.length > MAX_LATENCY_POINTS) {
          newLatency.splice(0, newLatency.length - MAX_LATENCY_POINTS);
        }
      }
      return {
        ...state,
        sensors: updated,
        history: readings ? appendHistory(state.history, readings) : state.history,
        latencyHistory: newLatency,
        totalUpdates: state.totalUpdates + (readings?.length ?? 0),
      };
    }
    case "SOURCE_STATUS":
      return { ...state, sources: action.payload };
    case "ALERT_EVENT": {
      const alert = alertFromLifecycle(action.payload);
      if (!alert.id) return state;
      return {
        ...state,
        alerts: { ...state.alerts, [alert.id]: alert },
      };
    }
    case "ALERT_RESPONSES": {
      if (action.payload.length === 0) return state;
      const responses = { ...state.responses };
      for (const record of action.payload) {
        const existing = responses[record.alert_id] ?? [];
        if (existing.some((r) => r.id === record.id)) continue;
        responses[record.alert_id] = [...existing, record];
      }
      return { ...state, responses };
    }
    case "PRODUCTION_STATE": {
      const next = action.payload;
      if (!next?.industry || !next.state) return state;
      const current = state.production[next.industry];
      // REST polls can land after a newer SSE change; keep the latest change.
      if (current?.changed_at && (!next.changed_at || next.changed_at < current.changed_at)) {
        return state;
      }
      return { ...state, production: { ...state.production, [next.industry]: next } };
    }
    case "SET_CONNECTED":
      return { ...state, wsConnected: action.payload };
    case "CLEAR_SENSORS":
      return {
        ...state,
        sensors: {},
        history: {},
        latencyHistory: [],
        totalUpdates: 0,
      };
    default: {
      const _exhaustive: never = action;
      void _exhaustive;
      return state;
    }
  }
}

const initialState: SensorState = {
  sensors: {},
  history: {},
  sources: {},
  alerts: {},
  responses: {},
  production: {},
  latencyHistory: [],
  wsConnected: false,
  totalUpdates: 0,
};

interface SensorContextValue extends SensorState {
  clearSensors: () => void;
  applyAlertEvent: (event: AlertLifecycleEvent) => void;
  applyAlertResponses: (records: AlertResponseRecord[]) => void;
  applyProduction: (production: ProductionState) => void;
}

const SensorContext = createContext<SensorContextValue>({
  ...initialState,
  clearSensors: () => {},
  applyAlertEvent: () => {},
  applyAlertResponses: () => {},
  applyProduction: () => {},
});

function dispatchRealtimeMessage(
  dispatch: (action: Action) => void,
  msg: WsMessage,
): void {
  switch (msg.type) {
    case "sensor_update":
      dispatch({ type: "SENSOR_UPDATE", payload: msg });
      return;
    case "source_status":
      if (msg.sources) {
        dispatch({ type: "SOURCE_STATUS", payload: msg.sources });
      }
      return;
    case "cluster_status":
      return;
    case "alert_opened":
    case "alert_updated":
    case "alert_acknowledged":
    case "alert_cleared":
      dispatch({ type: "ALERT_EVENT", payload: msg });
      return;
    case "alert_response":
      dispatch({ type: "ALERT_RESPONSES", payload: [msg.response] });
      return;
    case "production_state":
      dispatch({ type: "PRODUCTION_STATE", payload: msg.production });
      return;
    default: {
      const _exhaustive: never = msg;
      void _exhaustive;
    }
  }
}

function industryFromPath(pathname: string): string | undefined {
  const match = pathname.match(/^\/i\/([^/]+)/);
  return match?.[1];
}

export function SensorProvider({ children }: { children: ReactNode }) {
  const [state, dispatch] = useReducer(reducer, initialState);
  const location = useLocation();
  const industry = industryFromPath(location.pathname);

  const handleMessage = useCallback((msg: WsMessage) => {
    dispatchRealtimeMessage(dispatch, msg);
  }, []);

  const clearSensors = useCallback(() => {
    dispatch({ type: "CLEAR_SENSORS" });
  }, []);

  const applyAlertEvent = useCallback((event: AlertLifecycleEvent) => {
    dispatch({ type: "ALERT_EVENT", payload: event });
  }, []);

  const applyAlertResponses = useCallback((records: AlertResponseRecord[]) => {
    dispatch({ type: "ALERT_RESPONSES", payload: records });
  }, []);

  const applyProduction = useCallback((production: ProductionState) => {
    dispatch({ type: "PRODUCTION_STATE", payload: production });
  }, []);

  const { connected } = useWebSocket({ onMessage: handleMessage, industry });

  if (connected !== state.wsConnected) {
    dispatch({ type: "SET_CONNECTED", payload: connected });
  }

  return (
    <SensorContext.Provider
      value={{ ...state, clearSensors, applyAlertEvent, applyAlertResponses, applyProduction }}
    >
      {children}
    </SensorContext.Provider>
  );
}

export function useSensorContext() {
  return useContext(SensorContext);
}
