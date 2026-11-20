import { useEffect, useMemo, useState } from "react";
import { api } from "@/lib/api";
import { useSensorContext } from "@/contexts/sensor-context";
import { formatNumber } from "@/lib/utils";
import type { AlertRecord, AlertStatus } from "@/lib/types";
import type {
  IndustryAlertRule,
  IndustryAssets,
  IndustryMaintenanceEvent,
} from "@/lib/industries-api";

interface AlertEvidence {
  siteName: string | null;
  assetId: string | null;
  assetName: string | null;
  componentId: string | null;
  componentName: string | null;
  sensorName: string | null;
}

function severityClass(severity: string | null | undefined): "crit" | "warn" | "info" {
  if (severity === "crit" || severity === "warn" || severity === "info") return severity;
  return "info";
}

function statusLabel(status: string | null | undefined): AlertStatus | string {
  return status || "open";
}

function stamp(value: string | null | undefined): string {
  if (!value) return "—";
  const parsed = Date.parse(value);
  if (Number.isNaN(parsed)) return value;
  return new Date(parsed).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

export function findAlertEvidence(
  assets: IndustryAssets | null,
  sensorId: string | null | undefined,
): AlertEvidence {
  const empty: AlertEvidence = {
    siteName: null,
    assetId: null,
    assetName: null,
    componentId: null,
    componentName: null,
    sensorName: null,
  };
  if (!assets || !sensorId) return empty;
  for (const site of assets.sites) {
    for (const asset of site.assets) {
      for (const component of asset.components) {
        for (const sensor of component.sensors) {
          if (sensor.sensor_id === sensorId || sensor.name === sensorId) {
            return {
              siteName: site.name,
              assetId: asset.asset_id,
              assetName: asset.name,
              componentId: component.component_id,
              componentName: component.name,
              sensorName: sensor.name,
            };
          }
        }
      }
    }
  }
  return empty;
}

function relatedMaintenance(
  history: IndustryMaintenanceEvent[],
  evidence: AlertEvidence,
): IndustryMaintenanceEvent | null {
  if (!evidence.assetId) return null;
  const matches = history.filter(
    (event) =>
      event.asset_id === evidence.assetId &&
      (!evidence.componentId || event.component_id === evidence.componentId),
  );
  return matches[0] ?? history.find((event) => event.asset_id === evidence.assetId) ?? null;
}

function mergeAlerts(
  rest: AlertRecord[],
  live: Record<string, AlertRecord>,
  industry: string,
): AlertRecord[] {
  const byId: Record<string, AlertRecord> = {};
  for (const alert of rest) {
    if (alert.industry === industry) byId[alert.id] = alert;
  }
  for (const alert of Object.values(live)) {
    if (alert.industry === industry) {
      byId[alert.id] = alert;
    }
  }
  return Object.values(byId).sort((a, b) => {
    const rank = (status: string) =>
      status === "open" ? 0 : status === "acknowledged" ? 1 : 2;
    const byStatus = rank(a.status) - rank(b.status);
    if (byStatus !== 0) return byStatus;
    return String(b.updated_at ?? "").localeCompare(String(a.updated_at ?? ""));
  });
}

/** REST snapshot overlaid with live SSE lifecycle events for one industry. */
export function useIndustryAlerts(industry: string) {
  const { alerts: liveAlerts } = useSensorContext();
  const [restAlerts, setRestAlerts] = useState<AlertRecord[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    api
      .listAlerts({ industry })
      .then((body) => {
        if (!cancelled) setRestAlerts(body.alerts ?? []);
      })
      .catch((err: unknown) => {
        if (!cancelled) setError(err instanceof Error ? err.message : "Alerts unavailable");
      });
    return () => {
      cancelled = true;
    };
  }, [industry]);

  const alerts = useMemo(
    () => mergeAlerts(restAlerts, liveAlerts, industry),
    [restAlerts, liveAlerts, industry],
  );
  return { alerts, error, setError };
}

interface AlertRailProps {
  industry: string;
  pipelineId: string | null;
  assets: IndustryAssets | null;
  maintenance: IndustryMaintenanceEvent[];
  rules?: IndustryAlertRule[];
  onExplainAlert?: (alert: AlertRecord) => void;
}

export function AlertRail({ industry, pipelineId, assets, maintenance, rules = [], onExplainAlert }: AlertRailProps) {
  const { sensors, applyAlertEvent } = useSensorContext();
  const { alerts, error, setError } = useIndustryAlerts(industry);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [ackBusy, setAckBusy] = useState(false);
  const [telemetryOpen, setTelemetryOpen] = useState(false);

  const selected = alerts.find((alert) => alert.id === selectedId) ?? alerts[0] ?? null;
  const evidence = findAlertEvidence(assets, selected?.sensor_id);
  const maintenanceEvent = selected ? relatedMaintenance(maintenance, evidence) : null;
  const sensorName = evidence.sensorName;
  const liveReading = sensorName ? sensors[sensorName] : undefined;
  const action = selected ? rules.find((r) => r.rule_id === selected.rule_id)?.action : undefined;

  async function handleAcknowledge() {
    if (!selected || selected.status !== "open") return;
    setAckBusy(true);
    try {
      const event = await api.acknowledgeAlert(selected.id);
      applyAlertEvent(event);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "Acknowledge failed");
    } finally {
      setAckBusy(false);
    }
  }

  return (
    <aside className="alert-rail" aria-label="Operational alerts">
      <div className="alert-rail__head">
        <span className="alert-rail__title">Alerts</span>
        <span className="alert-rail__count tnum">{alerts.filter((a) => a.status !== "cleared").length}</span>
      </div>
      {error && <p className="alert-rail__error">{error}</p>}
      <div className="alert-rail__list">
        {alerts.length === 0 ? (
          <div className="alert-rail__empty">No open alerts</div>
        ) : (
          alerts.map((alert) => {
            const sev = severityClass(alert.severity);
            const active = selected?.id === alert.id;
            return (
              <button
                type="button"
                key={alert.id}
                className={`alert-rail__item ${sev}${active ? " is-active" : ""}${alert.status === "cleared" ? " is-cleared" : ""}`}
                onClick={() => {
                  setSelectedId(alert.id);
                  setTelemetryOpen(false);
                }}
              >
                <span className="rail-c" />
                <span className="alert-rail__fault">{alert.fault_code || alert.rule_id}</span>
                <span className="alert-rail__meta">
                  <span className={`alert-rail__sev ${sev}`}>{alert.severity}</span>
                  <span className="alert-rail__status">{statusLabel(alert.status)}</span>
                </span>
              </button>
            );
          })
        )}
      </div>

      {selected && (
        <div className="alert-rail__detail">
          {action && (
            <div className="alert-rail__action" data-testid="alert-action">
              <span>Action</span>
              <b>{action}</b>
            </div>
          )}
          <div className="alert-rail__detail-row">
            <span>Severity</span>
            <b className={severityClass(selected.severity)}>{selected.severity}</b>
          </div>
          <div className="alert-rail__detail-row">
            <span>Status</span>
            <b>{statusLabel(selected.status)}</b>
          </div>
          <div className="alert-rail__detail-row">
            <span>Asset</span>
            <b>{evidence.assetName ?? "—"}</b>
          </div>
          <div className="alert-rail__detail-row">
            <span>Component</span>
            <b>{evidence.componentName ?? "—"}</b>
          </div>
          <div className="alert-rail__detail-row">
            <span>Sensor</span>
            <b>{evidence.sensorName ?? selected.sensor_id}</b>
          </div>
          <div className="alert-rail__detail-row">
            <span>Value</span>
            <b className="tnum">
              {selected.value == null ? "—" : formatNumber(selected.value, 2)}
              {selected.threshold != null ? ` / ${formatNumber(selected.threshold, 2)}` : ""}
            </b>
          </div>
          {maintenanceEvent && (
            <div className="alert-rail__maint">
              <span>Maintenance</span>
              <p>
                {maintenanceEvent.kind} · {maintenanceEvent.notes}
              </p>
            </div>
          )}
          <div className="alert-rail__actions">
            <button
              type="button"
              className="btn sm"
              disabled={selected.status !== "open" || ackBusy}
              onClick={() => void handleAcknowledge()}
            >
              Acknowledge
            </button>
            <button
              type="button"
              className="btn sm"
              onClick={() => setTelemetryOpen((open) => !open)}
            >
              Telemetry
            </button>
            {onExplainAlert && (
              <button
                type="button"
                className="btn sm"
                data-testid="genie-explain-alert"
                onClick={() => onExplainAlert(selected)}
              >
                Explain this alert
              </button>
            )}
          </div>
          {telemetryOpen && (
            <div className="alert-rail__telemetry" id="alert-telemetry">
              <div className="alert-rail__detail-row">
                <span>Live</span>
                <b className="tnum">
                  {liveReading ? `${formatNumber(liveReading.value, 2)} ${liveReading.unit}` : "no live sample"}
                </b>
              </div>
              <div className="alert-rail__detail-row">
                <span>Opened</span>
                <b>{stamp(selected.opened_at)}</b>
              </div>
              <a className="alert-rail__link" href="#historical-dbsql">
                Drill down · historical window
              </a>
              {pipelineId && evidence.sensorName && (
                <span className="alert-rail__pipe">{pipelineId.slice(0, 8)} · {evidence.sensorName}</span>
              )}
            </div>
          )}
        </div>
      )}
    </aside>
  );
}
