import { useState, useEffect, useMemo } from "react";
import {
  FlaskConical,
  Factory,
  Pickaxe,
  Zap,
  Droplets,
  Car,
  Cpu,
  Building2,
  Rocket,
  Globe,
  Wheat,
  Sun,
  Beaker,
  UtensilsCrossed,
  Server,
  Home,
  Eye,
  Loader2,
  CheckCircle2,
  Gauge,
  Settings2,
  BarChart3,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { CollapsibleSection } from "@/components/collapsible-section";
import { cn } from "@/lib/utils";
import type { LucideIcon } from "lucide-react";
import type { FrequencyMode } from "@/lib/types";

interface IndustryInfo {
  id: string;
  label: string;
  sensor_count: number;
}

interface SampleRecord {
  sensor_name: string;
  industry: string;
  value: number;
  unit: string;
  sensor_type: string;
  timestamp: number;
  fault_active: boolean;
}

const ESTIMATED_RECORD_BYTES = 175;

const industryIcons: Record<string, LucideIcon> = {
  pharmaceutical: FlaskConical,
  manufacturing: Factory,
  mining: Pickaxe,
  electric_power: Zap,
  water_wastewater: Droplets,
  automotive: Car,
  data_center: Server,
  oil_gas: Building2,
  aerospace: Rocket,
  space: Globe,
  chemical: Beaker,
  food_beverage: UtensilsCrossed,
  smart_building: Home,
  agriculture: Wheat,
  renewable_energy: Sun,
  utilities: Cpu,
};

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
}

export interface DataHubStepProps {
  selectedIndustry: string;
  onIndustryChange: (id: string) => void;
  sensorConfigs: Record<string, unknown>[];
  onSensorConfigsChange: (configs: Record<string, unknown>[]) => void;
  updateRateHz: number;
  onUpdateRateHzChange: (hz: number) => void;
  frequencyMode: FrequencyMode;
  onFrequencyModeChange: (mode: FrequencyMode) => void;
}

export function DataHubStep({
  selectedIndustry,
  onIndustryChange,
  sensorConfigs,
  onSensorConfigsChange,
  updateRateHz,
  onUpdateRateHzChange,
  frequencyMode,
  onFrequencyModeChange,
}: DataHubStepProps) {
  const [industries, setIndustries] = useState<IndustryInfo[]>([]);
  const [loadingIndustries, setLoadingIndustries] = useState(true);
  const [loadingSensors, setLoadingSensors] = useState(false);
  const [sampleRecords, setSampleRecords] = useState<SampleRecord[]>([]);
  const [loadingSample, setLoadingSample] = useState(false);
  const [sampleViewMode, setSampleViewMode] = useState<"table" | "json">("table");

  useEffect(() => {
    fetch("/api/industries")
      .then((r) => r.json())
      .then((data) => {
        setIndustries(data);
        setLoadingIndustries(false);
      })
      .catch(() => setLoadingIndustries(false));
  }, []);

  useEffect(() => {
    if (!selectedIndustry) return;
    setLoadingSensors(true);
    fetch(`/api/industries/${selectedIndustry}/sensors`)
      .then((r) => r.json())
      .then((data) => {
        onSensorConfigsChange(data);
        setLoadingSensors(false);
      })
      .catch(() => setLoadingSensors(false));
  }, [selectedIndustry]); // eslint-disable-line react-hooks/exhaustive-deps

  const generateSample = async () => {
    setLoadingSample(true);
    try {
      const res = await fetch("/api/generate-sample", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          industry: selectedIndustry,
          update_rate_hz: updateRateHz,
        }),
      });
      const data = await res.json();
      setSampleRecords(data.records || []);
    } catch {
      setSampleRecords([]);
    }
    setLoadingSample(false);
  };

  const handleSensorFrequencyChange = (index: number, hz: number) => {
    const updated = sensorConfigs.map((cfg, i) =>
      i === index ? { ...cfg, update_frequency_hz: hz } : cfg,
    );
    onSensorConfigsChange(updated);
  };

  const payload = useMemo(() => {
    const sensorCount = sensorConfigs.length;
    const perTick = sensorCount * ESTIMATED_RECORD_BYTES;
    const totalHz = sensorConfigs.reduce(
      (sum, cfg) => sum + (Number(cfg.update_frequency_hz) || updateRateHz),
      0,
    );
    const perSecond = totalHz * ESTIMATED_RECORD_BYTES;
    const perMinute = perSecond * 60;
    return { sensorCount, perTick, perSecond, perMinute, totalHz };
  }, [sensorConfigs, updateRateHz]);

  return (
    <div className="space-y-6">
      {/* ---- Select Industry ---- */}
      <CollapsibleSection
        title="Select Industry"
        icon={Factory}
        complete={!!selectedIndustry}
        defaultOpen
      >
        {loadingIndustries ? (
          <div className="flex items-center gap-2 text-sm text-muted-foreground">
            <Loader2 className="h-4 w-4 animate-spin" />
            Loading industries...
          </div>
        ) : (
          <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-4">
            {industries.map((ind) => {
              const Icon = industryIcons[ind.id] || Cpu;
              const isSelected = selectedIndustry === ind.id;
              return (
                <div
                  key={ind.id}
                  onClick={() => onIndustryChange(ind.id)}
                  className={cn(
                    "glass cursor-pointer rounded-lg p-4 flex items-center gap-3 transition-all hover:border-primary/60",
                    isSelected
                      ? "border-primary glow-primary"
                      : "border-border/50",
                  )}
                >
                  <div
                    className={cn(
                      "flex h-10 w-10 shrink-0 items-center justify-center rounded-lg",
                      isSelected
                        ? "bg-primary/15 text-primary"
                        : "bg-surface-highest text-muted-foreground",
                    )}
                  >
                    <Icon className="h-5 w-5" />
                  </div>
                  <div className="min-w-0">
                    <p className="truncate text-sm font-medium">
                      {ind.label}
                    </p>
                    <p className="text-xs text-muted-foreground">
                      {ind.sensor_count} sensors
                    </p>
                  </div>
                  {isSelected && (
                    <CheckCircle2 className="ml-auto h-4 w-4 shrink-0 text-primary" />
                  )}
                </div>
              );
            })}
          </div>
        )}
      </CollapsibleSection>

      {/* ---- Sensor Configuration ---- */}
      <CollapsibleSection
        title="Sensor Configuration"
        icon={Settings2}
        complete={sensorConfigs.length > 0}
        defaultOpen={!!selectedIndustry}
      >
        {!selectedIndustry ? (
          <div className="flex h-24 items-center justify-center text-sm text-muted-foreground">
            Select an industry above to load sensor configurations
          </div>
        ) : (
          <div className="glass rounded-lg border-border/50 overflow-hidden">
            <div className="flex flex-row items-center justify-between px-4 py-3 border-b border-border/50">
              <div className="flex items-center gap-2">
                <Badge variant="secondary" className="capitalize">
                  {selectedIndustry.replace(/_/g, " ")}
                </Badge>
                <Badge variant="outline" className="ml-2">
                  {sensorConfigs.length} sensors
                </Badge>
              </div>
            </div>
            <div className="p-0">
              {loadingSensors ? (
                <div className="flex items-center justify-center gap-2 p-8 text-sm text-muted-foreground">
                  <Loader2 className="h-4 w-4 animate-spin" />
                  Loading sensor configs...
                </div>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full">
                    <thead>
                      <tr className="border-b border-border/50 bg-surface-highest">
                        {[
                          "Name",
                          "Type",
                          "Unit",
                          "Min",
                          "Max",
                          "Nominal",
                          ...(frequencyMode === "per-sensor" ? ["Freq (Hz)"] : []),
                        ].map((h) => (
                          <th
                            key={h}
                            className="px-3 py-2 text-left text-[10px] font-ui font-medium uppercase tracking-wider text-muted-foreground"
                          >
                            {h}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {sensorConfigs.map((cfg, i) => (
                        <tr
                          key={i}
                          className="border-b border-border/50 last:border-0 bg-surface-low hover:bg-surface-high transition-colors"
                        >
                          <td className="px-3 py-1.5 text-xs font-medium">
                            {String(cfg.name)}
                          </td>
                          <td className="px-3 py-1.5">
                            <Badge
                              variant="secondary"
                              className="text-[10px] capitalize"
                            >
                              {String(cfg.sensor_type)}
                            </Badge>
                          </td>
                          <td className="px-3 py-1.5 text-xs text-muted-foreground">
                            {String(cfg.unit)}
                          </td>
                          <td className="px-3 py-1.5 text-xs tabular-nums">
                            {Number(cfg.min_value).toFixed(1)}
                          </td>
                          <td className="px-3 py-1.5 text-xs tabular-nums">
                            {Number(cfg.max_value).toFixed(1)}
                          </td>
                          <td className="px-3 py-1.5 text-xs tabular-nums">
                            {Number(cfg.nominal_value).toFixed(1)}
                          </td>
                          {frequencyMode === "per-sensor" && (
                            <td className="px-3 py-1.5">
                              <input
                                type="number"
                                min={0.1}
                                max={2000}
                                step={0.1}
                                value={Number(cfg.update_frequency_hz || 1)}
                                onChange={(e) =>
                                  handleSensorFrequencyChange(
                                    i,
                                    parseFloat(e.target.value) || 1,
                                  )
                                }
                                className="w-16 rounded border border-border/50 bg-input px-1.5 py-0.5 text-xs tabular-nums focus:outline-none focus:ring-1 focus:ring-primary"
                              />
                            </td>
                          )}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          </div>
        )}
      </CollapsibleSection>

      {/* ---- Payload & Frequency ---- */}
      <CollapsibleSection
        title="Payload & Frequency"
        icon={Gauge}
        complete={!!selectedIndustry && sensorConfigs.length > 0}
        defaultOpen={!!selectedIndustry}
      >
        <div className="space-y-5">
          {/* Frequency mode toggle */}
          <div className="glass rounded-lg p-5 space-y-4">
              <div className="flex items-center justify-between">
                <label className="text-xs font-ui font-medium uppercase tracking-wider text-muted-foreground">
                  Frequency Control
                </label>
                <div className="flex rounded-md border border-border/50">
                  {(["global", "per-sensor"] as const).map((mode) => (
                    <button
                      key={mode}
                      onClick={() => onFrequencyModeChange(mode)}
                      className={cn(
                        "px-3 py-1.5 text-xs font-medium transition-colors",
                        frequencyMode === mode
                          ? "bg-primary text-primary-foreground"
                          : "text-muted-foreground hover:bg-muted",
                        mode === "global" && "rounded-l-md",
                        mode === "per-sensor" && "rounded-r-md",
                      )}
                    >
                      {mode === "global" ? "Global Rate" : "Per-Sensor"}
                    </button>
                  ))}
                </div>
              </div>

              {frequencyMode === "global" ? (
                <>
                  <div>
                    <div className="flex items-center justify-between mb-2">
                      <span className="text-xs text-muted-foreground">
                        All sensors generate at the same rate
                      </span>
                      <span className="rounded-md bg-primary/10 px-2 py-0.5 text-sm font-semibold tabular-nums text-primary">
                        {updateRateHz.toFixed(1)} Hz
                      </span>
                    </div>
                    <input
                      type="range"
                      min={0.5}
                      max={2000}
                      step={updateRateHz >= 100 ? 10 : updateRateHz >= 10 ? 1 : 0.5}
                      value={updateRateHz}
                      onChange={(e) => {
                        const hz = parseFloat(e.target.value);
                        onUpdateRateHzChange(hz);
                        const updated = sensorConfigs.map((cfg) => ({
                          ...cfg,
                          update_frequency_hz: hz,
                        }));
                        onSensorConfigsChange(updated);
                      }}
                      className="w-full accent-primary"
                    />
                    <div className="mt-1 flex justify-between text-[10px] text-muted-foreground tabular-nums">
                      <span>0.5 Hz</span>
                      <span>500 Hz</span>
                      <span>1000 Hz</span>
                      <span>2000 Hz</span>
                    </div>
                  </div>
                  <p className="text-xs text-muted-foreground leading-relaxed">
                    <strong>Hz (Hertz)</strong> measures frequency — how many
                    times per second data is generated. 1 Hz = 1 reading/second
                    per sensor; 10 Hz = 10 readings/second per sensor.
                  </p>
                </>
              ) : (
                <p className="text-xs text-muted-foreground leading-relaxed">
                  Each sensor has its own frequency set in the{" "}
                  <strong>Sensor Configuration</strong> table above. Adjust the{" "}
                  <strong>Freq (Hz)</strong> column per row.
                </p>
              )}
          </div>

          {/* Payload size summary */}
          {sensorConfigs.length > 0 && (
            <div className="glass rounded-lg border-border/50 overflow-hidden">
              <div className="flex items-center gap-2 px-4 py-3 border-b border-border/50">
                <BarChart3 className="h-4 w-4 text-primary" />
                <h3 className="font-display text-sm font-semibold">Estimated Payload Size</h3>
              </div>
              <div className="p-4">
                <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
                  <div>
                    <p className="text-[10px] font-medium uppercase tracking-wider text-muted-foreground">
                      Sensors
                    </p>
                    <p className="mt-1 text-lg font-semibold tabular-nums">
                      {payload.sensorCount}
                    </p>
                  </div>
                  <div>
                    <p className="text-[10px] font-medium uppercase tracking-wider text-muted-foreground">
                      Per Tick
                    </p>
                    <p className="mt-1 text-lg font-semibold tabular-nums">
                      {formatBytes(payload.perTick)}
                    </p>
                    <p className="text-[10px] text-muted-foreground">
                      ~{ESTIMATED_RECORD_BYTES} B/record
                    </p>
                  </div>
                  <div>
                    <p className="text-[10px] font-medium uppercase tracking-wider text-muted-foreground">
                      Per Second
                    </p>
                    <p className={cn(
                      "mt-1 text-lg font-semibold tabular-nums",
                      payload.perSecond > 10 * 1024 * 1024
                        ? "text-destructive"
                        : payload.perSecond > 5 * 1024 * 1024
                          ? "text-amber-500"
                          : "",
                    )}>
                      {formatBytes(payload.perSecond)}/s
                    </p>
                    <p className="text-[10px] text-muted-foreground">
                      {payload.totalHz.toFixed(1)} Hz total
                    </p>
                    {payload.perSecond > 10 * 1024 * 1024 && (
                      <Badge variant="destructive" className="mt-1 text-[10px]">
                        Exceeds 10 MB/s
                      </Badge>
                    )}
                    {payload.perSecond > 5 * 1024 * 1024 && payload.perSecond <= 10 * 1024 * 1024 && (
                      <Badge variant="secondary" className="mt-1 text-[10px] text-amber-500 border-amber-500/30">
                        High throughput
                      </Badge>
                    )}
                  </div>
                  <div>
                    <p className="text-[10px] font-medium uppercase tracking-wider text-muted-foreground">
                      Per Minute
                    </p>
                    <p className="mt-1 text-lg font-semibold tabular-nums">
                      {formatBytes(payload.perMinute)}/min
                    </p>
                  </div>
                </div>
              </div>
            </div>
          )}
        </div>
      </CollapsibleSection>

      {/* ---- Sample Preview ---- */}
      <CollapsibleSection
        title="Sample Preview"
        icon={Eye}
        complete={sampleRecords.length > 0}
        defaultOpen={false}
      >
        <div className="glass rounded-lg border-border/50 overflow-hidden">
          <div className="flex flex-row items-center justify-between px-4 py-3 border-b border-border/50">
            <div className="flex items-center gap-2">
              {sampleRecords.length > 0 && (
                <Badge variant="secondary">
                  {sampleRecords.length} records
                </Badge>
              )}
              {sampleRecords.length > 0 && (
                <div className="flex rounded-md border border-border/50">
                  {(["table", "json"] as const).map((mode) => (
                    <button
                      key={mode}
                      onClick={() => setSampleViewMode(mode)}
                      className={cn(
                        "px-2.5 py-1 text-[10px] font-ui font-medium uppercase transition-colors",
                        sampleViewMode === mode
                          ? "bg-primary text-primary-foreground"
                          : "text-muted-foreground hover:bg-surface-highest",
                        mode === "table" && "rounded-l-md",
                        mode === "json" && "rounded-r-md",
                      )}
                    >
                      {mode}
                    </button>
                  ))}
                </div>
              )}
            </div>
            <Button
              size="sm"
              onClick={generateSample}
              disabled={loadingSample || !selectedIndustry}
            >
              {loadingSample ? (
                <Loader2 className="mr-2 h-3 w-3 animate-spin" />
              ) : (
                <Eye className="mr-2 h-3 w-3" />
              )}
              Generate Sample
            </Button>
          </div>
          <div className="p-0">
            {sampleRecords.length > 0 ? (
              sampleViewMode === "json" ? (
                <pre className="max-h-96 overflow-auto p-4 font-mono text-xs text-foreground bg-surface-low">
                  {JSON.stringify(sampleRecords, null, 2)}
                </pre>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full">
                    <thead>
                      <tr className="border-b border-border/50 bg-surface-highest">
                        {["Sensor", "Value", "Unit", "Type", "Fault"].map(
                          (h) => (
                            <th
                              key={h}
                              className="px-3 py-2 text-left text-[10px] font-ui font-medium uppercase tracking-wider text-muted-foreground"
                            >
                              {h}
                            </th>
                          ),
                        )}
                      </tr>
                    </thead>
                    <tbody>
                      {sampleRecords.slice(0, 20).map((rec, i) => (
                        <tr
                          key={i}
                          className="border-b border-border/50 last:border-0 bg-surface-low hover:bg-surface-high transition-colors"
                        >
                          <td className="px-3 py-1.5 text-xs font-medium">
                            {rec.sensor_name}
                          </td>
                          <td className="px-3 py-1.5 font-mono text-xs font-semibold tabular-nums">
                            {rec.value.toFixed(3)}
                          </td>
                          <td className="px-3 py-1.5 text-xs text-muted-foreground">
                            {rec.unit}
                          </td>
                          <td className="px-3 py-1.5">
                            <Badge
                              variant="secondary"
                              className="text-[10px] capitalize"
                            >
                              {rec.sensor_type}
                            </Badge>
                          </td>
                          <td className="px-3 py-1.5">
                            {rec.fault_active ? (
                              <Badge
                                variant="destructive"
                                className="text-[10px]"
                              >
                                FAULT
                              </Badge>
                            ) : (
                              <Badge variant="success" className="text-[10px]">
                                OK
                              </Badge>
                            )}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )
            ) : (
              <div className="flex h-24 items-center justify-center text-sm text-muted-foreground">
                Click &quot;Generate Sample&quot; to preview sensor data
              </div>
            )}
          </div>
        </div>
      </CollapsibleSection>
    </div>
  );
}
