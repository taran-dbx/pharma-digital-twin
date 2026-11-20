import { useState, useEffect } from "react";
import {
  Database,
  Server,
  Shield,
  Cpu,
  Warehouse,
  Loader2,
  Save,
  ClipboardCheck,
} from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { api } from "@/lib/api";
import { toast } from "sonner";
import type {
  UserSettings,
  UcSettings,
  LakebaseSettings,
  SecretsSettings,
  ClusterSettings,
  WarehouseSettings,
  NodeType,
  SparkVersion,
  SettingsValidationResult,
  SettingsStatus,
  ProvisionedResource,
  LakebaseCredentialInfo,
  WarehouseInfo,
} from "@/lib/types";
import { ValidationSection } from "./settings/settings-shared";
import { UcSettingsTab } from "./settings/uc-settings-tab";
import { LakebaseSettingsTab } from "./settings/lakebase-settings-tab";
import { SecretsSettingsTab } from "./settings/secrets-settings-tab";
import { ClusterSettingsTab } from "./settings/cluster-settings-tab";
import { WarehouseSettingsTab } from "./settings/warehouse-settings-tab";

// ---------------------------------------------------------------------------
// Default values
// ---------------------------------------------------------------------------
const DEFAULT_UC: UcSettings = {
  catalog_pattern: "iot_{name}",
  schemas: ["raw", "metrics", "checkpoints"],
  checkpoint_volume: "stream_checkpoints",
  storage_root: "",
  catalog_isolation_mode: "per-pipeline",
};

const DEFAULT_LAKEBASE: LakebaseSettings = {
  mode: "autoscale",
  project_id: "iot-digital-twin",
  branch_id: "production",
  database: "databricks_postgres",
  pg_schema: "raw",
  pg_table: "sensor_readings",
  host: "",
  port: "5432",
};

// Default secret-scope key *names* (identifiers, not secrets).
const LB_LOGIN_ENTRY_NAME = "lb-password";
const ZB_OAUTH_ENTRY_NAME = "zerobus-client-secret";

const DEFAULT_SECRETS: SecretsSettings = {
  scope: "iot-secrets",
  lb_user_key: "lb-user",
  lb_password_key: LB_LOGIN_ENTRY_NAME,
  zb_client_id_key: "zerobus-client-id",
  zb_client_secret_key: ZB_OAUTH_ENTRY_NAME,
};

const DEFAULT_CLUSTER: ClusterSettings = {
  workers: 2,
  worker_node_type: "",
  dbr_version: "",
  auto_terminate_min: 60,
};

const DEFAULT_WAREHOUSE: WarehouseSettings = {
  warehouse_id: "",
  warehouse_name: "",
};

function mergeDefaults(partial: Partial<UserSettings>): UserSettings {
  return {
    uc: { ...DEFAULT_UC, ...partial.uc },
    lakebase: { ...DEFAULT_LAKEBASE, ...partial.lakebase },
    secrets: { ...DEFAULT_SECRETS, ...partial.secrets },
    cluster: { ...DEFAULT_CLUSTER, ...partial.cluster },
    warehouse: { ...DEFAULT_WAREHOUSE, ...partial.warehouse },
  };
}

// ---------------------------------------------------------------------------
// Tab definitions
// ---------------------------------------------------------------------------
const TABS = [
  { key: "uc", label: "UC Catalog", icon: Database },
  { key: "lakebase", label: "Lakebase", icon: Server },
  { key: "secrets", label: "Secrets", icon: Shield },
  { key: "cluster", label: "Cluster", icon: Cpu },
  { key: "warehouse", label: "SQL Warehouse", icon: Warehouse },
] as const;

type TabKey = (typeof TABS)[number]["key"];

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------
export function SettingsPage() {
  const [activeTab, setActiveTab] = useState<TabKey>("uc");
  const [settings, setSettings] = useState<UserSettings>(mergeDefaults({}));
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);

  // Cluster dropdown data
  const [nodeTypes, setNodeTypes] = useState<NodeType[]>([]);
  const [sparkVersions, setSparkVersions] = useState<SparkVersion[]>([]);
  const [nodeTypesLoading, setNodeTypesLoading] = useState(false);
  const [sparkVersionsLoading, setSparkVersionsLoading] = useState(false);

  // Validation state (global)
  const [validating, setValidating] = useState(false);
  const [validationResult, setValidationResult] =
    useState<SettingsValidationResult | null>(null);

  // Per-section provision state
  const [provisioningUc, setProvisioningUc] = useState(false);
  const [provisioningLb, setProvisioningLb] = useState(false);
  const [provisioningSec, setProvisioningSec] = useState(false);
  const [ucProvisionResult, setUcProvisionResult] =
    useState<ProvisionedResource[] | null>(null);
  const [lbProvisionResult, setLbProvisionResult] =
    useState<ProvisionedResource[] | null>(null);
  const [secProvisionResult, setSecProvisionResult] =
    useState<ProvisionedResource[] | null>(null);
  const [lbCredentialInfo, setLbCredentialInfo] =
    useState<LakebaseCredentialInfo | null>(null);

  // Settings status (existing resources)
  const [settingsStatus, setSettingsStatus] = useState<SettingsStatus | null>(null);
  const [statusLoading, setStatusLoading] = useState(false);

  // Warehouse data
  const [warehouses, setWarehouses] = useState<WarehouseInfo[]>([]);

  useEffect(() => {
    api
      .getUserSettings()
      .then((partial) => setSettings(mergeDefaults(partial)))
      .catch(() => {})
      .finally(() => setLoading(false));

    setNodeTypesLoading(true);
    api
      .getNodeTypes()
      .then((r) => setNodeTypes(r.node_types.filter((n) => !n.is_deprecated)))
      .catch(() => {})
      .finally(() => setNodeTypesLoading(false));

    setSparkVersionsLoading(true);
    api
      .getSparkVersions()
      .then((r) => setSparkVersions(r.spark_versions))
      .catch(() => {})
      .finally(() => setSparkVersionsLoading(false));

    // Fetch current resource status
    setStatusLoading(true);
    api
      .getSettingsStatus()
      .then((s) => {
        setSettingsStatus(s);
        // Pre-populate warehouse list from status
        if (s.warehouse?.warehouses) setWarehouses(s.warehouse.warehouses);
      })
      .catch(() => {})
      .finally(() => setStatusLoading(false));
  }, []);

  // Immutable updaters
  const updateUc = (patch: Partial<UcSettings>) =>
    setSettings((prev) => ({ ...prev, uc: { ...prev.uc, ...patch } }));
  const updateLakebase = (patch: Partial<LakebaseSettings>) =>
    setSettings((prev) => ({ ...prev, lakebase: { ...prev.lakebase, ...patch } }));
  const updateSecrets = (patch: Partial<SecretsSettings>) =>
    setSettings((prev) => ({ ...prev, secrets: { ...prev.secrets, ...patch } }));
  const updateCluster = (patch: Partial<ClusterSettings>) =>
    setSettings((prev) => ({ ...prev, cluster: { ...prev.cluster, ...patch } }));
  const updateWarehouse = (patch: Partial<WarehouseSettings>) =>
    setSettings((prev) => ({ ...prev, warehouse: { ...prev.warehouse, ...patch } }));

  // ---------------------------------------------------------------------------
  // Handlers
  // ---------------------------------------------------------------------------
  const handleSave = async () => {
    setSaving(true);
    try {
      await api.saveUserSettings(settings);
      toast.success("Global settings saved");
    } catch (err) {
      toast.error(`Failed to save: ${err instanceof Error ? err.message : err}`);
    } finally {
      setSaving(false);
    }
  };

  const handleValidate = async () => {
    setValidating(true);
    try {
      const result = await api.validateSettings(settings);
      setValidationResult(result);
      const allPass = (["uc", "lakebase", "secrets", "cluster"] as const).every(
        (k) => result[k].status === "pass",
      );
      if (allPass) toast.success("All checks passed");
    } catch (err) {
      toast.error(`Validation failed: ${err instanceof Error ? err.message : err}`);
    } finally {
      setValidating(false);
    }
  };

  const handleProvisionUc = async () => {
    setProvisioningUc(true);
    setUcProvisionResult(null);
    try {
      const result = await api.provisionUcSettings(settings.uc);
      setUcProvisionResult(result.provisioned);
      setValidationResult((prev) =>
        prev ? { ...prev, uc: result.validation } : null,
      );
      const failCount = result.provisioned.filter((p) => p.status === "failed").length;
      if (failCount === 0) toast.success("UC resources provisioned");
      else toast.warning(`${failCount} UC resource(s) failed`);
    } catch (err) {
      toast.error(`UC provisioning failed: ${err instanceof Error ? err.message : err}`);
    } finally {
      setProvisioningUc(false);
    }
  };

  const handleProvisionLakebase = async (lbUser?: string, lbPassword?: string) => {
    setProvisioningLb(true);
    setLbProvisionResult(null);
    setLbCredentialInfo(null);
    try {
      const result = await api.provisionLakebaseSettings({
        lakebase: settings.lakebase,
        secrets: settings.secrets,
        ...(lbUser ? { lb_user: lbUser } : {}),
        ...(lbPassword ? { lb_password: lbPassword } : {}),
      });
      setLbProvisionResult(result.provisioned);
      if (result.credentials) setLbCredentialInfo(result.credentials);
      setValidationResult((prev) =>
        prev ? { ...prev, lakebase: result.validation } : null,
      );
      const failCount = result.provisioned.filter((p) => p.status === "failed").length;
      const skipCount = result.provisioned.filter((p) => p.status === "skipped").length;
      if (failCount === 0 && skipCount === 0) toast.success("Lakebase resources provisioned");
      else if (failCount === 0 && skipCount > 0)
        toast.info("Lakebase resources provisioned (password skipped — notebooks use OAuth)");
      else toast.warning(`${failCount} Lakebase resource(s) failed`);
    } catch (err) {
      toast.error(
        `Lakebase provisioning failed: ${err instanceof Error ? err.message : err}`,
      );
    } finally {
      setProvisioningLb(false);
    }
  };

  const handleProvisionSecrets = async () => {
    setProvisioningSec(true);
    setSecProvisionResult(null);
    try {
      const result = await api.provisionSecretsSettings(settings.secrets);
      setSecProvisionResult(result.provisioned);
      setValidationResult((prev) =>
        prev ? { ...prev, secrets: result.validation } : null,
      );
      const failCount = result.provisioned.filter((p) => p.status === "failed").length;
      if (failCount === 0) toast.success("Secrets scope provisioned");
      else toast.warning(`${failCount} secret resource(s) failed`);
    } catch (err) {
      toast.error(
        `Secrets provisioning failed: ${err instanceof Error ? err.message : err}`,
      );
    } finally {
      setProvisioningSec(false);
    }
  };

  // ---------------------------------------------------------------------------
  // Render
  // ---------------------------------------------------------------------------
  if (loading) {
    return (
      <div className="flex items-center justify-center py-20">
        <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
      </div>
    );
  }

  return (
    <div className="mx-auto max-w-4xl space-y-6">
      <div>
        <h1 className="font-display text-2xl font-bold tracking-tight">Global Pipeline Defaults</h1>
        <p className="text-sm text-muted-foreground">
          Configure default resource locations shared across all pipelines. Individual
          pipelines can override these.
        </p>
      </div>

      {/* Tab bar */}
      <div className="flex border-b border-border/50">
        {TABS.map((tab) => (
          <button
            key={tab.key}
            onClick={() => setActiveTab(tab.key)}
            className={cn(
              "flex items-center gap-2 px-4 py-2.5 text-sm transition-colors font-ui",
              activeTab === tab.key
                ? "border-b-2 border-primary text-primary"
                : "text-muted-foreground hover:text-foreground",
            )}
          >
            <tab.icon className="h-4 w-4" />
            {tab.label}
          </button>
        ))}
      </div>

      {/* Tab content */}
      <Card className="glass">
        <CardContent className="space-y-5 pt-6">
          {activeTab === "uc" && (
            <UcSettingsTab
              settings={settings.uc}
              onUpdate={updateUc}
              onProvision={handleProvisionUc}
              provisioning={provisioningUc}
              provisionResult={ucProvisionResult}
              status={settingsStatus?.uc}
              statusLoading={statusLoading}
            />
          )}
          {activeTab === "lakebase" && (
            <LakebaseSettingsTab
              settings={settings.lakebase}
              secretsSettings={settings.secrets}
              onUpdate={updateLakebase}
              onProvision={handleProvisionLakebase}
              provisioning={provisioningLb}
              provisionResult={lbProvisionResult}
              credentialInfo={lbCredentialInfo}
              status={settingsStatus?.lakebase}
              statusLoading={statusLoading}
            />
          )}
          {activeTab === "secrets" && (
            <SecretsSettingsTab
              settings={settings.secrets}
              onUpdate={updateSecrets}
              onProvision={handleProvisionSecrets}
              provisioning={provisioningSec}
              provisionResult={secProvisionResult}
              status={settingsStatus?.secrets}
              statusLoading={statusLoading}
            />
          )}
          {activeTab === "cluster" && (
            <ClusterSettingsTab
              settings={settings.cluster}
              onUpdate={updateCluster}
              nodeTypes={nodeTypes}
              sparkVersions={sparkVersions}
              nodeTypesLoading={nodeTypesLoading}
              sparkVersionsLoading={sparkVersionsLoading}
            />
          )}
          {activeTab === "warehouse" && (
            <WarehouseSettingsTab
              settings={settings.warehouse}
              onUpdate={updateWarehouse}
              warehouses={warehouses}
              warehousesLoading={statusLoading}
              onWarehouseCreated={(wh) =>
                setWarehouses((prev) => [...prev, wh])
              }
            />
          )}
        </CardContent>
      </Card>

      {/* Action buttons — Validate All + Save Defaults */}
      <div className="sticky bottom-0 flex items-center gap-3 justify-end rounded-lg glass-strong px-4 py-3 backdrop-blur-xl">
        <Button
          variant="outline"
          onClick={handleValidate}
          disabled={validating}
          className="gap-2 font-ui"
        >
          {validating ? (
            <Loader2 className="h-4 w-4 animate-spin" />
          ) : (
            <ClipboardCheck className="h-4 w-4" />
          )}
          Validate All
        </Button>
        <Button onClick={handleSave} disabled={saving} className="gap-2 font-ui hover:shadow-[0_0_16px_rgba(255,54,33,0.4)] transition-shadow">
          {saving ? (
            <Loader2 className="h-4 w-4 animate-spin" />
          ) : (
            <Save className="h-4 w-4" />
          )}
          Save Defaults
        </Button>
      </div>

      {/* Validation results */}
      {validationResult && (
        <Card className="glass">
          <CardHeader className="pb-3">
            <CardTitle className="font-display text-sm font-semibold">Validation Results</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2">
            <ValidationSection label="UC Catalog" icon={Database} result={validationResult.uc} />
            <ValidationSection label="Lakebase" icon={Server} result={validationResult.lakebase} />
            <ValidationSection label="Secrets" icon={Shield} result={validationResult.secrets} />
            <ValidationSection label="Cluster" icon={Cpu} result={validationResult.cluster} />
          </CardContent>
        </Card>
      )}
    </div>
  );
}
