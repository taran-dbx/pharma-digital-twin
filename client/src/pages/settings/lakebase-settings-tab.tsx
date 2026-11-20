import { useState } from "react";
import { Loader2, Wrench, KeyRound, Server, CheckCircle2 } from "lucide-react";
import { CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Separator } from "@/components/ui/separator";
import { InputField, ProvisionResultList } from "./settings-shared";
import { LakehouseSyncStatus } from "./lakehouse-sync-status";
import { LakeTsRetentionDeploy } from "./lakets-retention-deploy";
import type {
  LakebaseSettings,
  SecretsSettings,
  LakebaseConnectionMode,
  ProvisionedResource,
  LakebaseCredentialInfo,
  SettingsStatus,
} from "@/lib/types";

interface LakebaseSettingsTabProps {
  settings: LakebaseSettings;
  secretsSettings: SecretsSettings;
  onUpdate: (patch: Partial<LakebaseSettings>) => void;
  onProvision: (lbUser?: string, lbPassword?: string) => void;
  provisioning: boolean;
  provisionResult: ProvisionedResource[] | null;
  credentialInfo: LakebaseCredentialInfo | null;
  status?: SettingsStatus["lakebase"] | null;
  statusLoading?: boolean;
}

export function LakebaseSettingsTab({
  settings,
  secretsSettings,
  onUpdate,
  onProvision,
  provisioning,
  provisionResult,
  credentialInfo,
  status,
  statusLoading,
}: LakebaseSettingsTabProps) {
  // Direct-mode credentials: local state, not persisted
  const [lbUser, setLbUser] = useState("");
  const [lbPassword, setLbPassword] = useState("");

  const handleProvision = () => {
    if (settings.mode === "direct" && lbUser && lbPassword) {
      onProvision(lbUser, lbPassword);
    } else {
      onProvision();
    }
  };

  return (
    <>
      <CardHeader className="p-0">
        <CardTitle className="font-display text-sm font-semibold">Lakebase Connection</CardTitle>
        <p className="text-xs text-muted-foreground">
          Default PostgreSQL connection for pipeline sink tables.
        </p>
      </CardHeader>
      <Separator />

      {/* Current resources */}
      <CurrentLakebaseResources status={status} loading={statusLoading} />

      {/* Lakehouse Sync readiness */}
      <LakehouseSyncStatus settings={settings} />

      {/* Scheduled retention job deployment */}
      <LakeTsRetentionDeploy settings={settings} />

      {/* Connection mode */}
      <div>
        <label className="mb-2 block font-ui text-xs font-medium uppercase tracking-wider text-muted-foreground">
          Connection Mode
        </label>
        <div className="flex gap-4">
          {(["autoscale", "direct"] as const).map((mode) => (
            <label
              key={mode}
              className="flex items-center gap-2 text-sm cursor-pointer"
            >
              <input
                type="radio"
                name="lb-mode"
                checked={settings.mode === mode}
                onChange={() =>
                  onUpdate({ mode: mode as LakebaseConnectionMode })
                }
                className="h-4 w-4"
              />
              {mode === "autoscale" ? "Autoscale" : "Direct"}
            </label>
          ))}
        </div>
      </div>

      {/* Mode-specific fields */}
      {settings.mode === "autoscale" ? (
        <>
          <div className="grid gap-4 sm:grid-cols-2">
            <InputField
              label="Project ID"
              placeholder="iot-digital-twin"
              value={settings.project_id}
              onChange={(v) => onUpdate({ project_id: v })}
            />
            <InputField
              label="Branch"
              placeholder="production"
              value={settings.branch_id}
              onChange={(v) => onUpdate({ branch_id: v })}
            />
            <InputField
              label="Database"
              placeholder="databricks_postgres"
              value={settings.database}
              onChange={(v) => onUpdate({ database: v })}
            />
            <InputField
              label="PG Schema"
              placeholder="raw"
              value={settings.pg_schema}
              onChange={(v) => onUpdate({ pg_schema: v })}
            />
            <InputField
              label="PG Table Name"
              placeholder="sensor_readings"
              value={settings.pg_table}
              onChange={(v) => onUpdate({ pg_table: v })}
            />
          </div>
          <p className="text-xs text-muted-foreground">
            Provisioning will automatically create the project, branch, a dedicated
            Postgres role, tables, and store credentials as UC secrets in your catalog.
          </p>
        </>
      ) : (
        <>
          <div className="grid gap-4 sm:grid-cols-2">
            <InputField
              label="Host"
              placeholder="your-host.cloud.databricks.com"
              value={settings.host}
              onChange={(v) => onUpdate({ host: v })}
            />
            <InputField
              label="Port"
              placeholder="5432"
              value={settings.port}
              onChange={(v) => onUpdate({ port: v })}
            />
            <InputField
              label="Database"
              placeholder="databricks_postgres"
              value={settings.database}
              onChange={(v) => onUpdate({ database: v })}
            />
            <InputField
              label="PG Schema"
              placeholder="raw"
              value={settings.pg_schema}
              onChange={(v) => onUpdate({ pg_schema: v })}
            />
            <InputField
              label="PG Table Name"
              placeholder="sensor_readings"
              value={settings.pg_table}
              onChange={(v) => onUpdate({ pg_table: v })}
            />
          </div>

          {/* Direct-mode credentials for provisioning */}
          <div className="border-t border-border pt-4 space-y-4">
            <div>
              <p className="text-xs font-medium text-muted-foreground">
                Provisioning Credentials
              </p>
              <p className="text-xs text-muted-foreground">
                Provide username and password to create tables. These will be saved to
                the UC secret scope (<code>{secretsSettings.scope}</code>) so pipelines
                can connect without handling passwords.
              </p>
            </div>
            <div className="grid gap-4 sm:grid-cols-2">
              <InputField
                label="Username"
                placeholder="postgres_user"
                value={lbUser}
                onChange={setLbUser}
              />
              <div>
                <label className="mb-1.5 block font-ui text-xs font-medium uppercase tracking-wider text-muted-foreground">
                  Password
                </label>
                <input
                  type="password"
                  value={lbPassword}
                  onChange={(e) => setLbPassword(e.target.value)}
                  placeholder="••••••••"
                  className="w-full rounded-md border border-border/50 bg-input px-3 py-2 text-sm placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-primary focus:border-primary"
                />
              </div>
            </div>
          </div>
        </>
      )}

      {/* Per-tab Create button */}
      <div className="flex items-center gap-3 pt-2">
        <Button
          variant="outline"
          onClick={handleProvision}
          disabled={provisioning}
          className="gap-2"
        >
          {provisioning ? (
            <Loader2 className="h-4 w-4 animate-spin" />
          ) : (
            <Wrench className="h-4 w-4" />
          )}
          Create Lakebase Resources
        </Button>
      </div>

      {/* Inline provision results */}
      {provisionResult && (
        <div className="rounded-md border border-border/50 glass p-3">
          <p className="mb-2 font-display text-xs font-semibold">Provisioned Resources</p>
          <ProvisionResultList items={provisionResult} />
        </div>
      )}

      {/* Credential info card */}
      {credentialInfo && (
        <div className="rounded-md border border-border/50 glass p-4 space-y-2">
          <div className="flex items-center gap-2 text-sm font-ui font-medium text-success">
            <KeyRound className="h-4 w-4" />
            Credentials Configured
          </div>
          <div className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-1 text-xs">
            <span className="font-medium text-muted-foreground">Postgres role</span>
            <code className="text-foreground">{credentialInfo.pg_role}</code>
            <span className="font-medium text-muted-foreground">Secret scope</span>
            <code className="text-foreground">{credentialInfo.secret_scope}</code>
            <span className="font-medium text-muted-foreground">Username key</span>
            <code className="text-foreground">{credentialInfo.user_key}</code>
            <span className="font-medium text-muted-foreground">Password key</span>
            <code className="text-foreground">{credentialInfo.password_key}</code>
          </div>
          <p className="text-xs text-muted-foreground">
            Pipelines will use{" "}
            <code>dbutils.secrets.get(&quot;{credentialInfo.secret_scope}&quot;, &quot;{credentialInfo.user_key}&quot;)</code>{" "}
            at runtime — no passwords needed in configuration.
          </p>
        </div>
      )}
    </>
  );
}

function CurrentLakebaseResources({
  status,
  loading,
}: {
  status?: SettingsStatus["lakebase"] | null;
  loading?: boolean;
}) {
  if (loading) {
    return (
      <div className="flex items-center gap-2 rounded-md border border-border/50 glass px-3 py-2 text-xs text-muted-foreground">
        <Loader2 className="h-3.5 w-3.5 animate-spin" />
        Loading current resources...
      </div>
    );
  }

  if (!status) return null;
  const hasAnything = status.endpoint_host || status.tables.length > 0 || status.role_exists;
  if (!hasAnything) return null;

  return (
    <div className="rounded-md border border-border/50 glass p-3 space-y-1.5">
      <div className="flex items-center gap-2 text-xs font-ui font-medium">
        <Server className="h-3.5 w-3.5 text-primary" />
        Current Lakebase Resources
      </div>
      <div className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-1 text-xs">
        {status.project_id && (
          <>
            <span className="text-muted-foreground">Project</span>
            <code>{status.project_id}</code>
          </>
        )}
        {status.branch_id && (
          <>
            <span className="text-muted-foreground">Branch</span>
            <code>{status.branch_id}</code>
          </>
        )}
        {status.endpoint_host && (
          <>
            <span className="text-muted-foreground">Endpoint</span>
            <code>{status.endpoint_host}</code>
          </>
        )}
        {status.role_exists && status.role_name && (
          <>
            <span className="text-muted-foreground">PG Role</span>
            <span className="flex items-center gap-1">
              <CheckCircle2 className="h-3 w-3 text-success" />
              <code>{status.role_name}</code>
            </span>
          </>
        )}
        {status.tables.length > 0 && (
          <>
            <span className="text-muted-foreground">Tables</span>
            <code>{status.tables.join(", ")}</code>
          </>
        )}
      </div>
    </div>
  );
}
