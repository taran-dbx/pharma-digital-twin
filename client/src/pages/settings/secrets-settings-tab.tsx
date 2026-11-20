import { Loader2, Wrench, Shield, CheckCircle2 } from "lucide-react";
import { CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Separator } from "@/components/ui/separator";
import { InputField, ProvisionResultList } from "./settings-shared";
import type { SecretsSettings, ProvisionedResource, SettingsStatus } from "@/lib/types";

const SECRET_FIELDS: Array<{
  key: keyof SecretsSettings;
  label: string;
  placeholder: string;
  description: string;
}> = [
  {
    key: "lb_user_key",
    label: "Lakebase User Key",
    placeholder: "lb-user",
    description:
      "Lakebase PostgreSQL username for streaming notebook connections",
  },
  {
    key: "lb_password_key",
    label: "Lakebase Password Key",
    placeholder: "lb-password",
    description:
      "Lakebase PostgreSQL password for streaming notebook connections",
  },
  {
    key: "zb_client_id_key",
    label: "ZeroBus Client ID Key",
    placeholder: "zerobus-client-id",
    description: "ZeroBus OAuth client ID for real-time data streaming",
  },
  {
    key: "zb_client_secret_key",
    label: "ZeroBus Client Secret Key",
    placeholder: "zerobus-client-secret",
    description: "ZeroBus OAuth client secret for real-time data streaming",
  },
];

interface SecretsSettingsTabProps {
  settings: SecretsSettings;
  onUpdate: (patch: Partial<SecretsSettings>) => void;
  onProvision: () => void;
  provisioning: boolean;
  provisionResult: ProvisionedResource[] | null;
  status?: SettingsStatus["secrets"] | null;
  statusLoading?: boolean;
}

export function SecretsSettingsTab({
  settings,
  onUpdate,
  onProvision,
  provisioning,
  provisionResult,
  status,
  statusLoading,
}: SecretsSettingsTabProps) {
  return (
    <>
      <CardHeader className="p-0">
        <CardTitle className="font-display text-sm font-semibold">Secrets Scope</CardTitle>
        <p className="text-xs text-muted-foreground">
          UC secret scope and key names used for credentials. These keys store
          sensitive values that streaming notebooks reference at runtime.
        </p>
      </CardHeader>
      <Separator />

      {/* Current resources */}
      {statusLoading && (
        <div className="flex items-center gap-2 rounded-md border border-border/50 glass px-3 py-2 text-xs text-muted-foreground">
          <Loader2 className="h-3.5 w-3.5 animate-spin" />
          Loading current resources...
        </div>
      )}
      {!statusLoading && status && status.scope_exists && (
        <div className="rounded-md border border-border/50 glass p-3 space-y-1.5">
          <div className="flex items-center gap-2 text-xs font-ui font-medium">
            <Shield className="h-3.5 w-3.5 text-primary" />
            Current Secrets
          </div>
          <div className="flex items-center gap-1 text-xs">
            <CheckCircle2 className="h-3 w-3 text-success" />
            <span>Scope <code>{status.scope_name}</code> exists</span>
          </div>
          {status.keys.length > 0 && (
            <div className="text-xs text-muted-foreground">
              Keys: {status.keys.map((k) => (
                <code key={k} className="mx-0.5 rounded bg-muted px-1 py-0.5">{k}</code>
              ))}
            </div>
          )}
        </div>
      )}

      <InputField
        label="Scope Name"
        placeholder="iot-secrets"
        value={settings.scope}
        onChange={(v) => onUpdate({ scope: v })}
        description="The Databricks secret scope that holds all credential keys below"
      />

      <div className="grid gap-4 sm:grid-cols-2">
        {SECRET_FIELDS.map((field) => (
          <InputField
            key={field.key}
            label={field.label}
            placeholder={field.placeholder}
            value={settings[field.key]}
            onChange={(v) => onUpdate({ [field.key]: v })}
            description={field.description}
          />
        ))}
      </div>

      {/* Per-tab Create button */}
      <div className="flex items-center gap-3 pt-2">
        <Button
          variant="outline"
          onClick={onProvision}
          disabled={provisioning}
          className="gap-2"
        >
          {provisioning ? (
            <Loader2 className="h-4 w-4 animate-spin" />
          ) : (
            <Wrench className="h-4 w-4" />
          )}
          Create Secret Scope
        </Button>
      </div>

      {/* Inline provision results */}
      {provisionResult && (
        <div className="rounded-md border border-border/50 glass p-3">
          <p className="mb-2 font-display text-xs font-semibold">Provisioned Resources</p>
          <ProvisionResultList items={provisionResult} />
        </div>
      )}
    </>
  );
}
