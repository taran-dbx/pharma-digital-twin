import type { Page } from "@playwright/test";

export const manufacturingCatalog = {
  is_admin: false,
  industries: [
    {
      id: "manufacturing",
      name: "Manufacturing",
      accent: "#e86247",
      icon: "factory",
      twin_type: "line",
      tag: "Assembly line",
      lede: "Factory",
      kpis: [],
      sensors: [],
      modes: [
        {
          id: "realtime",
          name: "Real-time",
          to: "Lakebase",
          desc: "",
          status: "running",
          pipeline_id: null,
          deployment_id: null,
        },
      ],
      running_count: 1,
    },
  ],
};

export const enabledGenieStatus = {
  enabled: true,
  permission: "CAN_VIEW",
  auth: "app_service_principal",
  capabilities: { can_mutate_alerts: false },
};

export const disabledGenieStatus = {
  enabled: false,
  permission: "CAN_VIEW",
  auth: "app_service_principal",
  reason: "GENIE_SPACE_ID is not configured. Add a genie-space app resource with CAN_VIEW; the runtime injects the id via valueFrom.",
};

export const sampleClearedAlert = {
  id: "alert-press-low",
  fingerprint: "m-press-low",
  industry: "manufacturing",
  pipeline_id: "pipe-mfg",
  rule_id: "press-low",
  sensor_id: "press_1_pressure",
  sensor_ids: ["press_1_pressure"],
  severity: "warn",
  status: "cleared",
  fault_code: "M-PRESS-LOW",
  value: 300,
  threshold: 120,
  opened_at: "2026-10-15T19:00:00Z",
  updated_at: "2026-10-15T19:20:00Z",
  acknowledged_at: null,
  acknowledged_by: null,
  cleared_at: "2026-10-15T19:20:00Z",
};

export async function mockModeDashboardApis(
  page: Page,
  options: {
    genieStatus?: Record<string, unknown>;
    alerts?: Array<Record<string, unknown>>;
  } = {},
) {
  const genieStatus = options.genieStatus ?? enabledGenieStatus;
  const alerts = options.alerts ?? [];

  await page.route("**/api/**", async (route) => {
    const pathname = new URL(route.request().url()).pathname;

    if (pathname === "/api/me") {
      await route.fulfill({ json: { email: "qa@example.com" } });
      return;
    }
    if (pathname === "/api/industries/catalog") {
      await route.fulfill({ json: manufacturingCatalog });
      return;
    }
    if (pathname === "/api/pipelines") {
      await route.fulfill({ json: [] });
      return;
    }
    if (pathname === "/api/industries/runtime") {
      await route.fulfill({ json: { pipelines: [] } });
      return;
    }
    if (pathname === "/api/industries/manufacturing/assets") {
      await route.fulfill({ json: { industry: "manufacturing", sites: [] } });
      return;
    }
    if (pathname === "/api/industries/manufacturing/reference") {
      await route.fulfill({
        json: { industry: "manufacturing", kpis: [], maintenance_history: [] },
      });
      return;
    }
    if (pathname === "/api/alerts" || pathname.startsWith("/api/alerts/")) {
      await route.fulfill({ json: { alerts } });
      return;
    }
    if (pathname === "/api/genie/status") {
      await route.fulfill({ json: genieStatus });
      return;
    }
    if (pathname === "/api/events") {
      await route.fulfill({ status: 204, body: "" });
      return;
    }

    await route.fulfill({
      status: 503,
      contentType: "application/json",
      body: JSON.stringify({ detail: `Unexpected test request: ${pathname}` }),
    });
  });

  await page.route("**/api/genie/status", async (route) => {
    await route.fulfill({ json: genieStatus });
  });
}
