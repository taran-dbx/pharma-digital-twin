import { expect, test } from "@playwright/test";

const MODES = ["realtime", "microbatch", "zerobus"] as const;

test.describe("[Windsor] Manufacturing dashboard twin", () => {
  test.setTimeout(90_000);

  test.beforeEach(async ({ page }) => {
    await page.route("**/api/**", (route) => {
      const path = new URL(route.request().url()).pathname;

      if (path === "/api/industries/catalog") {
        return route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify({
            is_admin: false,
            industries: [
              {
                id: "manufacturing",
                name: "Manufacturing",
                accent: "#e86247",
                icon: "factory",
                twin_type: "line",
                tag: "Assembly line · CNC · robotics",
                lede: "Real-time visibility into the production line.",
                kpis: [],
                sensors: [],
                modes: MODES.map((mode) => ({
                  id: mode,
                  name: mode,
                  to: mode === "realtime" ? "Lakebase" : "Delta",
                  desc: "",
                  status: "running",
                  pipeline_id: null,
                  deployment_id: `manufacturing-${mode}`,
                })),
                running_count: MODES.length,
              },
            ],
          }),
        });
      }

      if (path === "/api/pipelines") {
        return route.fulfill({
          status: 200,
          contentType: "application/json",
          body: "[]",
        });
      }

      if (path.endsWith("/lakebase-health")) {
        return route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify({
            rows_5m: 0,
            latest_at: null,
            latency_sec: 0,
            inserts_per_sec_recent: 0,
          }),
        });
      }

      if (path.endsWith("/zerobus-metrics")) {
        return route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify({
            table_name: "catalog.schema.manufacturing_zerobus",
            total_records: 0,
            total_bytes: 0,
            records_per_sec: 0,
            records_per_min: 0,
            last_commit_time: null,
            stream_status: "none",
            error_count: 0,
            series: [],
            recent_errors: [],
            latest_readings: [],
          }),
        });
      }

      if (path === "/api/latency/zerobus") {
        return route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify({
            table_name: "catalog.schema.manufacturing_zerobus",
            has_data: false,
            last_commit_time: null,
            seconds_since_commit: null,
            committed_records_total: 0,
            committed_bytes_total: 0,
            records_per_min: 0,
            recent_error_count: 0,
            recent_errors: [],
          }),
        });
      }

      return route.fulfill({
        status: 200,
        contentType: "application/json",
        body: "{}",
      });
    });
  });

  test("renders the 3D machining cell in every pipeline mode [C7301][smoke][regression]", async ({
    page,
  }, testInfo) => {
    for (const mode of MODES) {
      await page.goto(`/i/manufacturing/${mode}/dashboard`);

      const cell = page.getByRole("region", {
        name: "Machining cell twin: mill, inspect, pack",
      });
      await expect(cell).toBeVisible();
      await expect(cell.locator("canvas")).toBeVisible();
      await expect(
        cell.getByRole("button", { name: "Select MILL · Spindle 3" }),
      ).toBeVisible();

      if (mode === "realtime") {
        await expect(page.getByText("Manufacturing · SSE offline")).toBeVisible();
      }

      await cell.screenshot({
        path: testInfo.outputPath(`manufacturing-${mode}-cell.png`),
      });
    }
  });
});
