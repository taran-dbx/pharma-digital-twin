import { expect, test } from "../fixtures/test";

const setupId = "11111111-1111-4111-8111-111111111111";

test.describe("[Windsor] Responsive admin surfaces", () => {
  test.use({ viewport: { width: 390, height: 844 } });

  test("keeps setup content within the mobile panel [C7305][defect][regression]", async ({
    page,
  }, testInfo) => {
    await page.goto("/setup-validate");
    await expect(
      page.getByRole("heading", { name: "Pipeline Creation & Validation" }),
    ).toBeVisible();
    await expect(page.getByText("Configuration Summary")).toBeVisible();

    const dimensions = await page.locator(".main-panel").evaluate((panel) => ({
      clientWidth: panel.clientWidth,
      scrollWidth: panel.scrollWidth,
    }));
    expect(dimensions.scrollWidth).toBeLessThanOrEqual(dimensions.clientWidth);

    await testInfo.attach("setup-mobile-layout", {
      body: JSON.stringify(dimensions, null, 2),
      contentType: "application/json",
    });
  });

  test("contains long resource identifiers on mobile [C7306][defect][regression]", async ({
    page,
  }) => {
    const longIdentifier =
      "example_eu_catalog.rtm_lakets_zb.manufacturing_realtime_" +
      "11111111-1111-4111-8111-111111111111";
    let resolvedHealthRequests = 0;
    await page.route("**/api/admin/pipelines", (route) =>
      route.fulfill({
        json: {
          pipelines: [
            {
              setup: { id: setupId, industry: "manufacturing" },
              deployments: [],
              health: { enabled_modes: [] },
              status: "stopped",
              modes: [],
            },
          ],
        },
      }),
    );
    await page.route(`**/api/admin/pipelines/${setupId}/health`, (route) => {
      resolvedHealthRequests += 1;
      return route.fulfill({
        json: {
          uc_catalog: "example_eu_catalog",
          uc_schema: "rtm_lakets_zb",
          app_sp_client_id: setupId,
          setup: {
            id: setupId,
            industry: "manufacturing",
            status: "deployed",
            flavor: "classic",
            source_type: "kafka",
            bootstrap_servers: null,
            enabled_modes: [],
            warehouse_id: null,
            user_email: null,
            created_at: null,
            updated_at: null,
          },
          deployments: [],
          resources: {
            provisioned_resource_with_a_long_group_name: [
              {
                id: setupId,
                identifier: longIdentifier,
                parent_ref: null,
                teardown_order: 10,
                status: "active",
                health: "healthy",
                metadata: {},
              },
            ],
          },
          checks: [],
          cluster_state: null,
          e2e_test: {
            tier: "e2e",
            available: false,
            spins_interactive_cluster: false,
            eta_minutes: 0,
            description: "Unavailable",
            start_endpoint: "",
            state_endpoint: "",
          },
          health: "healthy",
          startable: true,
          ready: true,
          validation_passed: true,
          modes: [],
          status: "stopped",
        },
      });
    });

    await page.goto("/pipelines/manufacturing/health");
    await expect(page.getByText("Resource ledger", { exact: true })).toBeVisible();
    await expect(page.getByTitle(longIdentifier)).toBeVisible();
    expect(resolvedHealthRequests).toBe(1);
    const dimensions = await page.locator(".main-panel").evaluate((panel) => ({
      clientWidth: panel.clientWidth,
      scrollWidth: panel.scrollWidth,
    }));
    expect(dimensions.scrollWidth).toBeLessThanOrEqual(dimensions.clientWidth);
  });

  test("renders missing health counters as zero [C7307][defect][regression]", async ({
    page,
  }) => {
    await page.route("**/api/admin/pipelines", (route) =>
      route.fulfill({
        json: {
          pipelines: [
            {
              setup: {
                id: setupId,
                industry: "manufacturing",
                status: "deployed",
                flavor: "classic",
                source_type: "kafka",
                bootstrap_servers: null,
                enabled_modes: [],
                warehouse_id: null,
                user_email: null,
                created_at: null,
                updated_at: null,
              },
              deployments: [],
              health: {
                overall: "healthy",
                enabled_modes: [],
                deployment_count: 0,
              },
              status: "stopped",
              modes: [],
            },
          ],
        },
      }),
    );

    await page.goto("/pipelines");
    await expect(page.getByText("0/0 healthy", { exact: true })).toBeVisible();
    await expect(page.getByText(/undefined\/undefined/)).toHaveCount(0);
  });

  test("uses separate links instead of nested card controls [C7308][accessibility][regression]", async ({
    page,
  }) => {
    await page.route("**/api/industries/catalog", (route) =>
      route.fulfill({
        json: {
          is_admin: true,
          industries: [
            {
              id: "manufacturing",
              name: "Manufacturing",
              accent: "#e86247",
              icon: "factory",
              twin_type: "line",
              tag: "Assembly line",
              lede: "Factory",
              modes: [
                {
                  id: "realtime",
                  name: "Real-time",
                  to: "Lakebase",
                  desc: "",
                  status: "running",
                  pipeline_id: null,
                  deployment_id: "deployment-1",
                },
              ],
              running_count: 1,
            },
          ],
        },
      }),
    );
    await page.route("**/api/industries/runtime", (route) =>
      route.fulfill({
        json: {
          pipelines: [
            {
              setup: { id: setupId, industry: "manufacturing" },
              deployments: [],
              health: { enabled_modes: ["realtime"] },
              status: "running",
              modes: [
                {
                  mode: "realtime",
                  status: "running",
                  health: "healthy",
                  startable: true,
                },
              ],
            },
          ],
        },
      }),
    );

    await page.goto("/industries");
    const openCard = page.getByRole("link", { name: "Open Manufacturing dashboards" });
    await expect(openCard).toBeVisible();
    await expect(page.locator(".card[role='button']")).toHaveCount(0);
    await expect(openCard.locator("a")).toHaveCount(0);
    await expect(page.getByRole("link", { name: "Config" })).toBeVisible();
    await expect(page.getByRole("link", { name: "Status" })).toBeVisible();
  });
});
