import { expect, test, type Page } from "@playwright/test";

/** UC service credential *name* returned by the mocked config (an identifier, not a secret). */
const UC_SC_NAME = "iot-rtm-sc";

async function selectZerobusOnly(page: Page) {
  await page.getByRole("button", { name: /Real-time/ }).click();
  await page.getByRole("button", { name: /Micro-batch/ }).click();
}

test.describe("[Windsor] Mode-adaptive pipeline setup", () => {
  test.beforeEach(async ({ page }) => {
    await page.route("**/api/**", (route) =>
      route.fulfill({
        status: 503,
        contentType: "application/json",
        body: JSON.stringify({ detail: "Backend intentionally unavailable in UI test" }),
      }),
    );
  });

  test("zerobus-only flow hides Kafka and cluster content [C7101][defect][regression]", async ({
    page,
  }, testInfo) => {
    await page.goto("/setup-validate");
    await selectZerobusOnly(page);

    await expect(page.getByText("Modes", { exact: true }).locator("..")).toContainText("Zerobus");
    await expect(page.getByText("ARCHITECT'S TIP")).toBeVisible();
    await expect(
      page.locator("p").filter({ hasText: "Zerobus is a serverless Delta Ingest path" }),
    ).toBeVisible();
    await expect(page.getByText("Source", { exact: true })).toHaveCount(0);

    await page.screenshot({
      path: testInfo.outputPath("zerobus-only-selection.png"),
      fullPage: true,
    });

    await page.getByRole("button", { name: /Next: Setup Preview/ }).click();
    await expect(page.getByText("Zerobus target")).toBeVisible();
    await expect(page.getByText(/Compute — interactive validation cluster/)).toHaveCount(0);
    await expect(page.getByText("Kafka topics")).toHaveCount(0);
    await expect(page.getByText("Real-time targets")).toHaveCount(0);

    await page.getByRole("button", { name: /Next: Validation/ }).click();
    await expect(page.getByRole("heading", { name: "Zerobus" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "Common" })).toHaveCount(0);
    await expect(page.getByText("Interactive cluster", { exact: true })).toHaveCount(0);

    await page.getByRole("button", { name: /Next: Metrics/ }).click();
    await expect(page.getByText("system.lakeflow.zerobus_ingest").first()).toBeVisible();

    await page.getByRole("button", { name: /Next: Notebook/ }).click();
    await expect(page.getByText("Zerobus producer", { exact: true })).toBeVisible();
    await expect(page.getByText("Final consumer notebook")).toHaveCount(0);
  });

  test("failed validation row shows its backend reason [C7211][defect][regression]", async ({
    page,
  }, testInfo) => {
    const failedState = {
      version: 1,
      enabled_modes: ["realtime"],
      sequence: [
        "provision_cluster",
        "source_connectivity",
        "topics_created",
        "schema_conformance",
        "realtime:partitions_precreate",
        "realtime:write_test",
      ],
      steps: {
        provision_cluster: { status: "passed", error: null, updated_at: "2026-08-31T15:00:00Z" },
        source_connectivity: { status: "passed", error: null, updated_at: "2026-08-31T15:01:00Z" },
        topics_created: { status: "passed", error: null, updated_at: "2026-08-31T15:02:00Z" },
        schema_conformance: { status: "passed", error: null, updated_at: "2026-08-31T15:03:00Z" },
        "realtime:partitions_precreate": {
          status: "passed",
          error: null,
          updated_at: "2026-08-31T15:04:00Z",
        },
        "realtime:write_test": {
          status: "failed",
          error: "Lakebase insert rejected: no partition found for event_time",
          updated_at: "2026-08-31T15:05:00Z",
        },
      },
      cluster_id: "0831-validation",
      cluster_state: "RUNNING",
      run_ids: {},
    };
    const setup = {
      id: "setup-pharma",
      industry: "pharmaceutical",
      status: "draft",
      flavor: "classic",
      source_type: "kafka",
      bootstrap_servers: "broker:9198",
      enabled_modes: ["realtime"],
      mode_configs: { realtime: { rate_hz: 2000 } },
      warehouse_id: "warehouse-1",
      validation_state: failedState,
    };

    await page.route("**/api/admin/pipelines", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          pipelines: [{
            setup,
            deployments: [],
            modes: [{ mode: "realtime", status: "draft", health: "draft", startable: false }],
            health: {
              overall: "draft",
              resources_healthy: 0,
              resources_total: 0,
              jobs_running: 0,
              jobs_total: 0,
              enabled_modes: ["realtime"],
              deployment_count: 0,
            },
          }],
        }),
      }),
    );
    await page.route(
      "**/api/admin/industries/pharmaceutical/validate/state",
      (route) =>
        route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify({ industry: "pharmaceutical", validation_state: failedState }),
        }),
    );
    await page.route("**/api/app-defaults", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ warehouse_id: "warehouse-1" }),
      }),
    );
    await page.route("**/api/admin/industries/pharmaceutical/setup", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(
          route.request().method() === "GET"
            ? { setup, deployments: [], pipeline: {} }
            : { created: false, setup },
        ),
      }),
    );
    await page.route("**/api/admin/pipelines/setup-pharma/modes", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ setup, added: [], removed: [], retained: ["realtime"] }),
      }),
    );
    await page.route("**/api/admin/industries/pharmaceutical/provision", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ setup_id: setup.id, summary: {}, resources: [] }),
      }),
    );
    await page.route("**/api/admin/industries/pharmaceutical/validate/start", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ industry: "pharmaceutical", validation_state: failedState }),
      }),
    );
    await page.route(
      "**/api/admin/industries/pharmaceutical/validate/can-deploy",
      (route) =>
        route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify({
            industry: "pharmaceutical",
            can_deploy: false,
            data_purged: false,
            outstanding: ["realtime:write_test"],
            modes: {
              realtime: {
                can_deploy: false,
                data_purged: false,
                outstanding: ["realtime:write_test"],
              },
            },
          }),
        }),
    );

    await page.goto("/setup-validate?industry=pharmaceutical&step=3");

    const writeTest = page.locator("div.flex.items-start.gap-3.py-2").filter({
      hasText: "Write test",
    });
    const failureReason = writeTest.getByRole("alert");
    await expect(failureReason).toContainText(
      "Lakebase insert rejected: no partition found for event_time",
    );
    await expect(writeTest.getByRole("button", { name: "Retry Write test" })).toBeEnabled();
    await writeTest.screenshot({
      path: testInfo.outputPath("validation-step-failure-reason.png"),
    });
  });

  test("mixed modes expose both metrics sources and notebook tabs [C7102][smoke][regression]", async ({
    page,
  }) => {
    await page.goto("/setup-validate");

    await page.getByRole("button", { name: /Next: Source/ }).click();
    await page.getByRole("button", { name: /Next: Setup Preview/ }).click();
    await page.getByRole("button", { name: /Next: Validation/ }).click();
    await page.getByRole("button", { name: /Next: Metrics/ }).click();

    await expect(page.getByText("Spark streaming metrics")).toBeVisible();
    await expect(page.getByText("Zerobus ingest metrics")).toBeVisible();

    await page.getByRole("button", { name: /Next: Notebook/ }).click();
    await expect(page.getByRole("tab", { name: /Real-time consumer/ })).toBeVisible();
    await expect(page.getByRole("tab", { name: /Micro-batch consumer/ })).toBeVisible();
    await expect(page.getByRole("tab", { name: /Zerobus producer/ })).toBeVisible();
  });

  test("edit flow hydrates saved enabled modes after reload [C7103][defect][regression]", async ({
    page,
  }) => {
    await page.route("**/api/admin/pipelines", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          pipelines: [
            {
              setup: {
                id: "setup-pharma",
                industry: "pharmaceutical",
                status: "deployed",
                flavor: "classic",
                source_type: null,
                bootstrap_servers: null,
                enabled_modes: ["zerobus"],
              },
              deployments: [
                {
                  id: "deployment-zb",
                  setup_id: "setup-pharma",
                  mode: "zerobus",
                  status: "deployed",
                  job_id: "901",
                },
              ],
              health: {
                overall: "healthy",
                resources_healthy: 1,
                resources_total: 1,
                jobs_running: 1,
                jobs_total: 1,
                enabled_modes: ["zerobus"],
                deployment_count: 1,
              },
            },
          ],
        }),
      }),
    );

    await page.goto("/setup-validate?industry=pharmaceutical");
    await expect(page.getByRole("button", { name: /Next: Setup Preview/ })).toBeVisible();
    await expect(page.getByText("Modes", { exact: true }).locator("..")).not.toContainText(
      "Real-time",
    );
    await expect(page.getByText("Source", { exact: true })).toHaveCount(0);
    await page.reload();
    await expect(page.getByRole("button", { name: /Next: Setup Preview/ })).toBeVisible();
    await expect(page.getByText("Modes", { exact: true }).locator("..")).not.toContainText(
      "Micro-batch",
    );
  });

  test("zerobus validation persists the default SQL warehouse [C7114][defect][regression]", async ({
    page,
  }, testInfo) => {
    let savedBody: Record<string, unknown> | null = null;
    await page.route("**/api/admin/pipelines", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ pipelines: [] }),
      }),
    );
    await page.route("**/api/app-defaults", async (route) => {
      await new Promise((resolve) => setTimeout(resolve, 500));
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ warehouse_id: "warehouse-from-app-defaults" }),
      });
    });
    await page.route("**/api/admin/industries/pharmaceutical/setup", async (route) => {
      if (route.request().method() === "GET") {
        await route.fulfill({
          status: 404,
          contentType: "application/json",
          body: JSON.stringify({ detail: "not configured" }),
        });
        return;
      }
      savedBody = route.request().postDataJSON() as Record<string, unknown>;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          created: true,
          setup: {
            id: "setup-pharma",
            industry: "pharmaceutical",
            enabled_modes: ["zerobus"],
            warehouse_id: savedBody.warehouse_id,
          },
        }),
      });
    });
    await page.route("**/api/admin/industries/pharmaceutical/provision", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ setup_id: "setup-pharma", summary: {}, resources: [] }),
      }),
    );
    await page.route("**/api/admin/industries/pharmaceutical/validate/start", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          industry: "pharmaceutical",
          validation_state: {
            version: 1,
            enabled_modes: ["zerobus"],
            sequence: ["zerobus:create_table"],
            steps: {
              "zerobus:create_table": { status: "running", error: null, updated_at: null },
            },
            cluster_id: null,
            run_ids: {},
          },
        }),
      }),
    );

    await page.goto("/setup-validate");
    await selectZerobusOnly(page);
    await page.getByRole("button", { name: /Next: Setup Preview/ }).click();
    await page.getByRole("button", { name: /Next: Validation/ }).click();

    await expect.poll(() => savedBody?.warehouse_id).toBe("warehouse-from-app-defaults");
    await expect(page.getByText(/missing: warehouse_id/)).toHaveCount(0);
    await page.screenshot({
      path: testInfo.outputPath("zerobus-default-warehouse-validation.png"),
      fullPage: true,
    });
  });

  test("admin sets a separate frequency for every selected mode [C7108][smoke][regression]", async ({
    page,
  }, testInfo) => {
    let savedBody: Record<string, unknown> | null = null;
    await page.route("**/api/admin/pipelines", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ pipelines: [] }),
      }),
    );
    await page.route("**/api/admin/industries/pharmaceutical/setup", async (route) => {
      if (route.request().method() === "GET") {
        await route.fulfill({
          status: 404,
          contentType: "application/json",
          body: JSON.stringify({ detail: "not configured" }),
        });
        return;
      }
      savedBody = route.request().postDataJSON() as Record<string, unknown>;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          created: true,
          setup: {
            id: "setup-pharma",
            industry: "pharmaceutical",
            enabled_modes: ["realtime", "microbatch", "zerobus"],
            mode_configs: (savedBody as { mode_configs?: unknown }).mode_configs,
          },
        }),
      });
    });

    await page.goto("/setup-validate");
    const realtimeFrequency = page.getByRole("slider", {
      name: "Real-time data frequency",
    });
    const microbatchFrequency = page.getByRole("slider", {
      name: "Micro-batch data frequency",
    });
    const zerobusFrequency = page.getByRole("slider", {
      name: "Zerobus data frequency",
    });

    await expect(realtimeFrequency).toHaveValue("2000");
    await expect(microbatchFrequency).toHaveValue("2000");
    await expect(zerobusFrequency).toHaveValue("2000");
    await realtimeFrequency.fill("1200");
    await zerobusFrequency.fill("3200");
    await page.getByRole("button", { name: "Save configuration" }).click();

    await expect.poll(() => savedBody).not.toBeNull();
    expect(savedBody).toMatchObject({
      mode_configs: {
        realtime: { rate_hz: 1200 },
        microbatch: { rate_hz: 2000 },
        zerobus: { rate_hz: 3200 },
      },
    });
    await page.screenshot({
      path: testInfo.outputPath("per-mode-initial-frequency.png"),
      fullPage: true,
    });
  });

  test("admin wizard locks frequency for a running mode [C7110][defect][regression]", async ({
    page,
  }) => {
    await page.route("**/api/admin/pipelines", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          pipelines: [{
            setup: {
              id: "setup-pharma",
              industry: "pharmaceutical",
              status: "deployed",
              flavor: "classic",
              source_type: "kafka",
              bootstrap_servers: "broker:9098",
              enabled_modes: ["realtime", "zerobus"],
              mode_configs: {
                realtime: { rate_hz: 1200 },
                zerobus: { rate_hz: 3200 },
              },
            },
            deployments: [
              { id: "dep-rt", setup_id: "setup-pharma", mode: "realtime", status: "deployed", job_id: "11" },
              { id: "dep-zb", setup_id: "setup-pharma", mode: "zerobus", status: "deployed", job_id: "13" },
            ],
            modes: [
              { mode: "realtime", status: "stopped", health: "healthy", startable: true },
              { mode: "zerobus", status: "running", health: "healthy", startable: false },
            ],
            health: {
              overall: "healthy",
              resources_healthy: 2,
              resources_total: 2,
              jobs_running: 1,
              jobs_total: 2,
              enabled_modes: ["realtime", "zerobus"],
              deployment_count: 2,
            },
          }],
        }),
      }),
    );

    await page.goto("/setup-validate?industry=pharmaceutical");
    await expect(
      page.getByRole("slider", { name: "Real-time data frequency" }),
    ).toBeEnabled();
    await expect(
      page.getByRole("slider", { name: "Zerobus data frequency" }),
    ).toBeDisabled();
    await expect(page.getByText("Stop this mode before changing its frequency")).toBeVisible();
  });

  test("mode cards lock running frequency and show topics only for Kafka [C7109][defect][regression]", async ({
    page,
  }) => {
    let configBody: Record<string, unknown> | null = null;
    let deployBody: Record<string, unknown> | null = null;
    let persistedMicrobatchRate = 2000;
    await page.route("**/api/industries/catalog", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          is_admin: true,
          industries: [{
            id: "pharmaceutical",
            name: "Pharmaceutical",
            accent: "#2db0a0",
            icon: "pharma",
            twin_type: "vessel",
            tag: "Bioreactor",
            lede: "Process integrity",
            modes: [
              { id: "realtime", name: "Real-time", to: "Lakebase", desc: "", status: "idle", pipeline_id: null },
              { id: "microbatch", name: "Micro-batch", to: "Delta", desc: "", status: "idle", pipeline_id: null },
              { id: "zerobus", name: "Zerobus", to: "Delta", desc: "", status: "idle", pipeline_id: null },
            ],
            running_count: 0,
          }],
        }),
      }),
    );
    await page.route("**/api/admin/pipelines", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          pipelines: [{
            setup: {
              id: "setup-pharma",
              industry: "pharmaceutical",
              enabled_modes: ["realtime", "microbatch", "zerobus"],
              bootstrap_servers: "broker:9098",
              mode_configs: {
                realtime: { rate_hz: 1000 },
                microbatch: { rate_hz: persistedMicrobatchRate },
                zerobus: { rate_hz: 3000 },
              },
            },
            deployments: [
              { id: "dep-rt", setup_id: "setup-pharma", mode: "realtime", status: "deployed", job_id: "11" },
              { id: "dep-mb", setup_id: "setup-pharma", mode: "microbatch", status: "deployed", job_id: "12" },
              { id: "dep-zb", setup_id: "setup-pharma", mode: "zerobus", status: "deployed", job_id: "13" },
            ],
            modes: [
              { mode: "realtime", status: "running", health: "healthy", startable: false },
              { mode: "microbatch", status: "stopped", health: "healthy", startable: true },
              { mode: "zerobus", status: "stopped", health: "healthy", startable: true },
            ],
            health: {
              overall: "healthy",
              resources_healthy: 3,
              resources_total: 3,
              jobs_running: 1,
              jobs_total: 3,
              enabled_modes: ["realtime", "microbatch", "zerobus"],
              deployment_count: 3,
            },
          }],
        }),
      }),
    );
    await page.route(
      "**/api/admin/pipelines/setup-pharma/modes/microbatch/config",
      async (route) => {
        configBody = route.request().postDataJSON() as Record<string, unknown>;
        await new Promise((resolve) => setTimeout(resolve, 300));
        persistedMicrobatchRate = Number(configBody.rate_hz);
        await route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify({
            mode: "microbatch",
            mode_config: configBody,
            applied: "on next start",
          }),
        });
      },
    );
    await page.route("**/api/app-defaults", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          service_credential: UC_SC_NAME,
          pipelines_project: "pipeline-project",
          uc_catalog: "catalog",
          uc_schema: "schema",
          msk_bootstrap: "broker:9098",
        }),
      }),
    );
    await page.route("**/api/test-connectivity/upload-wheel", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ wheel_path: "/Workspace/wheels/simulator.whl" }),
      }),
    );
    await page.route(
      "**/api/admin/pipelines/setup-pharma/deploy/microbatch",
      async (route) => {
        deployBody = route.request().postDataJSON() as Record<string, unknown>;
        await route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify({ job_id: "job-mb", mode: "microbatch" }),
        });
      },
    );

    await page.goto("/i/pharmaceutical");
    const realtimeCard = page.locator(".fcard").filter({ hasText: "Real-time mode" });
    const microbatchCard = page.locator(".fcard").filter({ hasText: "Micro-batch mode" });
    const zerobusCard = page.locator(".fcard").filter({ hasText: "Zerobus" });

    await expect(
      realtimeCard.getByRole("slider", { name: "Real-time data frequency" }),
    ).toBeDisabled();
    await expect(
      microbatchCard.getByRole("slider", { name: "Micro-batch data frequency" }),
    ).toBeEnabled();
    await expect(
      zerobusCard.getByRole("slider", { name: "Zerobus data frequency" }),
    ).toBeEnabled();
    await expect(realtimeCard.getByText("iot.pharmaceutical.realtime")).toBeVisible();
    await expect(microbatchCard.getByText("iot.pharmaceutical.microbatch")).toBeVisible();
    await expect(zerobusCard.getByText(/iot\.pharmaceutical/)).toHaveCount(0);
    await expect(page.locator(".ctlbar").getByText("Data frequency")).toHaveCount(0);
    await zerobusCard.getByRole("button", { name: "View sample" }).click();
    await expect(page.getByText(/published to iot\.pharmaceutical/)).toHaveCount(0);
    await page.keyboard.press("Escape");

    const microbatchFrequency = microbatchCard.getByRole("slider", {
      name: "Micro-batch data frequency",
    });
    await microbatchFrequency.fill("2400");
    await microbatchFrequency.dispatchEvent("mouseup");
    await microbatchCard.getByRole("button", { name: /Start pipeline/ }).click();
    await expect.poll(() => configBody).toEqual({ rate_hz: 2400 });
    await expect.poll(() => deployBody).not.toBeNull();
    expect(String(deployBody?.producer_notebook)).toContain("update_rate_hz=2400");
  });

  test("zerobus health uses the serverless validation lane only [C7104][defect][regression]", async ({
    page,
  }) => {
    const setup = {
      id: "setup-pharma",
      industry: "pharmaceutical",
      status: "deployed",
      flavor: "classic",
      source_type: null,
      bootstrap_servers: null,
      enabled_modes: ["zerobus"],
      mode_configs: { zerobus: { rate_hz: 3200 } },
      warehouse_id: "warehouse-1",
    };
    await page.route("**/api/admin/pipelines/setup-pharma/health", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          setup,
          deployments: [],
          resources: {},
          checks: [],
          cluster_state: null,
          e2e_test: {
            tier: "e2e",
            available: true,
            spins_interactive_cluster: false,
            eta_minutes: 3,
            description: "Serverless Zerobus end-to-end validation.",
            start_endpoint: "POST /api/admin/industries/pharmaceutical/validate/start",
            state_endpoint: "GET /api/admin/industries/pharmaceutical/validate/state",
          },
          health: "draft",
          startable: false,
          ready: false,
          validation_passed: true,
          modes: [{ mode: "zerobus", status: "draft", health: "draft", startable: false }],
          status: "draft",
        }),
      }),
    );
    await page.route("**/api/admin/industries/pharmaceutical/validate/state", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          industry: "pharmaceutical",
          validation_state: {
            version: 1,
            enabled_modes: ["zerobus"],
            sequence: ["zerobus:create_table", "show_final_notebook", "done"],
            steps: {
              "zerobus:create_table": { status: "passed", error: null, updated_at: null },
              show_final_notebook: { status: "passed", error: null, updated_at: null },
              done: { status: "passed", error: null, updated_at: null },
            },
            cluster_id: null,
            run_ids: {},
          },
        }),
      }),
    );

    await page.goto("/pipelines/setup-pharma/health");
    await expect(page.getByText(/Serverless runner · no cluster/)).toBeVisible();
    await expect(page.getByRole("heading", { name: "Zerobus" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "Common" })).toHaveCount(0);
    await expect(page.getByRole("heading", { name: "Real-time" })).toHaveCount(0);
    await expect(page.getByText("Interactive cluster", { exact: true })).toHaveCount(0);
    await expect(page.getByText("Validation passed")).toBeVisible();
  });

  test("unselected modes require configuration before deployment [C7105][defect][regression]", async ({
    page,
  }, testInfo) => {
    await page.route("**/api/industries/catalog", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          is_admin: true,
          industries: [
            {
              id: "pharmaceutical",
              name: "Pharmaceutical",
              accent: "#2db0a0",
              icon: "pharma",
              twin_type: "vessel",
              tag: "Bioreactor",
              lede: "Process integrity",
              modes: [
                { id: "realtime", name: "Real-time", to: "Lakebase", desc: "", status: "idle", pipeline_id: null },
                { id: "microbatch", name: "Micro-batch", to: "Delta", desc: "", status: "idle", pipeline_id: null },
                { id: "zerobus", name: "Zerobus", to: "Delta", desc: "", status: "idle", pipeline_id: null },
              ],
              running_count: 0,
            },
          ],
        }),
      }),
    );
    const runtime = {
      pipelines: [
        {
          setup: {
            id: "setup-pharma",
            industry: "pharmaceutical",
            enabled_modes: ["zerobus"],
            bootstrap_servers: null,
          },
          deployments: [],
          modes: [
            {
              mode: "zerobus",
              status: "draft",
              health: "draft",
              startable: false,
              deployed: false,
            },
          ],
          health: {
            overall: "draft",
            resources_healthy: 0,
            resources_total: 0,
            jobs_running: 0,
            jobs_total: 0,
            enabled_modes: ["zerobus"],
            deployment_count: 0,
          },
        },
      ],
    };
    await page.route("**/api/industries/runtime", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(runtime),
      }),
    );
    await page.route("**/api/admin/pipelines", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(runtime),
      }),
    );

    await page.goto("/i/pharmaceutical");

    const realtimeCard = page.locator(".fcard").filter({ hasText: "Real-time mode" });
    const microbatchCard = page.locator(".fcard").filter({ hasText: "Micro-batch mode" });
    const zerobusCard = page.locator(".fcard").filter({ hasText: "Zerobus" });

    await expect(realtimeCard.locator(".fc-badge")).toHaveText(/not configured/);
    await expect(microbatchCard.locator(".fc-badge")).toHaveText(/not configured/);
    await expect(realtimeCard.getByRole("button", { name: /Configure & deploy/ })).toBeVisible();
    await expect(microbatchCard.getByRole("button", { name: /Configure & deploy/ })).toBeVisible();
    await expect(zerobusCard.locator(".fc-badge")).toHaveText(/not configured/);
    await expect(zerobusCard.getByRole("button", { name: /Configure & deploy/ })).toBeVisible();

    await page.screenshot({
      path: testInfo.outputPath("unconfigured-modes.png"),
      fullPage: true,
    });
  });

  test("only deployed modes drive card state, rollup, and animated flow [C7111][defect][regression]", async ({
    page,
  }, testInfo) => {
    await page.route("**/api/industries/catalog", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          is_admin: true,
          industries: [{
            id: "pharmaceutical",
            name: "Pharmaceutical",
            accent: "#2db0a0",
            icon: "pharma",
            twin_type: "vessel",
            tag: "Bioreactor",
            lede: "Process integrity",
            modes: [
              { id: "realtime", name: "Real-time", to: "Lakebase", desc: "", status: "idle", pipeline_id: null },
              { id: "microbatch", name: "Micro-batch", to: "Delta", desc: "", status: "idle", pipeline_id: null },
              { id: "zerobus", name: "Zerobus", to: "Delta", desc: "", status: "idle", pipeline_id: null },
            ],
            running_count: 0,
          }],
        }),
      }),
    );
    const runtime = {
      pipelines: [{
        setup: {
          id: "setup-pharma",
          industry: "pharmaceutical",
          enabled_modes: ["realtime", "microbatch", "zerobus"],
          bootstrap_servers: null,
          mode_configs: {},
        },
        deployments: [
          {
            id: "deployment-rt-failed",
            setup_id: "setup-pharma",
            mode: "realtime",
            status: "failed",
            job_id: "900",
          },
          {
            id: "deployment-zb",
            setup_id: "setup-pharma",
            mode: "zerobus",
            status: "deployed",
            job_id: "901",
          },
        ],
        modes: [
          { mode: "realtime", status: "draft", health: "draft", startable: false, deployed: false },
          { mode: "microbatch", status: "draft", health: "draft", startable: false, deployed: false },
          { mode: "zerobus", status: "running", health: "healthy", startable: true, deployed: true, flowing: true },
        ],
        status: "running",
        health: {
          overall: "healthy",
          resources_healthy: 1,
          resources_total: 1,
          jobs_running: 1,
          jobs_total: 1,
          enabled_modes: ["realtime", "microbatch", "zerobus"],
          deployment_count: 1,
        },
      }],
    };
    await page.route("**/api/industries/runtime", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(runtime),
      }),
    );
    await page.route("**/api/admin/pipelines", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(runtime),
      }),
    );

    await page.goto("/i/pharmaceutical");
    const realtimeCard = page.locator(".fcard").filter({ hasText: "Real-time mode" });
    const microbatchCard = page.locator(".fcard").filter({ hasText: "Micro-batch mode" });
    const zerobusCard = page.locator(".fcard").filter({ hasText: "Zerobus" });

    await expect(page.locator(".setup-head .badge")).toContainText("1/3 running");
    await expect(realtimeCard).toHaveClass(/unconfigured/);
    await expect(microbatchCard).toHaveClass(/unconfigured/);
    await expect(realtimeCard.getByRole("button", { name: /Start pipeline/ })).toHaveCount(0);
    await expect(microbatchCard.getByRole("button", { name: /Start pipeline/ })).toHaveCount(0);
    await expect(realtimeCard.getByRole("button", { name: /Configure & deploy/ })).toBeVisible();
    await expect(microbatchCard.getByRole("button", { name: /Configure & deploy/ })).toBeVisible();
    await expect(zerobusCard.locator(".fc-badge")).toHaveText(/running/);

    const flows = page.locator(".arch-flow");
    await expect(flows).toHaveCount(1);
    await expect(flows.first()).toHaveAttribute("data-mode", "zerobus");
    await expect(flows.first()).toHaveAttribute("data-state", "running");
    await expect(flows.first().locator(".anode")).toHaveCount(4);
    await expect(flows.first().locator(".flow-pulse")).toHaveCount(3);
    await page.screenshot({
      path: testInfo.outputPath("deployment-aware-animated-flow.png"),
      fullPage: true,
    });
  });

  test("sanitized runtime without job_id still opens the dashboard [C7265][defect][regression]", async ({
    page,
  }, testInfo) => {
    await page.route("**/api/industries/catalog", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          is_admin: true,
          industries: [{
            id: "manufacturing",
            name: "Manufacturing",
            accent: "#e86247",
            icon: "factory",
            twin_type: "line",
            tag: "Assembly line",
            lede: "Real-time visibility into your production line.",
            modes: [
              { id: "realtime", name: "Real-time", to: "Lakebase", desc: "", status: "idle", pipeline_id: null },
              { id: "microbatch", name: "Micro-batch", to: "Delta", desc: "", status: "idle", pipeline_id: null },
              { id: "zerobus", name: "Zerobus", to: "Delta", desc: "", status: "idle", pipeline_id: null },
            ],
            running_count: 0,
          }],
        }),
      }),
    );
    await page.route("**/api/industries/runtime", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          pipelines: [{
            setup_id: "setup-mfg",
            industry: "manufacturing",
            status: "running",
            setup: {
              id: "setup-mfg",
              industry: "manufacturing",
              mode_configs: { realtime: { rate_hz: 300 } },
            },
            deployments: [{ mode: "realtime", status: "deployed" }],
            modes: [{
              mode: "realtime",
              status: "running",
              health: "unhealthy",
              startable: false,
              deployed: true,
              flowing: true,
              runtime_detail: null,
              restartable: false,
            }],
          }],
        }),
      }),
    );

    await page.goto("/i/manufacturing");
    const realtimeCard = page.locator(".fcard").filter({ hasText: "Real-time mode" });
    const microbatchCard = page.locator(".fcard").filter({ hasText: "Micro-batch mode" });

    await expect(page.locator(".setup-head .badge")).toContainText("1/1 running");
    await expect(realtimeCard.locator(".fc-badge")).toHaveText(/running/);
    await expect(realtimeCard.getByRole("button", { name: /Open dashboard/ })).toBeVisible();
    await expect(realtimeCard.getByRole("button", { name: /Configure & deploy/ })).toHaveCount(0);
    await expect(microbatchCard.getByRole("button", { name: /Configure & deploy/ })).toBeVisible();

    await page.screenshot({
      path: testInfo.outputPath("sanitized-runtime-open-dashboard.png"),
      fullPage: true,
    });
  });

  test("industry catalog and runtime state load concurrently [C7250][defect][regression]", async ({
    page,
  }) => {
    let catalogFinishedAt = 0;
    let runtimeStartedAt = 0;
    const catalog = {
      is_admin: true,
      industries: [{
        id: "pharmaceutical",
        name: "Pharmaceutical",
        accent: "#2db0a0",
        icon: "pharma",
        twin_type: "vessel",
        tag: "Bioreactor",
        lede: "Process integrity",
        modes: [
          { id: "realtime", name: "Real-time", to: "Lakebase", desc: "", status: "idle", pipeline_id: null },
        ],
        running_count: 0,
      }],
    };
    const runtime = {
      pipelines: [{
        setup: { id: "setup-pharma", industry: "pharmaceutical", mode_configs: {} },
        deployments: [{ mode: "realtime", status: "deployed", job_id: "901" }],
        modes: [{ mode: "realtime", status: "running", health: "healthy", startable: true, deployed: true, flowing: true }],
        status: "running",
      }],
    };

    await page.route("**/api/industries/catalog", async (route) => {
      await new Promise((resolve) => setTimeout(resolve, 300));
      catalogFinishedAt ||= Date.now();
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(catalog) });
    });
    await page.route("**/api/industries/runtime", async (route) => {
      runtimeStartedAt ||= Date.now();
      await new Promise((resolve) => setTimeout(resolve, 400));
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(runtime) });
    });

    await page.goto("/industries");
    await expect(page.getByText("Loading industries…")).toBeVisible();
    await expect(page.getByRole("heading", { name: "Pharmaceutical" })).toBeVisible();
    expect(runtimeStartedAt).toBeGreaterThan(0);
    expect(runtimeStartedAt).toBeLessThan(catalogFinishedAt);
  });

  test("runtime loading never flashes Start pipeline [C7251][defect][regression]", async ({
    page,
  }) => {
    let releaseRuntime: () => void = () => {};
    const runtimeGate = new Promise<void>((resolve) => {
      releaseRuntime = resolve;
    });
    await page.route("**/api/industries/catalog", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          is_admin: true,
          industries: [{
            id: "pharmaceutical",
            name: "Pharmaceutical",
            accent: "#2db0a0",
            icon: "pharma",
            twin_type: "vessel",
            tag: "Bioreactor",
            lede: "Process integrity",
            modes: [
              { id: "realtime", name: "Real-time", to: "Lakebase", desc: "", status: "idle", pipeline_id: null },
              { id: "microbatch", name: "Micro-batch", to: "Delta", desc: "", status: "idle", pipeline_id: null },
              { id: "zerobus", name: "Zerobus", to: "Delta", desc: "", status: "idle", pipeline_id: null },
            ],
            running_count: 0,
          }],
        }),
      }),
    );
    await page.route("**/api/industries/runtime", async (route) => {
      await runtimeGate;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          pipelines: [{
            setup: { id: "setup-pharma", industry: "pharmaceutical", mode_configs: {} },
            deployments: [{ mode: "realtime", status: "deployed", job_id: "901" }],
            modes: [
              { mode: "realtime", status: "running", health: "healthy", startable: true, deployed: true, flowing: true },
              { mode: "microbatch", status: "draft", health: "draft", startable: false, deployed: false },
              { mode: "zerobus", status: "draft", health: "draft", startable: false, deployed: false },
            ],
            status: "running",
          }],
        }),
      });
    });

    await page.goto("/i/pharmaceutical");
    await expect(page.getByText("Loading industry…")).toBeVisible();
    await expect(page.locator(".fcard")).toHaveCount(0);
    await expect(page.getByRole("button", { name: /Start pipeline/ })).toHaveCount(0);
    releaseRuntime();

    const realtimeCard = page.locator(".fcard").filter({ hasText: "Real-time mode" });
    await expect(realtimeCard.getByRole("button", { name: /Open dashboard/ })).toBeVisible();
    await expect(page.locator(".setup-head .badge")).toContainText("1/3 running");
  });

  test("running Zerobus stop targets its deployment and reports failures [C7112][defect][regression]", async ({
    page,
  }, testInfo) => {
    let stopRequest: { method: string; url: string } | null = null;
    let adminListRequests = 0;
    await page.route("**/api/industries/catalog", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          is_admin: true,
          industries: [{
            id: "pharmaceutical",
            name: "Pharmaceutical",
            accent: "#2db0a0",
            icon: "pharma",
            twin_type: "vessel",
            tag: "Bioreactor",
            lede: "Process integrity",
            modes: [
              { id: "realtime", name: "Real-time", to: "Lakebase", desc: "", status: "idle", pipeline_id: null },
              { id: "microbatch", name: "Micro-batch", to: "Delta", desc: "", status: "idle", pipeline_id: null },
              {
                id: "zerobus",
                name: "Zerobus",
                to: "Delta",
                desc: "",
                status: "idle",
                pipeline_id: "legacy-zb",
                deployment_id: "deployment-zb",
              },
            ],
            running_count: 0,
          }],
        }),
      }),
    );
    await page.route("**/api/admin/pipelines", (route) => {
      adminListRequests += 1;
      return route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          pipelines: [{
            setup: {
              id: "setup-pharma",
              industry: "pharmaceutical",
              enabled_modes: ["zerobus"],
              bootstrap_servers: null,
            },
            deployments: [{
              id: "deployment-zb",
              setup_id: "setup-pharma",
              mode: "zerobus",
              status: "deployed",
              job_id: "901",
            }],
            modes: [{
              mode: "zerobus",
              status: "running",
              health: "healthy",
              startable: false,
              deployed: true,
              flowing: true,
            }],
            status: "running",
            health: {
              overall: "healthy",
              resources_healthy: 1,
              resources_total: 1,
              jobs_running: 1,
              jobs_total: 1,
              enabled_modes: ["zerobus"],
              deployment_count: 1,
            },
          }],
        }),
      });
    });
    await page.route(
      "**/api/admin/pipelines/setup-pharma/zerobus/stop",
      async (route) => {
        stopRequest = {
          method: route.request().method(),
          url: new URL(route.request().url()).pathname,
        };
        await route.fulfill({
          status: 502,
          contentType: "application/json",
          body: JSON.stringify({ detail: "Jobs API denied cancellation" }),
        });
      },
    );

    await page.goto("/i/pharmaceutical");
    const zerobusCard = page.locator(".fcard").filter({ hasText: "Zerobus" });
    await expect(zerobusCard.locator(".fc-badge")).toHaveText(/running/);
    const requestsBeforeStop = adminListRequests;
    await zerobusCard.getByRole("button", { name: "Stop" }).click();

    await expect.poll(() => stopRequest).toEqual({
      method: "POST",
      url: "/api/admin/pipelines/setup-pharma/zerobus/stop",
    });
    await expect.poll(() => adminListRequests).toBeGreaterThan(requestsBeforeStop);
    await expect(page.getByText(/Jobs API denied cancellation/)).toBeVisible();
    await page.screenshot({
      path: testInfo.outputPath("zerobus-stop-error-feedback.png"),
      fullPage: true,
    });
  });

  test("zerobus deploy sends its generated producer notebook [C7106][defect][regression]", async ({
    page,
  }) => {
    const setup = {
      id: "setup-pharma",
      industry: "pharmaceutical",
      status: "validated",
      flavor: "classic",
      source_type: null,
      bootstrap_servers: null,
      enabled_modes: ["zerobus"],
      mode_configs: { zerobus: { rate_hz: 3200 } },
      warehouse_id: "warehouse-1",
    };
    const validationState = {
      version: 1,
      enabled_modes: ["zerobus"],
      sequence: ["zerobus:create_table", "show_final_notebook", "done"],
      steps: {
        "zerobus:create_table": { status: "passed", error: null, updated_at: null },
        show_final_notebook: { status: "passed", error: null, updated_at: null },
        done: { status: "passed", error: null, updated_at: null },
      },
      cluster_id: null,
      run_ids: {},
    };
    let deployBody: Record<string, unknown> | null = null;

    await page.route("**/api/admin/pipelines", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          pipelines: [{
            setup,
            deployments: [],
            modes: [{ mode: "zerobus", status: "draft", health: "draft", startable: false }],
            health: {
              overall: "draft",
              resources_healthy: 0,
              resources_total: 0,
              jobs_running: 0,
              jobs_total: 0,
              enabled_modes: ["zerobus"],
              deployment_count: 0,
            },
          }],
        }),
      }),
    );
    await page.route("**/api/admin/industries/pharmaceutical/validate/state", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ industry: "pharmaceutical", validation_state: validationState }),
      }),
    );
    await page.route("**/api/admin/industries/pharmaceutical/validate/can-deploy", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          industry: "pharmaceutical",
          can_deploy: true,
          data_purged: true,
          outstanding: [],
          modes: {
            zerobus: { can_deploy: true, data_purged: true, outstanding: [] },
          },
        }),
      }),
    );
    await page.route("**/api/admin/pipelines/setup-pharma/modes", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ setup, added: [], removed: [], retained: ["zerobus"] }),
      }),
    );
    await page.route("**/api/admin/industries/pharmaceutical/setup", async (route) => {
      if (route.request().method() === "GET") {
        await route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify({ setup, deployments: [], pipeline: {} }),
        });
        return;
      }
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ created: false, setup }),
      });
    });
    await page.route("**/api/app-defaults", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          zerobus_endpoint: "123.zerobus.eu-central-1.cloud.databricks.com",
          zerobus_client_id: "client-id",
          workspace_url: "https://workspace.cloud.databricks.com",
          uc_catalog: "catalog",
          uc_schema: "schema",
        }),
      }),
    );
    await page.route("**/api/test-connectivity/upload-wheel", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ wheel_path: "/Workspace/wheels/simulator.whl" }),
      }),
    );
    await page.route("**/api/admin/pipelines/setup-pharma/deploy/zerobus", async (route) => {
      deployBody = route.request().postDataJSON() as Record<string, unknown>;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ job_id: "job-1", mode: "zerobus" }),
      });
    });

    await page.goto("/setup-validate?industry=pharmaceutical&step=4");
    await expect(page.getByRole("button", { name: "Deploy" })).toBeEnabled();
    await page.getByRole("button", { name: "Deploy" }).click();

    await expect.poll(() => deployBody).not.toBeNull();
    expect(deployBody).toMatchObject({ gate_override: true });
    expect(String(deployBody?.producer_notebook)).toContain("source");
    expect(String(deployBody?.producer_notebook)).toContain("zerobus");
    expect(String(deployBody?.producer_notebook)).toContain("update_rate_hz=3200");
  });

  test("industry-first zerobus deployment opens its dashboard [C7107][defect][regression]", async ({
    page,
  }, testInfo) => {
    let metricsUrl = "";
    await page.route("**/api/industries/catalog", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          is_admin: true,
          industries: [{
            id: "pharmaceutical",
            name: "Pharmaceutical",
            accent: "#2db0a0",
            icon: "pharma",
            twin_type: "vessel",
            tag: "Bioreactor",
            lede: "Process integrity",
            kpis: [],
            sensors: [],
            modes: [
              { id: "realtime", name: "Real-time", to: "Lakebase", desc: "", status: "idle", pipeline_id: null },
              { id: "microbatch", name: "Micro-batch", to: "Delta", desc: "", status: "idle", pipeline_id: null },
              {
                id: "zerobus",
                name: "Zerobus",
                to: "Delta",
                desc: "",
                status: "idle",
                pipeline_id: "legacy-zb",
                deployment_id: "deployment-zb",
              },
            ],
            running_count: 0,
          }],
        }),
      }),
    );
    await page.route("**/api/pipelines", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify([{
          id: "legacy-zb",
          name: "iot-pharmaceutical-zerobus",
          industry: "pharmaceutical",
          source_type: "zerobus",
          source_config: {},
          resources: {},
        }]),
      }),
    );
    await page.route("**/api/pipelines/deployment-zb", (route) =>
      route.fulfill({ status: 404, contentType: "application/json", body: "{}" }),
    );
    await page.route("**/api/admin/pipelines", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          pipelines: [{
            setup: {
              id: "setup-pharma",
              industry: "pharmaceutical",
              enabled_modes: ["zerobus"],
            },
            deployments: [{
              id: "deployment-zb",
              setup_id: "setup-pharma",
              mode: "zerobus",
              status: "deployed",
              job_id: "901",
            }],
            modes: [{
              mode: "zerobus",
              status: "running",
              health: "healthy",
              startable: true,
            }],
          }],
        }),
      }),
    );
    await page.route("**/api/pipelines/*/zerobus-metrics**", (route) => {
      metricsUrl = route.request().url();
      return route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          table_name: "catalog.schema.pharmaceutical_zerobus",
          total_records: 10,
          total_bytes: 1000,
          records_per_sec: 1,
          records_per_min: 60,
          last_commit_time: "2026-08-27T18:00:00Z",
          stream_status: "closed",
          error_count: 0,
          series: [],
          recent_errors: [],
          latest_readings: [
            {
              sensor_name: "bioreactor_1_temp",
              sensor_type: "temperature",
              value: 71.2,
              unit: "°C",
              fault_active: false,
              event_time: "2026-08-27T18:00:00Z",
            },
            {
              sensor_name: "wfi_flow",
              sensor_type: "flow",
              value: 63,
              unit: "L/min",
              fault_active: false,
              event_time: "2026-08-27T18:00:00Z",
            },
            {
              sensor_name: "buffer_pressure",
              sensor_type: "pressure",
              value: 3.25,
              unit: "bar",
              fault_active: false,
              event_time: "2026-08-27T18:00:00Z",
            },
          ],
        }),
      });
    });
    await page.route("**/api/latency/zerobus**", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          table_name: "catalog.schema.pharmaceutical_zerobus",
          has_data: true,
          last_commit_time: "2026-08-27T18:00:00Z",
          seconds_since_commit: 1,
          committed_records_total: 10,
          committed_bytes_total: 1000,
          records_per_min: 60,
          recent_error_count: 0,
          recent_errors: [],
        }),
      }),
    );
    await page.route("**/api/pipeline/deployment-zb/dbsql-timeseries**", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          source: "dbsql",
          mode: "zerobus",
          table: "catalog.schema.pharmaceutical_zerobus",
          resolution: "hour",
          minutes: 1440,
          count: 0,
          rows: [],
        }),
      }),
    );

    await page.goto("/i/pharmaceutical/zerobus/dashboard");

    await expect(page.getByText("Ingest health")).toBeVisible();
    await expect(page.getByText("Historical · DBSQL")).toBeVisible();
    await expect(page.getByText("Commit freshness", { exact: true })).toBeVisible();
    await expect(page.getByText(/Bioreactor 71\.2/)).toBeVisible();
    await expect(page.getByText(/WFI 63/)).toBeVisible();
    await expect(page.getByText(/Buffer 3\.25/)).toBeVisible();
    await expect(page.getByText("● LIVE · ZEROBUS", { exact: true })).toBeVisible();
    await expect(page.getByText(/loading dashboard/)).toHaveCount(0);
    await expect.poll(() => metricsUrl).toContain(
      "/api/pipelines/deployment-zb/zerobus-metrics",
    );
    expect(metricsUrl).toContain("industry=pharmaceutical");
    await page.screenshot({
      path: testInfo.outputPath("industry-first-zerobus-dashboard.png"),
      fullPage: true,
    });
  });
});

test.describe("[Windsor] Pipeline resource health", () => {
  test("refresh rechecks UC grants without cached health [C7214][defect][regression]", async ({
    page,
  }, testInfo) => {
    let healthRequests = 0;
    let granted = false;
    await page.route("**/api/admin/pipelines/setup-pharma/health", async (route) => {
      healthRequests += 1;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          setup_id: "setup-pharma",
          industry: "pharmaceutical",
          uc_catalog: "configured-catalog",
          uc_schema: "configured-schema",
          app_sp_client_id: "current-app-sp",
          status: "draft",
          health: granted ? "healthy" : "unhealthy",
          modes: [],
          startable: granted,
          ready: false,
          validation_passed: true,
          setup: {
            id: "setup-pharma",
            industry: "pharmaceutical",
            enabled_modes: [],
            validation_state: {},
            status: "draft",
          },
          deployments: [],
          resource_health: {
            overall: "draft",
            resources_healthy: 0,
            resources_total: 0,
            jobs_running: 0,
            jobs_total: 0,
            enabled_modes: [],
            deployment_count: 0,
          },
          resources: {},
          e2e_test: {
            tier: "e2e",
            available: false,
            spins_interactive_cluster: false,
            eta_minutes: 0,
            description: "No enabled modes.",
            start_endpoint: "",
            state_endpoint: "",
          },
          checks: [
            {
              name: "uc_grants",
              tier: "api",
              status: granted ? "pass" : "fail",
              detail: granted
                ? "app SP current-app-sp granted on example-catalog"
                : "app SP current-app-sp has no grants on catalog example-catalog",
            },
          ],
          cluster_state: null,
          deploy_health: "draft",
          deploy_startable: false,
        }),
      });
    });

    await page.goto("/pipelines/setup-pharma/health");
    await expect(page.getByText(/has no grants on catalog example-catalog/)).toBeVisible();

    const requestsBeforeRefresh = healthRequests;
    granted = true;
    await page.getByRole("button", { name: "Refresh" }).click();

    await expect.poll(() => healthRequests).toBeGreaterThan(requestsBeforeRefresh);
    await expect(page.getByText(/current-app-sp granted on example-catalog/)).toBeVisible();
    await expect(page.getByText(/has no grants on catalog example-catalog/)).toHaveCount(0);
    await page.screenshot({
      path: testInfo.outputPath("grants-refresh-rechecked.png"),
      fullPage: true,
    });
  });
});
