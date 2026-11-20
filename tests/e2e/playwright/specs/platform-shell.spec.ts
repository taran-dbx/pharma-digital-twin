import { PlatformPage } from "../pages/platform.page";
import { expect, test } from "../fixtures/test";

test.describe("[Windsor] Platform shell", () => {
  test.describe("mobile admin navigation", () => {
    test.use({ viewport: { width: 375, height: 812 } });

    test("keeps every header action inside the viewport [C7302][defect][regression]", async ({
      page,
    }, testInfo) => {
      const platform = new PlatformPage(page);
      await platform.open();
      await expect(platform.allPipelines).toBeVisible();
      await expect(platform.themeToggle).toBeVisible();

      const metrics = await platform.viewportMetrics();
      expect(metrics.documentWidth).toBeLessThanOrEqual(metrics.viewportWidth);
      expect(metrics.actionsRight).toBeLessThanOrEqual(metrics.viewportWidth);

      await testInfo.attach("mobile-layout-metrics", {
        body: JSON.stringify(metrics, null, 2),
        contentType: "application/json",
      });
      await page.screenshot({
        path: testInfo.outputPath("C7302-admin-mobile-header.png"),
        fullPage: true,
      });
    });
  });

  test("loads a real application favicon [C7303][defect][smoke]", async ({ page, request }) => {
    const platform = new PlatformPage(page);
    await platform.open();

    const favicon = page.locator('link[rel="icon"]');
    await expect(favicon).toHaveAttribute(
      "href",
      "/brand/data-streaming-icon-full-color.svg",
    );
    const response = await request.get(await favicon.getAttribute("href") as string);
    expect(response.ok()).toBeTruthy();
    expect(response.headers()["content-type"]).toContain("image/svg+xml");
  });

  test("does not expose an inert help control [C7304][defect][regression]", async ({ page }) => {
    const platform = new PlatformPage(page);
    await platform.open();

    await expect(page.getByRole("main")).toBeVisible();
    await expect(platform.themeToggle).toBeVisible();
    await expect(page.getByRole("button", { name: "Help" })).toHaveCount(0);
  });

  test("gives dashboard routes a page heading [C7309][accessibility][regression]", async ({
    page,
  }) => {
    await page.route("**/api/industries/catalog", (route) =>
      route.fulfill({
        json: {
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
                  deployment_id: "deployment-1",
                },
              ],
              running_count: 1,
            },
          ],
        },
      }),
    );

    await page.goto("/i/manufacturing/realtime/dashboard");
    await expect(
      page.getByRole("heading", { level: 1, name: /Manufacturing twin/ }),
    ).toBeVisible();
  });
});
