import { expect, test } from "@playwright/test";

test.describe("[Windsor] Platform Landing", () => {
  test.beforeEach(async ({ page }) => {
    await page.addInitScript(() => {
      localStorage.setItem("iot-twin-theme", "light");
    });
    await page.route("**/api/**", (route) =>
      route.fulfill({
        status: 503,
        contentType: "application/json",
        body: JSON.stringify({ detail: "Backend intentionally unavailable in UI test" }),
      }),
    );
  });

  test("root shows the platform paths and routes to industries [C7201][smoke][regression]", async ({
    page,
  }, testInfo) => {
    await page.goto("/");

    await expect(
      page.getByRole("heading", { name: "Trace sensor data from edge to twin." }),
    ).toBeVisible();
    const chooseIndustry = page.getByRole("link", { name: "Choose an industry" });
    await expect(chooseIndustry).toHaveAttribute("href", "/industries");
    await expect(page.getByRole("button", { name: "View architecture" })).toHaveCount(0);

    const paths = page.locator(".platform-path-list > li");
    await expect(paths).toHaveCount(3);
    await expect(paths.nth(0)).toContainText("SUB-SECOND TARGET");
    await expect(paths.nth(0)).toContainText("Kafka (MSK)");
    await expect(paths.nth(0)).toContainText("Kinesis");
    await expect(paths.nth(1)).toContainText("NEAR REAL-TIME");
    await expect(paths.nth(2)).toContainText("NEAR REAL-TIME");

    const technologies = page.locator(".platform-technologies");
    await expect(technologies).toContainText("Databricks Genie");
    await expect(technologies).not.toContainText("Kafka");
    await expect(technologies).not.toContainText("Kinesis");

    await expect(
      page.getByRole("heading", {
        name: "Timeseries Database powered by LakeTS on Lakebase",
      }),
    ).toBeVisible();
    await expect(page.getByRole("link", { name: "Explore more with LakeTS" })).toHaveAttribute(
      "href",
      "https://databricks-solutions.github.io/lakets/",
    );

    await page.screenshot({
      path: testInfo.outputPath("platform-landing.png"),
      fullPage: true,
    });

    await chooseIndustry.click();
    await expect(page).toHaveURL(/\/industries$/);
    await expect(page.locator("#view-gallery")).toBeVisible();
  });

  test("Genie stays dense in both themes [C7202][defect][regression]", async ({
    page,
  }, testInfo) => {
    await page.goto("/");

    const samples = page.locator(".platform-genie-samples");
    await expect(page.locator(".platform-genie-samples > span")).toHaveCount(4);
    await expect(samples).toHaveCSS("align-self", "auto");

    const samplesBox = await samples.boundingBox();
    const panelBox = await page.locator(".platform-genie").boundingBox();
    expect(samplesBox?.height).toBeLessThan(190);
    expect(panelBox?.height).toBeLessThan(240);

    await page.getByTitle("Toggle light / dark").click();
    await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
    await expect(samples).toBeVisible();

    await page.screenshot({
      path: testInfo.outputPath("platform-landing-dark.png"),
      fullPage: true,
    });
  });
});
