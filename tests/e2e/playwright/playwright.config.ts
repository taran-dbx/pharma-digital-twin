import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { defineConfig, devices } from "@playwright/test";

const repoRoot = fileURLToPath(new URL("../../..", import.meta.url));
const reportsRoot = join(repoRoot, "qa-reports");

export default defineConfig({
  testDir: "./specs",
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 1 : undefined,
  outputDir: join(reportsRoot, "playwright-artifacts"),
  reporter: [
    ["line"],
    ["json", { outputFile: join(reportsRoot, "results", "playwright.json") }],
    ["junit", { outputFile: join(reportsRoot, "results", "playwright.xml") }],
    ["html", { outputFolder: join(reportsRoot, "playwright-report"), open: "never" }],
  ],
  use: {
    baseURL: process.env.PLAYWRIGHT_BASE_URL ?? "http://127.0.0.1:4317",
    screenshot: "only-on-failure",
    trace: "retain-on-failure",
    video: "retain-on-failure",
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
  webServer: process.env.PLAYWRIGHT_BASE_URL
    ? undefined
    : {
        command: "PORT=4317 node server.mjs",
        url: "http://127.0.0.1:4317",
        reuseExistingServer: !process.env.CI,
      },
});
