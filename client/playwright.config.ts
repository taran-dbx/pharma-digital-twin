import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: ".",
  testMatch: [
    "manufacturing-dashboard.spec.ts",
    "manufacturing-operator-twin.spec.ts",
    "mode-adaptive.spec.ts",
    "platform-landing.spec.ts",
  ],
  fullyParallel: false,
  reporter: [
    ["line"],
    ["json", { outputFile: "./test-results/output.json" }],
  ],
  use: {
    baseURL: "http://127.0.0.1:4173",
    screenshot: "only-on-failure",
    trace: "retain-on-failure",
  },
  webServer: {
    command: "npm run dev -- --host 127.0.0.1 --port 4173",
    url: "http://127.0.0.1:4173",
    reuseExistingServer: true,
  },
});
