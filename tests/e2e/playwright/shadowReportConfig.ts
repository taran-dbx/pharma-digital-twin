export default {
  teamNames: ["Windsor"],
  testTypes: ["ui"],
  testCategories: ["smoke", "regression", "defect", "accessibility"],
  googleSpreadsheetUrl: process.env.QA_SHADOW_SPREADSHEET_URL ?? "",
  googleKeyFilePath: process.env.QA_SHADOW_KEY_FILE ?? "",
  testData: "../../../qa-reports/results/playwright.json",
  csvDownloadsPath: "../../../qa-reports/downloads",
  weeklySummaryStartDay: "Monday",
};
