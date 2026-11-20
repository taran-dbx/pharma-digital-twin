export default {
  teamNames: ["Windsor"],
  testTypes: ["ui"],
  testCategories: ["smoke", "regression", "defect"],
  googleSpreadsheetUrl: process.env.QA_SHADOW_SPREADSHEET_URL ?? "",
  googleKeyFilePath: process.env.QA_SHADOW_KEY_FILE ?? "",
  testData: "./test-results/output.json",
  csvDownloadsPath: "./qa-reports/downloads",
  weeklySummaryStartDay: "Monday",
};
