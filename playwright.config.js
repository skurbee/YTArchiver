const path = require("node:path");
const os = require("node:os");
const { defineConfig } = require("@playwright/test");

module.exports = defineConfig({
  testDir: path.join(__dirname, "tests", "frontend", "browser"),
  testMatch: "**/*.spec.js",
  fullyParallel: false,
  workers: 1,
  retries: 0,
  timeout: 15_000,
  expect: { timeout: 5_000 },
  reporter: "line",
  outputDir: path.join(os.tmpdir(), "ytarchiver-playwright-results"),
  use: {
    browserName: "chromium",
    // YTArchiver ships on Windows/WebView2. Using the installed Chrome
    // channel keeps local and Windows-CI runs self-contained; callers can
    // select Edge with YTARCHIVER_BROWSER_CHANNEL=msedge.
    channel: process.env.YTARCHIVER_BROWSER_CHANNEL || "chrome",
    headless: true,
    launchOptions: { args: ["--disable-gpu"] },
    viewport: { width: 1440, height: 1000 },
    actionTimeout: 5_000,
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
  },
});
