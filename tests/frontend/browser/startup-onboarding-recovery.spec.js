const { test, expect } = require("@playwright/test");
const { loadApp } = require("./fixtures");

const EXISTING_INSTALL = {
  onboarded: true, has_config_file: true, output_dir: "C:\\FixtureArchive",
  log_mode: "Simple",
};

async function bootWithRuntime(page, runtime) {
  await page.addInitScript(runtime => {
    const configure = setHandler => setHandler("get_runtime_info", () => Promise.resolve(runtime));
    if (typeof window.__setBridgeHandler === "function") configure(window.__setBridgeHandler);
    else Object.defineProperty(window, "__setBridgeHandler", {
      configurable: true,
      set(handler) {
        Object.defineProperty(window, "__setBridgeHandler", {
          configurable: true, writable: true, value: handler,
        });
        configure(handler);
      },
    });
  }, runtime);
  await loadApp(page);
  await page.evaluate(() => window.seedLogs());
}

for (const [name, runtime] of Object.entries({
  "an unavailable native method": {
    ok: false, code: "NATIVE_BRIDGE_UNAVAILABLE", error: "App connection is still starting",
  },
  "an empty response": {},
  "an incomplete first-run response": { onboarded: false },
  "a malformed configuration flag": { onboarded: false, has_config_file: "false", output_dir: "" },
})) {
  test(`${name} offers startup retry without opening first-time setup`, async ({ page }) => {
    await bootWithRuntime(page, runtime);
    await expect(page.locator("#onboarding-overlay")).not.toBeVisible();
    await expect(page.locator("#boot-issue-retry-seed")).toBeVisible();

    await page.evaluate(runtime => {
      window.__setBridgeHandler("get_runtime_info", () => Promise.resolve(runtime));
    }, EXISTING_INSTALL);
    await page.locator("#boot-issue-retry-seed").click();
    await expect(page.locator("#boot-issue-retry-seed")).toHaveCount(0);
    await expect(page.locator("#boot-issue-banner")).not.toBeVisible();
    await expect(page.locator("#onboarding-overlay")).not.toBeVisible();
    expect(await page.evaluate(() => window.__bridgeCallsFor("onboarding_finish").length)).toBe(0);
  });
}

test("a confirmed new installation still opens first-time setup", async ({ page }) => {
  await bootWithRuntime(page, { onboarded: false, has_config_file: false, output_dir: "" });
  await expect(page.locator("#onboarding-overlay")).toBeVisible();
  await expect(page.locator("#boot-issue-retry-seed")).toHaveCount(0);
});

test("an existing installation loads without opening setup", async ({ page }) => {
  await bootWithRuntime(page, EXISTING_INSTALL);
  await expect(page.locator("#onboarding-overlay")).not.toBeVisible();
  await expect(page.locator("#boot-issue-retry-seed")).toHaveCount(0);
});

test("a partially published native API recovers on bridge readiness without opening setup", async ({ page }) => {
  await page.addInitScript(() => {
    Object.defineProperty(window, "pywebview", {
      configurable: true,
      set(bridge) {
        const completeApi = bridge.api;
        window.__publishCompleteApi = () => {
          bridge.api = completeApi;
          window.dispatchEvent(new Event("pywebviewready"));
        };
        bridge.api = {};
        Object.defineProperty(window, "pywebview", { configurable: true, writable: true, value: bridge });
      },
    });
  });
  await loadApp(page);
  await page.evaluate(() => window.seedLogs());
  await expect(page.locator("#boot-issue-retry-seed")).toBeVisible();
  await expect(page.locator("#onboarding-overlay")).not.toBeVisible();
  await page.evaluate(() => window.__publishCompleteApi());
  await expect(page.locator("#boot-issue-retry-seed")).toHaveCount(0);
  await expect(page.locator("#boot-issue-banner")).not.toBeVisible();
  await expect(page.locator("#onboarding-overlay")).not.toBeVisible();
  for (const method of ["startup_ready", "get_runtime_info", "get_subs_channels", "get_index_summary", "get_queues"]) {
    expect(await page.evaluate(method => window.__bridgeCallsFor(method).length, method)).toBeGreaterThan(0);
  }
});

test("runtime recovery retries after an in-flight seed and retains unrelated startup warnings", async ({ page }) => {
  await page.addInitScript(() => {
    const configure = setHandler => {
      setHandler("get_runtime_info", () => Promise.resolve({ ok: false, error: "Starting" }));
      setHandler("get_activity_log_history", () => new Promise(resolve => {
        window.__finishActivitySeed = () => resolve([]);
      }));
    };
    if (typeof window.__setBridgeHandler === "function") configure(window.__setBridgeHandler);
    else Object.defineProperty(window, "__setBridgeHandler", {
      configurable: true,
      set(handler) {
        Object.defineProperty(window, "__setBridgeHandler", { configurable: true, writable: true, value: handler });
        configure(handler);
      },
    });
  });
  await loadApp(page);
  await page.waitForFunction(() => typeof window.__finishActivitySeed === "function");
  await page.evaluate(runtime => {
    window._reportBootIssue("Another feature", "Separate startup problem");
    window._reportBootIssue("App connection", "Connection was slow to start");
    window.__setBridgeHandler("get_runtime_info", () => Promise.resolve(runtime));
    window.dispatchEvent(new Event("pywebviewready"));
    window.__finishActivitySeed();
  }, EXISTING_INSTALL);
  await expect.poll(() => page.evaluate(() => window.__bridgeCallsFor("get_runtime_info").length)).toBe(2);
  await expect(page.locator("#boot-issue-retry-seed")).toHaveCount(0);
  await expect(page.locator("#boot-issue-banner")).toBeVisible();
  expect(await page.evaluate(() => window.YT.bootIssues.map(issue => issue.name))).toEqual(["Another feature"]);
  await expect(page.locator("#onboarding-overlay")).not.toBeVisible();
});
