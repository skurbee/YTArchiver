const { test, expect } = require("@playwright/test");
const { loadApp } = require("./fixtures");

async function expectPermanentMiniLog(page, panelId, logId) {
  const geometry = await page.evaluate(({ panelId, logId }) => {
    const panel = document.getElementById(panelId);
    const main = panel?.querySelector(".settings-main");
    const log = document.getElementById(logId);
    const panelRect = panel?.getBoundingClientRect();
    const logRect = log?.getBoundingClientRect();
    return {
      panelActive: !!panel?.classList.contains("active"),
      logVisible: !!logRect && logRect.width > 0 && logRect.height > 0,
      logTop: logRect?.top ?? -1,
      logBottom: logRect?.bottom ?? -1,
      panelTop: panelRect?.top ?? -1,
      panelBottom: panelRect?.bottom ?? -1,
      mainScrollWidth: main?.scrollWidth ?? 0,
      mainClientWidth: main?.clientWidth ?? 0,
    };
  }, { panelId, logId });

  expect(geometry.panelActive).toBe(true);
  expect(geometry.logVisible).toBe(true);
  expect(geometry.logTop).toBeGreaterThanOrEqual(geometry.panelTop);
  expect(geometry.logBottom).toBeLessThanOrEqual(geometry.panelBottom + 1);
  expect(geometry.mainScrollWidth).toBeLessThanOrEqual(
    geometry.mainClientWidth + 1);
}

test("Settings and every Health destination keep their permanent mini logs", async ({ page }) => {
  await page.setViewportSize({ width: 980, height: 720 });
  await loadApp(page);

  await page.locator('.tab[data-tab="settings"]').click();
  await expect(page.locator("#panel-settings .settings-sidebar")).toHaveCount(0);
  await expect(page.locator("#settings-view-preferences")).toBeVisible();
  for (const section of [
    "settings-section-storage", "settings-section-downloads", "settings-section-app",
  ]) {
    await expect(page.locator(`#${section}`)).toBeVisible();
  }
  await expect(page.locator("#settings-autosave-note")).toBeVisible();
  await expectPermanentMiniLog(page, "panel-settings", "settings-mini-log");

  await page.locator('.tab[data-tab="health"]').click();
  for (const view of ["overview", "library", "backups"]) {
    await page.locator(
      `#panel-health [data-settings-view="${view}"]`).click();
    await expect(page.locator(`#settings-view-${view}`)).toBeVisible();
    await expectPermanentMiniLog(page, "panel-health", "health-mini-log");
  }
});

test("Browse keeps its mini log while Download keeps the full log", async ({ page }) => {
  await page.setViewportSize({ width: 980, height: 720 });
  await loadApp(page);

  await page.locator('.tab[data-tab="browse"]').click();
  await expect(page.locator("#browse-mini-log")).toBeVisible();

  await page.locator('.tab[data-tab="download"]').click();
  await expect(page.locator("#main-log")).toBeVisible();
  await expect(page.locator("#browse-mini-log")).toBeHidden();
  await expect(page.locator("#settings-mini-log")).toBeHidden();
  await expect(page.locator("#health-mini-log")).toBeHidden();
});

test("Search preferences stay with Storage while Health keeps index maintenance", async ({ page }) => {
  await loadApp(page);

  await page.locator('.tab[data-tab="settings"]').click();
  await expect(page.locator("#settings-roots-list")).toBeVisible();
  await expect(page.locator("#btn-settings-add-root")).toBeVisible();
  await expect(page.locator("#btn-settings-remove-root")).toBeVisible();
  await expect(page.locator("#settings-roots-list"))
    .toContainText("C:\\FixtureArchive");

  await page.locator("#settings-background-checks").evaluate((details) => {
    details.open = true;
  });
  const autoIndex = page.locator("#settings-auto-index-enabled");
  await expect(autoIndex).toBeVisible();
  await expect(page.locator("#settings-auto-index-threshold")).toBeVisible();
  await autoIndex.check();
  await expect.poll(() => page.evaluate(() =>
    window.__bridgeCallsFor("settings_save").some((entry) =>
      entry.args[0]?.auto_index_enabled === true
      && entry.args[0]?.auto_index_threshold === 10)))
    .toBe(true);

  await page.locator('.tab[data-tab="health"]').click();
  await page.locator(
    '#panel-health [data-settings-view="library"]').click();
  await expect(page.locator("#index-stats-text")).toBeVisible();
  await expect(page.locator("#btn-idx-build")).toBeVisible();
  await expect(page.locator("#panel-health #settings-roots-list"))
    .toHaveCount(0);
  await expect(page.locator("#panel-health #settings-auto-index-enabled"))
    .toHaveCount(0);
  await expect(page.locator("#panel-settings #settings-auto-backup"))
    .toHaveCount(0);
  await page.locator('#panel-health [data-settings-view="backups"]').click();
  await expect(page.locator("#settings-auto-backup")).toHaveCount(1);
  await expect(page.locator(
    '.yt-dd:has(+ #settings-auto-backup) .yt-dd-trigger')).toBeVisible();
  await expect(page.locator("#settings-auto-backup")).toHaveValue("weekly");
  await expect(page.locator("#backup-auto-age-display"))
    .toHaveText("Last automatic backup: 2 days ago");
});

test("Auto-sync keeps timing mode in Settings and only shows a needed clock picker", async ({ page }) => {
  await loadApp(page);
  const timing = page.locator("#autorun-timing-mode-wrap");
  const clock = page.locator("#autorun-clock-time-wrap");

  await expect(timing).toHaveCount(0);
  await expect(page.locator("#panel-download #settings-autorun-mode"))
    .toHaveCount(0);
  await expect(page.locator("#panel-settings #settings-autorun-mode"))
    .toHaveCount(1);
  await expect(clock).toBeHidden();

  await page.evaluate(() => {
    window.__setBridgeHandler("autorun_state", () => Promise.resolve({
      label: "1 hr",
      mins: 60,
      mode: "timer",
      seconds_remaining: 3600,
      clock_time_available: false,
    }));
    window.dispatchEvent(new Event("autorun-state-changed"));
  });
  await expect(clock).toBeHidden();

  await page.evaluate(() => {
    window.__setBridgeHandler("autorun_state", () => Promise.resolve({
      label: "24 hr",
      mins: 1440,
      mode: "clock",
      seconds_remaining: 3600,
      clock_time_available: true,
      clock_anchor_minutes: 540,
    }));
    window.dispatchEvent(new Event("autorun-state-changed"));
  });
  await expect(clock).toBeVisible();
  await expect(page.locator("#settings-autorun-mode")).toHaveValue("clock");

  await page.locator('.tab[data-tab="settings"]').click();
  await expect(page.locator(
    '.yt-dd:has(+ #settings-autorun-mode) .yt-dd-trigger')).toBeVisible();

  await page.evaluate(() => {
    window.__setBridgeHandler("autorun_set_mode", () => Promise.resolve({
      ok: false,
      error: "Fixture could not save timing",
    }));
    const select = document.getElementById("settings-autorun-mode");
    select.value = "timer";
    select.dispatchEvent(new Event("change", { bubbles: true }));
  });
  await expect(page.locator("#settings-autorun-mode")).toHaveValue("clock");
  await expect(page.locator(".toast").last())
    .toContainText("Fixture could not save timing");

  await page.evaluate(() => {
    window.__setBridgeHandler("autorun_state", () => Promise.resolve({
      label: "When budget allows",
      mins: -1,
      mode: "timer",
      budget_mode: true,
      clock_time_available: false,
    }));
    window.dispatchEvent(new Event("autorun-state-changed"));
  });
  await expect(clock).toBeHidden();
});
