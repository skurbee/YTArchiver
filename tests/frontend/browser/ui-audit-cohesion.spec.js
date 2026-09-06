const { test, expect } = require("@playwright/test");
const { loadApp } = require("./fixtures");

test.beforeEach(async ({ page }) => { await loadApp(page); });

test("subscription refresh retains filtering and keyboard selection updates bulk controls", async ({ page }) => {
  await page.evaluate(async () => {
    const settings = await window.pywebview.api.settings_load();
    // Startup reloads settings after wiring controls. Keep the saved choice
    // consistent so that reload cannot hide the tab while this test uses it.
    window.__setBridgeHandler("settings_load", () => ({ ...settings, legacy_subs_tab: true }));
    window._applyLegacySubsMode(true);
    const tab = document.querySelector('.tab[data-tab="subs"]');
    tab.click();
    window.fixtureSubs = [{ folder: "Apple" }, { folder: "Banana" }, { folder: "Apricot" }];
    window.renderSubsTable(window.fixtureSubs);
  });
  await page.locator("#subs-filter").fill("Ap");
  await page.evaluate(() => window.renderSubsTable(window.fixtureSubs));
  await expect(page.locator("#subs-table-body tr")).toHaveCount(2);
  await expect(page.locator("#subs-filter")).toHaveValue("Ap");
  const selection = await page.evaluate(() => {
    const body = document.getElementById("subs-table-body");
    const rows = body.querySelectorAll("tr");
    rows[0].dispatchEvent(new MouseEvent("click", { bubbles: true }));
    rows[1].dispatchEvent(new MouseEvent("click", { bubbles: true, ctrlKey: true }));
    const before = document.getElementById("subs-bulk-count").textContent;
    body.dispatchEvent(new KeyboardEvent("keydown", { key: "ArrowDown", bubbles: true }));
    return { before, after: document.getElementById("subs-bulk-count").textContent,
      selected: body.querySelectorAll("tr.row-selected").length,
      hidden: document.getElementById("subs-bulk-bar").hidden };
  });
  expect(selection).toEqual({ before: "2 channels selected", after: "1 channel selected", selected: 1, hidden: true });
});

test("Queue all waits for transcript layout and cancellation starts no work", async ({ page }) => {
  await page.evaluate(() => {
    window.bulkCalls = [];
    window.askConfirm = async () => true;
    window.__setBridgeHandler("subs_queue_all", (...args) => {
      window.bulkCalls.push(args);
      return args.length ? { ok: true, started: true }
        : { ok: true, needs_choice: true, channels: ["Example"] };
    });
    document.getElementById("btn-queue-pending").dispatchEvent(new MouseEvent("contextmenu", { bubbles: true }));
  });
  await expect(page.getByRole("dialog", { name: "Transcript layout" })).toBeVisible();
  expect(await page.evaluate(() => window.bulkCalls)).toEqual([[]]);
  await page.getByRole("button", { name: "Cancel", exact: true }).click();
  expect(await page.evaluate(() => window.bulkCalls)).toEqual([[]]);
  await page.evaluate(() => document.getElementById("btn-queue-pending").dispatchEvent(new MouseEvent("contextmenu", { bubbles: true })));
  await page.getByRole("button", { name: "One combined transcript per channel", exact: true }).click();
  await expect.poll(() => page.evaluate(() => window.bulkCalls)).toEqual([[], [], [true]]);
});

test("partial availability refreshes stay unknown while complete coverage is reported", async ({ page }) => {
  await page.evaluate(async () => {
    window.availabilityRows = [
      { name: "Partial", last_views_refresh_ts: Date.now() / 1000 },
      { name: "Complete", last_availability_check_ts: Date.now() / 1000, availability_checked_count: 10 },
      { name: "Changed archive", last_availability_check_ts: Date.now() / 1000, availability_checked_count: 9 },
    ].map(row => ({ ...row, folder: row.name, id_total: 10, id_with_id: 10, removed_from_yt: 0 }));
    window.__setBridgeHandler("get_channel_metadata_status", () => window.availabilityRows);
    window.__setBridgeHandler("thumbnail_status_bulk", () => ({ ok: true, rows: {} }));
    document.querySelector('.tab[data-tab="health"]').click();
    document.querySelector('#panel-health [data-settings-view="library"]').click();
    await window._refreshMetadataTab({ force: true });
  });
  const row = name => page.locator("#metadata-tbody tr").filter({ has: page.locator(".md-col-name", { hasText: name }) });
  await expect(row("Partial").locator('[title*="not been fully checked"]')).toHaveCount(1);
  await expect(row("Changed archive").locator('[title*="not been fully checked"]')).toHaveCount(1);
  await expect(row("Complete").locator('[title*="full availability check"]')).toHaveText("✓ 100%");
});

test("late transcript repair cannot overwrite a reopened dialog", async ({ page }) => {
  await page.evaluate(() => {
    window.YT.util.loadSubsChannels = async () => [{ name: "Example", folder: "Example" }];
    window.__setBridgeHandler("drift_scan_channel", () => ({ ok: true, jsonl_without_txt: [{ title: "Missing text" }] }));
    window.__setBridgeHandler("drift_apply_channel", () => new Promise(resolve => { window.finishOldFix = resolve; }));
    document.getElementById("btn-drift-scan").click();
  });
  await page.locator("#drift-scan-btn").click();
  await expect(page.locator("#drift-fix-btn")).toBeEnabled();
  await page.locator("#drift-fix-btn").click();
  await expect(page.locator("#drift-body")).toHaveText("Applying fixes…");
  await page.locator("#drift-close").click();
  await page.evaluate(() => document.getElementById("btn-drift-scan").click());
  await expect(page.locator("#drift-body")).toHaveText("Pick a channel and click Scan.");
  await page.evaluate(() => window.finishOldFix({ ok: true, actions: { txt_reconstructed: 1 } }));
  await page.waitForTimeout(50);
  await expect(page.locator("#drift-body")).toHaveText("Pick a channel and click Scan.");
  await expect(page.locator("#drift-fix-btn")).toBeDisabled();
});

test("supported small windows keep Search, Watch and history reachable", async ({ page }) => {
  await page.setViewportSize({ width: 640, height: 480 });
  await page.locator('.tab[data-tab="browse"]').click();
  await page.locator('[data-submode="search"]').click();
  const search = await page.locator("#view-search").evaluate(el => ({ client: el.clientWidth, scroll: el.scrollWidth }));
  expect(search.scroll).toBeLessThanOrEqual(search.client + 1);
  await page.evaluate(() => {
    window.showView("watch");
    document.getElementById("watch-transcript").innerHTML = "<p>Transcript segment</p>".repeat(50);
  });
  await page.locator("#watch-find").scrollIntoViewIfNeeded();
  const watch = await page.locator("#view-watch").evaluate(el => ({ overflow: getComputedStyle(el).overflowY, scroll: el.scrollTop }));
  expect(watch.overflow).toBe("auto");
  expect(watch.scroll).toBeGreaterThan(0);
  await page.evaluate(() => { document.getElementById("autorun-history-backdrop").hidden = false; });
  const history = await page.locator("#autorun-history-backdrop .askq-dialog").boundingBox();
  expect(history.x).toBeGreaterThanOrEqual(0);
  expect(history.x + history.width).toBeLessThanOrEqual(640);
  const grids = await page.locator(".askq-dialog .edit-grid").evaluateAll(els => els.map(el => getComputedStyle(el).display));
  expect(grids.length).toBeGreaterThanOrEqual(4);
  expect(grids.every(display => display === "grid")).toBe(true);
});

test("known-ID missing bookmarks never resolve a different video by title", async ({ page }) => {
  await page.evaluate(() => {
    window.fallbackCalls = [];
    window.bookmarkMessages = [];
    window._showToast = message => window.bookmarkMessages.push(message);
    window.__setBridgeHandler("bookmark_list", () => [{ id: 1, title: "Reused title", channel: "Example", video_id: "MISSING0001", filepath: "" }]);
    window.__setBridgeHandler("recent_resolve", (...args) => { window.fallbackCalls.push(args); return { filepath: "wrong.mp4" }; });
    document.querySelector('.tab[data-tab="browse"]').click();
    document.querySelector('[data-submode="bookmarks"]').click();
  });
  await page.locator(".bookmark-card").click();
  await expect.poll(() => page.evaluate(() => window.bookmarkMessages.join(" "))).toContain("unavailable");
  expect(await page.evaluate(() => window.fallbackCalls)).toEqual([]);
});
