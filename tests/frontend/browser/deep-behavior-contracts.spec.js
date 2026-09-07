const { test, expect, loadApp } = require("./fixtures");

async function changeSelect(page, id, value) {
  await page.locator(`#${id}`).selectOption(value, { force: true });
}

test("updater preferences save actual choices, disable Off interval, and reject invalid days", async ({ page }) => {
  await loadApp(page);
  await page.locator('.tab[data-tab="settings"]').click();
  await page.locator("#settings-downloader-updates > summary").click();
  await expect(page.locator("#settings-ytdlp-update-mode")).toHaveValue("automatic");
  await changeSelect(page, "settings-ytdlp-update-mode", "off");
  await expect(page.locator("#settings-ytdlp-check-days")).toBeDisabled();
  await expect.poll(() => page.evaluate(() => window.__bridgeCallsFor("settings_save")
    .some(call => call.args[0].ytdlp_update_mode === "off"))).toBe(true);
  await changeSelect(page, "settings-ytdlp-update-mode", "notify");
  await expect(page.locator("#settings-ytdlp-check-days")).toBeEnabled();
  await expect(page.locator("#settings-ytdlp-update-note")).toContainText("without installing");
  const days = page.locator("#settings-ytdlp-check-days");
  await days.fill("14");
  await days.press("Tab");
  await expect.poll(() => page.evaluate(() => window.__bridgeCallsFor("settings_save")
    .some(call => call.args[0].ytdlp_update_check_days === 14))).toBe(true);
  await days.fill("366");
  await days.press("Tab");
  await expect(days).toHaveValue("14");
  expect(await page.evaluate(() => window.__bridgeCallsFor("settings_save")
    .some(call => call.args[0].ytdlp_update_check_days === 366))).toBe(false);
});

test("compact channel menu toggles the actual tab and persists the preference", async ({ page }) => {
  await loadApp(page);
  await page.locator('.tab[data-tab="browse"]').click();
  const channels = page.locator('.submode-btn[data-submode="channels"]');
  await channels.click({ button: "right" });
  const item = page.getByRole("menuitemcheckbox").filter({ hasText: "View Dense Subs List" });
  await expect(item).toHaveAttribute("aria-checked", "false");
  await item.click();
  await expect(page.locator('.tab[data-tab="subs"]')).toBeVisible();
  await expect.poll(() => page.evaluate(() => window.__bridgeCallsFor("settings_save")
    .some(call => call.args[0].legacy_subs_tab === true))).toBe(true);
  await page.locator('.tab[data-tab="subs"]').click({ button: "right" });
  await expect(item).toHaveAttribute("aria-checked", "true");
  await item.click();
  await expect(page.locator('.tab[data-tab="subs"]')).toBeHidden();
  await expect.poll(() => page.evaluate(() => window.__bridgeCallsFor("settings_save")
    .some(call => call.args[0].legacy_subs_tab === false))).toBe(true);
});

test("channel menu groups actions and Continue resumes the captured saved resolution", async ({ page }) => {
  await loadApp(page);
  await page.locator('.tab[data-tab="browse"]').click();
  await page.evaluate(() => {
    const channel = { name: "Example", folder: "Example", _pending_redownload: true, _redownload_res: "720" };
    window.__setBridgeHandler("chan_redownload", () => ({ ok: true, queued: true }));
    window._browseState.channels = [channel];
    window.renderChannelGrid([channel], () => {});
  });
  await page.locator('#channel-grid .channel-card[data-channel-name="Example"]').click({ button: "right" });
  const menu = page.locator("#ctx-menu-root > .ctx-menu");
  await expect(menu).toContainText("Open & manage");
  await expect(menu).toContainText("Maintenance");
  const labels = await menu.locator(":scope > .ctx-menu-item").allTextContents();
  const indexOf = text => labels.findIndex(label => label.trim().startsWith(text));
  expect(indexOf("Sync now")).toBeLessThan(indexOf("Edit settings"));
  expect(indexOf("Edit settings")).toBeLessThan(indexOf("Metadata"));
  await page.getByRole("menuitem", { name: "Continue redownload at 720p", exact: true }).click();
  await expect.poll(() => page.evaluate(() => window.__bridgeCallsFor("chan_redownload").length)).toBe(1);
  expect(await page.evaluate(() => window.__bridgeCallsFor("chan_redownload")[0].args))
    .toEqual([{ name: "Example" }, "720"]);
});

test("empty channel invitation opens the real Add dialog", async ({ page }) => {
  await loadApp(page);
  await page.locator('.tab[data-tab="browse"]').click();
  await page.locator('.submode-btn[data-submode="channels"]').click();
  const welcome = page.locator("#channel-grid .browse-first-channel");
  await expect(welcome).toContainText("Add your first channel");
  await welcome.getByRole("button").click();
  await expect(page.locator("#channel-editor-backdrop")).toBeVisible();
  await expect(page.locator("#edit-url")).toHaveValue("");
});

test("Browse Edit opens the modern dialog even while compact Subs is enabled", async ({ page }) => {
  await loadApp(page, { bridge: { settings: { legacy_subs_tab: true } } });
  await page.locator('.tab[data-tab="browse"]').click();
  await page.evaluate(() => {
    const channel = { name: "Example", folder: "Example", url: "https://www.youtube.com/@Example" };
    window.__setBridgeHandler("subs_get_channel", () => ({ ok: true, channel }));
    window._browseState.channels = [channel];
    window.renderChannelGrid([channel], () => {});
  });
  await page.locator('#channel-grid .channel-card[data-channel-name="Example"]').click({ button: "right" });
  await page.getByRole("menuitem", { name: "Edit settings", exact: true }).click();
  await expect(page.locator("#channel-editor-backdrop")).toBeVisible();
  await expect(page.locator("#edit-url")).toHaveValue("https://www.youtube.com/@Example");
  await expect(page.locator("#edit-url")).toBeEnabled();
});

test("clearing session errors sends zero to the native indicator", async ({ page }) => {
  await loadApp(page);
  // The real counter intentionally ignores startup's historical seed.
  await page.waitForTimeout(2600);
  await page.evaluate(() => {
    const line = document.createElement("div");
    line.className = "log-line";
    const span = document.createElement("span");
    span.className = "t-error_detail";
    span.textContent = "Fixture operation failed";
    line.append(span);
    document.getElementById("main-log").append(line);
  });
  await expect(page.locator("#gsb-errors-count")).toHaveText("1");
  await expect.poll(() => page.evaluate(() => window.__bridgeCallsFor("app_session_errors_changed")
    .at(-1)?.args[0]?.count)).toBe(1);
  await page.locator("#gsb-errors").click();
  await page.locator("#gsb-errors-clear").click();
  await expect(page.locator("#gsb-errors")).toBeHidden();
  await expect.poll(() => page.evaluate(() => window.__bridgeCallsFor("app_session_errors_changed")
    .at(-1)?.args[0]?.count)).toBe(0);
});

test("queue edges refresh traffic and retain one trailing read during an in-flight request", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    window.__trafficReads = 0;
    window.__setBridgeHandler("youtube_traffic_status", () => {
      window.__trafficReads += 1;
      const snapshot = { ok: true, mode: "balanced", hourly_limit: 100, daily_limit: 1000,
        hourly_used: window.__trafficReads === 1 ? 2 : 9, daily_used: 20 };
      return window.__trafficReads === 1 ? new Promise(resolve => {
        window.__finishTraffic = () => resolve(snapshot);
      }) : snapshot;
    });
    window.setQueueState({ sync: { count: 1, running: true }, gpu: {} });
  });
  await expect.poll(() => page.evaluate(() => window.__trafficReads)).toBe(1);
  await page.evaluate(() => window.setQueueState({ sync: { count: 1, running: true, trafficWaiting: true }, gpu: {} }));
  // Allow the edge's debounce callback to observe the outstanding request.
  await page.waitForTimeout(80);
  await page.evaluate(() => window.__finishTraffic());
  await expect.poll(() => page.evaluate(() => window.__trafficReads)).toBe(2);
  await expect(page.locator("#gsb-traffic-hourly")).toHaveText("Hour 9/100");
});

for (const hold of ["trafficWaiting", "sessionLimited"]) {
  test(`${hold} parks Sync and keeps only the custom pause tooltip`, async ({ page }) => {
    await loadApp(page);
    await page.evaluate(hold => window.setQueueState({
      sync: { running: true, count: 1, [hold]: true }, gpu: {},
    }), hold);
    await expect(page.locator("#btn-sync-tasks")).toHaveAttribute("data-blink-state", "paused");
    await page.waitForTimeout(760);
    await expect(page.locator("#btn-sync-tasks")).toHaveAttribute("data-blink-state", "paused");
    await page.locator("#btn-sync-tasks").click();
    const pause = page.locator("#btn-pause-sync-queue");
    await expect(pause).toHaveAttribute("data-tooltip", /queue/);
    await expect(pause).not.toHaveAttribute("title");
  });
}
