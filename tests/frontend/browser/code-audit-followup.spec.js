const { test, expect } = require("@playwright/test");
const { loadApp } = require("./fixtures");


async function mouseHistory(page, button) {
  await page.evaluate((mouseButton) => {
    for (const type of ["mousedown", "mouseup", "auxclick"]) {
      window.dispatchEvent(new MouseEvent(type, {
        button: mouseButton,
        bubbles: true,
        cancelable: true,
      }));
    }
  }, button);
}


test("static dialogs contain focus and pause mouse navigation history", async ({ page }) => {
  await loadApp(page);
  await page.locator('.tab[data-tab="health"]').click();
  await page.locator('.tab[data-tab="settings"]').click();

  await page.locator("#settings-about-troubleshooting").evaluate((details) => {
    details.open = true;
  });
  await page.locator("#btn-about").click();
  const dialog = page.getByRole("dialog", { name: "About YTArchiver" });
  const close = page.getByRole("button", { name: "Close" });
  await expect(dialog).toBeVisible();
  await expect(dialog).toHaveAttribute("aria-modal", "true");
  await expect(close).toBeFocused();

  await page.keyboard.press("Tab");
  await expect(close).toBeFocused();
  const before = await page.evaluate(() =>
    window.YT.navigationHistory.snapshot());
  await mouseHistory(page, 3);
  const whileOpen = await page.evaluate(() =>
    window.YT.navigationHistory.snapshot());
  expect(whileOpen.position).toBe(before.position);
  await expect(page.locator('.tab.active[data-tab="settings"]')).toBeVisible();

  await close.click();
  await expect(page.locator("#btn-about")).toBeFocused();
  await mouseHistory(page, 3);
  await expect(page.locator('.tab.active[data-tab="health"]')).toBeVisible();
});


test("enhanced selects and the Download splitter keep useful keyboard semantics", async ({ page }) => {
  await loadApp(page);
  await page.locator('.tab[data-tab="settings"]').click();

  const quality = page.locator(
    '.yt-dd:has(+ #settings-default-res) .yt-dd-trigger',
  );
  await expect(quality).toHaveAttribute("role", "combobox");
  await expect(quality).toHaveAccessibleName("Default video quality");

  await page.locator('.tab[data-tab="download"]').click();
  await page.locator("#activity-log").evaluate((log) => {
    log.appendChild(document.createElement("div"));
    window._syncActivityLogVisibility?.();
  });
  const splitter = page.getByRole("separator", {
    name: "Resize activity log",
  });
  await splitter.focus();
  await page.keyboard.press("Home");
  const minimum = Number(await splitter.getAttribute("aria-valuemin"));
  await expect(splitter).toHaveAttribute("aria-valuenow", String(minimum));
  await page.keyboard.press("ArrowDown");
  await expect(splitter).toHaveAttribute(
    "aria-valuenow", String(minimum + 16));

  await splitter.evaluate((element) => {
    element.dispatchEvent(new CustomEvent("ytarchiver:splitter-restore", {
      detail: { height: 999999 },
    }));
  });
  const values = await splitter.evaluate((element) => ({
    now: Number(element.getAttribute("aria-valuenow")),
    max: Number(element.getAttribute("aria-valuemax")),
  }));
  expect(values.now).toBe(values.max);
});


test("metadata sorting and channel actions are fully keyboard reachable", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    localStorage.removeItem("ytarchiver_meta_rows");
    window.__setBridgeHandler("get_channel_metadata_status", () => [{
      name: "Fixture Channel",
      folder: "Fixture Channel",
      url: "https://youtube.example/@fixture",
      video_count: 3,
      id_total: 3,
      id_with_id: 3,
      tx_total: 3,
      tx_transcribed: 2,
    }]);
    window.__setBridgeHandler("thumbnail_status_bulk", () => ({ rows: {} }));
  });
  await page.locator('.tab[data-tab="health"]').click();
  await page.locator('[data-settings-view="library"]').click();
  await page.locator("#health-library-metadata").evaluate((details) => {
    details.open = true;
  });
  await page.evaluate(() => window._refreshMetadataTab({ force: true }));

  const viewsHeader = page.locator('#metadata-table th[data-sort="views"]');
  const nameHeader = page.locator('#metadata-table th[data-sort="name"]');
  await expect(viewsHeader).toHaveAttribute("aria-sort", "ascending");
  await nameHeader.focus();
  await page.keyboard.press("Enter");
  await expect(nameHeader).toHaveAttribute("aria-sort", "ascending");
  await expect(page.locator("#metadata-table th.md-col-act"))
    .not.toHaveAttribute("aria-sort", /.+/);

  const row = page.locator("#metadata-tbody tr.md-row-clickable");
  await row.focus();
  await page.keyboard.press("Shift+F10");
  const menu = page.locator(".md-context-menu");
  await expect(menu).toBeVisible();
  await expect(menu.getByRole("menuitem", { name: "Transcribe missing" }))
    .toBeFocused();
  const views = menu.getByRole("menuitem", { name: /Refresh views\/likes/ });
  await page.keyboard.press("ArrowDown");
  await expect(views).toBeFocused();
  await page.keyboard.press("ArrowRight");
  await expect(menu.getByRole("menuitem", { name: "Last week" }))
    .toBeFocused();
  await page.keyboard.press("ArrowLeft");
  await expect(views).toBeFocused();
  await expect(views).toHaveAttribute("aria-expanded", "false");
  await page.keyboard.press("Escape");
  await expect(menu).toBeHidden();
  await expect(row).toBeFocused();
});


test("a failed traffic-mode save cannot turn automatic syncing off", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    window.__setBridgeHandler("settings_load", () => ({
      output_dir: "C:\\FixtureArchive",
      youtube_traffic_mode: "balanced",
      youtube_traffic: {
        ok: true,
        mode: "balanced",
        daily_limit: 1000,
        hourly_limit: 100,
        hourly_used: 0,
        daily_used: 0,
        hourly_remaining: 100,
        daily_remaining: 1000,
        projection: {},
      },
    }));
    window.__setBridgeHandler("settings_save", () => ({
      ok: false,
      error: "Fixture save failure",
    }));
  });
  await page.locator('.tab[data-tab="settings"]').click();
  const mode = page.locator("#settings-youtube-traffic-mode");
  await expect(mode).toHaveValue("balanced");
  await page.locator("#auto-sync-select").evaluate((select) => {
    select.value = "When budget allows";
    select._ytddRepaint?.();
  });

  await mode.evaluate((select) => {
    select.value = "unlimited";
    select.dispatchEvent(new Event("change", { bubbles: true }));
  });

  await expect(mode).toHaveValue("balanced");
  await expect(page.locator("#auto-sync-select"))
    .toHaveValue("When budget allows");
  expect(await page.evaluate(() =>
    window.__bridgeCallsFor("autorun_set").length)).toBe(0);
});


test("hiding Download never persists a zero-height log splitter", async ({ page }) => {
  await loadApp(page);
  await expect.poll(() => page.evaluate(() =>
    window.__bridgeCallsFor("window_state_load").length)).toBeGreaterThan(0);
  await page.evaluate(() => {
    window.__splitterSaves = [];
    window.__setBridgeHandler("window_state_save", (value) => {
      window.__splitterSaves.push(value);
      return { ok: true };
    });
  });
  await page.locator("#activity-log").evaluate((log) => {
    log.appendChild(document.createElement("div"));
    window._syncActivityLogVisibility?.();
  });
  const splitter = page.getByRole("separator", {
    name: "Resize activity log",
  });
  await splitter.evaluate((element) => {
    element.dispatchEvent(new CustomEvent("ytarchiver:splitter-restore", {
      detail: { height: 120 },
    }));
    element.dispatchEvent(new CustomEvent("ytarchiver:splitter-adjusted"));
  });
  await page.locator('.tab[data-tab="settings"]').click();
  await expect.poll(() => page.evaluate(() =>
    window.__splitterSaves.length)).toBeGreaterThan(0);
  const savedHeights = await page.evaluate(() =>
    window.__splitterSaves.map((value) => value.splitter_top_px));
  expect(savedHeights.every((height) => height > 0)).toBe(true);
});
