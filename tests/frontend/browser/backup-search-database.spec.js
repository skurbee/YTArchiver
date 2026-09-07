const { test, expect, loadApp } = require("./fixtures");

async function openBackups(page) {
  await page.locator('.tab[data-tab="health"]').click();
  await page.locator('#panel-health [data-settings-view="backups"]').click();
}

test("Search database is included by default and its actual large size is visible", async ({ page }) => {
  await loadApp(page, { bridge: { settings: {
    backup_search_db_size_bytes: 35 * 1024 ** 3,
    backup_search_db_size_label: "35.0 GiB",
  } } });
  await openBackups(page);
  await expect(page.getByRole("checkbox", { name: /Include Search database/ })).toBeChecked();
  await expect(page.locator("#backup-search-db-size")).toHaveText("(35.0 GiB)");
  await expect(page.locator("#backup-search-db-help")).toContainText("manual and automatic backups");
  await expect(page.locator("#backup-search-db-help")).toContainText("bookmarks and notes are still included");
  await expect(page.locator("#settings-view-backups .health-view-intro")).toContainText("no size limit");
  expect(await page.evaluate(() => window.__bridgeCallsFor("settings_save"))).toEqual([]);
});

test("Search database opt-out persists across reload and can be enabled again", async ({ page }) => {
  await loadApp(page);
  await openBackups(page);
  const choice = page.locator("#settings-backup-include-search-db");
  await choice.uncheck();
  await expect(choice).toBeEnabled();
  expect(await page.evaluate(() => window.__bridgeCallsFor("settings_save").at(-1).args))
    .toEqual([{ backup_include_search_db: false }]);
  await page.reload();
  await page.evaluate(() => window.YT.settingsReady);
  await openBackups(page);
  await expect(choice).not.toBeChecked();
  await choice.check();
  await expect(choice).toBeEnabled();
  expect(await page.evaluate(() => window.__bridgeCallsFor("settings_save").at(-1).args))
    .toEqual([{ backup_include_search_db: true }]);
});

test("failed preference save restores the saved choice and blocks export until resolved", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    window.__setBridgeHandler("settings_save", () => new Promise(resolve => {
      window.__resolveBackupPreference = resolve;
    }));
    window.__messages = [];
    window._showToast = message => window.__messages.push(message);
  });
  await openBackups(page);
  const choice = page.locator("#settings-backup-include-search-db");
  await choice.uncheck();
  await expect(choice).toBeDisabled();
  await expect(page.locator("#btn-export-backup")).toBeDisabled();
  await page.evaluate(() => {
    document.getElementById("btn-export-backup").dispatchEvent(new MouseEvent("click"));
    window.__resolveBackupPreference({ ok: false, error: "Fixture preference save failed" });
  });
  await expect(choice).toBeEnabled();
  await expect(choice).toBeChecked();
  await expect(page.locator("#btn-export-backup")).toBeEnabled();
  expect(await page.evaluate(() => window.__bridgeCallsFor("export_full_backup"))).toEqual([]);
  expect(await page.evaluate(() => window.__messages.join(" "))).toContain("Fixture preference save failed");
});

for (const scenario of [
  { name: "unavailable", settings: { backup_search_db_size_bytes: null }, expected: "(size unavailable)" },
  { name: "not created", settings: { backup_search_db_size_bytes: null,
    backup_search_db_size_label: "not created yet" }, expected: "(not created yet)" },
  { name: "zero bytes", settings: { backup_search_db_size_bytes: 0 }, expected: "(not created yet)" },
  { name: "numeric fallback", settings: { backup_search_db_size_bytes: 35 * 1024 ** 3 }, expected: "(35 GiB)" },
]) {
  test(`Search database size is truthful when ${scenario.name}`, async ({ page }) => {
    await loadApp(page, { bridge: { settings: scenario.settings } });
    await openBackups(page);
    await expect(page.locator("#backup-search-db-size")).toHaveText(scenario.expected);
  });
}

test("Search database size refreshes when Health is reopened", async ({ page }) => {
  await loadApp(page, { bridge: { settings: { backup_search_db_size_label: "3.0 GiB" } } });
  await openBackups(page);
  await expect(page.locator("#backup-search-db-size")).toHaveText("(3.0 GiB)");
  await page.evaluate(() => {
    const saved = window.__fixtureDefaultResult("settings_load");
    window.__setBridgeHandler("settings_load", () => ({ ...saved,
      backup_search_db_size_label: "3.5 GiB" }));
  });
  await page.locator('.tab[data-tab="settings"]').click();
  await openBackups(page);
  await expect(page.locator("#backup-search-db-size")).toHaveText("(3.5 GiB)");
});

test("a pending backup stays visible and cannot be started twice", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    window.__setBridgeHandler("export_full_backup", () => new Promise(resolve => {
      window.__resolveBackupExport = resolve;
    }));
  });
  await openBackups(page);
  const save = page.locator("#btn-export-backup");
  await save.click();
  await expect(save).toHaveText("Saving backup\u2026");
  await expect(save).toHaveAttribute("aria-busy", "true");
  await expect(save).toBeDisabled();
  await expect(page.locator("#settings-backup-include-search-db")).toBeDisabled();
  await expect(page.locator("#btn-import-backup")).toBeDisabled();
  await page.evaluate(() => {
    document.getElementById("btn-export-backup").dispatchEvent(new MouseEvent("click"));
  });
  await page.locator('.tab[data-tab="settings"]').click();
  await openBackups(page);
  await expect(save).toBeDisabled();
  expect(await page.evaluate(() => window.__bridgeCallsFor("export_full_backup").length)).toBe(1);
  await page.evaluate(() => window.__resolveBackupExport({ ok: true, files: 8,
    bookmarks_included: true, last_backup_ts: Date.now() / 1000, path: "C:\\Fixture\\complete.zip" }));
  await expect(save).toBeEnabled();
  await expect(save).toHaveText("Save backup\u2026");
  await expect(save).toHaveAttribute("aria-busy", "false");
  await expect(page.locator("#settings-backup-include-search-db")).toBeEnabled();
  await expect(page.locator("#btn-import-backup")).toBeEnabled();
  await expect(page.locator("#backup-age-display")).toContainText("C:\\Fixture\\complete.zip");
});

for (const outcome of ["cancelled", "failed", "rejected"]) {
  test(`backup controls recover when export is ${outcome}`, async ({ page }) => {
    await loadApp(page);
    await page.evaluate(outcome => {
      window.__setBridgeHandler("export_full_backup", () => {
        if (outcome === "rejected") return Promise.reject(new Error("Fixture export rejected"));
        return outcome === "cancelled" ? { cancelled: true }
          : { ok: false, error: "Fixture export failed" };
      });
      window.__messages = [];
      window._showToast = message => window.__messages.push(message);
    }, outcome);
    await openBackups(page);
    const save = page.locator("#btn-export-backup");
    await save.click();
    await expect(save).toBeEnabled();
    await expect(save).toHaveText("Save backup\u2026");
    await expect(page.locator("#settings-backup-include-search-db")).toBeEnabled();
    await expect(page.locator("#btn-import-backup")).toBeEnabled();
    const messages = await page.evaluate(() => window.__messages.join(" "));
    if (outcome === "cancelled") expect(messages).toBe("");
    else expect(messages).toMatch(/failed|rejected/i);
  });
}

test("an intentionally omitted database reports the returned reason and preserved bookmarks", async ({ page }) => {
  await loadApp(page, { bridge: { settings: { backup_include_search_db: false } } });
  await page.evaluate(() => {
    window.__setBridgeHandler("export_full_backup", () => ({ ok: true, files: 7,
      fts_skipped: "Search database excluded by your backup setting.", bookmarks_included: true }));
    window.__messages = [];
    window._showToast = message => window.__messages.push(message);
  });
  await openBackups(page);
  await expect(page.locator("#settings-backup-include-search-db")).not.toBeChecked();
  await page.locator("#btn-export-backup").click();
  await expect.poll(() => page.evaluate(() => window.__messages.join(" ")))
    .toContain("Search database excluded by your backup setting.");
  const messages = await page.evaluate(() => window.__messages.join(" "));
  expect(messages).toContain("Your bookmarks and notes are included.");
  expect(messages).not.toContain("too large");
});
