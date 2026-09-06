const { test, expect } = require("@playwright/test");
const { loadApp } = require("./fixtures");

async function openBackups(page) {
  await page.locator('.tab[data-tab="health"]').click();
  await page.locator('#panel-health [data-settings-view="backups"]').click();
}

test("large-library export clearly includes bookmarks despite omitted Search index", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    window.__messages = [];
    window._showToast = (message) => window.__messages.push(message);
    window.__setBridgeHandler("export_full_backup", () => ({
      ok: true, files: 5, fts_skipped: "Search index exceeds size limit",
      bookmarks_included: true, bookmark_count: 2,
    }));
  });
  await openBackups(page);
  await page.locator("#btn-export-backup").click();
  await expect.poll(() => page.evaluate(() => window.__messages.join(" ")))
    .toContain("Your bookmarks and notes are included.");
});

for (const source of ["backup", "current_installation"]) {
  test(`restore explains bookmark source: ${source}`, async ({ page }) => {
    await loadApp(page);
    await page.evaluate((source) => {
      window.__messages = [];
      window._showToast = (message) => window.__messages.push(message);
      window.__setBridgeHandler("import_full_backup_preview", () => ({
        ok: true, zip_path: "C:\\Fixture\\backup.zip", items: [], total_label: "1 KB",
        bookmarks_included: source === "backup",
      }));
      window.__setBridgeHandler("import_full_backup", () => ({
        ok: true, files_restored: 3, bookmarks_source: source, bookmark_count: 2,
      }));
    }, source);
    await openBackups(page);
    await page.locator("#btn-import-backup").click();
    const dialog = page.getByRole("dialog", { name: "Restore this backup?" });
    await expect(dialog).toContainText(source === "backup"
      ? "Bookmarks and notes will be restored from this backup."
      : "Your current bookmarks and notes will be retained");
    await dialog.locator('[data-act="confirm"]').click();
    await expect.poll(() => page.evaluate(() => window.__messages.join(" ")))
      .toContain(source === "backup" ? "restored from the backup" : "current bookmarks and notes were retained");
  });
}

test("saving a primary archive refreshes the root list and a failed save preserves it", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    window.__fixtureSettings = { output_dir: "C:\\FixtureArchive", tp_archive_roots: ["D:\\Additional"] };
    window.__rejectFolderSave = false;
    window.__setBridgeHandler("settings_load", () => ({ ...window.__fixtureSettings }));
    window.__setBridgeHandler("pick_folder", () => ({ ok: true, path: window.__rejectFolderSave
      ? "C:\\Rejected" : "C:\\NewPrimary" }));
    window.__setBridgeHandler("settings_save", (updates) => {
      if (window.__rejectFolderSave) return { ok: false, error: "Fixture rejection" };
      Object.assign(window.__fixtureSettings, updates);
      return { ok: true };
    });
  });
  await page.locator('.tab[data-tab="settings"]').click();
  await page.locator("#settings-browse-output").click();
  await expect(page.locator("#settings-roots-list .auto")).toHaveText("Primary — C:\\NewPrimary");
  await expect(page.locator("#settings-roots-list")).toContainText("D:\\Additional");
  await page.evaluate(() => { window.__rejectFolderSave = true; });
  await page.locator("#settings-browse-output").click();
  await expect(page.locator("#settings-output-dir")).toHaveValue("C:\\NewPrimary");
  await expect(page.locator("#settings-roots-list .auto")).toHaveText("Primary — C:\\NewPrimary");
});
