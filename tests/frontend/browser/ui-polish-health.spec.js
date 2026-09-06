const { test, expect } = require("@playwright/test");
const { loadApp } = require("./fixtures");

async function health(page, view = "overview") {
  await page.locator('.tab[data-tab="health"]').click();
  await page.locator(`#panel-health [data-settings-view="${view}"]`).click();
}

async function metadata(page) {
  await page.evaluate(() => {
    const rows = [
      { name: "Complete", folder: "Complete", id_missing: 0, id_total: 100, id_with_id: 100 },
      { name: "Few missing", folder: "Few missing", id_missing: 2, id_total: 5, id_with_id: 3 },
      { name: "Most missing", folder: "Most missing", id_missing: 12, id_total: 100, id_with_id: 88 },
    ].map(row => ({ ...row, url: `https://www.youtube.com/@${row.folder.replaceAll(" ", "")}`,
      tx_total: row.id_total, tx_transcribed: row.id_total - 1 }));
    window.__setBridgeHandler("get_channel_metadata_status", () => rows);
    window.__setBridgeHandler("thumbnail_status_bulk", () => ({ channels: [] }));
    window.__setBridgeHandler("get_index_summary", () => ({ cards: {
      videos: 205, channels: 3, physical_copies: 206, size_label: "4 GB",
      scan_complete: true, scanned_channels: 3, total_channels: 3,
    } }));
    window.__setBridgeHandler("index_summary", () => ({ videos: 310, channels: 4, segments: 900 }));
  });
}

test("Health identifies count scopes and missing-ID warning targets affected channels", async ({ page }) => {
  await loadApp(page);
  await metadata(page);
  await health(page);
  await page.evaluate(() => window._refreshHealthOverview());
  await expect(page.locator("#settings-view-overview")).toContainText("Saved channel scan");
  await expect(page.locator("#settings-view-overview")).toContainText("3 of 3 current channels");
  await expect(page.locator("#settings-view-overview")).toContainText("Full catalog · all archive roots");
  await expect(page.locator("#settings-view-overview")).toContainText("current subscriptions, main archive");
  await page.locator("#health-attention-list button").filter({ hasText: "missing a video ID" }).click();
  await page.evaluate(() => window._refreshMetadataTab({ force: true }));
  await expect(page.locator(".metadata-filter-notice")).toBeVisible();
  await expect(page.locator("#metadata-tbody .md-row-clickable")).toHaveCount(2);
  await expect(page.locator("#metadata-tbody .md-row-clickable").first()).toContainText("Most missing");
  await page.locator('#metadata-table th[data-sort="name"]').click();
  await expect(page.locator("#metadata-tbody .md-row-clickable").first()).toContainText("Few missing");
  await expect(page.locator("#metadata-tbody .md-row-clickable")).toHaveCount(2);
  await page.getByRole("button", { name: "Show all channels", exact: true }).click();
  await expect(page.locator("#metadata-tbody .md-row-clickable")).toHaveCount(3);
});

test("metadata flyout stays inside a small window and keeps keyboard navigation", async ({ page }) => {
  await page.setViewportSize({ width: 640, height: 480 });
  await loadApp(page);
  await metadata(page);
  await health(page, "library");
  await page.evaluate(() => {
    document.getElementById("health-library-metadata").open = true;
    return window._refreshMetadataTab({ force: true });
  });
  await page.locator("#metadata-tbody .md-row-clickable").first().evaluate(row => {
    row.dispatchEvent(new MouseEvent("click", { bubbles: true, clientX: 630, clientY: 465 }));
  });
  const head = page.locator(".md-context-menu .md-cm-has-sub").first();
  await head.focus();
  await page.keyboard.press("ArrowRight");
  const sub = head.locator("xpath=following-sibling::*[1]");
  await expect(sub).toBeVisible();
  const bounds = await sub.boundingBox();
  expect(bounds.x).toBeGreaterThanOrEqual(0);
  expect(bounds.y).toBeGreaterThanOrEqual(0);
  expect(bounds.x + bounds.width).toBeLessThanOrEqual(641);
  expect(bounds.y + bounds.height).toBeLessThanOrEqual(481);
  await expect(sub.locator(".md-cm-item").first()).toBeFocused();
  await page.keyboard.press("ArrowLeft");
  await expect(head).toBeFocused();
  await expect(sub).toBeHidden();
  await page.keyboard.press("Escape");
  await expect(page.locator(".md-context-menu")).toHaveCount(0);
});

test("backup preview leads with file identity and distinguishes an older backup's file date", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    window.__setBridgeHandler("import_full_backup_preview", () => ({
      ok: true, zip_path: "C:\\Fixture\\older-backup.zip", zip_name: "older-backup.zip",
      zip_modified_at: 1788562800, items: [{ name: "ytarchiver_config.json", size_label: "1 KB", modified: "unknown" }],
      total_label: "1 KB", bookmarks_included: false, index_included: false,
    }));
  });
  await health(page, "backups");
  await page.locator("#btn-import-backup").click();
  const dialog = page.getByRole("dialog", { name: "Restore this backup?" });
  await expect(dialog.locator(".backup-preview-identity strong")).toHaveText("older-backup.zip");
  await expect(dialog.locator(".backup-preview-identity")).toContainText("Creation time was not recorded");
  await expect(dialog.locator(".backup-preview-identity")).toContainText("ZIP file modified:");
  await expect(dialog.locator(".backup-preview-identity")).toContainText("Settings and subscriptions included");
  await expect(dialog.locator(".backup-preview-frame")).toBeHidden();
  await dialog.getByText("Included files (1)", { exact: true }).click();
  await expect(dialog.locator(".backup-preview-frame")).toBeVisible();
  await dialog.locator('[data-act="cancel"]').click();
  expect(await page.evaluate(() => window.__bridgeCallsFor("import_full_backup").length)).toBe(0);
});

test("backup age is exact with relative hours and its saved location remains visible", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    window.__setBridgeHandler("settings_load", () => ({
      output_dir: "C:\\Fixture", auto_backup_interval: "weekly",
      last_backup_ts: Date.now() / 1000 - 23 * 3600,
      last_backup_path: "C:\\Fixture\\chosen.zip",
      last_auto_backup_ts: Date.now() / 1000 - 23 * 3600,
      last_auto_backup_path: "C:\\Fixture\\automatic.zip",
    }));
  });
  await health(page, "backups");
  await expect(page.locator("#backup-age-display")).toContainText("23h ago");
  await expect(page.locator("#backup-age-display")).toContainText("C:\\Fixture\\chosen.zip");
  await expect(page.locator("#backup-auto-age-display")).toContainText("C:\\Fixture\\automatic.zip");
  await expect(page.locator("#backup-age-display")).not.toContainText("today");
  await expect(page.locator("#settings-view-backups .health-view-intro")).toContainText("bookmarks, and notes");
  await expect(page.locator("#btn-export-backup")).toHaveAttribute("title", /bookmarks, and notes/);
});

test("deep check remains cancellable in background and progress preserves focus and dependency results", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    window.__deepJob = null;
    window.__setBridgeHandler("diagnostics_run", () => ({ ok: true, rows: [{ name: "Fixture tool", ok: true }] }));
    window.__setBridgeHandler("integrity_scan_start", () => {
      window.__deepJob = { ok: true, job_id: "fixture-check", running: true,
        phase: "Checking stored search tokens", completed: 6400, unit: "tokens checked", elapsed_seconds: 123 };
      return { ok: true, job_id: "fixture-check", started: true };
    });
    window.__setBridgeHandler("integrity_scan_state", () => window.__deepJob || { ok: true, running: false });
    window.__setBridgeHandler("integrity_scan_cancel", (id) => {
      if (id !== "fixture-check") throw new Error("Wrong scan");
      window.__deepJob = { ...window.__deepJob, running: false,
        result: { ok: false, cancelled: true, preview_only: true,
          error: "Deep archive check cancelled. No files were changed; results are incomplete." } };
      return { ok: true, cancel_requested: true };
    });
  });
  await page.locator('.tab[data-tab="settings"]').click();
  await page.locator("#settings-about-troubleshooting > summary").click();
  await page.locator("#btn-diagnostics").click();
  await expect(page.locator("#diag-summary")).toContainText("dependency checks passed");
  await page.locator("#diag-integrity").click();
  await page.getByRole("dialog", { name: "Deep archive check", exact: true }).getByRole("button", { name: "Start deep check" }).click();
  await expect(page.locator("#diag-integrity-results")).toContainText("6,400 tokens checked · 2m 03s elapsed");
  const cancel = page.locator("#diag-integrity-cancel");
  await cancel.focus();
  await expect.poll(() => page.evaluate(() => window.__bridgeCallsFor("integrity_scan_state").length)).toBeGreaterThan(2);
  await expect(cancel).toBeFocused();
  await page.locator("#diag-close").click();
  await expect(page.locator(".integrity-background-status")).toBeVisible();
  expect(await page.evaluate(() => window.__bridgeCallsFor("integrity_scan_cancel").length)).toBe(0);
  await page.locator(".integrity-background-status").click();
  await cancel.click();
  await expect(page.locator("#diag-integrity-results")).toContainText("results are incomplete");
  await expect(page.locator("#diag-summary")).toContainText("dependency checks passed");
  await expect(page.locator("#diag-integrity-results")).not.toContainText("No integrity problems found");
});

test("channel refresh labels identify local work and YouTube requests", async ({ page }) => {
  await loadApp(page);
  await health(page, "library");
  await expect(page.locator("#btn-md-refresh-all-views")).toContainText("from YouTube");
  await expect(page.locator("#btn-md-recheck-thumbs")).toContainText("Recount local files and coverage");
  await expect(page.locator("#btn-md-reload")).toHaveText("Reload saved status");
  await expect(page.locator("#btn-md-reload")).toHaveAttribute("title", /Does not contact YouTube/);
});
