const fs = require("node:fs");
const path = require("node:path");
const { test, expect } = require("@playwright/test");
const { installBridgeStub, loadApp } = require("./fixtures");

const REPO_ROOT = path.resolve(__dirname, "..", "..", "..");
async function loadTrashFixture(page, configureBridge) {
  await page.addInitScript(installBridgeStub);
  if (configureBridge) await page.addInitScript(configureBridge);
  await loadApp(page, { bridgeDelayed: true });
}

async function openTrash(page) {
  await page.locator('.tab[data-tab="browse"]').click();
  await page.locator('.submode-btn[data-submode="trash"]').click();
  await expect(page.locator("#view-trash")).toBeVisible();
  await expect(page.locator("#trash-list")).toHaveAttribute("aria-busy", "false");
}

test("source templates wire Trash, retention choices, and truthful backup copy", async () => {
  const browse = fs.readFileSync(
    path.join(REPO_ROOT, "web", "partials", "tab-browse.html"), "utf8");
  const settings = fs.readFileSync(
    path.join(REPO_ROOT, "web", "partials", "tab-settings.html"), "utf8");
  const health = fs.readFileSync(
    path.join(REPO_ROOT, "web", "partials", "tab-health.html"), "utf8");
  const template = fs.readFileSync(
    path.join(REPO_ROOT, "web", "index.template.html"), "utf8");
  const browseStyles = fs.readFileSync(
    path.join(REPO_ROOT, "web", "styles-browse.css"), "utf8");
  const assembled = fs.readFileSync(
    path.join(REPO_ROOT, "web", "index.html"), "utf8");

  expect(browse.indexOf('data-submode="trash"')).toBeGreaterThan(
    browse.indexOf('data-submode="manual"'));
  expect(browse).toContain(
    '<button class="submode-btn submode-btn-with-count trash-submode-btn"');
  expect(browse).toContain('id="view-trash"');
  expect(browse).toContain('aria-label="About Trash"');
  expect(browse).not.toContain('class="trash-explainer"');
  expect(browse).toContain("Removed files can be restored from here.");
  expect(browse).not.toContain("stay on the same archive drive");
  expect(browse).not.toContain("Space is freed only when");
  const countStyles = browseStyles.match(/\.trash-nav-count\s*\{[^}]+\}/s)?.[0] || "";
  expect(countStyles).toContain("var(--c-dim)");
  expect(countStyles).not.toContain("danger");
  const buttonStyles = browseStyles.match(/\.trash-submode-btn\s*\{[^}]+\}/s)?.[0] || "";
  expect(buttonStyles).toContain("border: 1px solid var(--c-border)");
  expect(buttonStyles).toContain("border-radius: 4px");
  expect(template).toContain('<script src="trashView.js?v=1"></script>');
  expect(settings).toContain('id="settings-trash-retention-days"');
  expect(settings).toContain("Off — keep until I delete them");
  for (const value of [0, 7, 14, 30, 60, 90, 180, 365]) {
    expect(settings).toContain(`value="${value}"`);
  }
  expect(settings).toContain("shortening the wait takes effect after 24 hours");
  expect(settings).not.toContain("does not free space until");
  expect(settings).not.toContain('id="settings-auto-backup"');
  expect(health).toContain('id="settings-auto-backup"');
  expect(health).toContain("Keeps the newest four.");
  expect(health).not.toContain("older ones move to the archive trash");
  expect(assembled).toContain('id="view-trash"');
  expect(assembled).toContain('id="settings-trash-retention-days"');
  expect(assembled).toContain('<script src="trashView.js?v=1"></script>');
});

test("Trash navigation renders authoritative details, policy, pending and untracked states", async ({ page }) => {
  await loadTrashFixture(page, () => {
    const entries = [{
      entry_id: "channel-ready",
      display_name: "Ready Channel",
      entry_type: "channel_folder",
      state: "complete",
      trashed_at: "2026-08-30T14:00:00Z",
      original_path: "Z:\\YouTube\\Ready Channel",
      size_bytes: 1610612736,
      file_count: 6,
      restore_scope: "full",
      can_restore: true,
      can_purge: true,
    }, {
      entry_id: "video-pending",
      display_name: "Still moving.mp4",
      entry_type: "video",
      state: "pending",
      trashed_at: "2026-08-31T14:00:00Z",
      original_path: "Z:\\YouTube\\Still moving.mp4",
      size_bytes: 1024,
      file_count: 1,
      can_restore: false,
      can_purge: false,
    }, {
      entry_id: "",
      display_name: "Unknown folder",
      entry_type: "files",
      state: "untracked",
      trashed_at: "",
      original_path: "Z:\\YouTube\\.YTArchiver Trash\\Unknown folder",
      size_bytes: 0,
      file_count: 1,
      can_restore: false,
      can_purge: false,
      warnings: ["Manifest could not be read"],
    }];
    window.__setBridgeHandler("trash_summary", () => ({
      ok: true, item_count: 3, file_count: 8,
      untracked_count: 1, retention_days: 30,
    }));
    window.__setBridgeHandler("trash_list", () => ({
      ok: true, entries, item_count: 3, file_count: 8,
      untracked_count: 1, retention_days: 30,
    }));
    window.__setBridgeHandler("trash_open_folder", () => new Promise((resolve) => {
      window.__resolveTrashOpen = resolve;
    }));
  });

  await expect(page.locator("#trash-nav-count")).toHaveText("3");
  await expect(page.locator("#trash-nav-count")).not.toHaveAttribute("hidden", "");
  await page.locator('.tab[data-tab="browse"]').click();
  const sidebarLayout = await page.evaluate(() => {
    const manual = document.querySelector('.submode-btn[data-submode="manual"]')
      ?.getBoundingClientRect();
    const trash = document.querySelector('.submode-btn[data-submode="trash"]')
      ?.getBoundingClientRect();
    const stats = document.querySelector(".library-stats")?.getBoundingClientRect();
    return {
      gapBelowManual: trash && manual ? trash.top - manual.bottom : 0,
      gapAboveStats: stats && trash ? stats.top - trash.bottom : 999,
    };
  });
  expect(sidebarLayout.gapBelowManual).toBeGreaterThan(20);
  expect(sidebarLayout.gapAboveStats).toBeGreaterThanOrEqual(4);

  const trashButtonStyle = await page.locator("button.trash-submode-btn")
    .evaluate((button) => {
      const style = getComputedStyle(button);
      return {
        borderStyle: style.borderStyle,
        borderWidth: style.borderWidth,
        borderColor: style.borderColor,
        borderRadius: style.borderRadius,
        cursor: style.cursor,
      };
    });
  expect(trashButtonStyle).toMatchObject({
    borderStyle: "solid",
    borderWidth: "1px",
    borderRadius: "4px",
    cursor: "pointer",
  });
  expect(trashButtonStyle.borderColor).not.toBe("rgba(0, 0, 0, 0)");

  await page.evaluate(() => {
    document.getElementById("stat-segments").textContent = "110,530";
  });
  const segmentStatFits = await page.locator(".library-stats div").first()
    .evaluate((row) => ({
      rowFits: row.scrollWidth <= row.clientWidth,
      labelFits: row.querySelector(".library-stat-label").scrollWidth
        <= row.querySelector(".library-stat-label").clientWidth,
    }));
  expect(segmentStatFits).toEqual({ rowFits: true, labelFits: true });

  await openTrash(page);
  await expect(page.locator("#trash-summary-text")).toHaveText("3 items · 8 files");
  await expect(page.locator("#trash-retention-note"))
    .toHaveText("Items are automatically deleted after 30 days.");
  const trashHelp = page.locator("#trash-help-popover");
  await expect(trashHelp).toBeHidden();
  await page.getByRole("button", { name: "About Trash" }).hover();
  await expect(trashHelp).toBeVisible();
  await expect(trashHelp).toContainText("Removed files can be restored");
  await expect(trashHelp).toContainText(
    "Items are automatically deleted after 30 days");

  const ready = page.locator('.trash-item[data-trash-id="channel-ready"]');
  await expect(ready).toContainText("Ready Channel");
  await expect(ready).toContainText("Channel folder");
  await expect(ready).toContainText("1.5 GB");
  await expect(ready).toContainText("Files: 6");
  await expect(ready).toContainText("Z:\\YouTube\\Ready Channel");
  await expect(ready).toContainText(
    "Restore includes files, subscription settings, and Browse catalog data.");
  await expect(ready.getByRole("button", { name: "Restore files" })).toBeEnabled();

  const pending = page.locator('.trash-item[data-trash-id="video-pending"]');
  await expect(pending).toContainText("Still moving files");
  await expect(pending.getByRole("button", { name: "Restore files" })).toBeDisabled();
  await expect(pending.getByRole("button", { name: "Delete forever…" })).toBeDisabled();
  await expect(page.locator("#trash-attention-banner"))
    .toHaveText("1 untracked item needs attention. YTArchiver will leave them alone.");

  await page.evaluate(() => {
    const button = document.getElementById("trash-open-folder");
    button.click();
    button.click();
  });
  await expect.poll(() => page.evaluate(() =>
    window.__bridgeCallsFor("trash_open_folder").length)).toBe(1);
  await page.evaluate(() => window.__resolveTrashOpen({ ok: true }));
});

test("permanent delete confirms once, keeps the card until success, then reloads", async ({ page }) => {
  await loadTrashFixture(page, () => {
    const entry = {
      entry_id: "delete-me", display_name: "Delete me.mp4",
      entry_type: "video", state: "complete",
      trashed_at: "2026-08-31T14:00:00Z",
      original_path: "Z:\\YouTube\\Delete me.mp4",
      size_bytes: 4096, file_count: 1,
      can_restore: true, can_purge: true,
    };
    window.__trashDeleted = false;
    window.__setBridgeHandler("trash_summary", () => ({
      ok: true, item_count: window.__trashDeleted ? 0 : 1,
      file_count: window.__trashDeleted ? 0 : 1,
    }));
    window.__setBridgeHandler("trash_list", () => ({
      ok: true,
      entries: window.__trashDeleted ? [] : [entry],
      item_count: window.__trashDeleted ? 0 : 1,
      file_count: window.__trashDeleted ? 0 : 1,
      untracked_count: 0, retention_days: 0,
    }));
    window.__setBridgeHandler("trash_purge", () => new Promise((resolve) => {
      window.__resolveTrashPurge = () => {
        window.__trashDeleted = true;
        resolve({ ok: true, purged: 1, freed_bytes: 4096 });
      };
    }));
  });
  await openTrash(page);

  const card = page.locator('.trash-item[data-trash-id="delete-me"]');
  await card.getByRole("button", { name: "Delete forever…" }).click();
  const firstDialog = page.getByRole("dialog", {
    name: 'Permanently delete "Delete me.mp4"?',
  });
  await expect(firstDialog).toContainText("cannot be undone");
  await expect(firstDialog).not.toContainText("frees space");
  await firstDialog.getByRole("button", { name: "Cancel" }).click();
  await expect(firstDialog).toHaveCount(0);
  await expect.poll(() => page.evaluate(() =>
    window.__bridgeCallsFor("trash_purge").length)).toBe(0);
  await expect(card).toBeVisible();

  await page.evaluate(() => {
    const button = document.querySelector(
      '.trash-item[data-trash-id="delete-me"] [data-trash-action="purge"]');
    button.click();
    button.click();
  });
  const dialog = page.getByRole("dialog", {
    name: 'Permanently delete "Delete me.mp4"?',
  });
  await dialog.getByRole("button", { name: "Delete forever" }).click();
  await expect.poll(() => page.evaluate(() =>
    window.__bridgeCallsFor("trash_purge").length)).toBe(1);
  expect(await page.evaluate(() =>
    window.__bridgeCallsFor("trash_purge")[0].args)).toEqual([{ id: "delete-me" }]);
  await expect(card).toBeVisible();

  await page.evaluate(() => window.__resolveTrashPurge());
  await expect(card).toHaveCount(0);
  await expect(page.locator("#trash-list")).toContainText("Trash is empty");
  await expect(page.locator("#trash-nav-count")).toBeHidden();
});

test("restore uses an opaque id, explains subscription status, and retention saves as a number", async ({ page }) => {
  await loadTrashFixture(page, () => {
    const entry = {
      entry_id: "restore-channel", display_name: "Restore Channel",
      entry_type: "channel_folder", state: "complete",
      trashed_at: "2026-08-31T14:00:00Z",
      original_path: "Z:\\YouTube\\Restore Channel",
      size_bytes: 2048, file_count: 2,
      can_restore: true, can_purge: true,
    };
    window.__trashRestored = false;
    window.__setBridgeHandler("trash_summary", () => ({
      ok: true, item_count: window.__trashRestored ? 0 : 1,
      file_count: window.__trashRestored ? 0 : 2,
    }));
    window.__setBridgeHandler("trash_list", () => ({
      ok: true, entries: window.__trashRestored ? [] : [entry],
      item_count: window.__trashRestored ? 0 : 1,
      file_count: window.__trashRestored ? 0 : 2,
      untracked_count: 0, retention_days: 90,
    }));
    window.__setBridgeHandler("trash_restore", () => {
      window.__trashRestored = true;
      return {
        ok: true, entry_type: "channel_folder",
        subscription_present: false, catalog_restored: true,
      };
    });
    window.__setBridgeHandler("settings_load", () => ({
      output_dir: "C:\\FixtureArchive",
      video_out_dir: "C:\\FixtureArchive",
      default_resolution: "1080",
      trash_retention_days: 90,
    }));
    window.__setBridgeHandler("settings_save", () => ({ ok: true }));
  });

  await openTrash(page);
  await page.getByRole("button", { name: "Restore files" }).click();
  await expect.poll(() => page.evaluate(() =>
    window.__bridgeCallsFor("trash_restore").length)).toBe(1);
  expect(await page.evaluate(() =>
    window.__bridgeCallsFor("trash_restore")[0].args))
    .toEqual([{ id: "restore-channel" }]);
  await expect(page.locator("#toast-root"))
    .toContainText("The channel was not re-added to subscriptions.");

  await page.locator('.tab[data-tab="settings"]').click();
  const retention = page.locator("#settings-trash-retention-days");
  await expect(retention).toHaveValue("90");
  await page.evaluate(() => {
    const select = document.getElementById("settings-trash-retention-days");
    select.value = "14";
    select.dispatchEvent(new Event("change", { bubbles: true }));
  });
  await expect.poll(() => page.evaluate(() => {
    const calls = window.__bridgeCallsFor("settings_save");
    return calls.some((call) => call.args[0]?.trash_retention_days === 14);
  })).toBe(true);
  const payload = await page.evaluate(() => window.__bridgeCallsFor("settings_save")
    .find((call) => call.args[0]?.trash_retention_days === 14)?.args[0]);
  expect(payload).toEqual({ trash_retention_days: 14 });
});

test("Settings preserves an existing custom retention period", async ({ page }) => {
  await loadTrashFixture(page, () => {
    window.__setBridgeHandler("trash_summary", () => ({
      ok: true, item_count: 0, file_count: 0, untracked_count: 0,
      retention_days: 45,
    }));
    window.__setBridgeHandler("trash_list", () => ({
      ok: true, entries: [], item_count: 0, file_count: 0,
      untracked_count: 0, retention_days: 45,
    }));
    window.__setBridgeHandler("settings_load", () => ({
      output_dir: "C:\\FixtureArchive",
      video_out_dir: "C:\\FixtureArchive",
      default_resolution: "1080",
      trash_retention_days: 45,
    }));
    window.__setBridgeHandler("settings_save", () => ({ ok: true }));
  });

  await page.locator('.tab[data-tab="settings"]').click();
  const retention = page.locator("#settings-trash-retention-days");
  await expect(retention).toHaveValue("45");
  await expect(retention.locator('option[value="45"]'))
    .toHaveText("45 days (custom)");
});

test("an interrupted video restore is clearly resumable", async ({ page }) => {
  await loadTrashFixture(page, () => {
    const entry = {
      entry_id: "resume-video", display_name: "Resume me.mp4",
      entry_type: "video", state: "restoring",
      trashed_at: "2026-08-31T14:00:00Z",
      original_path: "Z:\\YouTube\\Resume me.mp4",
      size_bytes: 4096, file_count: 1,
      can_restore: true, can_purge: false,
    };
    window.__setBridgeHandler("trash_summary", () => ({
      ok: true, item_count: 1, file_count: 1, untracked_count: 0,
      retention_days: 30,
    }));
    window.__setBridgeHandler("trash_list", () => ({
      ok: true, entries: [entry], item_count: 1, file_count: 1,
      untracked_count: 0, retention_days: 30,
    }));
  });

  await openTrash(page);
  const resume = page.getByRole("button", { name: "Resume restore" });
  await expect(resume).toBeEnabled();
  await expect(resume).not.toHaveAttribute("title");
});

test("older channel entries explain files-only restore before the click", async ({ page }) => {
  await loadTrashFixture(page, () => {
    const entry = {
      entry_id: "legacy-channel", display_name: "Older Channel",
      entry_type: "channel_folder", state: "complete",
      restore_scope: "files_only",
      trashed_at: "2026-08-31T14:00:00Z",
      original_path: "Z:\\YouTube\\Older Channel",
      size_bytes: 4096, file_count: 2,
      can_restore: true, can_purge: true,
    };
    window.__restoredLegacy = false;
    window.__setBridgeHandler("trash_summary", () => ({
      ok: true, item_count: window.__restoredLegacy ? 0 : 1,
      file_count: window.__restoredLegacy ? 0 : 2,
      retention_days: 30,
    }));
    window.__setBridgeHandler("trash_list", () => ({
      ok: true, entries: window.__restoredLegacy ? [] : [entry],
      item_count: window.__restoredLegacy ? 0 : 1,
      file_count: window.__restoredLegacy ? 0 : 2,
      untracked_count: 0, retention_days: 30,
    }));
    window.__setBridgeHandler("trash_restore", () => {
      window.__restoredLegacy = true;
      return {
        ok: true, entry_type: "channel_folder",
        subscription_present: false,
        warnings: [
          "Files were restored, but this older Trash item cannot re-add the subscription automatically.",
        ],
      };
    });
  });

  await openTrash(page);
  const card = page.locator('.trash-item[data-trash-id="legacy-channel"]');
  await expect(card).toContainText(
    "Files only — this older item cannot re-add the subscription automatically.");
  await card.getByRole("button", { name: "Restore files" }).click();
  await expect(page.locator("#toast-root")).toContainText(
    "this older Trash item cannot re-add the subscription automatically");
  await expect(page.locator("#toast-root")).not.toContainText(
    "cleanup needs attention");
});

test("active cleanup grace is visible in Browse", async ({ page }) => {
  await loadTrashFixture(page, () => {
    const grace = Math.floor(Date.now() / 1000) + 3600;
    window.__setBridgeHandler("trash_summary", () => ({
      ok: true, item_count: 0, file_count: 0,
      retention_days: 30, retention_grace_until_ts: grace,
    }));
    window.__setBridgeHandler("trash_list", () => ({
      ok: true, entries: [], item_count: 0, file_count: 0,
      untracked_count: 0, retention_days: 30,
      retention_grace_until_ts: grace,
    }));
  });

  await openTrash(page);
  await expect(page.locator("#trash-retention-note"))
    .toContainText("Automatic cleanup is paused until");
});

test("Empty Trash confirms and keeps cards until the backend succeeds", async ({ page }) => {
  await loadTrashFixture(page, () => {
    const entries = ["One", "Two"].map((name, index) => ({
      entry_id: `empty-${index}`, display_name: `${name}.mp4`,
      entry_type: "video", state: "complete",
      trashed_at: "2026-08-31T14:00:00Z",
      original_path: `Z:\\YouTube\\${name}.mp4`,
      size_bytes: 1024, file_count: 1,
      can_restore: true, can_purge: true,
    }));
    window.__trashEmptied = false;
    window.__setBridgeHandler("trash_summary", () => ({
      ok: true, item_count: window.__trashEmptied ? 0 : 2,
      file_count: window.__trashEmptied ? 0 : 2,
    }));
    window.__setBridgeHandler("trash_list", () => ({
      ok: true, entries: window.__trashEmptied ? [] : entries,
      item_count: window.__trashEmptied ? 0 : 2,
      file_count: window.__trashEmptied ? 0 : 2,
      untracked_count: 0, retention_days: 30,
    }));
    window.__setBridgeHandler("trash_empty", () => new Promise((resolve) => {
      window.__resolveTrashEmpty = () => {
        window.__trashEmptied = true;
        resolve({ ok: true, purged: 2, failed: 0, freed_bytes: 2048 });
      };
    }));
  });

  await openTrash(page);
  await page.getByRole("button", { name: "Empty Trash…" }).click();
  const dialog = page.getByRole("dialog", { name: "Empty Trash?" });
  await expect(dialog).toContainText("2 ready items");
  await expect(dialog).toContainText("This cannot be undone.");
  await expect(dialog).not.toContainText("frees space");
  await dialog.getByRole("button", { name: "Empty Trash" }).click();
  await expect.poll(() => page.evaluate(() =>
    window.__bridgeCallsFor("trash_empty").length)).toBe(1);
  await expect(page.locator(".trash-item")).toHaveCount(2);
  await page.evaluate(() => window.__resolveTrashEmpty());
  await expect(page.locator(".trash-item")).toHaveCount(0);
  await expect(page.locator("#toast-root")).toContainText("2 items permanently deleted");
});

test("the visible retention dropdown locks while a save is pending", async ({ page }) => {
  await loadTrashFixture(page, () => {
    window.__setBridgeHandler("settings_load", () => ({
      output_dir: "C:\\FixtureArchive",
      video_out_dir: "C:\\FixtureArchive",
      default_resolution: "1080",
      trash_retention_days: 30,
    }));
    window.__setBridgeHandler("settings_save", () => new Promise((resolve) => {
      window.__resolveRetentionSave = resolve;
    }));
  });

  await page.locator('.tab[data-tab="settings"]').click();
  await page.evaluate(() => {
    const select = document.getElementById("settings-trash-retention-days");
    select.value = "14";
    select.dispatchEvent(new Event("change", { bubbles: true }));
  });
  const select = page.locator("#settings-trash-retention-days");
  await expect(select).toBeDisabled();
  const visibleDropdown = select.locator("xpath=preceding-sibling::*[1]");
  await visibleDropdown.locator(".yt-dd-trigger").click({ force: true });
  await expect(visibleDropdown).not.toHaveClass(/open/);
  await page.evaluate(() => window.__resolveRetentionSave({ ok: true }));
  await expect(select).toBeEnabled();
});
