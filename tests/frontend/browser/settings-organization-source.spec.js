const fs = require("node:fs");
const path = require("node:path");
const { test, expect } = require("@playwright/test");

const REPO_ROOT = path.resolve(__dirname, "..", "..", "..");
const readWeb = (name) => fs.readFileSync(path.join(REPO_ROOT, "web", name), "utf8");

function markupBetween(source, startId, endId) {
  const start = source.indexOf(`id="${startId}"`);
  expect(start).toBeGreaterThan(-1);
  const end = endId ? source.indexOf(`id="${endId}"`, start + 1) : source.length;
  if (endId) expect(end).toBeGreaterThan(start);
  return source.slice(start, end);
}

function expectIdOnce(source, id) {
  expect((source.match(new RegExp(`id="${id}"`, "g")) || [])).toHaveLength(1);
}

test("Settings uses one page with three task-based sections and unique controls", async () => {
  const settings = readWeb(path.join("partials", "tab-settings.html"));
  expect(settings).not.toContain('class="settings-sidebar"');
  expect(settings).not.toContain("data-settings-view=");
  expectIdOnce(settings, "settings-view-preferences");

  const storage = markupBetween(
    settings, "settings-section-storage", "settings-section-downloads");
  for (const id of [
    "settings-output-dir", "settings-video-dir",
    "settings-archive-capacity-mode", "settings-archive-capacity-threshold",
    "settings-trash-retention-days", "settings-roots-list",
    "btn-settings-add-root", "btn-settings-remove-root",
    "settings-background-checks", "settings-auto-index-enabled",
    "settings-auto-index-threshold", "settings-disk-staleness",
  ]) expect(storage).toContain(`id="${id}"`);
  expect(storage).toContain("Additional archive folders");
  expect(storage).toContain(
    "Your main archive folder is always included. Add other folders here if Search should include them too.");

  const downloads = markupBetween(
    settings, "settings-section-downloads", "settings-section-app");
  for (const id of [
    "settings-default-res", "settings-whisper-model",
    "settings-autorun-mode",
    "settings-youtube-traffic-mode", "settings-traffic-meter",
    "settings-downloader-updates", "settings-ytdlp-version",
    "btn-ytdlp-update", "settings-ytdlp-channel",
    "settings-ytdlp-update-mode", "settings-ytdlp-check-days",
  ]) expect(downloads).toContain(`id="${id}"`);

  const app = markupBetween(settings, "settings-section-app", "settings-actions-footer");
  for (const id of [
    "settings-launch-at-boot", "settings-boot-minimized",
    "settings-close-behavior", "settings-legacy-subs-tab",
    "settings-show-avg-size", "settings-log-mode",
    "settings-about-troubleshooting", "btn-about", "btn-diagnostics",
    "btn-run-setup",
  ]) expect(app).toContain(`id="${id}"`);

  expect(settings).not.toContain('id="settings-auto-backup"');
  expect(settings).not.toContain("does not free space until");
  expect(settings).toContain("shortening the wait takes effect after 24 hours");
  expect((settings.match(/id="settings-mini-log"/g) || [])).toHaveLength(1);

  for (const id of [
    "settings-output-dir", "settings-video-dir", "settings-roots-list",
    "settings-auto-index-enabled", "settings-disk-staleness",
    "settings-default-res", "settings-whisper-model",
    "settings-autorun-mode",
    "settings-youtube-traffic-mode", "settings-ytdlp-version",
    "settings-ytdlp-channel", "settings-launch-at-boot",
    "settings-close-behavior", "settings-legacy-subs-tab",
    "settings-log-mode", "btn-about", "btn-diagnostics", "btn-run-setup",
  ]) expectIdOnce(settings, id);

  const health = readWeb(path.join("partials", "tab-health.html"));
  const healthIndex = markupBetween(
    health, "health-library-index", "health-library-archive");
  for (const movedPreference of [
    "settings-roots-list", "btn-settings-add-root",
    "btn-settings-remove-root", "settings-auto-index-enabled",
    "settings-auto-index-threshold",
  ]) expect(healthIndex).not.toContain(`id="${movedPreference}"`);
  for (const statusOrAction of [
    "index-stats-text", "btn-idx-build", "btn-idx-rebuild", "index-log",
  ]) expect(healthIndex).toContain(`id="${statusOrAction}"`);

  const backups = markupBetween(health, "settings-view-backups", "health-mini-log");
  for (const id of [
    "settings-auto-backup", "backup-auto-age-display",
    "btn-export-backup", "btn-import-backup", "backup-age-display",
    "btn-export-channels", "btn-import-channels",
  ]) {
    expect(backups).toContain(`id="${id}"`);
    expectIdOnce(`${settings}\n${health}`, id);
  }
  expect(backups).toContain("settings, subscriptions, queues, and app history");
  expect(backups).toContain("do not copy downloaded videos or transcripts");
  expect(backups).toContain("While YTArchiver is open");
  expect(backups).toContain("Keeps the newest four");
});

test("Auto-sync timing stays in Settings instead of occupying the Download row", async () => {
  const download = readWeb(path.join("partials", "tab-download.html"));
  const settings = readWeb(path.join("partials", "tab-settings.html"));
  const settingsJs = readWeb("settingsTab.js");
  const interval = download.indexOf('id="auto-sync-select"');
  const clock = download.indexOf('id="auto-sync-clock-time"');

  expect(interval).toBeGreaterThan(-1);
  expect(clock).toBeGreaterThan(interval);
  expect(download).not.toContain('id="settings-autorun-mode"');
  expect(download).not.toContain('id="autorun-timing-mode-wrap"');
  expect(settings).toContain('id="settings-autorun-mode"');
  expect(settings).toContain("Fixed times");
  expect(settings).toContain("Timer since last sync");
  expect(settings).toContain("show a time picker beside Auto-sync on Download");
  expect(settingsJs).toContain('bridgeCall("autorun_set_mode", requested)');
  expect(settingsJs).toContain('if (!result?.ok)');
});
