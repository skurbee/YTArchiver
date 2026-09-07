const path = require("node:path");
const { pathToFileURL } = require("node:url");
const { test: base, expect } = require("@playwright/test");

const REPO_ROOT = path.resolve(__dirname, "..", "..", "..");
const APP_URL = pathToFileURL(path.join(REPO_ROOT, "web", "index.html")).href;

// Runs inside the browser before any YTArchiver script. It mirrors the one
// property the real pywebview host injects (window.pywebview.api), records
// every call, and returns deliberately small fixture-safe result shapes.
// Tests can replace one endpoint at a time through __setBridgeHandler.
function installBridgeStub(options = {}) {
  const calls = [];
  const handlers = Object.create(null);
  const unexpected = JSON.parse(sessionStorage.getItem("yt_fixture_unexpected") || "[]");
  const persisted = JSON.parse(sessionStorage.getItem("yt_fixture_settings") || "{}");
  const settings = {
    output_dir: "C:\\FixtureArchive", video_out_dir: "C:\\FixtureArchive",
    default_resolution: "1080", legacy_subs_tab: false,
    auto_backup_interval: "weekly", tp_archive_roots: [],
    last_auto_backup_ts: Math.floor(Date.now() / 1000) - (2 * 86400),
    ...persisted, ...options.settings,
  };

  const defaultResult = (name) => {
    switch (name) {
      case "get_runtime_info":
        return {
          log_mode: "Simple",
          show_avg_size: true,
          onboarded: true,
          output_dir: "C:\\FixtureArchive",
          has_config_file: true,
        };
      case "get_activity_log_history":
      case "url_history":
      case "browse_list_channels":
      case "get_channel_metadata_status":
      case "browse_list_videos":
        return [];
      case "bookmark_list":
        return { ok: true, rows: [] };
      case "list_manual_videos":
      case "list_all_videos":
      case "browse_list_videos_page":
        return { rows: [], total: 0, has_more: false };
      case "manual_backfill_review_list":
        return { ok: true, items: [] };
      case "browse_get_transcript":
        return { ok: true, segments: [] };
      case "browse_search_context":
        return { ok: true, segments: [], matches: [] };
      case "get_subs_channels":
        return [[], "No channels"];
      case "get_index_summary":
        return { cards: { channels: 0, videos: 0, transcripts: 0, segments: 0,
          physical_copies: 0, size_label: "0 B", scan_complete: true,
          scanned_channels: 0, total_channels: 0 } };
      case "index_summary":
        return { videos: 0, channels: 0, segments: 0, bookmarks: 0 };
      case "get_index_db_stats":
        return { ok: true, total_videos: 0, transcribed_videos: 0,
          segments: 0, hours: 0, index_db_size_label: "0 B" };
      case "get_queues":
        return {
          sync: [],
          gpu: [],
          sync_count: 0,
          gpu_count: 0,
          identity_ids_durable: true,
        };
      case "settings_load":
        return { ...settings };
      case "startup_ready":
      case "window_state_save":
      case "app_window_focus_changed":
      case "queue_auto_set":
      case "app_session_errors_changed":
        return { ok: true };
      case "metadata_fill_missing_channel":
      case "manual_refresh_metadata":
      case "metadata_refresh_views_channel":
      case "metadata_refresh_comments_channel":
      case "refetch_thumbnails":
      case "browse_refresh_video_metadata":
        return { ok: true, queued: true };
      case "queue_video_thumbnails":
        return { ok: true, queued: 0 };
      case "browse_repair_video_thumbnail":
        return { ok: true, queued: false, reason: "No fixture thumbnail source" };
      case "queues_sync_remove":
        return { ok: true };
      case "about_info":
        return { app_name: "YTArchiver", app_version: "fixture", channels: 0,
          config_path: "C:\\FixtureProfile\\config.json", output_dir: settings.output_dir,
          ytdlp_version: "fixture", python_version: "fixture" };
      case "onboarding_state":
        return { onboarded: false, output_dir: "", version: "fixture", deps: {},
          youtube_traffic: { mode: "conservative" }, installing: { running: false } };
      case "channel_transcription_stats":
        return { ok: true, total: 0, transcribed: 0, pending: 0, failed: 0 };
      case "chan_redownload_progress_peek":
        return { ok: true, pending: false };
      case "subs_check_duplicate":
        return { dup_url: null, dup_folder: null };
      case "subs_get_defaults":
        return { resolution: "720", min_duration: 3, max_duration: 0,
          auto_metadata: true, auto_transcribe: true, compress_enabled: false,
          mode: "new", folder_org: "years" };
      case "get_header_version":
        return { version: "fixture", date: "Fixture build" };
      case "video_lengths_backfill_running":
        return { ok: true, running: false };
      case "archive_single_status":
        return { ok: true, tasks: [] };
      case "queue_auto_get":
        return { sync: false, gpu: false };
      case "livestreams_drawer_state":
        return { ok: true, visible: false, snooze_until_ts: 0, now_ts: 0 };
      case "browse_week_summary":
        return { ok: true, new_videos: 0, new_channels: 0, total_channels: 0, channel_list: [] };
      case "index_unindexed_count":
        return { ok: true, count: 0 };
      case "index_rebuild_fts_state":
        return { running: false, started_at: null, completed_at: null,
          ok: null, rows_indexed: null, error: null };
      case "window_state_load":
        return {};
      case "get_last_sync_label":
        return { label: "Last Full Sync: Not yet synced" };
      case "autorun_state":
        return { mins: 0, label: "Off", mode: "timer", seconds_remaining: null,
          overdue_seconds: 0, waiting_for_sync: false, scheduled_sync_running: false,
          next_fire_ts: null, busy_reason: "", budget_mode: false,
          budget_impossible: false, budget_message: "", startup_waiting: false,
          startup_grace_active: false, clock_time_available: false, clock_anchor_minutes: 0 };
      case "launch_at_boot_get":
        return { enabled: false, minimized: false };
      case "ytdlp_version":
        return { ok: true, version: "fixture", auto_updatable: true };
      case "youtube_traffic_status":
        return { mode: "conservative", paused: false, recent_requests: [] };
      case "archive_rescan_state":
        return { running: false };
      case "thumbnail_status_bulk":
        return { channels: [] };
      case "check_channel_folders":
        return { ok: true, missing: [] };
      case "browse_get_video_metadata":
        return { ok: false, error: "No fixture metadata" };
      case "browse_video_url":
        return { ok: false, error: "No fixture media" };
      case "trash_summary":
        return {
          ok: true,
          item_count: 0,
          file_count: 0,
          expired_count: 0,
          oldest_trashed_at: null,
        };
      case "trash_list":
        return {
          ok: true,
          entries: [],
          item_count: 0,
          file_count: 0,
          untracked_count: 0,
          retention_days: 30,
        };
      case "single_video_archived":
        return { ok: true, archived: false };
      case "archive_single_video":
        return { ok: true, queued: 1 };
      default:
        unexpected.push(name);
        sessionStorage.setItem("yt_fixture_unexpected", JSON.stringify(unexpected));
        throw new Error(`No browser fixture registered for ${name}`);
    }
  };

  const api = new Proxy({}, {
    get(_target, property) {
      if (typeof property === "symbol") return undefined;
      if (property === "then") return undefined;
      return (...args) => {
        const name = String(property);
        calls.push({ name, args });
        if (typeof handlers[name] === "function") {
          return handlers[name](...args);
        }
        if (Object.hasOwn(options.responses || {}, name)) return options.responses[name];
        if (name === "settings_save") {
          Object.assign(settings, args[0]);
          sessionStorage.setItem("yt_fixture_settings", JSON.stringify(settings));
          return Promise.resolve({ ok: true });
        }
        if (name === "subs_add_channel") {
          return Promise.resolve({ ok: true, channel: { ...args[0],
            name: args[0]?.folder || "Fixture channel" } });
        }
        return Promise.resolve().then(() => defaultResult(name));
      };
    },
  });

  window.__bridgeCalls = calls;
  window.__unexpectedBridgeCalls = unexpected;
  window.__fixtureDefaultResult = defaultResult;
  window.__setBridgeHandler = (name, handler) => {
    handlers[String(name)] = handler;
  };
  window.__bridgeCallsFor = (name) =>
    calls.filter((entry) => entry.name === String(name));
  window.pywebview = { api };
}

async function loadApp(page, options = {}) {
  if (!options.bridgeDelayed) {
    // One init script gives fixture installation and caller setup a fixed order.
    const configure = options.configure
      ? `;(${options.configure.toString()})(${JSON.stringify(options.args || null)})` : "";
    await page.addInitScript({ content:
      `(${installBridgeStub.toString()})(${JSON.stringify(options.bridge || {})})${configure}` });
  }
  await page.goto(APP_URL, { waitUntil: "load" });
  await page.waitForFunction(() =>
    typeof window.askDanger === "function"
      && typeof window.renderQueues === "function"
      && typeof window._openVideoInWatch === "function"
      && window._watchActionsInited === true);
  if (!options.bridgeDelayed && options.waitFor !== "handlers") {
    await page.evaluate(() => window.YT.settingsReady);
  }
}

async function installDelayedBridge(page) {
  await page.evaluate(installBridgeStub);
  await page.evaluate(() => {
    window.dispatchEvent(new Event("pywebviewready"));
  });
}

module.exports = {
  test: base.extend({
    bridgeContract: [async ({ page }, use) => {
      await use();
      if (!page.isClosed()) {
        const unexpected = await page.evaluate(() => window.__unexpectedBridgeCalls || []);
        expect(unexpected, "Every native call needs an explicit browser fixture").toEqual([]);
      }
    }, { auto: true }],
  }),
  expect,
  APP_URL,
  installBridgeStub,
  installDelayedBridge,
  loadApp,
};
