const path = require("node:path");
const { pathToFileURL } = require("node:url");

const REPO_ROOT = path.resolve(__dirname, "..", "..", "..");
const APP_URL = pathToFileURL(path.join(REPO_ROOT, "web", "index.html")).href;

// Runs inside the browser before any YTArchiver script. It mirrors the one
// property the real pywebview host injects (window.pywebview.api), records
// every call, and returns deliberately small fixture-safe result shapes.
// Tests can replace one endpoint at a time through __setBridgeHandler.
function installBridgeStub() {
  const calls = [];
  const handlers = Object.create(null);

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
      case "list_bookmarks":
      case "list_manual_videos":
      case "browse_list_channels":
        return [];
      case "get_subs_channels":
        return [[], []];
      case "get_index_summary":
        return { channels: 0, videos: 0, transcripts: 0, segments: 0 };
      case "get_queues":
        return {
          sync: [],
          gpu: [],
          sync_count: 0,
          gpu_count: 0,
          identity_ids_durable: true,
        };
      case "settings_load":
        return {
          output_dir: "C:\\FixtureArchive",
          video_out_dir: "C:\\FixtureArchive",
          default_resolution: "1080",
          auto_backup_interval: "weekly",
          last_auto_backup_ts: Math.floor(Date.now() / 1000) - (2 * 86400),
        };
      case "check_channel_folders":
        return { ok: true, missing: [] };
      case "browse_get_video_metadata":
        return { ok: false, error: "No fixture metadata" };
      case "browse_get_video_url":
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
        return { ok: true };
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
        return Promise.resolve(defaultResult(name));
      };
    },
  });

  window.__bridgeCalls = calls;
  window.__setBridgeHandler = (name, handler) => {
    handlers[String(name)] = handler;
  };
  window.__bridgeCallsFor = (name) =>
    calls.filter((entry) => entry.name === String(name));
  window.pywebview = { api };
}

async function loadApp(page, options = {}) {
  if (!options.bridgeDelayed) {
    await page.addInitScript(installBridgeStub);
  }
  await page.goto(APP_URL, { waitUntil: "load" });
  await page.waitForFunction(() =>
    typeof window.askDanger === "function"
      && typeof window.renderQueues === "function"
      && typeof window._openVideoInWatch === "function"
      && window._watchActionsInited === true);
}

async function installDelayedBridge(page) {
  await page.evaluate(installBridgeStub);
  await page.evaluate(() => {
    window.dispatchEvent(new Event("pywebviewready"));
  });
}

module.exports = {
  APP_URL,
  installBridgeStub,
  installDelayedBridge,
  loadApp,
};
