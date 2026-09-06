"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

const WEB = path.join(__dirname, "..", "web");

function runScript(name, window, document) {
  const context = {
    window,
    document,
    console,
    Promise,
    Map,
    Set,
    Node: class Node {},
    MutationObserver: class MutationObserver {
      observe() {}
      disconnect() {}
    },
    setTimeout,
    clearTimeout,
    setInterval,
    clearInterval,
    queueMicrotask,
  };
  vm.runInNewContext(
    fs.readFileSync(path.join(WEB, name), "utf8"),
    context,
    { filename: name },
  );
  return context;
}

function baseDocument() {
  return {
    activeElement: null,
    readyState: "complete",
    addEventListener() {},
    removeEventListener() {},
    getElementById() { return null; },
    querySelector() { return null; },
    querySelectorAll() { return []; },
  };
}

test("danger-dialog Enter follows the focused action and has no unsafe fallback", () => {
  const document = baseDocument();
  const window = {
    YT: {},
    getComputedStyle: () => ({ display: "block", visibility: "visible" }),
  };
  runScript("modals.js", window, document);
  const decide = window.YT.modals._questionEnterDecision;

  assert.equal(decide(true, "cancel"), "cancel");
  assert.equal(decide(true, "confirm"), "confirm");
  assert.equal(decide(true, undefined), "ignore"); // danger + no Cancel/focus
  assert.equal(decide(false, undefined), "confirm");
});

test("hidden modal backdrops do not suppress global shortcuts", () => {
  let keydown = null;
  let syncClicks = 0;
  const modal = { hidden: true, getAttribute: () => null };
  const document = baseDocument();
  document.addEventListener = (kind, fn) => {
    if (kind === "keydown") keydown = fn;
  };
  document.querySelectorAll = (selector) =>
    selector === ".askq-backdrop" ? [modal] : [];
  document.getElementById = (id) => id === "btn-sync-subbed"
    ? { click: () => { syncClicks += 1; } }
    : null;
  const window = {
    YT: {
      bridge: { isUp: () => false },
      modals: { isVisible: (el) => !el.hidden },
    },
  };
  runScript("shortcuts.js", window, document);
  window.initKeyboardShortcuts();

  const event = {
    key: "s",
    ctrlKey: true,
    metaKey: false,
    altKey: false,
    target: { tagName: "DIV", isContentEditable: false },
    preventDefault() {},
  };
  keydown(event);
  assert.equal(syncClicks, 1);

  modal.hidden = false;
  keydown(event);
  assert.equal(syncClicks, 1, "a genuinely visible dialog must still block it");
});

test("single-video queue helper preserves a resolved backend failure", async () => {
  const document = baseDocument();
  const window = {
    YT: {
      bridge: {
        isUp: () => true,
        bridgeCall: async () => ({ ok: false, error: "yt-dlp not found" }),
      },
    },
  };
  runScript("downloadUrl.js", window, document);

  const result = await window._queueSingleVideo("https://youtu.be/example", {});
  assert.equal(result.ok, false);
  assert.equal(result.error, "yt-dlp not found");
});

test("startup hydration waits for the canonical bridge-ready promise", async () => {
  let bridgeUp = false;
  let releaseReady;
  const ready = new Promise((resolve) => { releaseReady = resolve; });
  const calls = [];
  const responses = {
    startup_ready: { ok: true },
    get_runtime_info: {
      onboarded: true,
      output_dir: "archive",
      has_config_file: true,
      log_mode: "Simple",
    },
    get_activity_log_history: [],
    get_subs_channels: [[], []],
    get_index_summary: { cards: {} },
    get_queues: { sync: [], gpu: [] },
  };
  const document = baseDocument();
  document.body = { dataset: {} };
  const window = {
    addEventListener() {},
    removeEventListener() {},
    YT: {
      bridge: {
        isUp: () => bridgeUp,
        ready,
        bridgeCall: async (method) => {
          calls.push(method);
          return responses[method];
        },
      },
    },
    renderActivityLog() {},
    renderSubsTable() {},
    renderQueues() {},
    _primeBrowse() {},
    _populateIndexTable() {},
  };
  runScript("seedLogs.js", window, document);

  const hydration = window.seedLogs();
  await new Promise((resolve) => setTimeout(resolve, 10));
  assert.equal(calls.length, 0, "hydration must not give up or run early");

  bridgeUp = true;
  releaseReady({});
  assert.equal(await hydration, true);
  assert.ok(calls.includes("get_runtime_info"));
  assert.ok(calls.includes("get_queues"));
});

test("window-close prompt does not poison hidden static dialogs", () => {
  let closePromptOpened = 0;
  const hiddenDialog = {
    hidden: true,
    id: "about-backdrop",
    style: {},
    querySelector: () => null,
  };
  const document = baseDocument();
  document.querySelectorAll = (selector) =>
    selector === ".askq-backdrop" ? [hiddenDialog] : [];
  const window = {
    YT: {
      bridge: { isUp: () => false },
      modals: {
        isVisible: (el) => !el.hidden,
        open: () => {
          closePromptOpened += 1;
          return new Promise(() => {});
        },
      },
    },
  };
  runScript("appDialogs.js", window, document);
  window._showCloseDialog();

  assert.equal(closePromptOpened, 1);
  assert.equal(hiddenDialog.style.display, undefined);
  assert.equal(hiddenDialog.hidden, true);
});

test("FTS rebuild waits for and displays the backend's finished outcome", async () => {
  let rebuildClick = null;
  let bridgeUp = false;
  const calls = [];
  const progress = { textContent: "" };
  const log = {
    children: [],
    scrollHeight: 0,
    appendChild(node) { this.children.push(node); },
  };
  const rebuild = {
    addEventListener(kind, fn) {
      if (kind === "click") rebuildClick = fn;
    },
  };
  const document = baseDocument();
  document.createElement = () => ({ className: "", textContent: "" });
  document.getElementById = (id) => ({
    "btn-idx-rebuild": rebuild,
    "idx-progress": progress,
    "index-log": log,
  }[id] || null);
  const window = {
    YT: {
      bridge: {
        isUp: () => bridgeUp,
        bridgeCall: async (method) => {
          calls.push(method);
          if (method === "index_rebuild_fts") {
            return { ok: true, started: true };
          }
          if (method === "index_rebuild_fts_state") {
            return {
              running: false,
              completed_at: 1,
              ok: true,
              rows_indexed: 42,
            };
          }
          return {};
        },
      },
    },
    addEventListener() {},
    askDanger: async () => true,
    _showToast() {},
  };
  runScript("indexControls.js", window, document);
  window.initIndexControls();
  bridgeUp = true;

  await rebuildClick();
  await new Promise((resolve) => setTimeout(resolve, 0));

  assert.ok(calls.includes("index_rebuild_fts_state"));
  assert.equal(progress.textContent, "Search index rebuild complete: 42 entries indexed.");
  assert.ok(log.children.some((node) => /rebuild complete/.test(node.textContent)));
});
