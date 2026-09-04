"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

class FakeClassList {
  add() {}
  remove() {}
  toggle() {}
}

class FakeElement {
  constructor(tag = "div") {
    this.tagName = tag.toUpperCase();
    this.children = [];
    this.dataset = {};
    this.classList = new FakeClassList();
    this.handlers = new Map();
    this.draggable = false;
    this.hidden = false;
    this.textContent = "";
    this._innerHTML = "";
    this._named = new Map();
  }

  set innerHTML(value) {
    this._innerHTML = String(value || "");
    this.children = [];
    this._named = new Map();
    if (this._innerHTML.includes("queue-task-name")) {
      this._named.set(".queue-task-name", new FakeElement("span"));
    }
    if (this._innerHTML.includes("queue-task-close")) {
      this._named.set(".queue-task-close", new FakeElement("button"));
    }
  }

  get innerHTML() { return this._innerHTML; }
  appendChild(child) { this.children.push(child); return child; }
  querySelector(selector) { return this._named.get(selector) || null; }
  addEventListener(kind, fn) { this.handlers.set(kind, fn); }
  trigger(kind, event = {}) { return this.handlers.get(kind)?.(event); }
  getBoundingClientRect() { return { top: 0, height: 20 }; }
}

function loadRenderer(api, windowOverrides = {}) {
  const elements = {
    "sync-tasks-body": new FakeElement(),
    "gpu-tasks-body": new FakeElement(),
    "badge-sync": new FakeElement(),
    "badge-gpu": new FakeElement(),
  };
  const document = {
    getElementById: (id) => elements[id] || null,
    createElement: (tag) => new FakeElement(tag),
    querySelectorAll: () => [],
  };
  const window = {
    _escapeHtml: (value) => String(value || ""),
    pywebview: { api },
    _showToast() {},
    ...windowOverrides,
  };
  vm.runInNewContext(
    fs.readFileSync(path.join(__dirname, "..", "web", "queueRender.js"), "utf8"),
    { window, document, console, Promise, Set, Map, confirm: () => true },
    { filename: "queueRender.js" },
  );
  return { window, elements };
}

function clickClose(row) {
  return row.querySelector(".queue-task-close").trigger("click", {
    stopPropagation() {},
  });
}

function loadSyncControlHarness(askConfirm) {
  const listeners = new Map();
  const document = { getElementById: () => null };
  const window = {
    YT: { bridge: { isUp: () => false } },
    askConfirm,
    addEventListener(kind, fn) {
      const entries = listeners.get(kind) || [];
      entries.push(fn);
      listeners.set(kind, entries);
    },
  };
  vm.runInNewContext(
    fs.readFileSync(path.join(__dirname, "..", "web", "syncSubbed.js"), "utf8"),
    { window, document, console, Promise, Set, confirm: () => true },
    { filename: "syncSubbed.js" },
  );
  return {
    dispatchControl(detail) {
      for (const listener of listeners.get("yt-control") || []) {
        listener({ detail });
      }
    },
  };
}

function flushAsyncWork() {
  return new Promise((resolve) => setImmediate(resolve));
}

test("duplicate sync rows remove only the clicked task ID after success", async () => {
  let release;
  const calls = [];
  const pending = new Promise((resolve) => { release = resolve; });
  const { window, elements } = loadRenderer({
    queues_sync_remove: async (taskId) => {
      calls.push(taskId);
      return pending;
    },
  });
  window.renderQueues({
    identity_ids_durable: true,
    sync: [
      { task_id: "sync-download", represented_task_ids: ["sync-download"],
        kind: "download", name: "Download Same", url: "same", status: "queued",
        pending_index: 0, pending_start: 0, pending_end: 0, draggable: true },
      { task_id: "sync-metadata", represented_task_ids: ["sync-metadata"],
        kind: "metadata", name: "Metadata Same", url: "same", status: "queued",
        pending_index: 1, pending_start: 1, pending_end: 1, draggable: true },
    ],
    gpu: [],
  });

  const action = clickClose(elements["sync-tasks-body"].children[1]);
  assert.equal(elements["sync-tasks-body"].children.length, 2,
    "frontend must not remove optimistically before backend success");
  release({ ok: true });
  await action;

  assert.deepEqual(calls, ["sync-metadata"]);
  assert.deepEqual(
    window._queueStateSnapshot().sync.map((row) => row.task_id),
    ["sync-download"],
  );
});

test("duplicate GPU paths and grouped rows use exact represented IDs", async () => {
  const singleCalls = [];
  const groupCalls = [];
  const { window, elements } = loadRenderer({
    queues_gpu_remove: async (taskId) => {
      singleCalls.push(taskId);
      return { ok: true };
    },
    queues_gpu_remove_many: async (taskIds) => {
      groupCalls.push([...taskIds]);
      return { ok: true };
    },
  });
  window.renderQueues({
    identity_ids_durable: true,
    sync: [],
    gpu: [
      { task_id: "gpu-running", represented_task_ids: ["gpu-running"],
        name: "Transcribing Same", path: "same.mp4", status: "running",
        draggable: false },
      { task_id: "gpu-transcribe", represented_task_ids: ["gpu-transcribe"],
        name: "Transcribe Same", path: "same.mp4", kind: "transcribe",
        status: "queued", pending_index: 0, pending_start: 0, pending_end: 0,
        draggable: true },
      { task_id: "gpu-compress", represented_task_ids: ["gpu-compress"],
        name: "Compress Same", path: "same.mp4", kind: "compress",
        status: "queued", pending_index: 1, pending_start: 1, pending_end: 1,
        draggable: true },
      { task_id: "gpu-b1", represented_task_ids: ["gpu-b1", "gpu-b2"],
        task_ids: ["gpu-b1", "gpu-b2"], name: "Transcribe Group (2 videos)",
        status: "queued", pending_index: 2, pending_start: 2, pending_end: 3,
        draggable: false },
    ],
  });

  const rows = elements["gpu-tasks-body"].children;
  assert.equal(rows[0].draggable, false, "running rows are never draggable");
  assert.equal(rows[3].draggable, false, "synthetic group rows are never draggable");
  await clickClose(rows[2]);
  await clickClose(elements["gpu-tasks-body"].children[2]);

  assert.deepEqual(singleCalls, ["gpu-compress"]);
  assert.deepEqual(groupCalls, [["gpu-b1", "gpu-b2"]]);
  assert.deepEqual(
    window._queueStateSnapshot().gpu.map((row) => row.task_id),
    ["gpu-running", "gpu-transcribe"],
  );
});

test("failed migration disables row mutations until IDs are durable", () => {
  const { window, elements } = loadRenderer({});
  window.renderQueues({
    identity_ids_durable: false,
    sync: [{ task_id: "temporary", represented_task_ids: ["temporary"],
      name: "Download Same", status: "queued", draggable: true }],
    gpu: [],
  });
  const row = elements["sync-tasks-body"].children[0];
  assert.equal(row.draggable, false);
  assert.equal(row.querySelector(".queue-task-close"), null);
});

test("drag reorder sends only task_id and waits for backend success", async () => {
  let release;
  const calls = [];
  const pending = new Promise((resolve) => { release = resolve; });
  const { window, elements } = loadRenderer({
    queues_sync_reorder: async (...args) => {
      calls.push(args);
      return pending;
    },
  });
  window.renderQueues({
    identity_ids_durable: true,
    sync: [
      { task_id: "first", represented_task_ids: ["first"], name: "Download Same",
        url: "same", status: "queued", pending_index: 0, pending_start: 0,
        pending_end: 0, draggable: true },
      { task_id: "second", represented_task_ids: ["second"], name: "Metadata Same",
        url: "same", status: "queued", pending_index: 1, pending_start: 1,
        pending_end: 1, draggable: true },
    ],
    gpu: [],
  });
  let transfer = "";
  const dataTransfer = {
    effectAllowed: "",
    dropEffect: "",
    setData(_kind, value) { transfer = value; },
    getData() { return transfer; },
  };
  const rows = elements["sync-tasks-body"].children;
  rows[0].trigger("dragstart", { dataTransfer, preventDefault() {} });
  const action = rows[1].trigger("drop", {
    dataTransfer, clientY: 15, preventDefault() {},
  });
  assert.deepEqual(
    window._queueStateSnapshot().sync.map((row) => row.task_id),
    ["first", "second"],
  );
  release({ ok: true });
  await action;
  assert.deepEqual(calls, [["first", 1]]);
  assert.deepEqual(
    window._queueStateSnapshot().sync.map((row) => row.task_id),
    ["second", "first"],
  );
});

test("cold Resume routes a restored redownload to its worker", async () => {
  const pauseSync = new FakeElement("button");
  const syncButton = new FakeElement("button");
  const calls = [];
  const api = {
    queue_is_paused: async () => ({ sync: true, gpu: false }),
    resume_pending_redownloads: async () => {
      calls.push("redownload");
      return { ok: true, resumed: 1, regular_pending: 0 };
    },
    sync_start_all: async () => {
      calls.push("regular-sync");
      return { ok: true };
    },
  };
  const document = {
    getElementById: (id) => ({
      "btn-pause-sync-queue": pauseSync,
      "btn-sync-subbed": syncButton,
    }[id] || null),
  };
  const window = {
    pywebview: { api },
    YT: { bridge: { isUp: () => true } },
    _blinkState: {
      sync: { running: false, paused: true, count: 1 },
      gpu: { running: false, paused: false, count: 0 },
    },
    _queueStateSnapshot: () => ({
      sync: [{ task_id: "restored", kind: "redownload" }], gpu: [],
    }),
    _showToast() {},
    addEventListener() {},
  };
  vm.runInNewContext(
    fs.readFileSync(path.join(__dirname, "..", "web", "syncSubbed.js"), "utf8"),
    { window, document, console, Promise, confirm: () => true },
    { filename: "syncSubbed.js" },
  );
  window.initSyncButton();
  await pauseSync.trigger("click", {});
  assert.deepEqual(calls, ["redownload"]);
});

test("global cold Resume routes a redownload without starting regular sync", async () => {
  const pauseAll = new FakeElement("button");
  const syncButton = new FakeElement("button");
  const calls = [];
  const api = {
    resume_pending_redownloads: async () => {
      calls.push("redownload");
      return { ok: true, resumed: 1, regular_pending: 0 };
    },
    sync_start_all: async () => {
      calls.push("regular-sync");
      return { ok: true };
    },
  };
  const document = {
    getElementById: (id) => ({
      "btn-pause": pauseAll,
      "btn-sync-subbed": syncButton,
    }[id] || null),
  };
  const window = {
    pywebview: { api },
    YT: { bridge: { isUp: () => true } },
    _blinkState: {
      sync: { running: false, paused: true, count: 1 },
      gpu: { running: false, paused: false, count: 0 },
    },
    _queueStateSnapshot: () => ({
      sync: [{ task_id: "restored", kind: "redownload" }], gpu: [],
    }),
    _showToast() {},
    addEventListener() {},
  };
  vm.runInNewContext(
    fs.readFileSync(path.join(__dirname, "..", "web", "syncSubbed.js"), "utf8"),
    { window, document, console, Promise, confirm: () => true },
    { filename: "syncSubbed.js" },
  );
  window.initSyncButton();
  await pauseAll.trigger("click", {});
  assert.deepEqual(calls, ["redownload"]);
});

test("cold mixed Resume stages redownload then starts ordinary sync", async () => {
  const pauseSync = new FakeElement("button");
  const syncButton = new FakeElement("button");
  const calls = [];
  const api = {
    queue_is_paused: async () => ({ sync: true, gpu: false }),
    resume_pending_redownloads: async () => {
      calls.push("redownload");
      return { ok: true, resumed: 1, regular_pending: 1 };
    },
    sync_start_all: async (addFresh) => {
      calls.push(["regular-sync", addFresh]);
      return { ok: true };
    },
  };
  const document = {
    getElementById: (id) => ({
      "btn-pause-sync-queue": pauseSync,
      "btn-sync-subbed": syncButton,
    }[id] || null),
  };
  const window = {
    pywebview: { api },
    YT: { bridge: { isUp: () => true } },
    _blinkState: {
      sync: { running: false, paused: true, count: 2 },
      gpu: { running: false, paused: false, count: 0 },
    },
    _queueStateSnapshot: () => ({
      sync: [
        { task_id: "regular", kind: "download" },
        { task_id: "redo", kind: "redownload" },
      ],
      gpu: [],
    }),
    _showToast() {},
    addEventListener() {},
  };
  vm.runInNewContext(
    fs.readFileSync(path.join(__dirname, "..", "web", "syncSubbed.js"), "utf8"),
    { window, document, console, Promise, confirm: () => true },
    { filename: "syncSubbed.js" },
  );
  window.initSyncButton();
  await pauseSync.trigger("click", {});
  assert.deepEqual(calls, ["redownload", ["regular-sync", false]]);
});

test("running exact-ID actions surface backend rejection and bridge errors", async () => {
  const toasts = [];
  let menuItems = [];
  const { window, elements } = loadRenderer({
    gpu_defer_current: async () => ({ ok: false, error: "defer rejected" }),
    gpu_skip_current: async () => { throw new Error("bridge down"); },
  }, {
    askConfirm: async () => true,
    showContextMenu: (_x, _y, items) => { menuItems = items; },
    _showToast: (message, kind) => { toasts.push([message, kind]); },
  });
  window.renderQueues({
    identity_ids_durable: true,
    sync: [],
    gpu: [{ task_id: "gpu-running", represented_task_ids: ["gpu-running"],
      name: "Transcribing Video", status: "running", draggable: false }],
  });

  elements["gpu-tasks-body"].children[0].trigger("contextmenu", {
    preventDefault() {}, stopPropagation() {}, clientX: 1, clientY: 2,
  });
  assert.equal(menuItems.length, 2);
  await menuItems[0].action();
  await menuItems[1].action();

  assert.deepEqual(toasts, [
    ["defer rejected", "error"],
    ["Task could not be cancelled; refresh and retry.: Error: bridge down", "error"],
  ]);
});

test("popover queue controls toast rejected bridge promises", async () => {
  const pauseGpu = new FakeElement("button");
  pauseGpu.dataset.pauseState = "start";
  const syncButton = new FakeElement("button");
  const toasts = [];
  const api = {
    gpu_start: async () => { throw new Error("bridge offline"); },
  };
  const document = {
    getElementById: (id) => ({
      "btn-pause-gpu-queue": pauseGpu,
      "btn-sync-subbed": syncButton,
    }[id] || null),
  };
  const window = {
    pywebview: { api },
    YT: { bridge: { isUp: () => true } },
    _showToast: (message, kind) => { toasts.push([message, kind]); },
    addEventListener() {},
  };
  vm.runInNewContext(
    fs.readFileSync(path.join(__dirname, "..", "web", "syncSubbed.js"), "utf8"),
    { window, document, console, Promise, confirm: () => true },
    { filename: "syncSubbed.js" },
  );
  window.initSyncButton();
  await pauseGpu.trigger("click", {});
  assert.deepEqual(toasts, [[
    "Couldn't start processing. Error: bridge offline", "error",
  ]]);
});

test("channel URL repair notice shows verified old/new copy and one-button options", async () => {
  const dialogs = [];
  const harness = loadSyncControlHarness(async (...args) => {
    dialogs.push(args);
    return true;
  });

  harness.dispatchControl({ kind: "clear_line", marker: "sync-progress" });
  await flushAsyncWork();
  assert.equal(dialogs.length, 0, "unrelated controls must be ignored");

  harness.dispatchControl({
    kind: "channel_url_changed",
    channel_name: "Example Channel",
    channel_id: "UC_permanent_id",
    old_url: "https://www.youtube.com/@OldHandle",
    new_url: "https://www.youtube.com/@NewHandle",
  });
  await flushAsyncWork();

  assert.equal(dialogs.length, 1);
  const [title, message, options] = dialogs[0];
  assert.equal(title, "YouTube channel address updated");
  assert.equal(
    message,
    "“Example Channel” changed its YouTube address.\n\n" +
    "Old: https://www.youtube.com/@OldHandle\n" +
    "New: https://www.youtube.com/@NewHandle\n\n" +
    "YTArchiver matched YouTube’s permanent channel ID, updated " +
    "the saved address automatically, and continued syncing.",
  );
  assert.equal(options.confirm, "Got it");
  assert.equal(options.noCancel, true);
  assert.equal(options.danger, false);
});

test("channel URL repair notice deduplicates the same transition for the session", async () => {
  const dialogs = [];
  const harness = loadSyncControlHarness(async (...args) => {
    dialogs.push(args);
    return true;
  });
  const transition = {
    kind: "channel_url_changed",
    channel_name: "Example Channel",
    channel_id: "UC_permanent_id",
    old_url: "https://www.youtube.com/@OldHandle",
    new_url: "https://www.youtube.com/@NewHandle",
  };

  harness.dispatchControl(transition);
  harness.dispatchControl({ ...transition });
  await flushAsyncWork();
  harness.dispatchControl({ ...transition });
  await flushAsyncWork();

  assert.equal(dialogs.length, 1);
});

test("distinct channel URL repair notices are serialized instead of dropped", async () => {
  const dialogs = [];
  let releaseFirst;
  const firstDialog = new Promise((resolve) => { releaseFirst = resolve; });
  const harness = loadSyncControlHarness((...args) => {
    dialogs.push(args);
    return dialogs.length === 1 ? firstDialog : Promise.resolve(true);
  });

  harness.dispatchControl({
    kind: "channel_url_changed",
    channel_name: "First Channel",
    channel_id: "UC_first",
    old_url: "https://www.youtube.com/@FirstOld",
    new_url: "https://www.youtube.com/@FirstNew",
  });
  harness.dispatchControl({
    kind: "channel_url_changed",
    channel_name: "Second Channel",
    channel_id: "UC_second",
    old_url: "https://www.youtube.com/@SecondOld",
    new_url: "https://www.youtube.com/@SecondNew",
  });
  await flushAsyncWork();

  assert.equal(dialogs.length, 1, "second dialog must wait for the first");
  assert.match(dialogs[0][1], /First Channel/);

  releaseFirst(true);
  await flushAsyncWork();

  assert.equal(dialogs.length, 2);
  assert.match(dialogs[1][1], /Second Channel/);
});
