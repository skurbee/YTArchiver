"use strict";

// Headless behavioral tests: no browser, native bridge, or live profile.
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

class Element {
  constructor(tag = "div") {
    this.tagName = tag.toUpperCase();
    this.children = [];
    this.dataset = {};
    this.style = {};
    this.attributes = {};
    this.handlers = new Map();
    this.named = new Map();
    this.hidden = false;
    this.disabled = false;
    this.value = "";
    this.textContent = "";
    const classes = new Set();
    this.classList = {
      add: (...names) => names.forEach(n => classes.add(n)),
      remove: (...names) => names.forEach(n => classes.delete(n)),
      contains: name => classes.has(name),
      toggle: (name, value) => value ? classes.add(name) : classes.delete(name),
    };
  }
  addEventListener(name, fn) {
    this.handlers.set(name, [...(this.handlers.get(name) || []), fn]);
  }
  removeEventListener(name, fn) {
    this.handlers.set(name, (this.handlers.get(name) || []).filter(f => f !== fn));
  }
  async trigger(name, event = {}) {
    for (const fn of this.handlers.get(name) || []) await fn(event);
  }
  setAttribute(name, value) { this.attributes[name] = value; }
  removeAttribute(name) { delete this.attributes[name]; }
  appendChild(child) { child.parent = this; this.children.push(child); return child; }
  append(...children) { children.forEach(child => this.appendChild(child)); }
  remove() { this.parent.children = this.parent.children.filter(child => child !== this); }
  get firstElementChild() { return this.children[0]; }
  get lastElementChild() { return this.children.at(-1); }
  querySelector(selector) { return this.named.get(selector) || null; }
  querySelectorAll(selector) { return this.named.get(selector) || []; }
  focus() {}
  contains(target) { return target === this || this.children.includes(target); }
  getBoundingClientRect() { return { top: 0, height: 20 }; }
  set innerHTML(value) {
    this._html = value;
    this.children = [];
    this.named.clear();
    for (const selector of [".queue-task-name", ".queue-task-close", ".deferred-id", ".deferred-title"]) {
      if (value.includes(selector.slice(1))) this.named.set(selector, new Element());
    }
    for (const name of ["ignore", "drop"]) {
      if (value.includes(`data-${name}`)) this.named.set(`[data-${name}]`, new Element("button"));
    }
  }
  get innerHTML() { return this._html || ""; }
}

function harness(ids, bridge = async () => ({ ok: true })) {
  const elements = Object.fromEntries(ids.map(id => [id, new Element()]));
  const document = new Element();
  document.getElementById = id => elements[id] || null;
  document.createElement = tag => new Element(tag);
  document.hidden = false;
  document.visibilityState = "visible";
  const timers = new Map();
  const observers = [];
  let nextTimer = 0;
  const toasts = [];
  const window = new Element();
  window.YT = { bridge: { isUp: () => true, bridgeCall: bridge } };
  window._showToast = (...args) => toasts.push(args);
  window._escapeHtml = value => String(value || "");
  const context = vm.createContext({
    window, document, URL, console, Promise, Map, Set,
    localStorage: { getItem: () => null, setItem() {} },
    MutationObserver: class {
      constructor(callback) { this.callback = callback; observers.push(this); }
      observe() {}
      disconnect() { this.disconnected = true; }
    },
    setTimeout(fn, delay) { timers.set(++nextTimer, { fn, delay }); return nextTimer; },
    clearTimeout(id) { timers.delete(id); },
    setInterval() { return ++nextTimer; }, clearInterval() {},
    confirm: () => true,
  });
  return {
    window, document, elements, timers, observers, toasts,
    load(file) {
      vm.runInContext(fs.readFileSync(path.join(__dirname, "..", "web", file), "utf8"), context, { filename: file });
    },
    async tick() {
      const item = timers.entries().next().value;
      assert.ok(item, "an active view schedules its next poll");
      timers.delete(item[0]);
      await item[1].fn();
    },
  };
}

const flush = () => new Promise(resolve => setImmediate(resolve));
const downloadIds = ["panel-download", "manual-download-jobs", "manual-download-list", "url-input", "btn-download-single"];

test("typed and dropped schemeless links reach the bridge as absolute URLs", async () => {
  const calls = [];
  const h = harness(["url-input", "btn-download-single"], async (...args) => {
    calls.push(args); return { ok: true };
  });
  h.load("downloadUrl.js");
  h.window.initUrlField();
  h.elements["url-input"].value = "www.youtube.com/watch?v=abcDEF12345";
  await h.elements["btn-download-single"].trigger("click");
  await h.window._queueSingleVideo("youtu.be/abcDEF12345", {});
  assert.deepEqual(calls.filter(call => call[0] === "archive_single_video").map(call => call[1]), [
    "https://www.youtube.com/watch?v=abcDEF12345", "https://youtu.be/abcDEF12345",
  ]);
  assert.equal(h.elements["url-input"].value, "");
});

test("manual download rows retain their Cancel button until worker acknowledgement", async () => {
  let tasks = [{ task_id: "one", title: "A long recording", state: "running" }];
  const calls = [];
  const h = harness(downloadIds, async (method, ...args) => {
    calls.push([method, ...args]);
    return method === "archive_single_status" ? { ok: true, tasks } : { ok: true };
  });
  h.elements["panel-download"].classList.add("active");
  h.load("downloadUrl.js");
  h.window.initUrlField();
  await flush();
  const list = h.elements["manual-download-list"];
  const row = list.children[0];
  const button = row.lastElementChild;
  assert.equal(row.firstElementChild.title, "A long recording");
  await h.tick();
  assert.equal(list.children[0].lastElementChild, button, "polling preserves focus/DOM identity");
  await button.trigger("click");
  await flush();
  assert.deepEqual(calls.filter(call => call[0] === "archive_single_cancel"), [["archive_single_cancel", "one"]]);
  assert.equal(button.textContent, "Cancelling…");
  assert.equal(button.disabled, true);
  assert.equal(list.children[0], row);
  await button.trigger("click");
  assert.equal(calls.filter(call => call[0] === "archive_single_cancel").length, 1);
  tasks = [];
  await h.tick();
  assert.equal(list.children.length, 0);
  assert.equal(h.elements["manual-download-jobs"].hidden, true);
});

test("manual status ignores stale replies and stops polling when hidden or unloaded", async () => {
  let release;
  let count = 0;
  const h = harness(downloadIds, method => {
    if (method !== "archive_single_status") return Promise.resolve({ ok: true });
    count += 1;
    if (count === 1) return new Promise(resolve => { release = resolve; });
    return Promise.resolve({ ok: true, tasks: [{ task_id: "new", title: "Current" }] });
  });
  const panel = h.elements["panel-download"];
  panel.classList.add("active");
  h.load("downloadUrl.js");
  h.window.initUrlField();
  panel.classList.remove("active");
  h.observers[0].callback();
  release({ ok: true, tasks: [{ task_id: "old", title: "Stale" }] });
  await flush();
  assert.equal(h.elements["manual-download-list"].children.length, 0);
  assert.equal(h.timers.size, 0);
  await h.window._refreshManualDownloads();
  assert.equal(count, 1);
  panel.classList.add("active");
  h.observers[0].callback();
  await flush();
  assert.equal(h.elements["manual-download-list"].children[0].dataset.taskId, "new");
  h.document.hidden = true;
  await h.document.trigger("visibilitychange");
  assert.equal(h.timers.size, 0);
  await h.window.trigger("pagehide");
  assert.equal(h.observers[0].disconnected, true);
  h.document.hidden = false;
  await h.window._refreshManualDownloads();
  assert.equal(count, 2);
});

test("failed manual cancellation restores the actionable button and displays the error", async () => {
  const h = harness(downloadIds, async method => method === "archive_single_status"
    ? { ok: true, tasks: [{ task_id: "one", title: "Video" }] }
    : method === "archive_single_cancel" ? { ok: false, error: "Could not signal worker" } : { ok: true });
  h.elements["panel-download"].classList.add("active");
  h.load("downloadUrl.js");
  h.window.initUrlField();
  await flush();
  const button = h.elements["manual-download-list"].children[0].lastElementChild;
  await button.trigger("click");
  assert.equal(button.disabled, false);
  assert.equal(button.textContent, "Cancel");
  assert.equal(h.toasts.at(-1)[0], "Could not signal worker");
});

test("Auto-off queued work paints Start and both Sync controls start only the saved queue", async () => {
  const calls = [];
  const h = harness(["btn-sync-subbed", "btn-pause", "btn-pause-sync-queue", "btn-pause-gpu-queue"]);
  for (const element of Object.values(h.elements)) {
    element.named.set("svg", new Element("svg"));
    element.named.set("span", new Element("span"));
  }
  h.window.pywebview = { api: {
    sync_start_all: async value => { calls.push(["sync_start_all", value]); return { ok: true }; },
    queue_is_paused: async () => ({ sync: false, gpu: false }),
    queue_pause: async queue => { calls.push(["queue_pause", queue]); return { ok: true }; },
  } };
  h.load("queueBlink.js");
  h.load("syncSubbed.js");
  h.window.initSyncButton();
  h.window._blinkState.sync.count = 2;
  h.window._paintBlinkState();
  h.window._syncPauseButtonState();
  assert.equal(h.elements["btn-pause"].disabled, false);
  assert.equal(h.elements["btn-pause"].dataset.pauseState, "start");
  assert.equal(h.elements["btn-pause-sync-queue"].querySelector("span").textContent, "Start");
  await h.elements["btn-pause"].trigger("click");
  await h.elements["btn-pause-sync-queue"].trigger("click");
  assert.deepEqual(calls, [["sync_start_all", false], ["sync_start_all", false]]);
});

test("a cancelling Sync row remains visible and suppresses repeated mutation actions", async () => {
  let menus = 0;
  const h = harness(["sync-tasks-body", "gpu-tasks-body"]);
  h.window.showContextMenu = () => { menus += 1; };
  h.load("queueRender.js");
  h.window.renderQueues({ sync: [{ task_id: "one", name: "Downloading Channel", status: "running", cancel_requested: true }], gpu: [] });
  const row = h.elements["sync-tasks-body"].children[0];
  assert.match(row.querySelector(".queue-task-name").innerHTML, /Cancelling/);
  assert.equal(row.querySelector(".queue-task-close"), null);
  assert.equal(row.draggable, false);
  await row.trigger("contextmenu", { preventDefault() {}, stopPropagation() {} });
  assert.equal(menus, 0);
  assert.equal(h.window._queueStateSnapshot().sync[0].task_id, "one");
});

test("deferred reminders describe snoozing and Auto-off staging truthfully", async () => {
  const calls = [];
  const h = harness(["deferred-retry-menu"], async (...args) => {
    calls.push(args); return { ok: true, started: false };
  });
  const buttons = ["now", "24h", "1w"].map(mode => {
    const button = new Element("button"); button.dataset.retry = mode; return button;
  });
  h.elements["deferred-retry-menu"].named.set("button[data-retry]", buttons);
  h.load("liveDrawer.js");
  h.window.initDeferredLivestreams();
  for (const button of buttons) await button.trigger("click");
  assert.deepEqual(calls, [["sync_start_all"], ["livestreams_snooze", 86400], ["livestreams_snooze", 604800]]);
  assert.match(h.toasts[0][0], /Channels queued.*Start/);
  assert.match(h.toasts[1][0], /Reminders hidden for 24 hours/);
  assert.match(h.toasts[2][0], /Reminders hidden for 1 week/);
});
