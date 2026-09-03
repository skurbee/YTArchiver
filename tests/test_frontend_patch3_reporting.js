"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

class FakeClassList {
  add() {}
  remove() {}
}

class FakeElement {
  constructor(tag = "div") {
    this.tagName = tag.toUpperCase();
    this.dataset = {};
    this.classList = new FakeClassList();
    this.handlers = new Map();
    this.children = [];
    this.style = {};
    this.textContent = "";
    this.innerHTML = "";
    this.value = "";
    this.checked = false;
    this.placeholder = "";
    this.parentElement = null;
    this.attributes = new Map();
  }

  addEventListener(kind, fn) { this.handlers.set(kind, fn); }
  trigger(kind, event = {}) { return this.handlers.get(kind)?.(event); }
  appendChild(child) { this.children.push(child); return child; }
  setAttribute(name, value) { this.attributes.set(name, String(value)); }
  getContext() { return {}; }
}

function loadGraphModule() {
  const elements = new Map();
  const make = (id, tag = "div") => {
    const el = new FakeElement(tag);
    elements.set(id, el);
    return el;
  };
  make("btn-graph-run", "button");
  const exportButton = make("btn-graph-export-csv", "button");
  const word = make("graph-word", "input");
  word.placeholder = "Enter a word";
  const channel = make("graph-channel", "select");
  channel.value = "All";
  const bucket = make("graph-bucket", "select");
  bucket.value = "month";
  make("graph-normalize", "input");
  make("graph-empty");
  const wrap = new FakeElement();
  const canvas = make("graph-canvas", "canvas");
  canvas.parentElement = wrap;
  const wordCloudButton = new FakeElement("button");
  wordCloudButton.dataset.type = "wordcloud";

  const document = {
    getElementById(id) { return elements.get(id) || null; },
    querySelectorAll(selector) {
      return selector === ".chart-type-btn" ? [wordCloudButton] : [];
    },
    createElement(tag) {
      const el = new FakeElement(tag);
      const originalAppend = wrap.appendChild.bind(wrap);
      wrap.appendChild = (child) => {
        if (child.id) elements.set(child.id, child);
        return originalAppend(child);
      };
      return el;
    },
  };
  let savedCsv = "";
  const samplingLabel =
    "Limited sample: word frequencies use at most the oldest 500,000 transcript segments, not the complete archive.";
  const window = {
    YT: {
      util: { escapeHtml: (value) => String(value || "") },
      bridge: {
        isUp: () => true,
        async catalogRead(_key, task, options = {}) {
          options.onStatus?.({ phase: "loading", text: "Loading…", elapsedMs: 0 });
          try {
            const value = await task({ isCurrent: () => true, generation: 1 });
            return { stale: false, skipped: false, value };
          } finally {
            options.onStatus?.({ phase: "done", text: "", elapsedMs: 0 });
          }
        },
        async bridgeCall(method, ...args) {
          if (method === "browse_word_cloud") {
            return {
              ok: true,
              words: [{ word: "archive", count: 2 }],
              sampling: { limited: true, limit: 500000, label: samplingLabel },
            };
          }
          if (method === "save_text_to_file") {
            savedCsv = args[1];
            return { ok: true };
          }
          throw new Error(`unexpected bridge method: ${method}`);
        },
      },
    },
    pywebview: { api: { browse_word_cloud() {} } },
    _showToast() {},
  };
  vm.runInNewContext(
    fs.readFileSync(path.join(__dirname, "..", "web", "graphTab.js"), "utf8"),
    { window, document, console, Promise, Set, Map, Chart: function Chart() {}, setTimeout },
    { filename: "graphTab.js" },
  );
  window.YT.graph.init();
  return {
    window,
    wordCloudButton,
    exportButton,
    samplingLabel,
    savedCsv: () => savedCsv,
  };
}

test("word-cloud CSV prominently discloses the limited sample", async () => {
  const graph = loadGraphModule();
  graph.wordCloudButton.trigger("click");
  await graph.window.YT.graph.draw();
  await graph.exportButton.trigger("click");

  const csv = graph.savedCsv();
  assert.match(csv, /^SAMPLE SCOPE,/);
  assert.match(csv, /oldest 500,000/);
  assert.match(csv, /not the complete archive/);
  assert.match(csv, /\n\nword,count\narchive,2$/);
});
