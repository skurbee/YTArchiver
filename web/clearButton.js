/**
 * web/clearButton.js — consolidated Clear dropdown next to the Pause button
 *
 * Exposed as window.initClearLog; app.js boot calls it once.
 */
(function () {
  "use strict";

  const _browseState = window._browseState || {};
  const showContextMenu = window.showContextMenu || (() => {});
  const askConfirm = window.askConfirm;
  const askDanger = window.askDanger;
  const askQuestion = window.askQuestion;
  function bridgeCall(method, ...args) {
    const fn = window.YT?.bridge?.bridgeCall;
    if (fn) return fn(method, ...args);
    return undefined;
  }

  // ─── Clear button wiring (consolidated dropdown) ─────────────────────
  function initClearLog() {
    // Single "Clear ▾" button (next to the Pause button in the main
    // controls row). Click opens a context menu with two options:
    //   - Clear log       (wipe the visible main log)
    //   - Clear activity  (wipe + persist the activity-log history)
    // Replaces the two separate buttons (main "Clear log" + autorun-
    // row "Clear") that used to take up space in different places.
    const btn = document.getElementById("btn-clear-menu");
    if (!btn) return;

    async function doClearMainLog() {
      const ok = await askConfirm(
        "Clear log",
        "Clear the main log?\n\nThis only clears the visible log \u2014 no files are affected.",
        { confirm: "Clear", danger: true });
      if (!ok) return;
      window.clearLog?.("main-log");
    }

    async function doClearActivity() {
      const ok = await askConfirm(
        "Clear activity log",
        "Permanently clear the activity-log history? This cannot be undone.",
        { confirm: "Clear", danger: true });
      if (!ok) return;
      const api = window.pywebview?.api;
      if (api?.autorun_history_clear) {
        try {
          const res = await api.autorun_history_clear();
          if (!res?.ok) {
            window._showToast?.(res?.error || "Clear failed.", "error");
            return;
          }
        } catch (e) {
          window._showToast?.("Clear failed: " + e, "error");
          return;
        }
      }
      window.clearLog?.("activity-log");
      try { window._syncActivityLogVisibility?.(); } catch (_e) {}
      window._showToast?.("Activity log cleared.", "ok");
    }

    btn.addEventListener("click", (ev) => {
      ev.stopPropagation();
      // Toggle: if the menu is already open, a second button click
      // should close it instead of popping another on top. Detect via
      // the shared `ctx-menu-root` container the context-menu helper
      // appends into.
      // Only close-and-bail when the open menu is OUR menu — otherwise
      // clicking Clear while a different context menu is open would
      // hijack it (audit: clearButton.js H132). Tag with data-source.
      const ctxRoot = document.getElementById("ctx-menu-root");
      if (ctxRoot && ctxRoot.querySelector('[data-source="clear-menu"]')) {
        ctxRoot.innerHTML = "";
        return;
      }
      if (ctxRoot && ctxRoot.childElementCount > 0) {
        // Different menu is open — close it and proceed to open ours.
        ctxRoot.innerHTML = "";
      }
      // Only surface the options that correspond to logs with actual
      // content. If the main log is empty, no "Clear log" item. If the
      // activity log is empty, no "Clear activity" item. If both are
      // empty, `syncClearButtonsVisibility` has already hidden the
      // button itself, so this code path doesn't run — but we guard
      // against an empty menu just in case.
      const mainLog = document.getElementById("main-log");
      const actLog = document.getElementById("activity-log");
      const hasMain = !!(mainLog && mainLog.childElementCount > 0);
      const hasAct = !!(actLog && actLog.childElementCount > 0);
      // Order: Clear activity on top, Clear log on bottom — the
      // activity row sits above the main log in the UI layout, so
      // the menu order mirrors that vertical arrangement.
      const items = [];
      if (hasAct) {
        items.push({ label: "Clear activity",
                     action: () => { doClearActivity(); } });
      }
      if (hasMain) {
        items.push({ label: "Clear log",
                     action: () => { doClearMainLog(); } });
      }
      if (!items.length) return;
      const rect = btn.getBoundingClientRect();
      if (window.showContextMenu) {
        window.showContextMenu(rect.left, rect.bottom + 2, items);
        // Tag the newly-opened menu so the toggle path above can
        // identify it (audit: clearButton.js H132).
        const _cm = document.getElementById("ctx-menu-root");
        if (_cm && _cm.firstElementChild) {
          _cm.firstElementChild.setAttribute("data-source", "clear-menu");
        }
      }
    });
  }

  window.initClearLog = initClearLog;
})();
