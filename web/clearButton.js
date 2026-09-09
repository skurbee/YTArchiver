/**
 * web/clearButton.js — Clear log action and optional activity-history dropdown
 *
 * Exposed as window.initClearLog; app.js boot calls it once.
 */
(function () {
  "use strict";

  const askConfirm = window.askConfirm;
  function bridgeCall(method, ...args) {
    const fn = window.YT?.bridge?.bridgeCall;
    if (fn) return fn(method, ...args);
    return undefined;
  }
  function nativeBridgeUp() {
    return !!window.YT?.bridge?.isUp?.();
  }
  function activityLogEnabled() {
    return typeof window._isActivityLogEnabled === "function"
      ? !!window._isActivityLogEnabled() : true;
  }

  // Hidden activity history must not keep the Clear log action visible.
  // When activity is enabled, either log can make the dropdown useful.
  // style.display is used rather than the [hidden] attribute because the
  // .btn class sets a display value that would override [hidden].
  function syncClearButtonVisibility() {
    const btn = document.getElementById("btn-clear-menu");
    if (!btn) return;
    const mainLog = document.getElementById("main-log");
    const actLog = document.getElementById("activity-log");
    const activityEnabled = activityLogEnabled();
    const hasMain = !!(mainLog && mainLog.childElementCount > 0);
    const hasAct = activityEnabled && !!(actLog && actLog.childElementCount > 0);
    btn.style.display = (hasMain || hasAct) ? "" : "none";
    const label = btn.querySelector(".clear-menu-label");
    if (label) label.textContent = activityEnabled ? "Clear" : "Clear log";
    const caret = btn.querySelector(".clear-menu-caret");
    if (caret) caret.hidden = !activityEnabled;
    const tip = activityEnabled ? "Clear log or activity history" : "Clear log";
    btn.setAttribute("data-tooltip", tip);
    btn.setAttribute("aria-label", tip);
    btn.removeAttribute("title");
    if (activityEnabled) btn.setAttribute("aria-haspopup", "menu");
    else {
      btn.removeAttribute("aria-haspopup");
      const ctxRoot = document.getElementById("ctx-menu-root");
      if (ctxRoot?.querySelector('[data-source="clear-menu"]')) {
        ctxRoot.innerHTML = "";
      }
    }
  }
  window._syncClearButtonVisibility = syncClearButtonVisibility;

  // ─── Clear button wiring ─────────────────────────────────────────────
  function initClearLog() {
    // With activity enabled, the button opens a context menu:
    //   - Clear log       (wipe the visible main log)
    //   - Clear activity  (wipe + persist the activity-log history)
    // Otherwise it directly confirms clearing the visible main log.
    const btn = document.getElementById("btn-clear-menu");
    if (!btn) return;

    // Keep the button's visibility synced to log content: observe both log
    // containers for childList changes (append / bulk render / clear / trim)
    // so the button shows or vanishes automatically, then set the initial
    // state once now.
    try {
      const _obs = new MutationObserver(() => syncClearButtonVisibility());
      ["main-log", "activity-log"].forEach((id) => {
        const t = document.getElementById(id);
        if (t) _obs.observe(t, { childList: true });
      });
    } catch (_e) { /* observer is best-effort; initial sync still applies */ }
    syncClearButtonVisibility();

    async function doClearMainLog() {
      const ok = await askConfirm(
        "Clear log",
        "Clear the main log?\n\nThis only clears the visible log \u2014 no files are affected.",
        { confirm: "Clear", danger: true });
      if (!ok) return;
      window.clearLog?.("main-log");
    }

    async function doClearActivity() {
      if (!activityLogEnabled()) return;
      const ok = await askConfirm(
        "Clear activity log",
        "Permanently clear the activity-log history? This cannot be undone.",
        { confirm: "Clear", danger: true });
      if (!ok || !activityLogEnabled()) return;
      if (nativeBridgeUp()) {
        try {
          const res = await bridgeCall("autorun_history_clear");
          if (!res?.ok) {
            window._showToast?.(res?.error || "Clear failed.", "error");
            return;
          }
        } catch (e) {
          window._showToast?.("Clear failed: " + e, "error");
          return;
        }
      }
      if (window.renderActivityLog) window.renderActivityLog([]);
      else window.clearLog?.("activity-log");
      try { window._syncActivityLogVisibility?.(); } catch (_e) {}
      try { window._syncClearButtonVisibility?.(); } catch (_e) {}
      window._showToast?.("Activity log cleared.", "ok");
    }

    btn.addEventListener("click", (ev) => {
      ev.stopPropagation();
      const ctxRoot = document.getElementById("ctx-menu-root");
      if (!activityLogEnabled()) {
        if (ctxRoot) ctxRoot.innerHTML = "";
        const mainLog = document.getElementById("main-log");
        if (mainLog && mainLog.childElementCount > 0) return doClearMainLog();
        return;
      }
      // Toggle: if the menu is already open, a second button click
      // should close it instead of popping another on top. Detect via
      // the shared `ctx-menu-root` container the context-menu helper
      // appends into.
      // Only close-and-bail when the open menu is OUR menu — otherwise
      // clicking Clear while a different context menu is open would
      // hijack it (audit: clearButton.js H132). Tag with data-source.
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
