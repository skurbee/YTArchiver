/**
 * web/activityLogVis.js — Auto-hide the activity-log frame when it would be empty
 */
(function () {
  "use strict";

  const _browseState = window._browseState || {};
  const showContextMenu = window.showContextMenu || (() => {});
  const askConfirm = window.askConfirm;
  const askDanger = window.askDanger;
  const askQuestion = window.askQuestion;
  const askChoice = window.askChoice;
  const askTextInput = window.askTextInput;
  const escapeHtml = window.YT?.util?.escapeHtml || ((s) => String(s ?? ""));
  function bridgeCall(method, ...args) {
    const fn = window.YT?.bridge?.bridgeCall;
    if (fn) return fn(method, ...args);
    return undefined;
  }

  // ─── Activity log auto-hide when empty ───────────────────────────────
  // When there are no autorun_history entries to show, collapse the
  // activity-log-frame so users don't see an empty 3-row blank above
  // the main log.
  //
  // When the log transitions from empty → populated (e.g. first log
  // emission after the user clicks Clear), drop any user-dragged
  // height back to the CSS default (3 rows). Without this, a user
  // who resized the log to 20 rows and then cleared it would see
  // the next single log line render inside a 20-row empty frame,
  // which looks broken. Design: open to roughly 3 lines worth of
  // height when the first log line fires after a clear.
  let _lastActivityHasItems = false;
  function syncActivityLogVisibility() {
    const el = document.getElementById("activity-log");
    const frame = document.querySelector(".activity-log-frame");
    const splitter = document.getElementById("paned-splitter");
    if (!el || !frame) return;
    const hasItems = el.childElementCount > 0;
    // Rising edge: empty → populated. Reset inline flex so the CSS
    // rule (`.activity-log-frame { flex: 0 0 56px }` → ~3 rows)
    // takes effect instead of whatever height the splitter drag
    // last applied.
    if (hasItems && !_lastActivityHasItems) {
      try { frame.style.removeProperty("flex"); } catch (_e) { /* noop */ }
    }
    _lastActivityHasItems = hasItems;
    frame.hidden = !hasItems;
    if (splitter) splitter.hidden = !hasItems;
  }

  // Exposed so app.js / logs.js / autorun-history hooks can re-evaluate
  // visibility whenever activity-log content changes.
  window._syncActivityLogVisibility = syncActivityLogVisibility;

  // Auto-re-evaluate on EVERY change to the activity log's children. The
  // runtime append paths (appendActivityLog + the batched _logBatch
  // handler + renderActivityLog) add rows WITHOUT calling
  // syncActivityLogVisibility, so an activity log that started empty (frame
  // auto-hidden) stayed hidden even after an overnight autorun filled it
  // with downloads — you could "Clear activity" but never SEE it. A
  // childList observer covers every add path at once. Setting frame.hidden
  // doesn't touch #activity-log's children, so this can't loop.
  try {
    const _watch = document.getElementById("activity-log");
    if (_watch && typeof MutationObserver === "function") {
      new MutationObserver(syncActivityLogVisibility)
        .observe(_watch, { childList: true });
      // Sync once now in case content is already present at load.
      syncActivityLogVisibility();
    }
  } catch (_e) { /* non-fatal */ }
})();
