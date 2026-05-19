/**
 * web/autoSync.js — Auto-sync interval dropdown + countdown ticker
 *
 * Exposed as window.initAutorun; app.js boot calls it once.
 */
(function () {
  "use strict";

  const _browseState = window._browseState || {};
  const showContextMenu = window.showContextMenu || (() => {});
  const askConfirm = window.askConfirm;
  const askDanger = window.askDanger;
  const askQuestion = window.askQuestion;
  const askChoice = window.askChoice;
  function bridgeCall(method, ...args) {
    const fn = window.YT?.bridge?.bridgeCall;
    if (fn) return fn(method, ...args);
    return undefined;
  }

  // ─── Auto-sync dropdown + countdown ──────────────────────────────────
  function initAutorun() {
    const sel = document.getElementById("auto-sync-select");
    const cd = document.getElementById("autorun-countdown");
    if (!sel) return;
    // When Auto-sync is enabled, the Sync Tasks "Auto" checkbox MUST stay
    // checked — otherwise the timer fires, items get queued, and nothing
    // runs them (reported quirk). Lock the checkbox on (and reflect the
    // forced state to the backend) while autorun is active; unlock when
    // it returns to Off so the checkbox behaves normally.
    const reconcileSyncAutoLock = (autorunIsOn) => {
      const syncCB = document.getElementById("sync-auto-checkbox");
      if (!syncCB) return;
      const wrap = syncCB.closest(".queue-auto-wrap");
      // Always sync title to the current autorun state. The prior
      // version only cleared the tooltip when `syncCB.disabled` was
      // true, so if the disabled flag and the title attribute ever
      // drifted (e.g. autorun toggled mid-render, or a re-init re-
      // enabled the checkbox without touching the label) the
      // "Auto-Sync enabled" hint would stick around even after auto-
      // sync went back to Off.
      if (autorunIsOn) {
        if (!syncCB.checked) {
          syncCB.checked = true;
          window.pywebview?.api?.queue_auto_set?.("sync", true);
        }
        if (!syncCB.disabled) syncCB.disabled = true;
        if (wrap && wrap.title !== "Auto-Sync enabled") {
          wrap.title = "Auto-Sync enabled";
        }
      } else {
        if (syncCB.disabled) syncCB.disabled = false;
        if (wrap && wrap.title) wrap.title = "";
      }
    };
    // IPC-bound polling replaced with a cached
    // anchor + local setInterval re-paint. Old loop hit
    // api.autorun_state() every 1s = 86,400 IPC calls/day just for
    // the countdown label. Now: re-anchor every 60s (visibility-gated)
    // and on user actions; decrement locally every 1s.
    let _anchor = null;
    const fetchAnchor = async () => {
      const api = window.pywebview?.api;
      if (!api?.autorun_state) return;
      try {
        const st = await api.autorun_state();
        if (!st) { _anchor = null; return; }
        _anchor = {
          label: st.label,
          mins: st.mins,
          waiting_for_sync: !!st.waiting_for_sync,
          seconds_remaining: st.seconds_remaining,
          anchored_at_ms: Date.now(),
        };
        if (sel.value !== st.label) sel.value = st.label;
        reconcileSyncAutoLock(st.label !== "Off");
      } catch (e) { /* ignore */ }
    };
    const paint = () => {
      if (!cd || !_anchor) { if (cd) cd.textContent = ""; return; }
      const st = _anchor;
      if (st.mins > 0 && st.waiting_for_sync) {
        cd.textContent = "waiting for queue\u2026";
      } else if (st.mins > 0 && st.seconds_remaining != null) {
        const elapsed = Math.floor((Date.now() - st.anchored_at_ms) / 1000);
        const sec = Math.max(0, st.seconds_remaining - elapsed);
        cd.textContent = `next in ${_fmtRemain(sec)}`;
        if (sec === 0) fetchAnchor();
      } else {
        cd.textContent = "";
      }
    };
    sel.addEventListener("change", async () => {
      const api = window.pywebview?.api;
      if (!api?.autorun_set) return;
      try {
        await api.autorun_set(sel.value);
        window._showToast?.(sel.value === "Off" ? "Auto-sync off." : `Auto-sync every ${sel.value}.`, "ok");
        await fetchAnchor();
        paint();
      } catch (e) { window._showToast?.("Error: " + e, "error"); }
    });
    fetchAnchor().then(paint);
    setInterval(paint, 1000);
    setInterval(() => {
      if (document.visibilityState === "visible") {
        fetchAnchor().then(paint);
      }
    }, 60_000);
    document.addEventListener("visibilitychange", () => {
      if (document.visibilityState === "visible") fetchAnchor().then(paint);
    });
  }

  function _fmtRemain(sec) {
    if (sec <= 0) return "now";
    if (sec < 60) return `${sec}s`;
    if (sec < 3600) return `${Math.floor(sec/60)}m ${sec%60}s`;
    return `${Math.floor(sec/3600)}h ${Math.floor((sec%3600)/60)}m`;
  }

  window.initAutorun = initAutorun;
})();
