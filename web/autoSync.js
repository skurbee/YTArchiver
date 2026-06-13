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
  function nativeBridgeUp() {
    return !!window.YT?.bridge?.isUp?.();
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
          if (nativeBridgeUp()) bridgeCall("queue_auto_set", "sync", true);
        }
        if (!syncCB.disabled) syncCB.disabled = true;
        if (wrap && wrap.title !== "Auto-Sync enabled") {
          wrap.title = "Auto-Sync enabled";
        }
      } else {
        if (syncCB.disabled) syncCB.disabled = false;
        if (wrap && wrap.title) wrap.title = "";
        // uxPolish migrates `title` → `data-tooltip` on hover. Clear
        // both so the tooltip text doesn't outlive the autorun-off
        // state (audit: autoSync.js H134).
        if (wrap && wrap.hasAttribute("data-tooltip")) {
          wrap.removeAttribute("data-tooltip");
        }
      }
    };
    // IPC-bound polling replaced with a cached
    // anchor + local setInterval re-paint. Old loop hit
    // api.autorun_state() every 1s = 86,400 IPC calls/day just for
    // the countdown label. Now: re-anchor every 60s (visibility-gated)
    // and on user actions; decrement locally every 1s.
    let _anchor = null;
    const fetchAnchor = async () => {
      if (!nativeBridgeUp()) return;
      try {
        const st = await bridgeCall("autorun_state");
        if (!st) { _anchor = null; return; }
        _anchor = {
          label: st.label,
          mins: st.mins,
          waiting_for_sync: !!st.waiting_for_sync,
          seconds_remaining: st.seconds_remaining,
          mode: st.mode || "timer",
          next_fire_ts: st.next_fire_ts || null,
          anchored_at_ms: Date.now(),
        };
        if (sel.value !== st.label) sel.value = st.label;
        reconcileSyncAutoLock(st.label !== "Off");
      } catch (e) {
        // Null the anchor so a failed fetch doesn't leave a stale
        // sec===0 state that re-triggers fetchAnchor every paint()
        // tick (1 IPC/sec burst while the backend is unresponsive).
        // The 60s anchor interval + visibilitychange refetch recover.
        _anchor = null;
      }
    };
    // Gate the sec===0 anchor re-fetch with an in-flight flag so the
    // 1Hz paint() can't spam fetchAnchor every tick while the
    // backend's response is still pending (audit: autoSync.js H133).
    let _fetchInFlight = false;
    const paint = () => {
      if (!cd || !_anchor) { if (cd) cd.textContent = ""; return; }
      const st = _anchor;
      if (st.mins > 0 && st.waiting_for_sync) {
        cd.textContent = "waiting for queue\u2026";
      } else if (st.mins > 0 && st.mode === "clock" && st.next_fire_ts) {
        // Clock-aligned mode: show the absolute fire time, e.g. "Next at 7:00pm".
        cd.textContent = `Next at ${_fmtClock(st.next_fire_ts)}`;
        const sec = Math.max(0, Math.floor(st.next_fire_ts - Date.now() / 1000));
        if (sec === 0 && !_fetchInFlight) {
          _fetchInFlight = true;
          fetchAnchor().finally(() => { _fetchInFlight = false; });
        }
      } else if (st.mins > 0 && st.seconds_remaining != null) {
        const elapsed = Math.floor((Date.now() - st.anchored_at_ms) / 1000);
        const sec = Math.max(0, st.seconds_remaining - elapsed);
        cd.textContent = `next in ${_fmtRemain(sec)}`;
        if (sec === 0 && !_fetchInFlight) {
          _fetchInFlight = true;
          fetchAnchor().finally(() => { _fetchInFlight = false; });
        }
      } else {
        cd.textContent = "";
      }
    };
    // Serialize select-change handler so rapid dropdown changes don't
    // overlap autorun_set calls and resolve out-of-order with the
    // last-to-resolve winning instead of last-to-fire (audit:
    // autoSync.js H142).
    let _selChangeInFlight = false;
    sel.addEventListener("change", async () => {
      if (_selChangeInFlight) return;
      if (!nativeBridgeUp()) return;
      _selChangeInFlight = true;
      try {
        await bridgeCall("autorun_set", sel.value);
        window._showToast?.(sel.value === "Off" ? "Auto-sync off." : `Auto-sync every ${sel.value}.`, "ok");
        await fetchAnchor();
        paint();
      } catch (e) { window._showToast?.("Error: " + e, "error"); }
      finally { _selChangeInFlight = false; }
    });
    fetchAnchor().then(paint);
    // Capture interval ids + clean up on beforeunload so they don't
    // leak across pywebview reloads (audit: autoSync.js H140).
    const _paintIv = setInterval(paint, 1000);
    const _anchorIv = setInterval(() => {
      if (document.visibilityState === "visible") {
        fetchAnchor().then(paint);
      }
    }, 60_000);
    window.addEventListener("beforeunload", () => {
      try { clearInterval(_paintIv); } catch {}
      try { clearInterval(_anchorIv); } catch {}
    });
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

  function _fmtClock(ts) {
    const d = new Date(ts * 1000);
    let h = d.getHours();
    const m = d.getMinutes();
    const ap = h >= 12 ? "pm" : "am";
    h = h % 12; if (h === 0) h = 12;
    return `${h}:${String(m).padStart(2, "0")}${ap}`;
  }

  window.initAutorun = initAutorun;
})();
