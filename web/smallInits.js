/* ═══════════════════════════════════════════════════════════════════════
   smallInits.js — half a dozen short single-purpose init wirings

   Each function is small enough that a dedicated file would be more
   ceremony than code. Grouped here for now; can be extracted further
   if any of them grow.

   Includes:
     • initLastSyncTicker — "Last Full Sync: XX min ago" label, 60s tick
     • initRecentFilter — Recent-tab live filter input
     • initSubsFilter — Subs-tab live filter input + clear button
     • persistSplitterOnResize — save the activity-log/main-log split
       height to window_state when the splitter is dragged
     • _onArchiveRescanComplete — Python push handler that refreshes
       the currently-visible Browse view after a rescan finishes

   Publishes:
     window.initLastSyncTicker
     window.initRecentFilter
     window.initSubsFilter
     window.persistSplitterOnResize
     window._onArchiveRescanComplete

   Reads:
     window.pywebview.api.* (various)
     window._browseState, window._applyRecentFilter, window._applySubsFilter
     window._showToast, window._reloadCurrentChannelVideos,
     window._reloadChannelsGrid
   ═══════════════════════════════════════════════════════════════════════ */

(function () {
  "use strict";

  // "Last Full Sync: XX min ago" label, refreshed every 60s.
  function initLastSyncTicker() {
    const el = document.getElementById("last-full-sync");
    if (!el) return;
    const tick = async () => {
      const api = window.pywebview?.api;
      if (!api?.get_last_sync_label) return false;
      try {
        const r = await api.get_last_sync_label();
        if (r?.label) el.textContent = r.label;
        return true;
      } catch (e) { /* ignore */ return false; }
    };
    // First tick — try immediately. If pywebview isn't ready yet
    // (the usual case at DOMContentLoaded), the call returns false and
    // the next attempt is up to 60s later, so the label sits at "—"
    // for the whole minute. Retry on `pywebviewready` + a short poll
    // fallback so the label appears within a few hundred ms of boot.
    tick().then((ok) => {
      if (ok) return;
      window.addEventListener("pywebviewready", () => { tick(); },
                              { once: true });
      let tries = 0;
      const poll = () => {
        if (tries >= 20) return;
        tries++;
        tick().then((ok2) => { if (!ok2) setTimeout(poll, 150); });
      };
      setTimeout(poll, 150);
    });
    // Same visibility gate as the deferred-livestreams ticker — the
    // label only matters when the user can see it.
    const _lastSyncTick = setInterval(() => {
      if (document.visibilityState === "visible") tick();
    }, 60_000);
    window.addEventListener("beforeunload", () => clearInterval(_lastSyncTick));
  }

  // Splitter position persistence — saves the activity-log frame's
  // height back to the window-state config so the user's drag survives
  // restart. 400ms debounce so dragging doesn't hammer the bridge.
  function persistSplitterOnResize() {
    const top = document.querySelector(".activity-log-frame");
    if (!top) return;
    let saveTimer = null;
    const obs = new ResizeObserver(() => {
      clearTimeout(saveTimer);
      saveTimer = setTimeout(() => {
        const h = top.getBoundingClientRect().height;
        window.pywebview?.api?.window_state_save?.({ splitter_top_px: Math.round(h) });
      }, 400);
    });
    obs.observe(top);
    // Tear down on unload so the observer + pending timer don't
    // leak across pywebview reloads (audit: smallInits.js H143).
    window.addEventListener("beforeunload", () => {
      try { obs.disconnect(); } catch {}
      try { clearTimeout(saveTimer); } catch {}
    });
    // Apply saved height on load. Guard the .then chain in case the
    // backend returns undefined (audit: smallInits.js H143).
    const _p = window.pywebview?.api?.window_state_load?.();
    if (_p && typeof _p.then === "function") {
      _p.then((st) => {
        if (st?.splitter_top_px && top) top.style.flex = `0 0 ${st.splitter_top_px}px`;
      }).catch(() => {});
    }
  }

  // Recent tab live filter — 100ms debounce, Esc clears.
  function initRecentFilter() {
    const input = document.getElementById("recent-filter");
    if (!input) return;
    let deb = null;
    input.addEventListener("input", () => {
      clearTimeout(deb);
      deb = setTimeout(() => {
        window._applyRecentFilter?.(input.value);
      }, 100);
    });
    input.addEventListener("keydown", (e) => {
      if (e.key === "Escape") { input.value = ""; window._applyRecentFilter?.(""); }
    });
  }

  // Subs tab live filter — 100ms debounce, Esc + clear-button both
  // wipe the input and re-apply the empty filter.
  function initSubsFilter() {
    const input = document.getElementById("subs-filter");
    const clearBtn = document.getElementById("subs-filter-clear");
    if (!input) return;
    let deb = null;
    const syncClear = () => {
      if (clearBtn) clearBtn.hidden = !input.value;
    };
    input.addEventListener("input", () => {
      syncClear();
      clearTimeout(deb);
      deb = setTimeout(() => {
        window._applySubsFilter?.(input.value);
      }, 100);
    });
    input.addEventListener("keydown", (e) => {
      if (e.key === "Escape") {
        input.value = "";
        window._applySubsFilter?.("");
        syncClear();
      }
    });
    clearBtn?.addEventListener("click", () => {
      input.value = "";
      window._applySubsFilter?.("");
      syncClear();
      input.focus();
    });
    syncClear();
  }

  // Push-event handler: backend calls this when archive_rescan finishes
  // so the currently-open Browse grid refreshes to reflect pruned /
  // newly-registered rows. "I click Rescan, wait 5 minutes,
  // nothing changes in the program."
  window._onArchiveRescanComplete = function () {
    try {
      const _browseState = window._browseState || {};
      // If we're viewing a channel's video grid right now, re-query it.
      const ch = (_browseState.currentChannel) || null;
      if (ch && _browseState.view === "videos" &&
          typeof window._reloadCurrentChannelVideos === "function") {
        window._reloadCurrentChannelVideos();
        window._showToast?.("Archive rescan complete — grid refreshed.", "ok");
        return;
      }
      // Channel list view — reload the channel cards so per-channel
      // counts update.
      if (_browseState.view === "channels" &&
          typeof window._reloadChannelsGrid === "function") {
        window._reloadChannelsGrid();
      }
      window._showToast?.("Archive rescan complete.", "ok");
    } catch (e) { /* noop */ }
  };

  window.initLastSyncTicker = initLastSyncTicker;
  window.initRecentFilter = initRecentFilter;
  window.initSubsFilter = initSubsFilter;
  window.persistSplitterOnResize = persistSplitterOnResize;
})();
