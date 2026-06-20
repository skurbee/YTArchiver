/**
 * web/liveDrawer.js — deferred-livestreams drawer (downstream from sync)
 *
 * Exposed as window.initDeferredLivestreams; app.js boot calls it once.
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

  // ─── Deferred livestreams drawer ─────────────────────────────────────
  async function refreshDeferredLivestreams() {
    const wrap = document.getElementById("deferred-livestreams");
    const list = document.getElementById("deferred-list");
    const count = document.getElementById("deferred-count");
    if (!wrap || !list) return;
    if (!nativeBridgeUp()) { wrap.hidden = true; return; }
    try {
      // Check snooze state first — if active, keep the drawer hidden
      // regardless of how many deferred items exist. Timer in
      // initDeferredLivestreams re-checks every 30s so the drawer
      // reappears automatically after snooze expires.
      const state = await bridgeCall("livestreams_drawer_state");
      if (state?.ok && state.visible === false) {
        wrap.hidden = true;
        return;
      }
      const res = await bridgeCall("livestreams_list");
      const items = res?.items || [];
      if (!items.length) { wrap.hidden = true; return; }
      wrap.hidden = false;
      if (count) count.textContent = `${items.length}`;
      list.innerHTML = "";
      for (const it of items) {
        const row = document.createElement("div");
        row.className = "deferred-row";
        row.innerHTML = `
          <span class="deferred-id"></span>
          <span class="deferred-title"></span>
          <button data-ignore class="deferred-ignore"
                  title="Never try this video again">Ignore</button>
          <button data-drop title="Forget this one for now (may reappear on next sync)">&times;</button>
        `;
        row.querySelector(".deferred-id").textContent = it.video_id;
        row.querySelector(".deferred-title").textContent = it.title
          ? ` \u2014 ${it.title.slice(0, 60)}` : "";
        row.querySelector("[data-drop]").addEventListener("click", async () => {
          try {
            await bridgeCall("livestreams_drop", it.video_id);
            refreshDeferredLivestreams();
          } catch (e) {
            window._showToast?.("Couldn't drop deferred entry: " + e, "error");
          }
        });
        row.querySelector("[data-ignore]").addEventListener("click", async () => {
          // UI audit #11: lead with the human title, not the raw
          // YouTube ID. ID is secondary detail under the title.
          const _name = (it.title && it.title.trim())
            ? it.title.slice(0, 80)
            : `(no title)  ${it.video_id}`;
          const ok = await window.askQuestion?.({
            title: "Ignore this video?",
            message: `Permanently skip "${_name}"? Future sync passes will not re-defer it.`,
            confirm: "Ignore",
            cancel: "Keep",
            danger: true,
          });
          if (!ok) return;
          try {
            await bridgeCall("livestreams_ignore", it.video_id);
            window._showToast?.("Ignored. Won't appear again.", "ok");
            refreshDeferredLivestreams();
          } catch (e) {
            window._showToast?.("Couldn't ignore video: " + e, "error");
          }
        });
        list.appendChild(row);
      }
    } catch (e) { console.warn("deferred:", e); }
  }

  function initDeferredLivestreams() {
    // Retry dropdown — Now kicks a sync immediately; 24h / 1 week
    // snooze the drawer so it stops nagging until that time.
    const retryBtn = document.getElementById("btn-deferred-retry");
    const retryMenu = document.getElementById("deferred-retry-menu");
    const closeMenu = () => { if (retryMenu) retryMenu.hidden = true; };
    retryBtn?.addEventListener("click", (e) => {
      e.stopPropagation();
      if (!retryMenu) return;
      retryMenu.hidden = !retryMenu.hidden;
    });
    document.addEventListener("click", (e) => {
      if (!retryMenu || retryMenu.hidden) return;
      if (!retryMenu.contains(e.target) && e.target !== retryBtn) closeMenu();
    });
    retryMenu?.querySelectorAll("button[data-retry]").forEach((b) => {
      b.addEventListener("click", async () => {
        const mode = b.dataset.retry;
        closeMenu();
        if (mode === "now") {
          if (nativeBridgeUp()) bridgeCall("sync_start_all");
          window._showToast?.("Retrying deferred livestreams via Sync Subbed.", "ok");
        } else if (mode === "24h") {
          if (nativeBridgeUp()) await bridgeCall("livestreams_snooze", 24 * 60 * 60);
          window._showToast?.("Deferred livestreams snoozed for 24 hours.", "ok");
          refreshDeferredLivestreams();
        } else if (mode === "1w") {
          if (nativeBridgeUp()) await bridgeCall("livestreams_snooze", 7 * 24 * 60 * 60);
          window._showToast?.("Deferred livestreams snoozed for 1 week.", "ok");
          refreshDeferredLivestreams();
        }
      });
    });
    document.getElementById("btn-deferred-clear")?.addEventListener("click", async () => {
      const ok = await askDanger(
        "Clear deferred livestreams",
        "Forget every deferred livestream in the journal?\n\nThis doesn't delete any files.", "Clear");
      if (!ok) return;
      if (!nativeBridgeUp()) return;
      const r = await bridgeCall("livestreams_list");
      let cleared = 0;
      let failed = 0;
      for (const it of (r?.items || [])) {
        try {
          const dropped = await bridgeCall("livestreams_drop", it.video_id);
          if (dropped?.ok) cleared += 1;
          else failed += 1;
        } catch (e) {
          failed += 1;
          console.warn("[deferred] clear failed", it?.video_id, e);
        }
      }
      const total = cleared + failed;
      if (failed) {
        window._showToast?.(`Cleared ${cleared} of ${total}; ${failed} failed.`, "warn");
      } else {
        window._showToast?.(`Cleared ${cleared} deferred livestream${cleared === 1 ? "" : "s"}.`, "ok");
      }
      refreshDeferredLivestreams();
    });
    refreshDeferredLivestreams();
    // pause the 30s refresh tick when the window is
    // hidden (minimized, different virtualdesktop, tray-only).
    // No reason to burn a backend round-trip every 30s while the
    // user can't see the drawer. The initial refresh above still
    // runs so a freshly-opened window is current.
    const _deferredTick = setInterval(() => {
      if (document.visibilityState === "visible") {
        refreshDeferredLivestreams();
      }
    }, 30_000);
    window.addEventListener("beforeunload", () => clearInterval(_deferredTick));
  }

  window.initDeferredLivestreams = initDeferredLivestreams;
})();
