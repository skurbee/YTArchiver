/* ═══════════════════════════════════════════════════════════════════════
   queuePending.js — Subs-header "Queue Pending" button

   Extracted from app.js boot(). Owns:
     • The Queue Pending badge count (sum of channels with > 0 pending
       transcriptions or metadata fetches)
      • The force-all action remains available when nothing is pending
     • Left-click: queue only channels with pending work
     • Right-click: queue ALL channels (after danger-style confirm)
     • Live re-count on Subs table re-render via MutationObserver

   Publishes:
     window.initQueuePendingButton(trackObserverFn)
       — trackObserverFn is the app's _trackObserver so the
         MutationObserver this module creates gets disconnected on
         beforeunload along with the rest.

   Reads:
     window._subsAllRows               — tables.js caches the row list
     window.pywebview.api.subs_queue_pending / subs_queue_all
     window.refreshSubsTable           — bootstrap helper, post-action repaint
     window._showToast, window.askConfirm
   ═══════════════════════════════════════════════════════════════════════ */

(function () {
  "use strict";

  function bridgeCall(method, ...args) {
    const fn = window.YT?.bridge?.bridgeCall;
    if (fn) return fn(method, ...args);
    return undefined;
  }

  function nativeBridgeUp() {
    return !!window.YT?.bridge?.isUp?.();
  }

  function initQueuePendingButton(trackObserver) {
    const qpBtn = document.getElementById("btn-queue-pending");
    const qpCount = document.getElementById("queue-pending-count");

    // Update the badge count: sum channels with > 0 pending transcribe
    // or metadata work. The backend exposes per-channel `_pending_tx`
    // / `_pending_meta` in the row payload; we just count rows where
    // either is positive.
    const updateBadge = () => {
      if (!qpCount) return;
      let total = 0;
      const rows = window._subsAllRows || [];
      for (const r of rows) {
        if (r._pending_tx > 0 || r._pending_meta > 0) {
          total += 1;
        }
      }
      if (total > 0) {
        qpCount.hidden = false;
        qpCount.textContent = String(total);
        if (qpBtn) qpBtn.hidden = false;
      } else {
        qpCount.hidden = true;
        // Keep the button available: its context menu is also the explicit
        // "Queue all" entry point, which remains useful at a zero count.
        if (qpBtn) qpBtn.hidden = false;
      }
    };

    // Refresh whenever subs render. Hook via a MutationObserver on the
    // subs table body since re-renders are frequent.
    const _obsTarget = document.getElementById("subs-table-body");
    if (_obsTarget) {
      // Debounce updateBadge so a full re-render's burst of mutations
      // doesn't trigger a hundred badge-walk passes in rapid sequence
      // (audit: queuePending.js H196).
      let _badgeTimer = null;
      const _debouncedUpdate = () => {
        if (_badgeTimer) clearTimeout(_badgeTimer);
        _badgeTimer = setTimeout(updateBadge, 50);
      };
      const obs = new MutationObserver(_debouncedUpdate);
      if (typeof trackObserver === "function") trackObserver(obs);
      obs.observe(_obsTarget, { childList: true });
      updateBadge();
    }

    if (!qpBtn) return;

    qpBtn.addEventListener("click", async () => {
      if (!nativeBridgeUp()) {
        window._showToast?.("App still starting - try again in a moment.", "warn");
        return;
      }
      try {
        const res = await bridgeCall("subs_queue_pending");
        if (res?.ok && res?.started) {
          // This endpoint starts a background walk. Its final counts arrive
          // through the backend event bus; do not invent synchronous totals.
          window._showToast?.("Checking channels for pending work…");
        } else {
          window._showToast?.(res?.error || "Queue check did not start.", "error");
        }
      } catch (error) {
        window._showToast?.(
          "Queue check failed: " + (error?.message || error), "error");
      }
    });

    let queueAllBusy = false;
    const queueAll = async () => {
      if (queueAllBusy) return;
      if (!nativeBridgeUp()) {
        window._showToast?.("App still starting - try again in a moment.", "warn");
        return;
      }
      queueAllBusy = true;
      try {
        const ok = await window.askConfirm?.(
          "Queue all channels",
          "Add all channels to the transcription queue? This may take a long time for large libraries.",
          { confirm: "Queue all" });
        if (!ok) return;
        let res = await bridgeCall("subs_queue_all");
        if (res?.ok && res?.needs_choice) {
          const pick = await window.askChoice?.({
            title: "Transcript layout",
            message: "Some organized channels do not have a saved transcript layout. Choose where their transcripts should be written.",
            choices: [
              { label: "One combined transcript per channel", value: "combined" },
              { label: "Separate transcripts by folder", value: "folders" },
            ],
          });
          if (pick !== "combined" && pick !== "folders") return;
          res = await bridgeCall("subs_queue_all", pick === "combined");
        }
        if (res?.ok && res?.started) {
          window._showToast?.("Checking all channels for transcription work…");
        } else {
          window._showToast?.(res?.error || "Queue-all check did not start.", "error");
        }
      } catch (error) {
        window._showToast?.(
          "Queue-all check failed: " + (error?.message || error), "error");
      } finally {
        queueAllBusy = false;
      }
    };
    qpBtn.addEventListener("contextmenu", (event) => {
      event.preventDefault();
      queueAll();
    });
    qpBtn.addEventListener("keydown", (event) => {
      if (event.key !== "ContextMenu"
          && !(event.shiftKey && event.key === "F10")) return;
      event.preventDefault();
      queueAll();
    });

    qpBtn.title = "Left-click: queue channels with pending transcriptions / metadata\nRight-click: queue ALL channels";
    qpBtn.setAttribute("aria-keyshortcuts", "Shift+F10");
  }

  window.initQueuePendingButton = initQueuePendingButton;
})();
