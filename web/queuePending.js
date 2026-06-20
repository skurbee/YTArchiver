/* ═══════════════════════════════════════════════════════════════════════
   queuePending.js — Subs-header "Queue Pending" button

   Extracted from app.js boot(). Owns:
     • The Queue Pending badge count (sum of channels with > 0 pending
       transcriptions or metadata fetches)
     • Auto-hide of the button when nothing is pending
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
        // Hide the whole button when nothing's pending — no point in
        // showing a "Queue Pending" affordance with zero work to do.
        if (qpBtn) qpBtn.hidden = true;
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
      const res = await bridgeCall("subs_queue_pending");
      if (res?.ok) {
        const parts = [];
        if (res.transcribe_queued) parts.push(`${res.transcribe_queued} for transcribe`);
        if (res.metadata_queued) parts.push(`${res.metadata_queued} for metadata`);
        window._showToast?.(parts.length
          ? `Queued ${parts.join(", ")}.`
          : "No pending channels.", parts.length ? "ok" : "warn");
      } else {
        window._showToast?.(res?.error || "Queue pending failed.", "error");
      }
      // Re-fetch channel rows so the badge reflects backend counter
      // resets. Without this, chan_transcribe_pending can zero a
      // channel's counter and the button badge happily keeps showing
      // the pre-click count because the row cache never refreshed.
      try { await window.refreshSubsTable?.(); } catch (_e) {}
      setTimeout(updateBadge, 500);
    });

    qpBtn.addEventListener("contextmenu", async (e) => {
      e.preventDefault();
      if (!nativeBridgeUp()) {
        window._showToast?.("App still starting - try again in a moment.", "warn");
        return;
      }
      const ok = await window.askConfirm?.(
        "Queue all channels",
        "Add ALL channels to the transcribe queue? This may take a long time for large libraries.",
        { confirm: "Queue all" });
      if (!ok) return;
      const res = await bridgeCall("subs_queue_all");
      if (res?.ok) {
        window._showToast?.(`Queued ${res.queued} channels.`, "ok");
      } else {
        window._showToast?.(res?.error || "Queue all failed.", "error");
      }
    });

    qpBtn.title = "Left-click: queue channels with pending transcriptions / metadata\nRight-click: queue ALL channels";
  }

  window.initQueuePendingButton = initQueuePendingButton;
})();
