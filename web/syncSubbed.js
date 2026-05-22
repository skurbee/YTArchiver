/**
 * web/syncSubbed.js — Sync Subbed primary action button + pause/resume state plumbing
 *
 * Exposed as window.initSyncSubbedButton; app.js boot calls it once.
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

  // Wraps an async click handler so a second click that lands before
  // the first finishes is dropped. Stops "user clicked Sync 3 times,
  // 3 sync_start_all requests in flight" races.
  // (Previously lived as a private helper in app.js; moved here when
  // syncSubbed.js was extracted into its own IIFE.)
  function _inFlight(fn) {
    let busy = false;
    return async function (...args) {
      if (busy) return;
      busy = true;
      try { return await fn.apply(this, args); }
      finally { busy = false; }
    };
  }

  // ─── Sync Subbed button ──────────────────────────────────────────────
  function initSyncButton() {
    const btn = document.getElementById("btn-sync-subbed");
    const pauseBtn = document.getElementById("btn-pause");
    if (!btn) return;

    // Right-click: append every subbed channel to end of the queue
    // without starting the worker. Dedupe-aware (skips already-queued
    // or currently-running channels). For when a sync is mid-pass and
    // you want another full pass to follow it, or for staging the queue
    // while Auto is off.
    btn.addEventListener("contextmenu", (e) => {
      e.preventDefault();
      const api = window.pywebview?.api;
      if (!api?.sync_enqueue_all_channels) return;
      showContextMenu(e.clientX, e.clientY, [
        { label: "Add all subbed channels to end of queue",
          action: async () => {
            try {
              const r = await api.sync_enqueue_all_channels();
              if (!r?.ok) {
                window._showToast?.(r?.error || "Enqueue failed.", "error");
                return;
              }
              const q = r.queued || 0;
              const s = r.skipped || 0;
              const msg = q > 0
                ? `Queued ${q} channel${q === 1 ? "" : "s"}`
                  + (s > 0 ? ` (${s} already queued).` : ".")
                : (s > 0
                    ? `All ${s} channels already in queue.`
                    : "No subbed channels to queue.");
              window._showToast?.(msg, q > 0 ? "ok" : "warn");
            } catch (err) {
              window._showToast?.("Error: " + err, "error");
            }
          }},
      ]);
    });

    btn.addEventListener("click", _inFlight(async () => {
      const api = window.pywebview?.api;
      if (!api) {
        window._showToast?.("Sync requires native mode (launch main.py).", "warn");
        return;
      }
      try {
        const res = await api.sync_start_all();
        if (!res.ok) {
          window._showToast?.(res.error || "Sync failed to start", "error");
          return;
        }
        // Branch on `started`: backend sets started=false when Auto is
        // off, meaning it queued channels but didn't spawn the worker.
        // Tell the user so they know the Start button in the popover
        // (or toggling Auto on) will fire the queue.
        if (res.started === false) {
          const n = typeof res.queued === "number" ? res.queued : 0;
          const head = n > 0
            ? `Queued ${n} channel${n === 1 ? "" : "s"}.`
            : "Already queued.";
          window._showToast?.(
            `${head} Start manually or enable Auto.`, "warn");
        } else {
          window._showToast?.("Sync started.", "ok");
        }
      } catch (e) {
        window._showToast?.("Error: " + e, "error");
      }
    }));

    // Big Pause button on the Download tab — GLOBAL pause/resume for
    // both the sync pipeline AND the GPU queue (transcribe / compress).
    // Decision source: _blinkState (client mirror kept in sync via backend
    // push notifications). Using client state here means the toggle always
    // matches what the user is visually seeing on the button — no async
    // roundtrip race, no "api.queue_is_paused undefined at click time"
    // falsy-short-circuit bug.
    //
    // This mirrors OLD's global pause button that gated every worker at
    // once. To target one queue independently, use the Pause button
    // inside the Sync Tasks / GPU Tasks popover.
    pauseBtn?.addEventListener("click", _inFlight(async () => {
      // Three click paths:
      // 1. Worker thread alive + not paused → pause (both queues)
      // 2. Worker thread alive + paused → resume (both queues)
      // 3. Worker thread dead + items queued + paused →
      // start a fresh sync thread. sync_start_all clears every
      // pause flag AND spawns the worker, so the restored queue
      // gets processed. Click-triggered only; never automatic —
      // matches Project rule: "launching with items in queue
      // must never auto-start" (main.py:168).
      const api = window.pywebview?.api;
      if (!api) return;
      const s = _blinkState.sync;
      const g = _blinkState.gpu;
      const syncActivelyPaused = s.running && s.paused;
      const gpuActivelyPaused = g.running && g.paused;
      const anyActivelyPaused = syncActivelyPaused || gpuActivelyPaused;
      const syncDeadPausedWithItems = !s.running && s.paused && s.count > 0;
      const gpuDeadPausedWithItems = !g.running && g.paused && g.count > 0;
      const deadPausedWithItems =
        syncDeadPausedWithItems || gpuDeadPausedWithItems;
      try {
        if (anyActivelyPaused) {
          if (typeof api.queue_resume === "function") {
            await api.queue_resume("both");
          }
          window._showToast?.("Resumed.", "ok");
        } else if (deadPausedWithItems) {
          // Clear pause flags AND start the sync thread so the queue
          // actually drains. Using sync_start_all (not queue_resume)
          // because queue_resume alone just clears flags — without a
          // live thread, the queue would stay frozen.
          // CRITICAL: pass `false` for add_downloads_from_config so
          // we DON'T enqueue a full Sync Subbed pass on resume. The
          // user's intent is "drain what's queued", not "start a
          // brand new sync of every subscribed channel".
          if (typeof api.sync_start_all === "function") {
            const res = await api.sync_start_all(false);
            if (res?.ok) {
              window._showToast?.("Resumed \u2014 starting queue.", "ok");
            } else {
              window._showToast?.(res?.error || "Resume failed.", "error");
            }
          }
        } else {
          if (typeof api.queue_pause === "function") {
            await api.queue_pause("both");
          }
          window._showToast?.(
            "Paused \u2014 current jobs finish first.", "warn");
        }
      } catch (e) { window._showToast?.("Error: " + e, "error"); }
    }));


    // Sync Tasks queue popover: Pause toggles pause/resume, Cancel cancels.
    // If the queue is paused AND there's no live sync worker thread (cold
    // launch with items restored), clicking Resume has to START the
    // worker — queue_resume alone just clears the flag and the queue
    // stays frozen. Matches the global Pause button's behavior.
    const pauseSyncBtn = document.getElementById("btn-pause-sync-queue");
    pauseSyncBtn?.addEventListener("click", async () => {
      const api = window.pywebview?.api;
      if (!api?.queue_is_paused) { api?.sync_cancel?.(); return; }
      const s = _blinkState.sync;
      const threadAlive = s.running;
      const st = await api.queue_is_paused();
      if (st?.sync) {
        if (!threadAlive && s.count > 0 && typeof api.sync_start_all === "function") {
          // before firing sync_start_all (which runs a
          // regular Sync Subbed pass), check whether the queue has
          // any redownload tasks left over from a previous run that
          // didn't drain. Those need the redownload chain worker,
          // not the sync worker \u2014 otherwise resume turns into "do a
          // full sync of every channel" and the redownload state on
          // disk is silently ignored.
          let routedRedownload = false;
          try {
            const snap = window._queueStateSnapshot?.();
            const hasRedwnl = (snap?.sync || []).some(
              t => (t?.kind || "").toLowerCase() === "redownload");
            if (hasRedwnl && api.resume_pending_redownloads) {
              const rr = await api.resume_pending_redownloads();
              if (rr?.ok && rr.resumed > 0) {
                routedRedownload = true;
                window._showToast?.(
                  `Resumed ${rr.resumed} redownload(s).`, "ok");
              }
            }
          } catch (e) {
            console.error("resume_pending_redownloads:", e);
          }
          if (!routedRedownload) {
            // Pass false so resume only drains the existing queue \u2014
            // does NOT enqueue a fresh Sync Subbed pass for every
            // subscribed channel (which is what `sync_start_all()`
            // with default `true` does).
            const res = await api.sync_start_all(false);
            window._showToast?.(res?.ok
              ? "Sync resumed \u2014 starting queue."
              : (res?.error || "Resume failed."),
              res?.ok ? "ok" : "error");
          }
        } else {
          await api.queue_resume("sync");
          window._showToast?.("Sync resumed.", "ok");
        }
      } else {
        await api.queue_pause("sync");
        window._showToast?.("Sync paused \u2014 finishing current channel.", "warn");
      }
    });
    document.getElementById("btn-cancel-sync-queue")?.addEventListener("click", async () => {
      const api = window.pywebview?.api;
      if (!api) return;
      // If the sync queue is already paused, "Pause" and "Stop now"
      // are useless options (nothing's actively running to kill or
      // pause). Show only Clear + Never mind. The opposite case
      // (queue is running) gets the full Pause / Clear / Stop now
      // trio.
      const _isPaused = !!(_blinkState && _blinkState.sync
                           && _blinkState.sync.paused);
      // Visual hierarchy on the action row (running-state):
      //   Pause = primary green (safe, fully reversible)
      //   Clear = ghost gray (intermediate — empties queue but current
      //                       job is allowed to finish)
      //   Stop now = danger red (kill subprocesses)
      // Without `kind: "ghost"` on Clear, both Pause and Clear default
      // to primary and render as two identical greens. Same visual
      // collision flagged on the paused-state dialog.
      const _buttons = _isPaused
        ? [{ label: "Clear queue", value: "clear", kind: "danger" }]
        : [
            { label: "Pause", value: "pause", kind: "primary" },
            { label: "Clear (finish current)", value: "clear", kind: "ghost" },
            { label: "Stop now", value: "stop", kind: "danger" },
          ];
      const _message = _isPaused
        ? "Queue is paused. Clear empties it; Never mind leaves it as-is so you can Resume later."
        : ("Pause: keeps the queue, resumes where it stopped.\n"
           + "Clear: empties the queue, current channel finishes "
           + "gracefully (can take minutes if mid yt-dlp fetch).\n"
           + "Stop now: kills yt-dlp/ffmpeg subprocesses immediately. "
           + "Use when Clear is taking too long.");
      const choice = await (window.askChoice
        ? askChoice({
            title: _isPaused ? "Clear the sync queue?" : "Stop the sync pass?",
            message: _message,
            buttons: _buttons,
            cancel: "Never mind",
            cancelPlacement: "right",
          })
        : Promise.resolve(confirm("Clear the sync queue?") ? "clear" : null));
      if (choice === "clear") {
        const res = await api.sync_clear_queue?.();
        const n = res?.removed || 0;
        window._showToast?.(
          n > 0 ? `Sync queue cleared (${n} pending).`
                : "Sync cancel requested.", "warn");
      } else if (choice === "stop") {
        const res = await api.sync_force_stop?.();
        const n = res?.removed || 0;
        const k = res?.killed || 0;
        window._showToast?.(
          `Stopped — cleared ${n} queued, killed ${k} subprocess(es).`,
          "warn");
      } else if (choice === "pause") {
        await api.queue_pause?.("sync");
        window._showToast?.("Sync paused \u2014 finishing current channel.", "warn");
      }
      // null → Cancel → no-op (dialog closed)
    });

    // GPU Tasks queue popover — mirror the Sync handlers.
    document.getElementById("btn-pause-gpu-queue")?.addEventListener("click", async () => {
      const api = window.pywebview?.api;
      if (!api?.queue_is_paused) { api?.transcribe_cancel_all?.(); return; }
      const st = await api.queue_is_paused();
      if (st?.gpu) {
        await api.queue_resume("gpu");
        window._showToast?.("GPU queue resumed.", "ok");
      } else {
        await api.queue_pause("gpu");
        window._showToast?.("GPU queue paused \u2014 current job will finish.", "warn");
      }
    });
    document.getElementById("btn-cancel-gpu-queue")?.addEventListener("click", async () => {
      const api = window.pywebview?.api;
      if (!api) return;
      const choice = await (window.askChoice
        ? askChoice({
            title: "Stop the GPU queue?",
            message: "Pause keeps the queue. Clear Queue empties it and " +
                     "cancels the current job.",
            buttons: [
              { label: "Pause", value: "pause", kind: "primary" },
              { label: "Clear Queue", value: "clear", kind: "danger" },
            ],
            cancel: "Cancel",
            cancelPlacement: "right",
          })
        : Promise.resolve(confirm("Clear the GPU queue?") ? "clear" : null));
      if (choice === "clear") {
        const res = await api.gpu_clear_queue?.();
        const n = res?.removed || 0;
        window._showToast?.(
          n > 0 ? `GPU queue cleared (${n} pending).`
                : "GPU queue cleared.", "warn");
      } else if (choice === "pause") {
        await api.queue_pause?.("gpu");
        window._showToast?.("GPU queue paused \u2014 current job will finish.", "warn");
      }
    });
  }

  window.initSyncButton = initSyncButton;
})();
