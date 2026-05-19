// queueBlink.js — Patch 14 phase 1 (v69.5)
//
// Queue button blink animation, paint state, and pause-button paint
// helpers. Extracted from app.js (was at lines 8285-8671).
//
// Self-contained IIFE — publishes the symbols app.js needs onto window:
//   window._blinkState           (read+write from app.js outside this module)
//   window.setQueueState         (assigned by initQueueBlink; backend pushes)
//   window._syncPauseButtonState (called by setQueueState)
//   window._paintBlinkState      (logs.js uses to repaint after queue change)
//   window.initQueueBlink        (called by app.js boot at L10443)
//
// The IIFE has its OWN local lexical scope for everything else
// (_PAUSE_PENDING_MIN_MS, _blinkDom, _bdom, _isPipelinePending,
//  _paintPopoverPauseBtn, _setPopoverFooterBtnsVisible, blinkTick,
//  pendingBlinkTick, ensureBlinkRunning, ensurePendingBlinkRunning,
//  paintBlinkState).
(() => {
  "use strict";

  // ─── Queue button blink animation ────────────────────────────────────
  //
  // Matches _blink_tick at YTArchiver.py:21049 — unified 700ms clock so
  // Sync + GPU buttons blink in phase. Running = blink, Paused = solid,
  // Idle = default bg.
  const _blinkState = {
    clockOn: false,
    timer: null,
    pendingTimer: null,  // separate clock for pause-pending blink
    pendingClockOn: false,
    // `count` = items queued for this pipeline. Used by
    // _syncPauseButtonState so the global Pause button enables when
    // items are queued-but-paused-without-a-live-thread (the state after
    // a cold launch when QueueState.load() restored persisted items).
    // `pausedActive` = true when the worker has actually entered its
    // pause-wait block. Distinct from `paused` (= request received).
    // When paused && !pausedActive, the Resume button BLINKS to show
    // the pause is queued but not yet effective (e.g. an in-flight
    // metadata refresh has to finish its current re-fetch loop before
    // pause takes hold — could be minutes).
    sync: { running: false, paused: false, pausedActive: false, count: 0,
            pausedAtMs: 0 },
    gpu: { running: false, paused: false, pausedActive: false, count: 0,
           pausedAtMs: 0 },
  };
  // Minimum visible duration of the pause-pending blink (ms). Without
  // this, a fast pause-handshake (worker hits its pause-wait within
  // 50ms of the click) would flip pausedActive=true so quickly the
  // user couldn't see the blink — they'd just see the button go from
  // "pause" straight to "resume" with no visible "queued" indication.
  // 1500ms = ~3 cycles of the 1s blink animation, enough to register.
  const _PAUSE_PENDING_MIN_MS = 1500;
  // Expose for logs.js's renderQueues so it can mirror queue counts
  // into _blinkState the moment a fresh payload arrives.
  window._blinkState = _blinkState;
  window._syncPauseButtonState = _syncPauseButtonState;
  window._paintBlinkState = paintBlinkState;

  function initQueueBlink() {
    // Backend drives blink state via:
    // window.setQueueState({ sync: {running, paused, pausedActive}, gpu: {...} })
    window.setQueueState = (state) => {
      // Capture the moment paused goes from false → true so we can
      // hold the "pending" blink for a minimum visible window even
      // if the backend flips pausedActive=true within 50ms (which it
      // will if yt-dlp is actively streaming output and the pause
      // check fires on the next line).
      if (state.sync) {
        if (state.sync.paused && !_blinkState.sync.paused) {
          _blinkState.sync.pausedAtMs = Date.now();
        } else if (!state.sync.paused) {
          _blinkState.sync.pausedAtMs = 0;
        }
        Object.assign(_blinkState.sync, state.sync);
      }
      if (state.gpu) {
        if (state.gpu.paused && !_blinkState.gpu.paused) {
          _blinkState.gpu.pausedAtMs = Date.now();
        } else if (!state.gpu.paused) {
          _blinkState.gpu.pausedAtMs = 0;
        }
        Object.assign(_blinkState.gpu, state.gpu);
      }
      ensureBlinkRunning();
      ensurePendingBlinkRunning();
      paintBlinkState();
    };
    // Paint initial idle state (buttons default to gray)
    paintBlinkState();
    // NOTE: auto-start for preview-only mode was removed — it was firing in
    // native mode too because pywebview.api isn't ready at boot time.
    // Pause button: disabled when nothing running
    _syncPauseButtonState();
    // Re-check whenever state is pushed
    const origSet = window.setQueueState;
    window.setQueueState = (state) => {
      origSet(state);
      _syncPauseButtonState();
    };
    // Wrap renderQueues so queue counts AND paused flags are mirrored
    // into _blinkState every payload. Critical for cold launch: the
    // backend only pushes setQueueState via _on_queue_changed (i.e.
    // when state CHANGES after the window is ready) — it never emits
    // a baseline after set_window. So the frontend's first view of
    // paused-at-launch comes from the api.get_queues() payload, which
    // DOES carry sync_paused / gpu_paused. Without mirroring those
    // flags here, _blinkState.paused stays false at boot, the global
    // Pause button's "paused + items queued" branch never fires, and
    // the button sits disabled with 99+ items waiting.
    const origRenderQueues = window.renderQueues;
    if (typeof origRenderQueues === "function") {
      window.renderQueues = (queues) => {
        origRenderQueues(queues);
        _blinkState.sync.count = (queues?.sync || []).length;
        _blinkState.gpu.count = (queues?.gpu || []).length;
        // Paused flags may be missing on partial payloads (e.g. a
        // legacy caller that only passes {sync, gpu}) — only overwrite
        // when the key is present so we don't reset the state that
        // setQueueState already established.
        if (queues && "sync_paused" in queues) {
          _blinkState.sync.paused = !!queues.sync_paused;
        }
        if (queues && "gpu_paused" in queues) {
          _blinkState.gpu.paused = !!queues.gpu_paused;
        }
        // Mirror the new pause-active flags (worker has entered its
        // wait block). Default to true when missing so an older backend
        // payload doesn't accidentally make the button blink forever.
        if (queues && "sync_paused_active" in queues) {
          _blinkState.sync.pausedActive = !!queues.sync_paused_active;
        } else if (queues) {
          _blinkState.sync.pausedActive = !!queues.sync_paused;
        }
        if (queues && "gpu_paused_active" in queues) {
          _blinkState.gpu.pausedActive = !!queues.gpu_paused_active;
        } else if (queues) {
          _blinkState.gpu.pausedActive = !!queues.gpu_paused;
        }
        _syncPauseButtonState();
        ensurePendingBlinkRunning();
        paintBlinkState();
      };
    }
  }

  function _syncPauseButtonState() {
    // Three states:
    // 1. Worker thread alive (running OR parked in _wait_if_paused) →
    // enabled. Click pauses/resumes an in-progress pass.
    // 2. Thread dead BUT items are queued AND queue is paused →
    // enabled. Click starts a new sync thread that will process
    // the restored queue. This covers the cold-launch case where
    // the user quit mid-pass and reopens with 99+ items pending.
    // 3. Thread dead AND (no items OR not paused) → disabled.
    // User clicks Sync Subbed to start fresh.
    const s = _blinkState.sync;
    const g = _blinkState.gpu;
    const anyAlive = s.running || g.running;
    const pausedWithItems =
      (s.paused && s.count > 0) || (g.paused && g.count > 0);
    const enable = anyAlive || pausedWithItems;
    const btn = document.getElementById("btn-pause");
    if (btn) {
      btn.disabled = !enable;
      btn.classList.toggle("disabled", !enable);
    }
  }

  function ensureBlinkRunning() {
    const anyActive = _blinkState.sync.running || _blinkState.gpu.running;
    if (anyActive && !_blinkState.timer) {
      _blinkState.clockOn = false;
      _blinkState.timer = setInterval(blinkTick, 700);
    } else if (!anyActive && _blinkState.timer) {
      clearInterval(_blinkState.timer);
      _blinkState.timer = null;
      _blinkState.clockOn = false;
      paintBlinkState(); // final solid paint
    }
  }

  function blinkTick() {
    _blinkState.clockOn = !_blinkState.clockOn;
    paintBlinkState();
  }

  // Returns true if THIS pipeline is currently in the pause-pending
  // visual state. Centralized so paint code + timer code agree.
  // "Pending" = paused requested AND ( worker hasn't entered its
  // pause-wait yet OR we're still inside the minimum visible window
  // since the pause click ).
  function _isPipelinePending(s) {
    if (!s.paused || !s.running) return false;
    if (!s.pausedActive) return true;
    if (s.pausedAtMs && (Date.now() - s.pausedAtMs) < _PAUSE_PENDING_MIN_MS) {
      return true;
    }
    return false;
  }

  // Separate clock for the pause-pending blink (Resume button while
  // the worker is still finishing its current operation). Faster
  // cadence (500ms) so it reads as "in progress, hold on" rather
  // than ambient state. Stops the moment pause becomes active AND
  // the minimum visible window has passed.
  function ensurePendingBlinkRunning() {
    const anyPending = _isPipelinePending(_blinkState.sync)
                        || _isPipelinePending(_blinkState.gpu);
    if (anyPending && !_blinkState.pendingTimer) {
      _blinkState.pendingClockOn = false;
      _blinkState.pendingTimer = setInterval(pendingBlinkTick, 500);
    } else if (!anyPending && _blinkState.pendingTimer) {
      clearInterval(_blinkState.pendingTimer);
      _blinkState.pendingTimer = null;
      _blinkState.pendingClockOn = false;
      paintBlinkState();
    }
  }

  function pendingBlinkTick() {
    _blinkState.pendingClockOn = !_blinkState.pendingClockOn;
    // Re-check on each tick: the minimum-window timer might have
    // expired since the timer started. paintBlinkState reads the
    // same _isPipelinePending so the visual stays in sync.
    ensurePendingBlinkRunning();
    paintBlinkState();
  }

  // paintBlinkState fires every ~700ms during active
  // sync. Cache DOM refs once on first call instead of re-doing 4+
  // getElementById lookups per tick. The cache is invalidated if any
  // ref's `isConnected` is false (e.g. tab DOM was torn down).
  const _blinkDom = {};
  function _bdom(id) {
    let el = _blinkDom[id];
    if (el && el.isConnected) return el;
    el = document.getElementById(id);
    _blinkDom[id] = el;
    return el;
  }

  function paintBlinkState() {
    // Three visual states, per user rule:
    // idle → grey (nothing running, nothing paused mid-pass)
    // running → green/red blink at clockOn cadence
    // paused → solid green/red when the worker thread is alive AND
    // paused. "Running" in _blinkState reflects "worker
    // thread is alive" (set by main.py _on_queue_changed),
    // so it stays true during pause-between-channels —
    // which means this cleanly distinguishes "actively
    // paused mid-pass" (solid) from "persisted paused flag
    // at idle" (grey).
    const syncBtn = _bdom("btn-sync-tasks");
    const gpuBtn = _bdom("btn-gpu-tasks");
    // Use a separate `pause-pending` state when the user has clicked
    // pause but the worker hasn't reached its wait block yet (e.g.
    // metadata refresh's long re-fetch loop has to finish first).
    // Visually a slow blink between paused + on. Stops the moment
    // pausedActive flips true.
    if (syncBtn) {
      const s = _blinkState.sync;
      let state = "idle";
      if (_isPipelinePending(s)) {
        state = _blinkState.pendingClockOn ? "paused" : "on";
      } else if (s.running && s.paused) {
        state = "paused";
      } else if (s.running) {
        state = _blinkState.clockOn ? "on" : "off";
      }
      syncBtn.dataset.blinkState = state;
    }
    if (gpuBtn) {
      const g = _blinkState.gpu;
      let state = "idle";
      if (_isPipelinePending(g)) {
        state = _blinkState.pendingClockOn ? "paused" : "on";
      } else if (g.running && g.paused) {
        state = "paused";
      } else if (g.running) {
        state = _blinkState.clockOn ? "on" : "off";
      }
      gpuBtn.dataset.blinkState = state;
    }
    // Global Pause button (Download tab). Three visual states:
    // running (ghost, pause icon) — a worker thread is alive,
    // nothing paused. Click pauses.
    // paused (solid, resume icon) — worker alive AND paused,
    // OR worker dead AND queue has items AND queue paused
    // (persisted-paused-at-launch). Click resumes — if the
    // thread is dead, the click handler starts a fresh one
    // that processes the restored queue.
    // disabled is set by _syncPauseButtonState when nothing to do.
    const pauseBtn = _bdom("btn-pause");
    if (pauseBtn) {
      const s = _blinkState.sync;
      const g = _blinkState.gpu;
      const syncActive = s.running && s.paused;
      const gpuActive = g.running && g.paused;
      const syncPausedWithItems = !s.running && s.paused && s.count > 0;
      const gpuPausedWithItems = !g.running && g.paused && g.count > 0;
      const anyPaused = syncActive || gpuActive ||
                        syncPausedWithItems || gpuPausedWithItems;
      // Pause-pending: the user clicked Pause but the worker is still
      // mid-operation (e.g. metadata refresh's long re-fetch loop).
      // Surface a third visual state so the click is acknowledged
      // without lying about whether the pause is actually in effect.
      // Also held for a minimum visible window after the click so a
      // fast pause-handshake doesn't skip the blink entirely.
      const anyPending = _isPipelinePending(s) || _isPipelinePending(g);
      let visState = anyPaused ? "paused" : "running";
      if (anyPending) visState = "pending";
      pauseBtn.dataset.pauseState = visState;
      pauseBtn.classList.toggle("pause-pending", anyPending);
      // Write to data-tooltip (not title) — the 700ms blink tick was
      // re-adding `title` mid-hover, after the custom tooltip system
      // had already migrated it. Both ended up visible at once. Bypass
      // the migration step by setting data-tooltip directly here.
      const _pauseTip = anyPending
        ? "Pause queued — current job will finish first. Click to cancel pause."
        : (anyPaused
            ? "Resume all queues"
            : "Pause all queues (current jobs finish first)");
      pauseBtn.setAttribute("data-tooltip", _pauseTip);
      pauseBtn.removeAttribute("title");
      const svg = pauseBtn.querySelector("svg");
      if (svg) {
        const want = anyPaused ? "play" : "bars";
        if (svg.dataset.icon !== want) {
          svg.dataset.icon = want;
          svg.innerHTML = anyPaused
            ? '<path d="M4 3v10l9-5z"/>'
            : '<rect x="4" y="3" width="3" height="10" rx="0.5"/>' +
              '<rect x="9" y="3" width="3" height="10" rx="0.5"/>';
        }
      }
    }
    // Sync Tasks + GPU Tasks popover footer buttons — flip Pause ↔ Resume
    // whenever the corresponding pipeline is paused. "Paused" covers both
    // the thread-alive-but-paused case AND the cold-launch case where
    // items are queued but no thread is running yet; clicking Resume in
    // the latter case should start a fresh worker (handled in the click
    // handlers below). Also hide the Pause/Cancel pair entirely when the
    // queue is empty — nothing to act on means no buttons.
    _paintPopoverPauseBtn("btn-pause-sync-queue",
                           _blinkState.sync, "Sync");
    _paintPopoverPauseBtn("btn-pause-gpu-queue",
                           _blinkState.gpu, "GPU");
    // Sync popover: no sibling buttons in the footer — hide the entire
    // footer when there's nothing to act on so we don't show an empty
    // bar with just the border-top.
    const syncFooter = _bdom("sync-tasks-footer");
    if (syncFooter) {
      syncFooter.style.display = _blinkState.sync.count > 0 ? "" : "none";
    }
    // GPU popover: footer also hosts the Manual Transcribe folder icon,
    // which should stay visible. Only hide the Pause/Cancel pair.
    _setPopoverFooterBtnsVisible("btn-pause-gpu-queue",
                                  "btn-cancel-gpu-queue",
                                  _blinkState.gpu.count > 0);
  }

  // Show/hide a popover's Pause + Cancel button pair. When the queue is
  // empty the pair goes `display: none` so the popover footer collapses
  // around whatever sibling buttons remain (e.g. Manual Transcribe).
  function _setPopoverFooterBtnsVisible(pauseBtnId, cancelBtnId, visible) {
    const pb = document.getElementById(pauseBtnId);
    const cb = document.getElementById(cancelBtnId);
    if (pb) pb.style.display = visible ? "" : "none";
    if (cb) cb.style.display = visible ? "" : "none";
  }

  // Helper: paint one popover-footer pause button based on a pipeline's
  // blink state. Called by paintBlinkState for both Sync and GPU
  // popovers. Flips the label, title, and SVG icon.
  function _paintPopoverPauseBtn(btnId, state, label) {
    const btn = document.getElementById(btnId);
    if (!btn) return;
    const activelyPaused = state.running && state.paused;
    const deadPausedWithItems = !state.running && state.paused && state.count > 0;
    const isPaused = activelyPaused || deadPausedWithItems;
    // Pause-pending: requested but worker still finishing current op.
    // Also held for a minimum window after the click so a fast pause
    // handshake doesn't skip the visual feedback.
    const isPending = _isPipelinePending(state);
    const svg = btn.querySelector("svg");
    const span = btn.querySelector("span");
    if (span) span.textContent = isPaused ? "Resume" : "Pause";
    btn.title = isPending
      ? `Pause queued — ${label.toLowerCase()} job finishing first. Click to cancel pause.`
      : (isPaused
          ? `Resume ${label.toLowerCase()} queue`
          : `Pause ${label.toLowerCase()} queue (current job finishes first)`);
    btn.dataset.pauseState = isPending
      ? "pending"
      : (isPaused ? "paused" : "running");
    btn.classList.toggle("pause-pending", isPending);
    if (svg) {
      const want = isPaused ? "play" : "bars";
      if (svg.dataset.icon !== want) {
        svg.dataset.icon = want;
        svg.innerHTML = isPaused
          ? '<path d="M3 2v10l8-5z"/>'
          : '<rect x="3" y="2" width="3" height="10" rx="0.5"/>' +
            '<rect x="8" y="2" width="3" height="10" rx="0.5"/>';
      }
    }
  }

  // Publish the public surface app.js uses by name.
  window.initQueueBlink = initQueueBlink;
  // _blinkState, _syncPauseButtonState, _paintBlinkState are already
  // published from inside the moved code via direct assignment.
})();
