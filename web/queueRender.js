/* ═══════════════════════════════════════════════════════════════════════
   queueRender.js — Sync Tasks + GPU Tasks popover row renderer

   Extracted from logs.js (~374 lines). Owns the actual rendering of
   queue task rows inside the two popovers:
     • renderQueues — top-level dispatcher (called from Python)
     • renderTaskList / paintTaskList — per-row build
     • _queueState — in-memory mirror so drag-reorder works without
       waiting on the backend roundtrip
     • Drag-and-drop reorder (HTML5 dragstart/dragover/drop)
     • Right-click menu (Skip / Move-to-top / Cancel-or-Remove)
     • Verb-color tagging (Downloading=green, Transcribing=blue, etc.)
     • Per-row "×" close button → removes from backend queue

   Note: this file renders ROWS inside the popovers. The popovers
   themselves (open/close behavior, anchor positioning) live in
   queuePopovers.js and were already extracted.

   Publishes:
     window.renderQueues              — called by Python backend
     window._queueStateSnapshot       — read by Subs context menu
     window._anySyncRunning           — read by Subs context menu
     window._queueHasSyncForChannel   — read by Subs context menu
     window._queueHasGpuForChannel    — read by Subs context menu

   Reads:
     window._escapeHtml               — from util.js
     window.askConfirm                — confirm dialogs from modals.js
     window.showContextMenu           — right-click menu from contextMenu.js
     window._showToast                — toasts.js
     window.pywebview.api             — Python bridge
   ═══════════════════════════════════════════════════════════════════════ */

(function () {
  "use strict";

  const escapeHtml = window._escapeHtml || ((s) => String(s ?? ""));

  /** Render the queue popovers for Sync Tasks + GPU Tasks. */
  window.renderQueues = function (queues) {
    renderTaskList("sync-tasks-body", queues.sync, "No sync tasks queued.", "sync");
    renderTaskList("gpu-tasks-body", queues.gpu, "No processing tasks queued.", "gpu");
    _updateBadge("badge-sync", (queues.sync || []).length);
    _updateBadge("badge-gpu", (queues.gpu || []).length);
  };

  function _updateBadge(id, n) {
    const el = document.getElementById(id);
    if (!el) return;
    if (!n || n <= 0) {
      el.hidden = true;
      el.textContent = "0";
      return;
    }
    el.hidden = false;
    el.textContent = n > 99 ? "99+" : String(n);
  }

  // In-memory queue state so drag-to-rearrange can update order.
  const _queueState = { sync: [], gpu: [] };
  // Exposed so context menus elsewhere (Subs tab) can check whether a
  // channel is currently queued / running and label menu items dynamically.
  // Mirrors OLD's dynamic-label mutation (YTArchiver.py:5596 _chan_ctx_menu).
  window._queueStateSnapshot = () => ({
    sync: _queueState.sync.slice(),
    gpu: _queueState.gpu.slice(),
  });
  // Issue #155 helper: is ANY sync task currently running on the worker?
  // Used by the Subs context menu to decide between "Sync now" and
  // "Add to Sync queue" labels.
  window._anySyncRunning = () => {
    for (const t of _queueState.sync) {
      if ((t?.status || "") === "running") return true;
    }
    return false;
  };
  // Convenience: does `channelName` have a sync queued? (running or queued)
  window._queueHasSyncForChannel = (channelName) => {
    const n = (channelName || "").toLowerCase();
    if (!n) return null;
    const match = (t) => {
      const s = String(t?.name || t?.url || "").toLowerCase();
      return s.includes(n) ? t.status || "queued" : null;
    };
    for (const t of _queueState.sync) {
      const s = match(t);
      if (s) return s; // "running" | "queued"
    }
    return null;
  };
  // Convenience: does a GPU task (transcribe/encode/compress) reference this channel?
  window._queueHasGpuForChannel = (channelName) => {
    const n = (channelName || "").toLowerCase();
    if (!n) return null;
    for (const t of _queueState.gpu) {
      const s = String(t?.name || t?.title || "").toLowerCase();
      if (s.includes(n)) return t.status || "queued";
    }
    return null;
  };

  function renderTaskList(bodyId, list, emptyText, queueKind) {
    const body = document.getElementById(bodyId);
    if (!body) return;
    _queueState[queueKind] = (list || []).slice();
    paintTaskList(body, _queueState[queueKind], emptyText, queueKind);
  }

  function paintTaskList(body, list, emptyText, queueKind) {
    body.innerHTML = "";
    if (!list || list.length === 0) {
      body.innerHTML = `<div class="queue-empty">${emptyText}</div>`;
      return;
    }
    list.forEach((t, i) => {
      const row = document.createElement("div");
      const statusCls = t.status || "queued";
      row.className = `queue-task-row ${statusCls}`;
      row.draggable = true;
      row.dataset.idx = i;
      row.dataset.queue = queueKind;

      const stateGlyph =
        statusCls === "running" ? "▶" :
        statusCls === "paused" ? "❚❚" :
                                  "○";

      // Color the verb (Downloading/Transcribing/Metadata) in tag color
      const nameHtml = colorizeTaskName(t.name || t.title || "");

      // Cycling dots after the active task's name ("..."/".. "/". ") —
      // pure CSS animation via ::after content keyframes. Matches
      // YTArchiver.py:20131 _active_label cycling dots.
      const dotsSpan = statusCls === "running" ? '<span class="queue-task-dots"></span>' : "";

      // X button hidden for the running row — that item lives in
      // current_sync / current_gpu, NOT in queues.sync / queues.gpu.
      // An index-based delete on the running row would silently drop
      // the next-queued item (the one that visually slid up to slot 0
      // after the running row's translation). For running rows the
      // user should use the right-click context menu's Skip / Cancel
      // actions instead.
      const closeBtnHtml = statusCls === "running"
        ? ""
        : '<button class="queue-task-close" title="Remove">&times;</button>';

      row.innerHTML = `
        <span class="queue-task-index">${i + 1}.</span>
        <span class="queue-task-state ${statusCls}">${stateGlyph}</span>
        <span class="queue-task-name"></span>${dotsSpan}
        ${closeBtnHtml}
      `;
      row.querySelector(".queue-task-name").innerHTML = nameHtml;

      row.querySelector(".queue-task-close")?.addEventListener("click", (e) => {
        e.stopPropagation();
        const popoverIdx = Number(row.dataset.idx);
        const removed = _queueState[queueKind][popoverIdx];
        // The X is a per-ROW action. Translate popover index ->
        // backend queue index (the popover prepends current_sync /
        // current_gpu as the running row, so any 'running' rows
        // before our position need to be subtracted off — that
        // position doesn't exist in the backend's queues list).
        let runningBefore = 0;
        for (let j = 0; j < popoverIdx; j++) {
          if ((_queueState[queueKind][j] || {}).status === "running") {
            runningBefore++;
          }
        }
        const queueIdx = popoverIdx - runningBefore;
        _queueState[queueKind].splice(popoverIdx, 1);
        paintTaskList(body, _queueState[queueKind], emptyText, queueKind);
        // Original code passed only a URL / path, which deleted EVERY
        // queue entry sharing that identifier (e.g. one X click on a
        // metadata-refresh row also dropped the download row for the
        // same channel because both shared the channel URL).
        // Fix: prefer the index-based remove API (queues_*_remove_at)
        // with identity guard. Falls back to the legacy URL-based API
        // only on backends that don't expose the new method.
        if (!window.pywebview?.api || !removed) return;
        const api = window.pywebview.api;
        if (queueKind === "sync") {
          if (api.queues_sync_remove_at) {
            api.queues_sync_remove_at(queueIdx,
              removed.url || "",
              removed.channel_name || removed.name || "");
          } else if (api.queues_sync_remove) {
            api.queues_sync_remove(removed.url || removed.channel_name
                                    || removed.name || "");
          }
        } else if (queueKind === "gpu") {
          // Coalesced "Transcribe {ch} (N videos)" row → bulk-remove
          // (drop all siblings). Single rows use index-based API.
          const isBulk = !!removed.bulk_id && (removed.bulk_count || 0) > 1;
          if (isBulk && api.queues_gpu_remove_bulk) {
            api.queues_gpu_remove_bulk(removed.bulk_id);
          } else if (api.queues_gpu_remove_at) {
            api.queues_gpu_remove_at(queueIdx,
              removed.path || "",
              removed.bulk_id || "");
          } else if (api.queues_gpu_remove) {
            api.queues_gpu_remove(removed.path || removed.bulk_id
                                   || removed.id || removed.name || "");
          }
        }
      });

      // Right-click menu on queue rows: skip / move-to-top / cancel-or-remove
      // Mirrors YTArchiver.py:20570-20584 (sync) + 21441-21455 (gpu) — each
      // destructive action pops a confirm, matching the old app's askyesno flow.
      row.addEventListener("contextmenu", (ev) => {
        ev.preventDefault();
        ev.stopPropagation();
        // Resolve index FRESH from the live _queueState by matching
        // the row's identity. row.dataset.idx is stale after a drag
        // reorder (set at render time, not updated until next paint)
        // so trusting it could cancel the wrong task (audit:
        // queueRender H205).
        let idx = Number(row.dataset.idx);
        try {
          const arr = _queueState[queueKind] || [];
          const _myUrl = t.url || "";
          const _myPath = t.path || "";
          const _myName = t.name || t.title || "";
          const _matched = arr.findIndex(x => x && (
            (x.url && _myUrl && x.url === _myUrl)
            || (x.path && _myPath && x.path === _myPath)
            || (x.name && _myName && x.name === _myName)));
          if (_matched >= 0) idx = _matched;
        } catch {}
        const api = window.pywebview?.api;
        const items = [];
        const taskLabel = (t.name || t.title || t.url || "this task").toString().slice(0, 60);
        // "Skip this job" — only meaningful for the currently-running
        // item. Semantics: send the running task to the END of the queue
        // and let the next queued item run. The deferred task isn't lost —
        // it gets a fresh attempt after everything else finishes.
        // Different from "Cancel task" (which drops it).
        if (statusCls === "running") {
          items.push({ label: "Skip this job",
            action: async () => {
              const ok = await (window.askConfirm
                ? window.askConfirm("Skip this job",
                    `Send "${taskLabel}" to the end of the queue and move on to the next job?`,
                    { confirm: "Skip", danger: false })
                : Promise.resolve(confirm(
                    `Send "${taskLabel}" to the end of the queue and move on?`)));
              if (!ok) return;
              if (queueKind === "sync") api?.sync_defer_current?.();
              else api?.gpu_defer_current?.();
            }});
        }
        // "Move to top" — only offered when there's something above the
        // task to overtake. Showing it on idx === 0 (running task or already-
        // first queued task) was confusing because the click silently did
        // nothing.
        if (idx > 0) {
          items.push(
            { label: "Move to top",
              action: () => {
                const [taken] = _queueState[queueKind].splice(idx, 1);
                _queueState[queueKind].unshift(taken);
                paintTaskList(body, _queueState[queueKind], emptyText, queueKind);
                if (queueKind === "sync" && api?.queues_sync_reorder)
                  api.queues_sync_reorder(taken?.url || taken?.name || "", 0);
                else if (queueKind === "gpu" && api?.queues_gpu_reorder)
                  api.queues_gpu_reorder(taken?.id || taken?.path || taken?.name || "", 0);
              }},
          );
        }
        // "Cancel task" (running) drops the in-flight job entirely so the
        // next queued item runs. The running row hides its X close button
        // by design (clicking it would silently drop the wrong queue entry
        // because the running item lives in current_sync, not in the queue
        // lists), so we route the cancel through the same skip_current API
        // that "Skip" uses but WITHOUT a re-enqueue.
        // "Remove from queue" (non-running) is a standard row delete via
        // the existing X-button click handler.
        items.push(
          { label: statusCls === "running" ? "Cancel task" : "Remove from queue",
            cls: "danger",
            action: async () => {
              const title = statusCls === "running" ? "Cancel task" : "Remove from queue";
              const msg = statusCls === "running"
                ? `Cancel "${taskLabel}" and remove it from the queue?\n\nThe current job will stop and won't run again unless re-queued.`
                : `Remove "${taskLabel}" from the queue?`;
              const ok = await (window.askConfirm
                ? window.askConfirm(title, msg, { confirm: title, danger: true })
                : Promise.resolve(confirm(msg)));
              if (!ok) return;
              if (statusCls === "running") {
                if (queueKind === "sync") api?.sync_skip_current?.();
                else api?.gpu_skip_current?.();
              } else {
                row.querySelector(".queue-task-close")?.click();
              }
            }},
        );
        if (window.showContextMenu) window.showContextMenu(ev.clientX, ev.clientY, items);
      });

      // Drag-and-drop (HTML5).
      // U-1: encode source-queue identity into dataTransfer so a drop
      //      across queues (Sync row dropped on GPU popover, etc.) can
      //      be rejected. Previously stored just the index — drop on the
      //      other queue would splice _queueState[wrong_queue] using
      //      the source's index = state corruption.
      // U-2: notify backend of the reorder. Without this the next push
      //      from main.py snaps the rows back to old order.
      row.addEventListener("dragstart", (e) => {
        row.classList.add("drag-src");
        e.dataTransfer.effectAllowed = "move";
        e.dataTransfer.setData("text/plain",
          JSON.stringify({ queueKind: queueKind, idx: i }));
      });
      row.addEventListener("dragend", () => {
        row.classList.remove("drag-src");
        body.querySelectorAll(".drag-target-above, .drag-target-below")
            .forEach(el => el.classList.remove("drag-target-above", "drag-target-below"));
      });
      row.addEventListener("dragover", (e) => {
        // Refuse drop visualization for cross-queue drags so the user
        // gets no false "you can drop here" feedback.
        let srcKind = queueKind;
        try {
          const raw = e.dataTransfer.types.includes("text/plain")
            ? null  // dragover doesn't expose the data — fall back to
                    // assuming same-queue and validate at drop time
            : null;
        } catch {}
        e.preventDefault();
        const rect = row.getBoundingClientRect();
        const halfway = rect.top + rect.height / 2;
        row.classList.toggle("drag-target-above", e.clientY < halfway);
        row.classList.toggle("drag-target-below", e.clientY >= halfway);
      });
      row.addEventListener("dragleave", () => {
        row.classList.remove("drag-target-above", "drag-target-below");
      });
      row.addEventListener("drop", (e) => {
        e.preventDefault();
        // Parse the source identity. Refuse cross-queue drops — the
        // dragged item belongs to a different queue's _queueState
        // and a different backend reorder API. Splicing across queues
        // would corrupt state (U-1).
        let parsed;
        try { parsed = JSON.parse(e.dataTransfer.getData("text/plain")); }
        catch { parsed = null; }
        // Back-compat: legacy payload was a bare index string. If parse
        // fails, treat as same-queue drop (matches old behavior).
        const srcKind = (parsed && parsed.queueKind) || queueKind;
        const srcIdx = parsed && Number.isFinite(parsed.idx)
          ? parsed.idx
          : Number(e.dataTransfer.getData("text/plain"));
        if (srcKind !== queueKind) {
          // Cross-queue drop: no-op. Show a brief toast so the user
          // knows the drag was registered but rejected on purpose.
          window._showToast?.(
            "Can't drag tasks between Sync and Processing queues.", "warn");
          return;
        }
        const dstIdx = Number(row.dataset.idx);
        if (Number.isNaN(srcIdx) || srcIdx === dstIdx) return;
        const rect = row.getBoundingClientRect();
        const below = e.clientY >= rect.top + rect.height / 2;
        const list = _queueState[queueKind];
        const [moved] = list.splice(srcIdx, 1);
        let insertAt = dstIdx;
        if (srcIdx < dstIdx) insertAt -= 1;
        if (below) insertAt += 1;
        list.splice(insertAt, 0, moved);
        paintTaskList(body, list, emptyText, queueKind);
        // U-2: notify the backend so the reorder actually persists.
        // Mirrors the right-click "Move to top" handler, which already
        // calls queues_*_reorder. Without this, the next backend push
        // snaps the rows back to the old order — the drag looked like
        // it took effect for one frame, then visually undid itself.
        const api = window.pywebview?.api;
        if (api && moved) {
          if (queueKind === "sync" && api.queues_sync_reorder) {
            api.queues_sync_reorder(
              moved.url || moved.channel_name || moved.name || "",
              insertAt);
          } else if (queueKind === "gpu" && api.queues_gpu_reorder) {
            api.queues_gpu_reorder(
              moved.path || moved.bulk_id || moved.id || moved.name || "",
              insertAt);
          }
        }
      });

      body.appendChild(row);
    });
  }

  function colorizeTaskName(name) {
    name = name || "";   // GPU tasks may carry `title` but no `name`; never deref undefined
    // Color the action verb in its tag color — mirrors YTArchiver's
    // log palette so Downloading=green, Metadata=pink, Transcribing=blue,
    // Redownloading=chartreuse, Encoding/Compressing=purple, Moving/Reorg=orange.
    // (Hex values live in styles.css as var(--c-log-*).)
    // Both present-continuous (running) and plain-verb (queued) forms.
    // Longer verbs listed first so "Redownloading" isn't matched by "Download".
    const verbs = [
      ["Redownloading", "qv-redwnl"], // chartreuse #c7e64f
      ["Redownload", "qv-redwnl"],
      ["Downloading", "qv-sync"], // green #3dd68c
      ["Download", "qv-sync"],
      ["Transcribing", "qv-trans"], // blue #6cb4ee
      ["Transcribe", "qv-trans"],
      ["Metadata", "qv-meta"], // pink #e87aac
      ["Compressing", "qv-compress"], // purple #c084fc
      ["Compress", "qv-compress"],
      ["Encoding", "qv-compress"],
      ["Encode", "qv-compress"],
      ["Moving", "qv-reorg"], // orange #ff8c42
      ["Reorg", "qv-reorg"],
      ["Syncing", "qv-sync"],
      ["Sync", "qv-sync"],
    ];
    for (const [verb, cls] of verbs) {
      if (name.startsWith(verb)) {
        const rest = name.slice(verb.length);
        return `<span class="${cls}">${escapeHtml(verb)}</span>${escapeHtml(rest)}`;
      }
    }
    return escapeHtml(name);
  }
})();
