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
     window.pywebview.api             — Python bridge (used directly on
       purpose — see note below)

   Bridge note: this module calls window.pywebview.api directly so it can
   require the exact task-ID mutation methods. URL/path/index fallbacks are
   intentionally forbidden: a stale row must fail instead of changing a
   different task that happens to share the same visible name or target.
   ═══════════════════════════════════════════════════════════════════════ */

(function () {
  "use strict";

  const escapeHtml = window._escapeHtml || ((s) => String(s ?? ""));

  /** Render the queue popovers for Sync Tasks + GPU Tasks. */
  window.renderQueues = function (queues) {
    _queueIdentityDurable = queues?.identity_ids_durable !== false;
    renderTaskList("sync-tasks-body", queues.sync, "No sync tasks queued.", "sync");
    renderTaskList("gpu-tasks-body", queues.gpu, "No processing tasks queued.", "gpu");
    _updateBadge("badge-sync", _queueCount(queues, "sync"));
    _updateBadge("badge-gpu", _queueCount(queues, "gpu"));
    window.YT?.eventState?.publish("queue-payload", queues || {
      sync: [], gpu: [],
    });
  };

  function _queueCount(queues, kind) {
    const raw = queues?.[`${kind}_count`];
    if (Number.isFinite(raw)) return Math.max(0, Number(raw));
    return (queues?.[kind] || []).length;
  }

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

  function clearDragTargets() {
    document.querySelectorAll(".drag-target-above, .drag-target-below")
      .forEach(el => el.classList.remove("drag-target-above", "drag-target-below"));
  }

  // In-memory queue state so drag-to-rearrange can update order.
  const _queueState = { sync: [], gpu: [] };
  let _dragSrcKind = "";
  let _queueIdentityDurable = true;
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

  function _queueChannelKey(value) {
    return String(value || "").trim().replace(/\s+/g, " ").toLowerCase();
  }

  function _queueTaskMatchesChannel(task, channelName) {
    const target = _queueChannelKey(channelName);
    if (!target) return false;
    return [
      task?.channel_name,
      task?.channel,
      task?.folder,
      task?.uploader,
      task?.title,
      task?.name,
    ].some(value => _queueChannelKey(value) === target);
  }

  function _rowTaskIds(task) {
    const values = Array.isArray(task?.represented_task_ids)
      ? task.represented_task_ids
      : (Array.isArray(task?.task_ids) ? task.task_ids : [task?.task_id]);
    return values.map(value => String(value || "").trim()).filter(Boolean);
  }

  function _rowHasTaskId(task, taskId) {
    const wanted = String(taskId || "").trim();
    return !!wanted && _rowTaskIds(task).includes(wanted);
  }

  function _findRowIndex(queueKind, taskId) {
    return (_queueState[queueKind] || [])
      .findIndex(item => _rowHasTaskId(item, taskId));
  }

  function _bridgeSucceeded(result) {
    return !!result && result.ok === true;
  }

  async function _runExactQueueAction(action, fallbackMessage) {
    let result;
    try {
      result = await action();
    } catch (error) {
      window._showToast?.(`${fallbackMessage}: ${error}`, "error");
      return false;
    }
    if (!_bridgeSucceeded(result)) {
      window._showToast?.(result?.error || fallbackMessage, "error");
      return false;
    }
    return true;
  }

  // Convenience: does `channelName` have a sync queued? (running or queued)
  window._queueHasSyncForChannel = (channelName) => {
    for (const t of _queueState.sync) {
      if (_queueTaskMatchesChannel(t, channelName)) {
        return t.status || "queued"; // "running" | "queued"
      }
    }
    return null;
  };
  // Convenience: does a GPU task (transcribe/encode/compress) reference this channel?
  window._queueHasGpuForChannel = (channelName) => {
    for (const t of _queueState.gpu) {
      if (_queueTaskMatchesChannel(t, channelName)) {
        return t.status || "queued";
      }
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
      // textContent, not innerHTML — defense-in-depth so emptyText can never
      // be an injection sink if a caller ever passes derived text (audit r2).
      const empty = document.createElement("div");
      empty.className = "queue-empty";
      empty.textContent = emptyText;
      body.appendChild(empty);
      return;
    }
    list.forEach((t, i) => {
      const row = document.createElement("div");
      // Whitelist status → a fixed class; never interpolate a raw backend
      // string into class/innerHTML below (defense-in-depth, audit r2).
      const _rawStatus = t.status || "queued";
      const cancelling = !!t.cancel_requested;
      const statusCls = (_rawStatus === "running" || _rawStatus === "paused")
        ? _rawStatus : "queued";
      row.className = `queue-task-row ${statusCls}`;
      const representedIds = _rowTaskIds(t);
      const taskId = representedIds[0] || "";
      const canDrag = _queueIdentityDurable && statusCls === "queued"
        && t.draggable !== false
        && representedIds.length === 1;
      row.draggable = canDrag;
      row.dataset.idx = i;
      row.dataset.queue = queueKind;
      row.dataset.taskId = taskId;

      const stateGlyph =
        statusCls === "running" ? "▶" :
        statusCls === "paused" ? "❚❚" :
                                  "○";

      // Color the verb (Downloading/Transcribing/Metadata) in tag color
      const nameHtml = colorizeTaskName(
        (cancelling ? "Cancelling… " : "") + (t.name || t.title || ""));

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
      const closeBtnHtml = statusCls === "running" || representedIds.length === 0
        || !_queueIdentityDurable
        ? ""
        : '<button class="queue-task-close" title="Remove">&times;</button>';

      row.innerHTML = `
        <span class="queue-task-index">${i + 1}.</span>
        <span class="queue-task-state ${statusCls}">${stateGlyph}</span>
        <span class="queue-task-name"></span>${dotsSpan}
        ${closeBtnHtml}
      `;
      row.querySelector(".queue-task-name").innerHTML = nameHtml;

      row.querySelector(".queue-task-close")?.addEventListener("click", async (e) => {
        e.stopPropagation();
        const api = window.pywebview?.api;
        if (!api || representedIds.length === 0) return;
        let result;
        try {
          if (queueKind === "sync") {
            result = await api.queues_sync_remove(representedIds[0]);
          } else if (representedIds.length > 1) {
            result = await api.queues_gpu_remove_many(representedIds);
          } else {
            result = await api.queues_gpu_remove(representedIds[0]);
          }
        } catch (error) {
          window._showToast?.(`Remove failed: ${error}`, "error");
          return;
        }
        if (!_bridgeSucceeded(result)) {
          window._showToast?.(result?.error || "Queue changed; refresh and retry.",
            "error");
          return;
        }
        const removedIds = new Set(representedIds);
        _queueState[queueKind] = _queueState[queueKind].filter(
          item => !_rowTaskIds(item).some(taskId => removedIds.has(taskId)));
        paintTaskList(body, _queueState[queueKind], emptyText, queueKind);
      });

      // Right-click menu on queue rows: skip / move-to-top / cancel-or-remove
      // Mirrors YTArchiver.py:20570-20584 (sync) + 21441-21455 (gpu) — each
      // destructive action pops a confirm, matching the old app's askyesno flow.
      row.addEventListener("contextmenu", (ev) => {
        ev.preventDefault();
        ev.stopPropagation();
        if (cancelling) return;
        const idx = _findRowIndex(queueKind, taskId);
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
              await _runExactQueueAction(
                () => queueKind === "sync"
                  ? api?.sync_defer_current?.(taskId)
                  : api?.gpu_defer_current?.(taskId),
                "Task could not be deferred; refresh and retry.",
              );
            }});
        }
        // "Move to top" — only offered when there's something above the
        // task to overtake. Showing it on idx === 0 (running task or already-
        // first queued task) was confusing because the click silently did
        // nothing.
        if (canDrag && Number(t.pending_index) > 0) {
          items.push(
            { label: "Move to top",
              action: async () => {
                let result;
                try {
                  result = queueKind === "sync"
                    ? await api?.queues_sync_reorder?.(taskId, 0)
                    : await api?.queues_gpu_reorder?.(taskId, 0);
                } catch (error) {
                  window._showToast?.(`Reorder failed: ${error}`, "error");
                  return;
                }
                if (!_bridgeSucceeded(result)) {
                  window._showToast?.(result?.error || "Queue changed; refresh and retry.",
                    "error");
                  return;
                }
                const liveIndex = _findRowIndex(queueKind, taskId);
                if (liveIndex < 0) return;
                const [taken] = _queueState[queueKind].splice(liveIndex, 1);
                const firstPending = _queueState[queueKind].findIndex(
                  item => (item?.status || "queued") !== "running");
                _queueState[queueKind].splice(
                  firstPending < 0 ? _queueState[queueKind].length : firstPending,
                  0, taken);
                paintTaskList(body, _queueState[queueKind], emptyText, queueKind);
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
                await _runExactQueueAction(
                  () => queueKind === "sync"
                    ? api?.sync_skip_current?.(taskId)
                    : api?.gpu_skip_current?.(taskId),
                  "Task could not be cancelled; refresh and retry.",
                );
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
        if (!canDrag || !taskId) {
          e.preventDefault();
          return;
        }
        row.classList.add("drag-src");
        _dragSrcKind = queueKind;
        e.dataTransfer.effectAllowed = "move";
        e.dataTransfer.setData("text/plain",
          JSON.stringify({
            queueKind: queueKind,
            task_id: taskId,
          }));
      });
      row.addEventListener("dragend", () => {
        row.classList.remove("drag-src");
        _dragSrcKind = "";
        clearDragTargets();
      });
      row.addEventListener("dragover", (e) => {
        if (_dragSrcKind && _dragSrcKind !== queueKind) {
          e.dataTransfer.dropEffect = "none";
          row.classList.remove("drag-target-above", "drag-target-below");
          return;
        }
        e.preventDefault();
        e.dataTransfer.dropEffect = "move";
        const rect = row.getBoundingClientRect();
        const halfway = rect.top + rect.height / 2;
        row.classList.toggle("drag-target-above", e.clientY < halfway);
        row.classList.toggle("drag-target-below", e.clientY >= halfway);
      });
      row.addEventListener("dragleave", () => {
        row.classList.remove("drag-target-above", "drag-target-below");
      });
      row.addEventListener("drop", async (e) => {
        e.preventDefault();
        let parsed;
        try { parsed = JSON.parse(e.dataTransfer.getData("text/plain")); }
        catch { parsed = null; }
        const srcKind = parsed?.queueKind || "";
        const sourceTaskId = String(parsed?.task_id || "").trim();
        if (srcKind !== queueKind) {
          row.classList.remove("drag-target-above", "drag-target-below");
          // Cross-queue drop: no-op. Show a brief toast so the user
          // knows the drag was registered but rejected on purpose.
          window._showToast?.(
            "Can't drag tasks between Sync and Processing queues.", "warn");
          return;
        }
        const srcIdx = _findRowIndex(queueKind, sourceTaskId);
        const dstIdx = _findRowIndex(queueKind, taskId);
        if (!sourceTaskId || srcIdx < 0 || dstIdx < 0 || srcIdx === dstIdx) return;
        const rect = row.getBoundingClientRect();
        const below = e.clientY >= rect.top + rect.height / 2;
        const list = _queueState[queueKind];
        const source = list[srcIdx];
        const sourcePending = Number(source?.pending_index);
        let insertAt = statusCls === "running"
          ? 0
          : (below ? Number(t.pending_end) + 1 : Number(t.pending_start));
        if (!Number.isFinite(insertAt) || !Number.isFinite(sourcePending)) return;
        if (sourcePending < insertAt) insertAt -= 1;
        const pendingCount = list.reduce(
          (count, item) => count + ((item?.status || "queued") === "running"
            ? 0 : _rowTaskIds(item).length), 0);
        insertAt = Math.max(0, Math.min(insertAt, pendingCount - 1));
        const api = window.pywebview?.api;
        let result;
        try {
          result = queueKind === "sync"
            ? await api?.queues_sync_reorder?.(sourceTaskId, insertAt)
            : await api?.queues_gpu_reorder?.(sourceTaskId, insertAt);
        } catch (error) {
          window._showToast?.(`Reorder failed: ${error}`, "error");
          return;
        }
        if (!_bridgeSucceeded(result)) {
          window._showToast?.(result?.error || "Queue changed; refresh and retry.",
            "error");
          return;
        }

        // Apply the visual reorder only after the backend accepted the exact
        // ID. A concurrent backend push may already have done this, so resolve
        // both source and destination again from the latest mirror.
        const liveSource = _findRowIndex(queueKind, sourceTaskId);
        const liveTarget = _findRowIndex(queueKind, taskId);
        if (liveSource < 0 || liveTarget < 0 || liveSource === liveTarget) return;
        const [moved] = list.splice(liveSource, 1);
        let displayInsert = liveTarget;
        if (liveSource < liveTarget) displayInsert -= 1;
        if (below) displayInsert += 1;
        list.splice(displayInsert, 0, moved);
        paintTaskList(body, list, emptyText, queueKind);
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
