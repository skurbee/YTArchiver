/**
 * web/columnSort.js — clickable column-header sort on Subs + Recent tables.
 *
 * Extracted from app.js. Each table's <thead> th becomes clickable; first
 * click sorts ascending by that column's type-aware comparator (string /
 * num / size / age / dur), second click flips direction. Arrow indicator
 * shows the active column.
 *
 * Exposed as window.initColumnSort; app.js boot calls it once.
 *
 * Depends on: nothing (pure DOM operations on existing tables).
 */
(function () {
  "use strict";

  // ─── Column sort on Subs + Recent tables ─────────────────────────────
  function initColumnSort() {
    // Subs table
    const subsThead = document.querySelector(".subs-table thead");
    if (subsThead) wireTableSort(subsThead, "subs-table-body",
                                 { folder: "string", res: "string",
                                   min: "num", max: "num",
                                   compress: "string", transcribed: "string", metadata: "string",
                                   last_sync: "string", n_vids: "num",
                                   size: "size", avg_size: "size" });
    // Recent table
    const recentThead = document.querySelector(".recent-table thead");
    if (recentThead) wireTableSort(recentThead, "recent-table-body",
                                   { title: "string", channel: "string",
                                     time: "age", duration: "dur", size: "size" });
  }

  function wireTableSort(thead, tbodyId, kinds) {
    const ths = thead.querySelectorAll("th");
    let currentSort = { col: null, dir: 1 };
    ths.forEach((th, i) => {
      // Re-init guard so a hot-reload / repeat initColumnSort call
      // doesn't stack N click handlers on each th — a single click
      // would otherwise trigger N sorts in succession (audit:
      // columnSort.js:38).
      if (th._sortWired) return;
      th._sortWired = true;
      th.style.cursor = "pointer";
      th.addEventListener("click", () => {
        // The arrow indicator (\u25B2/\u25BC) is stored in data-arrow and
        // rendered via CSS ::after, so th.textContent itself is clean.
        // But if a future change ever appends the arrow into the th's
        // text, the fallback identity here would drift after the first
        // click. Strip arrow chars defensively so the same column always
        // produces the same `col` key across clicks.
        const _txt = (th.textContent || "")
          .replace(/[\u25B2\u25BC]/g, "")
          .trim()
          .toLowerCase();
        const col = th.dataset.sort || _txt;
        const dir = (currentSort.col === col) ? -currentSort.dir : 1;
        currentSort = { col, dir };
        sortTableBody(tbodyId, i, kinds[col] || "string", dir);
        // Arrow indicator
        ths.forEach(x => x.dataset.arrow = "");
        th.dataset.arrow = dir > 0 ? "\u25B2" : "\u25BC";
      });
    });
  }

  function sortTableBody(tbodyId, colIdx, kind, dir) {
    const tbody = document.getElementById(tbodyId);
    if (!tbody) return;
    const rows = Array.from(tbody.querySelectorAll("tr"));
    rows.sort((a, b) => {
      const av = (a.cells[colIdx]?.textContent || "").trim();
      const bv = (b.cells[colIdx]?.textContent || "").trim();
      const cmp = compareByKind(av, bv, kind);
      return dir > 0 ? cmp : -cmp;
    });
    const frag = document.createDocumentFragment();
    rows.forEach(r => frag.appendChild(r));
    tbody.appendChild(frag);
  }

  function compareByKind(a, b, kind) {
    if (kind === "num") {
      // Treat blanks / em-dash placeholders as +Infinity so they
      // always sort to the END regardless of direction. Previous
      // `|| 0` collapsed missing values into the middle of the
      // numeric range and conflated them with actual zero counts.
      const _aBlank = !a || a === "—" || a === "-" || a === "–";
      const _bBlank = !b || b === "—" || b === "-" || b === "–";
      if (_aBlank && _bBlank) return 0;
      if (_aBlank) return 1;
      if (_bBlank) return -1;
      const ai = parseFloat(a.replace(/[^\d.\-]/g, ""));
      const bi = parseFloat(b.replace(/[^\d.\-]/g, ""));
      const aN = Number.isFinite(ai) ? ai : Infinity;
      const bN = Number.isFinite(bi) ? bi : Infinity;
      return aN - bN;
    }
    if (kind === "size") {
      return parseBytes(a) - parseBytes(b);
    }
    if (kind === "dur") {
      return parseDuration(a) - parseDuration(b);
    }
    if (kind === "age") {
      return parseAge(a) - parseAge(b);
    }
    return a.toLowerCase().localeCompare(b.toLowerCase());
  }
  function parseBytes(s) {
    if (!s) return 0;
    const m = s.match(/([\d.]+)\s*(KB|MB|GB|TB|B)/i);
    if (!m) return 0;
    const mult = { b: 1, kb: 1024, mb: 1024**2, gb: 1024**3, tb: 1024**4 }[m[2].toLowerCase()] || 1;
    return parseFloat(m[1]) * mult;
  }
  function parseDuration(s) {
    if (!s) return 0;
    const parts = s.split(":").map(x => parseInt(x, 10) || 0);
    if (parts.length === 3) return parts[0]*3600 + parts[1]*60 + parts[2];
    if (parts.length === 2) return parts[0]*60 + parts[1];
    return 0;
  }
  function parseAge(s) {
    if (!s) return 0;
    // Match the longest unit first ("mo"/"y" before "m"). Without this,
    // a cell showing "3mo" would match `3m` and be treated as 3 minutes
    // instead of 3 months — flipping the sort order completely.
    // Years approximated as 365d, months as 30d (good enough for
    // a coarse Last-Sync column).
    const m = s.match(/(\d+)\s*(mo|y|m|h|d|w)/i);
    if (!m) return 0;
    const n = parseInt(m[1], 10);
    if (!Number.isFinite(n)) return 0;
    const unit = {
      m: 60,
      h: 3600,
      d: 86400,
      w: 604800,
      mo: 2592000,    // 30d
      y: 31536000,    // 365d
    }[m[2].toLowerCase()] || 60;
    return n * unit;
  }

  // feature F7: helpers for the bulk-actions bar above the Subs table.
  // Scoped to this IIFE via closure over `tbody` below — we don't
  // bother exporting since nothing outside needs to call them.
  function _selectedSubsRows(tbody) {
    return [...tbody.querySelectorAll("tr.row-selected")];
  }
  function _selectedSubsNames(tbody) {
    return _selectedSubsRows(tbody).map(tr =>
      tr.dataset.channelName
      || (tr.querySelector(".col-folder")?.textContent || "").trim())
      .filter(Boolean);
  }
  function _updateSubsBulkBar() {
    const tbody = document.getElementById("subs-table-body");
    const bar = document.getElementById("subs-bulk-bar");
    if (!tbody || !bar) return;
    const rows = _selectedSubsRows(tbody);
    const count = rows.length;
    const countEl = document.getElementById("subs-bulk-count");
    if (countEl) {
      countEl.textContent = count === 1
        ? "1 channel selected"
        : `${count} channels selected`;
    }
    // Only show the bar for multi-select. Single-select keeps the
    // existing single-row UX (right-click menu, Enter = edit, etc.)
    // unambiguous.
    bar.hidden = count < 2;
  }
  // Bulk-action button wiring — deferred until the tbody exists.
  function _wireSubsBulkButtons() {
    const tbody = document.getElementById("subs-table-body");
    if (!tbody) return;
    const api = () => window.pywebview?.api;
    const clear = () => {
      tbody.querySelectorAll("tr.row-selected")
        .forEach(r => r.classList.remove("row-selected"));
      _updateSubsBulkBar();
    };
    document.getElementById("btn-bulk-clear")?.addEventListener("click", clear);
    // Change resolution
    document.getElementById("btn-bulk-resolution")
      ?.addEventListener("click", async () => {
        const names = _selectedSubsNames(tbody);
        if (!names.length) return;
        const pick = await (window.askChoice ? window.askChoice({
          title: `Change resolution for ${names.length} channel(s)`,
          message: "Pick the new resolution. Applies only to future syncs — " +
                   "already-downloaded videos stay at their current " +
                   "resolution (use the Recheck Resolution tool per-channel " +
                   "to re-download).",
          choices: [
            { label: "audio-only", value: "audio" },
            { label: "360p", value: "360" },
            { label: "480p", value: "480" },
            { label: "720p", value: "720", primary: true },
            { label: "1080p", value: "1080" },
            { label: "1440p", value: "1440" },
            { label: "2160p", value: "2160" },
            { label: "best", value: "best" },
          ],
        }) : null);
        if (!pick) return;
        const res = await api()?.subs_bulk_update?.(names, { resolution: pick });
        if (res?.ok) {
          window._showToast?.(
            `Updated ${res.updated} channel(s) to ${pick}.`, "ok");
          clear();
          window.refreshSubsTable?.();
        } else {
          window._showToast?.(res?.error || "Bulk update failed.", "error");
        }
      });
    // Toggle auto-transcribe
    document.getElementById("btn-bulk-auto-tx")
      ?.addEventListener("click", async () => {
        const names = _selectedSubsNames(tbody);
        if (!names.length) return;
        const pick = await (window.askChoice ? window.askChoice({
          title: `Auto-transcribe for ${names.length} channel(s)`,
          message: "Toggle the Auto-transcribe flag for all selected " +
                   "channels. Future downloads will (or won't) run through " +
                   "Whisper automatically.",
          choices: [
            { label: "Enable for all", value: "on", primary: true },
            { label: "Disable for all", value: "off" },
          ],
        }) : null);
        if (!pick) return;
        const changes = { auto_transcribe: pick === "on" };
        const res = await api()?.subs_bulk_update?.(names, changes);
        if (res?.ok) {
          window._showToast?.(
            `Updated ${res.updated} channel(s).`, "ok");
          clear();
          window.refreshSubsTable?.();
        } else {
          window._showToast?.(res?.error || "Bulk update failed.", "error");
        }
      });
    // Queue metadata
    document.getElementById("btn-bulk-metadata")
      ?.addEventListener("click", async () => {
        const names = _selectedSubsNames(tbody);
        if (!names.length) return;
        const ok = await askConfirm(
          `Queue metadata for ${names.length} channel(s)`,
          `Enqueue a metadata refresh for every selected channel. ` +
          `Each becomes its own task on the Sync Tasks popover and ` +
          `fires as soon as the current sync is idle.`,
          { confirm: "Queue all" });
        if (!ok) return;
        const res = await api()?.subs_bulk_queue_metadata?.(names, true);
        if (res?.ok) {
          window._showToast?.(
            `Queued metadata refresh for ${res.queued} channel(s).`, "ok");
          clear();
        } else {
          window._showToast?.(res?.error || "Bulk queue failed.", "error");
        }
      });
    // Delete
    document.getElementById("btn-bulk-delete")
      ?.addEventListener("click", async () => {
        const names = _selectedSubsNames(tbody);
        if (!names.length) return;
        const choice = await (window.askChoice ? window.askChoice({
          title: `Delete ${names.length} channel(s)?`,
          message: `You're about to unsubscribe from ${names.length} ` +
                   `channel(s). Keep files = just remove the subscription. ` +
                   `Delete files = also wipe the on-disk folders (videos, ` +
                   `transcripts, metadata).`,
          choices: [
            { label: "Keep files", value: "keep", primary: true },
            { label: "Delete files too", value: "delete", kind: "danger" },
          ],
          cancel: "Cancel",
        }) : null);
        if (!choice) return;
        const deleteFiles = choice === "delete";
        const res = await api()?.subs_bulk_delete?.(names, deleteFiles);
        if (res?.ok) {
          window._showToast?.(
            `Removed ${res.deleted} channel(s).`, "ok");
          clear();
          window.refreshSubsTable?.();
        } else {
          window._showToast?.(res?.error || "Bulk delete failed.", "error");
        }
      });
  }

  function initSubsContextMenu() {
    const tbody = document.getElementById("subs-table-body");
    if (!tbody) return;
    // feature F7: wire the bulk-actions bar buttons. Idempotent guard
    // via a dataset flag so re-inits (e.g. hot reload) don't double-bind.
    if (!tbody.dataset.f7Wired) {
      tbody.dataset.f7Wired = "1";
      try { _wireSubsBulkButtons(); } catch (e) { console.error("F7 wire:", e); }
    }

    // Make the tbody focusable so keyboard events fire when the Subs tab is
    // focused. Click selects the row; Enter opens edit; Delete removes.
    // feature F7: Ctrl/Cmd-click toggles this row; Shift-click selects a
    // range. Matches the Recent-table pattern (logs.js:760+). When >1
    // row is selected, the bulk-actions bar shows up automatically
    // (wired below in _updateSubsBulkBar).
    tbody.setAttribute("tabindex", "0");
    let _subsLastClickedIdx = -1;
    tbody.addEventListener("click", (e) => {
      const tr = e.target.closest("tr");
      if (!tr) return;
      const allTrs = [...tbody.querySelectorAll("tr")];
      const idx = allTrs.indexOf(tr);
      if (e.ctrlKey || e.metaKey) {
        tr.classList.toggle("row-selected");
      } else if (e.shiftKey && _subsLastClickedIdx >= 0) {
        const [a, b] = [Math.min(_subsLastClickedIdx, idx),
                        Math.max(_subsLastClickedIdx, idx)];
        allTrs.forEach((r, i) => {
          if (i >= a && i <= b) r.classList.add("row-selected");
        });
      } else {
        allTrs.forEach(r => r.classList.remove("row-selected"));
        tr.classList.add("row-selected");
      }
      _subsLastClickedIdx = idx;
      tbody.focus();
      _updateSubsBulkBar();
    });
    tbody.addEventListener("keydown", async (e) => {
      const selected = tbody.querySelector("tr.row-selected");
      if (!selected) return;
      const folder = selected.dataset.channelName
        || (selected.querySelector(".col-folder")?.textContent || "").trim();
      if (!folder) return;
      if (e.key === "Enter" || e.key === "F2") {
        e.preventDefault();
        window._editChannelFromContext?.(folder);
      } else if (e.key === "Delete") {
        e.preventDefault();
        const res = await window._removeChannelWithPrompt(folder);
        // refresh in place instead of reloading the entire
        // page. `location.reload()` nuked the main log, queue state,
        // any pending toasts, and — critically — the Undo toast that
        // the remove path tries to show. Use the same refresh
        // helpers the right-click Remove path uses so keyboard
        // Delete behaves consistently.
        if (res && res.ok) {
          try { window.refreshSubsTable?.(); } catch {}
          try { window._primeBrowse?.(); } catch {}
        }
      } else if (e.key === "ArrowDown" || e.key === "ArrowUp") {
        e.preventDefault();
        const allTrs = [...tbody.querySelectorAll("tr")];
        const idx = allTrs.indexOf(selected);
        const next = e.key === "ArrowDown" ? Math.min(allTrs.length - 1, idx + 1) : Math.max(0, idx - 1);
        allTrs.forEach(t => t.classList.remove("row-selected"));
        allTrs[next].classList.add("row-selected");
        allTrs[next].scrollIntoView({ block: "nearest" });
      }
    });

    tbody.addEventListener("contextmenu", (e) => {
      const tr = e.target.closest("tr");
      if (!tr) return;
      e.preventDefault();
      // Visual select. If the user has a multi-select active AND the
      // right-clicked row is part of it, leave the existing selection
      // alone — otherwise right-click silently collapsed N-row
      // selections to one row (audit: columnSort.js:329). The bulk
      // toolbar above the table is the right place to act on the
      // multi-selection; right-click stays a per-row action.
      const _existingSelected =
        tbody.querySelectorAll("tr.row-selected");
      const _hasMulti = _existingSelected.length > 1
        && tr.classList.contains("row-selected");
      if (!_hasMulti) {
        _existingSelected.forEach(x => x.classList.remove("row-selected"));
        tr.classList.add("row-selected");
      }
      // Prefer the clean `data-channel-name` stashed by the renderer.
      // Fall back to `.col-folder`'s textContent ONLY if the data attr
      // is missing — the cell now may contain a trailing dot span so
      // the textContent path is polluted (e.g. "Channel Name ●").
      const chan = tr.dataset.channelName
        || (tr.querySelector(".col-folder")?.textContent || "").trim();

      const api = window.pywebview?.api;
      // Dynamic-label helpers — peek at live queue state so menu items
      // reflect what's already queued. Matches OLD's _chan_ctx_menu label
      // mutation (YTArchiver.py:5596 — "Add to Sync List" → "Already in
      // Sync List" → "Channel Transcribing...").
      const _syncState = window._queueHasSyncForChannel?.(chan);
      const _gpuState = window._queueHasGpuForChannel?.(chan);
      // when a sync pipeline is active, "Sync now" should
      // ENQUEUE the channel rather than do nothing. Only the
      // already-running and already-queued states stay disabled.
      const _syncActiveButOtherChannel =
        (window._anySyncRunning?.() || false) && !_syncState;
      const _syncLabel = _syncState === "running" ? "Syncing now \u2026 (already running)"
                       : _syncState === "queued" ? "Already in Sync queue"
                       : _syncActiveButOtherChannel ? "Add to Sync queue"
                                                  : "Sync now";
      const _syncDisabled = Boolean(_syncState);
      const _txLabel = _gpuState === "running" ? "Channel transcribing \u2026"
                     : _gpuState === "queued" ? "Already queued for transcribe"
                                               : "Transcribe channel";
      const _txDisabled = Boolean(_gpuState);
      // Match YTArchiver.py _chan_ctx_menu (line 5596-6180): 15-item menu
      // with sub-menus for organization mode + redownload quality.
      showContextMenu(e.clientX, e.clientY, [
        { label: _syncLabel, cls: _syncDisabled ? "dim" : "",
          action: async () => {
            if (_syncDisabled) return;
            const r = await api?.sync_one_channel?.({ name: chan });
            if (r?.ok && r?.queued) {
              window._showToast?.(`Added "${r.name || chan}" to sync queue.`, "ok");
            } else if (r?.error) {
              window._showToast?.(r.error, "error");
            }
          }},
        { label: "Edit settings", action: () => window._editChannelFromContext?.(chan) },
        { label: "Open folder in Explorer", action: () => api?.chan_open_folder?.(chan) },
        { label: "Open URL in browser", action: () => api?.chan_open_url?.(chan) },
        { sep: true },
        { label: "Reorg folder",
          submenu: [
            { label: "Flat (no split)", action: () => api?.reorg_channel_folder?.({ name: chan }, false, false, false) },
            { label: "Split by year", action: () => api?.reorg_channel_folder?.({ name: chan }, true, false, false) },
            { label: "Split by year + month", action: () => api?.reorg_channel_folder?.({ name: chan }, true, true, false) },
            { label: "Re-apply organization", action: () => api?.reorg_channel_folder?.({ name: chan }, null, null, false) },
            // Recheck-dates + fix-file-dates are long operations — OLD app
            // shows an all-caps warning dialog. YTArchiver.py:5721-5742.
            { label: "Re-check dates + year/month", action: async () => {
              const ok = await askDanger("Re-check dates",
                `Re-check upload dates for every video in "${chan}" and re-sort into Year/Month folders?\n\n` +
                `\u26A0\uFE0F WARNING: THIS CAN TAKE MULTIPLE HOURS ON LARGE CHANNELS`,
                "Re-check dates");
              if (!ok) return;
              api?.reorg_channel_folder?.({ name: chan }, true, true, true);
            }},
            { label: "Fix file dates only", action: async () => {
              const ok = await askDanger("Fix file dates",
                `Re-fetch upload dates from YouTube for every video in "${chan}" ` +
                `and stamp each file's mtime to match?\n\n` +
                `\u26A0\uFE0F WARNING: THIS CAN TAKE MULTIPLE HOURS ON LARGE CHANNELS`,
                "Fix dates");
              if (!ok) return;
              api?.chan_fix_file_dates?.({ name: chan });
            }},
          ]},
        // "Fetch channel art" used to live here but the user flagged it as
        // redundant — channel art is fetched automatically as part of the
        // full metadata sweep. Removed to keep the menu focused.
        { sep: true },
        { label: _txLabel, cls: _txDisabled ? "dim" : "",
          action: () => { if (!_txDisabled) _askTranscribeChannel(chan); }},
        // right-click → re-transcribe entire channel with
        // model selection. Confirms first because this can be a long
        // GPU job (hundreds of videos on large channels).
        { label: "Re-transcribe channel…", action: async () => {
          const model = await (window._askWhisperModel?.(`channel "${chan}"`));
          if (!model) return;
          const ok = await askDanger(
            "Re-transcribe entire channel",
            `Queue every video in "${chan}" for re-transcription with `
              + `Whisper ${model}?\n\nThis can take hours on large channels.`,
            "Queue all");
          if (!ok) return;
          const res = await api?.transcribe_retranscribe_channel?.(
            { name: chan }, model);
          if (res?.ok) {
            window._showToast?.(
              `Queued ${res.queued} video(s) from ${chan} for Whisper ${model}.`,
              "ok");
          } else {
            window._showToast?.(res?.error || "Channel retranscribe failed.",
                                "error");
          }
        }},
        { label: "Download metadata", action: () => api?.metadata_recheck_channel?.({ name: chan }) },
        { sep: true },
        // Pending-redownload swap: when `_redownload_progress.json` is
        // present for this channel (flagged by the backend via
        // `tr.dataset.pendingRedownload`), replace the "Redownload
        // at..." submenu with a single "Continue Redownload at X"
        // action that just fires chan_redownload with the stored
        // resolution — no submenu, no confirm prompt.
        ...(tr.dataset.pendingRedownload ? [
          { label: `Continue Redownload at ${(() => {
              const r = tr.dataset.redownloadRes || "";
              return r === "best" ? "Best available"
                   : r ? `${r}p` : "target resolution";
            })()}`,
            action: async () => {
              const res = tr.dataset.redownloadRes || "";
              if (!res) return;
              try {
                const r = await api?.chan_redownload?.({ name: chan }, res);
                if (!r) return;
                if (!r.ok) {
                  window._showToast?.(r.error || "Redownload failed", "error");
                  return;
                }
                if (r.queued) {
                  window._showToast?.(
                    `Queued redownload of ${chan}.`, "ok");
                } else if (r.started) {
                  window._showToast?.(
                    `Redownload started: ${chan}.`, "ok");
                }
              } catch (e) {
                window._showToast?.("Error: " + e, "error");
              }
            }},
        ] : [
          { label: "Redownload at\u2026",
            submenu: [
              { label: "Best available", action: () => _askRedownload(chan, "best") },
              { label: "2160p (4K)", action: () => _askRedownload(chan, "2160") },
              { label: "1440p", action: () => _askRedownload(chan, "1440") },
              { label: "1080p", action: () => _askRedownload(chan, "1080") },
              { label: "720p", action: () => _askRedownload(chan, "720") },
              { label: "480p", action: () => _askRedownload(chan, "480") },
              { label: "360p", action: () => _askRedownload(chan, "360") },
            ]},
        ]),
        { sep: true },
        { label: "Remove channel", cls: "dim", action: async () => {
          // Two-step (subscription → optional disk delete) via shared helper.
          const res = await window._removeChannelWithPrompt(chan);
          if (!res || !res.ok) return;
          // Refresh Subs data without a full page reload, so the undo toast survives
          if (api?.get_subs_channels) {
            const data = await api.get_subs_channels();
            if (Array.isArray(data) && data.length === 2) {
              window.renderSubsTable(data[0], data[1]);
              window._primeBrowse?.(data[0]);
            }
          }
          if (res?.can_undo) {
            window._showToast({
              msg: `Removed "${chan}".`,
              kind: "warn",
              ttlMs: 10_000,
              action: {
                label: "Undo",
                onClick: async () => {
                  const u = await api.subs_undo_remove();
                  if (u?.ok) {
                    window._showToast?.("Channel restored.", "ok");
                    const data = await api.get_subs_channels();
                    if (Array.isArray(data) && data.length === 2) {
                      window.renderSubsTable(data[0], data[1]);
                      window._primeBrowse?.(data[0]);
                    }
                  } else {
                    window._showToast?.(u?.error || "Undo failed.", "error");
                  }
                },
              },
            });
          }
        }},
      ]);
    });
  }

  window.initColumnSort = initColumnSort;
  window.initSubsContextMenu = initSubsContextMenu;
})();
