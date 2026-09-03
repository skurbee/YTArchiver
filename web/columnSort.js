/**
 * web/columnSort.js — clickable column-header sort on the Subs table.
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

  function bridgeCall(method, ...args) {
    const fn = window.YT?.bridge?.bridgeCall;
    if (fn) return fn(method, ...args);
    return undefined;
  }
  function nativeBridgeUp() {
    return !!window.YT?.bridge?.isUp?.();
  }

  async function checkedAction(action, {
    success = "",
    failure = "Action failed.",
  } = {}) {
    try {
      const result = await action();
      if (!result?.ok) {
        window._showToast?.(result?.error || failure, "error");
      } else if (success) {
        window._showToast?.(success, "ok");
      }
      return result;
    } catch (error) {
      window._showToast?.(
        `${failure.replace(/[.]+$/, "")}: ${error?.message || error}`,
        "error");
      return { ok: false, error: String(error) };
    }
  }

  // ─── Column sort on Subs table ───────────────────────────────────────
  function initColumnSort() {
    // Subs table
    const subsThead = document.querySelector(".subs-table thead");
    if (subsThead) wireTableSort(subsThead, "subs-table-body",
                                 { folder: "string", res: "string",
                                   min: "num", max: "num",
                                   compress: "string", transcribed: "string", metadata: "string",
                                   // "age" kind parses "10d ago" / "3h" etc. so the
                                   // column sorts by recency, not by alphabetical
                                   // ordering of the formatted string (which had it
                                   // jumping 10d → 5d → 18h instead of monotonic).
                                   last_sync: "age", n_vids: "num",
                                   size: "size", avg_size: "size" });
  }

  function wireTableSort(thead, tbodyId, kinds) {
    const ths = thead.querySelectorAll("th");
    window._tableSortState = window._tableSortState || {};
    let currentSort = window._tableSortState[tbodyId]
      || { col: null, colIdx: -1, kind: "string", dir: 1 };
    const paintSortState = () => {
      ths.forEach((header, index) => {
        if (header.hasAttribute("data-nosort")) {
          header.removeAttribute("aria-sort");
          return;
        }
        const active = index === currentSort.colIdx && currentSort.col;
        header.dataset.arrow = active
          ? (currentSort.dir > 0 ? "\u25B2" : "\u25BC") : "";
        header.setAttribute("aria-sort", active
          ? (currentSort.dir > 0 ? "ascending" : "descending") : "none");
      });
    };
    const applyCurrentSort = () => {
      currentSort = window._tableSortState[tbodyId] || currentSort;
      if (currentSort.col && currentSort.colIdx >= 0) {
        sortTableBody(
          tbodyId, currentSort.colIdx, currentSort.kind, currentSort.dir);
      }
      paintSortState();
    };
    if (tbodyId === "subs-table-body") {
      window._applySubsSort = applyCurrentSort;
    }
    ths.forEach((th, i) => {
      // Skip non-sortable headers (e.g. the row-actions kebab column) —
      // they carry data-nosort so they don't get a pointer cursor or a
      // click-to-sort handler (sorting by a button column is nonsense).
      if (th.hasAttribute("data-nosort")) return;
      th.tabIndex = 0;
      // Re-init guard so a hot-reload / repeat initColumnSort call
      // doesn't stack N click handlers on each th — a single click
      // would otherwise trigger N sorts in succession (audit:
      // columnSort.js:38).
      if (th._sortWired) return;
      th._sortWired = true;
      th.style.cursor = "pointer";
      const activate = () => {
        currentSort = window._tableSortState[tbodyId] || currentSort;
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
        currentSort = { col, colIdx: i, kind: kinds[col] || "string", dir };
        window._tableSortState[tbodyId] = currentSort;
        applyCurrentSort();
      };
      th.addEventListener("click", activate);
      th.addEventListener("keydown", (event) => {
        if (event.key !== "Enter" && event.key !== " ") return;
        event.preventDefault();
        activate();
      });
    });
    applyCurrentSort();
  }

  function sortTableBody(tbodyId, colIdx, kind, dir) {
    const tbody = document.getElementById(tbodyId);
    if (!tbody) return;
    const rows = Array.from(tbody.querySelectorAll("tr"));
    rows.sort((a, b) => {
      const av = (a.cells[colIdx]?.textContent || "").trim();
      const bv = (b.cells[colIdx]?.textContent || "").trim();
      const aPlaceholder = isPlaceholderValue(av, kind);
      const bPlaceholder = isPlaceholderValue(bv, kind);
      // Unknown values stay at the bottom in either direction. Applying the
      // descending multiplier to Infinity used to pull them to the top.
      if (aPlaceholder && bPlaceholder) return 0;
      if (aPlaceholder) return 1;
      if (bPlaceholder) return -1;
      const cmp = compareByKind(av, bv, kind);
      return dir > 0 ? cmp : -cmp;
    });
    const frag = document.createDocumentFragment();
    rows.forEach(r => frag.appendChild(r));
    tbody.appendChild(frag);
  }

  function isPlaceholderValue(value, kind) {
    const text = String(value || "").trim();
    const lower = text.toLowerCase();
    if (!text || ["—", "-", "–", "n/a", "na", "unknown",
      "not available"].includes(lower)) return true;
    if (kind === "age") return !Number.isFinite(parseAge(text));
    if (kind === "num") {
      return !Number.isFinite(parseFloat(text.replace(/[^\d.\-]/g, "")));
    }
    if (kind === "size") {
      return !/([\d.]+)\s*(KB|MB|GB|TB|B)/i.test(text);
    }
    return false;
  }

  function compareByKind(a, b, kind) {
    if (kind === "num") {
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
    if (!s) return Infinity;
    const _s = s.trim().toLowerCase();
    // "Never" channels are infinitely-old by definition — sort them to
    // the end regardless of direction. Without this they returned 0
    // (== "just now") and clustered with the most-recently-synced rows.
    if (_s === "never" || _s === "—" || _s === "-") return Infinity;
    // "just now" is 0 seconds ago — most recent. Caught explicitly so
    // it doesn't fall into the regex-miss path (which now returns
    // Infinity instead of 0).
    if (_s.startsWith("just now")) return 0;
    // Match the longest unit first ("mo"/"y" before "m"). Without this,
    // a cell showing "3mo" would match `3m` and be treated as 3 minutes
    // instead of 3 months — flipping the sort order completely.
    // Years approximated as 365d, months as 30d (good enough for
    // a coarse Last-Sync column).
    const m = s.match(/(\d+)\s*(mo|y|m|h|d|w)/i);
    if (!m) return Infinity;
    const n = parseInt(m[1], 10);
    if (!Number.isFinite(n)) return Infinity;
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
  async function _bulkBridgeCall(method, ...args) {
    try {
      return await bridgeCall(method, ...args);
    } catch (error) {
      window._showToast?.(
        `Bulk action failed: ${error?.message || error}`, "error");
      return null;
    }
  }
  function _firstBulkIssue(result) {
    const issue = Array.isArray(result?.failed) ? result.failed[0] : null;
    if (!issue) return "";
    return [issue.name, issue.reason].filter(Boolean).join(": ");
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
        const res = await _bulkBridgeCall(
          "subs_bulk_update", names, { resolution: pick });
        if (!res) return;
        if (res?.ok) {
          const failed = res.failed?.length || 0;
          const detail = _firstBulkIssue(res);
          window._showToast?.(
            `Updated ${res.updated} channel(s) to ${pick}.` +
            (failed ? ` ${failed} could not be updated.` : "") +
            (detail ? ` First issue: ${detail}` : ""),
            failed ? "warn" : "ok");
          clear();
          await window.refreshSubsTable?.();
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
        const res = await _bulkBridgeCall("subs_bulk_update", names, changes);
        if (!res) return;
        if (res?.ok) {
          const failed = res.failed?.length || 0;
          const detail = _firstBulkIssue(res);
          window._showToast?.(
            `Updated ${res.updated} channel(s).` +
            (failed ? ` ${failed} could not be updated.` : "") +
            (detail ? ` First issue: ${detail}` : ""),
            failed ? "warn" : "ok");
          clear();
          await window.refreshSubsTable?.();
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
        const res = await _bulkBridgeCall(
          "subs_bulk_queue_metadata", names, true);
        if (!res) return;
        if (res?.ok) {
          const already = Number(res.already_queued || 0);
          const failed = res.failed?.length || 0;
          const detail = _firstBulkIssue(res);
          window._showToast?.(
            `Queued metadata refresh for ${res.queued} channel(s).` +
            (already ? ` ${already} already queued.` : "") +
            (failed ? ` ${failed} could not be queued.` : "") +
            (detail ? ` First issue: ${detail}` : ""),
            failed ? "warn" : "ok");
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
          title: `Remove ${names.length} channel(s)?`,
          message: `You're about to unsubscribe from ${names.length} ` +
                   `channel(s). You can keep their downloaded files in the ` +
                   `archive or move those channel folders to YTArchiver Trash.`,
          choices: [
            { label: "Keep files", value: "keep", primary: true },
            { label: "Move files to Trash", value: "delete", kind: "danger" },
          ],
          cancel: "Cancel",
        }) : null);
        if (!choice) return;
        const deleteFiles = choice === "delete";
        const res = await _bulkBridgeCall(
          "subs_bulk_delete", names, deleteFiles);
        if (!res) return;
        if (res?.ok && res?.started) {
          window._showToast?.(
            `Removing ${names.length} channel(s)… You’ll be notified when it finishes.`,
            "warn");
          clear();
        } else {
          window._showToast?.(res?.error || "Bulk removal did not start.", "error");
        }
      });
  }

  async function _confirmFolderReorg(channel, years, months, label) {
    const ok = await window.askConfirm?.(
      "Organize channel folder",
      `Move the downloaded files in "${channel}" into ${label}?`,
      { confirm: "Organize", cancel: "Cancel" });
    if (!ok) return;
    await checkedAction(
      () => bridgeCall(
        "reorg_channel_folder", { name: channel }, years, months, false),
      {
        success: "Folder organization started.",
        failure: "Folder organization did not start.",
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
    let _subsSeenRenderGeneration = tbody.dataset.renderGeneration || "";
    tbody.addEventListener("click", (e) => {
      const renderGeneration = tbody.dataset.renderGeneration || "";
      if (renderGeneration !== _subsSeenRenderGeneration) {
        _subsSeenRenderGeneration = renderGeneration;
        _subsLastClickedIdx = -1;
      }
      const tr = e.target.closest("tr");
      if (!tr) return;
      // Kebab (⋮) click → open the SAME menu the right-click uses,
      // anchored at the button. This is the visible trigger for the
      // otherwise right-click-only channel actions. stopPropagation so
      // the click doesn't reach document-level outside-click handlers.
      const kebab = e.target.closest(".row-kebab");
      if (kebab) {
        e.preventDefault();
        e.stopPropagation();
        [...tbody.querySelectorAll("tr.row-selected")]
          .forEach(r => r.classList.remove("row-selected"));
        tr.classList.add("row-selected");
        _updateSubsBulkBar();
        const kr = kebab.getBoundingClientRect();
        tr.dispatchEvent(new MouseEvent("contextmenu", {
          bubbles: true, cancelable: true,
          clientX: Math.min(window.innerWidth - 8, kr.left),
          clientY: Math.min(window.innerHeight - 8, kr.bottom),
        }));
        return;
      }
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
      if (e.key === "ContextMenu" || (e.shiftKey && e.key === "F10")) {
        e.preventDefault();
        const r = selected.getBoundingClientRect();
        selected.dispatchEvent(new MouseEvent("contextmenu", {
          bubbles: true,
          cancelable: true,
          clientX: Math.min(window.innerWidth - 8, r.left + 24),
          clientY: Math.min(window.innerHeight - 8, r.top + Math.min(28, r.height || 28)),
        }));
      } else if (e.key === "Enter" || e.key === "F2") {
        e.preventDefault();
        window._editChannelFromContext?.(folder);
      } else if (e.key === "Delete") {
        e.preventDefault();
        await window._removeChannelWithPrompt(folder);
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
        { label: _syncLabel, disabled: _syncDisabled,
          title: _syncDisabled ? "This channel is already in Sync tasks." : "",
          action: async () => {
            if (_syncDisabled) return;
            try {
              const r = await bridgeCall("sync_one_channel", { name: chan });
              window.YT?.bridge?.reportSyncOneResult?.(r, chan);
            } catch (error) {
              window.YT?.bridge?.reportSyncOneResult?.({
                ok: false,
                error: "Sync failed: " + (error?.message || error),
              }, chan);
            }
          }},
        { label: "Edit settings", action: () => window._editChannelFromContext?.(chan) },
        { label: "Open folder", action: () => checkedAction(
          () => bridgeCall("chan_open_folder", chan),
          { failure: "Could not open channel folder." }) },
        { label: "Open URL in browser", action: () => checkedAction(
          () => bridgeCall("chan_open_url", chan),
          { failure: "Could not open the channel URL." }) },
        { sep: true },
        { label: "Reorg folder",
          submenu: [
            { label: "Flat (no split)", action: () =>
              _confirmFolderReorg(chan, false, false, "one folder") },
            { label: "Split by year", action: () =>
              _confirmFolderReorg(chan, true, false, "year folders") },
            { label: "Split by year + month", action: () =>
              _confirmFolderReorg(
                chan, true, true, "year and month folders") },
            { label: "Re-apply organization", action: () =>
              _confirmFolderReorg(
                chan, null, null, "its currently saved folder layout") },
            // Recheck-dates + fix-file-dates are long operations — OLD app
            // shows an all-caps warning dialog. YTArchiver.py:5721-5742.
            { label: "Re-check dates + year/month", action: async () => {
              const ok = await askDanger("Re-check dates",
                `Re-check upload dates for every video in "${chan}" and re-sort into Year/Month folders?\n\n` +
                `This may take several hours on a large channel.`,
                "Re-check dates");
              if (!ok) return;
              await checkedAction(
                () => bridgeCall("reorg_channel_folder", { name: chan }, true, true, true),
                { success: "Date check and reorganization started.", failure: "Date check did not start." });
            }},
            { label: "Fix file dates only", action: async () => {
              const ok = await askDanger("Fix file dates",
                `Re-fetch upload dates from YouTube for every video in "${chan}" ` +
                `and stamp each file's mtime to match?\n\n` +
                `This may take several hours on a large channel.`,
                "Fix dates");
              if (!ok) return;
              await checkedAction(
                () => bridgeCall("chan_fix_file_dates", { name: chan }),
                { success: "File-date repair started.", failure: "File-date repair did not start." });
            }},
            { sep: true },
            // Cancel affordance for the long passes above \u2014 both were
            // previously unstoppable from the UI (audit S4). Helper is
            // defined in browseContextMenus.js and shared here.
            { label: "Cancel running reorg / date fix",
              action: () => window._cancelFolderOps?.() },
          ]},
        // "Fetch channel art" used to live here but the user flagged it as
        // redundant — channel art is fetched automatically as part of the
        // full metadata sweep. Removed to keep the menu focused.
        { sep: true },
        { label: _txLabel, disabled: _txDisabled,
          title: "Transcribe only videos that don't have a transcript yet (YouTube captions first, Whisper fallback)",
          action: () => { if (!_txDisabled) _askTranscribeChannel(chan); }},
        // right-click → re-transcribe entire channel with
        // model selection. Confirms first because this can be a long
        // GPU job (hundreds of videos on large channels).
        { label: "Re-transcribe channel…",
          title: "Redo every video with Whisper, replacing existing transcripts (use this to fix bad/corrupted ones)",
          action: async () => {
          const model = await (window._askWhisperModel?.(`channel "${chan}"`));
          if (!model) return;
          const ok = await askDanger(
            "Re-transcribe entire channel",
            `Queue every video in "${chan}" for re-transcription with `
              + `Whisper ${model}?\n\nThis can take hours on large channels.`,
            "Queue all");
          if (!ok) return;
          const res = await bridgeCall("transcribe_retranscribe_channel",
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
        { label: "Download metadata", action: () => checkedAction(
          () => bridgeCall("metadata_recheck_channel", { name: chan }),
          { success: "Metadata download queued.", failure: "Metadata download was not queued." }) },
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
                const r = await bridgeCall("chan_redownload", { name: chan }, res);
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
        { label: "Remove channel…",
          cls: "danger",
          title: "Stop syncing this channel and choose whether to keep its downloaded files",
          action: () => window._removeChannelWithPrompt(chan) },
      ]);
    });
  }

  window.initColumnSort = initColumnSort;
  window.initSubsContextMenu = initSubsContextMenu;
})();
