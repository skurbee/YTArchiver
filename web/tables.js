/* ═══════════════════════════════════════════════════════════════════════
   tables.js — YTArchiver Subs tab + Recent tab tabular renderers

   Extracted from logs.js (~300 lines). Owns:
     • Subs tab channel table (renderSubsTable, filter, avg-col toggle)
     • Recent tab list view (renderRecentTable's list path)
     • Recent tab grid view (thumbnail cards, reuses _buildVideoCard)
     • View-mode dispatch (list vs. grid) + filter pipeline

   Publishes:
     window.renderSubsTable
     window.renderRecentTable
     window._applySubsFilter
     window._applySubsAvgVisibility
     window._applyRecentFilter
     window._applyRecentViewMode

   Reads:
     window._escapeHtml               — single canonical escapeHtml from util.js
     window._buildVideoCard           — Recent grid borrows the Browse card
     window._openVideoInWatch         — set by app.js, opens Watch view
     window._subsAllRows / _recentAllRows — cached datasets for refilter
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

  const escapeHtml = window._escapeHtml || ((s) => String(s ?? ""));
  const escapeAttr = escapeHtml;

  // ─── Subs tab table ──────────────────────────────────────────────────

  /** Render the Subs tab channel table. */
  window.renderSubsTable = function (rows, totalLabel) {
    const tbody = document.getElementById("subs-table-body");
    if (!tbody) return;
    window._subsAllRows = rows; // keep a copy for the filter
    _renderSubsFiltered(rows);
    const totalEl = document.getElementById("subs-total-size");
    if (totalEl && totalLabel) totalEl.textContent = totalLabel;
    // Re-apply the Avg column visibility in case this is the first render
    // (class would otherwise only be applied on Settings open / change).
    // Default undefined -> show, matching legacy behavior.
    const tbl = document.getElementById("subs-table");
    if (tbl && window._subsShowAvg === false) tbl.classList.add("hide-avg-col");
    // feature F7: the new tbody rows have no row-selected classes, so
    // the bulk-actions bar should hide itself on re-render. Invoke the
    // bar updater if it's been wired.
    const bar = document.getElementById("subs-bulk-bar");
    if (bar) bar.hidden = true;
  };

  function _renderSubsFiltered(rows) {
    const tbody = document.getElementById("subs-table-body");
    if (!tbody) return;
    tbody.innerHTML = "";
    const frag = document.createDocumentFragment();
    for (const r of rows) {
      const tr = document.createElement("tr");
      // Per-channel "Sync now" is accessed via right-click → Sync now
      // (matches the original tkinter version). No inline hover button.
      // A chartreuse dot after the channel name flags an unfinished
      // resolution redownload (`_redownload_progress.json` present).
      const dot = r._pending_redownload
        ? ' <span class="sub-redwnl-dot" title="Unfinished redownload">●</span>'
        : "";
      // Stash pending-redownload metadata on the TR so the right-click
      // menu in app.js can switch "Redownload at..." to "Continue
      // Redownload at 480p" without a backend roundtrip. ALSO stash
      // the clean channel name because `.col-folder` now contains
      // the name PLUS the dot indicator span, so `.textContent`
      // reads "Channel Name ●" instead of just "Channel Name",
      // which made `chan_redownload({name: "Channel Name ●"}, ...)`
      // fail the name lookup silently.
      tr.dataset.channelName = r.folder || "";
      if (r._pending_redownload) {
        tr.dataset.pendingRedownload = "1";
        if (r._redownload_res) tr.dataset.redownloadRes = r._redownload_res;
      }
      // Per-cell tooltips for the mark columns. The header already
      // explains the legend, but cell-level tips give a 1-line spec to
      // the exact value the user is hovering — without these, "A ✓"
      // is opaque to anyone who hasn't read the header carefully.
      // Decode the compact mark shorthand into a plain-English tooltip.
      // Backend (_mark) emits: "A ✓" = auto on + caught up; "✓" = done
      // manually; a trailing " -N" = N videos still waiting; "—" = not
      // enabled. The pending suffix can ride along with ANY prefix
      // ("A ✓ -3" = auto on but 3 behind), so check it first — the old
      // logic reported "fully transcribed" for "A ✓ -N" because it matched
      // the prefix before noticing the -N.
      const _markTip = (label, v) => {
        const s = String(v || "").trim();
        const lc = label.toLowerCase();
        const verb = lc === "metadata" ? "fetched" : "transcribed";
        const behind = parseInt((s.match(/-(\d+)\s*$/) || [])[1] || "0", 10);
        const autoOn = s.startsWith("A ");
        if (behind > 0) {
          return `${label}: ${behind} video${behind === 1 ? "" : "s"} waiting to be ${verb}`
               + (autoOn ? " (auto is on — will catch up next sync)"
                         : ` (auto-${lc} is off)`);
        }
        if (autoOn) return `${label}: auto-${lc} is ON — up to date`;
        if (s.includes("✓")) return `${label}: complete (done manually; auto-${lc} is off)`;
        if (s === "—") return `${label}: not enabled for this channel`;
        return label;
      };
      const _compressTip = (v) => {
        const s = String(v || "").trim();
        if (s.startsWith("✓") || s.startsWith("✓")) return "Compress: ON (videos will be re-encoded to AV1 to save space)";
        return "Compress: OFF";
      };
      tr.innerHTML = `
        <td class="col-folder">${escapeHtml(r.folder)}${dot}</td>
        <td>${escapeHtml(r.res)}</td>
        <td>${escapeHtml(r.min)}</td>
        <td>${escapeHtml(r.max)}</td>
        <td class="col-mark" title="${escapeHtml(_compressTip(r.compress))}">${escapeHtml(r.compress)}</td>
        <td class="col-mark" title="${escapeHtml(_markTip("Transcribe", r.transcribe))}">${escapeHtml(r.transcribe)}</td>
        <td class="col-mark" title="${escapeHtml(_markTip("Metadata", r.metadata))}">${escapeHtml(r.metadata)}</td>
        <td>${escapeHtml(r.last_sync)}</td>
        <td>${escapeHtml(r.n_vids)}</td>
        <td>${escapeHtml(r.size)}</td>
        <td>${escapeHtml(r.avg_size || "—")}</td>
        <td class="col-actions"><button type="button" class="row-kebab" tabindex="-1" title="Channel actions" aria-label="Channel actions">&#8942;</button></td>
      `;
      frag.appendChild(tr);
    }
    tbody.appendChild(frag);
  }

  window._applySubsFilter = function (query) {
    const all = window._subsAllRows || [];
    const q = (query || "").toLowerCase().trim();
    if (!q) { _renderSubsFiltered(all); return; }
    const filtered = all.filter(r => (r.folder || "").toLowerCase().includes(q));
    _renderSubsFiltered(filtered);
  };

  // Toggle visibility of the Avg filesize column on the Subs table.
  // Wired from Settings ("Show Avg filesize in subs tab"). CSS handles
  // the actual hide via `.hide-avg-col th:last-child, .hide-avg-col td:last-child`.
  // Cached on window._subsShowAvg so future renders respect the choice.
  window._applySubsAvgVisibility = function (show) {
    window._subsShowAvg = !!show;
    const tbl = document.getElementById("subs-table");
    if (!tbl) return;
    tbl.classList.toggle("hide-avg-col", !show);
  };

  // ─── Recent tab list + grid ──────────────────────────────────────────

  /** Render the Recent tab downloads. Dispatches between the legacy
   * table ("list") and the thumbnail grid ("grid") based on the
   * user's setting (cached on window._recentViewMode, set by
   * window._applyRecentViewMode — default "list"). */
  window.renderRecentTable = function (rows) {
    window._recentAllRows = rows || [];
    _dispatchRecent(window._recentAllRows);
  };

  window._applyRecentFilter = function (query) {
    const all = window._recentAllRows || [];
    const q = (query || "").toLowerCase().trim();
    const filtered = !q ? all : all.filter(r =>
      (r.title || "").toLowerCase().includes(q) ||
      (r.channel || "").toLowerCase().includes(q)
    );
    // Clear any per-row selection FIRST so the delete button stays
    // in sync with the visible filtered set. Otherwise a selection
    // made before the filter survives into the filtered view but
    // points at a now-hidden / non-existent row (audit: tables.js
    // L94).
    try {
      document.querySelectorAll("#recent-list tr.row-selected, "
        + "#recent-grid .row-selected").forEach(
          el => el.classList.remove("row-selected"));
      const delBtn = document.getElementById("btn-delete-file");
      if (delBtn) delBtn.hidden = true;
    } catch {}
    _dispatchRecent(filtered);
  };

  // Flip which view is visible + pick the matching renderer. Called at
  // boot via the runtime_info seed and live whenever the Settings radio
  // changes. Remembers the choice on window so filter re-applies can
  // dispatch without needing to re-read the setting each time.
  window._applyRecentViewMode = function (mode) {
    const m = (mode === "grid") ? "grid" : "list";
    window._recentViewMode = m;
    const listFrame = document.getElementById("recent-list-frame");
    const gridFrame = document.getElementById("recent-grid-frame");
    if (listFrame) listFrame.style.display = (m === "list") ? "" : "none";
    if (gridFrame) gridFrame.style.display = (m === "grid") ? "" : "none";
    // Re-render the current dataset through the newly-active view so
    // switching modes doesn't leave the other view empty.
    if (Array.isArray(window._recentAllRows)) {
      _dispatchRecent(window._recentAllRows);
    }
  };

  function _dispatchRecent(rows) {
    if (window._recentViewMode === "grid") {
      _renderRecentFilteredGrid(rows);
    } else {
      _renderRecentFiltered(rows);
    }
  }

  function _renderRecentFiltered(rows) {
    const tbody = document.getElementById("recent-table-body");
    if (!tbody) return;
    tbody.innerHTML = "";
    const frag = document.createDocumentFragment();
    for (const r of rows) {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td class="col-title" title="${escapeAttr(r.title)}">${escapeHtml(r.title)}</td>
        <td class="col-channel">${escapeHtml(r.channel)}</td>
        <td class="col-time">${escapeHtml(r.time)}</td>
        <td class="col-length right">${escapeHtml(r.duration)}</td>
        <td class="col-size right">${escapeHtml(r.size)}</td>
      `;
      // Stash the full row data so handlers can reach filepath/video_id/etc.
      tr.dataset.filepath = r.filepath || "";
      tr.dataset.videoId = r.video_id || "";
      tr.dataset.title = r.title || "";
      tr.dataset.channel = r.channel || "";
      tr.title = "Double-click to play in Watch view";
      // Double-click opens the video in the embedded Watch view with transcript
      tr.addEventListener("dblclick", (e) => {
        e.preventDefault();
        const v = {
          title: tr.dataset.title,
          channel: tr.dataset.channel,
          filepath: tr.dataset.filepath,
          video_id: tr.dataset.videoId,
          duration: r.duration || "",
          uploaded: r.time || "",
        };
        // Switch to Browse tab + Watch view (call the helper in app.js).
        if (typeof window._openVideoInWatch === "function") {
          window._openVideoInWatch(v);
        } else if (v.filepath && nativeBridgeUp()) {
          // Fallback — external player
          bridgeCall("browse_open_video", v.filepath);
        }
      });
      frag.appendChild(tr);
    }
    tbody.appendChild(frag);

    // Row click selection — supports Ctrl/Shift multi-select
    const allRows = [...tbody.querySelectorAll("tr")];

    // Show/hide Clear list + Delete File buttons based on rows.
    // Use the [hidden] attribute (audit #6) instead of inline
    // style.display so they respect the global [hidden] rule and
    // don't fight CSS specificity.
    const hasItems = allRows.length > 0;
    const clearBtn = document.getElementById("btn-clear-recent");
    const delBtn = document.getElementById("btn-delete-file");
    if (clearBtn) clearBtn.hidden = !hasItems;
    if (delBtn) delBtn.hidden = true; // revealed when a row is selected

    let lastClickedIdx = -1;
    allRows.forEach((tr, idx) => {
      tr.addEventListener("click", (e) => {
        if (e.ctrlKey || e.metaKey) {
          // Toggle this row's selection
          tr.classList.toggle("row-selected");
        } else if (e.shiftKey && lastClickedIdx >= 0) {
          // Range-select from last clicked to this one
          const [a, b] = [Math.min(lastClickedIdx, idx), Math.max(lastClickedIdx, idx)];
          allRows.forEach((r, i) => {
            if (i >= a && i <= b) r.classList.add("row-selected");
          });
        } else {
          allRows.forEach(r => r.classList.remove("row-selected"));
          tr.classList.add("row-selected");
        }
        lastClickedIdx = idx;
        const any = tbody.querySelectorAll("tr.row-selected").length;
        if (delBtn) {
          delBtn.hidden = !any;
          delBtn.textContent = any > 1 ? `Delete ${any} files` : "Delete File";
        }
      });
    });
  }

  // ─── Recent grid renderer ───────────────────────────────────────────
  //
  // Thumbnail-card view of recent downloads — visually matches the video
  // grid inside a channel (same _buildVideoCard helper). Selection isn't
  // exposed on cards (no multi-select) because the Browse grid doesn't
  // have it either; users who need bulk-delete can flip back to List.
  //
  // Click behavior mirrors the Browse grid:
  // - single-click → open in Watch view (embedded player + transcript)
  // - double-click → open in external player (VLC / system default)
  //
  // The Delete File button depends on selection and is therefore hidden
  // in this mode; Clear list remains functional.
  function _renderRecentFilteredGrid(rows) {
    const grid = document.getElementById("recent-grid");
    if (!grid) return;
    grid.innerHTML = "";
    // _buildVideoCard is on window from browseGrids.js; fall back to
    // a plain-text row if somehow it's missing so the view isn't blank.
    const build = window._buildVideoCard;
    const frag = document.createDocumentFragment();
    for (const r of rows) {
      const v = {
        title: r.title || "",
        channel: r.channel || "",
        filepath: r.filepath || "",
        video_id: r.video_id || "",
        duration: r.duration || "",
        uploaded: r.uploaded || r.time || "",
        size_bytes: r.size_bytes || 0,
        thumbnail_url: r.thumbnail_url || "",
        // Recent is a cross-channel view — force the channel line on so
        // every card identifies its channel (Browse cards don't need it).
        show_channel: true,
      };
      if (!build) {
        const d = document.createElement("div");
        d.className = "video-card";
        d.textContent = v.title;
        frag.appendChild(d);
        continue;
      }
      // onVideoClick → open Watch view (same path the list view uses on dblclick).
      const onClick = (vv) => {
        if (typeof window._openVideoInWatch === "function") {
          window._openVideoInWatch(vv);
        } else if (vv.filepath && nativeBridgeUp()) {
          bridgeCall("browse_open_video", vv.filepath);
        }
      };
      const card = build(v, onClick);
      // Flag untracked (manual single-video) downloads so the right-click
      // menu can hide the channel-only actions (Refresh metadata,
      // Redownload) that hard-fail on loose downloads. Default to tracked
      // when the field is absent (legacy rows) so nothing is hidden
      // unexpectedly.
      card.dataset.tracked = (r.tracked === false) ? "0" : "1";
      frag.appendChild(card);
    }
    grid.appendChild(frag);

    // Button visibility — Clear list follows "has items", Delete File is
    // hidden in grid mode since there's no card-selection UX.
    // Use [hidden] attribute (audit #6) for the same reasons as above.
    const hasItems = rows.length > 0;
    const clearBtn = document.getElementById("btn-clear-recent");
    const delBtn = document.getElementById("btn-delete-file");
    if (clearBtn) clearBtn.hidden = !hasItems;
    if (delBtn) delBtn.hidden = true;
  }
})();
