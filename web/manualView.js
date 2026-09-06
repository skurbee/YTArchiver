/**
 * web/manualView.js — the "Manual" Browse sub-mode.
 *
 * Lists video files from cfg['video_out_dir'] (single-URL downloads),
 * sorted and lazy-loaded, backed by api.list_manual_videos(sort, limit, offset).
 *
 * Public:
 *   window._loadManualView()            — (re)load page 1
 *   window._refreshManualViewIfActive() — re-query page 1 if the view is
 *                                          active; prepend new files without
 *                                          blanking the grid.
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

  const PAGE = 60;
  let _sort = "newest";
  let _query = "";
  let _offset = 0;
  let _loading = false;
  let _hasMore = true;
  let _seq = 0;
  let _wired = false;
  let _firstPageSig = "";
  // Background metadata can finish before the bridge promise that started it
  // resolves. Keep those early completions so the fresh render cannot erase
  // them when it replaces the cached/skeleton cards.
  const _pendingThumbPatches = new Map();
  const _pendingDurationPatches = new Map();

  const $ = (id) => document.getElementById(id);
  const grid = () => $("manual-grid");

  function isActive() {
    const v = $("view-manual");
    return !!(v && !v.hidden && v.offsetParent !== null);
  }

  function _fmtSize(bytes) {
    if (!bytes) return "";
    if (bytes >= 1073741824) return (bytes / 1073741824).toFixed(1) + " GB";
    if (bytes >= 1048576) return (bytes / 1048576).toFixed(0) + " MB";
    return (bytes / 1024).toFixed(0) + " KB";
  }

  function _setManualBulkStatus(text, kind = "active") {
    const el = $("manual-bulk-status");
    if (!el) return;
    const msg = String(text || "").trim();
    if (!msg) {
      el.hidden = true;
      el.textContent = "";
      el.title = "";
      return;
    }
    el.hidden = false;
    el.className = `manual-bulk-status manual-bulk-status-${kind}`;
    el.textContent = msg;
    el.title = msg;
  }

  function _shortStatusTitle(title) {
    const s = String(title || "").replace(/\s+/g, " ").trim();
    if (!s) return "";
    return s.length > 54 ? `${s.slice(0, 51)}...` : s;
  }

  function _fmtInt(value) {
    const n = Number(value || 0);
    return Number.isFinite(n) ? n.toLocaleString() : "0";
  }

  function _fmtPct(value) {
    const n = Number(value || 0);
    if (!Number.isFinite(n)) return "0%";
    return `${n >= 99.95 || Math.abs(n - Math.round(n)) < 0.05
      ? Math.round(n)
      : n.toFixed(1)}%`;
  }

  async function _manualBulkSummary() {
    try {
      const res = await bridgeCall("manual_bulk_action_summary");
      if (res?.ok) return res;
      window._showToast?.(res?.error || "Could not count manual downloads.", "warn");
    } catch (_e) {
      window._showToast?.("Could not count manual downloads.", "warn");
    }
    return null;
  }

  async function _manualBulkSummaryWithFeedback(button) {
    const oldText = button?.textContent || "";
    const wasDisabled = !!button?.disabled;
    if (button) {
      button.disabled = true;
      button.textContent = "Checking\u2026";
      button.setAttribute("aria-busy", "true");
    }
    try {
      return await _manualBulkSummary();
    } finally {
      if (button) {
        button.textContent = oldText;
        button.disabled = wasDisabled;
        button.removeAttribute("aria-busy");
      }
    }
  }

  async function _confirmRecoverIds(s) {
    if (!s) return false;
    if (!s.total) {
      window._showToast?.("No manual downloads found.", "warn");
      return false;
    }
    if (!s.recover_eligible) {
      const msg = s.recover_excluded
        ? `No missing-ID videos are eligible. ${_fmtInt(s.recover_excluded)} are excluded after repeated failed recovery attempts.`
        : "Every manual download already has a video ID.";
      window._showToast?.(msg, "ok");
      _setManualBulkStatus(msg, "ok");
      return false;
    }
    const lines = [
      `${_fmtInt(s.with_id)} of ${_fmtInt(s.total)} manual downloads already have IDs (${_fmtPct(s.percent_with_id)}).`,
      `Recover IDs will check ${_fmtInt(s.recover_eligible)} missing-ID video(s).`,
    ];
    if (s.recover_excluded) {
      lines.push(`${_fmtInt(s.recover_excluded)} no-ID video(s) are excluded after repeated failed searches.`);
    }
    if (s.recover_tried) {
      lines.push(`${_fmtInt(s.recover_tried)} eligible video(s) have been tried before but are not excluded yet.`);
    }
    return window.askConfirm
      ? await window.askConfirm("Recover manual video IDs",
          lines.join("\n"), { confirm: "Recover IDs" })
      : true;
  }

  async function _confirmRefreshMetadata(s) {
    if (!s) return false;
    if (!s.total) {
      window._showToast?.("No manual downloads found.", "warn");
      return false;
    }
    if (!s.metadata_eligible) {
      const msg = "No manual downloads have video IDs, so metadata cannot be refreshed yet.";
      window._showToast?.(msg, "warn");
      _setManualBulkStatus(msg, "warn");
      return false;
    }
    const lines = [
      `Refresh YouTube metadata for ${_fmtInt(s.metadata_eligible)} manual download(s) with IDs.`,
    ];
    if (s.metadata_skipped_no_id) {
      lines.push(`${_fmtInt(s.metadata_skipped_no_id)} no-ID video(s) will be skipped.`);
    }
    return window.askConfirm
      ? await window.askConfirm("Refresh manual metadata",
          lines.join("\n"), { confirm: "Refresh metadata" })
      : true;
  }

  async function _confirmTranscribeMissing(s) {
    if (!s) return false;
    if (!s.total) {
      window._showToast?.("No manual downloads found.", "warn");
      return false;
    }
    if (!s.transcribe_eligible) {
      const msg = "Every manual download is already transcribed or marked no-speech.";
      window._showToast?.(msg, "ok");
      _setManualBulkStatus(msg, "ok");
      return false;
    }
    const lines = [
      `Queue transcription for ${_fmtInt(s.transcribe_eligible)} manual download(s).`,
      `${_fmtInt(s.transcribe_skipped)} already transcribed/no-speech video(s) will be skipped.`,
    ];
    return window.askConfirm
      ? await window.askConfirm("Transcribe missing manual downloads",
          lines.join("\n"), { confirm: "Transcribe missing" })
      : true;
  }

  function _manualMetaStatusText(summary) {
    const total = Number(summary?.total || 0);
    const current = Number(summary?.current || 0);
    const refreshed = Number(summary?.refreshed || 0);
    const skipped = Number(summary?.skipped_no_id || 0);
    const failed = Number(summary?.failed || 0);
    const prefix = total > 0
      ? `Metadata ${Math.min(current, total)}/${total}`
      : "Metadata scanning";
    const counts = `${refreshed} updated, ${skipped} no ID, ${failed} failed`;
    const title = _shortStatusTitle(summary?.title);
    if (summary?.phase === "fetching" && title)
      return `${prefix} - fetching: ${title} - ${counts}`;
    if (summary?.phase === "skipping" && title)
      return `${prefix} - skipping no-ID: ${title} - ${counts}`;
    return `${prefix} - ${counts}`;
  }

  function _manualTxStatusText(summary) {
    const total = Number(summary?.candidate_total || summary?.total || 0);
    const current = Number(summary?.current || 0);
    const queued = Number(summary?.queued || 0);
    const skipped = Number(summary?.skipped || 0);
    const failed = Number(summary?.failed || 0);
    const prefix = total > 0
      ? `Transcribe ${Math.min(current, total)}/${total}`
      : "Transcribe queue";
    const title = _shortStatusTitle(summary?.title);
    const counts = `${queued} queued, ${skipped} skipped, ${failed} failed`;
    return title ? `${prefix} - ${title} - ${counts}` : `${prefix} - ${counts}`;
  }

  function _uploadTsMs(value) {
    const n = Number(value || 0);
    if (!Number.isFinite(n) || n <= 0) return null;
    return n < 1000000000000 ? n * 1000 : n;
  }

  function _pageSig(rows) {
    return (rows || []).map(r => [
      r.filepath || "",
      r.thumbnail_url || "",
      r.thumbnail_source || "",
      r.duration || "",
      r.tx_status || "",
      JSON.stringify(r.manual_badges || []),
    ].join("~")).join("|");
  }

  function _decorateManualCard(card, r) {
    if (!card) return card;
    card.dataset.manual = "1";
    const badges = Array.isArray(r.manual_badges) ? r.manual_badges : [];
    if (!badges.length) return card;
    const thumb = card.querySelector(".video-thumb");
    if (!thumb) return card;
    let wrap = thumb.querySelector(".manual-card-badges");
    if (!wrap) {
      wrap = document.createElement("div");
      wrap.className = "manual-card-badges";
      thumb.appendChild(wrap);
    }
    wrap.innerHTML = "";
    for (const badge of badges.slice(0, 3)) {
      const pill = document.createElement("span");
      pill.className = "manual-card-badge manual-card-badge-" + (badge.kind || "neutral");
      pill.textContent = badge.label || "";
      wrap.appendChild(pill);
    }
    return card;
  }

  function _removeManualPlayPlaceholder(thumb) {
    for (const child of Array.from(thumb.children)) {
      if (child.tagName === "SPAN" && !child.className
          && child.textContent.trim() === "\u25b6") {
        child.remove();
      }
    }
  }

  window._manualThumbsReady = function (items) {
    const ready = Array.isArray(items) ? items : [];
    if (!ready.length) return;
    const cards = Array.from(
      document.querySelectorAll('.video-card[data-manual="1"]'));
    for (const item of ready) {
      const fp = item?.filepath || "";
      const url = item?.thumbnail_url || "";
      if (!fp || !url) continue;
      const patch = {
        thumbnail_url: url,
        thumbnail_source: item?.thumbnail_source || "",
      };
      if (_loading) _pendingThumbPatches.set(fp, patch);
      _patchCachedPage1(fp, patch);
      for (const card of cards) {
        if ((card.dataset.filepath || "") !== fp) continue;
        const thumb = card.querySelector(".video-thumb");
        if (!thumb) continue;
        thumb.querySelector(".video-thumb-img")?.remove();
        const img = document.createElement("img");
        img.className = "video-thumb-img";
        img.alt = "";
        // These callbacks are specifically for cards already rendered in the
        // Manual grid. Marking the replacement image lazy can leave a visible
        // card parked in Chromium's lazy queue until a tab/sort/scroll forces
        // another intersection pass.
        img.loading = "eager";
        img.fetchPriority = "high";
        img.decoding = "async";
        img.addEventListener("load", () => {
          thumb.style.background = "";
          _removeManualPlayPlaceholder(thumb);
        }, { once: true });
        img.addEventListener("error", () => img.remove(), { once: true });
        img.src = url;
        thumb.insertBefore(img, thumb.firstChild);
      }
    }
  };

  window._manualDurationsReady = function (items) {
    const ready = Array.isArray(items) ? items : [];
    if (!ready.length) return;
    const cards = Array.from(
      document.querySelectorAll('.video-card[data-manual="1"]'));
    for (const item of ready) {
      const fp = item?.filepath || "";
      const duration = item?.duration || "";
      if (!fp || !duration) continue;
      if (_loading) _pendingDurationPatches.set(fp, { duration });
      _patchCachedPage1(fp, { duration });
      for (const card of cards) {
        if ((card.dataset.filepath || "") !== fp) continue;
        const badge = card.querySelector(".video-duration-badge");
        if (badge) badge.textContent = duration;
      }
    }
  };

  function _cardFor(r) {
    // Try the shared card builder first (gives thumbnail + channel line).
    // Falls back to a simple text card if the builder isn't available.
    // Rows now come from the index (rich: channel/video_id/thumbnail/date)
    // with a folder-walk fallback that only fills title/size/path.
    const build = window._buildVideoCard;
    if (build) {
      const v = {
        title: r.title || r.filepath || "",
        channel: r.channel || "",
        filepath: r.filepath || "",
        video_id: r.video_id || "",
        duration: r.duration || "",
        uploaded: r.uploaded || "",
        upload_ts: _uploadTsMs(r.upload_ts),
        size_bytes: r.size_bytes || 0,
        views: r.views || "",
        view_count: (r.view_count != null) ? r.view_count : null,
        thumbnail_url: r.thumbnail_url || "",
        // Manual renders at most one 60-card page at a time and all URLs are
        // local. Eager loading prevents visible cards from being stranded in
        // Chromium's lazy-image scheduler after a sort or tab change.
        eager_thumbnail: true,
        tx_status: r.tx_status || "",
        removed_from_yt: !!r.removed_from_yt,
        show_channel: true,
        tracked: false,
      };
      const onClick = (vv) => {
        if (typeof window._openVideoInWatch === "function")
          window._openVideoInWatch(vv);
        else if (vv.filepath) window._openVideoExternally?.(vv.filepath);
      };
      const card = build(v, onClick);
      if (card) {
        card.dataset.tracked = "0";
        return _decorateManualCard(card, r);
      }
    }
    // Fallback card
    const el = document.createElement("div");
    el.className = "video-card";
    el.style.cssText = "padding:8px;cursor:pointer;";
    el.setAttribute("role", "button");
    el.tabIndex = 0;
    el.setAttribute("aria-haspopup", "menu");
    el.setAttribute("aria-expanded", "false");
    el.setAttribute("aria-keyshortcuts", "Shift+F10");
    const nameEl = document.createElement("div");
    nameEl.className = "video-card-title";
    nameEl.textContent = r.title || r.filepath || "(untitled)";
    el.setAttribute("aria-label", nameEl.textContent);
    el.appendChild(nameEl);
    if (r.size_bytes) {
      const sz = document.createElement("div");
      sz.style.cssText = "font-size:var(--fs-xs);color:var(--text-dim);margin-top:4px;";
      sz.textContent = _fmtSize(r.size_bytes);
      el.appendChild(sz);
    }
    el.addEventListener("click", () => {
      if (r.filepath) window._openVideoExternally?.(r.filepath);
    });
    el.addEventListener("keydown", (event) => {
      if (event.key !== "Enter" && event.key !== " ") return;
      event.preventDefault();
      el.click();
    });
    el.dataset.tracked = "0";
    return _decorateManualCard(el, r);
  }

  // ── Instant-paint cache (perceived performance) ──────────────────────
  // First open of this view used to show a bare "Loading…" spinner for the
  // whole ~query time. Now the first page's rows are remembered (in memory
  // + localStorage across sessions) and painted instantly; the fresh query
  // replaces them in place. With no cache yet, shaped skeletons stand in.
  const _CACHE_KEY = "ytarchiver_manual_page1";
  let _cachedPage1 = null;
  function _loadCachedPage1() {
    if (_cachedPage1) return _cachedPage1;
    try {
      const raw = localStorage.getItem(_CACHE_KEY);
      const arr = raw ? JSON.parse(raw) : null;
      if (Array.isArray(arr) && arr.length) { _cachedPage1 = arr; return arr; }
    } catch (e) { /* unavailable / corrupt */ }
    return null;
  }
  function _saveCachedPage1(rows) {
    _cachedPage1 = rows;
    try {
      // Local thumbnail URLs contain the current fileserver's random port and
      // session token. Persisting them made the next app launch paint dead
      // image URLs; keep the URLs in the in-memory cache only.
      const persisted = rows.slice(0, 24).map(r => ({
        ...r,
        thumbnail_url: "",
      }));
      localStorage.setItem(_CACHE_KEY, JSON.stringify(persisted));
    }
    catch (e) { /* quota / unavailable */ }
  }
  function _patchCachedPage1(filepath, patch) {
    if (!filepath || !patch) return;
    const rows = _loadCachedPage1();
    if (!rows) return;
    const row = rows.find(r => (r?.filepath || "") === filepath);
    if (!row) return;
    Object.assign(row, patch);
    _saveCachedPage1(rows);
  }
  function _skeletonHtml(n) {
    let s = "";
    for (let i = 0; i < n; i++) {
      s += '<div class="video-card skeleton-card" aria-hidden="true">'
         + '<div class="video-thumb skeleton-box"></div>'
         + '<div class="video-card-body">'
         + '<div class="skeleton-line skeleton-box"></div>'
         + '<div class="skeleton-line skeleton-box short"></div>'
         + '</div></div>';
    }
    return s;
  }
  function _paintRows(g, rows) {
    if (!g) return;
    g.innerHTML = "";
    const frag = document.createDocumentFragment();
    for (const r of rows) { const c = _cardFor(r); if (c) frag.appendChild(c); }
    g.appendChild(frag);
  }

  function _mergeEarlyBackgroundPatches(rows) {
    for (const row of rows || []) {
      const fp = row?.filepath || "";
      if (!fp) continue;
      const thumb = _pendingThumbPatches.get(fp);
      if (thumb) {
        Object.assign(row, thumb);
        _pendingThumbPatches.delete(fp);
      }
      const duration = _pendingDurationPatches.get(fp);
      if (duration) {
        Object.assign(row, duration);
        _pendingDurationPatches.delete(fp);
      }
    }
  }

  function _paintManualCatalogStatus(status, reset) {
    const g = grid();
    if (!g) return;
    if (!reset) {
      const label = $("manual-load-more")?.querySelector(".grid-loading-label");
      if (label && status.phase !== "done") label.textContent = status.text;
      return;
    }
    let note = $("manual-catalog-status");
    if (status.phase === "done") {
      note?.remove();
      return;
    }
    if (!note) {
      note = document.createElement("div");
      note.id = "manual-catalog-status";
      note.className = "catalog-read-status";
      note.setAttribute("role", "status");
      note.setAttribute("aria-live", "polite");
      g.prepend(note);
    }
    note.setAttribute("aria-live", status.announce === false ? "off" : "polite");
    note.textContent = status.text;
  }

  async function loadPage(reset) {
    if (!nativeBridgeUp()) return;
    _loading = true;
    const myId = ++_seq;
    if (reset) { _offset = 0; _hasMore = true; }
    const sortAtCall = _sort;
    const queryAtCall = _query;
    const g = grid();
    const moreEl = $("manual-load-more");
    if (reset && g) {
      // Instant paint instead of a bare spinner: last-known cards (memory
      // or last session) if any, else shaped skeletons. Fresh data below
      // replaces this the moment the query returns.
      const cached = queryAtCall ? null : _loadCachedPage1();
      if (cached && cached.length) { _paintRows(g, cached); g.classList.add("is-refreshing"); }
      else { g.innerHTML = _skeletonHtml(8); }
    } else if (moreEl) { moreEl.hidden = false; }
    const pageOffset = _offset;
    try {
      const outcome = await window.YT.bridge.catalogRead(
        "manual",
        () => bridgeCall("list_manual_videos", sortAtCall, PAGE, pageOffset, queryAtCall),
        {
          label: "manual downloads",
          onStatus: (status) => _paintManualCatalogStatus(status, reset),
        });
      if (outcome.stale || myId !== _seq) return false;
      const res = outcome.value;
      if (res?.error) throw new Error(res.error);
      const rows = (res && res.rows) || [];
      _mergeEarlyBackgroundPatches(rows);
      if (reset) {
        _firstPageSig = _pageSig(rows);
        if (!queryAtCall) _saveCachedPage1(rows);
        if (g) g.classList.remove("is-refreshing");
      }
      if (reset && g) g.innerHTML = "";
      const frag = document.createDocumentFragment();
      for (const r of rows) { const c = _cardFor(r); if (c) frag.appendChild(c); }
      if (g) g.appendChild(frag);
      _offset += rows.length;
      _hasMore = !!(res && res.has_more);

      // Update folder label
      const lbl = $("manual-folder-label");
      if (lbl && res?.folder) {
        const n = (res.total != null) ? (queryAtCall
          ? ` (${res.total} of ${res.unfiltered_total ?? res.total})` : ` (${res.total})`) : "";
        lbl.textContent = `Manual downloads${n} — ${res.folder}`;
      }

      if (reset && g && _offset === 0) {
        const folder = res?.folder || "";
        g.innerHTML = folder
          ? `<div class="browse-empty">No video files found in<br><code>${
              folder.replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]))
            }</code>.</div>`
          : '<div class="browse-empty">Set an Individual video folder in Settings &gt; Storage &amp; library to see manual downloads here.</div>';
        if (queryAtCall) {
          const empty = document.createElement("div");
          empty.className = "browse-empty";
          empty.textContent = `No manual downloads match "${queryAtCall}".`;
          g.replaceChildren(empty);
        }
      }
      if (res?.warning) {
        window._showToast?.(res.warning, "warn");
      }
      return true;
    } catch (e) {
      console.error("[manual] load failed", e);
      if (myId === _seq && reset && g) {
        g.classList.remove("is-refreshing");
        g.innerHTML = "";
        const error = document.createElement("div");
        error.className = "browse-empty";
        error.textContent = `Couldn’t load files. ${e?.message || e}`;
        g.appendChild(error);
      }
      return false;
    } finally {
      if (myId === _seq) {
        _loading = false;
        if (moreEl) moreEl.hidden = true;
      }
    }
  }

  function _nearBottom(el) {
    return window.YT.util.nearScrollBottom(el);
  }

  let _scrollRaf = null;
  function onScroll() {
    if (_scrollRaf) return;
    _scrollRaf = requestAnimationFrame(() => {
      _scrollRaf = null;
      if (!isActive() || !_hasMore || _loading) return;
      // Like Videos, Manual can scroll on either the inner frame or the
      // outer .browse-view depending on the current layout. Listen/check both
      // so reaching the bottom always requests the next page.
      if (_nearBottom($("manual-grid-frame"))
          || _nearBottom($("view-manual"))
          || _nearBottom(document.scrollingElement || document.documentElement)) {
        loadPage(false);
      }
    });
  }

  function wireOnce() {
    if (_wired) return;
    _wired = true;
    let filterTimer = null;
    $("manual-filter")?.addEventListener("input", (event) => {
      _query = event.target.value.trim();
      ++_seq;
      clearTimeout(filterTimer);
      filterTimer = setTimeout(() => loadPage(true), 200);
    });
    $("manual-sort")?.addEventListener("change", (e) => {
      _sort = e.target.value || "newest";
      loadPage(true);
    });
    const recBtn = $("manual-recover-ids");
    if (recBtn) {
      recBtn.addEventListener("click", async () => {
        if (!nativeBridgeUp()) return;
        if (recBtn.dataset.running === "1") {          // toggle -> cancel
          recBtn.dataset.running = "0";
          recBtn.textContent = "Recover IDs";
          try { await bridgeCall("manual_backfill_ids_cancel"); } catch (_e) {}
          window._showToast?.("Stopping ID recovery...", "warn");
          return;
        }
        if (recBtn.dataset.confirming === "1") return;
        recBtn.dataset.confirming = "1";
        try {
          const summary = await _manualBulkSummaryWithFeedback(recBtn);
          const proceed = await _confirmRecoverIds(summary);
          if (!proceed) return;
          // Real run: resolves IDs, registers them in the index, and pulls
          // metadata. Confident matches are written; ambiguous ones go to the
          // review picker.
          const res = await bridgeCall("manual_backfill_ids", false);
          if (res && res.ok && res.started) {
            recBtn.dataset.running = "1";
            recBtn.textContent = "Stop";
            window._showToast?.("Recovering IDs + metadata (writing to your library) - watch the activity log.", "ok");
          } else {
            window._showToast?.((res && res.error) || "Couldn't start.", "warn");
          }
        } catch (_e) {
          window._showToast?.("Couldn't start ID recovery.", "error");
        } finally {
          recBtn.dataset.confirming = "0";
        }
      });
    }
    const metaBtn = $("manual-refresh-all-metadata");
    if (metaBtn) {
      metaBtn.addEventListener("click", async () => {
        if (!nativeBridgeUp() || metaBtn.dataset.running === "1") return;
        if (metaBtn.dataset.confirming === "1") return;
        metaBtn.dataset.confirming = "1";
        let proceed = false;
        try {
          const summary = await _manualBulkSummaryWithFeedback(metaBtn);
          proceed = await _confirmRefreshMetadata(summary);
        } finally {
          metaBtn.dataset.confirming = "0";
        }
        if (!proceed) return;
        metaBtn.dataset.running = "1";
        metaBtn.disabled = true;
        metaBtn.textContent = "Refreshing...";
        _setManualBulkStatus("Metadata refresh starting...", "active");
        try {
          const res = await bridgeCall("manual_refresh_all_metadata");
          if (res && res.ok && res.started) {
            window._showToast?.("Refreshing manual metadata.", "ok");
          } else {
            metaBtn.dataset.running = "0";
            metaBtn.disabled = false;
            metaBtn.textContent = "Refresh metadata";
            _setManualBulkStatus((res && res.error) || "Couldn't start metadata refresh.", "error");
            window._showToast?.((res && res.error) || "Couldn't start metadata refresh.", "warn");
          }
        } catch (_e) {
          metaBtn.dataset.running = "0";
          metaBtn.disabled = false;
          metaBtn.textContent = "Refresh metadata";
          _setManualBulkStatus("Couldn't start metadata refresh.", "error");
          window._showToast?.("Couldn't start metadata refresh.", "error");
        }
      });
    }
    const txBtn = $("manual-transcribe-all");
    if (txBtn) {
      txBtn.addEventListener("click", async () => {
        if (!nativeBridgeUp() || txBtn.dataset.running === "1") return;
        if (txBtn.dataset.confirming === "1") return;
        txBtn.dataset.confirming = "1";
        let proceed = false;
        try {
          const summary = await _manualBulkSummaryWithFeedback(txBtn);
          proceed = await _confirmTranscribeMissing(summary);
        } finally {
          txBtn.dataset.confirming = "0";
        }
        if (!proceed) return;
        const model = await (window._askWhisperModel?.("manual downloads"));
        if (model === null) return;
        txBtn.dataset.running = "1";
        txBtn.disabled = true;
        txBtn.textContent = "Queueing...";
        _setManualBulkStatus("Transcription queue starting...", "active");
        try {
          const res = await bridgeCall("manual_transcribe_all", model || "");
          if (res && res.ok && res.started) {
            window._showToast?.("Queueing manual transcriptions.", "ok");
          } else {
            txBtn.dataset.running = "0";
            txBtn.disabled = false;
            txBtn.textContent = "Transcribe missing";
            _setManualBulkStatus((res && res.error) || "Couldn't queue transcriptions.", "error");
            window._showToast?.((res && res.error) || "Couldn't queue transcriptions.", "warn");
          }
        } catch (_e) {
          txBtn.dataset.running = "0";
          txBtn.disabled = false;
          txBtn.textContent = "Transcribe missing";
          _setManualBulkStatus("Couldn't queue transcriptions.", "error");
          window._showToast?.("Couldn't queue transcriptions.", "error");
        }
      });
    }
    $("manual-review-btn")?.addEventListener("click", () => {
      if (typeof window._openManualReview === "function") window._openManualReview();
    });
    $("manual-grid-frame")?.addEventListener("scroll", onScroll, { passive: true });
    $("view-manual")?.addEventListener("scroll", onScroll, { passive: true });
    window.addEventListener("scroll", onScroll, { passive: true });
  }

  window._loadManualView = async function () {
    wireOnce();
    const loaded = await loadPage(true);
    if (loaded && isActive()
        && typeof window._refreshManualReviewCount === "function")
      await window._refreshManualReviewCount();
  };

  // Reset the Recover-IDs button when the backend run finishes (the backend
  // calls this via evaluate_js in its finally block) + refresh the review pile.
  window._manualBackfillDone = async function (summary) {
    const b = $("manual-recover-ids");
    if (b) { b.dataset.running = "0"; b.textContent = "Recover IDs"; }
    let n = Number(summary?.review || 0);
    if (typeof window._refreshManualReviewCount === "function") {
      const count = await window._refreshManualReviewCount();
      if (Number.isFinite(count)) n = count;
    }
    if (n > 0 && typeof window._openManualReview === "function") {
      setTimeout(() => window._openManualReview(), 120);
    }
  };

  window._manualRefreshAllProgress = function (summary) {
    _setManualBulkStatus(
      _manualMetaStatusText(summary),
      (summary?.failed || 0) ? "warn" : "active");
  };

  window._manualRefreshAllDone = function (summary) {
    const b = $("manual-refresh-all-metadata");
    if (b) {
      b.dataset.running = "0";
      b.disabled = false;
      b.textContent = "Refresh metadata";
    }
    if (summary?.ok) {
      _setManualBulkStatus(
        `Metadata done - ${summary.refreshed || 0} updated, ${summary.skipped_no_id || 0} without ID, ${summary.failed || 0} failed.`,
        (summary.failed || 0) ? "warn" : "ok");
      window._showToast?.(
        `Manual metadata refreshed: ${summary.refreshed || 0} updated, ${summary.skipped_no_id || 0} without ID, ${summary.failed || 0} failed.`,
        (summary.failed || 0) ? "warn" : "ok");
      window._refreshManualViewIfActive?.();
    } else {
      _setManualBulkStatus(summary?.error || "Metadata refresh failed.", "error");
      window._showToast?.(summary?.error || "Metadata refresh failed.", "error");
    }
  };

  window._manualTranscribeAllProgress = function (summary) {
    _setManualBulkStatus(
      _manualTxStatusText(summary),
      (summary?.failed || 0) ? "warn" : "active");
  };

  window._manualTranscribeAllQueued = function (summary) {
    const b = $("manual-transcribe-all");
    if (b) {
      b.dataset.running = "0";
      b.disabled = false;
      b.textContent = "Transcribe missing";
    }
    if (summary?.ok) {
      _setManualBulkStatus(
        `Transcribe queued - ${summary.queued || 0} queued, ${summary.skipped || 0} skipped, ${summary.failed || 0} failed.`,
        (summary.failed || 0) ? "warn" : "ok");
      window._showToast?.(
        `Manual transcription queued: ${summary.queued || 0} queued, ${summary.skipped || 0} skipped, ${summary.failed || 0} failed.`,
        (summary.failed || 0) ? "warn" : "ok");
      window._refreshManualViewIfActive?.();
    } else {
      _setManualBulkStatus(summary?.error || "Transcribe queue failed.", "error");
      window._showToast?.(summary?.error || "Transcribe queue failed.", "error");
    }
  };

  // Show/hide the "Review matches (N)" button based on the saved review pile.
  window._refreshManualReviewCount = async function () {
    const b = $("manual-review-btn");
    if (!nativeBridgeUp()) return 0;
    try {
      const res = await bridgeCall("manual_backfill_review_list");
      const n = (res && res.items) ? res.items.length : 0;
      if (b) {
        if (n > 0) { b.hidden = false; b.textContent = `Review matches (${n})`; }
        else { b.hidden = true; }
      }
      return n;
    } catch (_e) { return 0; }
  };

  window._refreshManualViewIfActive = async function () {
    if (!isActive() || _loading) return;
    if (!nativeBridgeUp()) return;
    const sortAtCall = _sort;
    const queryAtCall = _query;
    try {
      const outcome = await window.YT.bridge.catalogRead(
        "manual",
        () => bridgeCall("list_manual_videos", sortAtCall, PAGE, 0, queryAtCall),
        { label: "manual downloads" });
      if (outcome.stale) return;
      const res = outcome.value;
      if (res?.error) throw new Error(res.error);
      if (sortAtCall !== _sort || queryAtCall !== _query || _loading) return;
      const rows = (res && res.rows) || [];
      const newSig = _pageSig(rows);
      if (newSig === _firstPageSig) return;
      if (sortAtCall === "newest" && _firstPageSig) {
        const oldFirst = _firstPageSig.split("|")[0].split("~")[0];
        const splitIdx = rows.findIndex(r => (r.filepath || "") === oldFirst);
        if (splitIdx > 0) {
          const g = grid();
          if (g) {
            const frag = document.createDocumentFragment();
            const existing = new Map([...g.querySelectorAll(".video-card")]
              .map(card => [card.dataset.filepath, card]));
            let added = 0;
            for (let i = 0; i < splitIdx; i++) {
              const c = _cardFor(rows[i]);
              if (c) {
                const previous = existing.get(rows[i].filepath);
                if (previous) previous.remove();
                else added++;
                existing.set(rows[i].filepath, c);
                frag.appendChild(c);
              }
            }
            g.insertBefore(frag, g.firstChild);
            _offset += added;
            _firstPageSig = newSig;
            return;
          }
        }
      }
      loadPage(true);
    } catch (_e) { /* non-fatal */ }
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", wireOnce);
  } else {
    wireOnce();
  }
})();
