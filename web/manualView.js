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
  let _offset = 0;
  let _loading = false;
  let _hasMore = true;
  let _seq = 0;
  let _wired = false;
  let _firstPageSig = "";

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

  async function _confirmRecoverIds() {
    const s = await _manualBulkSummary();
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

  async function _confirmRefreshMetadata() {
    const s = await _manualBulkSummary();
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

  async function _confirmTranscribeMissing() {
    const s = await _manualBulkSummary();
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

  function _addManualLocalThumbBadge(card) {
    const thumb = card?.querySelector(".video-thumb");
    if (!thumb) return;
    let wrap = thumb.querySelector(".manual-card-badges");
    if (!wrap) {
      wrap = document.createElement("div");
      wrap.className = "manual-card-badges";
      thumb.appendChild(wrap);
    }
    const exists = Array.from(wrap.children).some(
      el => (el.textContent || "").trim().toLowerCase() === "local thumb");
    if (exists) return;
    const pill = document.createElement("span");
    pill.className = "manual-card-badge manual-card-badge-neutral";
    pill.textContent = "Local thumb";
    wrap.appendChild(pill);
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
      for (const card of cards) {
        if ((card.dataset.filepath || "") !== fp) continue;
        const thumb = card.querySelector(".video-thumb");
        if (!thumb) continue;
        thumb.querySelector(".video-thumb-img")?.remove();
        const img = document.createElement("img");
        img.className = "video-thumb-img";
        img.alt = "";
        img.loading = "lazy";
        img.decoding = "async";
        img.addEventListener("load", () => {
          thumb.style.background = "";
          _removeManualPlayPlaceholder(thumb);
          _addManualLocalThumbBadge(card);
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
        tx_status: r.tx_status || "",
        removed_from_yt: !!r.removed_from_yt,
        show_channel: true,
      };
      const onClick = (vv) => {
        if (typeof window._openVideoInWatch === "function")
          window._openVideoInWatch(vv);
        else if (vv.filepath && nativeBridgeUp())
          bridgeCall("browse_open_video", vv.filepath);
      };
      const card = build(v, onClick);
      if (card) {
        card.dataset.tracked = "1";
        return _decorateManualCard(card, r);
      }
    }
    // Fallback card
    const el = document.createElement("div");
    el.className = "video-card";
    el.style.cssText = "padding:8px;cursor:pointer;";
    const nameEl = document.createElement("div");
    nameEl.className = "video-card-title";
    nameEl.textContent = r.title || r.filepath || "(untitled)";
    el.appendChild(nameEl);
    if (r.size_bytes) {
      const sz = document.createElement("div");
      sz.style.cssText = "font-size:11px;color:var(--text-dim);margin-top:4px;";
      sz.textContent = _fmtSize(r.size_bytes);
      el.appendChild(sz);
    }
    el.addEventListener("click", () => {
      if (r.filepath && nativeBridgeUp())
        bridgeCall("browse_open_video", r.filepath);
    });
    return _decorateManualCard(el, r);
  }

  async function loadPage(reset) {
    if (!nativeBridgeUp()) return;
    if (_loading) return;
    _loading = true;
    const myId = ++_seq;
    if (reset) { _offset = 0; _hasMore = true; }
    const g = grid();
    const moreEl = $("manual-load-more");
    if (reset && g) {
      g.innerHTML = '<div class="grid-loading"><div class="grid-spinner"></div>'
        + '<span class="grid-loading-label">Loading…</span></div>';
    } else if (moreEl) { moreEl.hidden = false; }
    try {
      const res = await bridgeCall("list_manual_videos", _sort, PAGE, _offset);
      if (myId !== _seq) return;
      const rows = (res && res.rows) || [];
      if (reset) {
        _firstPageSig = _pageSig(rows);
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
        const n = (res.total != null) ? ` (${res.total})` : "";
        lbl.textContent = `Manual downloads${n} — ${res.folder}`;
      }

      if (reset && g && _offset === 0) {
        const folder = res?.folder || "";
        g.innerHTML = folder
          ? `<div class="browse-empty">No video files found in<br><code>${
              folder.replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]))
            }</code>.</div>`
          : '<div class="browse-empty">Set a "Video downloads" folder in Settings &gt; General to see manual downloads here.</div>';
      }
    } catch (e) {
      console.error("[manual] load failed", e);
      if (reset && g) g.innerHTML = '<div class="browse-empty">Couldn’t load files.</div>';
    } finally {
      _loading = false;
      if (moreEl) moreEl.hidden = true;
    }
  }

  function _nearBottom(el) {
    if (!el) return false;
    return (el.scrollHeight - el.scrollTop - el.clientHeight) < 700;
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
          const proceed = await _confirmRecoverIds();
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
          proceed = await _confirmRefreshMetadata();
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
          proceed = await _confirmTranscribeMissing();
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

  window._loadManualView = function () {
    wireOnce(); loadPage(true);
    if (typeof window._refreshManualReviewCount === "function")
      window._refreshManualReviewCount();
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
    try {
      const res = await bridgeCall("list_manual_videos", sortAtCall, PAGE, 0);
      if (sortAtCall !== _sort || _loading) return;
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
            for (let i = 0; i < splitIdx; i++) {
              const c = _cardFor(rows[i]);
              if (c) frag.appendChild(c);
            }
            g.insertBefore(frag, g.firstChild);
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
