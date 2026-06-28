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

  function _cardFor(r) {
    // Try the shared card builder first (gives thumbnail + channel line).
    // Falls back to a simple text card if the builder isn't available.
    const build = window._buildVideoCard;
    if (build) {
      const v = {
        title: r.title || r.filepath || "",
        channel: "",
        filepath: r.filepath || "",
        video_id: r.video_id || "",
        duration: "",
        uploaded: "",
        upload_ts: null,
        size_bytes: r.size_bytes || 0,
        views: "",
        view_count: null,
        thumbnail_url: r.thumbnail_url || "",
        tx_status: "",
        removed_from_yt: false,
        show_channel: false,
      };
      const onClick = (vv) => {
        if (typeof window._openVideoInWatch === "function")
          window._openVideoInWatch(vv);
        else if (vv.filepath && nativeBridgeUp())
          bridgeCall("browse_open_video", vv.filepath);
      };
      const card = build(v, onClick);
      if (card) { card.dataset.tracked = "1"; return card; }
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
    return el;
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
        _firstPageSig = rows.map(r => r.filepath || "").join("|");
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

  let _scrollRaf = null;
  function onScroll() {
    if (_scrollRaf) return;
    _scrollRaf = requestAnimationFrame(() => {
      _scrollRaf = null;
      if (!isActive() || !_hasMore || _loading) return;
      const frame = $("manual-grid-frame");
      const near = (el) => {
        if (!el) return false;
        return (el.scrollHeight - el.scrollTop - el.clientHeight) < 700;
      };
      if (near(frame) || near(document.scrollingElement || document.documentElement)) {
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
    $("manual-grid-frame")?.addEventListener("scroll", onScroll, { passive: true });
    window.addEventListener("scroll", onScroll, { passive: true });
  }

  window._loadManualView = function () { wireOnce(); loadPage(true); };

  window._refreshManualViewIfActive = async function () {
    if (!isActive() || _loading) return;
    if (!nativeBridgeUp()) return;
    const sortAtCall = _sort;
    try {
      const res = await bridgeCall("list_manual_videos", sortAtCall, PAGE, 0);
      if (sortAtCall !== _sort || _loading) return;
      const rows = (res && res.rows) || [];
      const newSig = rows.map(r => r.filepath || "").join("|");
      if (newSig === _firstPageSig) return;
      if (sortAtCall === "newest" && _firstPageSig) {
        const oldFirst = _firstPageSig.split("|")[0];
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
