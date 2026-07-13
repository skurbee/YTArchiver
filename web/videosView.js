/**
 * web/videosView.js — the "Videos" Browse sub-mode.
 *
 * Every video in the archive, sortable (recent / newest / oldest / views /
 * likes / title / channel / largest) and lazy-loaded a page at a time,
 * backed by api.list_all_videos(sort, limit, offset). Renders into the
 * existing #recent-grid using the shared Browse video-card builder.
 *
 * Public:
 *   window._loadVideosView()            — (re)load page 1 with the current sort
 *   window._refreshVideosViewIfActive() — if the view is showing, re-query
 *                                          page 1 and re-render ONLY if it
 *                                          actually changed. Called when the
 *                                          user returns to the Browse tab so a
 *                                          download that landed while they were
 *                                          on another tab shows up without a
 *                                          manual sort change. No-op (no flash,
 *                                          scroll preserved) when nothing was
 *                                          added.
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
  let _sort = "recent";
  let _filter = "";      // title/channel substring filter (server-side)
  let _offset = 0;
  let _loading = false;
  let _hasMore = true;
  let _seq = 0;          // stale-load guard: a newer sort/reset wins
  let _wired = false;
  // Signature (joined ids/paths) of the page-1 rows currently rendered.
  // Used by _refreshVideosViewIfActive to decide whether a return-to-tab
  // re-render is actually needed, so an unchanged grid isn't torn down.
  let _firstPageSig = "";

  const $ = (id) => document.getElementById(id);
  const grid = () => $("recent-grid");

  function _uploadTsMs(value) {
    const n = Number(value || 0);
    if (!Number.isFinite(n) || n <= 0) return null;
    return n < 1000000000000 ? n * 1000 : n;
  }

  function _pageSig(rows) {
    return (rows || []).map(r => [
      r.video_id || r.filepath || "",
      r.thumbnail_url || "",
      r.duration || "",
      r.tx_status || "",
    ].join("~")).join("|");
  }

  function isActive() {
    const v = $("view-recent");
    return !!(v && !v.hidden && v.offsetParent !== null);
  }

  function _cardFor(r) {
    const v = {
      title: r.title || "", channel: r.channel || "",
      filepath: r.filepath || "", video_id: r.video_id || "",
      duration: r.duration || "", uploaded: r.uploaded || "",
      upload_ts: _uploadTsMs(r.upload_ts), size_bytes: r.size_bytes || 0,
      views: r.views || "", view_count: r.view_count,
      thumbnail_url: r.thumbnail_url || "",
      // This view is already server-paginated to 60 cards. WebView2's native
      // loading="lazy" intermittently leaves *visible* images dormant inside
      // the nested Browse scroller until the user scrolls away and back.
      // Eager loading this bounded page fixes the blank-gray-card failure;
      // large per-channel/grouped grids keep their lazy/batched behavior.
      eager_thumbnail: true,
      tx_status: r.tx_status || "", removed_from_yt: !!r.removed_from_yt,
      // Cross-channel view — always show the channel line on each card.
      show_channel: true,
    };
    const build = window._buildVideoCard;
    if (!build) {
      const d = document.createElement("div");
      d.className = "video-card"; d.textContent = v.title; return d;
    }
    const onClick = (vv) => {
      if (typeof window._openVideoInWatch === "function") window._openVideoInWatch(vv);
      else if (vv.filepath && nativeBridgeUp())
        bridgeCall("browse_open_video", vv.filepath);
    };
    const card = build(v, onClick);
    if (card) card.dataset.tracked = "1";
    return card;
  }

  async function loadPage(reset) {
    if (!nativeBridgeUp()) return;
    if (_loading) return;
    _loading = true;
    const myId = ++_seq;
    if (reset) { _offset = 0; _hasMore = true; }
    const g = grid();
    const moreEl = $("videos-load-more");
    if (reset && g) {
      g.innerHTML = '<div class="grid-loading"><div class="grid-spinner"></div>'
        + '<span class="grid-loading-label">Loading videos…</span></div>';
    } else if (moreEl) { moreEl.hidden = false; }
    try {
      const res = await bridgeCall("list_all_videos", _sort, PAGE, _offset, _filter);
      if (myId !== _seq) return;  // superseded by a newer sort/reset
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
      if (g && _offset === 0) {
        g.innerHTML = _filter
          ? `<div class="browse-empty">No videos match “${_filter
              .replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]))}”.</div>`
          : '<div class="browse-empty">No videos in the archive yet.</div>';
      }
    } catch (e) {
      console.error("[videos] load failed", e);
      if (reset && g) g.innerHTML = '<div class="browse-empty">Couldn’t load videos.</div>';
    } finally {
      _loading = false;
      if (moreEl) moreEl.hidden = true;
    }
  }

  function _nearBottom(el) {
    if (!el) return false;
    return (el.scrollHeight - el.scrollTop - el.clientHeight) < 700;
  }
  // RAF-debounced scroll handler: layout reads (scrollHeight / clientHeight)
  // are batched into one rAF tick per scroll burst instead of firing on
  // every individual scroll event, which caused jank on large grids.
  let _scrollRaf = null;
  function onScroll() {
    if (_scrollRaf) return;
    _scrollRaf = requestAnimationFrame(() => {
      _scrollRaf = null;
      if (!isActive() || !_hasMore || _loading) return;
      // The Videos grid's scroll can live on EITHER the inner frame
      // (#recent-grid-frame) or the outer .browse-view (#view-recent) — the
      // latter is a block-level overflow-y:auto container, so the inner
      // frame's flex:1 is inert and #view-recent is what actually scrolls.
      // Check both (plus the document) so load-more fires regardless of
      // which element owns the scroll.
      if (_nearBottom($("recent-grid-frame"))
          || _nearBottom($("view-recent"))
          || _nearBottom(document.scrollingElement || document.documentElement)) {
        loadPage(false);
      }
    });
  }

  function wireOnce() {
    if (_wired) return;
    _wired = true;
    $("videos-sort")?.addEventListener("change", (e) => {
      _sort = e.target.value || "recent";
      loadPage(true);
    });
    // Title/channel filter — debounced so each keystroke doesn't fire a
    // query. Reloads page 1 server-side (filters the WHOLE archive, not
    // just the lazy-loaded cards already on screen).
    let _filterTimer = null;
    $("videos-filter")?.addEventListener("input", (e) => {
      const val = e.target.value || "";
      if (_filterTimer) clearTimeout(_filterTimer);
      _filterTimer = setTimeout(() => {
        const next = val.trim();
        if (next === _filter) return;
        _filter = next;
        loadPage(true);
      }, 220);
    });
    $("recent-grid-frame")?.addEventListener("scroll", onScroll, { passive: true });
    $("view-recent")?.addEventListener("scroll", onScroll, { passive: true });
    window.addEventListener("scroll", onScroll, { passive: true });
  }

  window._loadVideosView = function () { wireOnce(); loadPage(true); };

  // Cheap "did page 1 change?" check. Re-query the first page with the
  // current sort and compare its id/path signature to what's rendered.
  // Re-render only on a real difference — so flipping Download↔Browse with
  // no new downloads leaves the grid (and scroll position) untouched, while
  // a download that landed while away shows up immediately on return. The
  // page-1 query is backend-cached, so the unchanged case is a fast cache
  // hit. _firstPageSig is keyed implicitly to the current sort because
  // loadPage() always rebuilds it for whatever sort is active.
  window._refreshVideosViewIfActive = async function () {
    if (_loading) return;
    if (!nativeBridgeUp()) return;
    // Background-capable: refresh when the view is visible OR when it was
    // already loaded once (`_firstPageSig` set). Updating the hidden grid
    // keeps the new video preloaded so it's already there — no "pop-in" —
    // when the user switches back to Browse. Skip only if never loaded
    // (it'll load fresh on first open). DOM prepends on a hidden grid are
    // cheap and safe.
    if (!isActive() && !_firstPageSig) return;
    const sortAtCall = _sort;
    const filterAtCall = _filter;
    try {
      const res = await bridgeCall("list_all_videos", sortAtCall, PAGE, 0, filterAtCall);
      if (sortAtCall !== _sort || filterAtCall !== _filter || _loading) return;
      const rows = (res && res.rows) || [];
      const newSig = _pageSig(rows);
      if (newSig === _firstPageSig) return; // nothing changed

      // For "recent" sort: try a no-flash prepend — find how many NEW
      // items are at the top (before the old first item) and insert only
      // those, avoiding the blank-grid flash that loadPage(true) causes.
      if (sortAtCall === "recent" && _firstPageSig) {
        const oldFirstId = _firstPageSig.split("|")[0].split("~")[0];
        const splitIdx = rows.findIndex(
          r => (r.video_id || r.filepath || "") === oldFirstId
        );
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
            return; // done — no blank flash, scroll position preserved
          }
        }
      }

      // Fallback: full reload (other sorts, filtered view, or old first
      // item no longer in the new page because many videos were added).
      loadPage(true);
    } catch (_e) { /* non-fatal — leave the current grid as-is */ }
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", wireOnce);
  } else {
    wireOnce();
  }
})();
