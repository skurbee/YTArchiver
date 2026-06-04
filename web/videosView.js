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
 *   window._refreshVideosViewIfActive() — reload if the view is showing
 *                                          (called when a new download lands)
 */
(function () {
  "use strict";

  const PAGE = 60;
  let _sort = "recent";
  let _offset = 0;
  let _loading = false;
  let _hasMore = true;
  let _seq = 0;          // stale-load guard: a newer sort/reset wins
  let _wired = false;

  const $ = (id) => document.getElementById(id);
  const grid = () => $("recent-grid");

  function isActive() {
    const v = $("view-recent");
    return !!(v && v.style.display !== "none" && v.offsetParent !== null);
  }

  function _cardFor(r) {
    const v = {
      title: r.title || "", channel: r.channel || "",
      filepath: r.filepath || "", video_id: r.video_id || "",
      duration: r.duration || "", uploaded: r.uploaded || "",
      upload_ts: r.upload_ts || null, size_bytes: r.size_bytes || 0,
      views: r.views || "", view_count: r.view_count,
      thumbnail_url: r.thumbnail_url || "",
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
      else if (vv.filepath && window.pywebview?.api?.browse_open_video)
        window.pywebview.api.browse_open_video(vv.filepath);
    };
    const card = build(v, onClick);
    if (card) card.dataset.tracked = "1";
    return card;
  }

  async function loadPage(reset) {
    const api = window.pywebview && window.pywebview.api;
    if (!api || !api.list_all_videos) return;
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
      const res = await api.list_all_videos(_sort, PAGE, _offset);
      if (myId !== _seq) return;  // superseded by a newer sort/reset
      const rows = (res && res.rows) || [];
      if (reset && g) g.innerHTML = "";
      const frag = document.createDocumentFragment();
      for (const r of rows) { const c = _cardFor(r); if (c) frag.appendChild(c); }
      if (g) g.appendChild(frag);
      _offset += rows.length;
      _hasMore = !!(res && res.has_more);
      if (g && _offset === 0) {
        g.innerHTML = '<div class="browse-empty">No videos in the archive yet.</div>';
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
  function onScroll() {
    if (!isActive() || !_hasMore || _loading) return;
    if (_nearBottom($("recent-grid-frame"))
        || _nearBottom(document.scrollingElement || document.documentElement)) {
      loadPage(false);
    }
  }

  function wireOnce() {
    if (_wired) return;
    _wired = true;
    $("videos-sort")?.addEventListener("change", (e) => {
      _sort = e.target.value || "recent";
      loadPage(true);
    });
    $("recent-grid-frame")?.addEventListener("scroll", onScroll, { passive: true });
    window.addEventListener("scroll", onScroll, { passive: true });
  }

  window._loadVideosView = function () { wireOnce(); loadPage(true); };
  window._refreshVideosViewIfActive = function () { if (isActive()) loadPage(true); };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", wireOnce);
  } else {
    wireOnce();
  }
})();
