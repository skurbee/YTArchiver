/**
 * web/browseView.js — Browse-tab sub-mode flow + watch-view lifecycle.
 *
 * Extracted from app.js. Owns the user's navigation through Browse:
 *   - Sub-mode buttons (Channels / Search / Graph / Bookmarks / Index)
 *   - Within Channels: channel grid → video grid → watch view
 *   - Back-button unwind + per-view scroll-position restore
 *   - Watch video lifecycle (stop / pause / source-load placeholders)
 *   - _watchReturnTo plumbing for Recent / Search / Bookmark entry points
 *
 * Exposed:
 *   - window.initBrowseSubmodes (called by app.js boot)
 *   - window.browseNavigate
 *   - window.showView
 *   - window._stopWatchVideo
 *   - window._pauseWatchVideo
 *   - window._paintWatchLoadingState (already exported from app.js)
 *
 * Depends on:
 *   - window._browseState (app.js still owns the const; this module
 *     uses the shared reference via window._browseState)
 *   - window._showToast, askConfirm, etc.
 */
(function () {
  "use strict";

  const _browseState = window._browseState;
  if (!_browseState) {
    console.warn("[browseView] window._browseState not published yet");
    return;
  }
  const askConfirm = window.askConfirm;
  const askDanger = window.askDanger;
  const askQuestion = window.askQuestion;
  const askChoice = window.askChoice;
  function bridgeCall(method, ...args) {
    const fn = window.YT?.bridge?.bridgeCall;
    if (fn) return fn(method, ...args);
    return undefined;
  }

  function initBrowseSubmodes() {
    const buttons = document.querySelectorAll(".submode-btn");
    buttons.forEach(b => {
      b.addEventListener("click", () => {
        buttons.forEach(x => x.classList.remove("active"));
        b.classList.add("active");
        _browseState.submode = b.dataset.submode;
        browseNavigate();
      });
    });

    // Back button — channels → videos → watch unwind, with scroll-position
    // restoration per-view so clicking back to the channel grid lands on
    // the same card the user drilled out of. Matches YTArchiver.py:28167
    // _on_browse_back_to_grid (scope tracking + grid restore).
    //
    // When the Watch view was entered from somewhere OTHER than the
    // channel→videos→watch drilldown (e.g., double-clicking a row in
    // the Recent submode, clicking a search hit, jumping from a
    // bookmark), we record `_browseState.watchReturnTo` at entry time
    // and return there. Without that, Back from Recent → Watch would
    // `showView("videos")` with no `currentChannel` set, landing the
    // user on a blank video grid — this was reported
    window._browseGoBack = () => {
      if (_browseState.view === "watch") {
        const returnTo = _browseState.watchReturnTo;
        _browseState.watchReturnTo = null;
        if (returnTo === "videos" && _browseState.currentChannel) {
          // Normal channel-drill return path: back to the video grid.
          showView("videos");
          _restoreScroll("video-grid");
          return;
        }
        if (returnTo === "recent" || returnTo === "search" ||
            returnTo === "bookmarks" || returnTo === "graph") {
          // The user launched Watch from a Browse submode. Re-click
          // the submode button so the sidebar's `.active` state
          // updates at the same time the view swaps.
          const btn = document.querySelector(
            `.submode-btn[data-submode="${returnTo}"]`);
          if (btn) { btn.click(); return; }
        }
        // Fallbacks: prefer current-channel video grid if one's set;
        // otherwise drop to the Channels grid. Never leave the user
        // staring at a blank video-grid page with no channel loaded.
        if (_browseState.currentChannel) {
          showView("videos");
          _restoreScroll("video-grid");
        } else {
          showView("channels");
          _restoreScroll("channel-grid");
        }
      } else if (_browseState.view === "videos") {
        showView("channels");
        _restoreScroll("channel-grid", _browseState.currentChannel);
        // clear any per-video filter text on the way
        // back to the channel grid. Without this, filter text
        // from the videos view sticks around even though it's
        // irrelevant to the Channels view and can mask channels
        // that don't match the old video-scoped query.
        try {
          const _f = document.getElementById("browse-filter");
          if (_f && _f.value) {
            _f.value = "";
            _f.dispatchEvent(new Event("input", { bubbles: true }));
          }
        } catch (_e) { /* non-fatal */ }
      }
    };
    document.getElementById("browse-back-btn")?.addEventListener("click", window._browseGoBack);

    // Mouse button 4 (back) / 5 (forward) — browser-style navigation
    // across the Browse tab. the user uses a 5-button mouse and expected
    // these to work like in a browser. `mouseup` event's `.button`
    // gives us 3 for back, 4 for forward on Windows; we listen on
    // `auxclick` which fires for non-primary buttons. `preventDefault`
    // blocks the browser's native gesture (which would try to navigate
    // the pywebview URL).
    //
    // Forward: re-enters the Watch view with the most recently viewed
    // video. Stored in `_browseState.lastWatched` when `_browseGoBack`
    // leaves Watch. If there's nothing to forward to, it's a no-op.
    window.addEventListener("mouseup", (e) => {
      if (e.button === 3) {
        // Back
        e.preventDefault();
        if (_browseState.view === "watch") {
          // Stash the current video so Forward can re-enter.
          _browseState.lastWatched = _browseState.currentVideo || null;
        }
        window._browseGoBack?.();
      } else if (e.button === 4) {
        // Forward — re-open the last Watch view we backed out of.
        e.preventDefault();
        const v = _browseState.lastWatched;
        if (v && typeof window._openVideoInWatch === "function") {
          _browseState.lastWatched = null;
          window._openVideoInWatch(v);
        }
      }
    });

    // Sort dropdown
    document.getElementById("browse-sort")?.addEventListener("change", (e) => {
      sortCurrentVideos(e.target.value);
    });

    // Group-by-year checkbox triggers a re-render with current sort
    document.getElementById("browse-group-year")?.addEventListener("change", () => {
      const sort = document.getElementById("browse-sort")?.value || "newest";
      sortCurrentVideos(sort);
    });
    // Group-by-month does the same, for channels organized yyyy/mm.
    document.getElementById("browse-group-month")?.addEventListener("change", () => {
      const sort = document.getElementById("browse-sort")?.value || "newest";
      sortCurrentVideos(sort);
    });

    // Filter input
    document.getElementById("browse-filter")?.addEventListener("input", (e) => {
      filterCurrentView(e.target.value);
    });
  }

  // Live filter for the channel grid + video grid (Browse tab's
  // top-right search box). Previously lived in indexControls.js where
  // it was unreachable from this IIFE — moved here so the wiring
  // above actually works.
  function filterCurrentView(q) {
    q = (q || "").toLowerCase().trim();
    if (_browseState.view === "channels") {
      const filtered = !q
        ? _browseState.channels
        : _browseState.channels.filter(c => (c.folder || "").toLowerCase().includes(q));
      window.renderChannelGrid(filtered, (c) => {
        _browseState.currentChannel = c;
        if (typeof window.loadVideosFor === "function") window.loadVideosFor(c);
        showView("videos");
      });
    } else if (_browseState.view === "videos") {
      const filtered = !q
        ? _browseState.videos
        : _browseState.videos.filter(v => (v.title || "").toLowerCase().includes(q));
      const groupByYear = !!document.getElementById("browse-group-year")?.checked;
      window.renderVideoGrid(filtered, (v) => {
        _browseState.currentVideo = v;
        showView("watch");
        // pass [] for "No transcript available." message
        window.renderWatchView(v, []);
      }, { groupByYear });
    }
  }

  function browseNavigate() {
    const mode = _browseState.submode;
    // Leaving Watch via a submode click — pause the <video> element
    // before we hide it. `display:none` does NOT pause HTML5 media
    // on its own (a bug: mouse back from Watch kept audio
    // playing in the background). `showView` does this too but only
    // when called through its own path; submode clicks bypass
    // showView for the non-channels modes.
    if (_browseState.view === "watch" &&
        typeof window._stopWatchVideo === "function") {
      try { window._stopWatchVideo(); } catch (e) { /* noop */ }
    }
    // Hide all views
    document.querySelectorAll(".browse-view").forEach(v => v.style.display = "none");
    const toolbar = document.getElementById("browse-main-toolbar");
    const backBtn = document.getElementById("browse-back-btn");
    const sortWrap = document.getElementById("browse-sort-wrap");
    const title = document.getElementById("browse-main-title");
    const filter = document.getElementById("browse-filter");
    // Grab the whole filter+icon wrap so we can hide the orphan
    // magnifying-glass icon alongside the input on views that don't
    // use the top-level Browse filter. Previously only the input was
    // being hidden, leaving a floating 🔍 with nothing to filter.
    const findWrap = filter?.closest(".browse-find-wrap");

    if (mode === "channels") {
      // Restart in channel grid
      _browseState.view = "channels";
      if (findWrap) findWrap.style.display = "";
      showView("channels");
    } else if (mode === "recent") {
      // Recent was moved from a top-level tab into a Browse submode
      // (users wanted all library navigation in one place). Reuses
      // the same #recent-table / #recent-table-body ids as before so
      // the existing render + click wiring keeps working. Recent has
      // its own filter input inside the view, so hide the top-level
      // find-wrap entirely (wrap, not just input — otherwise the
      // icon orphans).
      document.getElementById("view-recent").style.display = "";
      title.textContent = "Recent downloads";
      backBtn.style.display = "none";
      sortWrap.style.display = "none";
      if (findWrap) findWrap.style.display = "none";
      _browseState.view = "recent";
    } else if (mode === "search") {
      // Search view has its own search input inside #view-search, so
      // hide the top-level find-wrap too (same orphan-icon bug as
      // recent if we only hid the input).
      document.getElementById("view-search").style.display = "";
      title.textContent = "Search transcripts";
      backBtn.style.display = "none";
      sortWrap.style.display = "none";
      if (findWrap) findWrap.style.display = "none";
    } else if (mode === "graph") {
      document.getElementById("view-graph").style.display = "";
      title.textContent = "Word frequency";
      backBtn.style.display = "none";
      sortWrap.style.display = "none";
      if (findWrap) findWrap.style.display = "none";
      populateGraphChannels();
    } else if (mode === "bookmarks") {
      document.getElementById("view-bookmarks").style.display = "";
      title.textContent = "Bookmarks";
      backBtn.style.display = "none";
      sortWrap.style.display = "none";
      if (findWrap) findWrap.style.display = "none";
      refreshBookmarks();
    } else if (mode === "index") {
      // Browse > Index sub-mode was removed; the Index controls now
      // live in Settings → Index. Null-check defensively in case
      // anything tries to switch to "index" mode in the future.
      const idx = document.getElementById("view-index");
      if (idx) idx.style.display = "";
      title.textContent = "Archive index";
      backBtn.style.display = "none";
      sortWrap.style.display = "none";
    }
  }

  // Per-view scroll state — remembers `scrollTop` of each grid so the back
  // button lands the user where they left off, plus which card was
  // currently "focused" so we can scroll that one into view + flash it.
  const _scrollSaved = { "channel-grid": 0, "video-grid": 0 };

  function _saveScroll(gridId) {
    const el = document.getElementById(gridId);
    if (!el) return;
    // Walk up to the scrollable ancestor — channel-grid itself may not scroll.
    let s = el;
    while (s && s !== document.body) {
      if (s.scrollHeight > s.clientHeight + 4) break;
      s = s.parentElement;
    }
    if (s && s !== document.body) {
      _scrollSaved[gridId] = s.scrollTop;
    }
  }

  function _restoreScroll(gridId, focusItem) {
    const el = document.getElementById(gridId);
    if (!el) return;
    let s = el;
    while (s && s !== document.body) {
      if (s.scrollHeight > s.clientHeight + 4) break;
      s = s.parentElement;
    }
    // Give the view one paint cycle, then restore
    setTimeout(() => {
      if (s && s !== document.body) s.scrollTop = _scrollSaved[gridId] || 0;
      // If we have a focus target, flash it briefly so the user orients
      if (focusItem) {
        const name = focusItem.folder || focusItem.name || "";
        const card = [...el.querySelectorAll(".channel-card")].find(c =>
          (c.querySelector(".channel-card-name")?.textContent || "") === name);
        if (card) {
          card.classList.add("flash-hit");
          setTimeout(() => card.classList.remove("flash-hit"), 1400);
        }
      }
    }, 60);
  }

  // Pause + unload the Watch view's <video> element. Called whenever
  // the user navigates away from the Watch view — without this the
  // element keeps playing in the background because `display:none` does
  // NOT pause HTML5 media. users hit this: clicked Back, video kept
  // playing audio with no UI to stop it. Clearing `src` + calling
  // `load()` cuts the stream so the browser releases the file handle
  // too (matters for the local_fileserver 206-range requests).
  function _stopWatchVideo() {
    // Full teardown — used when the user is DONE watching (Back
    // button, Library sidebar click). Pauses + unloads the media
    // resource. After this, returning to the Watch view shows the
    // empty placeholder and the video needs to be re-opened.
    const vEl = document.getElementById("watch-video");
    if (!vEl) return;
    try { vEl.pause(); } catch (e) { /* noop */ }
    try {
      vEl.removeAttribute("src");
      // `load()` is what actually tears down the HTMLMediaElement's
      // internal resource; without it, `src=""` just blanks the URL
      // but the stream keeps buffering.
      if (typeof vEl.load === "function") vEl.load();
    } catch (e) { /* noop */ }
  }
  window._stopWatchVideo = _stopWatchVideo;

  function _pauseWatchVideo() {
    // Soft pause — used when the user navigates AWAY from Browse to
    // a different top-level tab while a video is loaded. Keeps src +
    // playhead intact so returning to Browse resumes right where they
    // left off. "pausing a video, going to a different tab,
    // then back to browse, closes the video ... it should still be
    // up (but if the user switches tabs while it's playing, pause)".
    const vEl = document.getElementById("watch-video");
    if (!vEl) return;
    try {
      vEl.pause();
      // Chromium/WebView keeps ~1s of audio buffered ahead of the
      // decode head. Plain pause() stops the decode but the buffer
      // still plays out for a second or three — audible after tab
      // switch. Re-seeking to the current position flushes the
      // buffer without moving the playhead, cutting audio
      // immediately on pause.
      vEl.currentTime = vEl.currentTime;
    } catch (e) { /* noop */ }
  }
  window._pauseWatchVideo = _pauseWatchVideo;

  function showView(viewName) {
    // Save scroll of outgoing view before swapping
    if (_browseState.view === "channels") _saveScroll("channel-grid");
    if (_browseState.view === "videos") _saveScroll("video-grid");
    // leaving the video grid — wipe the metadata-missing
    // banner so it doesn't persist into the next channel's grid load
    // if the user switched mid-fetch. (The banner is recreated on next
    // sortCurrentVideos call if still appropriate.)
    if (_browseState.view === "videos" && viewName !== "videos") {
      try {
        const vg = document.getElementById("video-grid");
        vg?.parentElement?.querySelector(".meta-nudge-banner")?.remove();
      } catch {}
    }
    // Leaving the Watch view — stop the backgrounded <video> element
    // so audio doesn't keep playing after Back / tab swap. Must happen
    // BEFORE we mutate _browseState.view so the "was in watch?" check
    // is accurate.
    if (_browseState.view === "watch" && viewName !== "watch") {
      _stopWatchVideo();
    }
    _browseState.view = viewName;
    document.querySelectorAll(".browse-view").forEach(v => v.style.display = "none");
    const title = document.getElementById("browse-main-title");
    const backBtn = document.getElementById("browse-back-btn");
    const sortWrap = document.getElementById("browse-sort-wrap");
    const filter = document.getElementById("browse-filter");
    const findWrap = filter?.closest(".browse-find-wrap");

    if (viewName === "channels") {
      document.getElementById("view-channels").style.display = "";
      title.textContent = "Channels";
      backBtn.style.display = "none";
      sortWrap.style.display = "none";
      if (filter) { filter.placeholder = "Filter channels\u2026"; filter.value = ""; }
      if (findWrap) findWrap.style.display = "";
    } else if (viewName === "videos") {
      document.getElementById("view-videos").style.display = "";
      title.textContent = _browseState.currentChannel?.folder || "Videos";
      backBtn.style.display = "";
      sortWrap.style.display = "";
      if (filter) { filter.placeholder = "Filter videos\u2026"; filter.value = ""; }
      if (findWrap) findWrap.style.display = "";
    } else if (viewName === "watch") {
      document.getElementById("view-watch").style.display = "";
      title.textContent = _browseState.currentVideo?.title || "Watch";
      backBtn.style.display = "";
      sortWrap.style.display = "none";
      // Hide the whole wrap (input + icon), not just the input —
      // otherwise a lone magnifying glass sits in the header with
      // nothing to filter.
      if (findWrap) findWrap.style.display = "none";
      // Paint loading state into the watch view BEFORE any async
      // transcript fetch resolves. Every code path that opens Watch
      // (video-grid click, search result click, Forward gesture,
      // _openVideoInWatch helper, …) routes through showView, so
      // landing the loading paint here covers them all instead of
      // each handler duplicating it (and forgetting to, in the
      // grid-click / search-click paths — symptom was the blank
      // placeholder text "Video Title" / "Channel · upload date ·
      // duration" surviving for several seconds when the transcript
      // fetch was slow).
      _paintWatchLoadingState(_browseState.currentVideo);
    }
  }

  // Repaint the Watch pane with a "Loading…" treatment for every slot
  // that the slow transcript fetch will eventually fill: title, meta,
  // <video> placeholder, transcript pane, and the description/comments
  // drawer body. Idempotent — safe to call repeatedly.
  function _paintWatchLoadingState(video) {
    try {
      const titleEl = document.getElementById("watch-title");
      const metaEl = document.getElementById("watch-meta");
      if (titleEl) titleEl.textContent = (video && video.title) || "Loading…";
      if (metaEl) {
        const parts = [];
        if (video?.channel) parts.push(video.channel);
        if (video?.uploaded) parts.push(video.uploaded);
        if (video?.duration) parts.push(video.duration);
        if (video?.views) parts.push(video.views + " views");
        metaEl.textContent = parts.join(" · ") || "Loading…";
      }
      const vEl = document.getElementById("watch-video");
      const ph = document.getElementById("watch-video-placeholder");
      if (ph && video && video.filepath) {
        ph.style.display = "";
        ph.innerHTML =
          '<div class="watch-play-icon" style="visibility:hidden;">▶</div>'
          + '<div class="watch-placeholder-text" '
          + 'style="font-size:13px;color:var(--c-text);">'
          + '<span class="spinner-inline"></span>Loading…</div>';
        if (vEl) vEl.style.display = "none";
      }
      const tr = document.getElementById("watch-transcript");
      if (tr) {
        tr.innerHTML = '<div style="color: var(--c-dim); font-style: italic; padding: 12px;">'
          + '<span class="spinner-inline"></span>Loading transcript…</div>';
      }
      // Drawer is open by default (v63.7) — without resetting it the
      // user sees the previous video's description and comments until
      // renderWatchView triggers _loadWatchMetadataDrawer.
      const descEl = document.getElementById("watch-meta-description");
      const commentsEl = document.getElementById("watch-meta-comments");
      const countEl = document.getElementById("watch-meta-comments-count");
      const statsEl = document.getElementById("watch-meta-stats");
      if (statsEl) statsEl.textContent = "";
      if (descEl) descEl.textContent = "Loading…";
      if (commentsEl) commentsEl.innerHTML = "";
      if (countEl) countEl.textContent = "";
    } catch (_e) { /* non-fatal */ }
  }
  window._paintWatchLoadingState = _paintWatchLoadingState;

  // Browse > Search sub-mode + viewer pane + un-indexed banner moved
  // to web/browseSearch.js (window.initSearchView).

  // escapeHtml moved to web/util.js. Local alias kept so
  // the existing call sites in this IIFE keep resolving — Patch 14
  // migrates them to YT.util.escapeHtml.
  const escapeHtml = window.YT?.util?.escapeHtml || ((s) => String(s ?? ""));

  // Auto-sync dropdown + countdown moved to web/autoSync.js (window.initAutorun).

  window.initBrowseSubmodes = initBrowseSubmodes;
  window.browseNavigate = browseNavigate;
  window.showView = showView;
  window._stopWatchVideo = _stopWatchVideo;
  window._pauseWatchVideo = _pauseWatchVideo;
})();
