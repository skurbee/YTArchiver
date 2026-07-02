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
  let _browseFilterTimer = null;
  function bridgeCall(method, ...args) {
    const fn = window.YT?.bridge?.bridgeCall;
    if (fn) return fn(method, ...args);
    return undefined;
  }

  function initBrowseSubmodes() {
    // Re-init guard so a second call doesn't double up the click
    // handlers on the same buttons (audit: browseView.js H167).
    if (initBrowseSubmodes._wired) return;
    initBrowseSubmodes._wired = true;
    const buttons = document.querySelectorAll(".submode-btn");
    document.querySelector(".browse-sidebar")?.setAttribute("role", "tablist");
    const syncSubmodeA11y = () => {
      buttons.forEach(x => {
        const active = x.classList.contains("active");
        x.setAttribute("aria-selected", active ? "true" : "false");
      });
    };
    buttons.forEach(b => {
      b.setAttribute("role", "tab");
      b.tabIndex = 0;
      b.addEventListener("keydown", (e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          b.click();
        }
      });
      b.addEventListener("click", () => {
        buttons.forEach(x => x.classList.remove("active"));
        b.classList.add("active");
        syncSubmodeA11y();
        _browseState.submode = b.dataset.submode;
        browseNavigate();
      });
    });
    syncSubmodeA11y();

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
            returnTo === "bookmarks" || returnTo === "graph" ||
            returnTo === "manual") {
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
      if (document.querySelector(".tab.active")?.dataset.tab !== "browse") {
        return;
      }
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
      if (_browseState.view === "videos" &&
          typeof window._reloadCurrentChannelVideos === "function") {
        window._reloadCurrentChannelVideos();
        return;
      }
      window.sortCurrentVideos?.(e.target.value);
    });

    // Group-by-year checkbox triggers a re-render with current sort
    document.getElementById("browse-group-year")?.addEventListener("change", () => {
      if (_browseState.view === "videos" &&
          typeof window._reloadCurrentChannelVideos === "function") {
        window._reloadCurrentChannelVideos();
        return;
      }
      const sort = document.getElementById("browse-sort")?.value || "newest";
      window.sortCurrentVideos?.(sort);
    });
    // Group-by-month does the same, for channels organized yyyy/mm.
    document.getElementById("browse-group-month")?.addEventListener("change", () => {
      if (_browseState.view === "videos" &&
          typeof window._reloadCurrentChannelVideos === "function") {
        window._reloadCurrentChannelVideos();
        return;
      }
      const sort = document.getElementById("browse-sort")?.value || "newest";
      window.sortCurrentVideos?.(sort);
    });

    // Filter input
    document.getElementById("browse-filter")?.addEventListener("input", (e) => {
      const value = e.target.value;
      if (_browseFilterTimer) clearTimeout(_browseFilterTimer);
      _browseFilterTimer = setTimeout(() => {
        _browseFilterTimer = null;
        filterCurrentView(value);
      }, 200);
    });

    // Channel-grid sort dropdown
    document.getElementById("browse-channel-sort")?.addEventListener("change", (e) => {
      window._setChannelSort?.(e.target.value);
    });
  }

  // Browse > Channels grid sort. Default A–Z. Keys map to fields the
  // backend (browse_list_channels) returns: name/folder, last_added_ts,
  // n_vids, size_bytes.
  let _channelSort = "name";
  function _sortChannels(list) {
    const arr = (list || []).slice();
    const by = _channelSort;
    arr.sort((a, b) => {
      if (by === "recent") return (b.last_added_ts || 0) - (a.last_added_ts || 0);
      if (by === "videos") return (Number(b.n_vids) || 0) - (Number(a.n_vids) || 0);
      if (by === "size")   return (b.size_bytes || 0) - (a.size_bytes || 0);
      return String(a.folder || a.name || "").localeCompare(
             String(b.folder || b.name || ""), undefined, { sensitivity: "base" });
    });
    return arr;
  }
  window._setChannelSort = (key) => {
    _channelSort = key || "name";
    filterCurrentView(document.getElementById("browse-filter")?.value || "");
  };

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
      window.renderChannelGrid(_sortChannels(filtered), (c) => {
        _browseState.currentChannel = c;
        if (typeof window.loadVideosFor === "function") window.loadVideosFor(c);
        showView("videos");
      });
    } else if (_browseState.view === "videos") {
      if (typeof window._filterChannelVideosPaged === "function" &&
          window._filterChannelVideosPaged(q)) {
        return;
      }
      const filtered = !q
        ? _browseState.videos
        : _browseState.videos.filter(v => (v.title || "").toLowerCase().includes(q));
      const groupByYear = !!document.getElementById("browse-group-year")?.checked;
      const groupByMonth = !!document.getElementById("browse-group-month")?.checked;
      window.renderVideoGrid(filtered, (v) => {
        // Route through the canonical opener (transcript fetch + _watchOpenToken
        // race guard), same as the unfiltered grid. The old inline path passed
        // [] so a video opened from a FILTERED grid always showed "No transcript
        // available." and skipped the race guard (audit r2). Also forward
        // groupByMonth (was dropped → month sections collapsed on each keystroke).
        if (typeof window._openVideoInWatch === "function") {
          window._openVideoInWatch(v);
        } else {
          _browseState.currentVideo = v;
          showView("watch");
          window.renderWatchView(v, []);
        }
      }, { groupByYear, groupByMonth });
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
    document.querySelectorAll(".browse-view").forEach(v => { v.hidden = true; });
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
    // Channel-sort dropdown belongs to the Channels grid only; hide it for
    // every submode here — showView("channels") re-shows it below.
    const chanSortWrap = document.getElementById("browse-channel-sort-wrap");
    if (chanSortWrap) chanSortWrap.hidden = true;

    if (mode === "channels") {
      // Restart in channel grid
      _browseState.view = "channels";
      if (findWrap) findWrap.hidden = false;
      showView("channels");
    } else if (mode === "recent") {
      // Recent was moved from a top-level tab into a Browse submode
      // (users wanted all library navigation in one place). Reuses
      // the same #recent-table / #recent-table-body ids as before so
      // the existing render + click wiring keeps working. Recent has
      // its own filter input inside the view, so hide the top-level
      // find-wrap entirely (wrap, not just input — otherwise the
      // icon orphans).
      document.getElementById("view-recent").hidden = false;
      title.textContent = "Videos";
      backBtn.hidden = true;
      sortWrap.hidden = true;
      if (findWrap) findWrap.hidden = true;
      _browseState.view = "recent";
      // Load (or reload) the global Videos list for the current sort.
      if (typeof window._loadVideosView === "function") window._loadVideosView();
    } else if (mode === "search") {
      // Search view has its own search input inside #view-search, so
      // hide the top-level find-wrap too (same orphan-icon bug as
      // recent if we only hid the input).
      document.getElementById("view-search").hidden = false;
      title.textContent = "Search transcripts";
      backBtn.hidden = true;
      sortWrap.hidden = true;
      if (findWrap) findWrap.hidden = true;
      // Record origin so Watch's "← Back" returns to Search (browseGoBack
      // re-clicks the submode) instead of dumping the user on Videos.
      _browseState.view = "search";
    } else if (mode === "graph") {
      document.getElementById("view-graph").hidden = false;
      title.textContent = "Word frequency";
      backBtn.hidden = true;
      sortWrap.hidden = true;
      if (findWrap) findWrap.hidden = true;
      _browseState.view = "graph";
      window.populateGraphChannels?.();
    } else if (mode === "bookmarks") {
      document.getElementById("view-bookmarks").hidden = false;
      title.textContent = "Bookmarks";
      backBtn.hidden = true;
      sortWrap.hidden = true;
      if (findWrap) findWrap.hidden = true;
      _browseState.view = "bookmarks";
      window.refreshBookmarks?.();
    } else if (mode === "manual") {
      document.getElementById("view-manual").hidden = false;
      title.textContent = "Manual Downloads";
      backBtn.hidden = true;
      sortWrap.hidden = true;
      if (findWrap) findWrap.hidden = true;
      _browseState.view = "manual";
      if (typeof window._loadManualView === "function") window._loadManualView();
    } else if (mode === "index") {
      // Browse > Index sub-mode was removed; the Index controls now
      // live in Settings → Index. Null-check defensively in case
      // anything tries to switch to "index" mode in the future.
      const idx = document.getElementById("view-index");
      if (idx) idx.hidden = false;
      title.textContent = "Archive index";
      backBtn.hidden = true;
      sortWrap.hidden = true;
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
    document.querySelectorAll(".browse-view").forEach(v => { v.hidden = true; });
    const title = document.getElementById("browse-main-title");
    const backBtn = document.getElementById("browse-back-btn");
    const sortWrap = document.getElementById("browse-sort-wrap");
    const chanSortWrap = document.getElementById("browse-channel-sort-wrap");
    const filter = document.getElementById("browse-filter");
    const findWrap = filter?.closest(".browse-find-wrap");

    if (viewName === "channels") {
      document.getElementById("view-channels").hidden = false;
      title.textContent = "Channels";
      backBtn.hidden = true;
      sortWrap.hidden = true;
      if (chanSortWrap) chanSortWrap.hidden = false;
      if (filter) { filter.placeholder = "Filter channels\u2026"; filter.value = ""; }
      if (findWrap) findWrap.hidden = false;
      // Re-render the grid to match the just-cleared filter. Without this,
      // a grid left filtered from a previous visit (or a stale per-channel
      // scope) persisted while the filter box read empty \u2014 so the user saw
      // only a subset (sometimes a single channel) of their channels until
      // they manually typed into and cleared the filter. Guarded on the
      // channel list being primed so we don't clobber the boot-time initial
      // render with an empty grid.
      if (Array.isArray(_browseState.channels) && _browseState.channels.length) {
        filterCurrentView("");
      }
    } else if (viewName === "videos") {
      document.getElementById("view-videos").hidden = false;
      title.textContent = _browseState.currentChannel?.folder || "Videos";
      backBtn.hidden = false;
      sortWrap.hidden = false;
      if (chanSortWrap) chanSortWrap.hidden = true;
      if (filter) { filter.placeholder = "Filter videos\u2026"; filter.value = ""; }
      if (findWrap) findWrap.hidden = false;
    } else if (viewName === "watch") {
      document.getElementById("view-watch").hidden = false;
      title.textContent = _browseState.currentVideo?.title || "Watch";
      backBtn.hidden = false;
      sortWrap.hidden = true;
      if (chanSortWrap) chanSortWrap.hidden = true;
      // Hide the whole wrap (input + icon), not just the input —
      // otherwise a lone magnifying glass sits in the header with
      // nothing to filter.
      if (findWrap) findWrap.hidden = true;
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
          '<div class="watch-play-icon watch-play-icon-hidden">▶</div>'
          + '<div class="watch-placeholder-text">'
          + '<span class="spinner-inline"></span>Loading…</div>';
        if (vEl) vEl.hidden = true;
      }
      const tr = document.getElementById("watch-transcript");
      if (tr) {
        tr.innerHTML = '<div class="watch-transcript-note watch-transcript-loading">'
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
  const escapeHtml = window.YT?.util?.escapeHtml || ((s) => String(s ?? "")
    .replace(/[&<>"']/g, (ch) => ({
      "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;", "'": "&#39;",
    }[ch])));

  // Auto-sync dropdown + countdown moved to web/autoSync.js (window.initAutorun).

  window.initBrowseSubmodes = initBrowseSubmodes;
  window.browseNavigate = browseNavigate;
  window.showView = showView;
  window._stopWatchVideo = _stopWatchVideo;
  window._pauseWatchVideo = _pauseWatchVideo;

  // Single entry point the backend calls after each download lands
  // (recent_mixin._push_recent_refresh). Fans out to every Browse grid that
  // could be showing the new video — the all-Videos submode, the current
  // channel grid, and the Manual (single-downloads) view. Each is a no-op
  // when unaffected, and all run whether or not Browse is the active tab so
  // hidden grids stay preloaded (no "pop-in" on return). `channel` is the
  // download's channel name when known, "" otherwise.
  window._onBrowseDownloadLanded = function (channel) {
    const ch = channel || null;
    try { window._refreshVideosViewIfActive && window._refreshVideosViewIfActive(); }
    catch (_e) { /* non-fatal */ }
    try { window._refreshChannelVideosIfLoaded && window._refreshChannelVideosIfLoaded(ch); }
    catch (_e) { /* non-fatal */ }
    try { window._refreshManualViewIfActive && window._refreshManualViewIfActive(); }
    catch (_e) { /* non-fatal */ }
  };
})();
