/**
 * web/browseSearch.js — Browse tab "Search" sub-mode + supporting bits.
 *
 * Extracted from app.js. Three closely-related responsibilities:
 *   1. Search viewer pane — shows context around a clicked search hit
 *      with Up/Down "load earlier / later" expansion.
 *   2. Un-indexed warning banner — shown on Search + Graph views when
 *      transcripts exist on disk but haven't been ingested into FTS.
 *   3. Browse > Search sub-mode — the search input + run button + result
 *      list, with channel multi-select + transcripts/titles toggles.
 *
 * Exposed:
 *   - window.initSearchView (called by app.js boot)
 *   - window._loadSearchViewer (used by result-row clicks)
 *   - window._searchSelectedChannels (used elsewhere)
 *   - window._refreshUnindexedWarning (used by tab-switch listeners)
 *
 * Depends on:
 *   - window._browseState (app.js)
 *   - window._formatTs (util.js)
 *   - window.YT.util.escapeHtml (util.js, falls back to local escaper)
 *   - window.YT.bridge.bridgeCall / isUp (bridge.js)
 */
(function () {
  "use strict";

  const _browseState = window._browseState || {};
  const _formatTs = (sec) => (window._formatTs ? window._formatTs(sec) : String(sec));
  const escapeHtml = window.YT?.util?.escapeHtml || ((s) => String(s ?? "")
    .replace(/[&<>"']/g, (ch) => ({
      "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;", "'": "&#39;",
    }[ch])));
  const displayText = window.YT?.util?.displayText || ((s) => String(s ?? ""));
  function bridgeCall(method, ...args) {
    const fn = window.YT?.bridge?.bridgeCall;
    if (fn) return fn(method, ...args);
    return undefined;
  }

  function nativeBridgeUp() {
    return !!window.YT?.bridge?.isUp?.();
  }

  // ─── Search viewer pane — shows context around a clicked hit ────────
  // Mirrors YTArchiver.py:29598 PanedWindow viewer. Loads N segments
  // before + hit + N segments after via `api.browse_search_context`, and
  // renders them with the hit highlighted. Up/Down "Load earlier / later"
  // buttons expand the window in chunks.
  const _searchViewerState = {
    segmentId: null,
    before: 30,
    after: 30,
    query: "",
    title: "",
    seq: 0,
    loading: false,
  };
  let _searchOpenSeq = 0;
  let _selectedSearchResult = null;

  function _paintSearchPlay(result, query) {
    _selectedSearchResult = result ? { ...result, _query: query || "" } : null;
    const button = document.getElementById("search-viewer-play");
    if (!button) return;
    button.hidden = !result;
    button.textContent = result?._match_kind === "title"
      ? "Open in Watch" : `Play at ${_formatTs(Number(result?.start_time) || 0)}`;
  }

  function _paintSearchDateFilter() {
    const range = window._searchExactDateRange;
    const wrap = document.getElementById("search-date-filter");
    const label = document.getElementById("search-date-filter-label");
    if (wrap) wrap.hidden = !range;
    if (!label || !range) return;
    const raw = String(range.label || "Selected date range");
    const match = /^(\d{4})-(\d{2})$/.exec(raw);
    label.textContent = match
      ? new Date(Number(match[1]), Number(match[2]) - 1, 1)
        .toLocaleDateString(undefined, { month: "long", year: "numeric" }) : raw;
  }
  window._paintSearchDateFilter = _paintSearchDateFilter;

  function _setSearchViewerLoading(loading) {
    _searchViewerState.loading = !!loading;
    const bEarly = document.getElementById("search-viewer-earlier");
    const bLater = document.getElementById("search-viewer-later");
    if (bEarly) bEarly.disabled = !!loading;
    if (bLater) bLater.disabled = !!loading;
  }

  function _resetSearchViewerPane() {
    _paintSearchPlay(null);
    const body = document.getElementById("search-viewer-body");
    const title = document.getElementById("search-viewer-title");
    const meta = document.getElementById("search-viewer-meta");
    const bEarly = document.getElementById("search-viewer-earlier");
    const bLater = document.getElementById("search-viewer-later");
    if (body) body.innerHTML = "";
    if (title) title.textContent = "Select a result to read";
    if (meta) meta.textContent = "";
    if (bEarly) bEarly.hidden = true;
    if (bLater) bLater.hidden = true;
  }

  async function _loadSearchViewer(resultRow, query) {
    const body = document.getElementById("search-viewer-body");
    const titleEl = document.getElementById("search-viewer-title");
    const metaEl = document.getElementById("search-viewer-meta");
    const bEarly = document.getElementById("search-viewer-earlier");
    const bLater = document.getElementById("search-viewer-later");
    if (!body || !titleEl) return;
    _paintSearchPlay(resultRow, query);
    // A title-table hit identifies a video, not a transcript segment. Asking
    // browse_search_context for an absent segment_id produces the technically
    // accurate but misleading "Segment not found" message. Keep the useful
    // double-click action on the result row and explain the single-click state
    // without making a backend call that cannot succeed.
    const hasSegmentId = resultRow?.segment_id !== undefined
      && resultRow?.segment_id !== null
      && String(resultRow.segment_id).trim() !== "";
    if (resultRow?._match_kind === "title" || !hasSegmentId) {
      _searchViewerState.seq++;
      _searchViewerState.segmentId = null;
      _searchViewerState.before = 30;
      _searchViewerState.after = 30;
      _searchViewerState.query = query || "";
      _searchViewerState.title = resultRow?.title || "";
      _searchViewerState._videoId = resultRow?.video_id || "";
      _searchViewerState._jsonlPath = resultRow?.jsonl_path || "";
      _searchViewerState._channel = resultRow?.channel || "";
      _setSearchViewerLoading(false);
      titleEl.textContent = displayText(resultRow?.title || "(untitled)");
      if (metaEl) {
        metaEl.textContent = resultRow?.channel
          ? `${displayText(resultRow.channel)} \u00b7 Title match`
          : "Title match";
      }
      if (bEarly) bEarly.hidden = true;
      if (bLater) bLater.hidden = true;
      body.innerHTML = "";
      const message = document.createElement("div");
      message.className = "browse-empty";
      message.textContent = "No transcript available for this title match. "
        + "Choose Open in Watch to play the video.";
      body.appendChild(message);
      return;
    }
    if (!nativeBridgeUp()) {
      _searchViewerState.seq++;
      _searchViewerState.segmentId = null;
      _setSearchViewerLoading(false);
      if (bEarly) bEarly.hidden = true;
      if (bLater) bLater.hidden = true;
      body.innerHTML = '<div class="browse-empty">The transcript viewer isn\'t ready yet. Try again in a moment.</div>';
      return;
    }
    _searchViewerState.segmentId = resultRow.segment_id;
    _searchViewerState.before = 30;
    _searchViewerState.after = 30;
    _searchViewerState.query = query || "";
    _searchViewerState.title = resultRow.title || "";
    _searchViewerState._videoId = resultRow.video_id || "";
    _searchViewerState._jsonlPath = resultRow.jsonl_path || "";
    _searchViewerState._channel = resultRow.channel || "";

    titleEl.textContent = displayText(resultRow.title || "(untitled)");
    metaEl.textContent = `${displayText(resultRow.channel)} \u00b7 ${_formatTs(resultRow.start_time)}`;
    const mySeq = ++_searchViewerState.seq;
    _setSearchViewerLoading(true);
    if (bEarly) bEarly.hidden = true;
    if (bLater) bLater.hidden = true;
    body.innerHTML = '<div class="browse-empty search-loading">Loading context\u2026</div>';

    let ctx;
    try {
      ctx = await bridgeCall("browse_search_context", {
        segment_id: resultRow.segment_id,
        before: _searchViewerState.before,
        after: _searchViewerState.after,
        query: _searchViewerState.query,
      });
    } catch (e) {
      if (mySeq !== _searchViewerState.seq) return;
      _setSearchViewerLoading(false);
      body.innerHTML = `<div class="browse-empty">Error: ${escapeHtml(String(e))}</div>`;
      return;
    }
    if (mySeq !== _searchViewerState.seq) return;
    _setSearchViewerLoading(false);
    if (!ctx?.ok) {
      body.innerHTML = `<div class="browse-empty">${escapeHtml(ctx?.error || "No context available.")}</div>`;
      return;
    }
    _renderSearchViewer(ctx, resultRow.segment_id);
    if (bEarly) bEarly.hidden = !ctx.before_more;
    if (bLater) bLater.hidden = !ctx.after_more;
  }

  function _renderSearchViewer(ctx, clickedId) {
    const body = document.getElementById("search-viewer-body");
    if (!body) return;
    body.innerHTML = "";
    const frag = document.createDocumentFragment();
    const q = _searchViewerState.query;
    // Strip FTS5 operator tokens before highlighting. Old code only
    // dropped quotes/star, so a query like "cats AND dogs" highlighted
    // the literal word "and" in every viewer segment (audit:
    // browseSearch.js:96). FTS5 ops we filter: AND, OR, NOT, NEAR,
    // and leading/embedded ^.
    const _FTS_OPS = new Set(["and", "or", "not", "near"]);
    const qWords = q
      ? q.toLowerCase()
         .replace(/["*]/g, "")
         .replace(/\bnear\s*\([^)]*\)/g, " ") // NEAR(...) → drop entirely
         .replace(/[\^()]/g, " ")
         .split(/\s+/)
         .filter(w => w && !_FTS_OPS.has(w))
      : [];
    // Pre-compile the alternation regex ONCE per render. Building it
    // inside the per-segment loop wasted ~N-1 RegExp compilations on a
    // 50-segment context window (same pattern, same flags). Hoisted
    // out so each segment just runs `re.exec` against fresh text.
    const _hlParts = qWords
      .filter(Boolean)
      .map(w => w.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"));
    const _hlRe = _hlParts.length ? new RegExp("(" + _hlParts.join("|") + ")", "gi") : null;
    let scrollTarget = null;
    for (const seg of (ctx.segments || [])) {
      const row = document.createElement("div");
      row.className = "sv-seg" + (seg.is_hit ? " hit" : "");
      if (seg.id === clickedId) row.classList.add("clicked");
      const tsEl = document.createElement("span");
      tsEl.className = "sv-ts";
      tsEl.textContent = `[${_formatTs(seg.s)}]`;
      const txtEl = document.createElement("span");
      txtEl.className = "sv-text";
      // Highlight query words inside the text for visual parity with the
      // snippet <mark> tags from the list side.
      // Build the highlight via DOM nodes (createTextNode + <mark>)
      // instead of innerHTML. The old multi-pass `html.replace(re, '<mark>')`
      // pattern was vulnerable to two issues: (a) overlapping matches from
      // sequential passes corrupted previously-inserted <mark> tags, and
      // (b) any future change that loosened the escapeHtml step would
      // let raw HTML from segment text reach innerHTML. textContent is
      // immune to both.
      const _txt = seg.t || "";
      if (_hlRe) {
        // Reuse the hoisted alternation regex; reset lastIndex
        // since /g state persists across exec() calls.
        _hlRe.lastIndex = 0;
        let last = 0;
        let m;
        while ((m = _hlRe.exec(_txt)) !== null) {
          if (m.index > last) {
            txtEl.appendChild(document.createTextNode(_txt.slice(last, m.index)));
          }
          const mk = document.createElement("mark");
          mk.textContent = m[0];
          txtEl.appendChild(mk);
          last = m.index + m[0].length;
          // Guard against zero-width matches (empty alternation branch).
          if (m[0].length === 0) _hlRe.lastIndex++;
        }
        if (last < _txt.length) {
          txtEl.appendChild(document.createTextNode(_txt.slice(last)));
        }
      } else {
        txtEl.textContent = _txt;
      }
      row.append(tsEl, txtEl);
      // Click a segment in the viewer → open in Watch view at that ts
      row.addEventListener("click", () => {
        _openSearchResultInWatch(_searchViewerState, seg);
      });
      if (seg.id === clickedId) scrollTarget = row;
      frag.appendChild(row);
    }
    body.appendChild(frag);
    if (scrollTarget) {
      setTimeout(() => {
        scrollTarget.scrollIntoView({ behavior: "instant", block: "center" });
      }, 20);
    }
  }

  async function _openResolvedSearchHit(hit, seekTo, query) {
    if (!nativeBridgeUp()) return;
    // Copy every field before awaiting. The viewer state object is reused
    // for each selection, so retaining it here let a late A response mix
    // A's resolved file with B's title/video id.
    const snapshot = {
      jsonlPath: hit?.jsonl_path || hit?._jsonlPath || "",
      videoId: hit?.video_id || hit?._videoId || "",
      title: hit?.title || "",
      channel: hit?.channel || hit?._channel || "",
      seekTo: Number(seekTo) || 0,
      query: String(query || ""),
    };
    const openSeq = ++_searchOpenSeq;
    const intentToken = window._reserveWatchOpenIntent?.();
    const originSubmode = window._browseState?.submode;
    const browseTab = document.querySelector('.tab[data-tab="browse"]');
    const originWasActive = !!browseTab?.classList.contains("active");
    const originIsCurrent = () =>
      (!originWasActive || !!browseTab?.classList.contains("active"))
      && (!originSubmode || window._browseState?.submode === originSubmode);
    try {
      const res = await bridgeCall("browse_resolve_segment",
        snapshot.jsonlPath, snapshot.videoId, snapshot.title);
      if (openSeq !== _searchOpenSeq) return;
      if (Number.isFinite(intentToken)
          && !window._isWatchOpenIntentCurrent?.(intentToken)) return;
      if (!originIsCurrent()) return;
      if (!res?.ok || !res.filepath) {
        window._showToast?.(
          res?.error || "Couldn't resolve source video.", "warn");
        return;
      }
      if (typeof window._openVideoInWatch !== "function") {
        window._showToast?.("Watch view is not ready yet.", "warn");
        return;
      }
      // Use the one canonical Watch opener. It owns transcript loading,
      // the Watch navigation token, source loading and seek handling.
      await window._openVideoInWatch({
        filepath: res.filepath,
        title: res.title || snapshot.title,
        channel: res.channel || snapshot.channel,
        video_id: res.video_id || snapshot.videoId,
        uploaded: res.upload_ts ? window._formatBrowseUploadAge?.(res.upload_ts) : "",
        duration: res.duration || "",
        views: res.views || "",
        tracked: res.tracked !== false,
        _seek_to: snapshot.seekTo,
        _search_query: snapshot.query,
      }, { intentToken });
    } catch (err) {
      if (openSeq !== _searchOpenSeq) return;
      if (Number.isFinite(intentToken)
          && !window._isWatchOpenIntentCurrent?.(intentToken)) return;
      if (!originIsCurrent()) return;
      // surface resolve failures so the user sees WHY a
      // segment click did nothing. Old .catch(() => {}) silently
      // ate every error (missing file, backend exception, network)
      // and left the user clicking with zero feedback.
      console.warn("[search] _openSearchResultInWatch failed:", err);
      window._showToast?.(
        `Could not open segment: ${err?.message || err || "unknown error"}`,
        "error");
    }
  }

  function _openSearchResultInWatch(state, seg) {
    _openResolvedSearchHit(state, seg?.s, state?.query);
  }

  async function _expandSearchViewer(beforeDelta, afterDelta, direction) {
    if (!_searchViewerState.segmentId || _searchViewerState.loading) return;
    const request = {
      segmentId: _searchViewerState.segmentId,
      before: _searchViewerState.before + beforeDelta,
      after: _searchViewerState.after + afterDelta,
      query: _searchViewerState.query,
    };
    const mySeq = ++_searchViewerState.seq;
    _setSearchViewerLoading(true);
    let ctx;
    try {
      ctx = await bridgeCall("browse_search_context", {
        segment_id: request.segmentId,
        before: request.before,
        after: request.after,
        query: request.query,
      });
    } catch (e) {
      if (mySeq !== _searchViewerState.seq) return;
      _setSearchViewerLoading(false);
      window._showToast?.(`Couldn't load ${direction} context: ${e}`, "error");
      return;
    }
    if (mySeq !== _searchViewerState.seq) return;
    _setSearchViewerLoading(false);
    if (!ctx?.ok) {
      window._showToast?.(
        ctx?.error || `Couldn't load ${direction} context.`, "error");
      return;
    }
    _searchViewerState.before = request.before;
    _searchViewerState.after = request.after;
    _renderSearchViewer(ctx, request.segmentId);
    const bEarly = document.getElementById("search-viewer-earlier");
    const bLater = document.getElementById("search-viewer-later");
    if (bEarly) bEarly.hidden = !ctx.before_more;
    if (bLater) bLater.hidden = !ctx.after_more;
  }

  function _initSearchViewerLoadMore() {
    // guard against double-wire. If this function runs
    // twice (hot reload, re-entered after view switch), each call
    // added another click handler and "Load earlier / later" fired
    // N times per click — queuing duplicate API calls and rendering
    // duplicate segments.
    if (_initSearchViewerLoadMore._wired) return;
    _initSearchViewerLoadMore._wired = true;
    // Resolve the bridge per-click via bridgeCall rather than capturing
    // window.pywebview.api once here — at init (boot) the bridge often
    // isn't injected yet, so a captured reference would be a stale
    // undefined when the buttons are actually clicked later.
    document.getElementById("search-viewer-earlier")?.addEventListener("click", async () => {
      await _expandSearchViewer(30, 0, "earlier");
    });
    document.getElementById("search-viewer-later")?.addEventListener("click", async () => {
      await _expandSearchViewer(0, 30, "later");
    });
  }

  // ─── Un-indexed warning banner (Search + Graph views) ───────────────
  // Fetches the count of transcript files on disk that aren't in the FTS
  // index, shows/hides the amber banner accordingly. Mirrors
  // YTArchiver.py:24756 _update_index_warning.
  async function _refreshUnindexedWarning() {
    if (!nativeBridgeUp()) return;
    let res;
    try { res = await bridgeCall("index_unindexed_count"); } catch { return; }
    if (!res?.ok) return;
    const n = Number(res.unindexed) || 0;
    const show = n > 0;
    for (const pair of [
      ["search-unindexed-warning", "search-unindexed-text"],
      ["graph-unindexed-warning", "graph-unindexed-text"],
    ]) {
      const banner = document.getElementById(pair[0]);
      const txt = document.getElementById(pair[1]);
      if (!banner) continue;
      banner.hidden = !show;
      if (txt) {
        txt.textContent = show
          ? `${n.toLocaleString()} transcript file${n === 1 ? "" : "s"} on disk ` +
            "aren't yet in the search index. Results + graph may be incomplete " +
            "until you rescan."
          : "";
      }
    }
  }
  window._refreshUnindexedWarning = _refreshUnindexedWarning;

  // Wire the "Rescan now" buttons on each banner to fire archive_rescan.
  function _initUnindexedRescanBtns() {
    const handler = async (e) => {
      e.preventDefault();
      if (!nativeBridgeUp()) {
        window._showToast?.("YTArchiver isn't ready yet. Try again in a moment.", "warn");
        return;
      }
      try {
        const result = await bridgeCall("archive_rescan");
        if (!result?.ok || result?.started === false) {
          window._showToast?.(
            result?.error || "Rescan did not start.", "error");
          return;
        }
        window._showToast?.("Archive rescan started.", "ok");
        // Poll until the rescan clears the unindexed count or times out.
        let tries = 0;
        const tick = async () => {
          tries++;
          await _refreshUnindexedWarning();
          const banner = document.getElementById("search-unindexed-warning");
          if (banner && !banner.hidden && tries < 60) {
            setTimeout(tick, 2000);
          }
        };
        setTimeout(tick, 1500);
      } catch (err) {
        window._showToast?.("Rescan failed.", "error");
      }
    };
    document.getElementById("search-rescan-btn")?.addEventListener("click", handler);
    document.getElementById("graph-rescan-btn")?.addEventListener("click", handler);
  }

  // ─── Browse > Search sub-mode ────────────────────────────────────────
  function initSearchView() {
    _initUnindexedRescanBtns();
    _initSearchViewerLoadMore();
    // Fire once on first Search-view click; refresh when user re-enters.
    document.querySelector('[data-view="search"]')?.addEventListener("click", _refreshUnindexedWarning);
    document.querySelector('[data-view="graph"]')?.addEventListener("click", _refreshUnindexedWarning);
    // And once on boot so the banner is correct right away.
    setTimeout(_refreshUnindexedWarning, 800);

    const input = document.getElementById("search-query");
    const scope = document.getElementById("search-scope");
    const btn = document.getElementById("btn-search-run");
    const results = document.getElementById("search-results");
    const counter = document.getElementById("search-count");
    document.getElementById("search-viewer-play")?.addEventListener("click", () => {
      const hit = _selectedSearchResult;
      if (hit) _openResolvedSearchHit(hit, hit.start_time, hit._query);
    });
    // Stale-response guard: each doSearch invocation bumps _searchSeq.
    // If the user types more before the API call returns, the late
    // response sees `myId !== _searchSeq` and bails — the most-recent
    // search wins, not whichever response arrived last.
    let _searchSeq = 0;
    const doSearch = async () => {
      const myId = ++_searchSeq;
      // A new result set invalidates context and file-resolution work from
      // the previous one. Otherwise an old double-click can finish later
      // and unexpectedly open a video that is no longer on screen.
      _searchOpenSeq++;
      _searchViewerState.seq++;
      _searchViewerState.segmentId = null;
      _setSearchViewerLoading(false);
      _resetSearchViewerPane();
      const q = (input?.value || "").trim();
      if (window._searchExactDateRange && window._searchExactDateRange.query !== q) {
        window._searchExactDateRange = null;
      }
      _paintSearchDateFilter();
      if (!q) {
        results.innerHTML = '<div class="browse-empty">Type a query and press Search or Enter.</div>';
        counter.textContent = "\u2014";
        return;
      }
      results.innerHTML = '<div class="search-progress-bar" id="search-progress-bar"></div>' +
                          '<div class="browse-empty">Searching\u2026</div>';
      counter.textContent = "\u2026";
      if (!nativeBridgeUp()) {
        results.innerHTML = '<div class="browse-empty">Search isn\'t ready yet. Try again in a moment.</div>';
        counter.textContent = "Search unavailable";
        return;
      }
      // Read what + where from the new search UI.
      //   wantTranscripts / wantTitles → which backends to call
      //   selectedChannels → array of channel folders (empty = all)
      // Legacy callers (Graph drill-in via _drillIntoSearch) still set
      // the hidden #search-scope select; we honor those signals so old
      // entry points keep working.
      const inTx = document.getElementById("search-in-transcripts");
      const inTi = document.getElementById("search-in-titles");
      let wantTranscripts = inTx ? inTx.checked : true;
      let wantTitles = inTi ? inTi.checked : false;
      const legacyScope = (scope?.value || "all");
      if (legacyScope === "titles") {
        wantTranscripts = false; wantTitles = true;
      }
      const selectedChannels = (typeof window._searchSelectedChannels === "function")
        ? (window._searchSelectedChannels() || [])
        : ((legacyScope === "channel" && _browseState.currentChannel?.folder)
            ? [_browseState.currentChannel.folder]
            : []);
      if (!wantTranscripts && !wantTitles) {
        results.innerHTML = '<div class="browse-empty">Pick at least one of Transcripts or Video Title.</div>';
        counter.textContent = "—";
        return;
      }
      // Sort selection — backend honors "relevance", "newest",
      // "oldest", "channel", "title". For the titles leg, "relevance"
      // is meaningless (LIKE has no score), so we map it to "newest"
      // before the call. Both legs are sorted server-side so the merged
      // result list arrives in the user's chosen order.
      const sortSel = document.getElementById("search-sort");
      const sortKey = (sortSel?.value || "relevance");
      const sortKeyTitles = (sortKey === "relevance") ? "newest" : sortKey;
      // Year window (inclusive). Blank / non-numeric → no bound. These
      // were previously collected by the UI but never sent, so the filter
      // did nothing; now both legs receive them.
      const _parseYear = (el) => {
        const n = parseInt((el?.value || "").trim(), 10);
        return Number.isFinite(n) ? n : null;
      };
      let yearFrom = _parseYear(document.getElementById("search-year-from"));
      let yearTo = _parseYear(document.getElementById("search-year-to"));
      // Inverted range (from > to) used to be silently ignored (fell back to
      // all years). Auto-swap to the obviously-intended window, and reflect
      // the swap back into the inputs so the correction is visible, not silent.
      if (yearFrom != null && yearTo != null && yearFrom > yearTo) {
        const _tmp = yearFrom; yearFrom = yearTo; yearTo = _tmp;
        const _fEl = document.getElementById("search-year-from");
        const _tEl = document.getElementById("search-year-to");
        if (_fEl) _fEl.value = String(yearFrom);
        if (_tEl) _tEl.value = String(yearTo);
      }
      let exactRange = window._searchExactDateRange || null;
      if (exactRange && String(exactRange.query || "") !== q) {
        exactRange = null;
        window._searchExactDateRange = null;
      }
      const dateFromTs = Number.isFinite(Number(exactRange?.fromTs))
        ? Number(exactRange.fromTs) : null;
      const dateToTs = Number.isFinite(Number(exactRange?.toTs))
        ? Number(exactRange.toTs) : null;
      try {
        const runLeg = async (enabled, label, call, mapRow) => {
          if (!enabled) return { rows: [], error: null, skipped: true, label };
          try {
            const response = await call();
            if (!Array.isArray(response)) {
              throw new Error(response?.error || "Invalid response from search service");
            }
            return {
              rows: response.map(mapRow), error: null, skipped: false, label,
            };
          } catch (error) {
            return { rows: [], error, skipped: false, label };
          }
        };
        // Keep failures separate from legitimate empty result sets. With
        // both boxes checked, one failed leg used to become [] and the UI
        // quietly presented the other leg as a complete search.
        const outcome = await window.YT.bridge.catalogRead(
          "search",
          async (context) => {
            // The backend serializes these catalog reads on one connection.
            // Run the selected legs one at a time so a combined search does
            // not create two Python calls that merely wait on each other.
            const txResult = await runLeg(
              wantTranscripts,
              "Transcript search",
              () => bridgeCall("browse_search", q, selectedChannels, 200,
                               sortKey, yearFrom, yearTo,
                               dateFromTs, dateToTs),
              r => ({ ...r, _match_kind: "transcript" }));
            if (!context.isCurrent()) return null;
            const tiResult = await runLeg(
              wantTitles,
              "Title search",
              () => bridgeCall("browse_search_titles", q, selectedChannels, 200,
                               sortKeyTitles, yearFrom, yearTo,
                               dateFromTs, dateToTs),
              r => ({
                ...r,
                text: r.title || "",
                // The renderer below uses text nodes. Pre-escaping here made
                // an ordinary title such as "A & B" display as "A &amp; B".
                snippet: r.title || "",
                start_time: 0,
                jsonl_path: "",
                _match_kind: "title",
              }));
            return { txResult, tiResult };
          },
          {
            label: "search results",
            onStatus: (status) => {
              if (status.phase === "done" || myId !== _searchSeq) return;
              const loading = results.querySelector(".browse-empty");
              if (loading) loading.textContent = status.phase === "loading"
                ? "Searching…"
                : status.text;
              if (status.phase === "slow") {
                counter.textContent = `Waiting ${Math.floor(status.elapsedMs / 1000)}s`;
              }
            },
          });
        if (outcome.stale || myId !== _searchSeq || !outcome.value) return;
        const { txResult, tiResult } = outcome.value;
        const txRows = txResult.rows;
        const tiRows = tiResult.rows;
        const legFailures = [txResult, tiResult]
          .filter(result => !result.skipped && result.error);
        const failureSummary = legFailures.map((result) => {
          const detail = result.error?.message || String(result.error || "unknown error");
          return `${result.label} failed: ${detail}`;
        }).join("; ");
        const requestedLegCount = [txResult, tiResult]
          .filter(result => !result.skipped).length;
        // Each leg is server-capped at 200 rows. If a leg came back full,
        // there are almost certainly more matches than we're showing — flag
        // it so the count reads "N+ … (capped)" instead of implying N is the
        // true total. (The cap keeps common-word searches snappy.)
        const _LEG_CAP = 200;
        const capped = (Array.isArray(txRows) && txRows.length >= _LEG_CAP) ||
                       (Array.isArray(tiRows) && tiRows.length >= _LEG_CAP);
        let rows = [...txRows, ...tiRows];
        // Re-sort the merged list by the user's chosen sort key. Each
        // leg was sorted server-side, but a naive [...txRows, ...tiRows]
        // concat put every transcript hit before every title hit
        // regardless of date / channel / title (audit:
        // browseSearch.js:381). For "relevance" we leave the order as-is
        // (transcripts have an FTS score, titles don't — no meaningful
        // unified score).
        if (rows.length > 1 && wantTranscripts && wantTitles && sortKey !== "relevance") {
          const _cmpStr = (a, b) =>
            String(a || "").localeCompare(String(b || ""),
                                          undefined, { sensitivity: "base" });
          const _cmpNum = (a, b) => (Number(a) || 0) - (Number(b) || 0);
          // Deterministic tie-break on video_id so equal-key rows
          // (same upload_ts, same channel, etc.) don't shuffle order
          // on every re-sort (audit: browseSearch.js H147).
          rows.sort((a, b) => {
            if (!a || !b) return 0;
            let _r = 0;
            if (sortKey === "newest")       _r = _cmpNum(b.added_ts, a.added_ts);
            else if (sortKey === "oldest")  _r = _cmpNum(a.added_ts, b.added_ts);
            else if (sortKey === "channel") _r = _cmpStr(a.channel, b.channel);
            else if (sortKey === "title")   _r = _cmpStr(a.title, b.title);
            if (_r !== 0) return _r;
            return _cmpStr(a.video_id, b.video_id);
          });
        }
        // When BOTH legs ran, the per-row renderer below prepends a
        // tiny [title]/[transcript] badge to the snippet so the user
        // can tell the source at a glance. Single-leg searches stay
        // unbadged to match the pre-refactor look.
        //
        // Badge HTML used to be string-concatenated into r.snippet
        // here, but Patch B's text-node snippet renderer treats every
        // non-<mark> chunk as literal text — so the raw `<span ...>`
        // markup leaked into the result rows as escaped angle-bracket
        // noise. Build the badge as a real DOM element down in the
        // render loop instead, keying off r._match_kind.
        const bothLegs = wantTranscripts && wantTitles;
        if (!Array.isArray(rows) || rows.length === 0) {
          let errMsg = "No matches.";
          if (legFailures.length === requestedLegCount) {
            errMsg = `Search failed. ${failureSummary}. Please try again.`;
          } else if (legFailures.length) {
            errMsg = `No matches in the available results. ${failureSummary}.`;
          }
          results.innerHTML = `<div class="browse-empty">${escapeHtml(errMsg)}</div>`;
          counter.textContent = legFailures.length ? "Search incomplete" : "0 matches";
          // Clear the right-hand reader pane — otherwise the previously
          // selected result's transcript lingers next to a "No matches".
          _resetSearchViewerPane();
          return;
        }
        const _matchWord = rows.length === 1 ? "match" : "matches";
        counter.textContent = (capped
          ? `${rows.length.toLocaleString()}+ ${_matchWord} (capped)`
          : `${rows.length.toLocaleString()} ${_matchWord}`)
          + (exactRange?.label ? ` · ${exactRange.label}` : "")
          + (legFailures.length ? " · partial" : "");
        const frag = document.createDocumentFragment();
        if (legFailures.length) {
          const warning = document.createElement("div");
          warning.className = "browse-hint";
          warning.textContent = `Some results may be missing. ${failureSummary}. `
            + "Try the search again.";
          frag.appendChild(warning);
        }
        for (const r of rows) {
          const row = document.createElement("div");
          row.className = "search-result";
          row.innerHTML = `
            <div class="search-result-head">
              <span class="search-result-title"></span>
              <span class="search-result-meta"></span>
            </div>
            <div class="search-result-snippet"></div>
          `;
          row.querySelector(".search-result-title").textContent =
            displayText(r.title || "(untitled)");
          row.querySelector(".search-result-meta").textContent =
            `${displayText(r.channel)} \u00b7 ${_formatTs(r.start_time)}`;
          // Patch B (XSS hardening): build snippet via DOM nodes
          // instead of innerHTML. FTS5 wraps matched terms in <mark>
          // tags, but the surrounding text comes from YouTube
          // transcripts/titles — theoretically containing HTML
          // metacharacters. Split on <mark>...</mark> and rebuild
          // with textContent so any user content is treated as
          // literal text, never parsed as HTML.
          const _snipEl = row.querySelector(".search-result-snippet");
          // When both legs ran, prepend the [title]/[transcript]
          // badge as a real DOM node — the previous string-concat path
          // got swallowed by the text-node renderer below (2026-05-20).
          if (bothLegs && r._match_kind) {
            const _badge = document.createElement("span");
            _badge.className =
              `search-match-pill search-match-${r._match_kind}`;
            _badge.textContent = r._match_kind;
            _snipEl.appendChild(_badge);
            _snipEl.appendChild(document.createTextNode(" "));
          }
          const _rawSnip = displayText(r.snippet || r.text || "");
          const _snipParts = _rawSnip.split(/<mark>([\s\S]*?)<\/mark>/);
          for (let _si = 0; _si < _snipParts.length; _si++) {
            if (_si % 2 === 0) {
              _snipEl.appendChild(document.createTextNode(_snipParts[_si]));
            } else {
              const _mk = document.createElement("mark");
              _mk.textContent = _snipParts[_si];
              _snipEl.appendChild(_mk);
            }
          }
          row.title = "Double-click to open in Watch view at this timestamp";
          const openHit = () => _openResolvedSearchHit(
            r, r.start_time, q);
          // Single-click → load context in the right-side viewer pane
          // (stay on the Search view). Double-click → open in Watch view.
          row.addEventListener("click", () => {
            results.querySelectorAll(".search-result.selected")
                   .forEach(x => x.classList.remove("selected"));
            row.classList.add("selected");
            _loadSearchViewer(r, q);
          });
          row.addEventListener("dblclick", (e) => { e.preventDefault(); openHit(); });
          frag.appendChild(row);
        }
        results.innerHTML = "";
        results.appendChild(frag);
      } catch (e) {
        if (myId !== _searchSeq) return;
        results.innerHTML = `<div class="browse-empty">Search failed: ${escapeHtml(String(e))}</div>`;
        counter.textContent = "Search failed";
      }
    };
    btn?.addEventListener("click", doSearch);
    // Repeat-fire guard: holding Enter generates a stream of keydown events
    // (autorepeat). Each one would queue a new doSearch and pile up API
    // calls. e.repeat short-circuits the autorepeat stream so we only
    // fire once per physical Enter press. _searchSeq still drops any
    // stale responses if the user does press Enter multiple times.
    input?.addEventListener("keydown", (e) => {
      if (e.key !== "Enter" || e.repeat) return;
      // IME composition Enter doesn't set e.repeat but DOES set
      // e.isComposing — skip those so picking a Japanese/Korean
      // candidate doesn't fire a search prematurely (audit:
      // browseSearch.js H187).
      if (e.isComposing) return;
      doSearch();
    });
    input?.addEventListener("input", () => {
      window._searchExactDateRange = null;
      _paintSearchDateFilter();
    });
    // Re-run on sort change so the user doesn't have to re-click Search
    // every time they pick a different ordering. Only re-runs when the
    // query box has something in it — avoids a spurious search on first
    // selection.
    document.getElementById("search-sort")?.addEventListener("change", () => {
      if ((input?.value || "").trim()) doSearch();
    });
    // Auto-apply the Year bounds: re-run when either field is committed
    // (blur / Enter / spinner step), so the user doesn't have to click back
    // into the main box and press Enter. Uses "change" (commit), NOT "input",
    // so typing a year digit-by-digit (2 → 20 → 202 → 2026) doesn't fire a
    // search on each partial value. Only re-runs when the query box has text.
    ["search-year-from", "search-year-to"].forEach((id) => {
      document.getElementById(id)?.addEventListener("change", () => {
        window._searchExactDateRange = null;
        _paintSearchDateFilter();
        if ((input?.value || "").trim()) doSearch();
      });
    });

    document.getElementById("search-date-filter-clear")?.addEventListener("click", () => {
      window._searchExactDateRange = null;
      _paintSearchDateFilter();
      if ((input?.value || "").trim()) doSearch();
    });

    // FTS5 operator buttons — click-to-insert at the current cursor position
    document.querySelectorAll(".search-op-btn").forEach(opBtn => {
      opBtn.addEventListener("click", (e) => {
        e.preventDefault();
        if (!input) return;
        const op = opBtn.dataset.op || "";
        const start = input.selectionStart ?? input.value.length;
        const end = input.selectionEnd ?? input.value.length;
        const before = input.value.slice(0, start);
        const selected = input.value.slice(start, end);
        const after = input.value.slice(end);
        let insert;
        if (op === '"…"') {
          // Wrap selection (if any) in double quotes for exact phrase search
          insert = selected ? `"${selected}"` : '""';
        } else if (op === "*") {
          // Append a prefix-match wildcard to the word at cursor
          insert = "*";
        } else {
          // Boolean op — pad with spaces
          insert = `${before.endsWith(" ") ? "" : " "}${op} `;
        }
        const newVal = before + (op === '"…"' ? "" : selected) + insert + after;
        input.value = newVal;
        // Position cursor sensibly
        const newPos = op === '"…"'
          ? start + 1 + selected.length
          : before.length + insert.length + selected.length;
        input.setSelectionRange(newPos, newPos);
        input.focus();
      });
    });

    // ── New search UI wiring: content-type checkboxes + channel multi ──
    _initSearchContentCheckboxes(scope);
    _initSearchChannelMulti(scope);
  }

  /** Wire the two "match against" checkboxes (Transcripts / Video
   * Title). Toggling them grey-out the FTS-only operator buttons and
   * year inputs when Transcripts is unchecked; they only apply to the
   * FTS leg. Also syncs the hidden #search-scope compat shim so the
   * legacy Graph drill-in callers and any third-party readers keep
   * seeing a sensible value. */
  function _initSearchContentCheckboxes(scopeShim) {
    const inTx = document.getElementById("search-in-transcripts");
    const inTi = document.getElementById("search-in-titles");
    const filters = document.querySelector(".search-filters");
    const btnRun = document.getElementById("btn-search-run");
    if (!inTx || !inTi) return;
    const sync = () => {
      const onlyTitles = !inTx.checked && inTi.checked;
      const onlyTranscripts = inTx.checked && !inTi.checked;
      const neither = !inTx.checked && !inTi.checked;
      if (filters) filters.classList.toggle("search-operators-disabled", onlyTitles);
      if (btnRun) btnRun.disabled = neither;
      // Keep the hidden compat select roughly in sync so legacy
      // drill-in callers from Graph view continue to function.
      if (scopeShim) {
        if (onlyTitles) scopeShim.value = "titles";
        else if (onlyTranscripts) scopeShim.value = "all";
        else scopeShim.value = "all";
      }
    };
    inTx.addEventListener("change", sync);
    inTi.addEventListener("change", sync);
    sync();
  }

  /** Initialize the channel multi-select dropdown.
   *
   * The trigger is a styled button; the panel below holds an "All
   * channels" master checkbox plus one row per channel. Master toggles
   * all individuals; toggling any individual unchecks master (and
   * checking the LAST one re-syncs master). Label collapses to "All
   * channels" / "<one name>" / "<N> channels" depending on state.
   * Closes on click-outside.
   *
   * Channel list source: prefer the cached `window._subsAllRows`
   * (populated whenever the Subs tab renders) for instant open; fall
   * back to `api.browse_list_channels` if the cache is empty (e.g.
   * the user hasn't opened Subs yet this session). Re-syncs on each
   * panel open so newly-added channels show up. */
  function _initSearchChannelMulti(scopeShim) {
    const wrap = document.getElementById("search-channel-multi");
    const trigger = document.getElementById("search-channel-trigger");
    const panel = document.getElementById("search-channel-panel");
    const label = document.getElementById("search-channel-label");
    const allCb = document.getElementById("search-channel-all");
    const list = document.getElementById("search-channel-list");
    if (!wrap || !trigger || !panel || !list || !allCb || !label) return;

    // Selected folder names, stored as a Set on the wrap element so it
    // survives panel re-renders. Empty Set = "All channels".
    const selected = new Set();
    wrap._searchSelected = selected;
    let populateSeq = 0;

    const isAllMode = () => selected.size === 0;

    const updateLabel = () => {
      if (isAllMode()) { label.textContent = "All channels"; return; }
      if (selected.size === 1) {
        const only = Array.from(selected)[0];
        // Use the display name if we have one cached
        const row = (window._subsAllRows || []).find(r => r.folder === only);
        label.textContent = displayText(row?.name || row?.folder || only);
        return;
      }
      label.textContent = `${selected.size} channels`;
    };

    const refreshAllCheckbox = () => {
      allCb.checked = isAllMode();
    };

    const paintMessage = (text, isError = false) => {
      list.innerHTML = "";
      const message = document.createElement("div");
      message.className = "search-channel-message";
      if (isError) message.classList.add("is-error");
      message.textContent = text;
      list.appendChild(message);
    };

    const populate = async () => {
      const mySeq = ++populateSeq;
      // Snapshot the channel list. Sort alphabetically by display
      // name so the user can scan it quickly.
      let rows = (window._subsAllRows || []).length
        ? window._subsAllRows
        : (window._browseState?.channels || []);
      if (!rows.length) {
        paintMessage("Loading channels…");
        if (!nativeBridgeUp()) {
          paintMessage("The channel list isn't ready yet. Try again in a moment.", true);
          return;
        }
        try {
          const outcome = await window.YT.bridge.catalogRead(
            "search-channels",
            () => bridgeCall("browse_list_channels"),
            {
              label: "search channels",
              onStatus: (status) => {
                if (mySeq !== populateSeq || status.phase === "done") return;
                paintMessage(status.phase === "loading"
                  ? "Loading channels…"
                  : status.text);
              },
            });
          if (outcome.stale || mySeq !== populateSeq) return;
          if (!Array.isArray(outcome.value)) {
            throw new Error(outcome.value?.error || "Invalid channel-list response");
          }
          rows = outcome.value;
        } catch (error) {
          if (mySeq !== populateSeq) return;
          console.warn("search channel list:", error);
          paintMessage("Couldn’t load channels. Close this list and try again.", true);
          return;
        }
      }
      if (mySeq !== populateSeq) return;
      rows = rows
        .map(r => ({ folder: r.folder || r.name || "", name: r.name || r.folder || "" }))
        .filter(r => r.folder)
        .sort((a, b) => (a.name || a.folder).toLowerCase()
                          .localeCompare((b.name || b.folder).toLowerCase()));
      list.innerHTML = "";
      if (!rows.length) {
        paintMessage("No channels found.");
        return;
      }
      const frag = document.createDocumentFragment();
      for (const r of rows) {
        const opt = document.createElement("label");
        opt.className = "search-channel-opt";
        const cb = document.createElement("input");
        cb.type = "checkbox";
        cb.value = r.folder;
        cb.checked = selected.has(r.folder);
        const sp = document.createElement("span");
        sp.textContent = displayText(r.name || r.folder);
        opt.appendChild(cb);
        opt.appendChild(sp);
        frag.appendChild(opt);
        cb.addEventListener("change", () => {
          if (cb.checked) selected.add(r.folder);
          else selected.delete(r.folder);
          refreshAllCheckbox();
          updateLabel();
          // Keep the hidden compat select in sync: "channel" when
          // exactly one is picked, "all" otherwise.
          if (scopeShim) {
            scopeShim.value = (selected.size === 1) ? "channel" : "all";
          }
        });
      }
      list.appendChild(frag);
    };

    const open = () => {
      panel.hidden = false;
      trigger.classList.add("is-open");
      trigger.setAttribute("aria-expanded", "true");
      populate();
    };
    const close = () => {
      panel.hidden = true;
      trigger.classList.remove("is-open");
      trigger.setAttribute("aria-expanded", "false");
    };
    const toggle = (e) => {
      e.stopPropagation();
      if (panel.hidden) open(); else close();
    };

    trigger.addEventListener("click", toggle);
    wrap.addEventListener("keydown", (e) => {
      if (e.key !== "Escape" || panel.hidden) return;
      e.preventDefault();
      e.stopPropagation();
      close();
      trigger.focus();
    });

    // Master "All channels" — checking it clears all individual
    // selections (and the empty-set state means "all"). Unchecking
    // it doesn't really make sense in isolation (no positive
    // selection), so re-tick it.
    allCb.addEventListener("change", () => {
      if (allCb.checked) {
        selected.clear();
        // Refresh individual checkboxes' visual state
        list.querySelectorAll('input[type="checkbox"]').forEach(cb => { cb.checked = false; });
      } else {
        // Re-tick — empty selection always means "All channels".
        allCb.checked = true;
      }
      updateLabel();
      if (scopeShim) scopeShim.value = "all";
    });

    // Click outside the dropdown closes it.
    document.addEventListener("click", (e) => {
      if (panel.hidden) return;
      if (!wrap.contains(e.target)) close();
    });

    // Expose the read function for doSearch (returns [] when "all").
    window._searchSelectedChannels = () => Array.from(selected);
    window._setSearchSelectedChannels = (channels) => {
      selected.clear();
      for (const name of (channels || [])) if (name) selected.add(name);
      list.querySelectorAll('input[type="checkbox"]').forEach(cb => {
        cb.checked = selected.has(cb.value);
      });
      updateLabel();
      refreshAllCheckbox();
      if (scopeShim) scopeShim.value = selected.size === 1 ? "channel" : "all";
    };

    updateLabel();
    refreshAllCheckbox();
  }

  // Publish the entry points the rest of the app calls into.
  window.initSearchView = initSearchView;
  window._loadSearchViewer = _loadSearchViewer;
  window._refreshUnindexedWarning = _refreshUnindexedWarning;
})();
