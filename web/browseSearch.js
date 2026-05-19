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
 *   - window.YT.util.escapeHtml (util.js, falls back to identity)
 *   - window.pywebview.api.* (native bridge)
 */
(function () {
  "use strict";

  const _browseState = window._browseState || {};
  const _formatTs = (sec) => (window._formatTs ? window._formatTs(sec) : String(sec));
  const escapeHtml = window.YT?.util?.escapeHtml || ((s) => String(s ?? ""));

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
  };

  async function _loadSearchViewer(resultRow, query) {
    const body = document.getElementById("search-viewer-body");
    const titleEl = document.getElementById("search-viewer-title");
    const metaEl = document.getElementById("search-viewer-meta");
    const bEarly = document.getElementById("search-viewer-earlier");
    const bLater = document.getElementById("search-viewer-later");
    if (!body || !titleEl) return;
    const api = window.pywebview?.api;
    if (!api?.browse_search_context) {
      body.innerHTML = '<div class="browse-empty">Viewer pane requires native mode.</div>';
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

    titleEl.textContent = resultRow.title || "(untitled)";
    metaEl.textContent = `${resultRow.channel || ""} \u00b7 ${_formatTs(resultRow.start_time)}`;
    body.innerHTML = '<div class="browse-empty">Loading\u2026</div>';

    let ctx;
    try {
      ctx = await api.browse_search_context({
        segment_id: resultRow.segment_id,
        before: _searchViewerState.before,
        after: _searchViewerState.after,
        query: _searchViewerState.query,
      });
    } catch (e) {
      body.innerHTML = `<div class="browse-empty">Error: ${escapeHtml(String(e))}</div>`;
      return;
    }
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
    const qWords = q ? q.toLowerCase().replace(/["*]/g, "").split(/\s+/).filter(Boolean) : [];
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
      if (qWords.length) {
        const esc = escapeHtml(seg.t || "");
        let html = esc;
        for (const w of qWords) {
          if (!w) continue;
          const re = new RegExp("(" + w.replace(/[.*+?^${}()|[\]\\]/g, "\\$&") + ")", "gi");
          html = html.replace(re, '<mark>$1</mark>');
        }
        txtEl.innerHTML = html;
      } else {
        txtEl.textContent = seg.t || "";
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

  function _openSearchResultInWatch(state, seg) {
    const api = window.pywebview?.api;
    if (!api?.browse_resolve_segment) return;
    api.browse_resolve_segment(state._jsonlPath || "",
                               state._videoId || "",
                               state.title || "").then((res) => {
      if (!res?.ok || !res.filepath) {
        window._showToast?.("Couldn't resolve source video.", "warn");
        return;
      }
      // feature F5: pass the active search query through so the Watch
      // view can auto-pre-fill the transcript Find box, letting the
      // user jump between hits of THIS video's transcript with
      // Enter / Shift+Enter.
      window._openVideoInWatch({
        filepath: res.filepath,
        title: state.title,
        channel: state._channel || res.channel || "",
        video_id: state._videoId || res.video_id || "",
        _seek_to: Number(seg.s) || 0,
        _search_query: state.query || _searchViewerState.query || "",
      });
    }).catch((err) => {
      // surface resolve failures so the user sees WHY a
      // segment click did nothing. Old .catch(() => {}) silently
      // ate every error (missing file, backend exception, network)
      // and left the user clicking with zero feedback.
      console.warn("[search] _openSearchResultInWatch failed:", err);
      window._showToast?.(
        `Could not open segment: ${err?.message || err || "unknown error"}`,
        "error");
    });
  }

  function _initSearchViewerLoadMore() {
    // guard against double-wire. If this function runs
    // twice (hot reload, re-entered after view switch), each call
    // added another click handler and "Load earlier / later" fired
    // N times per click — queuing duplicate API calls and rendering
    // duplicate segments.
    if (_initSearchViewerLoadMore._wired) return;
    _initSearchViewerLoadMore._wired = true;
    const api = window.pywebview?.api;
    document.getElementById("search-viewer-earlier")?.addEventListener("click", async () => {
      if (!_searchViewerState.segmentId) return;
      _searchViewerState.before += 30;
      try {
        const ctx = await api.browse_search_context({
          segment_id: _searchViewerState.segmentId,
          before: _searchViewerState.before,
          after: _searchViewerState.after,
          query: _searchViewerState.query,
        });
        if (ctx?.ok) {
          _renderSearchViewer(ctx, _searchViewerState.segmentId);
          document.getElementById("search-viewer-earlier").hidden = !ctx.before_more;
          document.getElementById("search-viewer-later").hidden = !ctx.after_more;
        }
      } catch (e) {
        window._showToast?.("Couldn't load earlier context: " + e, "error");
      }
    });
    document.getElementById("search-viewer-later")?.addEventListener("click", async () => {
      if (!_searchViewerState.segmentId) return;
      _searchViewerState.after += 30;
      try {
        const ctx = await api.browse_search_context({
          segment_id: _searchViewerState.segmentId,
          before: _searchViewerState.before,
          after: _searchViewerState.after,
          query: _searchViewerState.query,
        });
        if (ctx?.ok) {
          _renderSearchViewer(ctx, _searchViewerState.segmentId);
          document.getElementById("search-viewer-earlier").hidden = !ctx.before_more;
          document.getElementById("search-viewer-later").hidden = !ctx.after_more;
        }
      } catch (e) {
        window._showToast?.("Couldn't load later context: " + e, "error");
      }
    });
  }

  // ─── Un-indexed warning banner (Search + Graph views) ───────────────
  // Fetches the count of transcript files on disk that aren't in the FTS
  // index, shows/hides the amber banner accordingly. Mirrors
  // YTArchiver.py:24756 _update_index_warning.
  async function _refreshUnindexedWarning() {
    const api = window.pywebview?.api;
    if (!api?.index_unindexed_count) return;
    let res;
    try { res = await api.index_unindexed_count(); } catch { return; }
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
      const api = window.pywebview?.api;
      if (!api?.archive_rescan) {
        window._showToast?.("Native mode required.", "warn");
        return;
      }
      window._showToast?.("Rescanning archive for new transcripts\u2026", "ok");
      try {
        await api.archive_rescan();
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
    // Stale-response guard: each doSearch invocation bumps _searchSeq.
    // If the user types more before the API call returns, the late
    // response sees `myId !== _searchSeq` and bails — the most-recent
    // search wins, not whichever response arrived last.
    let _searchSeq = 0;
    const doSearch = async () => {
      const myId = ++_searchSeq;
      const q = (input?.value || "").trim();
      if (!q) {
        results.innerHTML = '<div class="browse-empty">Type a query and press Search or Enter.</div>';
        counter.textContent = "\u2014";
        return;
      }
      results.innerHTML = '<div class="search-progress-bar" id="search-progress-bar"></div>' +
                          '<div class="browse-empty">Searching\u2026</div>';
      counter.textContent = "\u2026";
      const api = window.pywebview?.api;
      if (!api?.browse_search) {
        results.innerHTML = '<div class="browse-empty">Search requires native mode.</div>';
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
      try {
        const promises = [];
        // Transcripts leg — FTS5 against segments.
        if (wantTranscripts) {
          promises.push(
            Promise.resolve(api.browse_search(q, selectedChannels, 200, sortKey))
              .then(rs => (rs || []).map(r => ({ ...r, _match_kind: "transcript" })))
              .catch(() => [])
          );
        } else {
          promises.push(Promise.resolve([]));
        }
        // Titles leg — videos table LIKE search, re-shaped to look
        // like a transcript hit so the renderer below stays unified.
        if (wantTitles) {
          promises.push(
            Promise.resolve(api.browse_search_titles?.(q, selectedChannels, 200, sortKeyTitles))
              .then(rs => (rs || []).map(r => ({
                ...r,
                text: r.title || "",
                snippet: escapeHtml(r.title || ""),
                start_time: 0,
                jsonl_path: "",
                _match_kind: "title",
              })))
              .catch(() => [])
          );
        } else {
          promises.push(Promise.resolve([]));
        }
        const [txRows, tiRows] = await Promise.all(promises);
        if (myId !== _searchSeq) return; // user kicked off a newer search; drop stale results
        let rows = [...txRows, ...tiRows];
        // Surface a backend-shaped error row if either leg returned one.
        const errRow = rows.find(r => r && r.error);
        if (errRow) rows = [errRow];
        // When BOTH legs ran, prepend a tiny [title]/[transcript] tag
        // to each row's snippet so the user can tell the source at a
        // glance. Single-leg searches stay unbadged to match the
        // pre-refactor look.
        const bothLegs = wantTranscripts && wantTitles;
        if (bothLegs) {
          rows = rows.map(r => {
            if (!r || r.error) return r;
            const kind = r._match_kind === "title" ? "title" : "transcript";
            const badge = `<span class="search-match-pill search-match-${kind}">${kind}</span>`;
            return { ...r, snippet: badge + " " + (r.snippet || "") };
          });
        }
        if (!Array.isArray(rows) || rows.length === 0 || (rows[0] && rows[0].error)) {
          const errMsg = (rows && rows[0] && rows[0].error) ? `Search error: ${rows[0].error}` : "No matches.";
          results.innerHTML = `<div class="browse-empty">${escapeHtml(errMsg)}</div>`;
          counter.textContent = "0 matches";
          return;
        }
        counter.textContent = `${rows.length.toLocaleString()} matches`;
        const frag = document.createDocumentFragment();
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
          row.querySelector(".search-result-title").textContent = r.title || "(untitled)";
          row.querySelector(".search-result-meta").textContent =
            `${r.channel || ""} \u00b7 ${_formatTs(r.start_time)}`;
          // Patch B (XSS hardening): build snippet via DOM nodes
          // instead of innerHTML. FTS5 wraps matched terms in <mark>
          // tags, but the surrounding text comes from YouTube
          // transcripts/titles — theoretically containing HTML
          // metacharacters. Split on <mark>...</mark> and rebuild
          // with textContent so any user content is treated as
          // literal text, never parsed as HTML.
          const _snipEl = row.querySelector(".search-result-snippet");
          const _rawSnip = r.snippet || r.text || "";
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
          const openHit = async () => {
            // Resolve the real video file (sibling of the jsonl_path) and
            // build a Watch view that can actually play. Falls back to the
            // transcript-only view when the video file is missing.
            let video = {
              title: r.title, channel: r.channel, video_id: r.video_id,
              start_at: Number(r.start_time) || 0,
            };
            try {
              const res = await api.browse_resolve_segment?.(
                r.jsonl_path || "", r.video_id || "", r.title || "");
              if (res?.ok && res.filepath) {
                video.filepath = res.filepath;
                if (res.channel) video.channel = res.channel;
              }
            } catch { /* leave video.filepath undefined */ }
            _browseState.currentVideo = video;
            showView("watch");
            try {
              const res = await api.browse_get_transcript({
                video_id: r.video_id, jsonl_path: r.jsonl_path, title: video.title || "",
              });
              const segs = Array.isArray(res)
                ? res
                : (res?.segments || []);
              const sourceInfo = (res && !Array.isArray(res)) ? (res.source || null) : null;
              const rendered = segs.map(seg => ({
                ts: _formatTs(seg.s), text: seg.t, words: seg.w, s: seg.s, e: seg.e,
              }));
              window.renderWatchView(video, rendered, sourceInfo);
              // Seek the <video> to the hit's start_time
              const vEl = document.getElementById("watch-video");
              if (vEl && video.start_at > 0) {
                const seek = () => {
                  try { vEl.currentTime = video.start_at; vEl.play?.().catch(() => {}); }
                  catch { /* ignore */ }
                };
                if (vEl.readyState >= 1) seek();
                else vEl.addEventListener("loadedmetadata", seek, { once: true });
              }
              // Scroll transcript to the clicked segment. Uses the
              // container-local scroll helper so we don't also scroll
              // the outer .browse-view (which would push the video
              // out of frame).
              setTimeout(() => {
                const tr = document.getElementById("watch-transcript");
                if (!tr) return;
                const segs = tr.querySelectorAll(".seg");
                for (const s of segs) {
                  const ts = s.querySelector(".timestamp")?.textContent || "";
                  if (ts && ts.includes(_formatTs(r.start_time))) {
                    window._scrollTranscriptTo?.(tr, s);
                    s.classList.add("search-hit");
                    break;
                  }
                }
              }, 80);
            } catch (e) { console.warn(e); }
          };
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
        results.innerHTML = `<div class="browse-empty">Search failed: ${escapeHtml(String(e))}</div>`;
      }
    };
    btn?.addEventListener("click", doSearch);
    input?.addEventListener("keydown", (e) => { if (e.key === "Enter") doSearch(); });
    // Re-run on sort change so the user doesn't have to re-click Search
    // every time they pick a different ordering. Only re-runs when the
    // query box has something in it — avoids a spurious search on first
    // selection.
    document.getElementById("search-sort")?.addEventListener("change", () => {
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

    const isAllMode = () => selected.size === 0;

    const updateLabel = () => {
      if (isAllMode()) { label.textContent = "All channels"; return; }
      if (selected.size === 1) {
        const only = Array.from(selected)[0];
        // Use the display name if we have one cached
        const row = (window._subsAllRows || []).find(r => r.folder === only);
        label.textContent = row?.name || row?.folder || only;
        return;
      }
      label.textContent = `${selected.size} channels`;
    };

    const refreshAllCheckbox = () => {
      allCb.checked = isAllMode();
    };

    const populate = async () => {
      // Snapshot the channel list. Sort alphabetically by display
      // name so the user can scan it quickly.
      let rows = window._subsAllRows || [];
      if (!rows.length) {
        try {
          const api = window.pywebview?.api;
          const res = await api?.browse_list_channels?.();
          if (Array.isArray(res)) rows = res;
        } catch { /* leave empty */ }
      }
      rows = rows
        .map(r => ({ folder: r.folder || r.name || "", name: r.name || r.folder || "" }))
        .filter(r => r.folder)
        .sort((a, b) => (a.name || a.folder).toLowerCase()
                          .localeCompare((b.name || b.folder).toLowerCase()));
      list.innerHTML = "";
      const frag = document.createDocumentFragment();
      for (const r of rows) {
        const opt = document.createElement("label");
        opt.className = "search-channel-opt";
        const cb = document.createElement("input");
        cb.type = "checkbox";
        cb.value = r.folder;
        cb.checked = selected.has(r.folder);
        const sp = document.createElement("span");
        sp.textContent = r.name || r.folder;
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

    const open = async () => {
      await populate();
      panel.hidden = false;
      trigger.classList.add("is-open");
    };
    const close = () => {
      panel.hidden = true;
      trigger.classList.remove("is-open");
    };
    const toggle = (e) => {
      e.stopPropagation();
      if (panel.hidden) open(); else close();
    };

    trigger.addEventListener("click", toggle);

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

    updateLabel();
    refreshAllCheckbox();
  }

  // Publish the entry points the rest of the app calls into.
  window.initSearchView = initSearchView;
  window._loadSearchViewer = _loadSearchViewer;
  window._refreshUnindexedWarning = _refreshUnindexedWarning;
})();
