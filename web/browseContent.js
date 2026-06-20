/**
 * web/browseContent.js — Browse-tab content: video grid loader, search/graph renderers, Whisper model picker, transcribe-channel flow, Index tab table
 */
(function () {
  "use strict";

  const _browseState = window._browseState || {};
  const showContextMenu = window.showContextMenu || (() => {});
  const askConfirm = window.askConfirm;
  const askDanger = window.askDanger;
  const askQuestion = window.askQuestion;
  const askChoice = window.askChoice;
  const askTextInput = window.askTextInput;
  const escapeHtml = window.YT?.util?.escapeHtml || ((s) => String(s ?? "")
    .replace(/[&<>"']/g, (ch) => ({
      "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;", "'": "&#39;",
    }[ch])));
  function bridgeCall(method, ...args) {
    const fn = window.YT?.bridge?.bridgeCall;
    if (fn) return fn(method, ...args);
    return undefined;
  }

  function nativeBridgeUp() {
    return !!window.YT?.bridge?.isUp?.();
  }

  // ─── Manual-queue Whisper model picker ───────────────────────
  // Mirrors YTArchiver.py:22030 `_ask_whisper_model_dialog`. Shows a 4-option
  // modal (tiny/small/medium/large-v3) with a 60-second countdown that
  // auto-picks the Settings-stored default. Used ONLY when the user
  // manually queues a video/channel/folder — sync-triggered auto-transcribes
  // use the Settings default silently. Returns the chosen model name, or
  // null on cancel. Swaps the running whisper model via the backend so the
  // next job uses it.
  async function _askWhisperModel(contextLabel = "") {
    let currentDefault = "small";
    try {
      const s = nativeBridgeUp() ? await bridgeCall("settings_load") : null;
      if (s?.whisper_model) currentDefault = String(s.whisper_model);
    } catch (_e) {}
    // Issue #150 \u2014 just the model names, no verbose blurbs.
    const models = ["tiny", "small", "medium", "large-v3"];
    const choices = models.map((m) => ({
      label: m,
      value: m,
      primary: m === currentDefault,
    }));
    const msg = contextLabel
      ? `YouTube auto-captions are used first when available. Pick the Whisper model to fall back to for ${contextLabel}.`
      : "YouTube auto-captions are used first when available. Pick the Whisper model to fall back to.";
    const pick = await askChoice({
      title: "Transcribe \u2014 Whisper fallback model",
      message: msg,
      choices,
      countdownSecs: 60,
      countdownLabel: `Defaulting to ${currentDefault} in`,
    });
    if (pick === null) return null;
    // Swap the running whisper process for the next job ONLY — do NOT
    // persist to config. The Settings > Whisper model dropdown is the
    // authoritative place for the default; a one-off manual pick for a
    // single retranscribe shouldn't mutate it. Second arg `false` =
    // don't persist. "manual retranscriptions have nothing to
    // do with that [settings default]".
    // Surface swap failure: the old code swallowed every error here, so
    // a backend swap_model failure left the worker on its previous
    // model while the user thought their pick had taken effect (audit:
    // watchActions.js:386-422). Now we toast + return null on failure
    // so the caller (retranscribe button) aborts instead of queuing
    // against the wrong model.
    try {
      const _swap = nativeBridgeUp() ? await bridgeCall("transcribe_swap_model", pick, false) : null;
      if (_swap && _swap.ok === false) {
        window._showToast?.(
          `Couldn't switch to ${pick}: ${_swap.error || "unknown error"}.`,
          "error");
        return null;
      }
    } catch (_e) {
      window._showToast?.(
        `Couldn't switch to ${pick}: ${_e?.message || _e || "bridge error"}.`,
        "error");
      return null;
    }
    return pick;
  }
  window._askWhisperModel = _askWhisperModel;

  async function _askTranscribeChannel(channelName, combined) {
    if (!nativeBridgeUp()) {
      window._showToast?.("Native mode required.", "warn");
      return;
    }
    // Manual channel transcribe → ask which whisper model (60s countdown
    // auto-picks Settings default). Skip on the recursive call after the
    // Follow-org/Combined dialog resolves.
    if (combined === undefined) {
      const model = await _askWhisperModel(`"${channelName}"`);
      if (model === null) return; // user cancelled
    }
    const res = await bridgeCall("chan_transcribe_all", channelName, combined);
    if (res?.ok === false) {
      window._showToast?.(res.error || "Transcribe failed to start.", "error");
      return;
    }
    if (res?.needs_choice) {
      // First-time transcribe on an organized channel — ask the user.
      // 60-second countdown auto-selects Follow-organization (matches OLD's
      // `_ask_whisper_model_dialog` pattern, YTArchiver.py:22030, and is the
      // safe default since it mirrors the channel's folder layout).
      const pick = await askChoice({
        title: "Transcribe \u2014 " + channelName,
        message: "Where should transcript files be placed?",
        choices: [
          { label: `Follow organization (${res.org_label} folders)`,
            value: "follow", primary: true },
          { label: "Combined (one file for entire channel)",
            value: "combined" },
        ],
        countdownSecs: 60,
        countdownLabel: "Auto-selecting Follow organization in",
      });
      if (pick === null) return; // user cancelled
      // Recurse with the resolved choice.
      return _askTranscribeChannel(channelName, pick === "combined");
    }
    if (res?.ok && res.queued != null) {
      window._showToast?.(
        `Queued ${res.queued} video(s) for transcription.`, "ok");
    }
  }
  window._askTranscribeChannel = _askTranscribeChannel;

  // Exposed so other modules (e.g. Recent table dblclick) can pop a video
  // into the Watch view with a proper transcript + karaoke bind.
  // a slow browse_get_transcript (DB lock contention with a
  // long indexing sweep) used to leave the player blank, and the
  // eventual response would render in the background — even if the
  // user had navigated away and the next video had been picked. Track
  // a monotonic token per open call and bail when it changes.
  let _watchOpenToken = 0;
  // _browseState publication moved to web/browseState.js — that module
  // loads early enough to be the canonical owner. We just expose the
  // _watchOpenToken getter for logs.js _loadVideoSource (which checks
  // it after its own awaits and bails if the user navigated away).
  Object.defineProperty(window, "_watchOpenToken", {
    get() { return _watchOpenToken; },
    configurable: true,
  });
  window._openVideoInWatch = async function (video) {
    if (!video) return;
    const myToken = ++_watchOpenToken;
    // Ensure we're on the Browse tab and in Watch view.
    document.querySelector('.tab[data-tab="browse"]')?.click();
    // Record where Watch was entered FROM so Back returns there. Prefer
    // the SUBMODE (recent / search / bookmarks / graph) — `view` only
    // tracks the within-Channels view (channels|videos|watch), so for a
    // search/bookmark/recent open it was "channels"/"videos" and Back
    // fell through to the channel grid (the search-result Back bug). In
    // the Channels submode the view ("videos" during a channel
    // drilldown) is still the correct return target.
    {
      const _sm = _browseState.submode;
      _browseState.watchReturnTo =
        (_sm && _sm !== "channels") ? _sm : (_browseState.view || null);
    }
    _browseState.currentVideo = video;
    showView("watch");

    // Loading-state paint now happens inside showView("watch") above,
    // so every entry path (video-grid click, search-result click,
    // Forward gesture, this helper) gets identical treatment without
    // each handler having to remember to call it.

    let transcript = null;
    let sourceInfo = null;
    if (nativeBridgeUp()) {
      try {
        const res = await bridgeCall("browse_get_transcript", {
          video_id: video.video_id || undefined,
          title: video.title || "",
        });
        // If the user navigated away (different video, different tab)
        // while we were waiting, drop the result on the floor so the
        // late response doesn't start playing the wrong video.
        if (myToken !== _watchOpenToken) return;
        if (Array.isArray(res)) {
          transcript = res;
        } else if (res && res.segments) {
          transcript = res.segments;
          sourceInfo = res.source || null;
        }
        // Carry tx_status (backend sets it when there are no segments) so the
        // empty-transcript branch can show "No speech detected" for a
        // genuinely-silent video instead of the generic message.
        if (res && !Array.isArray(res) && res.tx_status) {
          video.tx_status = res.tx_status;
        }
      } catch (e) {
        // Surface bridge errors so the user knows the transcript
        // couldn't load (was: silent swallow → empty "No transcript
        // available" with no clue why; audit: browseContent H149).
        console.warn("browse_get_transcript failed:", e);
        try {
          window._showToast?.(
            `Couldn't load transcript: ${e?.message || e}`, "warn");
        } catch {}
      }
    }
    if (myToken !== _watchOpenToken) return;
    // Bug fix: if user navigated away from Watch view entirely (back
    // button, Browse sub-mode switch, etc.) abort rather than render
    // into a hidden Watch view and accidentally autoplay audio.
    if (_browseState.view !== "watch") return;
    if (!transcript || transcript.length === 0) {
      // pass empty array so the renderer's "No transcript
      // available." message shows. Previously synthesizeTranscript()
      // returned demo Lorem-ipsum-style text which is wrong for a
      // legitimately silent/music-only video.
      transcript = [];
    } else {
      transcript = transcript.map(seg => ({
        ts: _formatTs(seg.s), text: seg.t, words: seg.w, s: seg.s, e: seg.e,
      }));
    }
    if (myToken !== _watchOpenToken) return;
    if (_browseState.view !== "watch") return;
    window.renderWatchView(video, transcript, sourceInfo);
    // feature F5: if we arrived from a search result, pre-fill the
    // transcript Find box with the query so Enter/Shift+Enter cycle
    // between hits in this specific video. Saves the round-trip of
    // Back → click next result → Watch.
    const _q = (video._search_query || "").trim();
    if (_q) {
      const _watchFind = document.getElementById("watch-find");
      if (_watchFind) {
        _watchFind.value = _q;
        // Trigger the existing input listener to build matches + jump
        // to the first hit. The existing _rebuildFindMatches is scoped
        // inside initWatchActions and not directly callable, but
        // dispatching `input` exercises it.
        try {
          _watchFind.dispatchEvent(new Event("input", { bubbles: true }));
        } catch {}
      }
    }
    // If the caller passed a seek target (bookmark jump, search-result jump,
    // transcript-segment click from elsewhere), seek the <video> element
    // once it's ready. Wait for `loadedmetadata` so duration is known.
    // Treat 0 / unset _seek_to as "no seek requested" — callers who
    // want to land on Watch view paused at the natural start used to
    // accidentally trigger an auto-seek-and-play because `>= 0`
    // accepted 0 as a real value (audit: browseContent.js:204).
    // Matches the pattern at line ~598 elsewhere in this file.
    const seekTo = Number(video._seek_to);
    if (Number.isFinite(seekTo) && seekTo > 0) {
      const vEl = document.querySelector("#watch-video video") ||
                  document.getElementById("watch-video");
      if (vEl) {
        const doSeek = () => {
          try {
            vEl.currentTime = seekTo;
            vEl.play().catch(() => {});
          } catch { /* noop */ }
        };
        if (vEl.readyState >= 1) doSeek();
        else vEl.addEventListener("loadedmetadata", doSeek, { once: true });
      }
    }
  };

  // Expose a "reload the currently-viewed channel's grid" handle
  // so push events (archive_rescan complete, etc.) can force the
  // grid to re-query after the DB changes under it.
  window._reloadCurrentChannelVideos = () => {
    const ch = (typeof _browseState !== "undefined")
      ? _browseState.currentChannel : null;
    if (ch) loadVideosFor(ch);
  };
  window._reloadChannelsGrid = () => {
    // Re-fetch the channel list (Subs table + per-channel counts).
    if (!nativeBridgeUp()) return;
    bridgeCall("get_index_summary").then((idx) => {
      if (typeof window._applyIndexSummary === "function") {
        window._applyIndexSummary(idx);
      }
    }).catch(() => {});
  };

  async function loadVideosFor(channel) {
    const name = channel.folder || channel.name || "";
    const sort = document.getElementById("browse-sort")?.value || "newest";

    // Show/hide the "Group by month" checkbox based on this channel's
    // folder layout. Only makes sense when the channel is organized
    // yyyy/mm on disk — otherwise there's nothing to group by.
    const monthWrap = document.getElementById("browse-group-month-wrap");
    if (monthWrap) {
      monthWrap.hidden = !channel.split_months;
    }
    // Uncheck month-grouping when switching to a channel that doesn't
    // support it, to avoid a stale-state re-render.
    if (!channel.split_months) {
      const mcb = document.getElementById("browse-group-month");
      if (mcb) mcb.checked = false;
    }

    // Clear the previous channel's grid + update the breadcrumb title
    // IMMEDIATELY so switching channels never shows stale content.
    _browseState.videos = [];
    const grid = document.getElementById("video-grid");
    if (grid) {
      grid.classList.remove("video-grid-grouped");
      grid.innerHTML = '<div class="browse-loading">Loading\u2026</div>';
    }
    const titleEl = document.getElementById("browse-main-title");
    if (titleEl) titleEl.textContent = name;

    // Track the in-flight channel name so if another channel is clicked
    // before this one's fetch returns, we discard the stale result.
    const myLoadSeq = (loadVideosFor._seq = (loadVideosFor._seq || 0) + 1);

    // Native mode → real DB
    if (nativeBridgeUp()) {
      try {
        const rows = await bridgeCall("browse_list_videos", name, sort, 50000);
        if (myLoadSeq !== loadVideosFor._seq) return; // stale, user clicked another channel
        if (Array.isArray(rows) && rows.length > 0) {
          _browseState.videos = rows.map(r => {
            // Prefer the YouTube upload time (file mtime — yt-dlp --mtime)
            // over the DB-insertion time. Falls back to added_ts when the
            // file is missing (e.g. moved offline).
            const epoch = r.upload_ts || r.added_ts || 0;
            return {
              title: r.title || "",
              channel: r.channel || name,
              filepath: r.filepath || "",
              video_id: r.video_id || "",
              uploaded: _formatAddedTs(epoch),
              duration: "",
              // Preserve the backend's view_count + formatted `views`
              // display string. Pre-fix these were hardcoded to 0 / ""
              // which silently broke the "Most Viewed" sort: the client
              // `sortCurrentVideos` uses `view_count - view_count = 0`
              // as the comparator → stable sort → no reorder → the grid
              // stayed in whatever order the backend returned. Backend
              // DOES sort by view_count when sort === "most_viewed", but
              // the client-side re-sort on dropdown change clobbered it
              // back to whatever was there first (usually newest).
              views: r.views || "",
              upload_ts: epoch * 1000,
              view_count: r.view_count || 0,
              like_count: r.like_count || 0,
              size_bytes: r.size_bytes || 0,
              tx_status: r.tx_status || "pending",
              year: r.year, month: r.month,
              // Thumbnail sidecar (file:// URL from .Thumbnails/ or next-to-video)
              thumbnail: r.thumbnail || "",
              thumbnail_url: r.thumbnail_url || "",
            };
          });
          sortCurrentVideos(sort);
          return;
        }
      } catch (e) { console.warn("browse_list_videos failed:", e); }
    }

    // Fallback for preview mode — synthesize placeholder videos
    _browseState.videos = [];
    window._showToast?.("Archive bridge unavailable. Videos will load once the app is ready.", "warn");
    sortCurrentVideos(sort);
  }

  function _formatAddedTs(ts) {
    if (!ts) return "";
    const now = Date.now() / 1000;
    const age = now - ts;
    if (age < 60) return "just now";
    if (age < 3600) return Math.floor(age / 60) + "m ago";
    if (age < 86400) return Math.floor(age / 3600) + "h ago";
    if (age < 86400*30) return Math.floor(age / 86400) + "d ago";
    if (age < 86400*365) return Math.floor(age / (86400*30)) + "mo ago";
    const years = Math.floor(age / (86400*365));
    // Abbreviation form is intentionally identical across counts
    // (matches "Nm/Nh/Nd/Nmo" style above). Was a dead ternary.
    return years + "y ago";
  }

  function sortCurrentVideos(sortBy) {
    const vids = _browseState.videos.slice();
    if (sortBy === "newest") vids.sort((a, b) => b.upload_ts - a.upload_ts);
    else if (sortBy === "oldest") vids.sort((a, b) => a.upload_ts - b.upload_ts);
    else if (sortBy === "most_viewed") vids.sort((a, b) => b.view_count - a.view_count);
    const groupByYear = !!document.getElementById("browse-group-year")?.checked;
    const groupByMonth = !!document.getElementById("browse-group-month")?.checked;
    // Contextual nudge when no metadata on this channel yet — matches
    // YTArchiver.py:25091 _grid_meta_banner_lbl. Banner appears above the
    // grid, clicking it queues metadata for the channel.
    _refreshVideoGridMetaBanner(vids);
    // Route through _openVideoInWatch so the canonical race-token
    // guard (myToken !== _watchOpenToken) protects against rapid
    // A-then-B clicks landing B's video with A's transcript (audit:
    // browseContent.js C26). The inline duplicate of the load logic
    // had no token check, so two awaits could resolve out of order.
    window.renderVideoGrid(vids, async (v) => {
      if (typeof window._openVideoInWatch === "function") {
        window._openVideoInWatch(v);
      }
    }, { groupByYear, groupByMonth });
  }

  function _refreshVideoGridMetaBanner(vids) {
    const grid = document.getElementById("video-grid");
    if (!grid) return;
    // Remove any prior banner
    grid.parentElement?.querySelector(".meta-nudge-banner")?.remove();
    if (!vids || !vids.length) return;
    // Detect: no video has a view_count OR an uploaded string — classic
    // "this channel hasn't had a metadata pass yet".
    const anyMeta = vids.some(v => (v.view_count && v.view_count > 0) ||
                                    (v.views && String(v.views).trim()) ||
                                    (v.uploaded && String(v.uploaded).trim()));
    if (anyMeta) return;
    const ch = _browseState.currentChannel;
    if (!ch) return;
    const banner = document.createElement("div");
    banner.className = "meta-nudge-banner";
    banner.innerHTML = `
      <span class="meta-nudge-icon">&#x1F4E5;</span>
      <span class="meta-nudge-text">
        No metadata yet for <b></b>. Click to queue a fetch (views, likes,
        descriptions, thumbnails, and top 50 comments per video).
      </span>
      <button class="btn btn-primary btn-thin">Download metadata</button>
    `;
    banner.querySelector("b").textContent = ch.folder || ch.name || "this channel";
    banner.querySelector("button").addEventListener("click", async (e) => {
      e.stopPropagation();
      if (!nativeBridgeUp()) {
        window._showToast?.("Native mode required.", "warn");
        return;
      }
      const name = ch.folder || ch.name || "";
      const res = await bridgeCall("metadata_recheck_channel", { name });
      if (res?.ok) {
        window._showToast?.(`Metadata fetch started for ${name}.`, "ok");
        banner.remove();
      } else {
        window._showToast?.(res?.error || "Start failed.", "error");
      }
    });
    grid.parentElement?.insertBefore(banner, grid);
  }

  function _formatTs(sec) {
    if (sec == null) return "0:00";
    const m = Math.floor(sec / 60);
    const s = Math.floor(sec % 60);
    return `${m}:${String(s).padStart(2, "0")}`;
  }
  // Expose for logs.js — the retranscribe-complete handler lives there
  // and needs to reformat timestamps when it re-renders the transcript.
  window._formatTs = _formatTs;

  // synthesizeTranscript() removed. It was demo/placeholder
  // data from the early UI prototype phase that incorrectly showed
  // fake "Welcome back to the channel everybody..." text whenever a
  // video legitimately had no transcript (music videos, silent clips).
  // All 4 callers now pass [] directly so the renderer's existing
  // "No transcript available." message displays.

  function initBrowseSubmodeContent() {
    // Search is fully wired by initSearchSubmode() (native + FTS) above.
    // Graph is wired by initGraphView() (Chart.js, native data).
    // No synthesized fallback needed once running in pywebview.
    //
    // Enter→Search wiring lives in browseSearch.js initSearchView()
    // — see web/browseSearch.js:543. Adding another handler here used
    // to fire doSearch TWICE on every Enter press (once via simulated
    // btn click, once via the direct keydown handler in browseSearch).
    // The browseSearch handler is the authoritative one because it
    // also short-circuits the e.repeat autorepeat stream (audit:
    // browseContent.js:472).
  }

  function renderSearchResults(container, hits, q) {
    container.innerHTML = "";
    if (hits.length === 0) {
      container.innerHTML = '<div class="browse-empty">No hits.</div>';
      return;
    }
    // Multi-word queries previously matched as a single contiguous
    // substring — "open source" would only highlight the exact pair,
    // never either word alone (audit: browseContent.js:514). Tokenize
    // on whitespace and join into an alternation regex so each word
    // highlights individually. Filter FTS5 operator tokens out.
    const _FTS_OPS = new Set(["and", "or", "not", "near"]);
    const _qParts = String(q || "")
      .toLowerCase()
      .replace(/["*]/g, "")
      .replace(/\bnear\s*\([^)]*\)/g, " ")
      .replace(/[\^()]/g, " ")
      .split(/\s+/)
      .filter(w => w && !_FTS_OPS.has(w))
      .map(escapeForRegex);
    const rx = _qParts.length
      ? new RegExp("(" + _qParts.join("|") + ")", "gi")
      : null;
    const frag = document.createDocumentFragment();
    for (const h of hits) {
      const row = document.createElement("div");
      row.className = "search-result";
      row.title = "Double-click to open in Watch view at this timestamp";
      row.innerHTML = `
        <span class="ts">[${h.timestamp}]</span>
        <span class="snippet"></span>
        <span class="meta"></span>
      `;
      // Patch B (XSS hardening): rebuild snippet via DOM nodes instead
      // of regex-then-innerHTML. Splitting on `rx` (which has a single
      // capture group for the matched term) puts plain text at even
      // indices and matched terms at odd indices.
      const _bmSnip = row.querySelector(".snippet");
      const _bmRaw = h.snippet || "";
      if (rx) {
        const _bmParts = _bmRaw.split(rx);
        for (let _bi = 0; _bi < _bmParts.length; _bi++) {
          if (_bi % 2 === 0) {
            _bmSnip.appendChild(document.createTextNode(_bmParts[_bi]));
          } else {
            const _mk = document.createElement("mark");
            _mk.textContent = _bmParts[_bi];
            _bmSnip.appendChild(_mk);
          }
        }
      } else {
        _bmSnip.textContent = _bmRaw;
      }
      row.querySelector(".meta").textContent = `${h.channel || ""} \u00b7 ${h.title || ""}`;
      row.addEventListener("dblclick", () => _openSearchHitInWatch(h));
      frag.appendChild(row);
    }
    container.appendChild(frag);
  }

  async function _openSearchHitInWatch(hit) {
    if (!nativeBridgeUp()) {
      window._showToast?.("Native mode required for playback.", "warn");
      return;
    }
    try {
      const res = await bridgeCall("browse_resolve_segment",
        hit.jsonl_path || "", hit.video_id || "", hit.title || "");
      if (!res?.ok) {
        window._showToast?.(res?.error || "Video file not found.", "error");
        return;
      }
      const video = {
        title: res.title || hit.title || "",
        channel: res.channel || hit.channel || "",
        filepath: res.filepath,
        video_id: res.video_id || hit.video_id || "",
        start_at: Number(hit.start_time) || 0,
      };
      _browseState.currentVideo = video;
      showView("watch");
      // Load real transcript, fall back to synthesized
      let transcript = null;
      let sourceInfo = null;
      try {
        // Pass video_id + title ONLY (no jsonl_path) — see browseSearch.js:
        // forwarding the hit's jsonl_path made the backend re-normpath a
        // slash-mixed stored path and the jsonl_path=? match missed, blanking
        // the Watch transcript. Let the backend resolve its canonical path.
        const res = await bridgeCall("browse_get_transcript", {
          video_id: video.video_id || undefined,
          title: video.title,
        });
        if (Array.isArray(res)) transcript = res;
        else if (res && res.segments) {
          transcript = res.segments;
          sourceInfo = res.source || null;
        }
        if (res && !Array.isArray(res) && res.tx_status) {
          video.tx_status = res.tx_status;
        }
      } catch { /* ignore */ }
      if (!transcript || transcript.length === 0) {
        // empty array → renderer shows the no-speech / no-transcript message
        transcript = [];
      } else {
        transcript = transcript.map(seg => ({
          ts: _formatTs(seg.s), text: seg.t, words: seg.w, s: seg.s, e: seg.e,
        }));
      }
      window.renderWatchView(video, transcript, sourceInfo);
      // Seek + flash-highlight the segment once the <video> element is ready.
      const vEl = document.getElementById("watch-video");
      if (vEl && video.start_at > 0) {
        const seek = () => {
          try { vEl.currentTime = video.start_at; vEl.play?.().catch(() => {}); }
          catch { /* ignore */ }
        };
        if (vEl.readyState >= 1) seek();
        else vEl.addEventListener("loadedmetadata", seek, { once: true });
      }
    } catch (e) {
      console.warn("open search hit failed:", e);
      window._showToast?.("Could not open video.", "error");
    }
  }

  function escapeForRegex(s) {
    return String(s).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  }

  function populateIndexTable(channels) {
    const tbody = document.getElementById("index-table-body");
    if (!tbody) return;
    tbody.innerHTML = "";
    const frag = document.createDocumentFragment();
    for (const c of channels.slice(0, 100)) {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td></td>
        <td class="right">${c.n_vids ? c.n_vids.toLocaleString() : "\u2014"}</td>
        <td class="right">${c.size || "\u2014"}</td>
        <td class="right">\u2014</td>
        <td class="right">${c.auto_transcribe ? "on" : "\u2014"}</td>
      `;
      tr.cells[0].textContent = c.folder;
      frag.appendChild(tr);
    }
    tbody.appendChild(frag);
  }
  window._populateIndexTable = populateIndexTable;

  /** Apply real Index-tab summary (from backend) — overrides static placeholders. */
  window._applyIndexSummary = function (idx) {
    if (!idx) return;
    const c = idx.cards || {};
    const setText = (id, txt) => { const el = document.getElementById(id); if (el) el.textContent = txt; };
    setText("idx-channels", (c.channels ?? "\u2014").toLocaleString?.() ?? c.channels);
    setText("idx-videos", (c.videos ?? "\u2014").toLocaleString?.() ?? c.videos);
    setText("idx-size", c.size_label || "\u2014");
    setText("idx-transcribed", c.transcribed_pct_channels != null
                               ? c.transcribed_pct_channels.toFixed(1) + "%"
                               : "\u2014");
    // Sidebar stats as well
    setText("stat-channels", (c.channels ?? "").toLocaleString?.() ?? "");
    setText("stat-videos", (c.videos ?? "").toLocaleString?.() ?? "");
    // Segment count isn't in the summary cards (it's an expensive full-DB
    // aggregate), so fetch it once via the same call Settings → Index uses
    // and fill the sidebar "segments" badge when it resolves — otherwise it
    // sits at "—" the whole session. (This lives here, not in the
    // indexControls compat shim, because browseContent.js loads last and
    // overwrites window._applyIndexSummary.)
    if (!window.__segStatFetched) {
      window.__segStatFetched = true;
      if (nativeBridgeUp()) {
        bridgeCall("get_index_db_stats").then((db) => {
          if (db && db.segments != null) {
            setText("stat-segments", Number(db.segments).toLocaleString());
          } else {
            window.__segStatFetched = false;
          }
        }).catch(() => { window.__segStatFetched = false; });
      } else {
        window.__segStatFetched = false;
      }
    }

    // Per-channel table
    if (Array.isArray(idx.per_channel)) {
      populateIndexTable(idx.per_channel);
    }
    // "Last built" status line under the control row. Prefer the
    // backend-supplied built_ts (when present) over `new Date()` so
    // the label reflects when the index was ACTUALLY rebuilt, not
    // when the UI happened to re-fetch the summary (audit:
    // browseContent.js:716). Falls back to now-time for backends
    // that don't supply the timestamp yet.
    const last = document.getElementById("idx-last-built");
    if (last) {
      let t;
      const _bt = Number(idx?.built_ts || c?.built_ts);
      if (Number.isFinite(_bt) && _bt > 0) {
        // built_ts is a unix-epoch number (seconds or ms \u2014 try both).
        t = new Date(_bt > 1e12 ? _bt : _bt * 1000);
      } else {
        t = new Date();
      }
      const hh = t.getHours();
      const mm = String(t.getMinutes()).padStart(2, "0");
      const ampm = hh >= 12 ? "pm" : "am";
      const h12 = ((hh + 11) % 12) + 1;
      last.textContent = `Last refresh \u00b7 ${h12}:${mm}${ampm}`;
    }
  };

  // Sync + GPU queue "Auto" checkboxes — when on, adding an item to an empty
  // queue auto-starts the queue. Mirrors YTArchiver.py autorun_sync +
  // autorun_gpu config keys. State is persisted to config via the backend.
  function initQueueAutoCheckboxes() {
    const syncCB = document.getElementById("sync-auto-checkbox");
    const gpuCB = document.getElementById("gpu-auto-checkbox");

    // Restore saved state on load. `window.pywebview.api` isn't injected
    // until AFTER DOMContentLoaded (pywebview fires a `pywebviewready`
    // event when it's ready, but boot() runs on DOMContentLoaded), so
    // the first api lookup usually returns undefined and the original
    // restore silently no-op'd — reported: toggle Auto, restart,
    // Auto is back to default. Fix: re-resolve the api when pywebview
    // signals ready, with a 600ms fallback poll in case the event was
    // missed or we're racing boot.
    // Once the FIRST queue_auto_get resolves we stop polling — old
    // code kept polling for 3s even after a successful restore, and a
    // late response could clobber a user toggle that happened in the
    // 600ms boot window (audit: browseContent.js:743).
    let _restored = false;
    const restore = () => {
      if (_restored) return true;
      if (!nativeBridgeUp()) return false;
      bridgeCall("queue_auto_get").then((st) => {
        if (_restored) return;            // late response — user already saw a result
        if (!st) return;
        _restored = true;
        if (syncCB) syncCB.checked = !!st.sync;
        if (gpuCB) gpuCB.checked = !!st.gpu;
      }).catch(() => {});
      return true;
    };
    if (!restore()) {
      window.addEventListener("pywebviewready", () => { restore(); },
                              { once: true });
      // Belt-and-suspenders: poll briefly in case `pywebviewready` was
      // already dispatched before we registered the listener. _restored
      // gates further polls so a late response can't trample a user
      // change that happened mid-boot.
      let tries = 0;
      const poll = () => {
        if (_restored) return;
        if (restore() && _restored) return;
        if (++tries < 20) setTimeout(poll, 150);
      };
      setTimeout(poll, 150);
    }

    // Change handler: bridgeCall re-resolves the api each call (handles
    // the same api-timing gotcha noted above). Gated on nativeBridgeUp so
    // a toggle in browser-preview mode stays a silent no-op.
    syncCB?.addEventListener("change", () => {
      if (nativeBridgeUp()) bridgeCall("queue_auto_set", "sync", syncCB.checked);
    });
    gpuCB?.addEventListener("change", () => {
      if (nativeBridgeUp()) bridgeCall("queue_auto_set", "gpu", gpuCB.checked);
    });
  }

  window.initBrowseSubmodeContent = initBrowseSubmodeContent;
  window.initQueueAutoCheckboxes = initQueueAutoCheckboxes;
  window._askWhisperModel = _askWhisperModel;
  window._askTranscribeChannel = _askTranscribeChannel;
  window.loadVideosFor = loadVideosFor;
  window.sortCurrentVideos = sortCurrentVideos;
  window._formatTs = _formatTs;
  window.renderSearchResults = renderSearchResults;
  window.populateIndexTable = populateIndexTable;
})();
