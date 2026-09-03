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
  const displayText = window.YT?.util?.displayText || ((s) => String(s ?? ""));
  function bridgeCall(method, ...args) {
    const fn = window.YT?.bridge?.bridgeCall;
    if (fn) return fn(method, ...args);
    return undefined;
  }

  function nativeBridgeUp() {
    return !!window.YT?.bridge?.isUp?.();
  }

  const CHANNEL_VIDEO_PAGE_SIZE = 120;
  const _channelPage = {
    active: false,
    channel: "",
    sort: "newest",
    query: "",
    offset: 0,
    hasMore: false,
    loading: false,
  };

  function _channelGroupingEnabled() {
    return !!document.getElementById("browse-group-year")?.checked ||
           !!document.getElementById("browse-group-month")?.checked;
  }

  function _currentVideoFilter() {
    if (_browseState.view !== "videos") return "";
    return (document.getElementById("browse-filter")?.value || "").trim();
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
        `Couldn't switch to ${pick}: ${_e?.message || _e || "app connection error"}.`,
        "error");
      return null;
    }
    return pick;
  }
  window._askWhisperModel = _askWhisperModel;

  async function _askTranscribeChannel(channelIdentity, combined) {
    if (!nativeBridgeUp()) {
      window._showToast?.("YTArchiver isn't ready yet. Try again in a moment.", "warn");
      return;
    }
    const channelName = typeof channelIdentity === "object" && channelIdentity
      ? String(channelIdentity.name || channelIdentity.folder || "").trim()
      : String(channelIdentity || "").trim();
    const channelArg = typeof channelIdentity === "object" && channelIdentity
      ? {
          name: channelIdentity.name || "",
          folder: channelIdentity.folder || "",
          url: channelIdentity.url || "",
        }
      : channelName;
    if (!channelName) {
      window._showToast?.("Could not identify the channel.", "error");
      return;
    }
    // Manual channel transcribe → ask which whisper model (60s countdown
    // auto-picks Settings default). Skip on the recursive call after the
    // Follow-org/Combined dialog resolves.
    if (combined === undefined) {
      const model = await _askWhisperModel(`"${channelName}"`);
      if (model === null) return; // user cancelled
    }
    const res = await bridgeCall("chan_transcribe_all", channelArg, combined);
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
      return _askTranscribeChannel(channelArg, pick === "combined");
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
  let _watchOpenIntentToken = 0;
  // Some entry paths must resolve a media file before they can invoke the
  // canonical opener. Reserving an intent immediately makes "last click
  // wins" apply across that pre-open await as well as transcript loading.
  window._reserveWatchOpenIntent = () => ++_watchOpenIntentToken;
  window._isWatchOpenIntentCurrent = (token) =>
    token === _watchOpenIntentToken;
  // _browseState publication moved to web/browseState.js — that module
  // loads early enough to be the canonical owner. We just expose the
  // _watchOpenToken getter for logs.js _loadVideoSource (which checks
  // it after its own awaits and bails if the user navigated away).
  Object.defineProperty(window, "_watchOpenToken", {
    get() { return _watchOpenToken; },
    configurable: true,
  });
  window._openVideoInWatch = async function (video, options = {}) {
    if (!video) return;
    const reservedIntent = Number(options.intentToken);
    if (Number.isFinite(reservedIntent)) {
      if (reservedIntent !== _watchOpenIntentToken) return;
    } else {
      ++_watchOpenIntentToken;
    }
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

    // Find belongs to one rendered transcript, not to the Watch pane as a
    // whole. Clear the previous video's query/count as soon as a new open
    // starts and dispatch input so watchActions also resets its private match
    // list. A deliberate Search-result query is applied again after the new
    // transcript renders below.
    const watchFind = document.getElementById("watch-find");
    const watchFindCount = document.getElementById("watch-find-count");
    if (watchFind) {
      watchFind.value = "";
      try {
        watchFind.dispatchEvent(new Event("input", { bubbles: true }));
      } catch {}
    }
    if (watchFindCount) watchFindCount.textContent = "";

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
          channel: video.channel || "",
          filepath: video.filepath || "",
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

  function _paintChannelVideoCatalogStatus(status) {
    if (status.phase === "done") return;
    const grid = document.getElementById("video-grid");
    const target = grid?.querySelector(
      ".browse-loading, #channel-video-page-sentinel");
    if (target) target.textContent = status.text;
  }

  function _paintChannelVideoError(message) {
    const grid = document.getElementById("video-grid");
    if (!grid) return;
    grid.innerHTML = "";
    const error = document.createElement("div");
    error.className = "browse-empty";
    error.textContent = message || "Couldn’t load this channel’s videos.";
    grid.appendChild(error);
  }

  // ── Channel action header (Batch 8) ────────────────────────────────
  // The channel currently shown in #view-videos, remembered so the header
  // buttons (Sync now / Settings / Open folder) know which channel to act on.
  let _cphChannel = null;
  let _cphSyncState = "";
  function _updateChannelHeader(channel) {
    _cphChannel = channel || null;
    const header = document.getElementById("channel-page-header");
    const info = document.getElementById("cph-info");
    if (!header) return;
    if (!channel) {
      _cphSyncState = "";
      header.hidden = true;
      return;
    }
    const name = channel.folder || channel.name || "";
    const parts = [];
    const vids = channel.n_vids ?? channel.video_count;
    if (vids !== undefined && vids !== null && Number.isFinite(Number(vids))) {
      parts.push(`${Number(vids).toLocaleString()} videos`);
    }
    if (channel.size) parts.push(String(channel.size));
    // Live sync state for this channel.
    let state = "";
    try { state = window._queueHasSyncForChannel?.(name) || ""; } catch (e) { /* ignore */ }
    _cphSyncState = state;
    let badge = "";
    if (state === "running") badge = '<span class="cph-badge cph-badge-run">Syncing now</span>';
    else if (state === "queued") badge = '<span class="cph-badge">Queued to sync</span>';
    if (info) {
      const esc = window._escapeHtml || ((s) => String(s ?? ""));
      info.innerHTML =
        parts.map((p) => `<span>${esc(p)}</span>`).join('<span class="cph-dot">·</span>')
        + badge;
    }
    // Disable Sync-now while this channel is already running/queued.
    const syncBtn = document.getElementById("cph-sync-now");
    if (syncBtn) syncBtn.disabled = !!state;
    header.hidden = false;
  }

  function _channelNameKey(channel) {
    return String(channel?.folder || channel?.name || "")
      .trim().toLowerCase();
  }

  function _reconcileCompleteChannelCount(channel) {
    // A filtered page is only the number of matches. A page with hasMore is
    // only the first slice of a large channel. Neither may replace the true
    // archive count. Once an unfiltered page is complete, however, the card
    // list itself is authoritative and can safely repair an old cache value.
    if (!_channelPage.active || _channelPage.query || _channelPage.hasMore) {
      return false;
    }
    const name = channel?.folder || channel?.name || "";
    const key = _channelNameKey(channel);
    if (!key) return false;
    const count = Array.isArray(_browseState.videos)
      ? _browseState.videos.length : 0;
    const changes = { n_vids: count, video_count: count };
    Object.assign(channel, changes);
    if (_cphChannel && _channelNameKey(_cphChannel) === key) {
      Object.assign(_cphChannel, changes);
    }
    for (const row of (Array.isArray(_browseState.channels)
      ? _browseState.channels : [])) {
      if (_channelNameKey(row) === key) Object.assign(row, changes);
    }
    window._refreshChannelCardSummary?.(name, changes);
    if (_browseState.view === "videos"
        && _channelNameKey(_browseState.currentChannel) === key) {
      _updateChannelHeader(_browseState.currentChannel || channel);
    }
    return true;
  }

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
    _channelPage.active = false;
    _browseState.videos = [];
    const grid = document.getElementById("video-grid");
    if (grid) {
      grid.classList.remove("video-grid-grouped");
      grid.innerHTML = '<div class="browse-loading">Loading\u2026</div>';
    }
    const titleEl = document.getElementById("browse-main-title");
    if (titleEl) titleEl.textContent = displayText(name);

    // Populate + show the channel action header (Batch 8 — channel page can
    // now sync / configure / open-folder, not just watch).
    _updateChannelHeader(channel);

    // Track the in-flight channel name so if another channel is clicked
    // before this one's fetch returns, we discard the stale result.
    const myLoadSeq = (loadVideosFor._seq = (loadVideosFor._seq || 0) + 1);

    // Native mode → real DB
    if (nativeBridgeUp()) {
      if (!_channelGroupingEnabled()) {
        const ok = await _loadChannelPage(channel, true, myLoadSeq);
        if (ok) return;
      }
      _channelPage.active = false;
      try {
        const outcome = await window.YT.bridge.catalogRead(
          "channel-videos",
          () => bridgeCall("browse_list_videos", name, sort, 50000),
          {
            label: "channel videos",
            onStatus: _paintChannelVideoCatalogStatus,
          });
        if (outcome.stale) return;
        const rows = outcome.value;
        if (myLoadSeq !== loadVideosFor._seq) return; // stale, user clicked another channel
        if (Array.isArray(rows)) {
          _browseState.videos = rows.map(r => _mapVideoRow(r, name));
          sortCurrentVideos(sort);
          return;
        }
        if (rows?.error) {
          _paintChannelVideoError(rows.error);
          window._showToast?.(rows.error, "error");
          return;
        }
      } catch (e) { console.warn("browse_list_videos failed:", e); }
    }

    // Fallback for preview mode — synthesize placeholder videos
    _browseState.videos = [];
    window._showToast?.(
      "YTArchiver is still starting. Videos will load when it is ready.", "warn");
    sortCurrentVideos(sort);
  }

  async function _loadChannelPage(channel, reset, seq) {
    const name = channel?.folder || channel?.name || "";
    if (!name || !nativeBridgeUp()) return false;
    if (!reset && (_channelPage.loading || !_channelPage.hasMore)) return true;

    const sort = document.getElementById("browse-sort")?.value || "newest";
    const query = _currentVideoFilter();
    const offset = reset ? 0 : _channelPage.offset;
    if (reset) {
      _browseState.videos = [];
      _channelPage.active = true;
      _channelPage.channel = name;
      _channelPage.sort = sort;
      _channelPage.query = query;
      _channelPage.offset = 0;
      _channelPage.hasMore = true;
    }
    _channelPage.loading = true;
    _renderChannelPageSentinel();
    let stale = false;
    try {
      const outcome = await window.YT.bridge.catalogRead(
        "channel-videos",
        () => bridgeCall(
          "browse_list_videos_page",
          name, sort, CHANNEL_VIDEO_PAGE_SIZE, offset, query),
        {
          label: "channel videos",
          onStatus: _paintChannelVideoCatalogStatus,
        });
      if (outcome.stale) {
        stale = true;
        return true;
      }
      const res = outcome.value;
      if (seq && seq !== loadVideosFor._seq) {
        stale = true;
        return true;
      }
      if (res?.error) {
        _channelPage.active = false;
        _paintChannelVideoError(res.error);
        window._showToast?.(res.error, "error");
        return true;
      }
      if (!res || !Array.isArray(res.rows)) {
        _channelPage.active = false;
        _paintChannelVideoError("Couldn’t load this channel’s videos.");
        return true;
      }
      const rows = res.rows;

      const mapped = rows.map(r => _mapVideoRow(r, name));
      if (reset) {
        _browseState.videos = mapped;
      } else {
        const seen = new Set((_browseState.videos || []).map(_videoKey));
        for (const v of mapped) {
          const key = _videoKey(v);
          if (!seen.has(key)) {
            seen.add(key);
            _browseState.videos.push(v);
          }
        }
      }
      const nextOffset = Number(res?.next_offset);
      _channelPage.active = true;
      _channelPage.channel = name;
      _channelPage.sort = sort;
      _channelPage.query = query;
      _channelPage.offset = Number.isFinite(nextOffset)
        ? nextOffset : offset + mapped.length;
      _channelPage.hasMore = !!res?.has_more;
      _channelPage.loading = false;
      _reconcileCompleteChannelCount(channel);
      sortCurrentVideos(sort);
      return true;
    } catch (e) {
      console.warn("browse_list_videos_page failed:", e);
      if (reset) _channelPage.active = false;
      return false;
    } finally {
      if (!stale) {
        _channelPage.loading = false;
        _renderChannelPageSentinel();
      }
    }
  }

  function _videoKey(v) {
    return v?.video_id || v?.filepath || v?.title || "";
  }

  function _renderChannelPageSentinel() {
    const grid = document.getElementById("video-grid");
    if (!grid) return;
    grid.querySelector("#channel-video-page-sentinel")?.remove();
    if (!_channelPage.active || _channelGroupingEnabled()) return;
    if (!_channelPage.hasMore && (_browseState.videos || []).length) return;
    if (!_channelPage.hasMore && !(_browseState.videos || []).length) {
      const q = _channelPage.query || _currentVideoFilter();
      grid.innerHTML = q
        ? `<div class="browse-empty">No videos match "${escapeHtml(q)}".</div>`
        : '<div class="browse-empty">No videos in this channel yet.</div>';
      return;
    }
    const sentinel = document.createElement("div");
    sentinel.id = "channel-video-page-sentinel";
    sentinel.className = "video-grid-sentinel";
    sentinel.textContent = _channelPage.loading
      ? "Loading more videos..."
      : "... more videos, scroll to load";
    grid.appendChild(sentinel);
  }

  function _nearBottom(el) {
    if (!el) return false;
    return (el.scrollHeight - el.scrollTop - el.clientHeight) < 700;
  }

  let _channelScrollRaf = null;
  function _onChannelScroll() {
    if (_channelScrollRaf) return;
    _channelScrollRaf = requestAnimationFrame(() => {
      _channelScrollRaf = null;
      if (_browseState.view !== "videos") return;
      if (!_channelPage.active || !_channelPage.hasMore ||
          _channelPage.loading || _channelGroupingEnabled()) {
        return;
      }
      const cur = _browseState.currentChannel;
      const shown = cur ? (cur.folder || cur.name || "") : "";
      if (!shown || shown !== _channelPage.channel) return;
      if (_nearBottom(document.getElementById("view-videos")) ||
          _nearBottom(document.scrollingElement || document.documentElement)) {
        _loadChannelPage(cur, false, loadVideosFor._seq);
      }
    });
  }

  function _wireChannelPagingScroll() {
    if (_wireChannelPagingScroll._wired) return;
    _wireChannelPagingScroll._wired = true;
    document.getElementById("view-videos")
      ?.addEventListener("scroll", _onChannelScroll, { passive: true });
    window.addEventListener("scroll", _onChannelScroll, { passive: true });
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

  // Map one backend browse_list_videos row into the in-memory video shape
  // used by _browseState.videos. Shared by loadVideosFor and the live
  // download refresh (_refreshChannelVideosIfLoaded) so both stay identical.
  function _mapVideoRow(r, name) {
    // Prefer the YouTube upload date materialized by the backend over the
    // DB-insertion time. Legacy/imported rows may still use file mtime.
    const epoch = r.upload_ts || r.added_ts || 0;
    return {
      title: r.title || "",
      channel: r.channel || name,
      filepath: r.filepath || "",
      video_id: r.video_id || "",
      uploaded: _formatAddedTs(epoch),
      duration: r.duration || "",
      // Keep the backend's view_count + formatted `views` string. If these
      // are zeroed/blanked, the client-side "Most Viewed" sort silently
      // no-ops (view_count - view_count === 0 → stable sort → no reorder).
      views: r.views || "",
      upload_ts: epoch * 1000,
      view_count: r.view_count || 0,
      like_count: r.like_count || 0,
      size_bytes: r.size_bytes || 0,
      tx_status: r.tx_status || "pending",
      year: r.year, month: r.month,
      // Thumbnail sidecar (file:// URL from .Thumbnails/ or next-to-video).
      thumbnail: r.thumbnail || "",
      thumbnail_url: r.thumbnail_url || "",
      removed_from_yt: !!r.removed_from_yt,
      tracked: true,
    };
  }

  function _videoRowSig(v) {
    return [
      v.video_id || v.filepath || "",
      v.title || "",
      v.channel || "",
      v.filepath || "",
      v.thumbnail_url || "",
      v.duration || "",
      v.uploaded || "",
      Number(v.upload_ts || 0),
      Number(v.view_count || 0),
      Number(v.like_count || 0),
      Number(v.size_bytes || 0),
      v.tx_status || "",
      v.removed_from_yt ? "1" : "0",
    ].join("~");
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
    const paged = _channelPage.active && !_channelGroupingEnabled();
    window.renderVideoGrid(vids, async (v) => {
      if (typeof window._openVideoInWatch === "function") {
        window._openVideoInWatch(v);
      }
    }, { groupByYear, groupByMonth, disableClientLazy: paged });
    if (!vids.length) {
      const grid = document.getElementById("video-grid");
      if (grid) {
        const q = _channelPage.query || _currentVideoFilter();
        grid.innerHTML = q
          ? `<div class="browse-empty">No videos match "${escapeHtml(q)}".</div>`
          : '<div class="browse-empty">No videos in this channel yet.</div>';
      }
      return;
    }
    if (paged) _renderChannelPageSentinel();
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
    banner.querySelector("b").textContent = displayText(
      ch.folder || ch.name || "this channel");
    banner.querySelector("button").addEventListener("click", async (e) => {
      e.stopPropagation();
      if (!nativeBridgeUp()) {
        window._showToast?.("YTArchiver isn't ready yet. Try again in a moment.", "warn");
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

    // ── Channel action header buttons (Batch 8) ──────────────────────
    // Reuse the exact backend paths the Subs context menu uses, so a
    // channel can be synced / configured / opened straight from its
    // Browse page instead of only from the Subs tab.
    document.getElementById("cph-sync-now")?.addEventListener("click", async () => {
      const ch = _cphChannel;
      const name = ch && (ch.folder || ch.name || "");
      if (!name || !nativeBridgeUp()) return;
      try {
        const r = await bridgeCall("sync_one_channel", { name });
        window.YT?.bridge?.reportSyncOneResult?.(r, name);
      } catch (e) {
        window.YT?.bridge?.reportSyncOneResult?.({
          ok: false,
          error: "Sync failed: " + (e?.message || e),
        }, name);
      }
      _updateChannelHeader(ch);
    });
    // Queue rows are rendered asynchronously after Sync now returns. Keep the
    // open channel header tied to that authoritative payload, and do one final
    // catalog read when this channel leaves the queue. The download-landed
    // push normally refreshes sooner; this completion read is the safety net
    // for an early/missed push and guarantees the final committed row appears.
    if (!initBrowseSubmodeContent._queuePayloadWired) {
      initBrowseSubmodeContent._queuePayloadWired = true;
      window.YT?.eventState?.subscribe("queue-payload", () => {
        const ch = _cphChannel;
        const name = ch && (ch.folder || ch.name || "");
        if (!name) return;
        const previous = _cphSyncState;
        let current = "";
        try {
          current = window._queueHasSyncForChannel?.(name) || "";
        } catch (_e) { /* leave idle */ }
        // The header lives inside #view-videos, so repainting it while Watch
        // or another Browse view is active cannot leak it onto that screen.
        // It does ensure Back never reveals a stale badge or disabled button.
        _updateChannelHeader(ch);
        if (previous && !current) {
          window._refreshChannelVideosIfLoaded?.(name);
        }
      });
    }
    document.getElementById("cph-settings")?.addEventListener("click", () => {
      const ch = _cphChannel;
      const name = ch && (ch.folder || ch.name || "");
      if (!name) return;
      window._editChannelFromBrowse?.(name);
    });
    document.getElementById("cph-folder")?.addEventListener("click", async () => {
      const ch = _cphChannel;
      const name = ch && (ch.folder || ch.name || "");
      if (!name || !nativeBridgeUp()) return;
      try {
        const result = await bridgeCall("chan_open_folder", name);
        if (!result?.ok) {
          window._showToast?.(
            result?.error || "Could not open channel folder.", "error");
        }
      } catch (error) {
        window._showToast?.(
          `Could not open channel folder: ${error?.message || error}`, "error");
      }
    });
    // ⋮ More — reuse the FULL channel-card context menu (reorg, redownload,
    // re-transcribe, remove, open-on-YouTube, …) by dispatching a
    // contextmenu on this channel's grid card, anchored at the button. No
    // menu duplication; the card menu stays the single source of truth.
    document.getElementById("cph-more")?.addEventListener("click", (e) => {
      e.stopPropagation();
      const ch = _cphChannel;
      if (!ch) return;
      const folder = String(ch.folder || "").trim().toLowerCase();
      const name = String(ch.name || ch.folder || "").trim().toLowerCase();
      const card = [...document.querySelectorAll(
        "#channel-grid .channel-card")].find(
        (candidate) => folder
          ? String(candidate.dataset.channelFolder || "").trim().toLowerCase()
            === folder
          : String(candidate.dataset.channelName || "").trim().toLowerCase()
            === name);
      if (!card) return;
      const r = e.currentTarget.getBoundingClientRect();
      const menuWidth = 180; // .ctx-menu min-width; keeps dropdown right-aligned to ⋮.
      card.dispatchEvent(new MouseEvent("contextmenu", {
        bubbles: true, cancelable: true,
        clientX: Math.min(window.innerWidth - 8, Math.max(8, r.right - menuWidth)),
        clientY: Math.min(window.innerHeight - 8, r.bottom),
      }));
      // Drop the redundant "Sync now" item — the header already has a
      // dedicated Sync now button (reuse the card menu, minus that one row).
      const menu = document.querySelector("#ctx-menu-root .ctx-menu");
      if (menu) {
        menu.querySelectorAll(":scope > .ctx-menu-item").forEach((item) => {
          if (!item.classList.contains("ctx-submenu-wrap")
              && (item.textContent || "").trim().toLowerCase() === "sync now") {
            item.remove();
          }
        });
        window._markBrowseContextTrigger?.(e.currentTarget);
      }
    });
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

  let _searchHitOpenSeq = 0;
  async function _openSearchHitInWatch(hit) {
    if (!nativeBridgeUp()) {
      window._showToast?.("Playback isn't ready yet. Try again in a moment.", "warn");
      return;
    }
    // Snapshot the row before awaiting. Callers reuse/mutate their result
    // state, and the last double-click should win even if an earlier file
    // resolution finishes later.
    const snapshot = {
      jsonl_path: hit?.jsonl_path || "",
      video_id: hit?.video_id || "",
      title: hit?.title || "",
      channel: hit?.channel || "",
      start_time: Number(hit?.start_time) || 0,
      search_query: hit?._search_query || "",
    };
    const openSeq = ++_searchHitOpenSeq;
    const intentToken = window._reserveWatchOpenIntent?.();
    const originSubmode = _browseState.submode;
    const browseTab = document.querySelector('.tab[data-tab="browse"]');
    const originWasActive = !!browseTab?.classList.contains("active");
    const originIsCurrent = () =>
      (!originWasActive || !!browseTab?.classList.contains("active"))
      && (!originSubmode || _browseState.submode === originSubmode);
    try {
      const res = await bridgeCall("browse_resolve_segment",
        snapshot.jsonl_path, snapshot.video_id, snapshot.title);
      if (openSeq !== _searchHitOpenSeq) return;
      if (Number.isFinite(intentToken)
          && !window._isWatchOpenIntentCurrent?.(intentToken)) return;
      if (!originIsCurrent()) return;
      if (!res?.ok || !res.filepath) {
        window._showToast?.(res?.error || "Video file not found.", "error");
        return;
      }
      if (typeof window._openVideoInWatch !== "function") {
        window._showToast?.("Watch view is not ready yet.", "warn");
        return;
      }
      // Route every result type through the canonical Watch opener. It owns
      // transcript loading, navigation cancellation, source loading and the
      // seek-after-metadata behavior; duplicating that work here caused
      // stale result A to repaint over a newer result B.
      await window._openVideoInWatch({
        title: res.title || snapshot.title,
        channel: res.channel || snapshot.channel,
        filepath: res.filepath,
        video_id: res.video_id || snapshot.video_id,
        _seek_to: snapshot.start_time,
        _search_query: snapshot.search_query,
      }, { intentToken });
    } catch (e) {
      if (openSeq !== _searchHitOpenSeq) return;
      if (Number.isFinite(intentToken)
          && !window._isWatchOpenIntentCurrent?.(intentToken)) return;
      if (!originIsCurrent()) return;
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
    // The segment total requires an archive-wide database aggregate. Keep it
    // lazy at startup; Health → Library runs that explicit detail request
    // and applies the result to these sidebar badges when it completes.

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

    // Save one optimistic toggle, but roll the checkbox back if persistence
    // fails. Otherwise the UI claims Auto is enabled while the saved config
    // still says it is off (and the queue remains parked after restart).
    const persistAuto = async (kind, checkbox) => {
      const requested = !!checkbox.checked;
      _restored = true; // a late startup read must not overwrite this choice
      checkbox.disabled = true;
      checkbox.setAttribute("aria-busy", "true");
      try {
        if (!nativeBridgeUp()) {
          checkbox.checked = !requested;
          window._showToast?.("This setting isn't ready yet. Try again in a moment.", "warn");
          return;
        }
        const result = await bridgeCall("queue_auto_set", kind, requested);
        if (!result?.ok) {
          checkbox.checked = !requested;
          window._showToast?.(
            result?.error || "Could not save the Auto setting.", "error");
        }
      } catch (error) {
        checkbox.checked = !requested;
        window._showToast?.(
          `Could not save the Auto setting: ${error?.message || error}`,
          "error");
      } finally {
        checkbox.disabled = false;
        checkbox.removeAttribute("aria-busy");
      }
    };
    syncCB?.addEventListener("change", () => persistAuto("sync", syncCB));
    gpuCB?.addEventListener("change", () => persistAuto("gpu", gpuCB));
  }

  window.initBrowseSubmodeContent = initBrowseSubmodeContent;
  window.initQueueAutoCheckboxes = initQueueAutoCheckboxes;
  window._askWhisperModel = _askWhisperModel;
  window._askTranscribeChannel = _askTranscribeChannel;
  window.loadVideosFor = loadVideosFor;
  function _summaryVideoCount(row) {
    const raw = row?.video_count ?? row?.n_vids;
    if (typeof raw === "number") {
      return Number.isFinite(raw) && raw >= 0 ? raw : null;
    }
    const cleaned = String(raw ?? "").replace(/[\s,]/g, "");
    if (!/^\d+$/.test(cleaned)) return null;
    const parsed = Number(cleaned);
    return Number.isSafeInteger(parsed) ? parsed : null;
  }

  // A per-channel completion push already fetches every lightweight Subs row.
  // Merge that paid-for count/size result into every Browse card, not only the
  // currently-open channel. The old current-channel-only merge discarded a
  // newly-added channel's fresh count while the user was on the Channels
  // landing screen, leaving its card at "0 videos" until they opened it.
  window._refreshBrowseChannelSummaries = function (rows) {
    if (!Array.isArray(rows)) return false;
    const stateRows = Array.isArray(_browseState.channels)
      ? _browseState.channels : [];
    const previous = new Map(stateRows.map((row) => [
      _channelNameKey(row), _summaryVideoCount(row),
    ]));
    const freshKeys = new Set();
    let countsChanged = previous.size !== rows.length;
    let totalVideos = 0;

    for (const row of rows) {
      const name = row?.folder || row?.name || "";
      const key = _channelNameKey(row);
      if (!key) continue;
      freshKeys.add(key);
      const count = _summaryVideoCount(row);
      if (count !== null) totalVideos += count;
      if (!previous.has(key) || previous.get(key) !== count) {
        countsChanged = true;
      }

      // Subs rows use display-formatted counts (for example "1,234" or
      // "—"). Cards and sort code require a real number, so normalize known
      // counts and preserve an existing rich-catalog count when the cache row
      // is unavailable.
      const summary = { ...row };
      if (count === null) {
        delete summary.n_vids;
        delete summary.video_count;
      } else {
        summary.n_vids = count;
        summary.video_count = count;
      }
      window._refreshChannelCardSummary?.(name, summary);
    }
    for (const key of previous.keys()) {
      if (key && !freshKeys.has(key)) countsChanged = true;
    }

    // These sidebar badges describe the configured channel library. They were
    // previously refreshed only at startup, so Add/Remove and completed syncs
    // could leave them stale for the rest of the session.
    const channelStat = document.getElementById("stat-channels");
    const videoStat = document.getElementById("stat-videos");
    if (channelStat) channelStat.textContent = rows.length.toLocaleString();
    if (videoStat) videoStat.textContent = totalVideos.toLocaleString();

    const current = _browseState.currentChannel || _cphChannel;
    const currentKey = _channelNameKey(current);
    if (current && currentKey) {
      const fresh = rows.find((row) => _channelNameKey(row) === currentKey);
      if (fresh) {
        const count = _summaryVideoCount(fresh);
        const summary = { ...fresh };
        if (count === null) {
          delete summary.n_vids;
          delete summary.video_count;
        } else {
          summary.n_vids = count;
          summary.video_count = count;
        }
        Object.assign(current, summary);
        if (_cphChannel && _cphChannel !== current) {
          Object.assign(_cphChannel, summary);
        }
        if (_browseState.view === "videos") _updateChannelHeader(current);
      }
    }
    return countsChanged;
  };
  // Backward-compatible name for callers/tests outside this module.
  window._refreshCurrentChannelSummary =
    window._refreshBrowseChannelSummaries;
  window._filterChannelVideosPaged = function () {
    if (_browseState.view !== "videos" || !nativeBridgeUp()) return false;
    if (_channelGroupingEnabled()) return false;
    const cur = _browseState.currentChannel;
    if (!cur) return false;
    loadVideosFor(cur);
    return true;
  };

  // Live refresh of the channel video grid when a download lands for the
  // channel currently being viewed. Re-fetches that channel and re-renders
  // ONLY if the set actually changed (no flash on a no-op). Runs whether or
  // not the grid is the active view, so a download that arrives while the
  // user is on another tab is already in place when they return to Browse.
  // Hidden refreshes deliberately use the hidden-grid path below. Calling
  // loadVideosFor() while Channels / Videos / Search / etc. is visible would
  // also repaint the shared title and channel-action chrome for a page the
  // user has already left.
  let _chanRefreshBusy = false;
  let _chanRefreshPendingName = null;
  window._refreshChannelVideosIfLoaded = async function (channelName) {
    const cur = _browseState.currentChannel;
    if (!cur || !nativeBridgeUp()) return;
    const shown = cur.folder || cur.name || "";
    if (!shown) return;
    if (channelName) {
      // A specific channel's download — only relevant if we're showing it.
      if (channelName !== shown) return;
    } else if (_browseState.view !== "videos") {
      // Unknown channel (the Browse-entry safety net): only refresh when the
      // channel grid is the active view, so an unrelated background download
      // doesn't trigger a full channel re-fetch.
      return;
    }
    if (_chanRefreshBusy) {
      // A sync can land several videos while the first catalog refresh is
      // still reading. Remember the newest notification instead of dropping
      // it; the first query's SQLite snapshot may predate that later commit.
      _chanRefreshPendingName = channelName || "";
      return;
    }
    _chanRefreshBusy = true;
    try {
      const sort = document.getElementById("browse-sort")?.value || "newest";
      const loadSeq = loadVideosFor._seq || 0;
      const channelViewIsVisible = () => {
        const active = _browseState.currentChannel;
        const activeName = active ? (active.folder || active.name || "") : "";
        return _browseState.view === "videos" && activeName === shown;
      };
      if (_channelPage.active && !_channelGroupingEnabled()) {
        if (channelViewIsVisible()) {
          await loadVideosFor(cur);
        } else {
          // Refresh paged data without the title/header/month-control writes
          // performed by loadVideosFor(). The page loader only touches the
          // hidden channel grid and keeps its offset/has-more state coherent.
          await _loadChannelPage(cur, true, loadSeq);
        }
        return;
      }
      const outcome = await window.YT.bridge.catalogRead(
        "channel-videos",
        () => bridgeCall("browse_list_videos", shown, sort, 50000),
        { label: "channel videos" });
      if (outcome.stale) return;
      const rows = outcome.value;
      // Discard if the user opened/reloaded a channel while this background
      // read was pending. That newer load is authoritative. Merely leaving
      // the channel page is safe: the refreshed grid is hidden and ready if
      // the user returns.
      if ((loadVideosFor._seq || 0) !== loadSeq) return;
      const curNow = _browseState.currentChannel;
      const shownNow = curNow ? (curNow.folder || curNow.name || "") : "";
      if (shownNow !== shown || !Array.isArray(rows)) return;
      const mappedRows = rows.map(r => _mapVideoRow(r, shown));
      const newSig = mappedRows.map(r => _videoRowSig(r)).join("|");
      const oldSig = (_browseState.videos || [])
        .map(v => _videoRowSig(v)).join("|");
      if (newSig === oldSig) return;   // nothing new — leave the grid as-is
      _browseState.videos = mappedRows;
      sortCurrentVideos(sort);
    } catch (_e) { /* non-fatal — leave the current grid as-is */ }
    finally {
      _chanRefreshBusy = false;
      const pendingName = _chanRefreshPendingName;
      _chanRefreshPendingName = null;
      if (pendingName !== null) {
        setTimeout(() => {
          window._refreshChannelVideosIfLoaded(
            pendingName || undefined);
        }, 0);
      }
    }
  };
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", _wireChannelPagingScroll,
                              { once: true });
  } else {
    _wireChannelPagingScroll();
  }
  window.sortCurrentVideos = sortCurrentVideos;
  window._formatTs = _formatTs;
  window.renderSearchResults = renderSearchResults;
  window.populateIndexTable = populateIndexTable;
})();
