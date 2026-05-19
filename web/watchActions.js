/**
 * web/watchActions.js — Watch view action buttons + state.
 *
 * Extracted from app.js. Wires every interactive control on the watch
 * view:
 *   - Playback speed (persisted)
 *   - Video-scoped keyboard shortcuts (Space, arrows, B, M)
 *   - Open in external player
 *   - Redownload at chosen resolution
 *   - Per-video metadata refresh
 *   - Re-transcribe with Whisper (model picker + in-flight tracking)
 *   - Transcript font size +/− (persisted)
 *   - Caption overlay size/bg toggles (persisted)
 *   - Transcript pane resize splitter (persisted)
 *   - Bookmark current moment / whole video
 *   - In-transcript find with next/prev cycling
 *
 * Also publishes the in-flight retranscribe state used by logs.js:
 *   window._inflightRetranscribes
 *   window._syncWatchRetranscribeButton
 *   window._retranscribeWatchUpdateProgress
 *   window._retranscribeWatchClear
 *
 * Exposed as window.initWatchActions; app.js boot calls it once.
 *
 * Depends on:
 *   - window._browseState (published by app.js)
 *   - window.askChoice (modals.js)
 *   - window._showToast (toasts.js)
 *   - window._askWhisperModel (app.js)
 *   - window._askBookmarkKind (app.js)
 *   - window._formatTs (util.js)
 *   - window._scrollTranscriptTo (logs.js, optional)
 *   - window.loadWatchMetadataDrawer (app.js, optional)
 *   - window.refreshBookmarks (app.js, optional)
 *   - window.setCaptionPref (logs.js)
 *   - window.YT.bridge.bridgeCall (bridge.js)
 *   - window.pywebview.api.* (native bridge)
 */
(function () {
  "use strict";

  function _bridgeCall(method, ...args) {
    const fn = window.YT?.bridge?.bridgeCall;
    if (fn) return fn(method, ...args);
    return undefined;
  }

  function initWatchActions() {
    const _browseState = window._browseState;
    if (!_browseState) {
      console.warn("[watchActions] window._browseState not published yet");
      return;
    }

    // Playback speed
    const speedSel = document.getElementById("watch-speed");
    const vEl = document.getElementById("watch-video");
    // Speed used to reset to 1.0x on every video switch because it
    // wasn't persisted. Mirror the volume-persistence pattern
    // (_applyPersistedVolume in logs.js) so the user's chosen speed
    // sticks across videos and sessions.
    const _SPEED_KEY = "ytarch.watchSpeed";
    try {
      // Use Number.isFinite so a stored "0" (or any other falsy-but-
      // valid number) isn't coerced back to 1.
      const _raw = parseFloat(localStorage.getItem(_SPEED_KEY) || "1");
      const saved = Number.isFinite(_raw) && _raw > 0 ? _raw : 1;
      if (speedSel) {
        // Only assign if the saved value exists as an <option>.
        if ([...speedSel.options].some(o => parseFloat(o.value) === saved)) {
          speedSel.value = String(saved);
        }
      }
      if (vEl) vEl.playbackRate = saved;
    } catch {}
    // Apply persisted speed every time a new video source loads.
    vEl?.addEventListener("loadedmetadata", () => {
      try {
        const _vRaw = parseFloat(localStorage.getItem(_SPEED_KEY) || "1");
        const v = Number.isFinite(_vRaw) && _vRaw > 0 ? _vRaw : 1;
        vEl.playbackRate = v;
      } catch {}
    });
    speedSel?.addEventListener("change", () => {
      const v = parseFloat(speedSel.value) || 1.0;
      if (vEl) vEl.playbackRate = v;
      try { localStorage.setItem(_SPEED_KEY, String(v)); } catch {}
    });

    // Video-scoped keyboard shortcuts (only active when the watch view is visible)
    document.addEventListener("keydown", (e) => {
      const watchVisible = document.getElementById("view-watch")
        && document.getElementById("view-watch").style.display !== "none";
      if (!watchVisible || !vEl) return;
      // Include TEXTAREA so typing in a bookmark-note textarea doesn't
      // trigger Space/Arrow/B/M video shortcuts.
      const _tag = e.target.tagName;
      const editing = _tag === "INPUT" || _tag === "TEXTAREA"
                      || e.target.isContentEditable;
      if (editing) return;
      if (e.key === " " || e.code === "Space") {
        e.preventDefault();
        if (vEl.paused) vEl.play().catch(()=>{}); else vEl.pause();
      } else if (e.key === "ArrowLeft") {
        e.preventDefault();
        vEl.currentTime = Math.max(0, vEl.currentTime - 5);
      } else if (e.key === "ArrowRight") {
        e.preventDefault();
        vEl.currentTime = Math.min(vEl.duration || vEl.currentTime, vEl.currentTime + 5);
      } else if (e.key === "ArrowUp") {
        e.preventDefault();
        vEl.volume = Math.min(1, (vEl.volume || 0) + 0.1);
      } else if (e.key === "ArrowDown") {
        e.preventDefault();
        vEl.volume = Math.max(0, (vEl.volume || 0) - 0.1);
      } else if (e.key === "b" || e.key === "B") {
        e.preventDefault();
        document.getElementById("btn-bookmark-now")?.click();
      } else if (e.key === "m" || e.key === "M") {
        e.preventDefault();
        vEl.muted = !vEl.muted;
      }
    });

    document.getElementById("btn-open-external")?.addEventListener("click", () => {
      const v = _browseState.currentVideo;
      if (!v?.filepath) { window._showToast?.("No file loaded.", "warn"); return; }
      window.pywebview?.api?.browse_open_video?.(v.filepath);
    });

    // Redownload current video — resolution picker, then video_redownload.
    document.getElementById("btn-watch-redownload")?.addEventListener("click", async () => {
      const v = _browseState.currentVideo;
      if (!v?.video_id && !v?.filepath) {
        window._showToast?.("No video loaded.", "warn");
        return;
      }
      const pick = await (window.askChoice ? window.askChoice({
        title: "Redownload at…",
        message: `Replace the local file for "${v.title || v.video_id}" ` +
                 `with a new download at the chosen resolution. This keeps the ` +
                 `existing filename so transcripts and bookmarks still match.`,
        choices: [
          { label: "360p", value: "360" },
          { label: "480p", value: "480" },
          { label: "720p", value: "720" },
          { label: "1080p", value: "1080" },
          { label: "1440p", value: "1440" },
          { label: "2160p (4K)", value: "2160" },
          { label: "Best available", value: "best", primary: true },
        ],
      }) : null);
      if (!pick) return;
      const _VALID_RES = new Set(
        ["audio", "144", "240", "360", "480", "720",
         "1080", "1440", "2160", "best"]);
      if (!_VALID_RES.has(pick)) {
        window._showToast?.(`Invalid resolution: ${pick}`, "error");
        return;
      }
      _bridgeCall("video_redownload", v.video_id || "", v.title || "", pick);
      window._showToast?.(`Redownload queued at ${pick}.`, "ok");
    });

    // Per-video metadata refresh: synchronous yt-dlp fetch for THIS video.
    document.getElementById("btn-watch-refresh-meta")?.addEventListener("click", async () => {
      const v = _browseState.currentVideo || window._watchCurrentVideo;
      if (!v?.filepath && !v?.video_id) {
        window._showToast?.("No video loaded.", "warn");
        return;
      }
      const btn = document.getElementById("btn-watch-refresh-meta");
      const api = window.pywebview?.api;
      if (!api?.browse_refresh_video_metadata) {
        window._showToast?.("Refresh unavailable in browser-preview mode.", "warn");
        return;
      }
      const _origText = btn ? btn.textContent : "";
      if (btn) { btn.disabled = true; btn.textContent = "Refreshing…"; }
      try {
        const res = await api.browse_refresh_video_metadata({
          filepath: v.filepath || "",
          video_id: v.video_id || "",
          title: v.title || "",
          channel: v.channel || "",
        });
        if (res?.ok) {
          window._showToast?.("Metadata refreshed.", "ok");
          window.loadWatchMetadataDrawer?.(v);
        } else {
          const msg = res?.error || "Refresh failed.";
          window._showToast?.(msg, res?.transient ? "warn" : "error");
        }
      } catch (e) {
        window._showToast?.(`Refresh failed: ${e.message || e}`, "error");
      } finally {
        if (btn) { btn.disabled = false; btn.textContent = _origText; }
      }
    });

    // Transcript font size +/- with persistence.
    const _TX_FONT_MIN = 9.5;
    const _TX_FONT_MAX = 22;
    const _txFontKey = "ytarchiver_tx_font_px";
    function _applyTxFontSize(px) {
      const v = Math.max(_TX_FONT_MIN,
        Math.min(_TX_FONT_MAX, parseFloat(px) || 12.5));
      document.documentElement.style.setProperty(
        "--watch-transcript-fz", v.toFixed(1) + "px");
      try { localStorage.setItem(_txFontKey, String(v)); } catch {}
      const _api = window.pywebview?.api;
      if (_api?.settings_save) {
        try { _api.settings_save({ transcript_font_size: v }); }
        catch {}
      }
    }
    try {
      const _stored = parseFloat(localStorage.getItem(_txFontKey) || "");
      if (Number.isFinite(_stored) && _stored > 0) _applyTxFontSize(_stored);
    } catch {}
    (async () => {
      try {
        const s = await window.pywebview?.api?.settings_load?.();
        const v = parseFloat(s?.transcript_font_size);
        if (Number.isFinite(v) && v > 0) _applyTxFontSize(v);
      } catch {}
    })();
    document.getElementById("btn-tx-font-down")?.addEventListener("click", () => {
      const cur = parseFloat(getComputedStyle(document.documentElement)
        .getPropertyValue("--watch-transcript-fz")) || 12.5;
      _applyTxFontSize(cur - 1);
    });
    document.getElementById("btn-tx-font-up")?.addEventListener("click", () => {
      const cur = parseFloat(getComputedStyle(document.documentElement)
        .getPropertyValue("--watch-transcript-fz")) || 12.5;
      _applyTxFontSize(cur + 1);
    });

    // Caption overlay size + background controls (persisted).
    const _capSizeKey = "ytarchiver_caption_size";
    const _capBgKey   = "ytarchiver_caption_bg";
    const _CAP_SIZES = new Set(["off", "small", "medium", "large"]);
    const _CAP_BGS   = new Set(["translucent", "outline", "none"]);
    function _applyCapSize(size) {
      const v = _CAP_SIZES.has(size) ? size : "off";
      window.setCaptionPref?.("size", v);
      const sel = document.getElementById("watch-cap-size");
      if (sel && sel.value !== v) sel.value = v;
      try { localStorage.setItem(_capSizeKey, v); } catch {}
      const _api = window.pywebview?.api;
      if (_api?.settings_save) {
        try { _api.settings_save({ caption_overlay_size: v }); } catch {}
      }
    }
    function _applyCapBg(bg) {
      const v = _CAP_BGS.has(bg) ? bg : "translucent";
      window.setCaptionPref?.("bg", v);
      const sel = document.getElementById("watch-cap-bg");
      if (sel && sel.value !== v) sel.value = v;
      try { localStorage.setItem(_capBgKey, v); } catch {}
      const _api = window.pywebview?.api;
      if (_api?.settings_save) {
        try { _api.settings_save({ caption_overlay_bg: v }); } catch {}
      }
    }
    try {
      const _sz = localStorage.getItem(_capSizeKey);
      if (_sz && _CAP_SIZES.has(_sz)) _applyCapSize(_sz);
      const _bg = localStorage.getItem(_capBgKey);
      if (_bg && _CAP_BGS.has(_bg)) _applyCapBg(_bg);
    } catch {}
    (async () => {
      try {
        const s = await window.pywebview?.api?.settings_load?.();
        if (s?.caption_overlay_size && _CAP_SIZES.has(s.caption_overlay_size)) {
          _applyCapSize(s.caption_overlay_size);
        }
        if (s?.caption_overlay_bg && _CAP_BGS.has(s.caption_overlay_bg)) {
          _applyCapBg(s.caption_overlay_bg);
        }
      } catch {}
    })();
    document.getElementById("watch-cap-size")?.addEventListener("change", (ev) => {
      _applyCapSize(ev.target.value);
    });
    document.getElementById("watch-cap-bg")?.addEventListener("change", (ev) => {
      _applyCapBg(ev.target.value);
    });

    // Drag-resize splitter between video and transcript panels.
    const _txWidthKey = "ytarchiver_tx_pane_width";
    const _TX_WIDTH_MIN = 240;
    const _TX_WIDTH_MAX = 1400;
    function _applyTxWidth(px) {
      const v = Math.max(_TX_WIDTH_MIN,
        Math.min(_TX_WIDTH_MAX, parseInt(px, 10) || 420));
      document.documentElement.style.setProperty(
        "--watch-tx-width", v + "px");
      try { localStorage.setItem(_txWidthKey, String(v)); } catch {}
    }
    function _persistTxWidth(px) {
      const _api = window.pywebview?.api;
      if (_api?.settings_save) {
        try { _api.settings_save({ transcript_pane_width: parseInt(px, 10) }); }
        catch {}
      }
    }
    try {
      const _stored = parseInt(localStorage.getItem(_txWidthKey) || "", 10);
      if (Number.isFinite(_stored) && _stored > 0) _applyTxWidth(_stored);
    } catch {}
    (async () => {
      try {
        const s = await window.pywebview?.api?.settings_load?.();
        const v = parseInt(s?.transcript_pane_width, 10);
        if (Number.isFinite(v) && v > 0) _applyTxWidth(v);
      } catch {}
    })();
    const _splitter = document.getElementById("watch-splitter");
    if (_splitter) {
      let _dragStart = null;
      _splitter.addEventListener("mousedown", (e) => {
        e.preventDefault();
        const layout = _splitter.parentElement;
        if (!layout) return;
        const cur = parseFloat(getComputedStyle(document.documentElement)
          .getPropertyValue("--watch-tx-width")) || 420;
        _dragStart = { x: e.clientX, startWidth: cur, layout };
        _splitter.classList.add("dragging");
        document.body.style.cursor = "col-resize";
      });
      window.addEventListener("mousemove", (e) => {
        if (!_dragStart) return;
        // Dragging RIGHT shrinks the transcript pane (it's on the right),
        // dragging LEFT grows it. So: newWidth = startWidth - delta.
        const delta = e.clientX - _dragStart.x;
        const newWidth = _dragStart.startWidth - delta;
        _applyTxWidth(newWidth);
      });
      window.addEventListener("mouseup", () => {
        if (!_dragStart) return;
        _dragStart = null;
        _splitter.classList.remove("dragging");
        document.body.style.cursor = "";
        // Persist on drop, not on every move.
        const cur = parseInt(getComputedStyle(document.documentElement)
          .getPropertyValue("--watch-tx-width"), 10);
        if (Number.isFinite(cur) && cur > 0) _persistTxWidth(cur);
      });
    }

    // Tracks every in-flight watch-view retranscribe by video_id. The
    // button is a property of the watch view (one DOM element), but
    // in-flight jobs are a property of the video — so the button state
    // must be derived from "is THIS video in the map?" rather than a
    // single global busy flag. Without this split, navigating to Video
    // B while A is retranscribing left the button locked on A's
    // progress; clicking Re-transcribe on B was blocked until A finished.
    window._inflightRetranscribes = window._inflightRetranscribes || new Map();

    // Paint the Re-transcribe button to match the currently-displayed
    // watch video. Called on click, on progress update for the current
    // video, on clear, and (from logs.js) when the watch view renders
    // a different video.
    window._syncWatchRetranscribeButton = function () {
      const btn = document.getElementById("btn-watch-retranscribe");
      if (!btn) return;
      const cur = window._watchCurrentVideo;
      const vid = cur && cur.video_id ? cur.video_id : "";
      if (vid && window._inflightRetranscribes.has(vid)) {
        const p = window._inflightRetranscribes.get(vid);
        btn.dataset.busy = "1";
        btn.disabled = true;
        btn.textContent = `Re-transcribing… ${p}%`;
      } else {
        btn.dataset.busy = "";
        btn.disabled = false;
        btn.textContent = "Re-transcribe…";
      }
    };

    document.getElementById("btn-watch-retranscribe")?.addEventListener("click", async () => {
      const v = _browseState.currentVideo;
      const api = window.pywebview?.api;
      if (!api?.transcribe_retranscribe || !v?.filepath) {
        window._showToast?.("No file loaded.", "warn");
        return;
      }
      const vid = v.video_id || "";
      if (vid && window._inflightRetranscribes.has(vid)) {
        window._showToast?.(
          "Re-transcribe already queued for this video.", "warn");
        return;
      }
      // Wrap the await chain so a thrown rejection (rare but possible
      // when pywebview's bridge times out) surfaces as a visible toast
      // instead of being swallowed as an unhandled promise.
      let model;
      try {
        model = await (window._askWhisperModel?.(`"${v.title}"`));
      } catch (e) {
        window._showToast?.(`Model picker failed: ${e?.message || e}`, "error");
        return;
      }
      if (!model) return; // user cancelled
      let res;
      try {
        res = await api.transcribe_retranscribe(
          v.filepath, v.title || "", vid);
      } catch (e) {
        window._showToast?.(`Re-transcribe call failed: ${e?.message || e}`, "error");
        return;
      }
      if (res?.ok) {
        window._showToast?.(
          `Queued ${model} re-transcription.`, "ok");
        if (vid) window._inflightRetranscribes.set(vid, 0);
        window._syncWatchRetranscribeButton();
      } else {
        window._showToast?.(res?.error || "Re-transcribe failed.", "error");
      }
    });

    // Called from logs.js when a whisper_pct line goes by.
    window._retranscribeWatchUpdateProgress = function (pct, video_id) {
      if (!video_id) return;
      const p = Math.max(0, Math.min(99, parseInt(pct, 10) || 0));
      if (window._inflightRetranscribes.has(video_id)) {
        window._inflightRetranscribes.set(video_id, p);
      }
      const cur = window._watchCurrentVideo;
      if (cur && cur.video_id === video_id) {
        window._syncWatchRetranscribeButton();
      }
    };
    // Called from _onRetranscribeComplete with the finished video_id.
    window._retranscribeWatchClear = function (video_id) {
      if (video_id) {
        window._inflightRetranscribes.delete(video_id);
      } else {
        window._inflightRetranscribes.clear();
      }
      window._syncWatchRetranscribeButton();
    };

    document.getElementById("btn-bookmark-now")?.addEventListener("click", async () => {
      const _vEl = document.getElementById("watch-video");
      const v = _browseState.currentVideo;
      if (!v || !window.pywebview?.api?.bookmark_add) {
        window._showToast?.("Native mode required.", "warn");
        return;
      }
      const kind = await (window._askBookmarkKind?.());
      if (!kind) return;
      let t = -1;            // -1 sentinel = whole-video bookmark
      let text = "";
      if (kind === "yes") {
        t = _vEl ? _vEl.currentTime : 0;
        const segs = document.querySelectorAll("#watch-transcript .seg");
        for (const s of segs) {
          if (s.classList.contains("active")) {
            text = s.textContent;
            break;
          }
        }
      }
      const res = await window.pywebview.api.bookmark_add({
        video_id: v.video_id || "",
        title: v.title || "",
        channel: v.channel || "",
        start_time: t,
        text: text.slice(0, 200),
        note: "",
      });
      if (res?.ok) {
        window._showToast?.(
          kind === "yes"
            ? "Bookmarked @ " + (window._formatTs ? window._formatTs(t) : t.toFixed(1))
            : "Video bookmarked.",
          "ok");
        try {
          if (typeof window.refreshBookmarks === "function") {
            window.refreshBookmarks();
          }
        } catch (_bre) { /* non-fatal */ }
      } else {
        window._showToast?.(res?.error || "Bookmark failed.", "error");
      }
    });

    // Watch-find: cycle through ALL matches with running "N of M" count.
    const watchFind = document.getElementById("watch-find");
    const watchFindCount = document.getElementById("watch-find-count");
    const watchFindNext = document.getElementById("watch-find-next");
    const watchFindPrev = document.getElementById("watch-find-prev");
    const findState = { matches: [], idx: -1, q: "" };

    function _rebuildFindMatches() {
      const q = (watchFind?.value || "").toLowerCase().trim();
      const tr = document.getElementById("watch-transcript");
      if (!tr) return;
      tr.querySelectorAll(".find-hit, .find-hit-active").forEach(e => {
        e.classList.remove("find-hit", "find-hit-active");
      });
      findState.q = q;
      findState.matches = [];
      findState.idx = -1;
      if (!q) {
        if (watchFindCount) watchFindCount.textContent = "";
        return;
      }
      const segs = tr.querySelectorAll(".seg");
      for (const s of segs) {
        if (s.textContent.toLowerCase().includes(q)) {
          s.classList.add("find-hit");
          findState.matches.push(s);
        }
      }
      if (watchFindCount) {
        watchFindCount.textContent = findState.matches.length
          ? `0 of ${findState.matches.length}`
          : "no matches";
      }
      if (findState.matches.length) _findGoTo(0);
    }

    function _findGoTo(i) {
      if (!findState.matches.length) return;
      const n = findState.matches.length;
      const idx = ((i % n) + n) % n;
      if (findState.idx >= 0 && findState.matches[findState.idx]) {
        findState.matches[findState.idx].classList.remove("find-hit-active");
      }
      findState.idx = idx;
      const el = findState.matches[idx];
      if (el) {
        el.classList.add("find-hit-active");
        const tr = document.getElementById("watch-transcript");
        if (tr && window._scrollTranscriptTo) {
          window._scrollTranscriptTo(tr, el);
        } else {
          el.scrollIntoView({ behavior: "smooth", block: "center" });
        }
      }
      if (watchFindCount) {
        watchFindCount.textContent = `${idx + 1} of ${n}`;
      }
    }

    watchFind?.addEventListener("input", _rebuildFindMatches);
    watchFind?.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        if (!findState.matches.length) {
          _rebuildFindMatches();
          return;
        }
        _findGoTo(findState.idx + (e.shiftKey ? -1 : 1));
      } else if (e.key === "Escape") {
        watchFind.value = "";
        _rebuildFindMatches();
        watchFind.blur();
      }
    });
    watchFindNext?.addEventListener("click", () => _findGoTo(findState.idx + 1));
    watchFindPrev?.addEventListener("click", () => _findGoTo(findState.idx - 1));
  }

  window.initWatchActions = initWatchActions;
})();
