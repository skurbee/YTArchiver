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
 */
(function () {
  "use strict";

  function _bridgeCall(method, ...args) {
    const fn = window.YT?.bridge?.bridgeCall;
    if (fn) return fn(method, ...args);
    return undefined;
  }

  function _nativeBridgeUp() {
    return !!window.YT?.bridge?.isUp?.();
  }

  function initWatchActions() {
    // Re-init guard — multiple inits would stack duplicate window
    // mousemove/mouseup/keydown listeners plus duplicate
    // loadedmetadata listeners on vEl. After a few inits, Space
    // toggled play/pause TWICE per press and ArrowRight skipped
    // 2x/3x as far.
    if (window._watchActionsInited) return;
    window._watchActionsInited = true;
    const _browseState = window._browseState;
    if (!_browseState) {
      console.warn("[watchActions] window._browseState not published yet");
      return;
    }

    function _sameWatchVideo(a, b) {
      if (!a || !b) return false;
      if (a.video_id && b.video_id) return a.video_id === b.video_id;
      const norm = (s) => String(s || "").replace(/\\/g, "/").toLowerCase();
      if (a.filepath && b.filepath) return norm(a.filepath) === norm(b.filepath);
      return (a.title || "") === (b.title || "")
        && (a.channel || "") === (b.channel || "");
    }

    function _watchActionVideo() {
      const rendered = window._watchCurrentVideo || null;
      const pending = _browseState.currentVideo || null;
      const openToken = window._watchOpenToken;
      const renderedToken = window._watchRenderedToken;
      if (rendered && pending && !_sameWatchVideo(rendered, pending)) {
        window._showToast?.("Video is still loading - try again in a moment.", "warn");
        return null;
      }
      if (rendered && Number.isFinite(openToken)
          && Number.isFinite(renderedToken)
          && renderedToken !== openToken) {
        window._showToast?.("Video is still loading - try again in a moment.", "warn");
        return null;
      }
      return rendered || pending;
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
      const watchView = document.getElementById("view-watch");
      const watchVisible = !!(watchView && !watchView.hidden);
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
        // Use Infinity as the upper bound when vEl.duration is NaN
        // (video still loading metadata). Old `vEl.duration ||
        // vEl.currentTime` collapsed NaN→currentTime, which clamped
        // the seek to the current time and made the right-arrow
        // appear broken (audit: watchActions.js:104-110).
        const _max = Number.isFinite(vEl.duration) ? vEl.duration : Infinity;
        vEl.currentTime = Math.min(_max, vEl.currentTime + 5);
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
      const v = _watchActionVideo();
      if (!v?.filepath) { window._showToast?.("No file loaded.", "warn"); return; }
      _bridgeCall("browse_open_video", v.filepath);
    });

    // Redownload current video — resolution picker, then video_redownload.
    document.getElementById("btn-watch-redownload")?.addEventListener("click", async () => {
      let v = _watchActionVideo();
      if (!v?.video_id && !v?.filepath) {
        window._showToast?.("No video loaded.", "warn");
        return;
      }
      const actionToken = window._watchOpenToken;
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
      if (Number.isFinite(actionToken)
          && Number.isFinite(window._watchOpenToken)
          && actionToken !== window._watchOpenToken) {
        window._showToast?.("Video changed before redownload was queued.", "warn");
        return;
      }
      v = _watchActionVideo();
      if (!v) return;
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
      const v = _watchActionVideo();
      if (!v?.filepath && !v?.video_id) {
        window._showToast?.("No video loaded.", "warn");
        return;
      }
      const btn = document.getElementById("btn-watch-refresh-meta");
      if (!_nativeBridgeUp()) {
        window._showToast?.("Refresh unavailable in browser-preview mode.", "warn");
        return;
      }
      // Use the stable label from dataset.label (or "Refresh metadata"
      // as a hard-coded fallback) instead of the live textContent. If
      // a previous in-flight call somehow paints "Refreshing…" while
      // a second click sneaks past the disabled guard, the old code
      // captured that intermediate label as "original" and the button
      // never recovered (audit: watchActions.js:198).
      if (btn && !btn.dataset.label) btn.dataset.label = btn.textContent;
      const _origText = (btn && btn.dataset.label) || "Refresh metadata";
      if (btn) { btn.disabled = true; btn.textContent = "Refreshing…"; }
      try {
        const res = await _bridgeCall("browse_refresh_video_metadata", {
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
    // Debounced settings_save so rapid +/- clicks don't spam the
    // bridge with one save per keypress (audit: watchActions.js:
    // 206-217). LocalStorage update is immediate so the size
    // restores correctly if the user navigates away mid-debounce.
    let _txFontSaveTimer = null;
    function _applyTxFontSize(px) {
      const v = Math.max(_TX_FONT_MIN,
        Math.min(_TX_FONT_MAX, parseFloat(px) || 12.5));
      document.documentElement.style.setProperty(
        "--watch-transcript-fz", v.toFixed(1) + "px");
      try { localStorage.setItem(_txFontKey, String(v)); } catch {}
      if (_nativeBridgeUp()) {
        if (_txFontSaveTimer) clearTimeout(_txFontSaveTimer);
        _txFontSaveTimer = setTimeout(() => {
          try { _bridgeCall("settings_save", { transcript_font_size: v }); }
          catch {}
        }, 300);
      }
    }
    try {
      const _stored = parseFloat(localStorage.getItem(_txFontKey) || "");
      if (Number.isFinite(_stored) && _stored > 0) _applyTxFontSize(_stored);
    } catch {}
    (async () => {
      try {
        if (!_nativeBridgeUp()) return;
        const s = await _bridgeCall("settings_load");
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

    // Caption overlay size + background + mode controls (persisted).
    const _capSizeKey = "ytarchiver_caption_size";
    const _capBgKey   = "ytarchiver_caption_bg";
    const _capModeKey = "ytarchiver_caption_mode";
    const _CAP_SIZES = new Set(["off", "small", "medium", "large"]);
    const _CAP_BGS   = new Set(["translucent", "outline", "none"]);
    const _CAP_MODES = new Set(["single", "phrase3"]);
    function _applyCapSize(size) {
      const v = _CAP_SIZES.has(size) ? size : "off";
      window.setCaptionPref?.("size", v);
      const sel = document.getElementById("watch-cap-size");
      if (sel && sel.value !== v) sel.value = v;
      // Style + word-count only matter when the overlay is ON. Collapse
      // them when size=off so the default state isn't two mystery selects.
      const extras = document.getElementById("watch-overlay-extras");
      if (extras) extras.classList.toggle("collapsed", v === "off");
      try { localStorage.setItem(_capSizeKey, v); } catch {}
      if (_nativeBridgeUp()) {
        try { _bridgeCall("settings_save", { caption_overlay_size: v }); } catch {}
      }
    }
    function _applyCapBg(bg) {
      const v = _CAP_BGS.has(bg) ? bg : "translucent";
      window.setCaptionPref?.("bg", v);
      const sel = document.getElementById("watch-cap-bg");
      if (sel && sel.value !== v) sel.value = v;
      try { localStorage.setItem(_capBgKey, v); } catch {}
      if (_nativeBridgeUp()) {
        try { _bridgeCall("settings_save", { caption_overlay_bg: v }); } catch {}
      }
    }
    function _applyCapMode(mode) {
      const v = _CAP_MODES.has(mode) ? mode : "single";
      window.setCaptionPref?.("mode", v);
      const sel = document.getElementById("watch-cap-mode");
      if (sel && sel.value !== v) sel.value = v;
      try { localStorage.setItem(_capModeKey, v); } catch {}
      if (_nativeBridgeUp()) {
        try { _bridgeCall("settings_save", { caption_overlay_mode: v }); } catch {}
      }
    }
    try {
      const _sz = localStorage.getItem(_capSizeKey);
      if (_sz && _CAP_SIZES.has(_sz)) _applyCapSize(_sz);
      const _bg = localStorage.getItem(_capBgKey);
      if (_bg && _CAP_BGS.has(_bg)) _applyCapBg(_bg);
      const _md = localStorage.getItem(_capModeKey);
      if (_md && _CAP_MODES.has(_md)) _applyCapMode(_md);
    } catch {}
    (async () => {
      try {
        if (!_nativeBridgeUp()) return;
        const s = await _bridgeCall("settings_load");
        if (s?.caption_overlay_size && _CAP_SIZES.has(s.caption_overlay_size)) {
          _applyCapSize(s.caption_overlay_size);
        }
        if (s?.caption_overlay_bg && _CAP_BGS.has(s.caption_overlay_bg)) {
          _applyCapBg(s.caption_overlay_bg);
        }
        if (s?.caption_overlay_mode && _CAP_MODES.has(s.caption_overlay_mode)) {
          _applyCapMode(s.caption_overlay_mode);
        }
      } catch {}
    })();
    document.getElementById("watch-cap-size")?.addEventListener("change", (ev) => {
      _applyCapSize(ev.target.value);
    });
    document.getElementById("watch-cap-bg")?.addEventListener("change", (ev) => {
      _applyCapBg(ev.target.value);
    });
    document.getElementById("watch-cap-mode")?.addEventListener("change", (ev) => {
      _applyCapMode(ev.target.value);
    });

    // Non-speech ([Music] / [Applause] / ♪) visibility toggle for the
    // transcript. Adds/removes `.hide-nonspeech` on the transcript
    // container (which survives per-video re-renders since only its inner
    // body is rebuilt). Persisted across sessions.
    const _nonSpeechKey = "ytarchiver_hide_nonspeech";
    function _applyNonSpeech(hide) {
      const tr = document.getElementById("watch-transcript");
      const btn = document.getElementById("btn-tx-nonspeech");
      if (tr) tr.classList.toggle("hide-nonspeech", !!hide);
      if (btn) {
        btn.classList.toggle("active", !!hide);
        btn.setAttribute("aria-pressed", hide ? "true" : "false");
        btn.innerHTML = hide ? "Show ♪" : "Hide ♪";
      }
      try { localStorage.setItem(_nonSpeechKey, hide ? "1" : "0"); } catch {}
    }
    try {
      if (localStorage.getItem(_nonSpeechKey) === "1") _applyNonSpeech(true);
    } catch {}
    document.getElementById("btn-tx-nonspeech")?.addEventListener("click", () => {
      const tr = document.getElementById("watch-transcript");
      _applyNonSpeech(!(tr && tr.classList.contains("hide-nonspeech")));
    });

    // ⋮ More — overflow menu for less-used watch-view actions.
    // Reuses the hidden source buttons' click handlers so we don't have
    // to re-implement the redownload / re-transcribe / refresh flows.
    document.getElementById("btn-watch-more")?.addEventListener("click", (e) => {
      e.preventDefault();
      const showMenu = window.showContextMenu;
      if (!showMenu) return;
      const rect = e.currentTarget.getBoundingClientRect();
      const click = (id) => () => document.getElementById(id)?.click();
      showMenu(rect.left, rect.bottom + 4, [
        { label: "Redownload…", action: click("btn-watch-redownload") },
        { label: "Re-transcribe…", action: click("btn-watch-retranscribe") },
        { label: "Refresh metadata", action: click("btn-watch-refresh-meta") },
      ]);
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
      if (_nativeBridgeUp()) {
        try {
          _bridgeCall("settings_save", {
            transcript_pane_width: parseInt(px, 10),
          });
        }
        catch {}
      }
    }
    try {
      const _stored = parseInt(localStorage.getItem(_txWidthKey) || "", 10);
      if (Number.isFinite(_stored) && _stored > 0) _applyTxWidth(_stored);
    } catch {}
    (async () => {
      try {
        if (!_nativeBridgeUp()) return;
        const s = await _bridgeCall("settings_load");
        const v = parseInt(s?.transcript_pane_width, 10);
        if (Number.isFinite(v) && v > 0) _applyTxWidth(v);
      } catch {}
    })();
    const _splitter = document.getElementById("watch-splitter");
    if (_splitter) {
      let _dragStart = null;
      // Splitter drag — bind mousemove/mouseup only WHILE dragging,
      // not for the entire page lifetime. The previous global
      // listeners fired on every mousemove anywhere in the app (60+
      // times/second during normal user motion); each fire was a
      // null-check early-return but the dispatch overhead was real.
      const _onMove = (e) => {
        if (!_dragStart) return;
        const delta = e.clientX - _dragStart.x;
        const newWidth = _dragStart.startWidth - delta;
        _applyTxWidth(newWidth);
      };
      const _onUp = () => {
        if (!_dragStart) return;
        _dragStart = null;
        _splitter.classList.remove("dragging");
        document.body.style.cursor = "";
        const cur = parseInt(getComputedStyle(document.documentElement)
          .getPropertyValue("--watch-tx-width"), 10);
        if (Number.isFinite(cur) && cur > 0) _persistTxWidth(cur);
        window.removeEventListener("mousemove", _onMove);
        window.removeEventListener("mouseup", _onUp);
      };
      _splitter.addEventListener("mousedown", (e) => {
        e.preventDefault();
        const layout = _splitter.parentElement;
        if (!layout) return;
        const cur = parseFloat(getComputedStyle(document.documentElement)
          .getPropertyValue("--watch-tx-width")) || 420;
        _dragStart = { x: e.clientX, startWidth: cur, layout };
        _splitter.classList.add("dragging");
        document.body.style.cursor = "col-resize";
        window.addEventListener("mousemove", _onMove);
        window.addEventListener("mouseup", _onUp);
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
      const cur = window._watchCurrentVideo;
      const vid = cur && cur.video_id ? cur.video_id : "";
      if (btn) {
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
      }
      window._syncWatchRetranscribeBanner?.();
    };

    document.getElementById("btn-watch-retranscribe")?.addEventListener("click", async () => {
      let v = _watchActionVideo();
      if (!v?.filepath) {
        window._showToast?.("No file loaded.", "warn");
        return;
      }
      if (!_nativeBridgeUp()) {
        window._showToast?.("Native mode required.", "warn");
        return;
      }
      const vid = v.video_id || "";
      if (vid && window._inflightRetranscribes.has(vid)) {
        window._showToast?.(
          "Re-transcribe already queued for this video.", "warn");
        return;
      }
      const actionToken = window._watchOpenToken;
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
      if (Number.isFinite(actionToken)
          && Number.isFinite(window._watchOpenToken)
          && actionToken !== window._watchOpenToken) {
        window._showToast?.("Video changed before re-transcribe was queued.", "warn");
        return;
      }
      v = _watchActionVideo();
      if (!v?.filepath) return;
      // Mark inflight BEFORE the bridge call so a whisper_pct event
      // racing between the bridge return and the post-await success
      // branch finds the entry and updates it. Roll back on failure
      // (audit: watchActions.js C27).
      if (vid) {
        window._inflightRetranscribes.set(vid, 0);
        window._syncWatchRetranscribeButton();
      }
      let res;
      try {
        res = await _bridgeCall("transcribe_retranscribe",
          v.filepath, v.title || "", vid);
      } catch (e) {
        if (vid) window._inflightRetranscribes.delete(vid);
        window._syncWatchRetranscribeButton?.();
        window._showToast?.(`Re-transcribe call failed: ${e?.message || e}`, "error");
        return;
      }
      if (res?.ok) {
        window._showToast?.(
          `Queued ${model} re-transcription.`, "ok");
        if (vid) {
          // already in the map from the optimistic insert above
        } else {
          // Edge case: no video_id available — the progress display
          // can't track this job since it keys on vid. Surface the
          // queued state directly on the button so the user has
          // SOME visual feedback (audit: watchActions.js:418).
          const _btn = document.getElementById("btn-watch-retranscribe");
          if (_btn) {
            _btn.disabled = true;
            _btn.textContent = "Re-transcribing…";
            // Auto-clear after 30s as a last-resort fallback.
            setTimeout(() => {
              if (_btn.textContent === "Re-transcribing…") {
                _btn.disabled = false;
                _btn.textContent = "Re-transcribe…";
              }
            }, 30000);
          }
        }
      } else {
        if (vid) window._inflightRetranscribes.delete(vid);
        window._syncWatchRetranscribeButton?.();
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
      let v = _watchActionVideo();
      if (!v) {
        window._showToast?.("No video loaded.", "warn");
        return;
      }
      if (!_nativeBridgeUp()) {
        window._showToast?.("Native mode required.", "warn");
        return;
      }
      const actionToken = window._watchOpenToken;
      const kind = await (window._askBookmarkKind?.());
      if (!kind) return;
      if (Number.isFinite(actionToken)
          && Number.isFinite(window._watchOpenToken)
          && actionToken !== window._watchOpenToken) {
        window._showToast?.("Video changed before bookmark was saved.", "warn");
        return;
      }
      v = _watchActionVideo();
      if (!v) return;
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
      const res = await _bridgeCall("bookmark_add", {
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
    const findState = { matches: [], idx: -1, q: "", primed: false };

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
      findState.primed = false;
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
      if (findState.matches.length) {
        _findGoTo(0);
        // Mark the state as freshly-primed at idx 0 so the very next
        // Enter keeps focus on match 1 instead of jumping to match 2
        // (audit: watchActions.js:559). After one Enter the flag
        // clears and Enter resumes its normal advance behavior.
        findState.primed = true;
      }
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

    // Debounce so each keystroke doesn't full-scan the transcript
    // (audit: watchActions.js H157). 120ms keeps the find feel
    // responsive while collapsing rapid typing into one scan.
    let _findDebounce = null;
    watchFind?.addEventListener("input", () => {
      if (_findDebounce) clearTimeout(_findDebounce);
      _findDebounce = setTimeout(_rebuildFindMatches, 120);
    });
    watchFind?.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        if (!findState.matches.length) {
          _rebuildFindMatches();
          return;
        }
        if (findState.primed && !e.shiftKey) {
          // First Enter after a fresh rebuild: keep focus on match 1
          // instead of skipping straight to match 2.
          findState.primed = false;
          _findGoTo(findState.idx);
        } else {
          findState.primed = false;
          _findGoTo(findState.idx + (e.shiftKey ? -1 : 1));
        }
      } else if (e.key === "Escape") {
        watchFind.value = "";
        _rebuildFindMatches();
        watchFind.blur();
      }
    });
    watchFindNext?.addEventListener("click", () => {
      findState.primed = false;
      _findGoTo(findState.idx + 1);
    });
    watchFindPrev?.addEventListener("click", () => {
      findState.primed = false;
      _findGoTo(findState.idx - 1);
    });
  }

  window.initWatchActions = initWatchActions;
})();
