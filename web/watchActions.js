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
 *   window._retranscribeWatchMarkFinalizing
 *   window._retranscribeWatchClear
 *   window._onRetranscribeState
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

  function _isActuallyVisible(el) {
    if (!el || !el.isConnected) return false;
    for (let node = el; node && node.nodeType === 1; node = node.parentElement) {
      if (node.hidden || node.getAttribute?.("aria-hidden") === "true") return false;
      const st = window.getComputedStyle?.(node);
      if (st && (st.display === "none" || st.visibility === "hidden")) return false;
    }
    return true;
  }

  // Watch's own `hidden` flag is only a sub-view state.  The Browse panel is
  // switched with the `active` class, so Watch can be logically open while a
  // different top-level tab is covering it.  Publish one canonical check for
  // keyboard controls and the playback/karaoke modules to share.
  function _isWatchViewVisible() {
    const panel = document.getElementById("panel-browse");
    const view = document.getElementById("view-watch");
    return !!(panel?.classList?.contains("active")
      && view && !view.hidden && _isActuallyVisible(view));
  }
  window._isWatchViewVisible = _isWatchViewVisible;

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

    function _watchActionVideo(quiet = false) {
      const rendered = window._watchCurrentVideo || null;
      const pending = _browseState.currentVideo || null;
      const openToken = window._watchOpenToken;
      const renderedToken = window._watchRenderedToken;
      if (rendered && pending && !_sameWatchVideo(rendered, pending)) {
        if (!quiet) window._showToast?.("Video is still loading - try again in a moment.", "warn");
        return null;
      }
      if (rendered && Number.isFinite(openToken)
          && Number.isFinite(renderedToken)
          && renderedToken !== openToken) {
        if (!quiet) window._showToast?.("Video is still loading - try again in a moment.", "warn");
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
      if (!_isWatchViewVisible() || !vEl || e.defaultPrevented) return;
      // Buttons, selects, links, sliders, and modal controls own their keys.
      // In particular, Space on a focused button must click that button, not
      // start hidden media playback behind it.
      const interactive = e.target?.closest?.([
        "input", "textarea", "select", "button", "a[href]",
        "[contenteditable='true']", "[role='button']", "[role='textbox']",
        "[role='menuitem']", "[role='option']",
      ].join(","));
      if (interactive || e.ctrlKey || e.metaKey || e.altKey) return;
      const modalOpen = Array.from(document.querySelectorAll(".askq-backdrop"))
        .some((el) => window.YT?.modals?.isVisible?.(el));
      if (modalOpen) return;
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

    document.getElementById("btn-open-external")?.addEventListener("click", async () => {
      const v = _watchActionVideo();
      if (!v?.filepath) { window._showToast?.("No file loaded.", "warn"); return; }
      try {
        const result = await _bridgeCall("browse_open_video", v.filepath);
        if (!result?.ok) {
          window._showToast?.(
            result?.error || "Could not open the video externally.", "error");
        }
      } catch (error) {
        window._showToast?.(
          `Could not open the video externally: ${error?.message || error}`,
          "error");
      }
    });

    // Redownload current video — resolution picker, then video_redownload.
    document.getElementById("btn-watch-redownload")?.addEventListener("click", async () => {
      let v = _watchActionVideo();
      if (!v?.video_id) {
        window._showToast?.("This video does not have a YouTube ID.", "warn");
        return;
      }
      if (v.tracked === false) {
        window._showToast?.(
          "Redownload is available for current subscriptions.", "warn");
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
      const button = document.getElementById("btn-watch-redownload");
      if (button) {
        button.disabled = true;
        button.setAttribute("aria-busy", "true");
      }
      try {
        const result = await _bridgeCall(
          "video_redownload", v.video_id || "", v.title || "", pick, v.filepath);
        if (result?.ok) {
          window._showToast?.(`Redownload queued at ${pick}.`, "ok");
        } else {
          window._showToast?.(
            result?.error || "Could not queue redownload.", "error");
        }
      } catch (error) {
        window._showToast?.(
          `Could not queue redownload: ${error?.message || error}`, "error");
      } finally {
        if (button) {
          button.disabled = false;
          button.removeAttribute("aria-busy");
        }
      }
    });

    // Per-video metadata refresh: synchronous yt-dlp fetch for THIS video.
    document.getElementById("btn-watch-refresh-meta")?.addEventListener("click", async () => {
      const v = _watchActionVideo();
      if (!v?.video_id) {
        window._showToast?.("This video does not have a YouTube ID.", "warn");
        return;
      }
      const btn = document.getElementById("btn-watch-refresh-meta");
      if (!_nativeBridgeUp()) {
        window._showToast?.("Refresh isn't ready yet. Try again in a moment.", "warn");
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
        const method = v.tracked === false
          ? "manual_refresh_metadata" : "browse_refresh_video_metadata";
        const payload = {
          filepath: v.filepath || "",
          video_id: v.video_id || "",
          title: v.title || "",
          channel: v.channel || "",
        };
        if (v.tracked !== false) payload.mode = "all";
        const res = await _bridgeCall(method, payload);
        if (res?.ok) {
          window._showToast?.(
            res.warning || "Metadata refreshed.",
            res.warning ? "warn" : "ok",
          );
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

    // Size and background are restored. Mode starts in YT Style on each
    // launch and each Off -> on transition; explicit choices last while on.
    const _capSizeKey = "ytarchiver_caption_size";
    const _capBgKey   = "ytarchiver_caption_bg";
    const _capModeKey = "ytarchiver_caption_mode";
    const _CAP_SIZES = new Set(["off", "xsmall", "small", "medium", "large"]);
    const _CAP_BGS   = new Set(["translucent", "outline", "none"]);
    const _CAP_MODES = new Set(["single", "phrase3", "default"]);
    let _capPrefsEdited = false;
    function _applyCapSize(size, { persist = true } = {}) {
      const v = _CAP_SIZES.has(size) ? size : "off";
      const wasOn = window._captionPrefs?.size && window._captionPrefs.size !== "off";
      if (v !== "off" && !wasOn) _applyCapMode("default", { persist });
      window.setCaptionPref?.("size", v);
      const sel = document.getElementById("watch-cap-size");
      if (sel && sel.value !== v) sel.value = v;
      // Style + word-count only matter when the overlay is ON. Collapse
      // them when size=off so the default state isn't two mystery selects.
      const extras = document.getElementById("watch-overlay-extras");
      if (extras) extras.classList.toggle("collapsed", v === "off");
      try { localStorage.setItem(_capSizeKey, v); } catch {}
      if (persist && _nativeBridgeUp()) {
        try { _bridgeCall("settings_save", { caption_overlay_size: v }); } catch {}
      }
    }
    function _applyCapBg(bg, { persist = true } = {}) {
      const v = _CAP_BGS.has(bg) ? bg : "translucent";
      window.setCaptionPref?.("bg", v);
      const sel = document.getElementById("watch-cap-bg");
      if (sel && sel.value !== v) sel.value = v;
      try { localStorage.setItem(_capBgKey, v); } catch {}
      if (persist && _nativeBridgeUp()) {
        try { _bridgeCall("settings_save", { caption_overlay_bg: v }); } catch {}
      }
    }
    function _applyCapMode(mode, { persist = true } = {}) {
      const v = _CAP_MODES.has(mode) ? mode : "default";
      window.setCaptionPref?.("mode", v);
      const sel = document.getElementById("watch-cap-mode");
      if (sel && sel.value !== v) sel.value = v;
      try { localStorage.setItem(_capModeKey, v); } catch {}
      if (persist && _nativeBridgeUp()) {
        try { _bridgeCall("settings_save", { caption_overlay_mode: v }); } catch {}
      }
    }
    _applyCapMode("default", { persist: false });
    try {
      const _sz = localStorage.getItem(_capSizeKey);
      if (_sz && _CAP_SIZES.has(_sz)) _applyCapSize(_sz, { persist: false });
      const _bg = localStorage.getItem(_capBgKey);
      if (_bg && _CAP_BGS.has(_bg)) _applyCapBg(_bg, { persist: false });
    } catch {}
    (async () => {
      try {
        if (!_nativeBridgeUp()) return;
        const s = await _bridgeCall("settings_load");
        // Slow startup hydration must not undo newer dropdown choices.
        if (_capPrefsEdited) return;
        if (s?.caption_overlay_size && _CAP_SIZES.has(s.caption_overlay_size)) {
          _applyCapSize(s.caption_overlay_size, { persist: false });
        }
        if (s?.caption_overlay_bg && _CAP_BGS.has(s.caption_overlay_bg)) {
          _applyCapBg(s.caption_overlay_bg, { persist: false });
        }
      } catch {}
    })();
    document.getElementById("watch-cap-size")?.addEventListener("change", (ev) => {
      _capPrefsEdited = true;
      _applyCapSize(ev.target.value);
    });
    document.getElementById("watch-cap-bg")?.addEventListener("change", (ev) => {
      _capPrefsEdited = true;
      _applyCapBg(ev.target.value);
    });
    document.getElementById("watch-cap-mode")?.addEventListener("change", (ev) => {
      _capPrefsEdited = true;
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
      const video = _watchActionVideo();
      const items = [];
      if (video?.video_id && video.tracked !== false) {
        items.push({ label: "Redownload…", action: click("btn-watch-redownload") });
      }
      items.push({ label: "Re-transcribe…", action: click("btn-watch-retranscribe") });
      if (video?.video_id) {
        items.push({ label: "Refresh metadata", action: click("btn-watch-refresh-meta") });
      }
      showMenu(rect.left, rect.bottom + 4, items);
      window._markBrowseContextTrigger?.(e.currentTarget);
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
      const splitter = document.getElementById("watch-splitter");
      if (splitter) {
        splitter.setAttribute("aria-valuenow", String(v));
        splitter.setAttribute("aria-valuetext", `${v} pixels wide`);
      }
      try { localStorage.setItem(_txWidthKey, String(v)); } catch {}
      return v;
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
      _splitter.addEventListener("keydown", (e) => {
        const current = parseInt(getComputedStyle(document.documentElement)
          .getPropertyValue("--watch-tx-width"), 10) || 420;
        const step = e.shiftKey ? 60 : 20;
        let next = null;
        // The transcript sits to the right of the separator: moving the
        // separator left makes it wider, and moving it right makes it narrower.
        if (e.key === "ArrowLeft") next = current + step;
        else if (e.key === "ArrowRight") next = current - step;
        else if (e.key === "Home") next = _TX_WIDTH_MIN;
        else if (e.key === "End") next = _TX_WIDTH_MAX;
        if (next == null) return;
        e.preventDefault();
        const applied = _applyTxWidth(next);
        _persistTxWidth(applied);
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

    const _FINALIZING_STILL_MS = 120000;
    let _finalizingUiTimer = 0;

    function _clampRetranscribePct(value) {
      return Math.max(0, Math.min(99, parseInt(value, 10) || 0));
    }

    // Older in-memory entries were plain numbers. Normalize both shapes so
    // navigation and a hot frontend refresh remain safe while the richer
    // finalizing phase rolls out.
    function _normalizeRetranscribeState(raw, now = Date.now()) {
      if (raw && typeof raw === "object") {
        const pct = _clampRetranscribePct(raw.pct);
        const requestedPhase = String(raw.phase || "").trim().toLowerCase();
        const knownPhases = new Set([
          "queued",
          "transcribing",
          "finalizing",
          "paused",
          "resuming",
          "needs_attention",
        ]);
        const phase = knownPhases.has(requestedPhase)
          ? requestedPhase
          : pct > 0 ? "transcribing" : "queued";
        const startedAt = Number.isFinite(Number(raw.started_at))
          ? Number(raw.started_at) : now;
        const phaseStartedAt = Number.isFinite(Number(raw.phase_started_at))
          ? Number(raw.phase_started_at) : startedAt;
        return {
          pct,
          phase,
          started_at: startedAt,
          phase_started_at: phaseStartedAt,
          filepath: String(raw.filepath || ""),
          message: String(raw.message || ""),
        };
      }
      const pct = _clampRetranscribePct(raw);
      return {
        pct,
        phase: pct > 0 ? "transcribing" : "queued",
        started_at: now,
        phase_started_at: now,
        filepath: "",
        message: "",
      };
    }

    function _formatRetranscribeElapsed(startedAt, now = Date.now()) {
      const elapsedSeconds = Math.max(0, Math.floor(
        (now - Number(startedAt || now)) / 1000));
      if (elapsedSeconds < 60) return `${elapsedSeconds}s`;
      const minutes = Math.floor(elapsedSeconds / 60);
      const seconds = elapsedSeconds % 60;
      return `${minutes}m ${seconds}s`;
    }

    function _finalizingDisplay(state, now = Date.now()) {
      const elapsedMs = Math.max(0, now - Number(state.phase_started_at || now));
      const elapsed = _formatRetranscribeElapsed(state.phase_started_at, now);
      const prefix = elapsedMs >= _FINALIZING_STILL_MS
        ? "Still finishing…" : "Finishing transcript…";
      return {
        text: `${prefix} ${elapsed}`,
        title: "Whisper is finished. YTArchiver is preparing, saving, and "
          + `indexing the transcript (${elapsed}).`,
      };
    }

    window._normalizeRetranscribeWatchState = _normalizeRetranscribeState;
    window._retranscribeWatchFinalizingDisplay = _finalizingDisplay;

    function _buttonDisplayForRetranscribeState(state) {
      if (state.phase === "finalizing") {
        return _finalizingDisplay(state);
      }
      if (state.phase === "needs_attention") {
        return {
          text: "Needs attention — retry in Processing",
          title: "This re-transcription needs attention. Open Processing to retry it.",
        };
      }
      if (state.phase === "paused") {
        return {
          text: "Re-transcription paused",
          title: "Re-transcription is paused. Resume it from Processing.",
        };
      }
      if (state.phase === "resuming") {
        return {
          text: "Resuming transcription…",
          title: "Re-transcription is resuming.",
        };
      }
      if (state.phase === "queued") {
        return {
          text: "Re-transcription queued",
          title: "Re-transcription is waiting in the Processing queue.",
        };
      }
      return {
        text: state.pct > 0
          ? `Re-transcribing… ${state.pct}%`
          : "Re-transcribing…",
        title: state.pct > 0
          ? `Whisper is re-transcribing this video (${state.pct}%).`
          : "Whisper is re-transcribing this video.",
      };
    }

    function _hasFinalizingRetranscribe() {
      for (const raw of window._inflightRetranscribes.values()) {
        if (_normalizeRetranscribeState(raw).phase === "finalizing") return true;
      }
      return false;
    }

    function _stopFinalizingUiTimerIfIdle() {
      if (_finalizingUiTimer && !_hasFinalizingRetranscribe()) {
        clearInterval(_finalizingUiTimer);
        _finalizingUiTimer = 0;
      }
    }

    function _ensureFinalizingUiTimer() {
      if (_finalizingUiTimer || !_hasFinalizingRetranscribe()) return;
      // The bar itself animates continuously. Repaint once per second so the
      // elapsed label proves the UI is alive without inventing percent work.
      _finalizingUiTimer = setInterval(() => {
        if (!_hasFinalizingRetranscribe()) {
          _stopFinalizingUiTimerIfIdle();
          return;
        }
        window._syncWatchRetranscribeButton?.();
      }, 1000);
    }

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
          const state = _normalizeRetranscribeState(
            window._inflightRetranscribes.get(vid));
          const display = _buttonDisplayForRetranscribeState(state);
          btn.dataset.busy = "1";
          btn.disabled = true;
          btn.textContent = display.text;
          btn.title = display.title;
          if (state.phase === "finalizing") _ensureFinalizingUiTimer();
        } else {
          btn.dataset.busy = "";
          btn.disabled = false;
          btn.textContent = "Re-transcribe…";
          btn.title = "";
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
        window._showToast?.("YTArchiver isn't ready yet. Try again in a moment.", "warn");
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
        const now = Date.now();
        window._inflightRetranscribes.set(vid, {
          pct: 0,
          phase: "queued",
          started_at: now,
          phase_started_at: now,
          filepath: v.filepath,
          message: "",
        });
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
      const p = _clampRetranscribePct(pct);
      if (window._inflightRetranscribes.has(video_id)) {
        const now = Date.now();
        const previous = _normalizeRetranscribeState(
          window._inflightRetranscribes.get(video_id), now);
        window._inflightRetranscribes.set(video_id, {
          pct: p,
          phase: "transcribing",
          started_at: previous.started_at,
          phase_started_at: previous.phase === "transcribing"
            ? previous.phase_started_at : now,
          filepath: previous.filepath,
          message: "",
        });
      }
      const cur = window._watchCurrentVideo;
      if (cur && cur.video_id === video_id) {
        window._syncWatchRetranscribeButton();
      }
    };

    // Called from logs.js when the backend reports that Whisper recognition
    // has completed and the transcript is being prepared for durable storage.
    window._retranscribeWatchMarkFinalizing = function (video_id) {
      if (!video_id || !window._inflightRetranscribes.has(video_id)) return;
      const now = Date.now();
      const previous = _normalizeRetranscribeState(
        window._inflightRetranscribes.get(video_id), now);
      window._inflightRetranscribes.set(video_id, {
        pct: previous.pct,
        phase: "finalizing",
        started_at: previous.started_at,
        phase_started_at: previous.phase === "finalizing"
          ? previous.phase_started_at : now,
        filepath: previous.filepath,
        message: "",
      });
      _ensureFinalizingUiTimer();
      const cur = window._watchCurrentVideo;
      if (cur && cur.video_id === video_id) {
        window._syncWatchRetranscribeButton();
      }
    };

    // Called from _onRetranscribeComplete with the finished video_id.
    window._retranscribeWatchClear = function (video_id) {
      // A job without a YouTube ID was never inserted into this ID-keyed map.
      // Treat its completion as a no-op here; clearing the whole map would
      // erase truthful progress for every unrelated retranscription.
      if (!video_id) return;
      window._inflightRetranscribes.delete(video_id);
      _stopFinalizingUiTimerIfIdle();
      window._syncWatchRetranscribeButton();
    };

    function _sameWatchFile(left, right) {
      const normalize = (value) => String(value || "")
        .replace(/\\/g, "/").replace(/\/+$/, "").toLowerCase();
      const a = normalize(left);
      const b = normalize(right);
      return !!(a && b && a === b);
    }

    // Called by Python for job states that are not percentage updates. Keeping
    // these separate prevents a paused or failed job from looking like it is
    // still actively working, and lets cancellation clear only its own video.
    window._onRetranscribeState = function (payload) {
      if (!payload || typeof payload !== "object") return;
      const phase = String(payload.state || payload.phase || "")
        .trim().toLowerCase();
      const filepath = String(payload.filepath || "");
      let videoId = String(payload.video_id || "");

      if (!videoId && filepath) {
        const current = window._watchCurrentVideo;
        if (current?.video_id && _sameWatchFile(filepath, current.filepath)) {
          videoId = String(current.video_id);
        } else {
          for (const [candidateId, raw] of window._inflightRetranscribes) {
            const state = _normalizeRetranscribeState(raw);
            if (_sameWatchFile(filepath, state.filepath)) {
              videoId = String(candidateId);
              break;
            }
          }
        }
      }
      if (!videoId) return;

      if (phase === "cancelled" || phase === "rejected") {
        window._retranscribeWatchClear(videoId);
        return;
      }
      if (!["paused", "resuming", "queued", "finalizing",
            "needs_attention"].includes(phase)) {
        return;
      }

      const now = Date.now();
      const previous = window._inflightRetranscribes.has(videoId)
        ? _normalizeRetranscribeState(
            window._inflightRetranscribes.get(videoId), now)
        : _normalizeRetranscribeState(0, now);
      window._inflightRetranscribes.set(videoId, {
        pct: previous.pct,
        phase,
        started_at: previous.started_at,
        phase_started_at: previous.phase === phase
          ? previous.phase_started_at : now,
        filepath: filepath || previous.filepath,
        message: String(payload.message || ""),
      });
      _stopFinalizingUiTimerIfIdle();
      if (phase === "finalizing") _ensureFinalizingUiTimer();
      window._syncWatchRetranscribeButton();
    };

    document.getElementById("btn-bookmark-now")?.addEventListener("click", async () => {
      const _vEl = document.getElementById("watch-video");
      let v = _watchActionVideo(true);
      if (!v) {
        window._showToast?.(_browseState.currentVideo
          ? "Video is still loading - try again in a moment." : "No video loaded.", "warn");
        return;
      }
      if (!v.video_id) {
        window._showToast?.("This video does not have a YouTube ID.", "warn");
        return;
      }
      if (!_nativeBridgeUp()) {
        window._showToast?.("YTArchiver isn't ready yet. Try again in a moment.", "warn");
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
      const res = await window._saveBookmark({
        video_id: v.video_id || "",
        title: v.title || "",
        channel: v.channel || "",
        start_time: t,
        text: text.slice(0, 200),
        note: "",
      });
      if (res?.pending) return;
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
