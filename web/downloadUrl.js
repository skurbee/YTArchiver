/**
 * web/downloadUrl.js — single-video URL input + Download button (Download tab)
 *
 * Exposed as window.initUrlField; app.js boot calls it once.
 */
(function () {
  "use strict";

  const _browseState = window._browseState || {};
  const showContextMenu = window.showContextMenu || (() => {});
  const askConfirm = window.askConfirm;
  const askDanger = window.askDanger;
  const askQuestion = window.askQuestion;
  function bridgeCall(method, ...args) {
    const fn = window.YT?.bridge?.bridgeCall;
    if (fn) return fn(method, ...args);
    return undefined;
  }
  function nativeBridgeUp() {
    return !!window.YT?.bridge?.isUp?.();
  }

  // One result contract shared by typed and dropped single-video URLs.
  // A resolved bridge promise is not necessarily success: backend validation
  // failures deliberately resolve as {ok:false, error:"..."}.
  async function queueSingleVideo(url, options) {
    if (!nativeBridgeUp()) {
      return { ok: false, error: "YTArchiver isn't ready yet. Try again in a moment." };
    }
    try {
      const result = await bridgeCall("archive_single_video", url, options || {});
      if (result?.ok) return result;
      return {
        ok: false,
        error: result?.error || "The download could not be queued.",
      };
    } catch (e) {
      return { ok: false, error: e?.message || String(e) };
    }
  }
  window._queueSingleVideo = queueSingleVideo;

  // ─── URL field + Download button ────────────────────────────────────
  //
  // Behavior matches YTArchiver.py:19706-19708 + _validate_download_btn:
  // - Field is empty → "▶ Download" hidden, Sync Subbed is the main action
  // - YouTube URL typed → "▶ Download" appears next to the URL field
  // - Click Download OR press Enter → calls archive_single_video + clears input
  // - Escape clears the field
  //
  // The old "Paste & archive" button is gone — pasting a URL just shows the
  // Download button, which is more discoverable and matches the original.
  // Parse the URL before inspecting its path. A regex over the full string
  // can mistake a foreign URL path or username (for example,
  // evil.example/youtube.com/... or youtube.com@evil.example) for the host.
  // YouTube uses several first-party subdomains, so accept youtube.com and
  // its subdomains, plus the youtu.be short-link host.
  function parseYouTubeUrl(value) {
    const raw = String(value || "").trim();
    if (!raw) return null;
    let candidate = raw;
    if (candidate.startsWith("//")) candidate = `https:${candidate}`;
    else if (!/^[a-z][a-z\d+.-]*:/i.test(candidate)) {
      candidate = `https://${candidate}`;
    }
    let parsed;
    try { parsed = new URL(candidate); } catch { return null; }
    if (!['http:', 'https:'].includes(parsed.protocol)) return null;
    // User-info is unnecessary for YouTube links and makes deceptive URLs
    // unnecessarily hard to read, even when the final hostname is YouTube.
    if (parsed.username || parsed.password) return null;
    if (parsed.port
        && !((parsed.protocol === "https:" && parsed.port === "443")
             || (parsed.protocol === "http:" && parsed.port === "80"))) {
      return null;
    }
    const host = parsed.hostname.toLowerCase().replace(/\.$/, "");
    if (host !== "youtu.be"
        && host !== "youtube.com"
        && !host.endsWith(".youtube.com")) {
      return null;
    }
    return parsed;
  }

  function urlLooksLikeVideo(value) {
    const parsed = parseYouTubeUrl(value);
    if (!parsed) return false;
    const host = parsed.hostname.toLowerCase().replace(/\.$/, "");
    const path = parsed.pathname;
    if (host === "youtu.be") {
      return /^\/[A-Za-z0-9_-]{6,}\/?$/.test(path);
    }
    if (/^\/watch\/?$/i.test(path)) {
      return /^[A-Za-z0-9_-]{6,}$/.test(parsed.searchParams.get("v") || "");
    }
    return /^\/(?:shorts|embed|live|clip)\/[A-Za-z0-9_-]{6,}(?:\/|$)/i
      .test(path);
  }

  function urlLooksLikeChannel(value) {
    const parsed = parseYouTubeUrl(value);
    if (!parsed) return false;
    const host = parsed.hostname.toLowerCase().replace(/\.$/, "");
    if (host === "youtu.be") return false;
    if (/^\/(?:@[^/]+|c\/[^/]+|channel\/[^/]+|user\/[^/]+)(?:\/|$)/i
        .test(parsed.pathname)) {
      return true;
    }
    return /^\/playlist\/?$/i.test(parsed.pathname)
      && !!parsed.searchParams.get("list");
  }

  window._parseYouTubeUrl = parseYouTubeUrl;
  window._urlLooksLikeYouTube = (value) => !!parseYouTubeUrl(value);
  window._urlLooksLikeVideo = urlLooksLikeVideo;

  function initUrlField() {
    const input = document.getElementById("url-input");
    const btn = document.getElementById("btn-download-single");
    const errRow = document.getElementById("url-error-row");
    const errText = document.getElementById("url-error-text");
    const voPanel = document.getElementById("video-opts-panel");
    const nudgePanel = document.getElementById("channel-nudge-panel");
    if (!input || !btn) return;
    // Hydrate bridge-backed defaults both now and when pywebview becomes
    // ready. The Download tab is initialized before bridge injection on a
    // normal launch, so a one-shot early return left the defaults blank.
    let _hydrateInFlight = false;
    const hydrateBridgePreferences = async () => {
      if (_hydrateInFlight || !nativeBridgeUp()) return;
      _hydrateInFlight = true;
      try {
        const s = await bridgeCall("settings_load");
        const def = (s?.video_out_dir || s?.output_dir || "").trim();
        const saveInput = document.getElementById("vo-save-to");
        if (saveInput && def) saveInput.placeholder = def;
        // Default the Resolution dropdown to Settings → General "Default
        // resolution" (e.g. 720p) on a FRESH field. It was hardcoded to
        // 1080p in markup and only ever overridden by a last-used value, so
        // it ignored the user's configured default. A saved last-used choice
        // still wins (this runs after the localStorage restore below).
        const _selRes = document.getElementById("vo-resolution");
        let _savedRes = null;
        try { _savedRes = localStorage.getItem("ytarch.vo.resolution"); } catch {}
        const _defRes = s?.default_resolution != null ? String(s.default_resolution) : "";
        if (_selRes && !_savedRes && _defRes &&
            [..._selRes.options].some(o => o.value === _defRes)) {
          _selRes.value = _defRes;
          _selRes._ytddRepaint?.();
        }
      } catch { /* non-fatal */ }
      finally { _hydrateInFlight = false; }
    };
    hydrateBridgePreferences();
    window.YT?.bridge?.ready?.then(hydrateBridgePreferences).catch(() => {});
    window.addEventListener(
      "pywebviewready", hydrateBridgePreferences, { once: true });

    // Show persistent error below URL field when input doesn't look like a
    // recognized YouTube URL. Matches YTArchiver.py:17060 url_error_var.
    const setErr = (msg) => {
      if (!errRow || !errText) return;
      if (msg) { errText.textContent = msg; errRow.hidden = false; }
      else { errText.textContent = ""; errRow.hidden = true; }
    };
    const refreshErr = () => {
      const t = (input.value || "").trim();
      if (!t) { setErr(""); return; }
      if (urlLooksLikeVideo(t) || urlLooksLikeChannel(t)) { setErr(""); return; }
      setErr("Invalid URL (must be a YouTube video, channel, or playlist).");
    };

    // Panel visibility — matches YTArchiver.py:17008 process_url_update flow.
    // Show video-options when URL is a video; show channel-nudge when it's
    // a channel URL we don't already have in subs.
    const refreshPanels = () => {
      const t = (input.value || "").trim();
      const isVid = urlLooksLikeVideo(t);
      const isChan = !isVid && urlLooksLikeChannel(t);
      if (voPanel) voPanel.hidden = !isVid;
      if (nudgePanel) nudgePanel.hidden = !isChan;
    };

    const updateBtnVisibility = () => {
      const show = urlLooksLikeVideo(input.value);
      btn.hidden = !show;
      refreshErr();
      refreshPanels();
    };

    // persist the Download-tab preferences so they survive
    // tab switches and restarts. Previously every new session reset
    // resolution to "1080p" + "Use YT title" to default-checked,
    // stomping the user's last-chosen settings.
    const _VO_KEYS = {
      resolution: "ytarch.vo.resolution",
      date_file: "ytarch.vo.date_file",
      add_date: "ytarch.vo.add_date",
      use_yt_title: "ytarch.vo.use_yt_title",
      grab_metadata: "ytarch.vo.grab_metadata",
    };
    try {
      const _load = (k, fallback) => {
        const v = localStorage.getItem(k);
        return v == null ? fallback : v;
      };
      const _saved_res = _load(_VO_KEYS.resolution, "");
      const _sel = document.getElementById("vo-resolution");
      if (_sel && _saved_res && [..._sel.options].some(o => o.value === _saved_res)) {
        _sel.value = _saved_res;
      }
      const _df = document.getElementById("vo-date-file");
      const _v_df = localStorage.getItem(_VO_KEYS.date_file);
      if (_df && _v_df != null) _df.checked = _v_df === "1";
      const _ad = document.getElementById("vo-add-date");
      const _v_ad = localStorage.getItem(_VO_KEYS.add_date);
      if (_ad && _v_ad != null) _ad.checked = _v_ad === "1";
      const _ut = document.getElementById("vo-use-yt-title");
      const _v_ut = localStorage.getItem(_VO_KEYS.use_yt_title);
      if (_ut && _v_ut != null) _ut.checked = _v_ut === "1";
      const _gm = document.getElementById("vo-grab-metadata");
      const _v_gm = localStorage.getItem(_VO_KEYS.grab_metadata);
      if (_gm && _v_gm != null) _gm.checked = _v_gm === "1";
    } catch {}
    // Persist on every change.
    const _persistVoField = (id, key, kind) => {
      const el = document.getElementById(id);
      if (!el) return;
      el.addEventListener("change", () => {
        try {
          if (kind === "bool") {
            localStorage.setItem(key, el.checked ? "1" : "0");
          } else {
            localStorage.setItem(key, el.value || "");
          }
        } catch {}
      });
    };
    _persistVoField("vo-resolution", _VO_KEYS.resolution, "value");
    _persistVoField("vo-date-file", _VO_KEYS.date_file, "bool");
    _persistVoField("vo-add-date", _VO_KEYS.add_date, "bool");
    _persistVoField("vo-use-yt-title", _VO_KEYS.use_yt_title, "bool");
    _persistVoField("vo-grab-metadata", _VO_KEYS.grab_metadata, "bool");

    // Read the Video-options panel into a plain dict to send to the backend.
    const readVideoOptions = () => {
      const saveTo = document.getElementById("vo-save-to")?.value?.trim() || "";
      const res = document.getElementById("vo-resolution")?.value || "1080";
      const dateFile = !!document.getElementById("vo-date-file")?.checked;
      const addDate = !!document.getElementById("vo-add-date")?.checked;
      const useYtTitle = !!document.getElementById("vo-use-yt-title")?.checked;
      const customName = document.getElementById("vo-custom-name")?.value?.trim() || "";
      const grabMeta = !!document.getElementById("vo-grab-metadata")?.checked;
      return {
        save_to: saveTo,
        resolution: res,
        date_file: dateFile,
        add_date: addDate,
        use_yt_title: useYtTitle,
        custom_name: customName,
        grab_metadata: grabMeta,
      };
    };
    // Expose on window so cross-IIFE callers (e.g. downloadDragDrop.js
    // when a URL is dropped onto the window) can read the same panel
    // values. Previously downloadDragDrop.js used a `typeof readVideoOptions`
    // bareword check that silently fell through to `{}` because the
    // const above was scoped to this IIFE — dropped URLs always used
    // backend defaults, ignoring everything the user filled in (audit:
    // downloadDragDrop.js C35).
    window._readVideoOptions = readVideoOptions;

    let submitInFlight = false;
    const submit = async () => {
      const url = (input.value || "").trim();
      if (!urlLooksLikeVideo(url)) {
        window._showToast?.("Paste a YouTube video URL first.", "warn");
        return;
      }
      if (!nativeBridgeUp()) {
        window._showToast?.("YTArchiver isn't ready yet. Try again in a moment.", "warn");
        return;
      }
      if (submitInFlight) return;
      submitInFlight = true;
      btn.disabled = true;
      try {
        // Already-archived warning. Checked against the live index by video id
        // (not a separate list), so it reflects what's actually archived now.
        // Any failure falls through and allows the download. User can override.
        try {
          if (askConfirm) {
            const chk = await bridgeCall("single_video_archived", url);
            if (chk?.ok && chk.archived) {
              const what = chk.title ? `"${chk.title}"` : "This video";
              const where = chk.channel ? ` in "${chk.channel}"` : "";
              const go = await askConfirm(
                "Already archived",
                `${what} is already archived${where}.\n\n` +
                `Download it again anyway?`,
                { confirm: "Download anyway" });
              if (!go) return;
            }
          }
        } catch { /* non-fatal — allow the download */ }

        const result = await queueSingleVideo(url, readVideoOptions());
        if (!result.ok) {
          const msg = result.error || "The download could not be queued.";
          // Keep the URL available for correction/retry.  Do not overwrite an
          // even newer URL the user typed while this bridge call was pending.
          if ((input.value || "").trim() === url) setErr(msg);
          window._showToast?.(msg, "error");
          return;
        }
        window._showToast?.("Queued: " + url.slice(0, 60), "ok");
        if ((input.value || "").trim() === url) {
          input.value = "";
          updateBtnVisibility();
        }
      } finally {
        submitInFlight = false;
        btn.disabled = false;
      }
    };

    input.addEventListener("input", updateBtnVisibility);
    input.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        if (urlLooksLikeVideo(input.value)) submit();
        else if (!input.value.trim()) document.getElementById("btn-sync-subbed")?.click();
      } else if (e.key === "Escape") {
        input.value = "";
        updateBtnVisibility();
      }
    });
    // paste fires before `input` in some engines — delay the sync so the
    // pasted text is actually reflected in input.value
    input.addEventListener("paste", () => setTimeout(updateBtnVisibility, 10));
    btn.addEventListener("click", submit);

    // Video options: Use-YT-title ↔ custom-name enable/disable.
    // Mirrors YTArchiver.py:4436 _toggle_custom_name.
    const useYtTitleCB = document.getElementById("vo-use-yt-title");
    const customNameInput = document.getElementById("vo-custom-name");
    const syncCustomName = () => {
      if (!customNameInput || !useYtTitleCB) return;
      customNameInput.disabled = useYtTitleCB.checked;
    };
    useYtTitleCB?.addEventListener("change", syncCustomName);
    syncCustomName();

    // Save-to folder Browse button → pywebview native folder picker
    document.getElementById("vo-save-to-browse")?.addEventListener("click", async () => {
      if (!nativeBridgeUp()) {
        window._showToast?.("YTArchiver isn't ready yet. Try again in a moment.", "warn");
        return;
      }
      const saveInput = document.getElementById("vo-save-to");
      const current = saveInput?.value || "";
      try {
        const res = await bridgeCall("pick_folder", "Save video to…", current);
        if (res?.ok && res.path) {
          // Custom locations are registered in the catalog when the download
          // completes, so they remain available in Browse and Search.
          if (saveInput) saveInput.value = res.path;
        } else if (!res?.cancelled) {
          window._showToast?.(
            res?.error || "Could not choose a destination folder.", "error");
        }
      } catch (error) {
        window._showToast?.(
          `Could not choose a destination folder: ${error}`, "error");
      }
    });

    // Channel-nudge button: open the shared Add Channel editor and pre-fill
    // the URL, regardless of whether Browse-first or legacy Subs mode is on.
    const goToAddChannel = () => {
      const url = (input.value || "").trim();
      if (!url) return;
      window._openAddChannelEditor?.(url);
      // Clear the Download-tab URL so the nudge hides
      input.value = "";
      updateBtnVisibility();
    };
    // The whole nudge box is clickable (it looked actionable but only the
    // button worked); the button stops propagation so the box handler
    // doesn't double-fire.
    document.getElementById("channel-nudge-panel")?.addEventListener("click", goToAddChannel);
    document.getElementById("btn-channel-nudge-add")?.addEventListener("click", (e) => {
      e.stopPropagation();
      goToAddChannel();
    });

    // Initial sync in case there's a value restored from somewhere
    updateBtnVisibility();
  }

  window.initUrlField = initUrlField;
})();
