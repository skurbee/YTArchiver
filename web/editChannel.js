/**
 * web/editChannel.js — shared Browse dialog / legacy Subs channel editor.
 *
 * Loading a row populates the form fields; Update/Remove/Cancel dispatch
 * to the bridge. Handles add and edit modes, three-state Update button
 * (disabled until changes pending), folder-name input that mirrors into
 * both `folder` and `folder_override` on save, From-date YYYY/MM/DD
 * with auto-advance, conditional Compress sub-fields, and the
 * "Continue redownload" / "Recheck resolution" affordances.
 *
 * Exposed as window.initEditChannelPanel; app.js boot calls it once.
 * Also publishes:
 *   - window.refreshSubsTable — re-fetches the Subs table data
 *   - window._editChannelFromContext(folder, urlGuess) — entry point
 *     used by the Subs context menu / double-click handlers.
 *
 * Depends on:
 *   - window.askConfirm, window.askQuestion, window.askDanger (modals.js)
 *   - window._showToast (toasts.js)
 *   - window._removeChannelWithPrompt (app.js)
 *   - window.renderSubsTable, window._primeBrowse (app.js)
 *   - window._subsAllRows (app.js, optional)
 *   - window.YT.bridge.bridgeCall / isUp (bridge.js)
 *   - window.pywebview.api.* (raw bridge — update handler dedup check only)
 */
(function () {
  "use strict";

  // Browse-first channel management is the default. Settings can flip this
  // at runtime after its persisted config arrives.
  if (typeof window._legacySubsTabEnabled !== "boolean") {
    window._legacySubsTabEnabled = false;
  }
  function bridgeCall(method, ...args) {
    const fn = window.YT?.bridge?.bridgeCall;
    if (fn) return fn(method, ...args);
    return undefined;
  }

  function nativeBridgeUp() {
    return !!window.YT?.bridge?.isUp?.();
  }

  function flashError(msg) {
    console.warn("[subs]", msg);
    if (window._showToast) {
      window._showToast(msg, "error");
    } else {
      alert(msg);
    }
  }
  function flashOk(msg) {
    console.info("[subs]", msg);
    window._showToast?.(msg, "ok");
  }

  function initEditChannelPanel() {
    const box = document.getElementById("edit-channel-box");
    const label = document.getElementById("edit-channel-label");
    const update = document.getElementById("btn-edit-update");
    const remove = document.getElementById("btn-edit-remove");
    const removeLabel = remove?.querySelector("span");
    const cancel = document.getElementById("btn-edit-cancel");
    if (!box) return;

    const legacyHome = document.getElementById("legacy-channel-editor-home");
    const editorBackdrop = document.getElementById("channel-editor-backdrop");
    const editorMount = document.getElementById("channel-editor-mount");
    const editorTitle = document.getElementById("channel-editor-title");
    const editorSubtitle = document.getElementById("channel-editor-subtitle");

    const hideModernEditor = () => {
      if (editorBackdrop) editorBackdrop.hidden = true;
      document.body.classList.remove("channel-editor-open");
    };

    const syncModernHeading = (mode, channel) => {
      const isEdit = mode === "edit" && channel;
      const name = channel?.folder || channel?.name || "";
      if (editorTitle) {
        editorTitle.textContent = isEdit && name ? `Edit ${name}` : "Add channel";
      }
      if (editorSubtitle) {
        editorSubtitle.textContent = isEdit
          ? "Update how this channel is archived. Existing files stay in place unless you choose a reorganization action."
          : "Choose what to archive now and how future downloads should be handled.";
      }
    };

    const presentEditorShell = (mode, channel, open = true, options = {}) => {
      // Dense mode controls edits launched from the Subs list. Browse owns
      // the modern dialog regardless of that preference, so enabling the
      // dense tab never pulls a Browse user out of their current context.
      const useLegacy = !!window._legacySubsTabEnabled
        && !options.forceModern;
      if (useLegacy) {
        if (legacyHome && box.parentElement !== legacyHome) legacyHome.appendChild(box);
        hideModernEditor();
        if (open) document.querySelector('.tab[data-tab="subs"]')?.click();
        return;
      }
      if (editorMount && box.parentElement !== editorMount) editorMount.appendChild(box);
      syncModernHeading(mode, channel);
      if (!open || !editorBackdrop) return;
      editorBackdrop.hidden = false;
      document.body.classList.add("channel-editor-open");
    };

    const applyLegacySubsMode = (enabled) => {
      const useLegacy = !!enabled;
      window._legacySubsTabEnabled = useLegacy;
      const tab = document.querySelector('.tab[data-tab="subs"]');
      const panel = document.getElementById("panel-subs");
      if (tab) tab.hidden = !useLegacy;
      document.getElementById("settings-subs-table-label")
        ?.toggleAttribute("hidden", !useLegacy);
      document.getElementById("settings-subs-table-options")
        ?.toggleAttribute("hidden", !useLegacy);
      if (!useLegacy && (tab?.classList.contains("active") || panel?.classList.contains("active"))) {
        document.querySelector('.tab[data-tab="browse"]')?.click();
      }
      presentEditorShell("add", null, false);
      if (useLegacy) hideModernEditor();
      window.dispatchEvent(new CustomEvent("legacy-subs-mode-changed", {
        detail: { enabled: useLegacy },
      }));
    };
    window._applyLegacySubsMode = applyLegacySubsMode;
    applyLegacySubsMode(window._legacySubsTabEnabled);

    window._setDenseSubsPreference = async (
        enabled, { openWhenEnabled = false } = {}) => {
      const useDense = !!enabled;
      if (!nativeBridgeUp()) {
        window._showToast?.("This setting isn't ready yet. Try again in a moment.", "warn");
        return false;
      }
      try {
        const result = await bridgeCall("settings_save", {
          legacy_subs_tab: useDense,
        });
        if (!result?.ok) {
          window._showToast?.(
            result?.error || "Could not save Dense Sub Tab setting.", "error");
          return false;
        }
      } catch (e) {
        window._showToast?.("Could not save Dense Sub Tab setting: " + e, "error");
        return false;
      }
      applyLegacySubsMode(useDense);
      const settingsToggle = document.getElementById(
        "settings-legacy-subs-tab");
      if (settingsToggle) settingsToggle.checked = useDense;
      if (useDense && openWhenEnabled) {
        document.querySelector('.tab[data-tab="subs"]')?.click();
        window.refreshSubsTable?.();
      }
      return true;
    };
    window._denseSubsContextMenuItem = () => ({
      label: "View Dense Subs List",
      checked: !!window._legacySubsTabEnabled,
      title: "Show or hide the Dense Subs tab and remember the choice",
      action: () => window._setDenseSubsPreference?.(
        !window._legacySubsTabEnabled, { openWhenEnabled: true }),
    });

    const mainSubsTab = document.querySelector('.tab[data-tab="subs"]');
    mainSubsTab?.addEventListener("contextmenu", (e) => {
      e.preventDefault();
      const item = window._denseSubsContextMenuItem?.();
      if (item) window.showContextMenu?.(e.clientX, e.clientY, [item]);
    });

    const resetFields = () => {
      // Clear every text/number field — INCLUDING the YYYY/MM/DD date
      // parts, which were previously left untouched. That omission let a
      // from-date (and other conditionally-populated fields) from a
      // previously-edited channel bleed into the next channel's panel.
      ["edit-folder","edit-url","edit-min-dur","edit-max-dur",
       "edit-date-year","edit-date-month","edit-date-day"].forEach(id => {
        const el = document.getElementById(id); if (el) el.value = "";
      });
      ["edit-compress"].forEach(id => {
        const el = document.getElementById(id); if (el) el.checked = false;
      });
      const _meta = document.getElementById("edit-metadata");
      if (_meta) _meta.checked = true;
      const _tx = document.getElementById("edit-transcribe");
      if (_tx) _tx.checked = false;
      const _res = document.getElementById("edit-resolution");
      if (_res) _res.value = "720";
      const _org = document.getElementById("edit-folder-org");
      if (_org) _org.value = "years";
      // Compress sub-fields back to defaults so the prior channel's
      // level / output-res / batch size don't linger.
      const _cq = document.getElementById("edit-compress-quality");
      if (_cq) _cq.value = _cq.querySelector("option[selected]")?.value
                         || _cq.options[0]?.value || "Generous";
      const _cr = document.getElementById("edit-compress-res");
      if (_cr) _cr.value = "720";
      const subs = document.querySelector(
        'input[name="edit-range"][value="subscribe"]');
      if (subs) { subs.checked = true; subs.dispatchEvent(new Event("change")); }
      document.getElementById("edit-compress")?.dispatchEvent(new Event("change"));
    };

    // Panel is always visible. `collapsed` class hides everything past the
    // Folder Name + Channel URL row until the user types or we switch to
    // Edit mode. Start collapsed.
    box.classList.add("collapsed");

    const _updateCollapsed = () => {
      const nameVal = (document.getElementById("edit-folder")?.value || "").trim();
      const urlVal = (document.getElementById("edit-url")?.value || "").trim();
      const editing = !!_editingIdentity;
      const shouldShow = !window._legacySubsTabEnabled
        || Boolean(nameVal || urlVal || editing);
      box.classList.toggle("collapsed", !shouldShow);
    };
    document.getElementById("edit-folder")?.addEventListener("input", _updateCollapsed);
    document.getElementById("edit-url")?.addEventListener("input", _updateCollapsed);

    // Reverse URL-type nudge: if the user pastes a video URL into the
    // channel URL field, show a handoff button to the Download tab.
    const _editUrlVideoNudge = document.getElementById("edit-url-video-nudge");
    const _editUrlField = document.getElementById("edit-url");
    const _updateEditUrlNudge = () => {
      if (!_editUrlVideoNudge || !_editUrlField) return;
      const t = (_editUrlField.value || "").trim();
      const isVideo = /^(?:https?:\/\/)?(?:(?:www|m)\.)?(?:youtube\.com\/(?:watch\?v=|shorts\/|live\/)|youtu\.be\/)[\w-]+/i.test(t);
      _editUrlVideoNudge.hidden = !isVideo;
    };
    _editUrlField?.addEventListener("input", _updateEditUrlNudge);
    _editUrlField?.addEventListener("paste", () => setTimeout(_updateEditUrlNudge, 10));
    _updateEditUrlNudge();

    // UX: in ADD mode, auto-fill the Folder Name from the channel URL's
    // @handle (or /c/Name, /user/Name) so the user doesn't have to retype
    // it. Only fills when the folder field is empty and we're adding a new
    // channel — never clobbers the user's own text or an existing
    // channel's name while editing.
    // Keep the editor's immediate structural validation aligned with
    // backend.subs.normalize_channel_url + validate_channel_url. The backend
    // remains authoritative when Save is clicked; this local mirror exists so
    // a disabled Add button can explain itself without a bridge round-trip on
    // every keystroke.
    const _normalizeChannelUrlForValidation = (raw) => {
      let value = String(raw || "").trim();
      if (!value) return "";
      if (value.startsWith("@")) {
        return `https://www.youtube.com/${value}`;
      }
      if (value.startsWith("/@")) {
        return `https://www.youtube.com${value}`;
      }
      if (!value.startsWith("http://") && !value.startsWith("https://")) {
        if (value.startsWith("youtube.com") || value.startsWith("www.youtube.com")) {
          return "https://" + value.replace(/^\/+/, "");
        }
        if (value.startsWith("/")) {
          return "https://www.youtube.com" + value;
        }
        if (/^[A-Za-z0-9_-]{2,30}$/.test(value)) {
          return `https://www.youtube.com/@${value}`;
        }
      }
      return value;
    };

    const _channelUrlValidationError = (raw) => {
      const value = String(raw || "").trim();
      if (!value) return "Enter a YouTube channel URL or @handle.";
      let parsed;
      try {
        parsed = new URL(_normalizeChannelUrlForValidation(value));
      } catch {
        return "Enter a YouTube channel URL or @handle.";
      }
      if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
        return "Enter a YouTube channel URL or @handle.";
      }
      const host = parsed.hostname.toLowerCase();
      if (!["youtube.com", "www.youtube.com", "m.youtube.com"].includes(host)) {
        return "Use a youtube.com channel link, not another site.";
      }
      const path = parsed.pathname.replace(/^\/+|\/+$/g, "");
      const isChannelPath = path.startsWith("@")
        || path.startsWith("channel/UC")
        || path.startsWith("c/")
        || path.startsWith("user/");
      if (!path || !isChannelPath) {
        return "Use a channel link (/@handle, /channel/UC…, /c/…, or /user/…), not a video or playlist URL.";
      }
      return "";
    };

    const _deriveFolderFromUrl = (url) => {
      if (_channelUrlValidationError(url)) return "";
      try {
        const parsed = new URL(_normalizeChannelUrlForValidation(url));
        const path = parsed.pathname.replace(/^\/+|\/+$/g, "");
        let match = path.match(/^@([^/?#\s]+)/);
        if (match) return decodeURIComponent(match[1]);
        match = path.match(/^(?:c|user)\/([^/?#\s]+)/i);
        if (match) return decodeURIComponent(match[1]);
      } catch {
        return "";
      }
      return "";
    };

    const _addValidationState = () => {
      const url = (document.getElementById("edit-url")?.value || "").trim();
      const folder = (document.getElementById("edit-folder")?.value || "").trim();
      const urlError = _channelUrlValidationError(url);
      const folderError = !folder
        ? "Enter a folder name. @handle links fill it automatically."
        : (/[\\/:*?"<>|]/.test(folder)
          ? 'Folder name can’t contain any of \\ / : * ? " < > |'
          : "");
      const pristine = !url && !folder;
      let message = "";
      if (pristine) {
        message = "Enter a YouTube channel URL or @handle. A folder name is also required.";
      } else if (urlError) {
        message = urlError;
      } else if (folderError) {
        message = folderError;
      }
      return {
        valid: !urlError && !folderError,
        message,
        isError: !pristine && !!message,
        urlInvalid: !pristine && !!urlError,
        folderInvalid: !pristine && !!folderError,
      };
    };

    const _paintAddValidation = () => {
      const state = _addValidationState();
      const row = document.getElementById("edit-channel-validation");
      const url = document.getElementById("edit-url");
      const folder = document.getElementById("edit-folder");
      if (row) {
        row.textContent = state.message;
        row.hidden = state.valid;
        row.classList.toggle("is-error", state.isError);
      }
      if (state.urlInvalid) url?.setAttribute("aria-invalid", "true");
      else url?.removeAttribute("aria-invalid");
      if (state.folderInvalid) folder?.setAttribute("aria-invalid", "true");
      else folder?.removeAttribute("aria-invalid");
      return state.valid;
    };

    const _clearAddValidation = () => {
      const row = document.getElementById("edit-channel-validation");
      if (row) {
        row.hidden = true;
        row.classList.remove("is-error");
      }
      document.getElementById("edit-url")?.removeAttribute("aria-invalid");
      document.getElementById("edit-folder")?.removeAttribute("aria-invalid");
    };
    // Track whether the user has MANUALLY edited the Folder Name. The old
    // guard ("stop once the folder field is non-empty") broke TYPED urls: the
    // first keystroke '...@N' derived "N", then every later keystroke saw a
    // non-empty folder and bailed, freezing the name at "N" (pasting worked
    // because the whole handle arrived in one event). Instead we keep
    // re-deriving on each URL keystroke until the user edits the folder
    // themselves. _suppressFolderEditFlag keeps the auto-fill's own synthetic
    // input event from falsely tripping the "user edited" flag.
    let _folderUserEdited = false;
    let _suppressFolderEditFlag = false;
    const _maybeAutoFillFolder = () => {
      if (_editingIdentity) return;          // editing an existing channel
      const folderEl = document.getElementById("edit-folder");
      const urlEl = document.getElementById("edit-url");
      if (!folderEl || !urlEl) return;
      if (_folderUserEdited) return;         // don't clobber the user's own text
      const guess = _deriveFolderFromUrl((urlEl.value || "").trim());
      if (!guess) return;
      // Sanitize to a valid Windows folder name.
      const clean = guess.replace(/[\\/:*?"<>|]/g, "_")
                         .replace(/\s+/g, " ").trim();
      if (!clean) return;
      if (folderEl.value === clean) return;  // nothing to change
      _suppressFolderEditFlag = true;
      folderEl.value = clean;
      folderEl.dispatchEvent(new Event("input", { bubbles: true }));
      _suppressFolderEditFlag = false;
    };
    // A genuine user edit of the Folder Name stops auto-derive from then on.
    document.getElementById("edit-folder")?.addEventListener("input", () => {
      if (!_suppressFolderEditFlag) _folderUserEdited = true;
    });
    _editUrlField?.addEventListener("input", _maybeAutoFillFolder);
    _editUrlField?.addEventListener("paste",
      () => setTimeout(_maybeAutoFillFolder, 15));

    document.getElementById("btn-edit-url-to-download")?.addEventListener("click", () => {
      const url = (_editUrlField?.value || "").trim();
      if (!url) return;
      document.querySelector('.tab[data-tab="download"]')?.click();
      setTimeout(() => {
        const dl = document.getElementById("url-input");
        if (dl) {
          dl.value = url;
          dl.dispatchEvent(new Event("input", { bubbles: true }));
          dl.focus();
        }
      }, 80);
      if (_editUrlField) {
        _editUrlField.value = "";
        _editUrlField.dispatchEvent(new Event("input", { bubbles: true }));
      }
      _updateEditUrlNudge();
      hideModernEditor();
    });

    const openPanel = (mode, channel) => {
      const panelGeneration = _editorGeneration;
      // A completed or cancelled asynchronous removal must never leave the
      // shared editor controls disabled when it is opened for another channel.
      if (remove) {
        remove.disabled = false;
        remove.removeAttribute("aria-busy");
      }
      if (removeLabel) removeLabel.textContent = "Remove";
      if (cancel) cancel.disabled = false;
      box.removeAttribute("aria-busy");
      const ds = document.getElementById("edit-diskstats");
      const recheckResolution = document.getElementById("edit-res-recheck");
      if (recheckResolution) recheckResolution.hidden = mode !== "edit";
      // Always start from a clean slate so NO field from a previously
      // opened channel (folder name, from-date, compress sub-fields, …)
      // lingers. The edit branch then repopulates from `channel`; the
      // add branch leaves these cleared defaults in place.
      resetFields();
      if (mode === "edit" && channel) {
        _clearAddValidation();
        label.textContent = `Edit channel — ${channel.folder}`;
        // Single folder field: prefer folder_override (on-disk name)
        // over folder (display name).
        const _folderVal = channel.folder_override || channel.folder || channel.name || "";
        document.getElementById("edit-folder").value = _folderVal;
        document.getElementById("edit-url").value = channel.url || "";
        const _rawMode = (channel.mode || "new").toLowerCase();
        const _normalizedMode = _rawMode === "full" ? "full"
                              : (_rawMode === "date" || _rawMode === "fromdate") ? "date"
                              : "new";
        window._editOriginalSnapshot = {
          folder: _folderVal, url: channel.url || "",
          resolution: String(channel.resolution || "720").replace("p",""),
          min_duration: channel.min_duration || 0,
          max_duration: channel.max_duration || 0,
          mode: _normalizedMode,
          folder_org: (channel.split_months ? "months"
                         : (channel.split_years ? "years" : "flat")),
          from_date: channel.from_date || channel.date_after || "",
          auto_transcribe: !!channel.auto_transcribe,
          auto_metadata: !!channel.auto_metadata,
          compress_enabled: !!channel.compress_enabled,
          compress_level: channel.compress_level || "Generous",
          compress_output_res: String(
            channel.compress_output_res || "720"),
        };
        document.getElementById("edit-resolution").value = String(channel.resolution || "720").replace("p", "");
        document.getElementById("edit-min-dur").value = channel.min_duration || "";
        document.getElementById("edit-max-dur").value = channel.max_duration || "";
        const _mode = _rawMode;
        const _rangeVal = _mode === "full" ? "all"
                         : (_mode === "date" || _mode === "fromdate") ? "fromdate"
                         : "subscribe";
        const _rangeRadio = document.querySelector(
          `input[name="edit-range"][value="${_rangeVal}"]`);
        if (_rangeRadio) {
          _rangeRadio.checked = true;
          _rangeRadio.dispatchEvent(new Event("change", { bubbles: true }));
        }
        const _fromDate = channel.from_date || channel.date_after || "";
        // Stricter regex: dashes must be present consistently or absent
        // entirely. Old `\d{4}-?\d{2}-?\d{2}` accepted mixed "1999-0115"
        // which then sliced wrong (audit: editChannel.js:147).
        const _dateMatch = /^(\d{4})(-?)(\d{2})\2(\d{2})$/.exec(_fromDate);
        if (_dateMatch) {
          const y = _dateMatch[1], m = _dateMatch[3], d = _dateMatch[4];
          const dy = document.getElementById("edit-date-year");
          const dm = document.getElementById("edit-date-month");
          const dd = document.getElementById("edit-date-day");
          if (dy) dy.value = y;
          if (dm) dm.value = m;
          if (dd) dd.value = d;
        }
        const _folderOrg = channel.split_months ? "months"
                            : (channel.split_years ? "years" : "flat");
        const foEl2 = document.getElementById("edit-folder-org");
        if (foEl2) foEl2.value = _folderOrg;
        document.getElementById("edit-transcribe").checked = !!channel.auto_transcribe;
        document.getElementById("edit-metadata").checked = !!channel.auto_metadata;
        document.getElementById("edit-compress").checked = !!channel.compress_enabled;
        // Compress sub-fields (level / output resolution) were
        // never loaded from the channel dict when the panel opened,
        // so the dropdowns showed HTML defaults. Any save then stomped
        // the real saved values with those defaults. Load alongside.
        const _lvlEl = document.getElementById("edit-compress-quality");
        if (_lvlEl && channel.compress_level) _lvlEl.value = channel.compress_level;
        const _cResEl = document.getElementById("edit-compress-res");
        if (_cResEl && channel.compress_output_res) _cResEl.value = channel.compress_output_res;
        document.getElementById("edit-compress")?.dispatchEvent(new Event("change"));
        _updateCollapsed();
        update.disabled = true;
        update.textContent = "💾 Update channel";
        remove.style.display = "";
        if (ds) {
          ds.hidden = false;
          const subsRow = (window._subsAllRows || [])
            .find(r => (r.folder || "").toLowerCase() === (channel.folder || "").toLowerCase());
          document.getElementById("ds-videos").textContent = subsRow?.n_vids ?? "—";
          document.getElementById("ds-size").textContent = subsRow?.size ?? "—";
          document.getElementById("ds-last-sync").textContent = subsRow?.last_sync ?? "—";
          const txSep = document.getElementById("ds-tx-sep");
          const txLbl = document.getElementById("ds-tx-label");
          const txVal = document.getElementById("ds-tx-count");
          if (txSep) txSep.hidden = true;
          if (txLbl) txLbl.hidden = true;
          if (txVal) txVal.hidden = true;
          if (nativeBridgeUp()) {
            bridgeCall("channel_transcription_stats", channel.folder || channel.name || "")
              .then((res) => {
                if (panelGeneration !== _editorGeneration) return;
                if (!res?.ok) return;
                if (!res.total) return;
                if (txSep) txSep.hidden = false;
                if (txLbl) txLbl.hidden = false;
                if (txVal) {
                  txVal.hidden = false;
                  // Don't round UP: 469/471 = 99.6% must not display as
                  // (100%) and imply "done". Use one decimal, and if that
                  // would still round to 100.0 while under 100, show 2 dp.
                  const rawPct = res.total ? (100 * res.transcribed / res.total) : 0;
                  let pctStr = rawPct.toFixed(1);
                  if (pctStr === "100.0" && rawPct < 100) pctStr = rawPct.toFixed(2);
                  const nsp = res.no_speech || 0;
                  // Show no-speech videos explicitly so "508 / 510" doesn't
                  // read as "2 behind" — they're checked-and-silent (done).
                  txVal.textContent = `${res.transcribed} / ${res.total} (${pctStr}%)`
                    + (nsp > 0 ? ` · ${nsp} no speech` : "");
                  txVal.title = `Pending: ${res.pending}, Failed: ${res.failed}`
                    + (nsp > 0 ? `, No speech: ${nsp}` : "");
                }
              })
              .catch(() => {});
          }
        }
      } else {
        label.textContent = "Add channel";
        resetFields();
        update.disabled = true;
        update.textContent = "Add channel";
        remove.style.display = "none";
        if (ds) ds.hidden = true;
        _updateCollapsed();
        window._editOriginalSnapshot = null;
        _paintAddValidation();
      }
      if (mode === "edit") {
        box.scrollIntoView({ behavior: "smooth", block: "end" });
      }
    };

    const closePanel = () => {
      // Invalidate any channel-record fetch or save callback that belongs
      // to the panel being closed. A late response must never populate or
      // close a different channel editor that the user opened meanwhile.
      _channelLoadSeq++;
      _editorGeneration++;
      _editingIdentity = null;
      resetFields();
      _clearAddValidation();
      label.textContent = "Add channel";
      update.disabled = true;
      update.textContent = "Add channel";
      remove.style.display = "none";
      remove.disabled = false;
      remove.removeAttribute("aria-busy");
      if (removeLabel) removeLabel.textContent = "Remove";
      cancel.disabled = false;
      box.removeAttribute("aria-busy");
      const ds = document.getElementById("edit-diskstats");
      if (ds) ds.hidden = true;
      _updateCollapsed();
      _folderUserEdited = false;   // a fresh add should auto-derive again
      window._editOriginalSnapshot = null;
      hideModernEditor();
      // A Browse-launched edit temporarily moves the shared form into the
      // modern modal. Put it back after close so the enabled dense Subs tab
      // still has its inline editor the next time the user visits it.
      if (window._legacySubsTabEnabled && legacyHome
          && box.parentElement !== legacyHome) {
        legacyHome.appendChild(box);
      }
    };

    // Dirty check before discarding edits. update.disabled === false
    // means _checkEditChanges saw a change relative to the snapshot
    // (Add mode treats any non-empty folder+url as "dirty"). Without
    // this confirm, Cancel silently nuked unsaved changes (audit:
    // editChannel.js:225).
    cancel.addEventListener("click", async () => {
      if (!update.disabled && window.askConfirm) {
        try {
          const ok = await window.askConfirm(
            "Discard changes?",
            "You have unsaved edits to this channel. Discard them?",
            { confirm: "Discard", cancel: "Keep editing" });
          if (!ok) return;
        } catch (error) {
          flashError("Could not open the confirmation. Your edits were kept.");
          return;
        }
      }
      closePanel();
    });

    // Live change-detection three-state Update button.
    const _editFields = [
      "edit-folder", "edit-url",
      "edit-resolution", "edit-min-dur", "edit-max-dur",
      "edit-folder-org",
      "edit-transcribe", "edit-metadata", "edit-compress",
      "edit-compress-quality", "edit-compress-res",
      "edit-date-year", "edit-date-month", "edit-date-day",
    ];
    const _checkEditChanges = () => {
      const snap = window._editOriginalSnapshot;
      if (!snap) {
        update.disabled = !_paintAddValidation();
        return;
      }
      _clearAddValidation();
      const _folderCur = (document.getElementById("edit-folder")?.value || "").trim();
      const _rangeCur = (document.querySelector('input[name="edit-range"]:checked')?.value || "subscribe");
      const _dateParts = [
        (document.getElementById("edit-date-year")?.value || "").trim(),
        (document.getElementById("edit-date-month")?.value || "").trim(),
        (document.getElementById("edit-date-day")?.value || "").trim(),
      ];
      const _dateCur = _dateParts.every(Boolean)
        ? _dateParts.join("-")
        : _dateParts.filter(Boolean).join("-");
      const cur = {
        folder: _folderCur,
        folder_override: _folderCur,
        url: (document.getElementById("edit-url")?.value || "").trim(),
        resolution: String(document.getElementById("edit-resolution")?.value || "720").replace("p",""),
        min_duration: parseInt(document.getElementById("edit-min-dur")?.value, 10) || 0,
        max_duration: parseInt(document.getElementById("edit-max-dur")?.value, 10) || 0,
        mode: _rangeCur === "all" ? "full"
                         : (_rangeCur === "fromdate" ? "date" : "new"),
        folder_org: (document.getElementById("edit-folder-org")?.value || "flat"),
        from_date: _dateCur,
        auto_transcribe:!!document.getElementById("edit-transcribe")?.checked,
        auto_metadata: !!document.getElementById("edit-metadata")?.checked,
        compress_enabled:!!document.getElementById("edit-compress")?.checked,
        compress_level:
          document.getElementById("edit-compress-quality")?.value || "Generous",
        compress_output_res: String(
          document.getElementById("edit-compress-res")?.value || "720"),
      };
      const dirty = Object.keys(snap).some(k => String(snap[k]) !== String(cur[k]));
      update.disabled = !dirty;
    };
    for (const id of _editFields) {
      const el = document.getElementById(id);
      if (!el) continue;
      el.addEventListener("input", _checkEditChanges);
      if (el.type === "checkbox" || el.tagName === "SELECT") {
        el.addEventListener("change", _checkEditChanges);
      }
    }

    // "Continue" / "Cancel" redownload buttons.
    const continueBtn = document.getElementById("edit-res-continue");
    const cancelBtn = document.getElementById("edit-res-cancel");
    let _redownloadPeekSeq = 0;
    const refreshContinueBtn = async (name) => {
      const peekSeq = ++_redownloadPeekSeq;
      if (!continueBtn) return;
      continueBtn.hidden = true;
      if (cancelBtn) cancelBtn.hidden = true;
      if (!name) return;
      if (!nativeBridgeUp()) return;
      try {
        const p = await bridgeCall("chan_redownload_progress_peek", name);
        if (peekSeq !== _redownloadPeekSeq
            || (_editFolderEl?.value.trim() || "") !== name) return;
        if (p?.ok && p.pending) {
          continueBtn.hidden = false;
          const res = p.resolution || "best";
          const lab = res === "best" ? "Best" : `${res}p`;
          continueBtn.textContent = `↻ Continue ${lab} (${p.done || 0} done)`;
          continueBtn.dataset.resolution = res;
          continueBtn.title = `Resume in-progress ${lab} redownload (${p.done || 0} videos complete)`;
          if (cancelBtn) cancelBtn.hidden = false;
        }
      } catch {}
    };
    const _editFolderEl = document.getElementById("edit-folder");
    _editFolderEl?.addEventListener("change", () => {
      refreshContinueBtn(_editFolderEl.value.trim());
    });
    const _editLabel = document.getElementById("edit-channel-label");
    if (_editLabel) {
      // If a previous initEditChannelPanel call (hot-reload, late-init
      // race) attached an observer, disconnect it before observing
      // again. Otherwise each call stacked another live observer and
      // every label flip fired N redundant peek() API calls (audit:
      // editChannel.js:321).
      if (_editLabel._mo) {
        try { _editLabel._mo.disconnect(); } catch {}
      }
      const mo = new MutationObserver(() => {
        refreshContinueBtn(_editFolderEl?.value.trim() || "");
      });
      mo.observe(_editLabel, { childList: true, characterData: true, subtree: true });
      _editLabel._mo = mo;
    }
    continueBtn?.addEventListener("click", async () => {
      const name = _editFolderEl?.value.trim() || "";
      if (!name) return;
      const res = continueBtn.dataset.resolution || "best";
      if (!nativeBridgeUp()) {
        window._showToast?.("YTArchiver isn't ready yet. Try again in a moment.", "warn");
        return;
      }
      const r = await bridgeCall("chan_redownload", name, res);
      if (r?.ok) {
        if (r.queued) {
          window._showToast?.(`Queued redownload of ${name}.`, "ok");
        } else {
          window._showToast?.("Redownload resumed.", "ok");
        }
      } else {
        window._showToast?.(r?.error || "Resume failed.", "error");
      }
    });
    cancelBtn?.addEventListener("click", async () => {
      const name = _editFolderEl?.value.trim() || "";
      if (!name) return;
      if (!nativeBridgeUp()) {
        window._showToast?.("YTArchiver isn't ready yet. Try again in a moment.", "warn");
        return;
      }
      const ok = await window.askQuestion?.({
        title: "Cancel Redownload",
        message: `Cancel the redownload for "${name}" and discard progress?`,
        confirm: "Cancel Redownload",
        cancel: "Keep",
        danger: true,
      });
      if (!ok) return;
      try {
        const r = await bridgeCall("chan_cancel_redownload", name);
        if (!r?.ok) {
          window._showToast?.(r?.error || "Cancel failed.", "error");
          return;
        }
        await refreshContinueBtn(name);
        try { window.refreshSubsTable?.(); } catch {}
        const bits = [];
        if (r.was_running) bits.push("stopped running job");
        else if (r.was_queued) bits.push("removed from queue");
        if (r.progress_removed) bits.push("cleared saved progress");
        const msg = bits.length
          ? `Cancelled — ${bits.join(", ")}.`
          : "Nothing to cancel.";
        window._showToast?.(msg, "ok");
      } catch (e) {
        window._showToast?.("Error: " + e, "error");
      }
    });

    // Reset resolution to the configured channel default.  The markup's
    // selected option is only a no-script fallback and may not match what the
    // user saved in Settings.
    const resetBtn = document.getElementById("edit-res-reset");
    if (resetBtn) {
      resetBtn.title = "Reset channel resolution to your configured default";
      resetBtn.setAttribute(
        "aria-label", "Reset channel resolution to your configured default");
    }
    resetBtn?.addEventListener("click", async (ev) => {
      ev.preventDefault();
      const resetGeneration = _editorGeneration;
      const resSel = document.getElementById("edit-resolution");
      if (!resSel) return;
      let def = "720";
      if (nativeBridgeUp()) {
        try {
          const defaults = await bridgeCall("subs_get_defaults");
          if (defaults?.resolution) def = String(defaults.resolution);
        } catch {
          // The safe fallback matches the backend's own fallback.
        }
      }
      if (resetGeneration !== _editorGeneration) return;
      if (![...resSel.options].some((option) => option.value === def)) {
        def = "720";
      }
      resSel.value = def;
      // Fire `change` so any live-mirroring widget (custom yt-dd
      // wrapper) syncs its visible label to the new value.
      resSel.dispatchEvent(new Event("change", { bubbles: true }));
      // Programmatic `change` does not fire the `input` listener used by the
      // editor's dirty tracker. Recompute explicitly so Update becomes
      // available after a meaningful reset in Edit mode.
      _checkEditChanges();
    });

    // Recheck resolution — scans the channel folder with ffprobe and
    // offers to queue mismatches for redownload.
    const recheckBtn = document.getElementById("edit-res-recheck");
    recheckBtn?.addEventListener("click", async () => {
      if (!nativeBridgeUp()) {
        window._showToast?.("YTArchiver isn't ready yet. Try again in a moment.", "warn");
        return;
      }
      const name = (document.getElementById("edit-folder")?.value || "").trim();
      const target = (document.getElementById("edit-resolution")?.value || "720").trim();
      if (!name) {
        window._showToast?.("Pick or fill a channel first.", "warn");
        return;
      }
      const targetLabel = target === "audio" ? "audio-only"
        : target === "best" ? "Best available"
        : `${target}p`;
      if (target === "audio" || target === "best") {
        window._showToast?.(
          `${targetLabel} cannot be compared with a local video height. ` +
          "Choose a numbered resolution to run this check.",
          "warn");
        return;
      }
      const scanGeneration = _editorGeneration;
      window._showToast?.("Scanning video files…", "ok");
      try {
        // Token+poll pattern — bridge returns immediately with a token;
        // poll every 500ms until the worker thread reports done. The
        // old synchronous call could freeze the UI for minutes on
        // large channels.
        const startRes = await bridgeCall("chan_scan_resolution_mismatch", name, target);
        if (scanGeneration !== _editorGeneration) return;
        if (!startRes?.ok) {
          window._showToast?.(startRes?.error || "Scan failed.", "error");
          return;
        }
        let res = startRes;
        if (startRes.token && startRes.started) {
          const deadline = Date.now() + 30 * 60 * 1000;
          while (Date.now() < deadline) {
            await new Promise(r => setTimeout(r, 500));
            const p = await bridgeCall("chan_scan_resolution_mismatch_poll", startRes.token);
            if (!p?.pending) { res = p; break; }
          }
          if (scanGeneration !== _editorGeneration) return;
          if (!res || res.pending) {
            window._showToast?.("Scan timed out.", "error");
            return;
          }
        }
        if (!res?.ok) {
          window._showToast?.(res?.error || "Scan failed.", "error");
          return;
        }
        const mismatch = res.mismatch || 0;
        const total = res.total || 0;
        const scanned = Number(res.scanned ?? total) || 0;
        if (mismatch === 0) {
          if (scanned < total) {
            window._showToast?.(
              `Checked ${scanned} of ${total} video(s); ` +
              `${total - scanned} could not be read.`, "warn");
          } else {
            window._showToast?.(
              `All ${total} video(s) match ${targetLabel}.`, "ok");
          }
          return;
        }
        const lab = targetLabel;
        if (scanGeneration !== _editorGeneration) return;
        const ok = await window.askDanger(
          "Redownload at target resolution",
          `${mismatch} of ${scanned || total} checked video(s) in "${name}" ` +
          `have a different resolution. Redownload them at ${lab}?\n\n` +
          "This scans local files, fetches the YouTube catalog, matches by ID, " +
          "downloads each video, and replaces the originals. Progress is saved — " +
          "you can cancel and resume later.",
          "Start redownload");
        if (!ok) return;
        const r2 = await bridgeCall("chan_redownload", name, target);
        if (r2?.ok) window._showToast?.(`Redownload started (${lab}).`, "ok");
        else window._showToast?.(r2?.error || "Redownload failed.", "error");
      } catch (e) {
        window._showToast?.("Error: " + e, "error");
      }
    });

    const applyEditorDefaults = (defs = {}) => {
      document.getElementById("edit-resolution").value = defs.resolution || "720";
      document.getElementById("edit-min-dur").value = defs.min_duration || "";
      document.getElementById("edit-max-dur").value = defs.max_duration || "";
      document.getElementById("edit-transcribe").checked = !!defs.auto_transcribe;
      document.getElementById("edit-metadata").checked =
        defs.auto_metadata !== false;
      document.getElementById("edit-compress").checked = !!defs.compress_enabled;
      document.getElementById("edit-compress")?.dispatchEvent(new Event("change"));
      document.getElementById("edit-folder-org").value = defs.folder_org || "years";
      const mode = String(defs.mode || "new").toLowerCase();
      const rangeValue = mode === "full" ? "all"
        : (mode === "fromdate" || mode === "date") ? "fromdate"
        : "subscribe";
      const rangeRadio = document.querySelector(
        `input[name="edit-range"][value="${rangeValue}"]`);
      if (rangeRadio) { rangeRadio.checked = true; rangeRadio.dispatchEvent(new Event("change")); }
      for (const id of ["edit-resolution", "edit-folder-org"]) {
        document.getElementById(id)?.dispatchEvent(
          new Event("change", { bubbles: true }));
      }
      for (const id of ["edit-date-year", "edit-date-month", "edit-date-day"]) {
        const element = document.getElementById(id);
        if (element) element.value = "";
      }
      try { _checkEditChanges?.(); } catch {}
    };

    // Restore defaults button — pulls from settings and applies.
    document.getElementById("btn-edit-restore")?.addEventListener("click", async () => {
      const restoreGeneration = _editorGeneration;
      const defs = nativeBridgeUp() ? await bridgeCall("subs_get_defaults") : null;
      if (restoreGeneration !== _editorGeneration) return;
      if (!defs) {
        window._showToast?.("Channel defaults could not be loaded.", "error");
        return;
      }
      applyEditorDefaults(defs);
      // Fire the dirty-check explicitly so the Update button enables
      // after a Restore Defaults — only compress fires `change`
      // events, so other fields' programmatic value-writes never
      // reached the input/change listener (audit: editChannel L88).
      window._showToast?.("Defaults restored.", "ok");
    });

    // Collect form state into a payload.
    const collectPayload = () => {
      const range = document.querySelector('input[name="edit-range"]:checked')?.value || "subscribe";
      const fromYear = document.getElementById("edit-date-year")?.value || "";
      const fromMonth = document.getElementById("edit-date-month")?.value || "";
      const fromDay = document.getElementById("edit-date-day")?.value || "";
      const from_date = (fromYear && fromMonth && fromDay)
        ? `${fromYear.padStart(4, "0")}-${fromMonth.padStart(2, "0")}-${fromDay.padStart(2, "0")}`
        : "";
      const _folderVal = document.getElementById("edit-folder").value.trim();
      const payload = {
        folder: _folderVal,
        url: document.getElementById("edit-url").value.trim(),
        folder_override: _folderVal,
        resolution: document.getElementById("edit-resolution").value,
        folder_org: document.getElementById("edit-folder-org").value,
        auto_transcribe: document.getElementById("edit-transcribe").checked,
        auto_metadata: document.getElementById("edit-metadata").checked,
        compress_enabled: document.getElementById("edit-compress").checked,
        compress_level: document.getElementById("edit-compress-quality")?.value || "Generous",
        compress_output_res: document.getElementById("edit-compress-res")?.value || "720",
        range,
        from_date,
      };
      const minimumRaw = String(
        document.getElementById("edit-min-dur")?.value ?? "").trim();
      const maximumRaw = String(
        document.getElementById("edit-max-dur")?.value ?? "").trim();
      // A blank field on an existing channel means "leave this saved limit
      // alone".  Omitting the key is important: JSON drops undefined values,
      // while serializing a literal zero silently clears the setting.  In Add
      // mode a blank is the visible no-limit value until configured defaults
      // finish loading, so preserve the established zero fallback there.
      if (minimumRaw || !_editingIdentity) {
        payload.min_duration = minimumRaw
          ? Number.parseInt(minimumRaw, 10) : 0;
      }
      if (maximumRaw || !_editingIdentity) {
        payload.max_duration = maximumRaw
          ? Number.parseInt(maximumRaw, 10) : 0;
      }
      return payload;
    };

    const validatePayload = (payload) => {
      const minimumRaw = String(
        document.getElementById("edit-min-dur")?.value || "").trim();
      const maximumRaw = String(
        document.getElementById("edit-max-dur")?.value || "").trim();
      if (minimumRaw && !/^\d+$/.test(minimumRaw)) {
        return "Minimum length must be a whole number of minutes.";
      }
      if (maximumRaw && !/^\d+$/.test(maximumRaw)) {
        return "Maximum length must be a whole number of minutes.";
      }
      const minimum = Number(payload.min_duration || 0);
      const maximum = Number(payload.max_duration || 0);
      if (minimum > 0 && maximum > 0 && minimum > maximum) {
        return "Minimum length cannot be greater than maximum length.";
      }

      if (payload.range === "fromdate") {
        const year = String(
          document.getElementById("edit-date-year")?.value || "").trim();
        const month = String(
          document.getElementById("edit-date-month")?.value || "").trim();
        const day = String(
          document.getElementById("edit-date-day")?.value || "").trim();
        if (!/^\d{4}$/.test(year)
            || !/^\d{1,2}$/.test(month)
            || !/^\d{1,2}$/.test(day)) {
          return "From date needs a year, month, and day.";
        }
        const y = Number(year), m = Number(month), d = Number(day);
        const parsed = new Date(Date.UTC(y, m - 1, d));
        if (parsed.getUTCFullYear() !== y
            || parsed.getUTCMonth() !== m - 1
            || parsed.getUTCDate() !== d) {
          return "From date is not a valid calendar date.";
        }
      }
      return "";
    };

    let _editingIdentity = null;
    let _editorGeneration = 0;
    let _channelLoadSeq = 0;
    const _origOpenPanel = openPanel;
    const wrappedOpenPanel = (mode, channel, options = {}) => {
      _editorGeneration++;
      const generation = _editorGeneration;
      presentEditorShell(mode, channel, true, options);
      _origOpenPanel(mode, channel);
      _editingIdentity = (mode === "edit" && channel)
        ? { url: channel.url, name: channel.folder || channel.name }
        : null;
      requestAnimationFrame(() => {
        const first = document.getElementById(
          mode === "edit" ? "edit-folder" : "edit-url");
        first?.focus();
        if (mode === "add") first?.select?.();
      });
      if (mode === "add" && nativeBridgeUp()) {
        const settingIds = [
          "edit-resolution", "edit-min-dur", "edit-max-dur",
          "edit-folder-org", "edit-transcribe", "edit-metadata",
          "edit-compress",
        ];
        const signature = () => settingIds.map((id) => {
          const element = document.getElementById(id);
          return element?.type === "checkbox"
            ? String(!!element.checked) : String(element?.value ?? "");
        }).join("\u0000") + "\u0000" +
          (document.querySelector('input[name="edit-range"]:checked')?.value || "");
        const initialSignature = signature();
        bridgeCall("subs_get_defaults").then((defs) => {
          if (generation !== _editorGeneration
              || signature() !== initialSignature || !defs) return;
          applyEditorDefaults(defs);
        }).catch(() => {});
      }
    };
    window._editChannelFromContext = (folder, urlGuess, options = {}) => {
      // No URL fallback construction here. Previously the fallback
      // built a guessed `youtube.com/@{folder-without-spaces}` URL
      // that was indistinguishable from a real URL when persisted —
      // a folder name with `?`, `#`, `/`, or `..` produced a
      // malformed/invalid URL that silently saved into the channel
      // record. Leave url empty when we don't have a real one; the
      // edit panel surfaces the missing URL so the user pastes a
      // legitimate one before saving.
      const chan = { folder, url: urlGuess || "" };
      const loadSeq = ++_channelLoadSeq;
      // Retire the previous editor immediately, before the asynchronous
      // channel fetch. Leaving its identity and enabled Save button in the
      // shared form made it possible to click B, still see A briefly, and
      // submit A while believing B was selected.
      _editorGeneration++;
      _editingIdentity = null;
      resetFields();
      label.textContent = `Loading channel — ${folder}`;
      update.disabled = true;
      remove.style.display = "none";
      window._editOriginalSnapshot = null;
      // Give immediate visual feedback while the full channel record loads.
      presentEditorShell("edit", chan, true, options);
      if (nativeBridgeUp()) {
        bridgeCall("subs_get_channel", { name: folder }).then(res => {
          if (loadSeq !== _channelLoadSeq) return;
          if (!res?.ok || !res?.channel) {
            flashError(res?.error || `Could not load "${folder}".`);
            closePanel();
            return;
          }
          const channel = {
            ...res.channel,
            folder: res.channel.name || res.channel.folder,
          };
          wrappedOpenPanel("edit", channel, options);
        }).catch((error) => {
          if (loadSeq === _channelLoadSeq) {
            flashError(
              `Could not load "${folder}": ${error?.message || error}`);
            closePanel();
          }
        });
      } else {
        flashError("YTArchiver isn't ready to load channel settings yet.");
        closePanel();
      }
    };

    window._editChannelFromBrowse = (folder, urlGuess) => {
      window._editChannelFromContext?.(
        folder, urlGuess, { forceModern: true });
    };

    window._openAddChannelEditor = (urlGuess = "") => {
      // This entry point belongs to Browse and the first-run empty state;
      // keep their new dialog even when the optional dense tab is enabled.
      _channelLoadSeq++;
      wrappedOpenPanel("add", null, { forceModern: true });
      if (!urlGuess) return;
      const urlField = document.getElementById("edit-url");
      if (!urlField) return;
      urlField.value = String(urlGuess).trim();
      urlField.dispatchEvent(new Event("input", { bubbles: true }));
      urlField.focus();
      urlField.select?.();
    };
    document.getElementById("browse-add-channel")?.addEventListener("click", () => {
      window._openAddChannelEditor?.();
    });

    const requestEditorClose = () => cancel?.click();
    document.getElementById("channel-editor-close")
      ?.addEventListener("click", requestEditorClose);
    editorBackdrop?.addEventListener("click", (e) => {
      if (e.target === editorBackdrop) requestEditorClose();
    });
    document.addEventListener("keydown", (e) => {
      if (e.key !== "Escape" || editorBackdrop?.hidden) return;
      const anotherDialogOpen = [...document.querySelectorAll(
        '.askq-backdrop:not(#channel-editor-backdrop)')]
        .some((el) => !el.hidden);
      if (anotherDialogOpen) return;
      e.preventDefault();
      e.stopPropagation();
      requestEditorClose();
    }, true);

    const _subsTbody = document.getElementById("subs-table-body");
    if (_subsTbody) {
      _subsTbody.addEventListener("dblclick", (e) => {
        const tr = e.target.closest("tr");
        if (!tr) return;
        const folder = tr.dataset.channelName
          || (tr.querySelector(".col-folder")?.textContent || "").trim();
        if (folder) window._editChannelFromContext(folder);
      });
    }

    update.addEventListener("click", async () => {
      // Disable the button during the in-flight subs_check_duplicate +
      // subs_update_channel/subs_add_channel sequence. Without this
      // guard a double-click fires two concurrent saves in parallel
      // — last-write-wins with potentially stale snapshot data on
      // the second, or duplicate "Channel added" toasts and DB rows
      // on the Add path. Snapshot _editingIdentity locally before
      // the awaits since closePanel() inside the success branch
      // nulls it, which would otherwise make the post-await toast
      // report the wrong action ("Channel added" vs "Channel
      // updated") if the user clicked Cancel mid-flight.
      if (update.disabled) return;
      update.disabled = true;
      const _savedIdentity = _editingIdentity;
      const _saveGeneration = _editorGeneration;
      try {
      const payload = collectPayload();
      const validationError = validatePayload(payload);
      if (validationError) { flashError(validationError); return; }
      if (!payload.folder) { flashError("Folder name is required."); return; }
      if (/[\\/:*?"<>|]/.test(payload.folder)) {
        flashError('Folder name can’t contain any of \\ / : * ? " < > |');
        return;
      }
      if (!payload.url) { flashError("Channel URL is required."); return; }
      // Keeps direct `api`: the dedup step below is gated on an
      // optional-method existence check (`if (api.subs_check_duplicate)`)
      // that the YT.api proxy can't express (it resolves every name to a
      // function), so route this handler through the raw bridge.
      const api = window.pywebview?.api;
      if (!api) { flashError("YTArchiver isn't ready to save changes yet. Try again in a moment."); return; }

      if (api.subs_check_duplicate) {
        try {
          const dup = await api.subs_check_duplicate(
            payload.url, payload.folder, _savedIdentity || null);
          // Cancel/reopen while the duplicate check was running means this
          // save no longer belongs to the visible editor. No write has
          // happened yet, so stop cleanly.
          if (_saveGeneration !== _editorGeneration) return;
          if (dup?.ok && (dup.dup_url || dup.dup_folder)) {
            const parts = [];
            if (dup.dup_url) parts.push(`• URL already used by:\n ${dup.dup_url}`);
            if (dup.dup_folder) parts.push(`• Folder name already taken by:\n ${dup.dup_folder}`);
            await window.askConfirm(
              "Duplicate channel",
              "This clashes with an existing subscription:\n\n" +
              parts.join("\n\n") +
              "\n\nResolve the conflict (change the URL or folder name) " +
              "and try again.",
              { confirm: "OK", noCancel: true });
            return;
          }
        } catch { /* if check fails, let the real add surface the error */ }
        if (_saveGeneration !== _editorGeneration) return;
      }

      let res;
      try {
        if (_savedIdentity) {
          res = await api.subs_update_channel(_savedIdentity, payload);
        } else {
          res = await api.subs_add_channel(payload);
        }
      } catch (e) { flashError("Error: " + e); return; }
      // Defensive: subs_update_channel can return undefined on bridge
      // failures (audit: editChannel.js:565). Old `!res.ok` threw
      // TypeError on undefined and silently bailed without a toast.
      if (!res || !res.ok || res.write_blocked) {
        flashError(res?.error ||
          "The channel changes could not be saved. Your edits are still open.");
        return;
      }
      const wasAdd = !_savedIdentity;
      const addedChannelName = (res.channel?.name || res.channel?.folder
        || payload.folder || payload.name || "").trim();
      const shouldReorgAfterSave = !!(
        _savedIdentity && res.folder_org_changed);
      const reorgChannelName = (res.channel?.name || res.channel?.folder
        || payload.folder || payload.name || "").trim();
      // Use the snapshot taken before the awaits — closePanel() below nulls
      // _editingIdentity, so this must not infer add/edit after the await.
      flashOk(_savedIdentity ? "Channel updated." : "Channel added.");
      if (res.processing_queue_warning) {
        window._showToast?.(
          `Channel updated, but queued Processing tasks need attention: ` +
          res.processing_queue_warning,
          "warn");
      }
      await refreshSubsTable();
      // The backend save may finish after Cancel and a new editor open.
      // Refresh shared data, but never close or alter that newer editor.
      const saveStillOwnsEditor = _saveGeneration === _editorGeneration;
      if (saveStillOwnsEditor) closePanel();
      if (shouldReorgAfterSave && reorgChannelName) {
        try {
          const rr = await bridgeCall("reorg_channel_folder", {
            name: reorgChannelName,
          }, !!res.channel?.split_years, !!res.channel?.split_months, false);
          if (!rr?.ok) {
            window._showToast?.(
              rr?.error || "Folder reorganization did not start.", "error");
          } else {
            window._showToast?.("Folder reorganization started.", "ok");
          }
        } catch (e) {
          window._showToast?.("Reorg error: " + e, "error");
        }
      }
      if (wasAdd && addedChannelName) {
        // A newer panel owns the shared UI now; do not put an old Add flow's
        // follow-up prompt over it.
        if (!saveStillOwnsEditor) return;
        const syncNow = await (window.askConfirm
          ? window.askConfirm("Channel added", "Channel added. Sync now?", {
              confirm: "Sync now",
              cancel: "Later",
            })
          : Promise.resolve(confirm("Channel added. Sync now?")));
        if (syncNow) {
          try {
            const syncRes = await bridgeCall("sync_one_channel", {
              name: addedChannelName,
            });
            window.YT?.bridge?.reportSyncOneResult?.(syncRes);
          } catch (e) {
            window.YT?.bridge?.reportSyncOneResult?.({
              ok: false,
              error: "Sync failed: " + (e?.message || e),
            });
          }
        }
      }
      } finally {
        // Do not re-enable the shared button for a different channel panel.
        if (_saveGeneration === _editorGeneration) update.disabled = false;
      }
    });

    remove.addEventListener("click", async () => {
      if (!_editingIdentity || remove.disabled) return;
      const removeIdentity = { ..._editingIdentity };
      const removeGeneration = _editorGeneration;
      remove.disabled = true;
      remove.setAttribute("aria-busy", "true");
      if (removeLabel) removeLabel.textContent = "Removing…";
      update.disabled = true;
      cancel.disabled = true;
      box.setAttribute("aria-busy", "true");
      try {
        const res = await window._removeChannelWithPrompt(removeIdentity.name);
        const committed = !!(res?.ok || res?.subscription_removed);
        // The shared removal helper refreshes Subs and Browse. Only close the
        // editor if this operation still owns the panel it started from.
        if (committed && removeGeneration === _editorGeneration) closePanel();
      } finally {
        if (removeGeneration === _editorGeneration) {
          remove.disabled = false;
          remove.removeAttribute("aria-busy");
          if (removeLabel) removeLabel.textContent = "Remove";
          cancel.disabled = false;
          box.removeAttribute("aria-busy");
          try { _checkEditChanges(); } catch {}
        }
      }
    });

    let _subsRefreshInFlight = null;
    let _subsRefreshQueued = false;
    let _subsRefreshQueuedPrime = false;

    async function refreshSubsTable(options = {}) {
      if (!nativeBridgeUp()) return;
      const primeBrowse = options.primeBrowse !== false;
      if (_subsRefreshInFlight) {
        _subsRefreshQueued = true;
        _subsRefreshQueuedPrime = _subsRefreshQueuedPrime || primeBrowse;
        return _subsRefreshInFlight;
      }
      try {
        _subsRefreshInFlight = (async () => {
          const data = await bridgeCall("get_subs_channels");
          if (Array.isArray(data) && data.length === 2) {
            window.renderSubsTable(data[0], data[1]);
            const browseCountsChanged =
              window._refreshBrowseChannelSummaries?.(data[0]) || false;
            // The rich Browse refresh can touch every channel's art on disk.
            // Per-channel sync pushes opt out; user/startup refreshes keep it.
            if (primeBrowse) {
              window._primeBrowse(data[0]);
            } else if (browseCountsChanged) {
              // A real count change means downloads landed (or a channel was
              // added/removed). Refresh the small "New this week" summary once
              // without paying for the all-channel art/catalog read.
              window._refreshBrowseWeekSummary?.();
            }
          }
        })();
        await _subsRefreshInFlight;
      } catch (e) { console.warn("refresh failed", e); }
      finally {
        _subsRefreshInFlight = null;
        if (_subsRefreshQueued) {
          const queuedPrime = _subsRefreshQueuedPrime;
          _subsRefreshQueued = false;
          _subsRefreshQueuedPrime = false;
          setTimeout(() => refreshSubsTable({ primeBrowse: queuedPrime }), 0);
        }
      }
    }
    window.refreshSubsTable = refreshSubsTable;

    // ── Conditional group visibility toggles ──
    const compressBox = document.getElementById("edit-compress");
    const compressGroup = document.getElementById("edit-compress-group");
    const syncCompressVis = () => {
      if (compressGroup) compressGroup.hidden = !compressBox?.checked;
    };
    compressBox?.addEventListener("change", syncCompressVis);
    syncCompressVis();

    const dateGroup = document.getElementById("edit-date-group");
    document.querySelectorAll('input[name="edit-range"]').forEach(r => {
      r.addEventListener("change", (e) => {
        if (dateGroup) dateGroup.hidden = (e.target.value !== "fromdate");
        if (e.target.value === "fromdate" && dateGroup) {
          document.getElementById("edit-date-year")?.focus();
        }
        try { _checkEditChanges(); } catch {}
      });
    });

    // Auto-advance between YYYY/MM/DD as user types digits
    const dateParts = [
      ["edit-date-year", 4, "edit-date-month"],
      ["edit-date-month", 2, "edit-date-day"],
      ["edit-date-day", 2, null],
    ];
    for (const [id, maxLen, nextId] of dateParts) {
      const el = document.getElementById(id);
      if (!el) continue;
      el.addEventListener("input", () => {
        el.value = el.value.replace(/\D/g, "");
        if (el.value.length >= maxLen && nextId) {
          document.getElementById(nextId)?.focus();
        }
      });
      el.addEventListener("keydown", (e) => {
        if (e.key === "Backspace" && el.value === "") {
          const prev = dateParts.find(([_, __, n]) => n === id);
          if (prev) document.getElementById(prev[0])?.focus();
        }
      });
    }
  }

  window.initEditChannelPanel = initEditChannelPanel;
})();
