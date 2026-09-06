/* ═══════════════════════════════════════════════════════════════════════
   settingsInfra.js — Settings/Health sub-navigation + archive-folder preferences

   Shared Settings navigation and infrastructure.

   Two related plumbing functions:
     • initSettingsSubTabs — wires the sub-navigation buttons
       that switch pages inside Settings and Health.
     • initSettingsArchiveRoots — Additional archive folders and
       Search-index background checks in Settings, plus the existing
       folder-list actions.

   Publishes:
     window.initSettingsSubTabs
     window.initSettingsArchiveRoots
     window._initSettingsArchiveRoots  (legacy alias)

   Reads:
     window.pywebview.api.settings_* / archive_roots_*
     window.askConfirm / askDanger / askChoice / askTextInput / showContextMenu
     window._showToast
     window._refreshIndexStats / window._refreshMetadataTab
   ═══════════════════════════════════════════════════════════════════════ */

(function () {
  "use strict";

  function bridgeCall(method, ...args) {
    const fn = window.YT?.bridge?.bridgeCall;
    if (fn) return fn(method, ...args);
    return undefined;
  }

  function nativeBridgeUp() {
    return !!window.YT?.bridge?.isUp?.();
  }

  const askConfirm = window.askConfirm;
  const askDanger = window.askDanger;
  const askQuestion = window.askQuestion;
  const askChoice = window.askChoice;
  const askTextInput = window.askTextInput;
  const showContextMenu = window.showContextMenu || (() => {});

  function initSettingsSubTabs() {
    // Wire each independent sub-nav area. Health uses the .settings-area /
    // .settings-subnav-btn / .settings-view structure.
    document.querySelectorAll(".settings-area").forEach(_initSubNavArea);
  }

  function _initSubNavArea(area) {
    const buttons = area.querySelectorAll(".settings-subnav-btn");
    if (!buttons.length) return;
    // Views are derived from their ids ("settings-view-<key>").
    const views = {};
    area.querySelectorAll(".settings-view").forEach((v) => {
      const k = v.id.replace(/^settings-view-/, "");
      if (k) views[k] = v;
    });
    // Only the Settings area carries the auto-save footer.
    const saveFooter = area.querySelector("#settings-actions-footer");
    area.querySelector(".settings-sidebar")?.setAttribute("role", "tablist");
    const show = (key) => {
      buttons.forEach((b) => {
        const active = b.dataset.settingsView === key;
        b.classList.toggle("active", active);
        b.setAttribute("aria-selected", active ? "true" : "false");
      });
      for (const k of Object.keys(views)) {
        if (views[k]) views[k].hidden = (k !== key);
      }
      // Each destination is a separate page. Do not carry the previous
      // page's scroll position into the next one (for example, opening
      // Overview at the bottom after scrolling through Metadata).
      const main = area.querySelector(".settings-main");
      if (main) main.scrollTop = 0;
      // Hide the auto-save note on views that have their own actions.
      if (saveFooter) {
        saveFooter.style.display =
          (key === "index" || key === "metadata") ? "none" : "";
      }
      // Refresh-on-show hooks.
      if (key === "index") window._refreshIndexStats?.();
      if (key === "metadata") window._refreshMetadataTab?.({ preferCache: true });
      if (key === "library") {
        window._refreshIndexStats?.();
        window._refreshMetadataTab?.({ preferCache: true });
      }
      window.YT?.navigationHistory?.record?.();
    };
    buttons.forEach((b) => {
      b.setAttribute("role", "tab");
      b.tabIndex = 0;
      if (b.dataset.settingsView) {
        b.setAttribute("aria-controls", "settings-view-" + b.dataset.settingsView);
      }
      b.addEventListener("keydown", (e) => {
        if (e.key === "Enter" || e.key === " ") { e.preventDefault(); b.click(); }
      });
      b.addEventListener("click", () => show(b.dataset.settingsView));
    });
    show(area.querySelector(".settings-subnav-btn.active")?.dataset.settingsView
         || buttons[0].dataset.settingsView);
  }

  // Settings > Storage & library: extra folders included in Search and
  // the automatic Search-index sweep cadence.
  // The list also retains its existing transcript-maintenance context action.
  function initSettingsArchiveRoots() {
    const rootsList = document.getElementById("settings-roots-list");
    const bAdd = document.getElementById("btn-settings-add-root");
    const bRemove = document.getElementById("btn-settings-remove-root");
    const autoCB = document.getElementById("settings-auto-index-enabled");
    const autoThr = document.getElementById("settings-auto-index-threshold");
    if (!rootsList && !autoCB) return;

    let _selectedRoot = null;

    const _pathKey = (value) => String(value || "")
      .trim().replaceAll("/", "\\").replace(/[\\]+$/, "").toLowerCase();
    const _pathWithin = (path, root) => {
      const child = _pathKey(path);
      const parent = _pathKey(root);
      return !!child && !!parent
        && (child === parent || child.startsWith(parent + "\\"));
    };

    const _setSelectedRoot = (row, path) => {
      rootsList?.querySelectorAll(".root-entry.selected").forEach((el) => {
        el.classList.remove("selected");
        el.setAttribute("aria-selected", "false");
      });
      row?.classList.add("selected");
      row?.setAttribute("aria-selected", "true");
      _selectedRoot = path || null;
      if (bRemove) bRemove.disabled = !path || row?.classList.contains("auto");
    };

    const saveSettingsChecked = async (payload, label) => {
      const res = await bridgeCall("settings_save", payload);
      if (!res?.ok) {
        throw new Error(res?.error || `${label || "Settings"} save failed.`);
      }
      return res;
    };

    const _confirmDeleteAllTranscriptions = async (folder) => {
      if (!nativeBridgeUp()) {
        window._showToast?.(
          "YTArchiver is still starting. Try again in a moment.", "warn");
        return;
      }
      window._showToast?.("Counting transcript files\u2026", "ok");
      let count;
      try {
        count = await bridgeCall("index_count_transcripts", folder);
      } catch (error) {
        window._showToast?.(`Count failed: ${error}`, "error");
        return;
      }
      if (!count?.ok) {
        window._showToast?.(count?.error || "Count failed.", "error");
        return;
      }
      const { txt_count = 0, jsonl_count = 0, total = 0, total_bytes = 0 } = count;
      if (!total) {
        window._showToast?.(`No transcript files under ${folder}.`, "ok");
        return;
      }
      const mb = (total_bytes / (1024 * 1024)).toFixed(1);
      const ok1 = await window.askDanger(
        "Delete all transcript data?",
        `This will permanently delete transcript data under:\n\n${folder}\n\n` +
        ` \u2022 ${txt_count.toLocaleString()} readable transcript file(s)\n` +
        ` \u2022 ${jsonl_count.toLocaleString()} search-support file(s)\n\n` +
        `Total: ${total.toLocaleString()} file(s), ${mb} MB\n\n` +
        "This cannot be undone.",
        "Continue");
      if (!ok1) return;
      const ok2 = await window.askDanger(
        "Final confirmation",
        `All ${total.toLocaleString()} transcript-related file(s) will be ` +
        "permanently deleted. Transcript search results for this folder " +
        "will also be cleared. Those videos will need new transcripts " +
        "before they can appear in transcript search again.",
        "Yes, DELETE EVERYTHING");
      if (!ok2) return;
      let res;
      try {
        res = await bridgeCall(
          "index_delete_all_transcripts", folder, "YES-DELETE-ALL");
      } catch (error) {
        window._showToast?.(`Transcript deletion did not start: ${error}`, "error");
        return;
      }
      if (res?.ok && res?.started) {
        window._showToast?.(
          "Transcript deletion started. Progress is shown in Activity; " +
          "you’ll be notified when it finishes.",
          "warn");
      } else {
        window._showToast?.(res?.error || "Transcript deletion did not start.", "error");
      }
    };

    const renderRoots = async () => {
      if (!rootsList) return;
      _selectedRoot = null;
      if (bRemove) bRemove.disabled = true;
      rootsList.innerHTML = "";
      rootsList.setAttribute("role", "listbox");
      let outDir = "";
      let extras = [];
      try {
        const s = nativeBridgeUp() ? await bridgeCall("settings_load") : null;
        outDir = (s?.output_dir || "").trim();
        extras = Array.isArray(s?.tp_archive_roots) ? s.tp_archive_roots : [];
      } catch (e) {
        console.warn("settings_load archive roots failed:", e);
        window._showToast?.(`Could not load archive folders: ${e}`, "error");
      }
      const entries = [];
      const seen = new Set();
      if (outDir) entries.push({ path: outDir, auto: true });
      if (outDir) seen.add(_pathKey(outDir));
      for (const r of extras) {
        const key = _pathKey(r);
        if (key && !seen.has(key)) {
          seen.add(key);
          entries.push({ path: r, auto: false });
        }
      }
      for (const e of entries) {
        const row = document.createElement("div");
        row.className = "root-entry" + (e.auto ? " auto" : "");
        row.dataset.path = e.path;
        row.setAttribute("role", "option");
        row.setAttribute("aria-selected", "false");
        row.tabIndex = 0;
        row.textContent = e.auto ? `Primary — ${e.path}` : e.path;
        row.addEventListener("click", () => _setSelectedRoot(row, e.path));
        row.addEventListener("keydown", (event) => {
          if (event.key === "Enter" || event.key === " ") {
            event.preventDefault();
            _setSelectedRoot(row, e.path);
          } else if (event.key === "ContextMenu"
                     || (event.shiftKey && event.key === "F10")) {
            event.preventDefault();
            _setSelectedRoot(row, e.path);
            const rect = row.getBoundingClientRect();
            window.showContextMenu?.(rect.left + 20, rect.top + 20, [
              { label: "Delete All Transcriptions", cls: "danger",
                action: () => _confirmDeleteAllTranscriptions(e.path) },
            ]);
          }
        });
        row.addEventListener("contextmenu", (ev) => {
          ev.preventDefault();
          _setSelectedRoot(row, e.path);
          if (window.showContextMenu) {
            window.showContextMenu(ev.clientX, ev.clientY, [
              { label: "\u{1F5D1} Delete All Transcriptions",
                cls: "danger",
                action: () => _confirmDeleteAllTranscriptions(e.path) },
            ]);
          }
        });
        rootsList.appendChild(row);
      }
    };

    bAdd?.addEventListener("click", async () => {
      if (!nativeBridgeUp()) {
        window._showToast?.(
          "YTArchiver is still starting. Try again in a moment.", "warn");
        return;
      }
      try {
        const res = await bridgeCall("pick_folder", "Select archive folder");
        if (!res?.ok || !res.path) {
          if (!res?.cancelled) {
            window._showToast?.(
              res?.error || "Could not choose a folder.", "error");
          }
          return;
        }
        const s = await bridgeCall("settings_load");
        if (!s) throw new Error("Settings could not be loaded.");
        const extras = Array.isArray(s?.tp_archive_roots)
          ? [...s.tp_archive_roots] : [];
        if (_pathWithin(res.path, s.output_dir)
            || _pathWithin(s.output_dir, res.path)) {
          window._showToast?.(
            "Choose a separate folder that does not overlap the main archive.",
            "warn");
          return;
        }
        if (extras.some((path) => _pathWithin(res.path, path))) {
          window._showToast?.("That folder is already included.", "warn");
          return;
        }
        extras.push(res.path);
        await saveSettingsChecked(
          { tp_archive_roots: extras }, "Archive folders");
        await renderRoots();
        const scan = await bridgeCall("archive_rescan");
        if (scan?.ok) {
          window._showToast?.(
            "Folder added. YTArchiver is adding its files to Search.", "ok");
        } else {
          window._showToast?.(
            scan?.error || "Folder added. Run Rescan when current work finishes.",
            "warn");
        }
      } catch (e) {
        window._showToast?.("Could not add the archive folder: " + e, "error");
      }
    });

    bRemove?.addEventListener("click", async () => {
      if (!_selectedRoot) {
        window._showToast?.("Select an additional archive folder first.", "warn");
        return;
      }
      if (!nativeBridgeUp()) return;
      try {
        const s = await bridgeCall("settings_load");
        if (!s || !Array.isArray(s.tp_archive_roots)) {
          throw new Error("Archive folder settings could not be loaded.");
        }
        const outDir = (s?.output_dir || "").trim();
        if (_pathKey(_selectedRoot) === _pathKey(outDir)) {
          window._showToast?.(
            "The primary archive folder can't be removed here. " +
            "Change it under Archive folder in Settings.", "warn");
          return;
        }
        const originalExtras = [...(s?.tp_archive_roots || [])];
        if (!originalExtras.some(
            path => _pathKey(path) === _pathKey(_selectedRoot))) {
          _selectedRoot = null;
          await renderRoots();
          throw new Error("That folder is no longer in Settings.");
        }
        const extras = originalExtras.filter(
          r => _pathKey(r) !== _pathKey(_selectedRoot));
        const removing = _selectedRoot;
        await saveSettingsChecked(
          { tp_archive_roots: extras }, "Archive folders");
        const removed = await bridgeCall("index_remove_archive_root", removing);
        if (!removed?.ok) {
          // Keep Settings and Search consistent if catalog cleanup could not
          // obtain its writer lease or failed for any other reason.
          await saveSettingsChecked(
            { tp_archive_roots: originalExtras }, "Archive folders");
          throw new Error(removed?.error || "Search cleanup failed.");
        }
        _selectedRoot = null;
        await renderRoots();
        window._showToast?.(
          "Folder removed from Search. Files were left in place.", "ok");
        window._refreshIndexStats?.();
        window._refreshMetadataTab?.({ force: true });
      } catch (e) {
        window._showToast?.("Could not remove the archive folder: " + e, "error");
      }
    });

    let _autoSaved = { enabled: false, threshold: 10 };
    let _autoSaving = false;
    const persistAuto = async () => {
      if (_autoSaving) return;
      if (!nativeBridgeUp()) {
        if (autoCB) autoCB.checked = _autoSaved.enabled;
        if (autoThr) autoThr.value = String(_autoSaved.threshold);
        window._showToast?.(
          "YTArchiver is still starting. Try again in a moment.", "warn");
        return;
      }
      const enabled = !!autoCB?.checked;
      let n = parseInt(autoThr?.value || "10", 10);
      if (!Number.isFinite(n) || n < 1) {
        if (autoThr) autoThr.value = String(_autoSaved.threshold);
        window._showToast?.(
          "Enter at least 1 download between Search checks.", "error");
        return;
      }
      if (n > 9999) n = 9999;
      if (autoThr) autoThr.value = String(n);
      _autoSaving = true;
      if (autoCB) autoCB.disabled = true;
      if (autoThr) autoThr.disabled = true;
      try {
        await saveSettingsChecked({
          auto_index_enabled: enabled,
          auto_index_threshold: n,
        }, "Automatic Search update");
        _autoSaved = { enabled, threshold: n };
      } catch (e) {
        if (autoCB) autoCB.checked = _autoSaved.enabled;
        if (autoThr) autoThr.value = String(_autoSaved.threshold);
        console.warn("auto-index settings save failed:", e);
        window._showToast?.(`Could not save automatic indexing: ${e}`, "error");
      } finally {
        _autoSaving = false;
        if (autoCB) autoCB.disabled = false;
        if (autoThr) autoThr.disabled = false;
      }
    };
    const loadSavedAuto = async () => {
      try {
        const s = nativeBridgeUp() ? await bridgeCall("settings_load") : null;
        if (s) {
          if (autoCB) autoCB.checked = !!s.auto_index_enabled;
          if (autoThr) autoThr.value = String(s.auto_index_threshold || 10);
          _autoSaved = {
            enabled: !!s.auto_index_enabled,
            threshold: Number(s.auto_index_threshold) || 10,
          };
        }
      } catch (e) {
        console.warn("auto-index settings load failed:", e);
      }
    };
    loadSavedAuto();
    autoCB?.addEventListener("change", persistAuto);
    autoThr?.addEventListener("blur", persistAuto);
    autoThr?.addEventListener("keydown", (e) => {
      if (e.key === "Enter") { e.preventDefault(); autoThr.blur(); }
    });

    renderRoots();
    window.addEventListener("archive-roots-changed", renderRoots);
    // Re-fetch once pywebview is ready (initial calls may have fired before
    // the bridge was live).
    window.addEventListener("pywebviewready", () => {
      renderRoots();
      loadSavedAuto();
    });
    setTimeout(() => {
      if (nativeBridgeUp()) { renderRoots(); loadSavedAuto(); }
    }, 800);
  }
  window._initSettingsArchiveRoots = initSettingsArchiveRoots;

  window.initSettingsSubTabs = initSettingsSubTabs;
  window.initSettingsArchiveRoots = initSettingsArchiveRoots;
})();
