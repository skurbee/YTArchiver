/* ═══════════════════════════════════════════════════════════════════════
   settingsInfra.js — Settings tab sub-tab navigation + Archive Roots panel

   Extracted from indexControls.js (Patch 15, v71.7).

   Two related Settings-tab plumbing functions:
     • initSettingsSubTabs — wires the sub-navigation buttons
       (General / Performance / Appearance / Tools / Metadata / Index)
       that switch which inner view is visible inside the Settings panel.
     • initSettingsArchiveRoots — Archive Roots list inside Settings →
       Index, plus the Auto-update-every-N controls and the right-click
       "Delete all transcriptions for this root" action.

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

  const askConfirm = window.askConfirm;
  const askDanger = window.askDanger;
  const askQuestion = window.askQuestion;
  const askChoice = window.askChoice;
  const askTextInput = window.askTextInput;
  const showContextMenu = window.showContextMenu || (() => {});

  function initSettingsSubTabs() {
    const buttons = document.querySelectorAll(".settings-subnav-btn");
    const views = {
      general: document.getElementById("settings-view-general"),
      performance: document.getElementById("settings-view-performance"),
      appearance: document.getElementById("settings-view-appearance"),
      tools: document.getElementById("settings-view-tools"),
      metadata: document.getElementById("settings-view-metadata"),
      index: document.getElementById("settings-view-index"),
    };
    const saveFooter = document.getElementById("settings-actions-footer");
    if (!buttons.length) return;
    const show = (key) => {
      buttons.forEach(b => b.classList.toggle("active", b.dataset.settingsView === key));
      for (const k of Object.keys(views)) {
        if (views[k]) views[k].style.display = (k === key) ? "" : "none";
      }
      // Hide Save on Index + Metadata (both have their own actions
      // and no form fields). Show on everything else.
      if (saveFooter) {
        saveFooter.style.display = (key === "index" || key === "metadata") ? "none" : "";
      }
      if (key === "index") {
        // Pull fresh numbers every visit — stale "Loading…" was showing
        // up after first paint because the initial fetch was racing boot.
        if (typeof window._refreshIndexStats === "function") {
          window._refreshIndexStats();
        }
      }
      if (key === "metadata") {
        // Pull fresh per-channel refresh timestamps every visit.
        if (typeof window._refreshMetadataTab === "function") {
          window._refreshMetadataTab();
        }
      }
    };
    buttons.forEach(b => {
      b.addEventListener("click", () => show(b.dataset.settingsView || "general"));
    });
  }

  // Settings > Index sub-tab: Archive Roots list + Auto-update-every-N +
  // delete-all-transcriptions right-click menu. Moved here from the
  // Browse > Index view per the user's request 2026-04-18.
  function initSettingsArchiveRoots() {
    const getApi = () => window.pywebview?.api;
    const rootsList = document.getElementById("settings-roots-list");
    const bAdd = document.getElementById("btn-settings-add-root");
    const bRemove = document.getElementById("btn-settings-remove-root");
    const autoCB = document.getElementById("settings-auto-index-enabled");
    const autoThr = document.getElementById("settings-auto-index-threshold");
    if (!rootsList && !autoCB) return;

    let _selectedRoot = null;

    const _confirmDeleteAllTranscriptions = async (folder) => {
      const api = getApi();
      if (!api?.index_count_transcripts || !api?.index_delete_all_transcripts) {
        window._showToast?.("Native mode required.", "warn");
        return;
      }
      window._showToast?.("Counting transcript files\u2026", "ok");
      const count = await api.index_count_transcripts(folder);
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
        "Delete all transcriptions?",
        `This will permanently delete ALL transcript files under:\n\n${folder}\n\n` +
        ` \u2022 ${txt_count.toLocaleString()} transcript .txt file(s)\n` +
        ` \u2022 ${jsonl_count.toLocaleString()} hidden .jsonl file(s)\n\n` +
        `Total: ${total.toLocaleString()} file(s), ${mb} MB\n\n` +
        "This cannot be undone. Are you sure?",
        "Continue");
      if (!ok1) return;
      const ok2 = await window.askDanger(
        "Final confirmation",
        `Are you absolutely sure?\n\nAll ${total.toLocaleString()} transcript ` +
        "file(s) will be permanently deleted. The FTS search index will " +
        "also be cleared. Sync will have to re-run Whisper on every video.",
        "Yes, DELETE EVERYTHING");
      if (!ok2) return;
      const res = await api.index_delete_all_transcripts(folder, "YES-DELETE-ALL");
      if (res?.ok) {
        window._showToast?.("Transcripts deleted.", "warn");
      } else {
        window._showToast?.(res?.error || "Delete failed.", "error");
      }
    };

    const renderRoots = async () => {
      if (!rootsList) return;
      rootsList.innerHTML = "";
      let outDir = "";
      let extras = [];
      try {
        const s = await getApi()?.settings_load?.();
        outDir = (s?.output_dir || "").trim();
        extras = Array.isArray(s?.tp_archive_roots) ? s.tp_archive_roots : [];
      } catch (_e) {}
      const entries = [];
      if (outDir) entries.push({ path: outDir, auto: true });
      for (const r of extras) if (r) entries.push({ path: r, auto: false });
      for (const e of entries) {
        const row = document.createElement("div");
        row.className = "root-entry" + (e.auto ? " auto" : "");
        row.dataset.path = e.path;
        row.textContent = e.auto ? `[auto] ${e.path}` : e.path;
        row.addEventListener("click", () => {
          rootsList.querySelectorAll(".root-entry.selected")
            .forEach(el => el.classList.remove("selected"));
          row.classList.add("selected");
          _selectedRoot = e.path;
        });
        row.addEventListener("contextmenu", (ev) => {
          ev.preventDefault();
          rootsList.querySelectorAll(".root-entry.selected")
            .forEach(el => el.classList.remove("selected"));
          row.classList.add("selected");
          _selectedRoot = e.path;
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
      const api = getApi();
      if (!api?.pick_folder) {
        window._showToast?.("Native mode required.", "warn");
        return;
      }
      const res = await api.pick_folder("Select Archive Root Folder");
      if (!res?.ok || !res.path) return;
      try {
        const s = await api.settings_load();
        const extras = Array.isArray(s?.tp_archive_roots) ? s.tp_archive_roots : [];
        if (extras.includes(res.path)) return;
        extras.push(res.path);
        await api.settings_save({ tp_archive_roots: extras });
        await renderRoots();
      } catch (e) {
        window._showToast?.("Add root failed: " + e, "error");
      }
    });

    bRemove?.addEventListener("click", async () => {
      const api = getApi();
      if (!_selectedRoot) {
        window._showToast?.("Pick a root to remove first.", "warn");
        return;
      }
      if (!api) return;
      try {
        const s = await api.settings_load();
        const outDir = (s?.output_dir || "").trim();
        if (_selectedRoot === outDir) {
          window._showToast?.(
            "The auto-detected output folder cannot be removed here. " +
            "Change the Archive root field above instead.", "warn");
          return;
        }
        const extras = (s?.tp_archive_roots || []).filter(r => r !== _selectedRoot);
        await api.settings_save({ tp_archive_roots: extras });
        _selectedRoot = null;
        await renderRoots();
      } catch (e) {
        window._showToast?.("Remove failed: " + e, "error");
      }
    });

    const persistAuto = async () => {
      const api = getApi();
      if (!api?.settings_save) return;
      const enabled = !!autoCB?.checked;
      let n = parseInt(autoThr?.value || "10", 10);
      if (!Number.isFinite(n) || n < 1) n = 10;
      if (n > 9999) n = 9999;
      if (autoThr) autoThr.value = String(n);
      try {
        await api.settings_save({
          auto_index_enabled: enabled,
          auto_index_threshold: n,
        });
      } catch (_e) {}
    };
    const loadSavedAuto = async () => {
      try {
        const s = await getApi()?.settings_load?.();
        if (s) {
          if (autoCB) autoCB.checked = !!s.auto_index_enabled;
          if (autoThr) autoThr.value = String(s.auto_index_threshold || 10);
        }
      } catch (_e) {}
    };
    loadSavedAuto();
    autoCB?.addEventListener("change", persistAuto);
    autoThr?.addEventListener("blur", persistAuto);
    autoThr?.addEventListener("keydown", (e) => {
      if (e.key === "Enter") { e.preventDefault(); persistAuto(); }
    });

    renderRoots();
    // Re-fetch once pywebview is ready (initial calls may have fired before
    // the bridge was live).
    window.addEventListener("pywebviewready", () => {
      renderRoots();
      loadSavedAuto();
    });
    setTimeout(() => {
      if (getApi()?.settings_load) { renderRoots(); loadSavedAuto(); }
    }, 800);
  }
  window._initSettingsArchiveRoots = initSettingsArchiveRoots;

  window.initSettingsSubTabs = initSettingsSubTabs;
  window.initSettingsArchiveRoots = initSettingsArchiveRoots;
})();
