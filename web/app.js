/* ═══════════════════════════════════════════════════════════════════════
   app.js — YTArchiver pywebview UI bootstrap + tab init orchestrator

   After the great extraction pass, app.js is no longer "the single
   frontend script." It's the boot sequence: it waits for the DOM,
   then calls each feature module's init function in the right order.
   Almost every piece of rendering, dialog, and feature behavior lives
   in a dedicated file under web/. See docs/PROJECT_MAP.md for the
   full module index.

   What's still here:
     • The IIFE wrapper + a small observer pool so MutationObservers
       created during boot get cleaned up on `beforeunload`.
     • boot() — calls every feature module's init in dependency order
       and exposes _trackObserver to modules that need it.
     • DOMContentLoaded / setTimeout(boot,0) launcher.

   What moved (representative — full list in PROJECT_MAP.md):
     • Log rendering        → logs.js
     • Watch view + karaoke → watchView.js
     • Channel/Video grids  → browseGrids.js
     • Subs + Recent tables → tables.js
     • Queue popover rows   → queueRender.js
     • Settings tab         → settingsTab.js, indexControls.js, …
     • Modals / toasts      → modals.js, toasts.js, appDialogs.js
     • Tooltips + defocus   → uxPolish.js
     • Remove-channel flow  → removeChannel.js
     • Queue Pending button → queuePending.js
     • Small inits          → smallInits.js
   ═══════════════════════════════════════════════════════════════════════ */

(function () {
  "use strict";

  // Observer pool — every MutationObserver created during boot is
  // tracked here so we can disconnect them all on app shutdown
  // (pywebview close → `beforeunload`). The old code created observers
  // and never disconnected — small leak per session, but it stacked
  // if the page reloaded.
  const _ObserverPool = new Set();
  function _trackObserver(obs) { _ObserverPool.add(obs); return obs; }
  window.addEventListener("beforeunload", () => {
    for (const obs of _ObserverPool) {
      try { obs.disconnect(); } catch (_e) {}
    }
    _ObserverPool.clear();
  });
  // Expose so feature modules (queuePending.js etc.) can attach their
  // own MutationObservers to the same cleanup pool.
  window._trackBootObserver = _trackObserver;

  function boot() {
    // Boot order matters: foundation (tabs, defocus, tooltips, splitter)
    // → tab-specific wirings → bridge-dependent things (seedLogs last).
    // Each line is wrapped in try/catch so one broken module doesn't
    // brick the rest of the UI.
    const _safe = (name, fn) => {
      try {
        fn();
      } catch (e) {
        if (typeof window._reportBootIssue === "function") {
          window._reportBootIssue(name, e, { level: "error" });
        } else {
          console.error(name + ":", e);
        }
      }
    };
    const _expectedBootFns = [
      "initTabs",
      "initGlobalDefocus",
      "initCustomTooltips",
      "initSplitter",
      "initLogMode",
      "initRefreshSizesClick",
      "initBrowseSubmodes",
      "initBrowseSubmodeContent",
      "initBrowseContextMenus",
      "initSearchView",
      "initGraphView",
      "initBookmarksExport",
      "initEditChannelPanel",
      "initSubsContextMenu",
      "initSubsFilter",
      "initSyncButton",
      "initColumnSort",
      "initQueuePendingButton",
      "initSettingsTab",
      "initSettingsSubTabs",
      "initSettingsArchiveRoots",
      "initIndexControls",
      "initMetadataTab",
      "initScanArchive",
      "initAutorun",
      "initAutorunHistoryDialog",
      "initManualTranscribe",
      "initAboutDialog",
      "initDiagnosticsDialog",
      "initDriftScanDialog",
      "initCompressDryRunDialog",
      "initRepairCaptionsDialog",
      "initPunctRestoreDialog",
      "initProvenanceDialog",
      "initQueueModals",
      "initWatchActions",
      "initLogContextMenu",
      "initUrlField",
      "initDragDropUrl",
      "initClearLog",
      "initQueueBlink",
      "initStatusBar",
      "initCommandPalette",
      "initQueueAutoCheckboxes",
      "initDeferredLivestreams",
      "initKeyboardShortcuts",
      "initLastSyncTicker",
      "persistSplitterOnResize",
      "persistColumnWidths",
      "seedLogs",
    ];
    for (const fnName of _expectedBootFns) {
      if (typeof window[fnName] !== "function") {
        window._reportBootIssue?.(
          fnName,
          `${fnName} is not loaded; related UI controls may not work.`,
        );
      }
    }

    // Foundation
    _safe("initTabs",            () => window.initTabs?.());
    _safe("initGlobalDefocus",   () => window.initGlobalDefocus?.());
    _safe("initCustomTooltips",  () => window.initCustomTooltips?.());
    _safe("initSplitter",        () => window.initSplitter?.());
    _safe("initLogMode",         () => window.initLogMode?.());
    _safe("initRefreshSizesClick", () => window.initRefreshSizesClick?.());

    // Browse tab
    _safe("initBrowseSubmodes",       () => window.initBrowseSubmodes?.());
    _safe("initBrowseSubmodeContent", () => window.initBrowseSubmodeContent?.());
    _safe("initBrowseContextMenus",   () => window.initBrowseContextMenus?.());
    _safe("initSearchView",           () => window.initSearchView?.());
    _safe("initGraphView",            () => window.initGraphView?.());
    _safe("initBookmarksExport",      () => window.initBookmarksExport?.());

    // Subs tab
    _safe("initEditChannelPanel", () => window.initEditChannelPanel?.());
    _safe("initSubsContextMenu",  () => window.initSubsContextMenu?.());
    _safe("initSubsFilter",       () => window.initSubsFilter?.());
    _safe("initSyncButton",       () => window.initSyncButton?.());
    _safe("initColumnSort",       () => window.initColumnSort?.());
    _safe("initQueuePendingButton", () => window.initQueuePendingButton?.(_trackObserver));

    // Settings tab + sub-tabs
    _safe("initSettingsTab",         () => window.initSettingsTab?.());
    _safe("initSettingsSubTabs",     () => window.initSettingsSubTabs?.());
    _safe("initSettingsArchiveRoots", () => window.initSettingsArchiveRoots?.());
    _safe("initIndexControls",       () => window.initIndexControls?.());
    _safe("initMetadataTab",         () => window.initMetadataTab?.());
    _safe("initScanArchive",         () => window.initScanArchive?.());
    _safe("initAutorun",             () => window.initAutorun?.());
    _safe("initAutorunHistoryDialog", () => window.initAutorunHistoryDialog?.());
    _safe("initManualTranscribe",    () => window.initManualTranscribe?.());

    // Dialogs / popovers
    _safe("initAboutDialog",       () => window.initAboutDialog?.());
    _safe("initDiagnosticsDialog", () => window.initDiagnosticsDialog?.());
    _safe("initDriftScanDialog",   () => window.initDriftScanDialog?.());
    _safe("initCompressDryRunDialog", () => window.initCompressDryRunDialog?.());
    _safe("initRepairCaptionsDialog", () => window.initRepairCaptionsDialog?.());
    _safe("initPunctRestoreDialog", () => window.initPunctRestoreDialog?.());
    _safe("initProvenanceDialog",  () => window.initProvenanceDialog?.());
    _safe("initQueueModals",       () => window.initQueueModals?.());
    // Global status bar wraps renderQueues / setQueueState / _setIndicator,
    // so it must init AFTER queue + log modules have assigned those globals.
    _safe("initStatusBar",         () => window.initStatusBar?.());
    _safe("initCommandPalette",    () => window.initCommandPalette?.());
    _safe("initWatchActions",      () => window.initWatchActions?.());
    _safe("initLogContextMenu",    () => window.initLogContextMenu?.());

    // Toolbar buttons (header + downloads)
    _safe("initUrlField",       () => window.initUrlField?.());
    _safe("initDragDropUrl",    () => window.initDragDropUrl?.());
    _safe("initClearLog",       () => window.initClearLog?.());
    _safe("initQueueBlink",     () => window.initQueueBlink?.());
    _safe("initQueueAutoCheckboxes", () => window.initQueueAutoCheckboxes?.());
    _safe("initDeferredLivestreams", () => window.initDeferredLivestreams?.());

    // Global wirings
    _safe("initKeyboardShortcuts", () => window.initKeyboardShortcuts?.());
    _safe("initLastSyncTicker",    () => window.initLastSyncTicker?.());
    _safe("persistSplitterOnResize", () => window.persistSplitterOnResize?.());
    _safe("persistColumnWidths",   () => window.persistColumnWidths?.());

    // Bridge-dependent: pull initial state from Python. seedLogs() is
    // the only thing that needs the pywebview bridge to be ready.
    _safe("seedLogs", () => window.seedLogs?.());

    // Run the missing-folder reconcile after seed so the Subs table is
    // already populated when any Remove/Relocate actions happen.
    setTimeout(() => {
      if (typeof window._reconcileMissingFolders !== "function") {
        window._reportBootIssue?.(
          "_reconcileMissingFolders",
          "Missing-folder reconcile did not load; subscription folder warnings may be stale.",
        );
        return;
      }
      _safe("_reconcileMissingFolders",
            () => window._reconcileMissingFolders?.());
    }, 1500);
  }

  // DOMContentLoaded may have already fired — check readyState and run
  // immediately if so. Otherwise wait for it. The setTimeout(0) defer
  // when already-loaded gives the rest of this IIFE a tick to finish
  // setting up `window._trackBootObserver` before boot reads it.
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot, { once: true });
  } else {
    setTimeout(boot, 0);
  }
})();
