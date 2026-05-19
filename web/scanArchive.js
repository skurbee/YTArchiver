/**
 * web/scanArchive.js — Scan archive button — manual rescan of disk roots
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
  const escapeHtml = window.YT?.util?.escapeHtml || ((s) => String(s ?? ""));
  function bridgeCall(method, ...args) {
    const fn = window.YT?.bridge?.bridgeCall;
    if (fn) return fn(method, ...args);
    return undefined;
  }

  // ─── Scan archive button (Browse tab toolbar) ───────────────────────
  function initScanArchive() {
    document.getElementById("btn-scan-archive")?.addEventListener("click", async () => {
      const api = window.pywebview?.api;
      if (!api?.archive_rescan) {
        window._showToast?.("Native mode required.", "warn");
        return;
      }
      await api.archive_rescan();
      window._showToast?.("Archive rescan started \u2014 check the log.", "ok");
    });
  }

  window.initScanArchive = initScanArchive;
})();
