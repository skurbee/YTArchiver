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
  function nativeBridgeUp() {
    return !!window.YT?.bridge?.isUp?.();
  }

  // ─── Scan archive button (Browse tab toolbar) ───────────────────────
  function initScanArchive() {
    document.getElementById("btn-scan-archive")?.addEventListener("click", async () => {
      if (!nativeBridgeUp()) {
        window._showToast?.("Native mode required.", "warn");
        return;
      }
      // Confirm first \u2014 this is a heavy, DB-mutating pass (it prunes index
      // rows for files no longer on disk), and previously an accidental
      // click ran it silently with no prompt.
      const ok = await (askConfirm
        ? askConfirm(
            "Rescan archive?",
            "Walks every channel folder for files added or removed outside " +
            "the app and prunes index entries for videos no longer on disk. " +
            "Safe, but it rewrites index state \u2014 continue?",
            "Rescan")
        : Promise.resolve(true));
      if (!ok) return;
      try {
        const result = await bridgeCall("archive_rescan");
        if (result?.started) {
          window._showToast?.("Archive rescan started.", "ok");
        } else if (result?.already_running) {
          window._showToast?.("Archive rescan is already running.", "warn");
        } else {
          window._showToast?.(result?.error || "Rescan could not start.", "warn");
        }
      } catch (e) {
        window._showToast?.(`Rescan failed to start: ${e}`, "error");
      }
    });
  }

  window.initScanArchive = initScanArchive;
})();
