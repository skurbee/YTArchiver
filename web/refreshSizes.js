/* ═══════════════════════════════════════════════════════════════════════
   refreshSizes.js — clickable "Total: N TB" label triggers archive rescan

   Extracted from app.js boot(). The Subs tab footer shows a running
   "Total: N TB" label; clicking it confirms and kicks off a fresh
   archive_rescan to walk every channel folder and refresh sizes.

   Publishes:
     window.initRefreshSizesClick()

   Reads:
     window.pywebview.api.archive_rescan / get_subs_channels
     window.askConfirm, window._showToast
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

  function initRefreshSizesClick() {
    const totalEl = document.getElementById("subs-total-size");
    if (!totalEl) return;
    totalEl.addEventListener("click", async () => {
      if (!nativeBridgeUp()) return;
      const ok = await (window.askConfirm
        ? window.askConfirm("Refresh sizes",
            "Rescan all channel folder sizes?\n\nThis walks every channel folder on disk " +
            "and can take a minute or two on large archives.",
            { confirm: "Rescan" })
        : Promise.resolve(confirm("Rescan all channel folder sizes?")));
      if (!ok) return;
      try {
        const result = await bridgeCall("archive_rescan");
        if (result?.started) {
          window._showToast?.("Rescanning archive folder sizes…", "ok");
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

  window.initRefreshSizesClick = initRefreshSizesClick;
})();
