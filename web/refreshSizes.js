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
      window._showToast?.("Rescanning archive folder sizes…", "ok");
      try {
        await bridgeCall("archive_rescan");
        // After rescan completes, refresh the Subs table so totals update.
        const subsData = await bridgeCall("get_subs_channels");
        if (Array.isArray(subsData) && subsData.length === 2) {
          window.renderSubsTable(subsData[0], subsData[1]);
        }
      } catch (e) { window._showToast?.("Rescan failed.", "error"); }
    });
  }

  window.initRefreshSizesClick = initRefreshSizesClick;
})();
