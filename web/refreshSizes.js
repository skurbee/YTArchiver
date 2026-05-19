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

  function initRefreshSizesClick() {
    const totalEl = document.getElementById("subs-total-size");
    if (!totalEl) return;
    totalEl.addEventListener("click", async () => {
      const api = window.pywebview?.api;
      if (!api?.archive_rescan) return;
      const ok = await (window.askConfirm
        ? window.askConfirm("Refresh sizes",
            "Rescan all channel folder sizes?\n\nThis walks every channel folder on disk " +
            "and can take a minute or two on large archives.",
            { confirm: "Rescan" })
        : Promise.resolve(confirm("Rescan all channel folder sizes?")));
      if (!ok) return;
      window._showToast?.("Rescanning archive folder sizes…", "ok");
      try {
        await api.archive_rescan();
        // After rescan completes, refresh the Subs table so totals update.
        if (api.get_subs_channels) {
          const subsData = await api.get_subs_channels();
          if (Array.isArray(subsData) && subsData.length === 2) {
            window.renderSubsTable(subsData[0], subsData[1]);
          }
        }
      } catch (e) { window._showToast?.("Rescan failed.", "error"); }
    });
  }

  window.initRefreshSizesClick = initRefreshSizesClick;
})();
