/**
 * web/logMode.js — Log mode dropdown — Simple vs Verbose log toggle
 */
(function () {
  "use strict";

  // ─── Log mode dropdown (Simple / Verbose) ────────────────────────────
  // Matches YTArchiver's ttk.Combobox with values=["Simple","Verbose"].
  function initLogMode() {
    // The log-mode dropdown now lives on the Settings tab as
    // `settings-log-mode`; keep listening to either id so changes made
    // via Settings propagate into `document.body.dataset.logMode`
    // (CSS rules use it to hide/show verbose-only rows).
    const sel = document.getElementById("log-mode-select")
              || document.getElementById("settings-log-mode");
    if (!sel) return;
    document.body.dataset.logMode = sel.value || "Simple";
    sel.addEventListener("change", (e) => {
      const mode = e.target.value;
      document.body.dataset.logMode = mode;
      // Settings owns persistence through settings_save(). Keeping this
      // listener presentation-only avoids two competing config writes for a
      // single dropdown change. The backend hot-applies log filtering as part
      // of that settings_save call.
    });
  }

  window.initLogMode = initLogMode;
})();
