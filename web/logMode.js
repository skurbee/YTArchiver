/**
 * web/logMode.js — Log mode dropdown — Simple vs Verbose log toggle
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
      if (nativeBridgeUp()) {
        bridgeCall("set_log_mode", mode);
      }
    });
  }

  window.initLogMode = initLogMode;
})();
