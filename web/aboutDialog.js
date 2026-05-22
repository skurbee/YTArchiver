/**
 * web/aboutDialog.js — About dialog — version + links + credits
 *
 * Exposed as window.initAboutDialog; app.js boot calls it once.
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
  function bridgeCall(method, ...args) {
    const fn = window.YT?.bridge?.bridgeCall;
    if (fn) return fn(method, ...args);
    return undefined;
  }

  // ─── About dialog ────────────────────────────────────────────────────
  function initAboutDialog() {
    const bd = document.getElementById("about-backdrop");
    const openBtn = document.getElementById("btn-about");
    const closeBtn = document.getElementById("about-close");
    const body = document.getElementById("about-body");
    if (!bd) return;
    const show = async () => {
      bd.style.display = "flex";
      const api = window.pywebview?.api;
      if (!api?.about_info) {
        body.textContent = "Native mode only.";
        return;
      }
      try {
        const info = await api.about_info();
        // UI audit fix: parent .askq-body sets `white-space: pre-wrap`
        // (so plain text askQuestion messages render with line breaks).
        // For this rich HTML About content that creates double-spacing
        // around every <div>. Override to `normal` and emit the markup
        // without inter-element whitespace. Result: rows sit at the
        // natural 1.5 line-height instead of ~2.5.
        body.innerHTML = (
          `<div style="line-height:1.45;white-space:normal;">`
            + `<div><strong>${escapeHtml(info.app_name)}</strong> `
            + `<span style="color:var(--c-dim)">${escapeHtml(info.app_version)}</span></div>`
            + `<div style="margin-top:10px;color:var(--c-dim);font-size:11.5px;">`
              + `<div>Channels subscribed: ${info.channels ?? "\u2014"}</div>`
              + `<div>yt-dlp: ${escapeHtml(info.ytdlp_version || "\u2014")}</div>`
              + `<div>Python: ${escapeHtml(info.python_version || "\u2014")}</div>`
              + `<div style="margin-top:8px;">Config: `
                + `<code style="font-size:11px;">${escapeHtml(info.config_path)}</code></div>`
              + `<div>Archive root: `
                + `<code style="font-size:11px;">${escapeHtml(info.output_dir || "\u2014")}</code></div>`
            + `</div>`
          + `</div>`);
      } catch (e) { body.textContent = "Error loading: " + e; }
    };
    const hide = () => { bd.style.display = "none"; };
    openBtn?.addEventListener("click", show);
    closeBtn?.addEventListener("click", hide);
    bd.addEventListener("click", (e) => { if (e.target === bd) hide(); });
    // Esc-to-close — matches every other modal in the app (audit:
    // aboutDialog H226). Only close when this modal is visible so
    // we don't intercept Esc for other open dialogs.
    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape" && bd.style.display !== "none") hide();
    });
  }

  window.initAboutDialog = initAboutDialog;
})();
