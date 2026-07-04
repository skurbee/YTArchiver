/**
 * web/diagnosticsDialog.js — Diagnostics modal (dep status + paths + logs).
 *
 * Exposed as window.initDiagnosticsDialog; app.js boot calls it once.
 */
(function () {
  "use strict";

  const askConfirm = window.askConfirm;
  function bridgeCall(method, ...args) {
    const fn = window.YT?.bridge?.bridgeCall;
    if (fn) return fn(method, ...args);
    return undefined;
  }
  function nativeBridgeUp() {
    return !!window.YT?.bridge?.isUp?.();
  }
  // Local escaper for the error path below. This IIFE never imported one,
  // so `escapeHtml(...)` in the catch branch threw ReferenceError and the
  // error UI silently broke. Bind the canonical helper with a hard fallback.
  const escapeHtml = window.YT?.util?.escapeHtml || window._escapeHtml
    || (s => String(s ?? ""));

  // ─── Diagnostics dialog ──────────────────────────────────────────────
  function initDiagnosticsDialog() {
    const bd = document.getElementById("diag-backdrop");
    const openBtn = document.getElementById("btn-diagnostics");
    const closeBtn = document.getElementById("diag-close");
    const refreshBtn = document.getElementById("diag-refresh");
    const rowsEl = document.getElementById("diag-rows");
    const summaryEl = document.getElementById("diag-summary");
    if (!bd) return;

    async function run() {
      // early bail if rowsEl is missing (DOM out of sync
      // during hot reload, partial render, etc). Old code hit a
      // TypeError on rowsEl.innerHTML and the dialog never opened.
      if (!rowsEl) return;
      rowsEl.innerHTML = '<div class="browse-empty askq-empty-padded">Running self-check\u2026</div>';
      if (summaryEl) summaryEl.textContent = "";
      if (!nativeBridgeUp()) {
        rowsEl.innerHTML = '<div class="browse-empty askq-empty-padded">Native mode required.</div>';
        return;
      }
      try {
        const res = await bridgeCall("diagnostics_run");
        if (!res?.ok || !Array.isArray(res.rows)) {
          rowsEl.innerHTML = '<div class="browse-empty askq-empty-padded">Self-check failed.</div>';
          return;
        }
        const frag = document.createDocumentFragment();
        let okN = 0, warnN = 0, failN = 0;
        for (const r of res.rows) {
          const row = document.createElement("div");
          const status = (r.status === "warning")
            ? "warning"
            : (r.ok ? "ok" : "fail");
          row.className = "diag-row diag-" + status;
          row.innerHTML = `
            <span class="diag-dot"></span>
            <span class="diag-name"></span>
            <span class="diag-detail"></span>
          `;
          row.querySelector(".diag-name").textContent = r.name;
          row.querySelector(".diag-detail").textContent = r.detail || "";
          frag.appendChild(row);
          if (status === "warning") warnN++;
          else if (status === "fail") failN++;
          else okN++;
        }
        rowsEl.innerHTML = "";
        rowsEl.appendChild(frag);
        if (failN > 0) {
          summaryEl.textContent = warnN > 0
            ? `${okN} ok - ${warnN} warning${warnN === 1 ? "" : "s"} - ${failN} problem${failN === 1 ? "" : "s"}`
            : `${okN} ok - ${failN} problem${failN === 1 ? "" : "s"}`;
        } else if (warnN > 0) {
          summaryEl.textContent = `${okN} ok - ${warnN} warning${warnN === 1 ? "" : "s"}`;
        } else {
          summaryEl.textContent = `All ${okN} checks passed`;
        }
      } catch (e) {
        rowsEl.innerHTML = `<div class="browse-empty askq-empty-padded">Error: ${escapeHtml(String(e))}</div>`;
      }
    }

    const show = () => { bd.hidden = false; run(); };
    const hide = () => { bd.hidden = true; };
    openBtn?.addEventListener("click", show);
    closeBtn?.addEventListener("click", hide);
    refreshBtn?.addEventListener("click", run);
    bd.addEventListener("click", (e) => { if (e.target === bd) hide(); });
    // BUG FIX 2026-05-15 (audit): Esc was a no-op on this dialog. Wire
    // it through to match the rest of the modal system.
    window.YT?.modals?.registerEscapeClose?.(bd, hide);
  }

  window.initDiagnosticsDialog = initDiagnosticsDialog;
})();
