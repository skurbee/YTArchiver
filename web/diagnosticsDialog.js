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
  const integrityCategoryLabels = {
    activity_history: "Activity history",
    canonical_links: "Video library",
    folder_overrides: "Channel folders",
    inputs: "Required files",
    migration_state: "App data",
    recovery_records: "Interrupted work",
    same_title_collisions: "Video titles",
    saved_media: "Saved videos",
    transcript_agreement: "Transcripts",
    fts: "Search index",
    transcript_fts: "Transcript search",
    video_title_fts: "Video-title search",
  };

  // ─── Diagnostics dialog ──────────────────────────────────────────────
  function initDiagnosticsDialog() {
    const bd = document.getElementById("diag-backdrop");
    const openBtn = document.getElementById("btn-diagnostics");
    const closeBtn = document.getElementById("diag-close");
    const refreshBtn = document.getElementById("diag-refresh");
    const integrityBtn = document.getElementById("diag-integrity");
    const rowsEl = document.getElementById("diag-rows");
    const summaryEl = document.getElementById("diag-summary");
    const integrityEl = document.getElementById("diag-integrity-results");
    if (!bd) return;

    function showIntegrityMessage(message, className) {
      if (!integrityEl) return;
      integrityEl.hidden = false;
      integrityEl.replaceChildren();
      const row = document.createElement("div");
      row.className = className || "diag-integrity-notice";
      row.textContent = message;
      integrityEl.appendChild(row);
    }

    function renderIntegrity(result) {
      if (!integrityEl) return;
      integrityEl.hidden = false;
      integrityEl.replaceChildren();

      const notice = document.createElement("div");
      notice.className = "diag-integrity-notice";
      notice.textContent = result?.backup_notice
        || "Read-only preview. Export and verify a full backup before applying any proposed repair.";
      integrityEl.appendChild(notice);

      if (!result?.preview_only) {
        showIntegrityMessage("The check didn't return any results.", "diag-integrity-error");
        return;
      }
      if (result.error) {
        const error = document.createElement("div");
        error.className = "diag-integrity-error";
        error.textContent = result.error;
        integrityEl.appendChild(error);
        return;
      }

      const issues = Array.isArray(result.issues) ? result.issues : [];
      const heading = document.createElement("div");
      heading.className = "diag-integrity-heading";
      heading.textContent = issues.length
        ? `${issues.length} proposed repair${issues.length === 1 ? "" : "s"} — nothing changed`
        : "No integrity problems found — nothing changed";
      integrityEl.appendChild(heading);

      for (const issue of issues) {
        const item = document.createElement("div");
        item.className = "diag-integrity-item diag-integrity-"
          + (issue.severity === "error" ? "error" : "warning");
        const title = document.createElement("div");
        title.className = "diag-integrity-title";
        const category = integrityCategoryLabels[issue.category]
          || String(issue.category || "Archive check").replaceAll("_", " ");
        title.textContent = `${category}: ${issue.subject || "item"}`;
        const detail = document.createElement("div");
        detail.className = "diag-integrity-detail";
        detail.textContent = issue.detail || "A stored record needs review.";
        const repair = document.createElement("div");
        repair.className = "diag-integrity-repair";
        repair.textContent = `Proposed repair: ${issue.proposed_repair || "Review manually."}`;
        item.append(title, detail, repair);
        integrityEl.appendChild(item);
      }
    }

    async function run() {
      // early bail if rowsEl is missing (DOM out of sync
      // during hot reload, partial render, etc). Old code hit a
      // TypeError on rowsEl.innerHTML and the dialog never opened.
      if (!rowsEl) return;
      rowsEl.innerHTML = '<div class="browse-empty askq-empty-padded">Checking\u2026</div>';
      if (summaryEl) summaryEl.textContent = "";
      if (!nativeBridgeUp()) {
        rowsEl.innerHTML = '<div class="browse-empty askq-empty-padded">YTArchiver isn\'t ready yet. Try again in a moment.</div>';
        return;
      }
      try {
        const res = await bridgeCall("diagnostics_run");
        if (!res?.ok || !Array.isArray(res.rows)) {
          rowsEl.innerHTML = '<div class="browse-empty askq-empty-padded">The checks could not finish.</div>';
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

    async function runIntegrityPreview() {
      if (!integrityBtn || !integrityEl) return;
      if (!nativeBridgeUp()) {
        showIntegrityMessage("YTArchiver isn't ready yet. Try again in a moment.", "diag-integrity-error");
        return;
      }
      const proceed = askConfirm
        ? await askConfirm(
          "Preview archive integrity",
          "This read-only scan may take a while on a large archive. It lists recommended repairs but does not change any files. Continue?",
          { confirm: "Run preview" },
        )
        : true;
      if (!proceed) return;

      integrityBtn.disabled = true;
      const oldLabel = integrityBtn.textContent;
      integrityBtn.textContent = "Scanning…";
      showIntegrityMessage(
        "Checking archive files, library data, queues, transcripts, and Search…",
      );
      try {
        const result = await bridgeCall("integrity_scan_preview");
        renderIntegrity(result);
        if (summaryEl && result?.summary) {
          const count = Number(result.summary.issues || 0);
          summaryEl.textContent = count
            ? `${count} integrity item${count === 1 ? "" : "s"} to review`
            : "Integrity preview clean";
        }
      } catch (error) {
        showIntegrityMessage(
          `Integrity preview failed: ${String(error)}`,
          "diag-integrity-error",
        );
      } finally {
        integrityBtn.disabled = false;
        integrityBtn.textContent = oldLabel;
      }
    }

    const show = () => { bd.hidden = false; run(); };
    const hide = () => { bd.hidden = true; };
    openBtn?.addEventListener("click", show);
    closeBtn?.addEventListener("click", hide);
    refreshBtn?.addEventListener("click", run);
    integrityBtn?.addEventListener("click", runIntegrityPreview);
    bd.addEventListener("click", (e) => { if (e.target === bd) hide(); });
    // BUG FIX 2026-05-15 (audit): Esc was a no-op on this dialog. Wire
    // it through to match the rest of the modal system.
    window.YT?.modals?.registerEscapeClose?.(bd, hide);
  }

  window.initDiagnosticsDialog = initDiagnosticsDialog;
})();
