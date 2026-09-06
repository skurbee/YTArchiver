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
    if (!bd || bd.dataset.diagnosticsReady) return;
    bd.dataset.diagnosticsReady = "true";
    if (refreshBtn) {
      refreshBtn.textContent = "Recheck dependencies";
      refreshBtn.title = "Check installed tools and app dependencies. This does not scan your archive.";
    }
    if (integrityBtn) integrityBtn.textContent = "Deep archive check…";
    const background = document.createElement("button");
    background.type = "button";
    background.className = "integrity-background-status";
    background.hidden = true;
    document.body.appendChild(background);
    let scanState = null;
    let pollTimer = null;
    let polling = false;
    let viewedJob = null;
    const elapsed = seconds => {
      const value = Math.max(0, Math.floor(Number(seconds) || 0));
      return `${Math.floor(value / 60)}m ${String(value % 60).padStart(2, "0")}s`;
    };
    function updateBackground() {
      const running = !!scanState?.running;
      background.hidden = !bd.hidden || !scanState?.job_id
        || (!running && viewedJob === scanState.job_id);
      background.textContent = running
        ? `Deep check: ${scanState.cancel_requested ? "stopping" : scanState.phase} · ${elapsed(scanState.elapsed_seconds)} — View`
        : "Deep archive check finished — View results";
      if (closeBtn) closeBtn.textContent = running ? "Close — continues in background" : "Close";
      if (integrityBtn) {
        integrityBtn.disabled = running;
        integrityBtn.textContent = running ? "Deep check running…" : "Deep archive check…";
      }
    }

    function renderProgress() {
      let cancel = integrityEl.querySelector("#diag-integrity-cancel");
      if (!cancel) {
        showIntegrityMessage("");
        integrityEl.firstElementChild.setAttribute("role", "status");
        const note = document.createElement("div");
        note.className = "diag-integrity-notice";
        note.textContent = "Read-only check. Closing this window continues the check in the background. Large Search indexes can take several minutes.";
        cancel = document.createElement("button");
        cancel.type = "button";
        cancel.className = "btn";
        cancel.id = "diag-integrity-cancel";
        integrityEl.append(note, cancel);
        cancel.addEventListener("click", async () => {
          cancel.disabled = true;
          try {
            const result = await bridgeCall("integrity_scan_cancel", scanState.job_id);
            if (!result?.ok) throw new Error(result?.error || "Could not request cancellation");
            scanState.cancel_requested = true;
            renderProgress();
            updateBackground();
          } catch (error) {
            cancel.disabled = false;
            note.textContent = `Cancellation could not be confirmed: ${String(error)}. The check may still be running.`;
          }
        });
      }
      integrityEl.firstElementChild.textContent = `${scanState.cancel_requested ? "Stopping after the current read…" : scanState.phase}`
        + ` · ${Number(scanState.completed || 0).toLocaleString()} ${scanState.unit || "items checked"}`
        + ` · ${elapsed(scanState.elapsed_seconds)} elapsed`;
      cancel.textContent = scanState.cancel_requested ? "Stopping…" : "Cancel deep check";
      cancel.disabled = !!scanState.cancel_requested;
    }

    async function pollScan() {
      if (polling) return;
      polling = true;
      try {
        const state = await bridgeCall("integrity_scan_state");
        if (!state?.ok) throw new Error(state?.error || "Status is unavailable");
        if (state.job_id) scanState = state;
        if (scanState?.running) renderProgress();
        else if (scanState?.result) {
          renderIntegrity(scanState.result);
          if (!bd.hidden) viewedJob = scanState.job_id;
        }
        updateBackground();
      } catch (error) {
        const message = `Deep check status is temporarily unavailable: ${String(error)}. The check may still be running.`;
        if (scanState?.running) {
          renderProgress();
          integrityEl.children[1].textContent = message;
        } else {
          showIntegrityMessage(message, "diag-integrity-error");
        }
      } finally {
        polling = false;
        if (scanState?.running) pollTimer = setTimeout(pollScan, 1000);
      }
    }

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
          summaryEl.textContent = `All ${okN} dependency checks passed`;
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
          "Deep archive check",
          "This read-only check can take several minutes on a large archive. It compares saved videos, transcripts, and Search data, and lists recommended repairs without changing files. You can cancel it or close this window to continue in the background.",
          { confirm: "Start deep check" },
        )
        : true;
      if (!proceed) return;

      integrityBtn.disabled = true;
      integrityBtn.textContent = "Starting…";
      showIntegrityMessage("Starting deep archive check…");
      try {
        const result = await bridgeCall("integrity_scan_start");
        if (!result?.ok) throw new Error(result?.error || "Could not start the check");
        scanState = { job_id: result.job_id, running: true, phase: "Starting deep archive check" };
        clearTimeout(pollTimer);
        await pollScan();
      } catch (error) {
        showIntegrityMessage(
          `Integrity preview failed: ${String(error)}`,
          "diag-integrity-error",
        );
      } finally {
        updateBackground();
      }
    }

    const show = () => {
      bd.hidden = false;
      if (!scanState?.running) viewedJob = scanState?.job_id;
      updateBackground();
      run();
      clearTimeout(pollTimer);
      pollScan();
    };
    const hide = () => { bd.hidden = true; updateBackground(); };
    background.addEventListener("click", show);
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
