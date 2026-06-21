/**
 * web/autorunHistory.js — Autorun history dialog — full-view modal of past auto-sync runs
 */
(function () {
  "use strict";

  const escapeHtml = window.YT?.util?.escapeHtml || ((s) => String(s ?? ""));
  function bridgeCall(method, ...args) {
    const fn = window.YT?.bridge?.bridgeCall;
    if (fn) return fn(method, ...args);
    return undefined;
  }
  function nativeBridgeUp() {
    return !!window.YT?.bridge?.isUp?.();
  }

  // ─── Autorun history full-view dialog ────────────────────────────────
  function initAutorunHistoryDialog() {
    const backdrop = document.getElementById("autorun-history-backdrop");
    const cancelBtn = document.getElementById("autorun-history-cancel");
    const exportBtn = document.getElementById("autorun-history-export");
    if (!backdrop) return;

    const show = async () => {
      backdrop.hidden = false;
      await refreshHistory();
    };
    const hide = () => { backdrop.hidden = true; };

    async function refreshHistory() {
      if (!nativeBridgeUp()) return;
      try {
        const hist = await bridgeCall("get_activity_log_history");
        const entries = Array.isArray(hist) ? hist : [];
        const body = document.getElementById("autorun-history-entries");
        const count = document.getElementById("autorun-history-count");
        if (count) count.textContent = `${entries.length} entries`;
        if (!body) return;
        window._autorunHistoryRaw = entries;
        _paintAutorunHistory(entries, "");
      } catch (e) {
        console.warn("autorun hist:", e);
        const body = document.getElementById("autorun-history-entries");
        const count = document.getElementById("autorun-history-count");
        if (count) count.textContent = "load failed";
        if (body) {
          body.innerHTML = `<div class="browse-empty askq-empty-padded askq-empty-danger">`
            + `Could not load autorun history: ${escapeHtml(String(e))}</div>`;
        }
        window._showToast?.(`Could not load autorun history: ${e}`, "warn");
      }
    }

    document.getElementById("autorun-history-filter")?.addEventListener("input", (e) => {
      _paintAutorunHistory(window._autorunHistoryRaw || [], e.target.value || "");
    });

    cancelBtn?.addEventListener("click", hide);
    backdrop.addEventListener("click", (e) => { if (e.target === backdrop) hide(); });
    window.YT?.modals?.registerEscapeClose?.(backdrop, hide);

    // Autorun-history viewer kept wired up for any remaining callers
    // (tray menu, etc.) but no longer reachable from the Clear button
    // menu per 2026-04-21 request — only the two clear actions in
    // the dropdown, no "view full history" entry.
    window._openAutorunHistory = show;

    exportBtn?.addEventListener("click", async () => {
      const entries = window._autorunHistoryRaw || [];
      if (!entries.length) {
        window._showToast?.("No autorun history to export.", "warn");
        return;
      }
      // Flatten entries (segments joined) into a single plain-text column
      const lines = entries.map(ent => {
        if (typeof ent === "string") return ent;
        const segs = ent.segments || [];
        return segs.map(s => (s && s[0]) || "").join("").trim();
      });
      const csvCell = (value) => `"${String(value ?? "")
        .replace(/\r\n/g, "\n")
        .replace(/\r/g, "\n")
        .replace(/"/g, '""')}"`;
      const csv = "\ufeffentry\r\n" + lines.map(csvCell).join("\r\n");
      if (nativeBridgeUp()) {
        const r = await bridgeCall("save_text_to_file", "autorun_history.csv", csv);
        if (r?.ok) window._showToast?.("Saved.", "ok");
      } else {
        const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url; a.download = "autorun_history.csv"; a.click();
        setTimeout(() => URL.revokeObjectURL(url), 1000);
      }
    });
  }

  function _paintAutorunHistory(entries, filter) {
    const body = document.getElementById("autorun-history-entries");
    if (!body) return;
    body.innerHTML = "";
    const q = (filter || "").toLowerCase().trim();
    const matches = [];
    for (const ent of entries) {
      const text = typeof ent === "string"
        ? ent
        : (ent.segments || []).map(s => s && s[0] || "").join("");
      if (q && !text.toLowerCase().includes(q)) continue;
      matches.push({ text, ent });
    }
    if (!matches.length) {
      body.innerHTML = '<div class="askq-empty-padded askq-empty-muted">No matching entries.</div>';
      return;
    }
    const frag = document.createDocumentFragment();
    for (const { text } of matches) {
      const row = document.createElement("div");
      row.className = "ah-row autorun-history-row";
      row.textContent = text;
      frag.appendChild(row);
    }
    body.appendChild(frag);
  }

  window.initAutorunHistoryDialog = initAutorunHistoryDialog;
})();
