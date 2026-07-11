/*
   tables.js - YTArchiver Subs tab table renderer.

   Publishes:
     window.renderSubsTable
     window._applySubsFilter
     window._applySubsAvgVisibility
*/
(function () {
  "use strict";

  const escapeHtml = window._escapeHtml || ((s) => String(s ?? ""));

  window.renderSubsTable = function (rows, totalLabel) {
    const tbody = document.getElementById("subs-table-body");
    if (!tbody) return;
    window._subsAllRows = rows || [];
    _renderSubsFiltered(window._subsAllRows);

    const totalEl = document.getElementById("subs-total-size");
    if (totalEl && totalLabel) totalEl.textContent = totalLabel;

    const tbl = document.getElementById("subs-table");
    if (tbl && window._subsShowAvg === false) tbl.classList.add("hide-avg-col");

    const bar = document.getElementById("subs-bulk-bar");
    if (bar) bar.hidden = true;
  };

  function _renderSubsFiltered(rows) {
    const tbody = document.getElementById("subs-table-body");
    if (!tbody) return;
    tbody.innerHTML = "";

    const frag = document.createDocumentFragment();
    for (const r of (rows || [])) {
      const tr = document.createElement("tr");
      const dot = r._pending_redownload
        ? ' <span class="sub-redwnl-dot" title="Unfinished redownload">&#9679;</span>'
        : "";

      tr.dataset.channelName = r.folder || "";
      if (r._pending_redownload) {
        tr.dataset.pendingRedownload = "1";
        if (r._redownload_res) tr.dataset.redownloadRes = r._redownload_res;
      }

      const markTip = (label, value) => {
        const s = String(value || "").trim();
        const lc = label.toLowerCase();
        const verb = lc === "metadata" ? "fetched" : "transcribed";
        const behind = parseInt((s.match(/-(\d+)\s*$/) || [])[1] || "0", 10);
        const autoOn = s.startsWith("A ");
        if (behind > 0) {
          return `${label}: ${behind} video${behind === 1 ? "" : "s"} waiting to be ${verb}`
            + (autoOn ? " (auto is on - will catch up next sync)"
                      : ` (auto-${lc} is off)`);
        }
        if (autoOn) return `${label}: auto-${lc} is ON - up to date`;
        if (s.includes("\u2713")) return `${label}: complete (done manually; auto-${lc} is off)`;
        if (s === "\u2014") return `${label}: not enabled for this channel`;
        return label;
      };

      const compressTip = (value) => {
        const s = String(value || "").trim();
        if (s.startsWith("\u2713")) {
          return "Compress: ON (videos will be re-encoded to AV1 to save space)";
        }
        return "Compress: OFF";
      };

      tr.innerHTML = `
        <td class="col-folder">${escapeHtml(r.folder)}${dot}</td>
        <td>${escapeHtml(r.res)}</td>
        <td>${escapeHtml(r.min)}</td>
        <td>${escapeHtml(r.max)}</td>
        <td class="col-mark" title="${escapeHtml(compressTip(r.compress))}">${escapeHtml(r.compress)}</td>
        <td class="col-mark" title="${escapeHtml(markTip("Transcribe", r.transcribe))}">${escapeHtml(r.transcribe)}</td>
        <td class="col-mark" title="${escapeHtml(markTip("Metadata", r.metadata))}">${escapeHtml(r.metadata)}</td>
        <td>${escapeHtml(r.last_sync)}</td>
        <td>${escapeHtml(r.n_vids)}</td>
        <td>${escapeHtml(r.size)}</td>
        <td>${escapeHtml(r.avg_size || "\u2014")}</td>
        <td class="col-actions"><button type="button" class="row-kebab" tabindex="-1" title="Channel actions" aria-label="Channel actions">&#8942;</button></td>
      `;
      frag.appendChild(tr);
    }
    tbody.appendChild(frag);
  }

  window._applySubsFilter = function (query) {
    const all = window._subsAllRows || [];
    const q = (query || "").toLowerCase().trim();
    if (!q) {
      _renderSubsFiltered(all);
      return;
    }
    _renderSubsFiltered(
      all.filter(r => (r.folder || "").toLowerCase().includes(q)),
    );
  };

  window._applySubsAvgVisibility = function (show) {
    window._subsShowAvg = !!show;
    const tbl = document.getElementById("subs-table");
    if (!tbl) return;
    tbl.classList.toggle("hide-avg-col", !show);
  };
})();
