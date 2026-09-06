/* ═══════════════════════════════════════════════════════════════════════
   compressDryRunDialog.js — "Compress dry-run" results modal

   The Settings → Tools → "Compress dry-run" button opens this modal, which queries
   every channel and shows projected file-size savings if AV1
   compression were applied. No actual encode runs from this dialog —
   it's purely informational.

   Publishes:
     window.initCompressDryRunDialog
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

  function _withTimeout(promise, ms, message) {
    let timer = null;
    const timeout = new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error(message)), ms);
    });
    return Promise.race([promise, timeout]).finally(() => {
      if (timer !== null) clearTimeout(timer);
    });
  }

  const askConfirm = window.askConfirm;

  function initCompressDryRunDialog() {
      const btn = document.getElementById("btn-compress-dry-run");
      const bd = document.getElementById("compress-dry-backdrop");
      const body = document.getElementById("compress-dry-body");
      const summary = document.getElementById("compress-dry-summary");
      const resSel = document.getElementById("compress-dry-res");
      const closeBtn = document.getElementById("compress-dry-close");
      const recalcBtn = document.getElementById("compress-dry-recalc");
      if (!btn || !bd || !body) return;
      const _fmt = (gb) => {
        const n = Number(gb) || 0;
        if (n >= 1024) return (n / 1024).toFixed(1) + " TB";
        return n.toFixed(1) + " GB";
      };
      const _diffPct = (current, projected) => {
        if (!current || current <= 0) return "";
        const pct = Math.round((1 - projected / current) * 100);
        return pct > 0 ? ` (-${pct}%)` : ` (+${-pct}%)`;
      };
      const _render = (data) => {
        const escapeHtml = window._escapeHtml || ((s) =>
          String(s).replace(/[&<>"']/g, c => (
            {"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[c])));
        if (!data?.ok) {
          body.innerHTML =
            `<div class="browse-empty askq-empty-padded askq-empty-danger">`
            + `${escapeHtml(data?.error || "Dry run failed.")}</div>`;
          if (summary) summary.textContent = "";
          return;
        }
        const t = data.total || {};
        if (summary) {
          // Bug [111]: optional-chain `t.videos?.toLocaleString()` returns
          // undefined when the field is missing/null, rendering literal
          // "undefined" in the summary. Coerce to 0 to match the per-row
          // table's `_n()` helper at line 7530+.
          const _vids = Number.isFinite(Number(t.videos)) ? Number(t.videos) : 0;
          const _hrs = Number.isFinite(Number(t.hours)) ? Number(t.hours) : 0;
          summary.textContent =
            `${_vids.toLocaleString()} videos · ${_hrs.toLocaleString()} hours`;
          if (Number(t.unknown_videos) > 0) {
            summary.textContent += ` · ${Number(t.unknown_videos).toLocaleString()} with unknown duration (${_fmt(t.unknown_gb)} kept at current size)`;
          }
        }
        // Build a simple table. Per-channel rows sorted by current_gb
        // desc (matches backend query); grand total pinned at top.
        let html = "";
        html += `<table class="compress-dry-table">`;
        html += `<thead><tr>`;
        html += `<th class="cell-left">Channel</th>`;
        html += `<th title="Total videos on disk for this channel.">Videos</th>`;
        html += `<th title="Sum of video durations.">Hours</th>`;
        html += `<th title="Current bytes used on disk before compression.">Current</th>`;
        html += `<th title="Estimated size after AV1 compression at the GENEROUS bitrate tier (largest files, best quality).">Generous</th>`;
        html += `<th title="Estimated size at the AVERAGE bitrate tier (middle ground — recommended for most archives).">Average</th>`;
        html += `<th title="Estimated size at the BELOW-AVERAGE bitrate tier (smallest files, most aggressive savings; some quality loss).">Below Avg</th>`;
        html += `</tr></thead><tbody>`;
        // Grand totals first, highlighted.
        html += `<tr class="compress-total-row">`;
        html += `<td>ALL CHANNELS (${escapeHtml(String(data.output_res))}p target)</td>`;
        html += `<td class="cell-num">${(t.videos || 0).toLocaleString()}</td>`;
        html += `<td class="cell-num">${(t.hours || 0).toLocaleString()}</td>`;
        html += `<td class="cell-num">${_fmt(t.current_gb)}</td>`;
        html += `<td class="cell-num">${_fmt(t.generous_gb)}${_diffPct(t.current_gb, t.generous_gb)}</td>`;
        html += `<td class="cell-num">${_fmt(t.average_gb)}${_diffPct(t.current_gb, t.average_gb)}</td>`;
        html += `<td class="cell-num">${_fmt(t.below_gb)}${_diffPct(t.current_gb, t.below_gb)}</td>`;
        html += `</tr>`;
        // null-safe numeric reads. A single malformed
        // channel row (e.g. missing videos/hours) used to TypeError
        // on toLocaleString and kill the whole table render, leaving
        // the modal blank. Coercing to 0 keeps the dialog usable
        // even when the backend returns a partial row.
        for (const c of (data.channels || [])) {
          const _n = (v) => (Number.isFinite(Number(v)) ? Number(v) : 0);
          html += `<tr>`;
          html += `<td>${escapeHtml(c.name || "(unknown)")}</td>`;
          html += `<td class="cell-num">${_n(c.videos).toLocaleString()}</td>`;
          html += `<td class="cell-num" title="${_n(c.unknown_videos)} video(s) have unknown duration; their current size is retained in each estimate.">${_n(c.hours).toLocaleString()}${_n(c.unknown_videos) ? " *" : ""}</td>`;
          html += `<td class="cell-num">${_fmt(c.current_gb)}</td>`;
          html += `<td class="cell-num">${_fmt(c.generous_gb)}</td>`;
          html += `<td class="cell-num">${_fmt(c.average_gb)}</td>`;
          html += `<td class="cell-num">${_fmt(c.below_gb)}</td>`;
          html += `</tr>`;
        }
        html += `</tbody></table>`;
        html += `<div class="edit-dim compress-dry-note">`
              + `Projections use MB/hour bitrate presets applied to each channel's `
              + `known duration. Videos with unknown duration keep their current size `
              + `in every estimate; no savings are assumed for them. `
              + `Actual compression results may differ.</div>`;
        body.innerHTML = html;
      };
      // Block backdrop-close while a compute is mid-flight so the
      // result doesn't land into a hidden dialog and look like
      // "nothing happened" when re-opened (audit:
      // compressDryRunDialog H242).
      let _computeInFlight = false;
      let _computeGeneration = 0;
      const _setComputing = (active) => {
        _computeInFlight = active;
        if (recalcBtn) recalcBtn.disabled = active;
        if (closeBtn) closeBtn.disabled = active;
        if (resSel) {
          resSel.disabled = active;
          resSel._ytddRepaint?.();
        }
        btn.disabled = active;
      };
      const _open = async () => {
        if (_computeInFlight) return;
        const generation = ++_computeGeneration;
        bd.hidden = false;
        body.innerHTML = `<div class="browse-empty askq-empty-padded">Computing…</div>`;
        if (summary) summary.textContent = "";
        _setComputing(true);
        try {
          if (!nativeBridgeUp()) {
            _render({ ok: false, error: "YTArchiver isn't ready yet. Try again in a moment." });
            return;
          }
          const res = await _withTimeout(
            bridgeCall("compress_dry_run", resSel?.value || "720"),
            5 * 60 * 1000,
            "Timed out computing projections."
          );
          if (generation !== _computeGeneration) return;
          _render(res);
        } catch (e) {
          if (generation !== _computeGeneration) return;
          _render({ ok: false, error: String(e) });
        } finally {
          if (generation === _computeGeneration) _setComputing(false);
        }
      };
      btn.addEventListener("click", _open);
      recalcBtn?.addEventListener("click", _open);
      const _close = () => {
        if (_computeInFlight) return;
        bd.hidden = true;
      };
      closeBtn?.addEventListener("click", _close);
      bd.addEventListener("click", (e) => {
        if (e.target === bd && !_computeInFlight) _close();
      });
      // BUG FIX 2026-05-15 (audit): Esc was a no-op on this dialog —
      // every other modal in the app dismisses on Esc but dry-run
      // didn't. Wire global keydown to close when the dialog is open.
      window.YT?.modals?.registerEscapeClose?.(bd, () => {
        if (!_computeInFlight) _close();
      });
  }

  window.initCompressDryRunDialog = initCompressDryRunDialog;
})();
