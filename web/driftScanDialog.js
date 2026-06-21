/* ═══════════════════════════════════════════════════════════════════════
   driftScanDialog.js — Transcript drift scan + fix modal

   Extracted from settingsTab.js (Patch 24, v72.6). Wires the
   Settings → Tools → "Scan for transcript drift" button:

     • Open the dialog (channel picker + Scan / Fix buttons)
     • Scan button → call api.drift_scan(channel) → render three
       drift categories (txt-without-jsonl, jsonl-without-txt,
       FTS phantoms)
     • Fix button → call api.drift_apply(channel) → reconstruct
       missing files, queue Whisper for txt-only entries, rebuild
       FTS if phantoms found

   Publishes:
     window.initDriftScanDialog (called from app.js boot)
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

  const askConfirm = window.askConfirm;
  const askDanger = window.askDanger;

  function initDriftScanDialog() {
      const btn = document.getElementById("btn-drift-scan");
      const bd = document.getElementById("drift-backdrop");
      const body = document.getElementById("drift-body");
      const summary = document.getElementById("drift-summary");
      const chanSel = document.getElementById("drift-channel");
      const scanBtn = document.getElementById("drift-scan-btn");
      const fixBtn = document.getElementById("drift-fix-btn");
      const closeBtn = document.getElementById("drift-close");
      if (!btn || !bd || !body || !chanSel || !scanBtn || !fixBtn) return;

      let _lastScan = null;  // { ok, channel, txt_without_jsonl, ... }

      const escapeHtml = window._escapeHtml || ((s) =>
        String(s).replace(/[&<>"']/g, c => (
          {"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[c])));

      const _loadChannels = async () => {
        try {
          const channels = await window.YT?.util?.loadSubsChannels?.() || [];
          chanSel.innerHTML = "";
          for (const ch of channels) {
            const nm = ch.displayName || ch.folder || ch.name || "";
            const opt = document.createElement("option");
            // drift_scan resolves the on-disk folder via
            // folder → folder_override → name; the Subs row only carries
            // the display name, so pass it in both slots.
            opt.value = JSON.stringify({ name: ch.name || nm, folder: ch.folder || nm });
            opt.textContent = nm || "(channel name missing — check config)";
            chanSel.appendChild(opt);
          }
          if (!channels.length) {
            const opt = document.createElement("option");
            opt.value = "";
            opt.textContent = "(no channels)";
            opt.disabled = true;
            chanSel.appendChild(opt);
          }
        } catch (e) {
          console.warn("drift: failed to load channels", e);
          window._showToast?.(`Could not load channels: ${e}`, "warn");
        }
      };

      // drift_scan_channel / drift_apply_channel are token-poll
      // endpoints: they return {ok:true, pending:true, token} at once
      // and the REAL result must be fetched via the matching *_poll
      // method. Without polling, the bare token dict rendered as a
      // green "No drift found" with Fix permanently disabled — the
      // entire feature was decorative.
      const _pollUntilDone = async (res, pollFn, timeoutMs) => {
        const t0 = Date.now();
        while (res && res.pending && res.token) {
          if (Date.now() - t0 > timeoutMs) {
            return { ok: false, error: "Timed out waiting for the result." };
          }
          await new Promise((r) => setTimeout(r, 500));
          res = await pollFn(res.token);
        }
        return res;
      };

      const _renderScan = (data) => {
        _lastScan = data;
        fixBtn.disabled = true;
        if (!data?.ok) {
          body.innerHTML = `<div class="browse-empty askq-empty-padded askq-empty-danger">`
            + `${escapeHtml(data?.error || "Scan failed.")}</div>`;
          if (summary) summary.textContent = "";
          return;
        }
        if (data.pending) {
          // Belt-and-braces: a pending token must NEVER render as a
          // successful empty result.
          body.innerHTML = `<div class="browse-empty askq-empty-padded">Still scanning…</div>`;
          if (summary) summary.textContent = "";
          return;
        }
        const txtOrphans = data.txt_without_jsonl || [];
        const jsonlOrphans = data.jsonl_without_txt || [];
        const phantoms = data.fts_phantoms || 0;
        const totals = data.totals || {};
        const totalDrift = txtOrphans.length + jsonlOrphans.length + (phantoms > 0 ? 1 : 0);

        if (summary) {
          summary.textContent =
            `${(totals.txt_titles || 0).toLocaleString()} .txt · `
            + `${(totals.jsonl_titles || 0).toLocaleString()} .jsonl entries scanned`;
        }

        let html = "";
        // Summary bar — green if no drift, red/orange if any found.
        html += `<div class="drift-summary-box${totalDrift === 0 ? " is-clean" : ""}">`;
        if (totalDrift === 0) {
          html += `<strong class="drift-summary-title">No drift found.</strong> `
            + `All .txt / .jsonl / FTS entries are consistent.`;
        } else {
          html += `<strong class="drift-summary-title">Drift detected:</strong> `
            + `${txtOrphans.length} .txt-only · ${jsonlOrphans.length} .jsonl-only`
            + (phantoms > 0 ? ` · ${phantoms.toLocaleString()} FTS phantoms (global)` : "");
        }
        html += `</div>`;

        // Category A: TXT without JSONL
        html += `<div class="drift-section">`;
        html += `<div class="drift-section-title">`
          + `A. In .txt but missing from .jsonl (${txtOrphans.length})</div>`;
        html += `<div class="edit-dim drift-section-note">`
          + `Fix: queue Whisper retranscribe to rebuild both sides. `
          + `Entries whose video file can't be located in the index are skipped.</div>`;
        if (!txtOrphans.length) {
          html += `<div class="edit-dim drift-empty-row">—</div>`;
        } else {
          html += `<ul class="drift-list">`;
          for (const o of txtOrphans.slice(0, 50)) {
            html += `<li class="drift-list-item"><span class="drift-list-title">`
              + `${escapeHtml(o.title)}</span>`;
            if (o.src_tag) html += ` <span class="edit-dim">(${escapeHtml(o.src_tag)})</span>`;
            html += `</li>`;
          }
          if (txtOrphans.length > 50) {
            html += `<li class="edit-dim">… and ${txtOrphans.length - 50} more</li>`;
          }
          html += `</ul>`;
        }
        html += `</div>`;

        // Category B: JSONL without TXT
        html += `<div class="drift-section">`;
        html += `<div class="drift-section-title">`
          + `B. In .jsonl but missing from .txt (${jsonlOrphans.length})</div>`;
        html += `<div class="edit-dim drift-section-note">`
          + `Fix: reconstruct .txt entry from .jsonl segments. Date from `
          + `.jsonl mtime; source tag = "RECOVERED-FROM-JSONL".</div>`;
        if (!jsonlOrphans.length) {
          html += `<div class="edit-dim drift-empty-row">—</div>`;
        } else {
          html += `<ul class="drift-list">`;
          for (const o of jsonlOrphans.slice(0, 50)) {
            html += `<li class="drift-list-item"><span class="drift-list-title">`
              + `${escapeHtml(o.title)}</span></li>`;
          }
          if (jsonlOrphans.length > 50) {
            html += `<li class="edit-dim">… and ${jsonlOrphans.length - 50} more</li>`;
          }
          html += `</ul>`;
        }
        html += `</div>`;

        // Category C: FTS phantoms (global)
        html += `<div class="drift-section-tight">`;
        html += `<div class="drift-section-title">`
          + `C. FTS5 phantom rows (${phantoms.toLocaleString()})</div>`;
        html += `<div class="edit-dim drift-section-note">`
          + `Fix: rebuild FTS5 index (idempotent; clears orphan rowids `
          + `from re-ingested transcripts — audit bug C-9). Counted globally; `
          + `fix is global.</div>`;
        html += `</div>`;

        body.innerHTML = html;
        fixBtn.disabled = (totalDrift === 0);
      };

      let _scanInFlight = false;
      const _scan = async () => {
        // In-flight guard so a rapid double-click on Scan can't
        // start two overlapping drift_scan_channel calls (audit:
        // driftScanDialog H215).
        if (_scanInFlight) return;
        const raw = chanSel.value;
        if (!raw) {
          _renderScan({ ok: false, error: "Pick a channel first." });
          return;
        }
        let identity;
        try { identity = JSON.parse(raw); }
        catch { identity = { name: raw }; }
        body.innerHTML = `<div class="browse-empty askq-empty-padded">Scanning…</div>`;
        if (summary) summary.textContent = "";
        fixBtn.disabled = true;
        if (scanBtn) scanBtn.disabled = true;
        _scanInFlight = true;
        try {
          if (!nativeBridgeUp()) {
            _renderScan({ ok: false, error: "Native mode required." });
            return;
          }
          let res = await bridgeCall("drift_scan_channel", identity);
          if (res?.pending && res?.token) {
            body.innerHTML = `<div class="browse-empty askq-empty-padded">Scanning…</div>`;
            res = await _pollUntilDone(
              res, (t) => bridgeCall("drift_scan_channel_poll", t), 10 * 60 * 1000);
          }
          // Stash the identity we just scanned with so _fix uses
          // the same one regardless of dropdown drift (audit H216).
          if (res && typeof res === "object") res._scan_identity = identity;
          _renderScan(res);
        } catch (e) {
          _renderScan({ ok: false, error: String(e) });
        } finally {
          _scanInFlight = false;
          if (scanBtn) scanBtn.disabled = false;
        }
      };

      const _fix = async () => {
        if (!_lastScan?.ok) return;
        // Use the identity FROM _lastScan, not chanSel.value — the
        // user could have changed the dropdown between Scan and Fix,
        // and we want to fix what we just scanned (audit:
        // driftScanDialog H216).
        const identity = _lastScan._scan_identity
          || (() => {
            const raw = chanSel.value;
            try { return JSON.parse(raw); }
            catch { return { name: raw }; }
          })();
        const txtCount = (_lastScan.txt_without_jsonl || []).length;
        const jsonlCount = (_lastScan.jsonl_without_txt || []).length;
        const phantoms = _lastScan.fts_phantoms || 0;
        // Confirm before queueing potentially many Whisper jobs.
        if (txtCount > 0 && window.askDanger) {
          const ok = await window.askDanger(
            "Fix transcript drift?",
            `This will queue ${txtCount} Whisper retranscribe job(s), `
              + `rebuild ${jsonlCount} .txt entries from .jsonl, and `
              + (phantoms > 0 ? `rebuild the FTS index. ` : ``)
              + `Proceed?`,
            "Fix all");
          if (!ok) return;
        }
        fixBtn.disabled = true;
        body.innerHTML = `<div class="browse-empty askq-empty-padded">Applying fixes…</div>`;
        try {
          if (!nativeBridgeUp()) {
            body.innerHTML = `<div class="browse-empty askq-empty-padded askq-empty-danger">`
              + `Native mode required.</div>`;
            return;
          }
          let res = await bridgeCall("drift_apply_channel", identity);
          if (res?.pending && res?.token) {
            res = await _pollUntilDone(
              res, (t) => bridgeCall("drift_apply_channel_poll", t), 10 * 60 * 1000);
          }
          if (!res?.ok) {
            body.innerHTML = `<div class="browse-empty askq-empty-padded askq-empty-danger">`
              + `${escapeHtml(res?.error || "Fix failed.")}</div>`;
            return;
          }
          const a = res.actions || {};
          const parts = [];
          if (a.txt_reconstructed) parts.push(`${a.txt_reconstructed} .txt rebuilt`);
          if (a.retranscribe_queued) parts.push(`${a.retranscribe_queued} queued for Whisper`);
          if (a.retranscribe_skipped) parts.push(`${a.retranscribe_skipped} skipped (video file missing)`);
          if (a.fts_rebuilt) parts.push("FTS rebuilt");
          if (!parts.length) parts.push("no actions taken");
          body.innerHTML = `<div class="drift-done-box">`
            + `<strong class="drift-done-title">Done.</strong> `
            + `${parts.map(escapeHtml).join(" · ")}.`
            + `</div>`
            + `<div class="edit-dim drift-done-note">`
            + `Click Scan again to refresh the report.`
            + `</div>`;
          window._showToast?.(`Drift fix applied: ${parts.join(" · ")}.`, "ok");
        } catch (e) {
          body.innerHTML = `<div class="browse-empty askq-empty-padded askq-empty-danger">`
            + `${escapeHtml(String(e))}</div>`;
        }
      };

      const _open = async () => {
        bd.hidden = false;
        body.innerHTML = `<div class="browse-empty askq-empty-padded">Loading channels…</div>`;
        if (summary) summary.textContent = "";
        fixBtn.disabled = true;
        _lastScan = null;
        await _loadChannels();
        body.innerHTML = `<div class="browse-empty askq-empty-padded">`
          + `Pick a channel and click Scan.</div>`;
      };

      btn.addEventListener("click", _open);
      scanBtn.addEventListener("click", _scan);
      fixBtn.addEventListener("click", _fix);
      const _close = () => { bd.hidden = true; };
      closeBtn?.addEventListener("click", _close);
      bd.addEventListener("click", (e) => {
        if (e.target === bd) _close();
      });
      // BUG FIX 2026-05-15 (audit): consistent Esc-to-close behavior
      // across all custom modals.
      window.YT?.modals?.registerEscapeClose?.(bd, _close);
  }

  window.initDriftScanDialog = initDriftScanDialog;
})();
