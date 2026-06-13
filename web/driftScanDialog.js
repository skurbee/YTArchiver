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
          const resp = nativeBridgeUp() ? await bridgeCall("get_subs_channels") : undefined;
          // get_subs_channels returns a (rows, total_label) TUPLE — which
          // arrives in JS as [rowsArray, "label string"]. The old code
          // treated the whole tuple as the channel list and iterated
          // [rowsArray, label] (2 items, neither with a usable name), so
          // EVERY dropdown entry rendered "(channel name missing)".
          // Unwrap rows[0]. Also: the row dicts key the channel's display
          // name under `folder` (there is no `name` key), so read that.
          let channels = [];
          if (Array.isArray(resp) && Array.isArray(resp[0])) channels = resp[0];
          else if (Array.isArray(resp)) channels = resp;
          else channels = (resp && resp.channels) || [];
          chanSel.innerHTML = "";
          const _nm = (c) => (c && (c.folder || c.name)) || "";
          // Sort alphabetically by name for picker sanity.
          const sorted = [...channels].sort((a, b) =>
            _nm(a).toLowerCase().localeCompare(_nm(b).toLowerCase()));
          for (const ch of sorted) {
            const nm = _nm(ch);
            const opt = document.createElement("option");
            // drift_scan resolves the on-disk folder via
            // folder → folder_override → name; the Subs row only carries
            // the display name, so pass it in both slots.
            opt.value = JSON.stringify({ name: nm, folder: nm });
            opt.textContent = nm || "(channel name missing — check config)";
            chanSel.appendChild(opt);
          }
          if (!sorted.length) {
            const opt = document.createElement("option");
            opt.value = "";
            opt.textContent = "(no channels)";
            opt.disabled = true;
            chanSel.appendChild(opt);
          }
        } catch (e) {
          console.warn("drift: failed to load channels", e);
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
          body.innerHTML = `<div class="browse-empty" style="padding:16px;color:#e78a8a;">`
            + `${escapeHtml(data?.error || "Scan failed.")}</div>`;
          if (summary) summary.textContent = "";
          return;
        }
        if (data.pending) {
          // Belt-and-braces: a pending token must NEVER render as a
          // successful empty result.
          body.innerHTML = `<div class="browse-empty" style="padding:16px;">Still scanning…</div>`;
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
        const okColor = totalDrift === 0 ? "#7acf8a" : "#e8a34e";
        html += `<div style="padding:8px 10px;border-left:3px solid ${okColor};`
          + `background:rgba(255,255,255,0.03);margin-bottom:12px;">`;
        if (totalDrift === 0) {
          html += `<strong style="color:${okColor};">No drift found.</strong> `
            + `All .txt / .jsonl / FTS entries are consistent.`;
        } else {
          html += `<strong style="color:${okColor};">Drift detected:</strong> `
            + `${txtOrphans.length} .txt-only · ${jsonlOrphans.length} .jsonl-only`
            + (phantoms > 0 ? ` · ${phantoms.toLocaleString()} FTS phantoms (global)` : "");
        }
        html += `</div>`;

        // Category A: TXT without JSONL
        html += `<div style="margin-bottom:12px;">`;
        html += `<div style="font-weight:bold;margin-bottom:4px;">`
          + `A. In .txt but missing from .jsonl (${txtOrphans.length})</div>`;
        html += `<div class="edit-dim" style="font-size:11px;margin-bottom:6px;">`
          + `Fix: queue Whisper retranscribe to rebuild both sides. `
          + `Entries whose video file can't be located in the index are skipped.</div>`;
        if (!txtOrphans.length) {
          html += `<div class="edit-dim" style="padding:4px 8px;">—</div>`;
        } else {
          html += `<ul style="margin:0;padding-left:18px;">`;
          for (const o of txtOrphans.slice(0, 50)) {
            html += `<li style="margin:2px 0;"><span style="color:#d8d8d8;">`
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
        html += `<div style="margin-bottom:12px;">`;
        html += `<div style="font-weight:bold;margin-bottom:4px;">`
          + `B. In .jsonl but missing from .txt (${jsonlOrphans.length})</div>`;
        html += `<div class="edit-dim" style="font-size:11px;margin-bottom:6px;">`
          + `Fix: reconstruct .txt entry from .jsonl segments. Date from `
          + `.jsonl mtime; source tag = "RECOVERED-FROM-JSONL".</div>`;
        if (!jsonlOrphans.length) {
          html += `<div class="edit-dim" style="padding:4px 8px;">—</div>`;
        } else {
          html += `<ul style="margin:0;padding-left:18px;">`;
          for (const o of jsonlOrphans.slice(0, 50)) {
            html += `<li style="margin:2px 0;"><span style="color:#d8d8d8;">`
              + `${escapeHtml(o.title)}</span></li>`;
          }
          if (jsonlOrphans.length > 50) {
            html += `<li class="edit-dim">… and ${jsonlOrphans.length - 50} more</li>`;
          }
          html += `</ul>`;
        }
        html += `</div>`;

        // Category C: FTS phantoms (global)
        html += `<div style="margin-bottom:4px;">`;
        html += `<div style="font-weight:bold;margin-bottom:4px;">`
          + `C. FTS5 phantom rows (${phantoms.toLocaleString()})</div>`;
        html += `<div class="edit-dim" style="font-size:11px;margin-bottom:6px;">`
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
        body.innerHTML = `<div class="browse-empty" style="padding:16px;">Scanning…</div>`;
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
            body.innerHTML = `<div class="browse-empty" style="padding:16px;">Scanning…</div>`;
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
        body.innerHTML = `<div class="browse-empty" style="padding:16px;">Applying fixes…</div>`;
        try {
          if (!nativeBridgeUp()) {
            body.innerHTML = `<div class="browse-empty" style="padding:16px;color:#e78a8a;">`
              + `Native mode required.</div>`;
            return;
          }
          let res = await bridgeCall("drift_apply_channel", identity);
          if (res?.pending && res?.token) {
            res = await _pollUntilDone(
              res, (t) => bridgeCall("drift_apply_channel_poll", t), 10 * 60 * 1000);
          }
          if (!res?.ok) {
            body.innerHTML = `<div class="browse-empty" style="padding:16px;color:#e78a8a;">`
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
          body.innerHTML = `<div style="padding:12px;border-left:3px solid #7acf8a;`
            + `background:rgba(122,207,138,0.08);">`
            + `<strong style="color:#7acf8a;">Done.</strong> `
            + `${parts.map(escapeHtml).join(" · ")}.`
            + `</div>`
            + `<div class="edit-dim" style="padding:8px 4px 0;font-size:11px;">`
            + `Click Scan again to refresh the report.`
            + `</div>`;
          window._showToast?.(`Drift fix applied: ${parts.join(" · ")}.`, "ok");
        } catch (e) {
          body.innerHTML = `<div class="browse-empty" style="padding:16px;color:#e78a8a;">`
            + `${escapeHtml(String(e))}</div>`;
        }
      };

      const _open = async () => {
        bd.style.display = "flex";
        body.innerHTML = `<div class="browse-empty" style="padding:16px;">Loading channels…</div>`;
        if (summary) summary.textContent = "";
        fixBtn.disabled = true;
        _lastScan = null;
        await _loadChannels();
        body.innerHTML = `<div class="browse-empty" style="padding:16px;">`
          + `Pick a channel and click Scan.</div>`;
      };

      btn.addEventListener("click", _open);
      scanBtn.addEventListener("click", _scan);
      fixBtn.addEventListener("click", _fix);
      const _close = () => { bd.style.display = "none"; };
      closeBtn?.addEventListener("click", _close);
      bd.addEventListener("click", (e) => {
        if (e.target === bd) _close();
      });
      // BUG FIX 2026-05-15 (audit): consistent Esc-to-close behavior
      // across all custom modals.
      document.addEventListener("keydown", (e) => {
        if (e.key === "Escape" && bd.style.display !== "none") _close();
      });
  }

  window.initDriftScanDialog = initDriftScanDialog;
})();
