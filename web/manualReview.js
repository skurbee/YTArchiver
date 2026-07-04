/**
 * web/manualReview.js - picker for ambiguous Manual Downloads ID recovery.
 *
 * Reads api.manual_backfill_review_list() and shows one unresolved manual
 * download at a time. Picking a candidate writes the ID immediately; metadata
 * and thumbnail refresh continue in the background.
 */
(function () {
  "use strict";

  function bridgeCall(method, ...args) {
    const fn = window.YT?.bridge?.bridgeCall;
    return fn ? fn(method, ...args) : undefined;
  }
  function nativeBridgeUp() { return !!window.YT?.bridge?.isUp?.(); }
  const esc = window._escapeHtml || ((s) => String(s ?? "")
    .replace(/[&<>"']/g, (ch) => ({
      "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;", "'": "&#39;",
    }[ch])));

  let _items = [];
  let _idx = 0;
  let _backdrop = null;
  let _busy = false;

  function _fmtDur(secs) {
    const s = Math.max(0, Math.round(Number(secs) || 0));
    if (!s) return "?";
    const h = Math.floor(s / 3600);
    const m = Math.floor((s % 3600) / 60);
    const sec = s % 60;
    const p = (n) => String(n).padStart(2, "0");
    return h ? `${h}:${p(m)}:${p(sec)}` : `${m}:${p(sec)}`;
  }

  function _fmtViews(value) {
    const n = Number(value);
    if (!Number.isFinite(n) || n < 0) return "views unknown";
    if (n >= 1000000000) return (n / 1000000000).toFixed(n >= 10000000000 ? 0 : 1) + "B views";
    if (n >= 1000000) return (n / 1000000).toFixed(n >= 10000000 ? 0 : 1) + "M views";
    if (n >= 1000) return (n / 1000).toFixed(n >= 10000 ? 0 : 1) + "K views";
    return Math.round(n).toLocaleString() + " views";
  }

  function _setBusy(on, label) {
    _busy = !!on;
    const body = document.getElementById("manual-review-body");
    body?.querySelectorAll("button").forEach((b) => { b.disabled = !!on; });
    const skip = document.getElementById("manual-review-skip");
    const close = document.getElementById("manual-review-close");
    if (skip) {
      skip.disabled = !!on;
      skip.textContent = on ? (label || "Working...") : "Skip this one";
    }
    if (close) close.disabled = !!on;
  }

  function _ensureDialog() {
    if (_backdrop) return _backdrop;
    const style = document.createElement("style");
    style.textContent =
      "#manual-review-body{max-height:60vh;overflow-y:auto;}" +
      ".mrev-local{padding:4px 0 10px;border-bottom:1px solid var(--border,#333);margin-bottom:8px;}" +
      ".mrev-local-title{font-weight:600;}" +
      ".mrev-local-sub,.mrev-cand-sub{font-size:var(--fs-xs);color:var(--text-dim,#999);}" +
      ".mrev-pick-label{font-size:var(--fs-sm);color:var(--text-dim,#999);margin:4px 0 6px;}" +
      ".mrev-cand{display:flex;align-items:center;gap:10px;padding:7px 4px;border-bottom:1px solid var(--border,#2a2a2a);}" +
      ".mrev-cand-info{flex:1;min-width:0;}" +
      ".mrev-cand-title{white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}" +
      ".mrev-yt{font-size:var(--fs-xs);white-space:nowrap;cursor:pointer;color:var(--accent,#6cf);}";
    document.head.appendChild(style);

    const bd = document.createElement("div");
    bd.className = "askq-backdrop";
    bd.id = "manual-review-backdrop";
    bd.hidden = true;
    bd.innerHTML =
      '<div class="askq-dialog askq-dialog-manual">' +
      '  <div class="askq-header"><span>Review matches</span>' +
      '    <span class="status-text dialog-summary-spacer" id="manual-review-progress"></span></div>' +
      '  <div class="askq-body" id="manual-review-body"></div>' +
      '  <div class="askq-buttons">' +
      '    <button class="btn btn-ghost" id="manual-review-skip">Skip this one</button>' +
      '    <button class="btn btn-ghost" id="manual-review-close">Close</button>' +
      '  </div>' +
      '</div>';
    document.body.appendChild(bd);
    bd.querySelector("#manual-review-close").addEventListener("click", _close);
    bd.querySelector("#manual-review-skip").addEventListener("click", _skip);
    bd.addEventListener("click", (e) => { if (e.target === bd && !_busy) _close(); });
    _backdrop = bd;
    return bd;
  }

  function _render() {
    const body = document.getElementById("manual-review-body");
    const prog = document.getElementById("manual-review-progress");
    if (!body) return;
    _setBusy(false);
    if (_idx >= _items.length) {
      body.innerHTML = '<div class="browse-empty">All reviewed.</div>';
      if (prog) prog.textContent = "";
      return;
    }
    const it = _items[_idx];
    if (prog) prog.textContent = `${_idx + 1} of ${_items.length}`;
    const leaf = (it.filepath || "").split(/[\\/]/).pop() || "";
    const cands = (it.candidates || []).map((c, i) => {
      const sub = [
        c.channel || "unknown channel",
        _fmtDur(c.duration),
        _fmtViews(c.view_count),
        "delta " + (c.dur_delta != null ? c.dur_delta : "?") + "s",
        "title " + Math.round((c.title_sim || 0) * 100) + "%",
      ].join(" - ");
      return (
        '<div class="mrev-cand">' +
        '  <button class="btn btn-primary btn-thin mrev-pick" data-i="' + i + '">This one</button>' +
        '  <div class="mrev-cand-info">' +
        '    <div class="mrev-cand-title">' + esc(c.title || "") + '</div>' +
        '    <div class="mrev-cand-sub">' + esc(sub) + '</div>' +
        '  </div>' +
        '  <span class="mrev-yt" data-yt="https://www.youtube.com/watch?v=' + esc(c.id) + '">YouTube</span>' +
        '</div>'
      );
    }).join("");
    body.innerHTML =
      '<div class="mrev-local">' +
      '  <div class="mrev-local-title">' + esc(it.title || leaf) + '</div>' +
      '  <div class="mrev-local-sub">your file - ' + _fmtDur(it.duration) + ' - ' + esc(leaf) + '</div>' +
      '</div>' +
      '<div class="mrev-pick-label">Pick the correct video:</div>' +
      '<div class="mrev-cands">' + (cands || '<div class="browse-empty">No candidates.</div>') + '</div>';
    body.querySelectorAll(".mrev-pick").forEach((b) => {
      b.addEventListener("click", () => _pick(parseInt(b.dataset.i, 10), b));
    });
    body.querySelectorAll(".mrev-yt").forEach((a) => {
      a.addEventListener("click", () => {
        try { window.open(a.dataset.yt, "_blank"); } catch (_e) {}
      });
    });
  }

  async function _pick(ci, btn) {
    if (_busy) return;
    const it = _items[_idx];
    const c = it && (it.candidates || [])[ci];
    if (!it || !c || !nativeBridgeUp()) return;
    _setBusy(true, "Saving...");
    if (btn) btn.textContent = "Saving...";
    let ok = false;
    try {
      const res = await bridgeCall("manual_backfill_apply_pick",
        it.filepath, c.id, c.channel || "", c.title || "");
      ok = !!(res && res.ok);
      if (ok) window._showToast?.("Resolved. Fetching metadata in the background.", "ok");
      else window._showToast?.((res && res.error) || "Couldn't apply.", "error");
    } catch (_e) {
      window._showToast?.("Couldn't apply.", "error");
    } finally {
      _setBusy(false);
    }
    if (ok) _advance();
    else _render();
  }

  async function _skip() {
    if (_busy) return;
    const it = _items[_idx];
    if (!it) { _close(); return; }
    _setBusy(true, "Skipping...");
    try { await bridgeCall("manual_backfill_review_skip", it.filepath); }
    catch (_e) { /* non-fatal */ }
    finally { _setBusy(false); }
    _advance();
  }

  function _advance() {
    _idx += 1;
    _render();
    window._refreshManualReviewCount?.();
    if (_idx >= _items.length) setTimeout(_close, 800);
  }

  function _close() {
    if (_backdrop) _backdrop.hidden = true;
    window._refreshManualReviewCount?.();
  }

  window._openManualReview = async function () {
    if (!nativeBridgeUp()) { window._showToast?.("Not ready.", "warn"); return; }
    let res;
    try { res = await bridgeCall("manual_backfill_review_list"); }
    catch (_e) {
      window._showToast?.("Couldn't load the review list.", "error");
      return;
    }
    _items = (res && res.items) || [];
    _idx = 0;
    if (!_items.length) { window._showToast?.("Nothing to review.", "ok"); return; }
    _ensureDialog();
    _backdrop.hidden = false;
    _render();
  };
})();
