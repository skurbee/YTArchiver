/**
 * web/statusBar.js — global bottom status bar.
 *
 * A single always-visible line answering "what is the app doing right now?"
 * — sync state, processing (GPU) state, indexing progress, and a session
 * error count. Before this, that information was scattered across the
 * activity log, the italic indicator line, the two queue-popover buttons,
 * and the tray icon.
 *
 * It does NOT own any state: it wraps the globals the backend already
 * pushes to (`renderQueues`, `setQueueState`, `_setIndicator`) and the log
 * DOM, so it stays a pure read-only mirror. Wrapping happens in
 * initStatusBar() (called from app.js boot, after the queue/log modules
 * have assigned those globals).
 *
 * Exposed as window.initStatusBar.
 */
(function () {
  "use strict";

  // Last-known inputs, refreshed by the wrapped globals below.
  let _queues = { sync: [], gpu: [] };
  let _state = { sync: {}, gpu: {} };
  const _indicator = { sweep: "" };
  let _errorCount = 0;

  function _runningItem(list) {
    for (const t of (list || [])) {
      if ((t && t.status) === "running") return t;
    }
    return null;
  }

  function _cleanName(item) {
    if (!item) return "";
    const n = item.channel_name || item.title || item.name || "";
    return String(n).trim();
  }

  function _queueCount(kind) {
    const raw = _queues?.[`${kind}_count`];
    if (Number.isFinite(raw)) return Math.max(0, Number(raw));
    return (_queues[kind] || []).length;
  }

  function _truncate(s, n) {
    s = String(s || "");
    return s.length > n ? s.slice(0, n - 1) + "…" : s;
  }

  // Build the text for one queue segment (sync or gpu).
  function _segText(kind, verbRunning, idleLabel) {
    const list = _queues[kind] || [];
    const st = _state[kind] || {};
    const count = _queueCount(kind);
    if (st.paused) {
      return count ? `${idleLabel.split(" ")[0]} paused (${count})`
                   : `${idleLabel.split(" ")[0]} paused`;
    }
    const running = _runningItem(list);
    if (running || st.running) {
      const name = _cleanName(running);
      const label = name ? `${verbRunning} ${_truncate(name, 34)}` : verbRunning;
      // Show how many more are waiting behind the running one.
      return count > 1 ? `${label}  (+${count - 1})` : label;
    }
    if (count > 0) return `${idleLabel.split(" ")[0]}: ${count} queued`;
    return idleLabel;
  }

  function _setSeg(segId, running, paused) {
    const seg = document.getElementById(segId);
    if (!seg) return;
    seg.classList.toggle("gsb-active", !!running);
    seg.classList.toggle("gsb-paused", !!paused);
    const dot = seg.querySelector(".gsb-dot");
    if (dot) {
      dot.classList.toggle("on", !!running);
      dot.classList.toggle("paused", !!paused);
    }
  }

  function _render() {
    const syncText = document.getElementById("gsb-sync-text");
    const gpuText = document.getElementById("gsb-gpu-text");
    if (syncText) syncText.textContent = _segText("sync", "Syncing", "Sync idle");
    if (gpuText) gpuText.textContent = _segText("gpu", "Processing", "Processing idle");

    const sSt = _state.sync || {}, gSt = _state.gpu || {};
    _setSeg("gsb-sync", sSt.running || _runningItem(_queues.sync), sSt.paused);
    _setSeg("gsb-gpu", gSt.running || _runningItem(_queues.gpu), gSt.paused);

    // Index / sweep indicator.
    const idxEl = document.getElementById("gsb-index");
    if (idxEl) {
      const txt = (_indicator.sweep || "").trim();
      if (txt) { idxEl.hidden = false; idxEl.textContent = _truncate(txt, 46); }
      else idxEl.hidden = true;
    }

    // Session error count.
    const errEl = document.getElementById("gsb-errors");
    const errCount = document.getElementById("gsb-errors-count");
    if (errEl) errEl.hidden = _errorCount <= 0;
    if (errCount) errCount.textContent = String(_errorCount);
  }

  // Count newly-appended error lines in the main log. Decoupled from
  // logs.js internals via a MutationObserver so nothing there changes.
  // Gated: counting only goes live ~2.5s after init so the initial log
  // seed (historical lines, incl. old errors) and any bulk re-render
  // don't inflate the count into a scary "N errors" on every launch. It
  // tracks NEW errors during the session, which is the useful signal.
  let _countLive = false;
  function _wireErrorCounter() {
    const log = document.getElementById("main-log");
    if (!log || log._gsbErrObserved) return;
    log._gsbErrObserved = true;
    const isErr = (node) =>
      node && node.nodeType === 1 && node.classList &&
      node.classList.contains("log-line") &&
      !!node.querySelector(".t-red");
    const obs = new MutationObserver((muts) => {
      if (!_countLive) return;
      let added = 0;
      for (const m of muts) {
        for (const n of m.addedNodes) if (isErr(n)) added++;
      }
      if (added) { _errorCount += added; _render(); }
    });
    obs.observe(log, { childList: true });
    if (window._trackBootObserver) window._trackBootObserver(obs);
    setTimeout(() => { _countLive = true; }, 2500);
  }

  function initStatusBar() {
    const bar = document.getElementById("global-status-bar");
    if (!bar || bar._inited) return;
    bar._inited = true;

    // Wrap the three globals the backend pushes to. Each wrapper calls the
    // previous implementation first (preserving any other wrappers, e.g.
    // queueBlink's setQueueState), then refreshes the bar.
    const origRender = window.renderQueues;
    window.renderQueues = function (q) {
      if (origRender) { try { origRender(q); } catch (e) { /* ignore */ } }
      _queues = q || { sync: [], gpu: [] };
      _render();
    };
    const origState = window.setQueueState;
    window.setQueueState = function (s) {
      if (origState) { try { origState(s); } catch (e) { /* ignore */ } }
      _state = s || { sync: {}, gpu: {} };
      _render();
    };
    const origInd = window._setIndicator;
    window._setIndicator = function (slot, text) {
      if (origInd) { try { origInd(slot, text); } catch (e) { /* ignore */ } }
      if (slot === "sweep") _indicator[slot] = text || "";
      _render();
    };

    // Sync / Processing segments open their existing queue popovers,
    // anchored to the SEGMENT (so it works on any tab — the toolbar button
    // is only on Download). stopPropagation so this same click doesn't
    // bubble to the document outside-click handler and instantly re-close
    // the popover we just opened (the bug where "clicking did nothing").
    document.getElementById("gsb-sync")?.addEventListener("click", (e) => {
      e.stopPropagation();
      window.toggleQueuePopover?.("sync", e.currentTarget);
    });
    document.getElementById("gsb-gpu")?.addEventListener("click", (e) => {
      e.stopPropagation();
      window.toggleQueuePopover?.("gpu", e.currentTarget);
    });
    // Log button + error segment jump to the Download tab (home of the
    // full activity log) and scroll it to the newest line.
    const gotoLog = () => {
      document.querySelector('.tab[data-tab="download"]')?.click();
      const log = document.getElementById("main-log");
      if (log) log.scrollTop = log.scrollHeight;
    };
    document.getElementById("gsb-log-btn")?.addEventListener("click", gotoLog);
    document.getElementById("gsb-errors")?.addEventListener("click", () => {
      // Viewing the log "acknowledges" the errors — reset the counter.
      _errorCount = 0;
      gotoLog();
      _render();
    });

    _wireErrorCounter();
    _render();
  }

  window.initStatusBar = initStatusBar;
})();
