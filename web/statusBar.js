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
  let _rescan = null;
  let _rescanHideTimer = null;
  let _errorCount = 0;
  let _errorItems = [];

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
    const idxText = document.getElementById("gsb-index-text");
    const progress = document.getElementById("gsb-progress-track");
    const fill = document.getElementById("gsb-progress-fill");
    if (idxEl) {
      const rescanVisible = !!(_rescan && _rescan.phase !== "idle" &&
        (_rescan.running || _rescan.message));
      const txt = rescanVisible
        ? (_rescan.message || "Archive rescan…")
        : (_indicator.sweep || "").trim();
      idxEl.hidden = !txt;
      if (idxText) idxText.textContent = _truncate(txt, 58);
      const showProgress = rescanVisible && _rescan.phase !== "error";
      if (progress) {
        const pct = Math.max(0, Math.min(100, Number(_rescan?.percent) || 0));
        progress.hidden = !showProgress;
        progress.setAttribute("aria-valuenow", String(pct));
        if (fill) fill.style.width = `${pct}%`;
      }
    }

    const rescanRunning = !!_rescan?.running;
    for (const id of ["btn-scan-archive", "btn-idx-build"]) {
      const btn = document.getElementById(id);
      if (!btn) continue;
      if (!btn.dataset.rescanIdleText) {
        btn.dataset.rescanIdleText = btn.textContent;
      }
      btn.disabled = rescanRunning;
      btn.textContent = rescanRunning
        ? `Rescanning ${Math.max(0, Number(_rescan?.percent) || 0)}%…`
        : btn.dataset.rescanIdleText;
    }
    const totalSize = document.getElementById("subs-total-size");
    if (totalSize) {
      totalSize.setAttribute("aria-busy", rescanRunning ? "true" : "false");
      totalSize.classList.toggle("rescan-busy", rescanRunning);
    }

    // Session error count.
    const errEl = document.getElementById("gsb-errors");
    const errCount = document.getElementById("gsb-errors-count");
    if (errEl) errEl.hidden = _errorCount <= 0;
    if (errCount) errCount.textContent = String(_errorCount);
  }

  function _renderErrorsPopover() {
    const body = document.getElementById("gsb-errors-body");
    if (!body) return;
    body.replaceChildren();
    if (!_errorItems.length) {
      const empty = document.createElement("div");
      empty.className = "queue-empty";
      empty.textContent = "No errors this session.";
      body.appendChild(empty);
      return;
    }
    for (const item of [..._errorItems].reverse()) {
      const row = document.createElement("div");
      row.className = "gsb-error-item";
      const dot = document.createElement("span");
      dot.className = "gsb-error-item-dot";
      dot.textContent = "!";
      const text = document.createElement("span");
      text.className = "gsb-error-item-text";
      text.textContent = item;
      row.append(dot, text);
      body.appendChild(row);
    }
  }

  function _positionErrorsPopover(pop, anchor) {
    const br = anchor.getBoundingClientRect();
    pop.style.visibility = "hidden";
    pop.style.display = "flex";
    const pr = pop.getBoundingClientRect();
    pop.style.display = "";
    pop.style.visibility = "";
    let left = Math.max(8, br.right - pr.width);
    if (left + pr.width > window.innerWidth - 8) {
      left = Math.max(8, window.innerWidth - pr.width - 8);
    }
    let top = br.top - pr.height - 6;
    if (top < 8) {
      top = Math.min(window.innerHeight - pr.height - 8, br.bottom + 6);
    }
    pop.style.left = left + "px";
    pop.style.top = Math.max(8, top) + "px";
  }

  function _closeErrorsPopover() {
    document.getElementById("popover-session-errors")
      ?.classList.remove("open");
    document.getElementById("gsb-errors")
      ?.setAttribute("aria-expanded", "false");
  }

  function _toggleErrorsPopover(anchor) {
    const pop = document.getElementById("popover-session-errors");
    if (!pop || !anchor) return;
    const wasOpen = pop.classList.contains("open");
    document.querySelectorAll(".queue-popover.open")
      .forEach(p => p.classList.remove("open"));
    document.getElementById("btn-sync-tasks")
      ?.setAttribute("aria-expanded", "false");
    document.getElementById("btn-gpu-tasks")
      ?.setAttribute("aria-expanded", "false");
    if (wasOpen) {
      anchor.setAttribute("aria-expanded", "false");
      return;
    }
    _renderErrorsPopover();
    _positionErrorsPopover(pop, anchor);
    pop.classList.add("open");
    anchor.setAttribute("aria-expanded", "true");
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
      if (added) {
        for (const m of muts) {
          for (const n of m.addedNodes) {
            if (!isErr(n)) continue;
            const text = String(n.textContent || "").trim();
            if (text) _errorItems.push(text);
          }
        }
        // Bound session-only UI memory without changing the visible count.
        if (_errorItems.length > 500) _errorItems = _errorItems.slice(-500);
        _errorCount += added;
        _renderErrorsPopover();
        _render();
      }
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
    // Log button jumps to the full log. The error segment opens a compact
    // session list; opening it does not acknowledge or erase anything.
    const gotoLog = () => {
      document.querySelector('.tab[data-tab="download"]')?.click();
      const log = document.getElementById("main-log");
      if (log) log.scrollTop = log.scrollHeight;
    };
    document.getElementById("gsb-log-btn")?.addEventListener("click", gotoLog);
    const errorBtn = document.getElementById("gsb-errors");
    errorBtn?.setAttribute("aria-expanded", "false");
    errorBtn?.addEventListener("click", (e) => {
      e.stopPropagation();
      _toggleErrorsPopover(e.currentTarget);
    });
    document.getElementById("gsb-errors-clear")?.addEventListener("click", () => {
      _errorCount = 0;
      _errorItems = [];
      _renderErrorsPopover();
      _render();
      _closeErrorsPopover();
    });
    document.addEventListener("click", () => {
      if (!document.getElementById("popover-session-errors")
          ?.classList.contains("open")) {
        errorBtn?.setAttribute("aria-expanded", "false");
      }
    });
    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape") _closeErrorsPopover();
    });
    window.addEventListener("resize", () => {
      const pop = document.getElementById("popover-session-errors");
      if (pop?.classList.contains("open") && errorBtn) {
        _positionErrorsPopover(pop, errorBtn);
      }
    });

    _wireErrorCounter();
    _renderErrorsPopover();
    _render();

    const hydrateRescan = async () => {
      if (!window.YT?.bridge?.isUp?.()) return;
      try {
        const state = await window.YT.bridge.bridgeCall("archive_rescan_state");
        if (state) window._onArchiveRescanProgress(state);
      } catch (e) { /* best-effort state recovery */ }
    };
    hydrateRescan();
    window.addEventListener("pywebviewready", hydrateRescan, { once: true });
  }

  // Python pushes this after each completed channel in the scan and folder-
  // size phases. It is defined at module load (before app boot) so the first
  // update cannot race status-bar initialization.
  window._onArchiveRescanProgress = function (state) {
    _rescan = state && typeof state === "object" ? { ...state } : null;
    if (_rescanHideTimer) {
      clearTimeout(_rescanHideTimer);
      _rescanHideTimer = null;
    }
    _render();
    if (_rescan && !_rescan.running &&
        ["complete", "error"].includes(_rescan.phase)) {
      _rescanHideTimer = setTimeout(() => {
        _rescan = null;
        _render();
      }, 8000);
    }
  };

  window.initStatusBar = initStatusBar;
})();
