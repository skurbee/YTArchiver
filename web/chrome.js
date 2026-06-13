/**
 * web/chrome.js — top-level UI chrome wiring.
 *
 * Three small helpers grouped together because each is a self-contained
 * piece of "shell" UI that has nothing to do with the per-tab feature
 * code that fills 90% of app.js:
 *
 *   - _updateHeaderVersion()   → fetches version + date from the backend
 *                                 and writes them into `#header-version`.
 *                                 Self-wires on pywebviewready +
 *                                 DOMContentLoaded.
 *   - window.initTabs()         → click-handler wiring for the .tab
 *                                 elements; coordinates "pause Watch
 *                                 video when switching away from Browse"
 *                                 and clears row-selection highlights.
 *                                 Called from app.js boot.
 *   - window.initSplitter()     → mousedown/mousemove/mouseup drag on
 *                                 the activity-log vs main-log divider.
 *                                 Called from app.js boot.
 *
 * Loaded BEFORE app.js so app.js can call window.initTabs() / initSplitter().
 */
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

  // ── Header version label ─────────────────────────────────────────
  // Bounded retry + once-only guard so a stuck pywebview bind doesn't
  // spin setTimeout forever, and so the double-event wiring (both
  // pywebviewready AND DOMContentLoaded) doesn't fire the API twice
  // (audit: chrome.js H129, H139).
  let _hdrVerTries = 0;
  let _hdrVerDone = false;
  function _updateHeaderVersion() {
    if (_hdrVerDone) return;
    const el = document.getElementById("header-version");
    if (!el) return;
    if (!nativeBridgeUp()) {
      if (_hdrVerTries++ > 40) return;  // ~8s of retries, then give up
      setTimeout(_updateHeaderVersion, 200);
      return;
    }
    bridgeCall("get_header_version").then((r) => {
      if (!r) return;
      _hdrVerDone = true;
      const v = r.version || "";
      const d = r.date || "";
      el.textContent = d ? `${v} - ${d}` : v;
    }).catch((e) => {
      console.warn("get_header_version failed:", e);
    });
  }
  window.addEventListener("pywebviewready", _updateHeaderVersion);
  // Belt-and-suspenders: also try on DOMContentLoaded in case pywebview
  // fired its ready event before our listener attached.
  document.addEventListener("DOMContentLoaded", _updateHeaderVersion);

  // ── Tab switching ────────────────────────────────────────────────
  function initTabs() {
    const tabs = document.querySelectorAll(".tab");
    tabs.forEach(t => {
      t.addEventListener("click", () => {
        // Switching AWAY from Browse with a Watch-view <video> playing:
        // pause it but don't unload. `display:none` doesn't stop HTML5
        // audio, and returning to Browse should resume right where the
        // user left off rather than tearing the video into a placeholder.
        const currentlyActive = document.querySelector(".tab.active")?.dataset.tab;
        const target = t.dataset.tab;
        if (currentlyActive === "browse" && target !== "browse" &&
            typeof window._pauseWatchVideo === "function") {
          window._pauseWatchVideo();
        }
        tabs.forEach(x => x.classList.remove("active"));
        t.classList.add("active");
        document.querySelectorAll(".tab-panel").forEach(p => p.classList.remove("active"));
        const panel = document.getElementById("panel-" + target);
        if (panel) panel.classList.add("active");
        // Re-entering Subs: clear any lingering filter text so the box starts
        // fresh (parity with Browse's clear-on-view-enter; previously the Subs
        // filter persisted across tab switches, so re-typing doubled it).
        if (target === "subs" && typeof window._clearSubsFilter === "function") {
          window._clearSubsFilter();
        }
        // Returning to Browse: the Videos sub-grid is a one-time render that
        // doesn't re-query on its own, so a download that landed while the
        // user was on another tab wouldn't appear until they changed the sort.
        // Re-check page 1 on return and re-render only if it actually changed
        // (no-op + no flash when nothing was added). rAF so the panel's
        // visibility has settled before the active-view check runs.
        if (target === "browse" && typeof window._refreshVideosViewIfActive === "function") {
          requestAnimationFrame(() => {
            try { window._refreshVideosViewIfActive(); } catch (_e) { /* non-fatal */ }
          });
        }
        // Clear any lingering row-selected highlights when switching
        // tabs. Without this, rows selected in Recent / Browse grid
        // stay selected behind the scenes; coming back to that tab
        // and clicking "Delete Selected" operates on rows the user
        // thought they deselected.
        try {
          // Scope the clear to ONLY rows in the panel being LEFT.
          // The previous document-wide query nuked intentional
          // selections in panels the user wasn't actively touching
          // (audit: chrome.js H144). The Delete button stays hidden
          // because its visibility tracks the visible panel's
          // selection state.
          const leavingPanel = document.getElementById("panel-" + currentlyActive);
          if (leavingPanel) {
            leavingPanel.querySelectorAll("tr.row-selected, .row-selected")
              .forEach(r => r.classList.remove("row-selected"));
          }
          const delBtn = document.getElementById("btn-delete-file");
          if (delBtn) delBtn.hidden = true;
        } catch (_e) { /* non-fatal */ }
      });
    });
  }
  window.initTabs = initTabs;

  // ── Paned splitter (drag to resize activity log vs main log) ─────
  function initSplitter() {
    const splitter = document.getElementById("paned-splitter");
    const top = document.querySelector(".activity-log-frame");
    const container = document.getElementById("log-paned");
    if (!splitter || !top || !container) return;

    let dragging = false;
    let startY = 0;
    let startTopH = 0;

    splitter.addEventListener("mousedown", (e) => {
      dragging = true;
      startY = e.clientY;
      startTopH = top.getBoundingClientRect().height;
      document.body.style.cursor = "ns-resize";
      e.preventDefault();
    });

    // Cache container height at mousedown so we don't read layout
    // on every mousemove (audit: chrome.js L74). rAF batches style
    // writes so 60 mousemoves per second collapse into one paint.
    let _splitContainerH = 0;
    splitter.addEventListener("mousedown", () => {
      _splitContainerH = container.getBoundingClientRect().height;
    });
    let _mmPending = false;
    let _mmLastY = 0;
    window.addEventListener("mousemove", (e) => {
      if (!dragging) return;
      _mmLastY = e.clientY;
      if (_mmPending) return;
      _mmPending = true;
      requestAnimationFrame(() => {
        _mmPending = false;
        if (!dragging) return;
        const dy = _mmLastY - startY;
        const containerH = _splitContainerH
          || container.getBoundingClientRect().height;
        const splitterH = 6;
        const minTop = 32;
        const maxTop = containerH - 80 - splitterH;
        const newH = Math.max(minTop, Math.min(maxTop, startTopH + dy));
        top.style.flex = `0 0 ${newH}px`;
      });
    });

    function _endDrag() {
      if (dragging) {
        dragging = false;
        document.body.style.cursor = "";
      }
    }
    window.addEventListener("mouseup", _endDrag);
    // Also end the drag if the mouse is released outside the window
    // (audit: chrome.js H130) — `mouseup` doesn't fire in that case
    // and the splitter would stay in drag mode until the next click.
    window.addEventListener("blur", _endDrag);
    document.addEventListener("mouseleave", _endDrag);
  }
  window.initSplitter = initSplitter;
})();
