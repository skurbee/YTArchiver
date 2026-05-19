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

  // ── Header version label ─────────────────────────────────────────
  function _updateHeaderVersion() {
    const el = document.getElementById("header-version");
    if (!el) return;
    const api = window.pywebview?.api;
    if (!api || !api.get_header_version) {
      // Retry shortly — pywebview may not have bound the api yet.
      setTimeout(_updateHeaderVersion, 200);
      return;
    }
    api.get_header_version().then((r) => {
      if (!r) return;
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
        // Clear any lingering row-selected highlights when switching
        // tabs. Without this, rows selected in Recent / Browse grid
        // stay selected behind the scenes; coming back to that tab
        // and clicking "Delete Selected" operates on rows the user
        // thought they deselected.
        try {
          document.querySelectorAll("tr.row-selected, .row-selected")
            .forEach(r => r.classList.remove("row-selected"));
          // Also hide the "Delete File" / "Delete N files" button tied
          // to the Recent table selection state — its visibility is
          // driven by row selection; clearing selection should hide it.
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

    window.addEventListener("mousemove", (e) => {
      if (!dragging) return;
      const dy = e.clientY - startY;
      const containerH = container.getBoundingClientRect().height;
      const splitterH = 6;
      const minTop = 32;
      const maxTop = containerH - 80 - splitterH;
      let newH = Math.max(minTop, Math.min(maxTop, startTopH + dy));
      top.style.flex = `0 0 ${newH}px`;
    });

    window.addEventListener("mouseup", () => {
      if (dragging) {
        dragging = false;
        document.body.style.cursor = "";
      }
    });
  }
  window.initSplitter = initSplitter;
})();
