/**
 * web/columnWidth.js — drag-to-resize column widths on Subs + Recent tables, persisted
 *
 * Exposed as window.initColumnWidth; app.js boot calls it once.
 */
(function () {
  "use strict";

  const _browseState = window._browseState || {};
  const showContextMenu = window.showContextMenu || (() => {});
  const askConfirm = window.askConfirm;
  const askDanger = window.askDanger;
  const askQuestion = window.askQuestion;
  const askChoice = window.askChoice;
  function bridgeCall(method, ...args) {
    const fn = window.YT?.bridge?.bridgeCall;
    if (fn) return fn(method, ...args);
    return undefined;
  }

  function nativeBridgeUp() {
    return !!window.YT?.bridge?.isUp?.();
  }

  // ─── Column width persistence (Subs + Recent) ───────────────────────
  function persistColumnWidths() {
    // Wire resize handles immediately — these don't need the API.
    // Since HTML tables don't natively support col drag-resize, add a
    // simple mousedown-on-th-border handler that updates <col> width.
    _wireColResize(".subs-table", "subs");
    _wireColResize(".recent-table", "recent");

    // Apply saved widths — needs pywebview.api, which may not be ready
    // at DOMContentLoaded. Retry on `pywebviewready` + poll fallback to
    // cover the case where the event already fired before we listened.
    // Without this, saved widths were silently dropped on every boot
    // (reported: Subs column widths don't persist across restart).
    const applySaved = () => {
      if (!nativeBridgeUp()) return false;
      bridgeCall("window_state_load").then((st) => {
        if (!st || !st.col_widths) return;
        _applyColWidths(".subs-table", st.col_widths.subs);
        _applyColWidths(".recent-table", st.col_widths.recent);
      }).catch(() => {});
      return true;
    };
    if (!applySaved()) {
      window.addEventListener("pywebviewready", () => { applySaved(); },
                              { once: true });
      // Belt-and-suspenders: poll briefly in case `pywebviewready` fired
      // before this listener was attached (same pattern as queue-auto).
      let tries = 0;
      const poll = () => {
        if (applySaved()) return;
        if (++tries < 20) setTimeout(poll, 150);
      };
      setTimeout(poll, 150);
    }
  }

  function _applyColWidths(tableSel, widths) {
    if (!widths) return;
    const table = document.querySelector(tableSel);
    if (!table) return;
    const cols = table.querySelectorAll("colgroup col");
    const ths = table.querySelectorAll("thead th");
    ths.forEach((th, i) => {
      const key = th.dataset.sort || th.textContent.trim().toLowerCase();
      if (widths[key] && cols[i]) cols[i].style.width = widths[key] + "px";
    });
  }

  function _wireColResize(tableSel, saveKey) {
    const table = document.querySelector(tableSel);
    if (!table) return;
    const ths = table.querySelectorAll("thead th");

    // Shared flag — when set, the next `click` on any th is swallowed so
    // a resize-drag doesn't also trigger a column sort. User feedback: // resize "acts weird" because sort fires alongside.
    if (!table._resizeState) {
      table._resizeState = { suppressNextSortClick: false };
      table.addEventListener("click", (e) => {
        if (table._resizeState.suppressNextSortClick && e.target.closest("th")) {
          e.stopImmediatePropagation();
          e.preventDefault();
          table._resizeState.suppressNextSortClick = false;
        }
      }, true); // capture phase so we win before sort's bubble-phase handler
    }

    ths.forEach((th, i) => {
      // Add a grab handle on the right edge of the header cell.
      // Skip the last column — nowhere for its extra width to go with
      // `table-layout: fixed` + `overflow-x: hidden`.
      if (th.querySelector(".col-resizer")) return;
      if (i === ths.length - 1) return;
      const handle = document.createElement("div");
      handle.className = "col-resizer";
      th.style.position = "relative";
      th.appendChild(handle);
      let startX = 0, startW = 0, startNextW = 0;
      handle.addEventListener("mousedown", (e) => {
        e.stopPropagation();
        e.preventDefault();
        startX = e.clientX;
        const cols = table.querySelectorAll("colgroup col");
        const thisCol = cols[i];
        const nextCol = cols[i + 1];
        if (!thisCol || !nextCol) return;
        // Seed from the current rendered widths so dragging feels accurate
        // even if this is the first time the user touches the divider.
        const ths2 = table.querySelectorAll("thead th");
        startW = ths2[i] ?.getBoundingClientRect().width || 100;
        startNextW = ths2[i + 1]?.getBoundingClientRect().width || 100;
        document.body.style.cursor = "col-resize";
        table._resizeState.suppressNextSortClick = true;
        const move = (ev) => {
          let dw = ev.clientX - startX;
          // Clamp so neither this nor the next column goes below 40px.
          dw = Math.max(-(startW - 40), Math.min(startNextW - 40, dw));
          thisCol.style.width = (startW + dw) + "px";
          nextCol.style.width = (startNextW - dw) + "px";
        };
        const up = () => {
          document.removeEventListener("mousemove", move);
          document.removeEventListener("mouseup", up);
          document.body.style.cursor = "";
          // Save widths — matches YTArchiver.py chan_col_widths persistence.
          const widths = {};
          table.querySelectorAll("thead th").forEach((t, idx) => {
            const key = t.dataset.sort || t.textContent.trim().toLowerCase();
            const col = table.querySelectorAll("colgroup col")[idx];
            if (col && col.style.width) {
              widths[key] = parseInt(col.style.width);
            }
          });
          if (nativeBridgeUp()) {
            bridgeCall("window_state_load").then((st) => {
              const cw = (st && st.col_widths) || {};
              cw[saveKey] = widths;
              bridgeCall("window_state_save", { col_widths: cw });
            });
          }
        };
        document.addEventListener("mousemove", move);
        document.addEventListener("mouseup", up);
      });
      // Also swallow click on the handle itself to keep sort inert.
      handle.addEventListener("click", (e) => {
        e.stopPropagation();
        e.preventDefault();
      });
    });
  }

  // Patch 14 phase 2: Graph sub-view moved to web/graphTab.js.
  // window.initGraphView / drawGraph / populateGraphChannels / _drillIntoSearch
  // remain as back-compat globals.

  // Wire Bookmarks → Export CSV button (backend shows save dialog)
  function initBookmarksExport() {
    const btn = document.getElementById("btn-bookmarks-export");
    if (!btn) return;
    btn.addEventListener("click", async () => {
      if (!nativeBridgeUp()) {
        window._showToast?.("Native mode required for export.", "warn");
        return;
      }
      const res = await bridgeCall("bookmark_export_csv");
      if (res?.ok) {
        window._showToast?.(`Exported ${res.count} bookmark(s) to CSV.`, "ok");
      } else if (res?.cancelled) {
        /* no-op */
      } else {
        window._showToast?.(res?.error || "Export failed.", "error");
      }
    });
  }

  window.persistColumnWidths = persistColumnWidths;
  window.initBookmarksExport = initBookmarksExport;
})();
