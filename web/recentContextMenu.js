/**
 * web/recentContextMenu.js — right-click menu + extended multi-select for the Recent table
 *
 * Exposed as window.initRecentContextMenu; app.js boot calls it once.
 */
(function () {
  "use strict";

  const _browseState = window._browseState || {};
  const showContextMenu = window.showContextMenu || (() => {});
  const askConfirm = window.askConfirm;
  const askDanger = window.askDanger;
  const askQuestion = window.askQuestion;
  function bridgeCall(method, ...args) {
    const fn = window.YT?.bridge?.bridgeCall;
    if (fn) return fn(method, ...args);
    return undefined;
  }

  // ─── Recent tab context menu (matches _recent_ctx_menu at line 33174) ─
  function initRecentContextMenu() {
    const tbody = document.getElementById("recent-table-body");
    if (!tbody) return;

    // Clicking the header row (th) deselects any row in the tbody.
    // Matches YTArchiver.py:32306 _header_deselect_browse.
    const thead = tbody.parentElement?.querySelector("thead");
    thead?.addEventListener("click", (e) => {
      if (e.target.closest(".col-resizer")) return; // don't deselect on resize
      tbody.querySelectorAll("tr.row-selected").forEach(r => r.classList.remove("row-selected"));
    });

    // Extended multi-select: Ctrl/Cmd-click toggles a row, Shift-click
    // selects a range from last anchor to clicked row. Matches
    // YTArchiver.py:32411 `selectmode="extended"`.
    let _anchor = null;
    tbody.addEventListener("click", (e) => {
      const tr = e.target.closest("tr");
      if (!tr) return;
      // Don't clobber selection if the click was on a button/link inside the row.
      if (e.target.closest("button, a, input")) return;
      const rows = [...tbody.querySelectorAll("tr")];
      if (e.shiftKey && _anchor) {
        const a = rows.indexOf(_anchor);
        const b = rows.indexOf(tr);
        if (a === -1 || b === -1) return;
        const lo = Math.min(a, b), hi = Math.max(a, b);
        rows.forEach((r, i) => {
          r.classList.toggle("row-selected", i >= lo && i <= hi);
        });
      } else if (e.ctrlKey || e.metaKey) {
        tr.classList.toggle("row-selected");
        _anchor = tr;
      } else {
        rows.forEach(r => r.classList.remove("row-selected"));
        tr.classList.add("row-selected");
        _anchor = tr;
      }
    });
    // Click outside any row deselects everything. Scoped to the recent
    // panel so clicking the main log or a different tab doesn't fire.
    const recentPanel = document.getElementById("panel-recent") ||
                        tbody.closest(".tab-panel") || document;
    recentPanel.addEventListener("click", (e) => {
      if (e.target.closest("#recent-table-body tr")) return;
      if (e.target.closest(".recent-table thead")) return;
      if (e.target.closest("button, a, input, select")) return;
      tbody.querySelectorAll("tr.row-selected").forEach(r => r.classList.remove("row-selected"));
      _anchor = null;
    });

    // keyboard handler for Recent table (Delete / Enter /
    // F2) to match what the Subs tbody supports. Without this, a user
    // selecting a Recent row couldn't delete it or open it in
    // Watch without right-clicking — keyboard workflow was broken.
    tbody.setAttribute("tabindex", "0");
    tbody.addEventListener("keydown", async (e) => {
      const selected = tbody.querySelector("tr.row-selected");
      if (!selected) return;
      const title = (selected.querySelector(".col-title")?.textContent || "").trim();
      const channel = (selected.querySelector(".col-channel")?.textContent || "").trim();
      if (e.key === "Delete") {
        e.preventDefault();
        const ok = await askDanger("Delete file",
          `Permanently delete "${title}" from disk?\n\nThis cannot be undone.`,
          "Delete");
        if (!ok) return;
        try {
          await bridgeCall("recent_delete_file", title, channel);
          const rows = await bridgeCall("get_recent_downloads");
          if (rows && typeof window.renderRecentTable === "function") {
            window.renderRecentTable(rows);
          }
        } catch {}
      } else if (e.key === "Enter") {
        e.preventDefault();
        const fp = selected.dataset?.filepath || "";
        if (fp) bridgeCall("browse_open_video", fp);
      } else if (e.key === "ArrowDown" || e.key === "ArrowUp") {
        e.preventDefault();
        const all = [...tbody.querySelectorAll("tr")];
        const idx = all.indexOf(selected);
        const next = e.key === "ArrowDown"
          ? Math.min(all.length - 1, idx + 1)
          : Math.max(0, idx - 1);
        all.forEach(t => t.classList.remove("row-selected"));
        all[next].classList.add("row-selected");
        all[next].scrollIntoView({ block: "nearest" });
      }
    });

    tbody.addEventListener("contextmenu", (e) => {
      const tr = e.target.closest("tr");
      if (!tr) return;
      e.preventDefault();
      // Respect existing selection if the right-clicked row is part of it;
      // otherwise select just this one.
      if (!tr.classList.contains("row-selected")) {
        tbody.querySelectorAll("tr.row-selected").forEach(x => x.classList.remove("row-selected"));
        tr.classList.add("row-selected");
      }
      const selected = [...tbody.querySelectorAll("tr.row-selected")];
      const n = selected.length;
      // trim textContent so indicator characters (●, ⚠)
      // or trailing whitespace in the rendered row don't leak into
      // the title/channel lookup. Exact-match on the backend side
      // would silently fail for rows with any prefix/suffix decor.
      const title = (tr.querySelector(".col-title")?.textContent || "").trim();
      const channel = (tr.querySelector(".col-channel")?.textContent || "").trim();
      const items = n > 1 ? [
        { label: `Delete ${n} files`, cls: "dim",
          action: async () => {
            const ok = await askDanger(
              `Delete ${n} files`,
              `Permanently delete ${n} file(s) from disk?\n\nThis cannot be undone.`,
              "Delete");
            if (!ok) return;
            // await each call instead of fire-and-forget, then
            // refresh the table so deleted rows disappear immediately
            // instead of lingering until tab switch.
            for (const row of selected) {
              const t = row.querySelector(".col-title")?.textContent;
              const c = row.querySelector(".col-channel")?.textContent;
              try {
                await bridgeCall("recent_delete_file", t, c);
              } catch {}
            }
            try {
              const rows = await bridgeCall("get_recent_downloads");
              if (rows && typeof window.renderRecentTable === "function") {
                window.renderRecentTable(rows);
              }
            } catch {}
          }},
      ] : [
        // "Play video" opens the Browse Watch view (embedded HTML5 <video>
        // + karaoke transcript), NOT a separate VLC window.
        { label: "Play video", action: async () => {
          // Resolve filepath + video_id via the recent lookup, then hand off
          // to the shared Watch-view opener used by the Browse grid.
          let fp = "", vid = "";
          try {
            const res = await bridgeCall("recent_resolve", title, channel);
            if (res?.ok) { fp = res.filepath || ""; vid = res.video_id || ""; }
          } catch {}
          if (!fp) {
            window._showToast?.("Couldn't locate file on disk.", "error");
            return;
          }
          window._openVideoInWatch?.({
            title, channel, filepath: fp, video_id: vid,
          });
        }},
        { label: "Play in external player", action: () => bridgeCall("recent_play", title, channel) },
        { label: "Show in Explorer", action: () => bridgeCall("recent_show_in_explorer", title, channel) },
        { label: "Open video on YouTube", action: () => bridgeCall("recent_open_youtube", title, channel) },
        { sep: true },
        // Re-queue download — fetches this video's URL from recent_downloads
        // and re-drives the single-video flow. Mirrors OLD's Recent menu
        // item (YTArchiver.py:33174 + friends).
        { label: "Re-queue download",
          action: async () => {
            const r = await bridgeCall("recent_requeue", title, channel);
            if (r?.ok) {
              window._showToast?.("Re-queued download.", "ok");
            } else {
              window._showToast?.(r?.error || "Could not requeue.", "error");
            }
          }},
        { sep: true },
        { label: "Delete File", cls: "dim",
          action: async () => {
            const ok = await askDanger("Delete file",
              `Delete "${title}" from disk?\n\nThis cannot be undone.`, "Delete");
            if (!ok) return;
            // refresh Recent after delete so the row
            // disappears instantly instead of needing a tab switch.
            try {
              await bridgeCall("recent_delete_file", title, channel);
              const rows = await bridgeCall("get_recent_downloads");
              if (rows && typeof window.renderRecentTable === "function") {
                window.renderRecentTable(rows);
              }
            } catch {}
          }},
      ];
      showContextMenu(e.clientX, e.clientY, items);
    });

    // Clear list button — empties the recent-downloads list (does NOT
    // delete files on disk). The button itself is shown/hidden by the
    // renderer based on whether there are rows. .txt: handler was
    // missing in the pywebview port, so the button did nothing.
    const clearListBtn = document.getElementById("btn-clear-recent");
    if (clearListBtn && !clearListBtn._wired) {
      clearListBtn._wired = true;
      clearListBtn.addEventListener("click", async () => {
        const ok = await askDanger(
          "Clear Recent Downloads",
          "Clear the entire recent downloads list?\n\n"
            + "This only removes the list entries — downloaded files are not deleted.",
          "Clear list");
        if (!ok) return;
        try {
          const res = await bridgeCall("clear_recent_downloads");
          if (res?.ok) {
            window._showToast?.("Recent list cleared.", "ok");
            try {
              const rows = await bridgeCall("get_recent_downloads");
              if (typeof window.renderRecentTable === "function") {
                window.renderRecentTable(rows || []);
              }
            } catch {}
          } else {
            window._showToast?.(res?.error || "Could not clear list.", "error");
          }
        } catch (err) {
          window._showToast?.(String(err), "error");
        }
      });
    }
  }

  window.initRecentContextMenu = initRecentContextMenu;
})();
