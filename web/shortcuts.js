/**
 * web/shortcuts.js — global keyboard shortcuts.
 *
 * Shortcuts:
 *   Ctrl+Q    — Quit (window_quit via pywebview)
 *   Ctrl+F    — Focus Browse > Search input (switches view if hidden)
 *   F5        — Reload (dev)
 *   F11       — Fullscreen toggle (native via pywebview, HTML5 fallback)
 *   Ctrl+L    — Focus URL field on Download tab
 *   Ctrl+S    — Sync Subbed
 *   Ctrl+K    — Jump to Subs tab + focus filter
 *   Ctrl+P    — Open Sync Tasks popover
 *   1-5       — Switch tabs by number
 *   Escape    — Close context menus + queue popovers
 *
 * When ANY .askq-backdrop modal is open, every shortcut except Esc/Enter
 * is suppressed — modals own input focus.
 *
 * Exposed as window.initKeyboardShortcuts; app.js boot calls it once.
 *
 * Depends on: window.closeContextMenu (from contextMenu.js).
 */
(function () {
  "use strict";

  function initKeyboardShortcuts() {
    document.addEventListener("keydown", (e) => {
      const tag = e.target.tagName;
      const editing = tag === "INPUT" || tag === "TEXTAREA" ||
                      e.target.isContentEditable;

      // When ANY modal is open (askq backdrop), every shortcut except
      // Esc/Enter is blocked. Modals own input focus — Ctrl+S firing
      // Sync while a "Delete files?" confirm is up was a real footgun.
      const _modalOpen = !!document.querySelector(".askq-backdrop");
      if (_modalOpen && e.key !== "Escape" && e.key !== "Enter") {
        return;
      }

      // Ctrl+Q: quit (close window via tray-quit path)
      if ((e.ctrlKey || e.metaKey) && e.key === "q") {
        e.preventDefault();
        if (window.pywebview?.api?.window_quit) window.pywebview.api.window_quit();
        return;
      }
      // Ctrl+F: focus Browse > Search input. If Search view isn't
      // currently visible, SWITCH to it + focus.
      if ((e.ctrlKey || e.metaKey) && e.key === "f") {
        e.preventDefault();
        const searchInput = document.getElementById("search-query");
        const browseTab = document.querySelector('.tab[data-tab="browse"]');
        const viewSearch = document.getElementById("view-search");
        if (viewSearch && viewSearch.style.display === "none") {
          browseTab?.click();
          document.querySelector('[data-browse-sub="search"]')?.click();
          setTimeout(() => {
            const si = document.getElementById("search-query");
            if (si) { si.focus(); si.select(); }
          }, 80);
        } else if (searchInput) {
          searchInput.focus();
          searchInput.select();
        }
        return;
      }
      // F5: reload (dev convenience)
      if (e.key === "F5") {
        e.preventDefault();
        location.reload();
        return;
      }
      // F11: fullscreen toggle (native via pywebview, HTML5 in browser preview)
      if (e.key === "F11") {
        e.preventDefault();
        const api = window.pywebview?.api;
        if (api?.window_toggle_fullscreen) {
          api.window_toggle_fullscreen();
        } else if (document.fullscreenElement) {
          document.exitFullscreen?.();
        } else {
          document.documentElement.requestFullscreen?.();
        }
        return;
      }
      // Ctrl+L: focus the URL field on Download tab
      if ((e.ctrlKey || e.metaKey) && e.key === "l") {
        e.preventDefault();
        const tabs = document.querySelectorAll(".tab");
        tabs[0]?.click();
        const input = document.querySelector("#panel-download .ctl-input");
        if (input) { input.focus(); input.select(); }
        return;
      }
      // Ctrl+S: Sync Subbed
      if ((e.ctrlKey || e.metaKey) && e.key === "s") {
        e.preventDefault();
        document.getElementById("btn-sync-subbed")?.click();
        return;
      }
      // Ctrl+K: jump to Subs tab + focus filter.
      if ((e.ctrlKey || e.metaKey) && e.key === "k") {
        e.preventDefault();
        document.querySelector('.tab[data-tab="subs"]')?.click();
        setTimeout(() => {
          const f = document.getElementById("subs-filter");
          if (f) { f.focus(); f.select?.(); }
        }, 60);
        return;
      }
      // Ctrl+P: open Sync Tasks popover
      if ((e.ctrlKey || e.metaKey) && e.key === "p") {
        e.preventDefault();
        document.getElementById("btn-sync-tasks")?.click();
        return;
      }
      // Number keys 1-5: switch tabs (Download / Subs / Recent / Settings / Browse)
      if (!editing && /^[1-5]$/.test(e.key)) {
        const tabs = document.querySelectorAll(".tab");
        const idx = parseInt(e.key, 10) - 1;
        if (tabs[idx]) tabs[idx].click();
        return;
      }
      // Escape: close context menus, popovers, dialogs
      if (e.key === "Escape") {
        window.closeContextMenu?.();
        document.querySelectorAll(".queue-popover.open").forEach(p => p.classList.remove("open"));
      }
    });
  }
  window.initKeyboardShortcuts = initKeyboardShortcuts;
})();
