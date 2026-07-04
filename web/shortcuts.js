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

  function bridgeCall(method, ...args) {
    const fn = window.YT?.bridge?.bridgeCall;
    if (fn) return fn(method, ...args);
    return undefined;
  }

  function nativeBridgeUp() {
    return !!window.YT?.bridge?.isUp?.();
  }

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
      // Block Ctrl+S/F/K/P + F5 from firing while the user is typing
      // into an input/textarea/contenteditable — these shortcuts
      // would otherwise hijack the keystroke and surprise the user
      // by losing unsaved input (audit: shortcuts.js H136, H145).
      if (editing) {
        if ((e.ctrlKey || e.metaKey)
            && ["s", "f", "k", "p"].includes(e.key)) return;
        if (e.key === "F5") return;
      }

      // "?" (Shift+/): toggle the keyboard-shortcuts help overlay. Only
      // when not typing, so "?" in a text field types normally.
      if (!editing && e.key === "?" && !e.ctrlKey && !e.metaKey && !e.altKey) {
        e.preventDefault();
        toggleShortcutsHelp();
        return;
      }
      // Ctrl+Q: quit (close window via tray-quit path)
      if ((e.ctrlKey || e.metaKey) && e.key === "q") {
        e.preventDefault();
        if (nativeBridgeUp()) bridgeCall("window_quit");
        return;
      }
      // Ctrl+F: focus Browse > Search input. If Search view isn't
      // currently visible, SWITCH to it + focus.
      if ((e.ctrlKey || e.metaKey) && e.key === "f") {
        e.preventDefault();
        const searchInput = document.getElementById("search-query");
        const browseTab = document.querySelector('.tab[data-tab="browse"]');
        const viewSearch = document.getElementById("view-search");
        if (viewSearch && viewSearch.hidden) {
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
        if (nativeBridgeUp()) {
          bridgeCall("window_toggle_fullscreen");
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
      // Ctrl+K: open the command palette (search actions + channels).
      if ((e.ctrlKey || e.metaKey) && e.key === "k") {
        e.preventDefault();
        if (typeof window.openCommandPalette === "function") {
          window.openCommandPalette();
        } else {
          // Fallback to the old behavior if the palette isn't loaded.
          document.querySelector('.tab[data-tab="subs"]')?.click();
          setTimeout(() => {
            const f = document.getElementById("subs-filter");
            if (f) { f.focus(); f.select?.(); }
          }, 60);
        }
        return;
      }
      // Ctrl+P: open Sync Tasks popover
      if ((e.ctrlKey || e.metaKey) && e.key === "p") {
        e.preventDefault();
        document.getElementById("btn-sync-tasks")?.click();
        return;
      }
      // Number keys 1-5: switch tabs (Download / Subs / Browse / Health / Settings).
      if (!editing && /^[1-5]$/.test(e.key)) {
        const tabs = document.querySelectorAll(".tab");
        const idx = parseInt(e.key, 10) - 1;
        if (tabs[idx]) tabs[idx].click();
        return;
      }
      // Escape: close context menus, popovers, dialogs — but ONLY
      // when no modal is open. With a modal up, Esc should reach
      // the modal's own handler instead of closing popovers behind
      // it (audit: shortcuts.js L67).
      if (e.key === "Escape" && !_modalOpen) {
        if (_closeShortcutsHelp()) return;
        window.closeContextMenu?.();
        document.querySelectorAll(".queue-popover.open").forEach(p => p.classList.remove("open"));
      }
    });

    // Visible entry point so the shortcuts are discoverable, not just
    // guessable. The "?" button lives in the header strip.
    document.getElementById("btn-shortcuts-help")
      ?.addEventListener("click", (e) => { e.preventDefault(); toggleShortcutsHelp(); });
  }

  // ── Keyboard-shortcuts help overlay ──────────────────────────────────
  // Built lazily on first open. The list is authored here (single source)
  // rather than scraped from the handlers above.
  const SHORTCUTS = [
    ["Ctrl + S", "Sync all subscribed channels"],
    ["Ctrl + L", "Focus the video-URL field (Download tab)"],
    ["Ctrl + F", "Search transcripts + titles"],
    ["Ctrl + K", "Command palette (search actions + channels)"],
    ["Ctrl + P", "Open the sync queue"],
    ["1 - 5", "Switch tabs (Download / Subs / Browse / Health / Settings)"],
    ["F11", "Toggle fullscreen"],
    ["Ctrl + Q", "Quit YTArchiver"],
    ["Esc", "Close menus, popovers, and dialogs"],
    ["?", "Show / hide this list"],
  ];

  function _closeShortcutsHelp() {
    const el = document.getElementById("shortcuts-help-backdrop");
    if (el) { el.remove(); return true; }
    return false;
  }

  function toggleShortcutsHelp() {
    if (_closeShortcutsHelp()) return;
    const backdrop = document.createElement("div");
    backdrop.id = "shortcuts-help-backdrop";
    backdrop.className = "shortcuts-help-backdrop";
    const rows = SHORTCUTS.map(([k, d]) =>
      `<div class="shk-row"><kbd class="shk-key">${k}</kbd>` +
      `<span class="shk-desc">${d}</span></div>`).join("");
    backdrop.innerHTML =
      `<div class="shortcuts-help" role="dialog" aria-label="Keyboard shortcuts">` +
      `<div class="shk-title">Keyboard shortcuts</div>${rows}` +
      `<div class="shk-hint">Press <kbd class="shk-key">?</kbd> or ` +
      `<kbd class="shk-key">Esc</kbd> to close</div></div>`;
    backdrop.addEventListener("click", (e) => {
      if (e.target === backdrop) _closeShortcutsHelp();
    });
    document.body.appendChild(backdrop);
  }

  window.initKeyboardShortcuts = initKeyboardShortcuts;
})();
