/**
 * web/logContextMenu.js — right-click menus for every log surface.
 *
 * Attaches a contextmenu handler to each of:
 *   #main-log, #activity-log, #subs-mini-log, #recent-mini-log,
 *   #browse-mini-log, #settings-mini-log
 *
 * The menu offers Copy selection / Copy this line / Copy all (Log) /
 * Save to file / Clear.
 *
 * Exposed as window.initLogContextMenu; app.js boot calls it once.
 *
 * Depends on:
 *   - window.showContextMenu / closeContextMenu (from contextMenu.js)
 *   - window.askConfirm (from modals.js)
 *   - window._showToast (from toasts.js)
 *   - window._syncActivityLogVisibility (from app.js)
 *   - pywebview.api.save_text_to_file (backend)
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

  async function copyText(text, label = "Copied.") {
    if (!navigator.clipboard?.writeText) {
      window._showToast?.("Clipboard unavailable.", "error");
      return;
    }
    try {
      await navigator.clipboard.writeText(text);
      window._showToast?.(label, "ok");
    } catch (_err) {
      window._showToast?.("Copy failed.", "error");
    }
  }

  function initLogContextMenu() {
    const handlers = [
      { el: document.getElementById("main-log"), label: "Main log" },
      { el: document.getElementById("activity-log"), label: "Activity log" },
      { el: document.getElementById("subs-mini-log"), label: "Mini log" },
      { el: document.getElementById("recent-mini-log"), label: "Mini log" },
      { el: document.getElementById("browse-mini-log"), label: "Mini log" },
      { el: document.getElementById("settings-mini-log"), label: "Mini log" },
    ];
    for (const { el, label } of handlers) {
      if (!el) continue;
      el.addEventListener("contextmenu", (e) => {
        e.preventDefault();
        const sel = window.getSelection()?.toString() || "";
        // Fall back to elementFromPoint when closest fails — clicks
        // on empty cell space or pseudo-element content can miss the
        // .log-line ancestor via `e.target.closest` alone (audit:
        // logContextMenu.js H200).
        let lineEl = e.target.closest(".log-line");
        if (!lineEl) {
          const _at = document.elementFromPoint(e.clientX, e.clientY);
          lineEl = _at && _at.closest ? _at.closest(".log-line") : null;
        }
        const items = [];
        if (sel) {
          items.push({ label: "Copy selection",
            action: () => copyText(sel) });
        }
        if (lineEl) {
          items.push({ label: "Copy this line",
            action: () => copyText(lineEl.innerText) });
        }
        items.push({ label: `Copy all (${label})`,
          action: () => copyText(el.innerText) });
        items.push({ sep: true });
        items.push({ label: "Save to file…",
          action: async () => {
            const text = el.innerText;
            if (nativeBridgeUp()) {
              const res = await bridgeCall("save_text_to_file", "ytarchiver_log.txt", text);
              if (res?.ok) window._showToast?.("Log saved.", "ok");
              else window._showToast?.(res?.error || "Save failed.", "error");
            } else {
              // Fallback: blob download (works in browser preview)
              try {
                const blob = new Blob([text], { type: "text/plain" });
                const url = URL.createObjectURL(blob);
                const a = document.createElement("a");
                a.href = url; a.download = "ytarchiver_log.txt"; a.click();
                setTimeout(() => URL.revokeObjectURL(url), 1000);
                window._showToast?.("Log download started.", "ok");
              } catch (_err) {
                window._showToast?.("Save failed.", "error");
              }
            }
          }});
        items.push({ sep: true });
        items.push({ label: "Clear", cls: "dim",
          action: async () => {
            const ok = await window.askConfirm("Clear log",
              `Clear the ${label.toLowerCase()}?\n\nThis only clears the visible log — no files on disk are affected.`,
              { confirm: "Clear", danger: true });
            if (!ok) return;
            if (el.id && typeof window.clearLog === "function") {
              window.clearLog(el.id);
            } else {
              el.innerHTML = "";
            }
            window._showToast?.(`${label} cleared.`, "ok");
            if (label === "Activity log") {
              try { window._syncActivityLogVisibility?.(); } catch {}
            }
          }});
        window.showContextMenu(e.clientX, e.clientY, items);
      });
    }
  }
  window.initLogContextMenu = initLogContextMenu;
})();
