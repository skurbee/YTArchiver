/**
 * web/contextMenu.js — right-click context menu primitive.
 *
 * extracted out of app.js (lines 7527-7595).
 * Provides:
 *   - YT.ctx.show(x, y, items) — open a context menu at coords with
 *     a list of { label, action, cls?, sep?, submenu? } entries.
 *   - YT.ctx.close() — explicit close (called automatically on
 *     outside-click and Escape).
 *
 * Items shape:
 *   { label: "Open", action: () => ... }
 *   { sep: true }
 *   { label: "More…", submenu: [...] }
 *   { label: "Delete", action: ..., cls: "danger" }
 *
 * Back-compat: window.showContextMenu / closeContextMenu are exported
 * (logs.js + app.js already reach for those globals).
 *
 * Depends on: nothing
 * Loaded BEFORE logs.js + app.js.
 */
(function () {
  "use strict";

  window.YT = window.YT || {};
  const YT = window.YT;

  function showContextMenu(x, y, items) {
    closeContextMenu();
    const root = document.getElementById("ctx-menu-root");
    if (!root) {
      console.warn("[ctx] no #ctx-menu-root in DOM");
      return;
    }
    const menu = document.createElement("div");
    menu.className = "ctx-menu";
    menu.style.left = x + "px";
    menu.style.top = y + "px";
    for (const it of items) {
      if (it.sep) {
        const sep = document.createElement("div");
        sep.className = "ctx-menu-sep";
        menu.appendChild(sep);
        continue;
      }
      const row = document.createElement("div");
      row.className = "ctx-menu-item" + (it.cls ? " " + it.cls : "");
      row.textContent = it.label;
      if (it.title) row.title = it.title;   // hover tooltip (e.g. why disabled)
      if (it.submenu) {
        row.classList.add("ctx-submenu-wrap");
        const sub = document.createElement("div");
        sub.className = "ctx-submenu";
        for (const sit of it.submenu) {
          const srow = document.createElement("div");
          srow.className = "ctx-menu-item";
          srow.textContent = sit.label;
          srow.addEventListener("click", (e) => {
            e.stopPropagation();
            closeContextMenu();
            if (sit.action) sit.action();
          });
          sub.appendChild(srow);
        }
        row.appendChild(sub);
      } else if (it.action) {
        row.addEventListener("click", () => {
          closeContextMenu();
          it.action();
        });
      }
      menu.appendChild(row);
    }
    root.appendChild(menu);
    // Clamp to viewport
    const r = menu.getBoundingClientRect();
    if (r.right > window.innerWidth) {
      menu.style.left = (window.innerWidth - r.width - 4) + "px";
    }
    if (r.bottom > window.innerHeight) {
      menu.style.top = (window.innerHeight - r.height - 4) + "px";
    }
    setTimeout(() => {
      document.addEventListener("click", closeContextMenu, { once: true });
      document.addEventListener("keydown", onCtxKey);
    }, 0);
  }

  function closeContextMenu() {
    const root = document.getElementById("ctx-menu-root");
    if (root) root.innerHTML = "";
    document.removeEventListener("keydown", onCtxKey);
  }

  function onCtxKey(e) {
    // stopPropagation so the same Escape press doesn't
    // bubble up and close popovers / dialogs that happen to be
    // open underneath the context menu.
    if (e.key === "Escape") {
      e.stopPropagation();
      closeContextMenu();
    }
  }

  YT.ctx = {
    show: showContextMenu,
    close: closeContextMenu,
  };
  // Back-compat globals — logs.js + app.js call these directly.
  window.showContextMenu = showContextMenu;
  window.closeContextMenu = closeContextMenu;
})();
