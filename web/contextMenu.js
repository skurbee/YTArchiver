/**
 * web/contextMenu.js — right-click context menu primitive.
 *
 * extracted out of app.js (lines 7527-7595).
 * Provides:
 *   - YT.ctx.show(x, y, items) — open a context menu at coords with
 *     a list of { label, action, cls?, checked?, disabled?, sep?, header?, submenu? }
 *     entries.
 *   - YT.ctx.close() — explicit close (called automatically on
 *     outside-click and Escape).
 *
 * Items shape:
 *   { label: "Open", action: () => ... }
 *   { label: "Enabled option", checked: true, action: () => ... }
 *   { header: "Maintenance" }
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

  function invokeAction(action) {
    if (typeof action !== "function") return;
    try {
      const result = action();
      if (result && typeof result.then === "function") {
        result.catch((error) => {
          console.error("[ctx] action failed", error);
          window._showToast?.(
            `Action failed: ${error?.message || error}`, "error");
        });
      }
    } catch (error) {
      console.error("[ctx] action failed", error);
      window._showToast?.(`Action failed: ${error?.message || error}`, "error");
    }
  }

  function positionSubmenu(wrap) {
    const sub = wrap?.querySelector(":scope > .ctx-submenu");
    if (!sub) return;
    // Measure even before hover has made the flyout visible.
    const oldDisplay = sub.style.display;
    const oldVisibility = sub.style.visibility;
    sub.style.visibility = "hidden";
    sub.style.display = "block";
    sub.style.top = "-4px";
    wrap.classList.remove("submenu-left");

    const wr = wrap.getBoundingClientRect();
    const sr = sub.getBoundingClientRect();
    const parentWrap = wrap.parentElement?.closest?.(".ctx-submenu-wrap");
    const preferLeft = parentWrap?.classList?.contains("submenu-left");
    if (((preferLeft || wr.right + sr.width > window.innerWidth - 4)
          && wr.left - sr.width > 4)) {
      wrap.classList.add("submenu-left");
    }
    // A root menu near the bottom used to let long flyouts extend hundreds of
    // pixels off-screen. Shift each flyout upward while keeping it in view.
    const top = Math.max(
      4 - wr.top,
      Math.min(-4, window.innerHeight - 4 - wr.top - sr.height));
    sub.style.top = `${Math.round(top)}px`;
    sub.style.display = oldDisplay;
    sub.style.visibility = oldVisibility;
  }

  function appendMenuItems(container, items) {
    for (const it of items || []) {
      if (it.header) {
        const header = document.createElement("div");
        header.className = "ctx-menu-header";
        header.textContent = it.header;
        container.appendChild(header);
        continue;
      }
      if (it.sep) {
        const sep = document.createElement("div");
        sep.className = "ctx-menu-sep";
        sep.setAttribute("role", "separator");
        container.appendChild(sep);
        continue;
      }

      const row = document.createElement("div");
      row.className = "ctx-menu-item" + (it.cls ? " " + it.cls : "");
      if (it.disabled) {
        row.classList.add("disabled");
        row.setAttribute("aria-disabled", "true");
      }
      if (typeof it.checked === "boolean") {
        row.classList.add("checkable");
        row.classList.toggle("checked", it.checked);
        row.setAttribute("role", "menuitemcheckbox");
        row.setAttribute("aria-checked", it.checked ? "true" : "false");
      } else {
        row.setAttribute("role", "menuitem");
      }
      row.tabIndex = -1;
      const hasSubmenu = Array.isArray(it.submenu) && it.submenu.length > 0;
      row.textContent = it.label;
      if (it.count !== undefined && it.count !== null) {
        row.classList.add("has-count");
        row.setAttribute(
          "aria-label", it.countAriaLabel || `${it.label}, ${it.count}`);
        const count = document.createElement("span");
        count.className = "ctx-menu-count" + (it.countDim ? " dim" : "");
        count.textContent = String(it.count);
        count.setAttribute("aria-hidden", "true");
        row.appendChild(count);
      }
      if (it.title) {
        if (hasSubmenu) {
          // A native title on a submenu trigger remains active while the
          // pointer is over its descendants, producing stacked tooltips.
          // Keep the description available to assistive technology without
          // showing a second tooltip over the child item.
          row.setAttribute("aria-description", it.title);
        } else {
          row.title = it.title;
        }
      }

      if (hasSubmenu) {
        row.classList.add("ctx-submenu-wrap");
        row.setAttribute("aria-haspopup", "menu");
        row.setAttribute("aria-expanded", "false");

        const sub = document.createElement("div");
        sub.className = "ctx-submenu";
        if (it.submenu.some((child) => Array.isArray(child?.submenu)
            && child.submenu.length)) {
          sub.classList.add("has-nested-submenu");
        }
        sub.setAttribute("role", "menu");
        appendMenuItems(sub, it.submenu);
        row.appendChild(sub);

        row.addEventListener("pointerenter", () => {
          row.classList.remove("submenu-keyboard-closed");
          row.setAttribute("aria-expanded", "true");
          positionSubmenu(row);
        });
        row.addEventListener("pointerleave", () => {
          row.classList.remove("submenu-keyboard-closed");
          if (!row.classList.contains("submenu-open")) {
            row.setAttribute("aria-expanded", "false");
          }
        });
        row.addEventListener("click", (event) => {
          event.stopPropagation();
          if (it.disabled) return;
          positionSubmenu(row);
          row.classList.remove("submenu-keyboard-closed");
          row.classList.add("submenu-open");
          row.setAttribute("aria-expanded", "true");
          sub.querySelector(":scope > .ctx-menu-item:not(.disabled)")?.focus();
        });
      } else if (it.action) {
        row.addEventListener("click", (event) => {
          event.stopPropagation();
          if (it.disabled) return;
          closeContextMenu();
          invokeAction(it.action);
        });
      }

      container.appendChild(row);
    }
  }

  function showContextMenu(x, y, items) {
    closeContextMenu();
    const root = document.getElementById("ctx-menu-root");
    if (!root) {
      console.warn("[ctx] no #ctx-menu-root in DOM");
      return;
    }
    const menu = document.createElement("div");
    menu.className = "ctx-menu";
    menu.setAttribute("role", "menu");
    menu.tabIndex = -1;
    menu.style.left = x + "px";
    menu.style.top = y + "px";
    appendMenuItems(menu, items);
    root.appendChild(menu);
    // Clamp to viewport
    const r = menu.getBoundingClientRect();
    if (r.right > window.innerWidth) {
      menu.style.left = (window.innerWidth - r.width - 4) + "px";
    }
    if (r.bottom > window.innerHeight) {
      menu.style.top = (window.innerHeight - r.height - 4) + "px";
    }
    // Keep the root menu anchored near the click/button; flip flyout
    // submenus left only when a right-opening submenu would leave the
    // viewport.
    menu.querySelectorAll(".ctx-submenu-wrap")
      .forEach((wrap) => positionSubmenu(wrap));
    const first = menu.querySelector(".ctx-menu-item:not(.disabled)");
    if (first) setTimeout(() => first.focus(), 0);
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
      return;
    }
    if (e.key === "Tab") {
      closeContextMenu();
      return;
    }
    if (e.key === "ArrowDown" || e.key === "ArrowUp") {
      const root = document.getElementById("ctx-menu-root");
      const cur = document.activeElement;
      const currentMenu = cur?.closest?.(".ctx-submenu")
        || cur?.closest?.(".ctx-menu")
        || root?.querySelector(".ctx-menu");
      // Move within the menu that currently owns focus. Querying every
      // descendant also included hidden flyout rows, which could make focus
      // appear to vanish when the user moved past a submenu trigger.
      const items = [...(currentMenu?.children || [])]
        .filter((item) => item.classList.contains("ctx-menu-item")
          && !item.classList.contains("disabled"));
      if (!items.length) return;
      e.preventDefault();
      const idx = items.indexOf(cur);
      const dir = e.key === "ArrowDown" ? 1 : -1;
      const next = idx === -1
        ? 0
        : (idx + dir + items.length) % items.length;
      items[next].focus();
      return;
    }
    if (e.key === "ArrowRight") {
      const row = document.activeElement;
      const sub = row?.classList?.contains("ctx-submenu-wrap")
        ? row.querySelector(":scope > .ctx-submenu")
        : null;
      const first = sub?.querySelector(":scope > .ctx-menu-item:not(.disabled)");
      if (first) {
        e.preventDefault();
        positionSubmenu(row);
        row.classList.remove("submenu-keyboard-closed");
        row.classList.add("submenu-open");
        row.setAttribute("aria-expanded", "true");
        first.focus();
      }
      return;
    }
    if (e.key === "ArrowLeft") {
      const row = document.activeElement;
      const sub = row?.closest?.(".ctx-submenu");
      const trigger = sub?.closest?.(".ctx-submenu-wrap");
      if (trigger) {
        e.preventDefault();
        trigger.classList.remove("submenu-open");
        trigger.classList.add("submenu-keyboard-closed");
        trigger.setAttribute("aria-expanded", "false");
        trigger.focus();
      }
      return;
    }
    if (e.key === "Enter" || e.key === " ") {
      const row = document.activeElement;
      if (row?.classList?.contains("ctx-menu-item")) {
        e.preventDefault();
        row.click();
      }
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
