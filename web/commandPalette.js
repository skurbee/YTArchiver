/**
 * web/commandPalette.js — Ctrl+K command palette.
 *
 * A single searchable overlay that makes the app's actions and channels
 * findable by typing, instead of hunting through tabs / right-click menus.
 * Type to filter; ↑/↓ to move, Enter to run, Esc to close.
 *
 * Sources:
 *   - Actions (jump to a tab, sync all, open Tools/Metadata/Index, search…)
 *   - Channels (jump straight to a channel's Browse page)
 *
 * Built entirely in JS (no markup) so it stays self-contained. Triggered by
 * Ctrl+K (wired in shortcuts.js).
 *
 * Publishes: window.initCommandPalette, window.openCommandPalette,
 *            window.closeCommandPalette.
 */
(function () {
  "use strict";

  const $ = (id) => document.getElementById(id);
  const _esc = (s) => (window._escapeHtml || ((x) => String(x ?? "")))(s);

  function _clickTab(tab) {
    document.querySelector(`.tab[data-tab="${tab}"]`)?.click();
  }
  function _healthSub(view) {
    _clickTab("health");
    setTimeout(() => document.querySelector(
      `#panel-health .settings-subnav-btn[data-settings-view="${view}"]`)?.click(), 50);
  }
  function _jumpChannel(name) {
    _clickTab("browse");
    // Reuse the exact channel-card click flow after the tab is shown.
    setTimeout(() => {
      const card = [...document.querySelectorAll("#channel-grid .channel-card")].find(
        (c) => (c.querySelector(".channel-card-name")?.textContent || "").trim() === name);
      if (card) card.click();
      else window._showToast?.(`Couldn't find "${name}" in Browse.`, "warn");
    }, 110);
  }

  // Build the full searchable list fresh each open (channels change).
  function _buildItems() {
    const items = [
      { label: "Sync all subscribed channels", hint: "action", run: () => $("btn-sync-subbed")?.click() },
      { label: "Search transcripts + titles", hint: "action", run: () => {
          _clickTab("browse");
          setTimeout(() => document.querySelector('[data-browse-sub="search"]')?.click(), 50);
        } },
      { label: "Download", hint: "tab", run: () => _clickTab("download") },
      { label: "Subs — channel list", hint: "tab", run: () => _clickTab("subs") },
      { label: "Browse — library", hint: "tab", run: () => _clickTab("browse") },
      { label: "Health", hint: "tab", run: () => _clickTab("health") },
      { label: "Settings", hint: "tab", run: () => _clickTab("settings") },
      { label: "Tools", hint: "health", run: () => _healthSub("tools") },
      { label: "Metadata status", hint: "health", run: () => _healthSub("metadata") },
      { label: "Index & rebuild", hint: "health", run: () => _healthSub("index") },
      { label: "Keyboard shortcuts", hint: "help", run: () => $("btn-shortcuts-help")?.click() },
    ];
    const chans = (window._browseState && window._browseState.channels) || [];
    for (const c of chans) {
      const name = (c.folder || c.name || "").trim();
      if (name) items.push({ label: name, hint: "channel", run: () => _jumpChannel(name) });
    }
    return items;
  }

  let _all = [];
  let _filtered = [];
  let _sel = 0;

  function _render(listEl, q) {
    const query = (q || "").trim().toLowerCase();
    _filtered = (query
      ? _all.filter((a) => a.label.toLowerCase().includes(query))
      : _all).slice(0, 50);
    _sel = 0;
    listEl.innerHTML = _filtered.length
      ? _filtered.map((a, i) =>
          `<div class="cmdp-item${i === 0 ? " sel" : ""}" data-i="${i}" role="option">` +
          `<span class="cmdp-label">${_esc(a.label)}</span>` +
          `<span class="cmdp-hint cmdp-hint-${a.hint}">${a.hint}</span></div>`).join("")
      : '<div class="cmdp-empty">No matches</div>';
  }

  function _move(delta, listEl) {
    if (!_filtered.length) return;
    _sel = (_sel + delta + _filtered.length) % _filtered.length;
    listEl.querySelectorAll(".cmdp-item").forEach((el, i) => el.classList.toggle("sel", i === _sel));
    listEl.querySelector(".cmdp-item.sel")?.scrollIntoView({ block: "nearest" });
  }

  function _runSel() {
    const a = _filtered[_sel];
    _close();
    if (a && a.run) { try { a.run(); } catch (e) { /* ignore */ } }
  }

  function _close() { $("command-palette-backdrop")?.remove(); }

  function _open() {
    if ($("command-palette-backdrop")) { _close(); return; }
    _all = _buildItems();
    const bd = document.createElement("div");
    bd.id = "command-palette-backdrop";
    bd.className = "cmdp-backdrop";
    bd.innerHTML =
      '<div class="cmdp" role="dialog" aria-label="Command palette">' +
      '<input type="text" class="cmdp-input" id="cmdp-input" ' +
      'placeholder="Type a command or channel…" autocomplete="off" spellcheck="false" />' +
      '<div class="cmdp-list" id="cmdp-list" role="listbox"></div>' +
      '</div>';
    document.body.appendChild(bd);
    const input = $("cmdp-input");
    const listEl = $("cmdp-list");
    _render(listEl, "");
    input.focus();
    input.addEventListener("input", () => _render(listEl, input.value));
    input.addEventListener("keydown", (e) => {
      if (e.key === "ArrowDown") { e.preventDefault(); _move(1, listEl); }
      else if (e.key === "ArrowUp") { e.preventDefault(); _move(-1, listEl); }
      else if (e.key === "Enter") { e.preventDefault(); _runSel(); }
      else if (e.key === "Escape") { e.preventDefault(); _close(); }
    });
    bd.addEventListener("mousedown", (e) => {
      if (e.target === bd) _close();
    });
    listEl.addEventListener("click", (e) => {
      const item = e.target.closest(".cmdp-item");
      if (item) { _sel = Number(item.dataset.i); _runSel(); }
    });
  }

  function initCommandPalette() {
    window.openCommandPalette = _open;
    window.closeCommandPalette = _close;
  }
  window.initCommandPalette = initCommandPalette;
})();
