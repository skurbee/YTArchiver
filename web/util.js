/**
 * web/util.js — pure utilities used by every other web module.
 *
 * Patch 11/12 (v67.7):
 *   - Single canonical escapeHtml — replaces 5 copies across app.js
 *     and logs.js.
 *   - Single canonical _formatTs — was on window._formatTs because
 *     logs.js needed it; now namespaced under YT.util.
 *   - onceIdempotent() — wraps the dataset-flag "already-wired" pattern
 *     that appeared 5+ times for event-handler setup.
 *
 * Loading order: this file MUST be loaded BEFORE bridge.js, logs.js,
 * and app.js (see web/index.html script tags).
 *
 * Namespace: everything attaches under `window.YT.util.*`.
 */
(function () {
  "use strict";

  window.YT = window.YT || {};
  const YT = window.YT;

  /** Escape HTML entities for safe insertion into innerHTML.
   * Null/undefined coerce to empty string. */
  function escapeHtml(s) {
    return String(s ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  /** Identical body to escapeHtml — kept as a separate name so callers
   * can read intent ("escape for attribute" vs "escape for element"). */
  function escapeAttr(s) {
    return escapeHtml(s);
  }

  /** Format a seconds count as "M:SS". null/undefined → "0:00". */
  function _formatTs(sec) {
    if (sec == null) return "0:00";
    const m = Math.floor(sec / 60);
    const s = Math.floor(sec % 60);
    return `${m}:${String(s).padStart(2, "0")}`;
  }

  /** Run `fn` once per (target, key) pair. Uses a dataset flag so the
   * same handler doesn't get attached twice if the calling code is
   * triggered repeatedly (tab re-activation, re-render, etc.). */
  function onceIdempotent(target, key, fn) {
    if (!target || !key) return;
    const flag = `_once_${key}`;
    if (target.dataset && target.dataset[flag] === "1") return;
    if (target.dataset) target.dataset[flag] = "1";
    try { fn(target); } catch (e) { console.error("[once " + key + "]", e); }
  }

  function normalizeSubsChannels(resp) {
    let rows = [];
    if (Array.isArray(resp) && Array.isArray(resp[0])) rows = resp[0];
    else if (Array.isArray(resp)) rows = resp;
    else if (resp && Array.isArray(resp.channels)) rows = resp.channels;
    return rows
      .map((ch) => {
        const folder = String(ch?.folder || ch?.folder_override || ch?.name || "").trim();
        const name = String(ch?.name || folder).trim();
        const displayName = folder || name;
        return Object.assign({}, ch || {}, {
          name,
          folder: folder || name,
          displayName,
        });
      })
      .filter((ch) => ch.displayName)
      .sort((a, b) => a.displayName.toLowerCase()
        .localeCompare(b.displayName.toLowerCase()));
  }

  async function loadSubsChannels() {
    const bridge = window.YT?.bridge;
    if (!bridge?.isUp?.() || !bridge?.bridgeCall) return [];
    const resp = await bridge.bridgeCall("get_subs_channels");
    return normalizeSubsChannels(resp);
  }

  YT.util = {
    escapeHtml,
    escapeAttr,
    _formatTs,
    onceIdempotent,
    normalizeSubsChannels,
    loadSubsChannels,
  };

  // Back-compat shims — logs.js + app.js modules still reach for these.
  // Deleting these globals is staged for Patch 14/15 when those files
  // are fully split and migrate to YT.util.*.
  window._escapeHtml = escapeHtml;
  window._formatTs = _formatTs;
})();
