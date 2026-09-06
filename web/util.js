/**
 * web/util.js — pure utilities used by every other web module.
 *
 * Canonical HTML escaping, timestamp formatting, idempotent event wiring,
 * and subscription-channel normalization.
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

  /** Repair one narrowly-identifiable legacy display artifact without
   * changing the stored value used for lookups/actions.
   *
   * U+FFFD means the original character has already been lost, so a broad
   * replacement would invent data.  The one safe presentation case is an
   * English possessive between a letter/number and a trailing "s":
   * `Creator\uFFFDs` can be shown as `Creator’s`.  Isolated or repeated
   * replacement characters stay visible so ambiguous corruption is never
   * silently disguised.
   */
  function displayText(s) {
    return String(s ?? "")
      .replace(/([\p{L}\p{N}])\uFFFD(?=s\b)/gu, "$1’");
  }

  /** Format a seconds count as "M:SS". null/undefined → "0:00". */
  function _formatTs(sec) {
    if (sec == null) return "0:00";
    const m = Math.floor(sec / 60);
    const s = Math.floor(sec % 60);
    return `${m}:${String(s).padStart(2, "0")}`;
  }

  /** Human-readable byte count for library/storage UI. Invalid or missing
   * values display as an em dash instead of pretending the item is empty. */
  function formatBytes(value) {
    const bytes = Number(value);
    if (!Number.isFinite(bytes) || bytes < 0) return "\u2014";
    if (bytes < 1024) return `${Math.round(bytes)} B`;
    const units = ["KB", "MB", "GB", "TB", "PB"];
    let amount = bytes;
    let unit = -1;
    do {
      amount /= 1024;
      unit += 1;
    } while (amount >= 1024 && unit < units.length - 1);
    const digits = amount >= 100 ? 0 : (amount >= 10 ? 1 : 2);
    return `${amount.toFixed(digits).replace(/\.0+$|(?<=\.[0-9])0$/, "")} ${units[unit]}`;
  }

  /** Convert an ISO string or epoch-seconds/milliseconds value into a Date. */
  function parseDateValue(value) {
    if (value instanceof Date) {
      return Number.isFinite(value.getTime()) ? value : null;
    }
    if (typeof value === "number" && Number.isFinite(value)) {
      const ms = Math.abs(value) < 1e11 ? value * 1000 : value;
      const date = new Date(ms);
      return Number.isFinite(date.getTime()) ? date : null;
    }
    if (typeof value !== "string" || !value.trim()) return null;
    const numeric = Number(value);
    if (Number.isFinite(numeric) && /^\d+(?:\.\d+)?$/.test(value.trim())) {
      return parseDateValue(numeric);
    }
    const date = new Date(value);
    return Number.isFinite(date.getTime()) ? date : null;
  }

  /** Short relative age such as "4 minutes ago" or "in 2 days". */
  function formatRelativeTime(value, nowMs = Date.now()) {
    const date = parseDateValue(value);
    if (!date) return "Date unknown";
    const deltaSeconds = Math.round((Number(nowMs) - date.getTime()) / 1000);
    const future = deltaSeconds < 0;
    const seconds = Math.abs(deltaSeconds);
    let amount;
    let unit;
    if (seconds < 45) return future ? "in a moment" : "just now";
    if (seconds < 3600) {
      amount = Math.max(1, Math.round(seconds / 60));
      unit = "minute";
    } else if (seconds < 86400) {
      amount = Math.max(1, Math.round(seconds / 3600));
      unit = "hour";
    } else if (seconds < 86400 * 45) {
      amount = Math.max(1, Math.round(seconds / 86400));
      unit = "day";
    } else if (seconds < 86400 * 545) {
      amount = Math.max(1, Math.round(seconds / (86400 * 30)));
      unit = "month";
    } else {
      amount = Math.max(1, Math.round(seconds / (86400 * 365)));
      unit = "year";
    }
    const phrase = `${amount} ${unit}${amount === 1 ? "" : "s"}`;
    return future ? `in ${phrase}` : `${phrase} ago`;
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

  function isElementVisible(el) {
    if (!el || el.isConnected === false) return false;
    for (let node = el; node; node = node.parentElement) {
      if (node.hidden || node.getAttribute?.("aria-hidden") === "true") return false;
      const style = window.getComputedStyle?.(node);
      if (style?.display === "none" || style?.visibility === "hidden") return false;
    }
    return true;
  }

  function nearScrollBottom(el, distance = 700) {
    if (!isElementVisible(el) || el.clientHeight <= 0
        || el.scrollHeight <= el.clientHeight + 1) return false;
    return el.scrollHeight - el.scrollTop - el.clientHeight < distance;
  }

  function formatCalendarDate(value) {
    const raw = String(value || "").trim();
    if (!raw) return "";
    const match = /^(\d{4})-?(\d{2})-?(\d{2})$/.exec(raw);
    const date = match
      ? new Date(Number(match[1]), Number(match[2]) - 1, Number(match[3]))
      : parseDateValue(value);
    if (!date || !Number.isFinite(date.getTime())) return raw;
    return date.toLocaleDateString(undefined, { year: "numeric", month: "short", day: "numeric" });
  }

  YT.util = {
    escapeHtml,
    escapeAttr,
    displayText,
    _formatTs,
    formatBytes,
    parseDateValue,
    formatRelativeTime,
    onceIdempotent,
    normalizeSubsChannels,
    loadSubsChannels,
    isElementVisible,
    nearScrollBottom,
    formatCalendarDate,
  };

  // Compatibility aliases for modules that still consume global helpers.
  window._escapeHtml = escapeHtml;
  window._displayText = displayText;
  window._formatTs = _formatTs;
})();
