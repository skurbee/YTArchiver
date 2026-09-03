/**
 * web/trashView.js — Browse > Trash.
 *
 * Lists app-managed Trash entries and owns restore/permanent-delete actions.
 * The backend remains authoritative: entries and the sidebar count change only
 * after a successful API response and a fresh list/summary read.
 *
 * Publishes:
 *   window.initTrashView
 *   window._loadTrashView
 *   window._refreshTrashSummary
 *   window._onTrashChanged
 *   window._goToTrash
 */
(function () {
  "use strict";

  const util = window.YT?.util || {};
  const escapeHtml = util.escapeHtml || ((value) => String(value ?? "")
    .replace(/[&<>"']/g, (ch) => ({
      "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;", "'": "&#39;",
    }[ch])));
  const displayText = util.displayText || ((value) => String(value ?? ""));
  const formatBytes = util.formatBytes || ((value) => {
    const n = Number(value);
    return Number.isFinite(n) && n >= 0 ? `${n.toLocaleString()} bytes` : "\u2014";
  });
  const parseDateValue = util.parseDateValue || ((value) => {
    const date = new Date(value);
    return Number.isFinite(date.getTime()) ? date : null;
  });
  const formatRelativeTime = util.formatRelativeTime || (() => "Date unknown");

  const state = {
    initialized: false,
    loadGeneration: 0,
    summaryGeneration: 0,
    actionBusy: false,
    openBusy: false,
    entries: [],
    itemCount: 0,
    fileCount: 0,
    untrackedCount: 0,
  };

  function bridgeCall(method, ...args) {
    const fn = window.YT?.bridge?.bridgeCall;
    return fn ? fn(method, ...args) : undefined;
  }

  async function waitForBridge() {
    try { await window.YT?.bridge?.ready; } catch (_error) { /* handled by call */ }
  }

  function plural(count, one, many) {
    return `${count.toLocaleString()} ${count === 1 ? one : (many || `${one}s`)}`;
  }

  function finiteCount(value, fallback = null) {
    const n = Number(value);
    return Number.isFinite(n) && n >= 0 ? Math.floor(n) : fallback;
  }

  function isTrashVisible() {
    const view = document.getElementById("view-trash");
    return !!(view && !view.hidden);
  }

  function entryId(entry) {
    return String(entry?.id ?? entry?.entry_id ?? "").trim();
  }

  function entryName(entry) {
    const explicit = entry?.display_name || entry?.name || entry?.title;
    if (explicit) return displayText(explicit);
    const original = String(entry?.original_path || "").replace(/\\/g, "/");
    return displayText(original.split("/").filter(Boolean).pop() || "Trash item");
  }

  function entryTypeLabel(entry) {
    const kind = String(entry?.entry_type || entry?.type || "").toLowerCase();
    if (kind === "channel_folder" || kind === "channel") return "Channel folder";
    if (kind === "video") return "Video";
    return "Files";
  }

  function normalizedState(entry) {
    if (entry?.tracked === false || entry?.untracked === true) return "untracked";
    return String(entry?.state || entry?.status || "complete").toLowerCase();
  }

  function warningText(value) {
    if (Array.isArray(value)) {
      return value.map((item) => String(item || "").trim())
        .filter(Boolean).join("; ");
    }
    return String(value || "").trim();
  }

  function entryStatus(entry) {
    const warning = warningText(entry?.warnings) || warningText(entry?.warning);
    const status = normalizedState(entry);
    if (warning) return { kind: "warning", label: warning };
    if (status === "complete" || status === "ready") {
      return { kind: "ready", label: "Ready" };
    }
    if (status === "pending" || status === "moving") {
      if (canRestore(entry)) {
        return { kind: "warning", label: "Move was interrupted — ready to restore" };
      }
      return { kind: "pending", label: "Still moving files" };
    }
    if (status === "restoring") {
      return canRestore(entry)
        ? { kind: "warning", label: "Restore was interrupted — ready to resume" }
        : { kind: "pending", label: "Restore in progress" };
    }
    if (status === "purging" || status === "deleting") {
      return { kind: "pending", label: "Permanent deletion in progress" };
    }
    if (status === "untracked") {
      return { kind: "warning", label: "Needs attention \u2014 not managed by YTArchiver" };
    }
    return { kind: "warning", label: "Needs attention" };
  }

  function canRestore(entry) {
    if (!entryId(entry)) return false;
    if (typeof entry?.can_restore === "boolean") return entry.can_restore;
    return normalizedState(entry) === "complete";
  }

  function canPurge(entry) {
    return !!entryId(entry)
      && normalizedState(entry) === "complete"
      && entry?.can_purge !== false;
  }

  function setCountBadge(count) {
    const badge = document.getElementById("trash-nav-count");
    if (!badge) return;
    const n = finiteCount(count, null);
    if (n == null) return;
    badge.textContent = n > 99 ? "99+" : String(n);
    badge.hidden = n === 0;
    badge.setAttribute("aria-label", `${plural(n, "item")} in Trash`);
    badge.title = `${plural(n, "item")} in Trash`;
  }

  function applySummary(payload) {
    if (!payload || payload.ok === false) return;
    const itemCount = finiteCount(payload.item_count, null);
    const fileCount = finiteCount(payload.file_count, null);
    if (itemCount != null) {
      state.itemCount = itemCount;
      setCountBadge(itemCount);
    }
    if (fileCount != null) state.fileCount = fileCount;
  }

  function setLoading(message = "Loading Trash\u2026") {
    const list = document.getElementById("trash-list");
    if (!list) return;
    list.setAttribute("aria-busy", "true");
    list.innerHTML = `<div class="browse-empty"><span class="spinner-inline"></span>${escapeHtml(message)}</div>`;
    const summary = document.getElementById("trash-summary-text");
    if (summary) summary.textContent = message;
    const empty = document.getElementById("trash-empty-btn");
    if (empty) empty.disabled = true;
  }

  function renderError(message) {
    const list = document.getElementById("trash-list");
    if (!list) return;
    list.setAttribute("aria-busy", "false");
    list.innerHTML = [
      '<div class="trash-state trash-state-error">',
      '  <strong>Couldn\'t load Trash.</strong>',
      `  <span>${escapeHtml(message || "Try again.")}</span>`,
      '  <button class="btn btn-ghost btn-thin" type="button" data-trash-action="retry">Try again</button>',
      '</div>',
    ].join("");
    const summary = document.getElementById("trash-summary-text");
    if (summary) summary.textContent = "Trash is unavailable";
    const empty = document.getElementById("trash-empty-btn");
    if (empty) empty.disabled = true;
  }

  function dateParts(value) {
    const date = parseDateValue(value);
    return {
      relative: formatRelativeTime(value),
      exact: date ? date.toLocaleString() : "Date unavailable",
      iso: date ? date.toISOString() : "",
    };
  }

  function renderEntry(entry) {
    const id = entryId(entry);
    const name = entryName(entry);
    const type = entryTypeLabel(entry);
    const status = entryStatus(entry);
    const date = dateParts(entry?.trashed_at);
    const sizeValue = entry?.size_bytes ?? entry?.bytes;
    const size = typeof entry?.size === "string" && entry.size.trim()
      ? entry.size.trim() : formatBytes(sizeValue);
    const files = finiteCount(entry?.file_count, null);
    const location = displayText(entry?.original_path || "Original location unavailable");
    const restoreDisabled = !canRestore(entry);
    const purgeDisabled = !canPurge(entry);
    const disabledHint = status.kind === "pending"
      ? "This item is still being processed."
      : (status.kind === "warning" ? "This item needs attention before it can be changed." : "");
    const restoreHint = restoreDisabled ? disabledHint : "";
    const purgeHint = purgeDisabled ? disabledHint : "";
    const restoreLabel = !restoreDisabled && normalizedState(entry) !== "complete"
      ? "Resume restore" : "Restore files";
    const scope = String(entry?.restore_scope || "").toLowerCase();
    let restoreScope = "";
    if (entryTypeLabel(entry) === "Channel folder" && scope === "full") {
      restoreScope = "Restore includes files, subscription settings, and Browse catalog data.";
    } else if (entryTypeLabel(entry) === "Channel folder" && scope === "files_only") {
      restoreScope = "Files only — this older item cannot re-add the subscription automatically.";
    } else if (scope === "files_and_catalog") {
      restoreScope = "Restore includes files and Browse catalog data.";
    }

    return [
      `<article class="trash-item" data-trash-id="${escapeHtml(id)}">`,
      '  <div class="trash-item-heading">',
      `    <strong class="trash-item-name" title="${escapeHtml(name)}">${escapeHtml(name)}</strong>`,
      `    <span class="trash-type-badge">${escapeHtml(type)}</span>`,
      '  </div>',
      '  <div class="trash-item-facts">',
      `    <span><b>Moved:</b> <time${date.iso ? ` datetime="${escapeHtml(date.iso)}"` : ""} title="${escapeHtml(date.exact)}">${escapeHtml(date.relative)}</time></span>`,
      `    <span><b>Size:</b> ${escapeHtml(size)}</span>`,
      `    <span><b>Files:</b> ${files == null ? "\u2014" : files.toLocaleString()}</span>`,
      '  </div>',
      `  <div class="trash-original-path" title="${escapeHtml(location)}"><b>Original location:</b> ${escapeHtml(location)}</div>`,
      restoreScope ? `  <div class="trash-restore-scope">${escapeHtml(restoreScope)}</div>` : "",
      `  <div class="trash-item-status trash-item-status-${escapeHtml(status.kind)}"><span aria-hidden="true"></span>${escapeHtml(status.label)}</div>`,
      '  <div class="trash-item-actions">',
      `    <button class="btn btn-ghost btn-thin trash-action" type="button" data-trash-action="restore"${restoreDisabled ? " disabled" : ""}${restoreHint ? ` title="${escapeHtml(restoreHint)}"` : ""}>${restoreLabel}</button>`,
      `    <button class="btn btn-thin trash-danger trash-action" type="button" data-trash-action="purge"${purgeDisabled ? " disabled" : ""}${purgeHint ? ` title="${escapeHtml(purgeHint)}"` : ""}>Delete forever\u2026</button>`,
      '  </div>',
      '</article>',
    ].join("");
  }

  function renderList(payload) {
    const list = document.getElementById("trash-list");
    if (!list) return;
    const entries = Array.isArray(payload?.entries) ? payload.entries : [];
    state.entries = entries.slice();
    state.itemCount = finiteCount(payload?.item_count, entries.length);
    state.fileCount = finiteCount(payload?.file_count,
      entries.reduce((total, entry) => total + finiteCount(entry?.file_count, 0), 0));
    state.untrackedCount = finiteCount(payload?.untracked_count,
      entries.filter((entry) => normalizedState(entry) === "untracked").length);
    setCountBadge(state.itemCount);

    const retention = document.getElementById("trash-retention-note");
    if (retention) {
      const days = finiteCount(payload?.retention_days, null);
      const graceUntil = Number(payload?.retention_grace_until_ts);
      const graceDate = Number.isFinite(graceUntil) && graceUntil * 1000 > Date.now()
        ? new Date(graceUntil * 1000) : null;
      retention.textContent = days === 0
        ? "Automatic cleanup is off. Items stay here until you delete them."
        : (days == null
          ? "Automatic cleanup schedule unavailable."
          : (graceDate
            ? `Automatic cleanup is paused until ${graceDate.toLocaleString()}.`
            : `Items are automatically deleted after ${plural(days, "day")}.`));
    }

    const summary = document.getElementById("trash-summary-text");
    if (summary) {
      summary.textContent = `${plural(state.itemCount, "item")} \u00b7 ${plural(state.fileCount, "file")}`;
    }
    const attention = document.getElementById("trash-attention-banner");
    if (attention) {
      attention.hidden = state.untrackedCount === 0;
      attention.textContent = state.untrackedCount
        ? `${plural(state.untrackedCount, "untracked item")} ${state.untrackedCount === 1 ? "needs" : "need"} attention. YTArchiver will leave them alone.`
        : "";
    }

    list.setAttribute("aria-busy", "false");
    list.innerHTML = entries.length
      ? entries.map(renderEntry).join("")
      : '<div class="trash-state trash-state-empty"><strong>Trash is empty.</strong><span>Files you move to Trash will appear here until they are restored or permanently deleted.</span></div>';

    const empty = document.getElementById("trash-empty-btn");
    if (empty) {
      empty.disabled = state.actionBusy || !entries.some(canPurge);
      empty.title = entries.some(canPurge)
        ? "Permanently delete every ready item in Trash"
        : "There are no ready Trash items to permanently delete";
    }
  }

  async function refreshTrashSummary() {
    const generation = ++state.summaryGeneration;
    await waitForBridge();
    try {
      const result = await bridgeCall("trash_summary");
      if (generation !== state.summaryGeneration) return result;
      applySummary(result);
      return result;
    } catch (error) {
      console.warn("trash summary:", error);
      return { ok: false, error: String(error) };
    }
  }

  async function loadTrashView() {
    const generation = ++state.loadGeneration;
    setLoading();
    await waitForBridge();
    try {
      const result = await bridgeCall("trash_list");
      if (generation !== state.loadGeneration) return result;
      if (!result?.ok) {
        renderError(result?.error || "Try again.");
        return result;
      }
      applySummary(result);
      renderList(result);
      return result;
    } catch (error) {
      if (generation !== state.loadGeneration) return null;
      renderError(error?.message || String(error));
      return { ok: false, error: String(error) };
    }
  }

  function findEntry(card) {
    const id = String(card?.dataset?.trashId || "");
    return state.entries.find((entry) => entryId(entry) === id) || null;
  }

  function setActionBusy(busy) {
    state.actionBusy = !!busy;
    document.querySelectorAll("#view-trash .trash-action").forEach((button) => {
      if (busy) {
        button.disabled = true;
        return;
      }
      const entry = findEntry(button.closest(".trash-item"));
      button.disabled = button.dataset.trashAction === "restore"
        ? !canRestore(entry) : !canPurge(entry);
    });
    const empty = document.getElementById("trash-empty-btn");
    if (empty) empty.disabled = busy || !state.entries.some(canPurge);
  }

  async function restoreEntry(entry) {
    if (state.actionBusy || !canRestore(entry)) return;
    const name = entryName(entry);
    setActionBusy(true);
    const progress = window._showToast?.({
      msg: `Restoring "${name}"\u2026`, kind: "warn", persist: true,
    });
    try {
      const result = await bridgeCall("trash_restore", { id: entryId(entry) });
      if (!result?.ok) {
        window._showToast?.(result?.error || "Restore failed.", "error");
        return;
      }
      await loadTrashView();
      const warning = warningText(result.warnings)
        || warningText(result.warning)
        || warningText(result.cleanup_warning);
      const channelFiles = String(result.entry_type || entry.entry_type || "")
        .toLowerCase().includes("channel");
      const subscriptionPresent = result.subscription_present === true
        || result.subscription_restored === true;
      if (channelFiles) {
        await Promise.allSettled([
          window.refreshSubsTable?.(),
          window._refreshVideosViewIfActive?.(),
          window._loadManualView?.(),
        ]);
      }
      const notResubscribed = channelFiles && !subscriptionPresent;
      const message = warning
        ? warning
        : (notResubscribed
          ? "Files restored. The channel was not re-added to subscriptions."
          : "Files restored to their original location.");
      window._showToast?.(message, warning ? "warn" : "ok");
    } catch (error) {
      window._showToast?.(`Restore failed: ${error?.message || error}`, "error");
    } finally {
      progress?.dismiss?.(true);
      setActionBusy(false);
    }
  }

  async function purgeEntry(entry) {
    if (state.actionBusy || !canPurge(entry)) return;
    const name = entryName(entry);
    setActionBusy(true);
    const confirmed = await window.askDanger?.(
      `Permanently delete "${name}"?`,
      "This cannot be undone.",
      "Delete forever");
    if (!confirmed) {
      setActionBusy(false);
      return;
    }
    const progress = window._showToast?.({
      msg: `Permanently deleting "${name}"\u2026`, kind: "warn", persist: true,
    });
    try {
      const result = await bridgeCall("trash_purge", { id: entryId(entry) });
      if (!result?.ok) {
        window._showToast?.(result?.error || "Permanent deletion failed.", "error");
        return;
      }
      await loadTrashView();
      const freed = finiteCount(result.freed_bytes, null);
      const warning = warningText(result.warnings)
        || warningText(result.warning);
      const successMessage = freed == null
        ? `Permanently deleted "${name}".`
        : `Permanently deleted "${name}" and freed ${formatBytes(freed)}.`;
      window._showToast?.(
        warning ? `${successMessage} ${warning}` : successMessage,
        warning ? "warn" : "ok");
    } catch (error) {
      window._showToast?.(
        `Permanent deletion failed: ${error?.message || error}`, "error");
    } finally {
      progress?.dismiss?.(true);
      setActionBusy(false);
    }
  }

  async function emptyTrash() {
    if (state.actionBusy || !state.entries.some(canPurge)) return;
    const readyCount = state.entries.filter(canPurge).length;
    setActionBusy(true);
    const confirmed = await window.askDanger?.(
      "Empty Trash?",
      `Permanently delete ${plural(readyCount, "ready item")} from Trash? ` +
        "This cannot be undone.",
      "Empty Trash");
    if (!confirmed) {
      setActionBusy(false);
      return;
    }
    const progress = window._showToast?.({
      msg: "Permanently deleting Trash items\u2026", kind: "warn", persist: true,
    });
    try {
      const result = await bridgeCall("trash_empty", { scope: "all" });
      const purged = finiteCount(result?.purged, 0);
      const failed = finiteCount(result?.failed, 0);
      const warning = warningText(result?.warnings)
        || warningText(result?.warning);
      if (!result?.ok && purged === 0) {
        window._showToast?.(result?.error || "Could not empty Trash.", "error");
        return;
      }
      await loadTrashView();
      if (failed > 0 || result?.ok === false || warning) {
        const base = `${plural(purged, "item")} permanently deleted; ${plural(failed, "item")} could not be deleted.`;
        window._showToast?.(
          warning ? `${base} ${warning}` : base,
          "warn");
      } else {
        const freed = finiteCount(result?.freed_bytes, null);
        window._showToast?.(
          freed == null
            ? `${plural(purged, "item")} permanently deleted.`
            : `${plural(purged, "item")} permanently deleted; ${formatBytes(freed)} freed.`,
          "ok");
      }
    } catch (error) {
      window._showToast?.(`Could not empty Trash: ${error?.message || error}`, "error");
    } finally {
      progress?.dismiss?.(true);
      setActionBusy(false);
    }
  }

  async function openTrashFolder() {
    if (state.openBusy) return;
    state.openBusy = true;
    const button = document.getElementById("trash-open-folder");
    if (button) button.disabled = true;
    try {
      const result = await bridgeCall("trash_open_folder");
      if (!result?.ok) {
        window._showToast?.(result?.error || "Could not open the Trash folder.", "error");
      } else {
        const warning = warningText(result.warnings)
          || warningText(result.warning);
        if (warning) window._showToast?.(warning, "warn");
      }
    } catch (error) {
      window._showToast?.(
        `Could not open the Trash folder: ${error?.message || error}`, "error");
    } finally {
      state.openBusy = false;
      if (button) button.disabled = false;
    }
  }

  function goToTrash() {
    document.querySelector('.tab[data-tab="browse"]')?.click();
    setTimeout(() => {
      document.querySelector('.submode-btn[data-submode="trash"]')?.click();
    }, 0);
  }

  async function onTrashChanged() {
    return isTrashVisible() ? loadTrashView() : refreshTrashSummary();
  }

  function initTrashView() {
    if (state.initialized) return;
    state.initialized = true;

    document.getElementById("trash-list")?.addEventListener("click", (event) => {
      const button = event.target.closest("[data-trash-action]");
      if (!button || button.disabled) return;
      const action = button.dataset.trashAction;
      if (action === "retry") { loadTrashView(); return; }
      const entry = findEntry(button.closest(".trash-item"));
      if (!entry) return;
      if (action === "restore") restoreEntry(entry);
      if (action === "purge") purgeEntry(entry);
    });
    document.getElementById("trash-empty-btn")?.addEventListener(
      "click", emptyTrash);
    document.getElementById("trash-open-folder")?.addEventListener(
      "click", openTrashFolder);

    waitForBridge().then(refreshTrashSummary);
  }

  window.initTrashView = initTrashView;
  window._loadTrashView = loadTrashView;
  window._refreshTrashSummary = refreshTrashSummary;
  window._onTrashChanged = onTrashChanged;
  window._goToTrash = goToTrash;
})();
