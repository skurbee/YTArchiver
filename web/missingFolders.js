/**
 * web/missingFolders.js — Missing-folder reconcile flow — prompt for each missing channel folder
 */
(function () {
  "use strict";

  const _browseState = window._browseState || {};
  const showContextMenu = window.showContextMenu || (() => {});
  const askConfirm = window.askConfirm;
  const askDanger = window.askDanger;
  const askQuestion = window.askQuestion;
  const askChoice = window.askChoice;
  const askTextInput = window.askTextInput;
  const escapeHtml = window.YT?.util?.escapeHtml || ((s) => String(s ?? ""));
  function bridgeCall(method, ...args) {
    const fn = window.YT?.bridge?.bridgeCall;
    if (fn) return fn(method, ...args);
    return undefined;
  }

  // ─── Missing-folder reconcile flow ──────────────────────────────────
  async function _reconcileMissingFolders() {
    const api = window.pywebview?.api;
    if (!api?.check_channel_folders) return;
    let res;
    try { res = await api.check_channel_folders(); } catch { return; }
    const missing = (res && res.missing) || [];
    if (!missing.length) return;
    for (const ch of missing) {
      // askChoice takes a SINGLE opts object, not three
      // positional args. The old call was passing (title, message,
      // choices) which askChoice interpreted as opts=title (a string),
      // ignoring the other two args — the dialog rendered with no
      // buttons and the user couldn't Locate / Remove / Skip. Now
      // passed as a proper opts object.
      const choice = await askChoice?.({
        title: `"${ch.name}" — folder missing`,
        message:
          `This channel is subscribed but its folder is gone:\n\n` +
          ` ${ch.expected}\n\n` +
          "Locate the folder if you moved it, Remove the subscription if it's " +
          "no longer needed, or Skip to decide later.",
        choices: [
          { label: "Locate\u2026", value: "locate", kind: "primary" },
          { label: "Remove", value: "remove", kind: "danger" },
          { label: "Skip", value: "skip", kind: "ghost" },
        ],
      });
      if (choice === "locate") {
        const pick = await api.subs_browse_for_channel_folder?.(ch.name);
        if (pick?.ok && pick.folder_name) {
          const r = await api.subs_relocate_channel?.(
            { name: ch.name, url: ch.url }, pick.folder_name);
          if (r?.ok) {
            window._showToast?.(
              `Relocated "${ch.name}" \u2192 ${pick.folder_name}`, "ok");
          } else {
            window._showToast?.(r?.error || "Relocate failed.", "error");
          }
        } else if (pick && !pick.cancelled) {
          window._showToast?.(pick.error || "Invalid folder.", "error");
        }
      } else if (choice === "remove") {
        await api.subs_remove_channel?.({ name: ch.name, url: ch.url });
        window._showToast?.(`Removed "${ch.name}" from subs.`, "warn");
      }
      // skip / cancel — leave as-is
    }
  }

  window._reconcileMissingFolders = _reconcileMissingFolders;
})();
