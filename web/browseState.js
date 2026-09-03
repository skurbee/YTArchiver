/**
 * web/browseState.js — canonical _browseState owner.
 *
 * Loaded early (right after util.js / bridge.js) so every other
 * extracted module captures the SAME object reference for
 * `window._browseState` at IIFE-evaluation time. Without this, modules
 * that did `const _browseState = window._browseState || {}` at the top
 * of their IIFE would each capture a separate empty `{}` and never see
 * the real state set by app.js later.
 *
 * Exposes:
 *   window._browseState — { submode, view, channels, currentChannel,
 *                            videos, currentVideo, watchReturnTo }
 *
 * Mutators (currentVideo, currentChannel, etc.) all happen via direct
 * property assignment on this shared object, so every reader sees the
 * latest value.
 */
(function () {
  "use strict";

  if (window._browseState) return; // already published — be idempotent

  window._browseState = {
    submode: "channels",   // current sidebar mode
    view: "channels",      // within Channels: channels|videos|watch
    channels: [],          // last fully-hydrated channel-card data
    channelsReady: false,  // false until browse_list_channels succeeds
    pendingChannels: [],   // cheap subscription rows held behind the loader
    channelsError: false,  // keeps Retry visible after a failed first load
    channelsRefreshState: "idle", // idle|initial|refreshing|error
    channelSummaryOverrides: new Map(), // fresher counts during hydration
    channelSummaryRevision: 0,
    currentChannel: null,
    videos: [],
    currentVideo: null,
    watchReturnTo: null,   // set when Watch view is entered from Recent
                           // / Search / Bookmark; back-button returns here
  };
})();
