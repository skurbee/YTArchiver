/* Own selection, rendered media and independent metadata/playback revisions. */
(function () {
  "use strict";
  const state = window._browseState;
  let selected = state.currentVideo;
  let rendered = null;
  let open = 0;
  let intent = 0;
  let metadata = 0;
  let transcript = 0;
  let playback = 0;
  let navigation = 0;
  let renderedOpen = null;
  const normalizePath = value => String(value || "").replace(/\\/g, "/").toLowerCase();
  const identity = video => !video ? "" : video.video_id ? `id:${video.video_id}`
    : video.filepath ? `file:${normalizePath(video.filepath)}`
    : `fallback:${video.channel || ""}\u0000${video.title || ""}`;
  function sameVideo(left, right) {
    if (!left || !right) return false;
    if (left.video_id && right.video_id) return left.video_id === right.video_id;
    if (left.filepath && right.filepath) return normalizePath(left.filepath) === normalizePath(right.filepath);
    return (left.title || "") === (right.title || "") && (left.channel || "") === (right.channel || "");
  }
  function ticket(video = selected) {
    return Object.freeze({ open, intent, metadata, transcript, playback, identity: identity(video) });
  }
  function isCurrent(request) {
    return !!request && request.open === open && request.identity === identity(selected);
  }
  function isRendered(request) {
    return isCurrent(request) && renderedOpen === open && request.identity === identity(rendered);
  }
  function begin(video, reservedIntent) {
    if (Number.isFinite(reservedIntent)) {
      if (reservedIntent !== intent) return null;
    } else ++intent;
    ++open;
    selected = video;
    return ticket();
  }
  function reserveResolution() {
    return Object.freeze({ intent: ++intent, navigation, submode: state.submode, view: state.view,
      tab: document.querySelector('.tab.active[data-tab]')?.dataset.tab });
  }
  function resolutionCurrent(request) {
    return request.intent === intent && request.navigation === navigation
      && request.submode === state.submode && request.view === state.view
      && request.tab === document.querySelector('.tab.active[data-tab]')?.dataset.tab;
  }
  function actionVideo() {
    if (rendered && selected && !sameVideo(rendered, selected)) return null;
    if (rendered && Number.isFinite(renderedOpen) && renderedOpen !== open) return null;
    return rendered || selected;
  }
  const session = {
    identity, sameVideo, ticket, isCurrent, isRendered, begin, reserveResolution, resolutionCurrent,
    reserveIntent: () => ++intent,
    intentCurrent: value => value === intent,
    render(video) { rendered = video; renderedOpen = open; },
    beginMetadata(video) { ++metadata; return ticket(video); },
    metadataCurrent: request => request.metadata === metadata && isRendered(request),
    beginTranscript(video) { ++transcript; return ticket(video); },
    transcriptCurrent: request => !!request && request.transcript === transcript && isRendered(request),
    cancelPlayback: () => ++playback,
    navigationChanged: () => ++navigation,
    playbackCurrent: request => request.playback === playback && isRendered(request),
    actionVideo,
    get playback() { return playback; },
    get open() { return open; },
  };
  window.YT.watchSession = Object.freeze(session);

  // Keep existing route snapshots and bridge consumers on the same owner.
  Object.defineProperty(state, "currentVideo", {
    get: () => selected, set: value => { selected = value; }, enumerable: true,
  });
  for (const [name, descriptor] of Object.entries({
    _watchOpenToken: { get: () => open },
    _watchPlaybackIntent: { get: () => playback },
    _watchCurrentVideo: { get: () => rendered, set: value => { rendered = value; } },
    _watchRenderedToken: { get: () => renderedOpen, set: value => { renderedOpen = value; } },
  })) Object.defineProperty(window, name, { ...descriptor, configurable: true });
  window._reserveWatchOpenIntent = session.reserveIntent;
  window._isWatchOpenIntentCurrent = session.intentCurrent;
})();
