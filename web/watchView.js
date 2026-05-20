/* ═══════════════════════════════════════════════════════════════════════
   watchView.js — YTArchiver Watch-view rendering, karaoke + captions

   Extracted from logs.js (the watch view + karaoke + caption-track
   section, ~850 lines). Owns:
     • The Watch view render path (`renderWatchView`)
     • The retranscribe-complete refresh hook
     • Source banner (Whisper / YT-captions / unknown)
     • Metadata drawer (description, comments, view/like stats)
     • Description-timestamp click-to-seek parser
     • <video> element source loading + race-token guard
     • Persisted volume across video switches
     • Karaoke rAF loop (active segment + active word highlights)
     • WebVTT cue track for native fullscreen captions
     • Caption preference (size/bg) cache + applier

   Externally-published handles (every other module reads these
   via `window.X` — they form the public surface of this module):
     window._onRetranscribeComplete   — Python evaluate_js entry point
     window.renderWatchView           — called by browseView.js
     window.loadWatchMetadataDrawer   — called by app.js (refresh btn)
     window._scrollTranscriptTo       — shared with karaoke loop
     window.setCaptionPref            — called by app.js (toolbar select)
     window._watchCurrentVideo        — read by app.js (current state)

   This file lives in the same IIFE pattern as logs.js so all internal
   helpers can call each other without `window.` prefix, but is loaded
   as a separate <script> tag so logs.js stays focused on log rendering.
   ═══════════════════════════════════════════════════════════════════════ */

(function () {
  "use strict";

  /** Retranscribe completion hook — called by Python via evaluate_js
   * when a `transcribe_retranscribe` job finishes. If the completed
   * video is the one currently on screen, refetch the transcript and
   * re-render the Watch view (this flips the source banner from
   * "auto-captions — approximate" to "Whisper transcription"). Mirrors
   * ArchivePlayer `_ytStartProgressPoll`'s post-finish refresh at
   * static/app.js:1209-1221. */
  window._onRetranscribeComplete = async function (payload) {
    try {
      const { video_id, filepath } = payload || {};
      // Clear the in-flight entry for THIS video so other watch-view
      // navigations stop showing its progress. Pass the video_id so
      // only the finished job is removed — multiple retranscribes can
      // be queued concurrently across different videos, and clearing
      // them all on one completion would mis-paint sibling buttons.
      try { window._retranscribeWatchClear?.(video_id || ""); } catch {}
      const wv = document.getElementById("view-watch");
      if (!wv || wv.style.display === "none") return;
      const cur = window._watchCurrentVideo || null;
      if (!cur) return;
      // Normalize filepaths — Python sends os.path.normpath() output which
      // uses backslashes on Windows, the video obj's `filepath` field may
      // carry whatever separator the source set. reported the Watch
      // view not refreshing after retranscribe; Python-side logs showed
      // the event fired but filepath comparison missed due to slash
      // direction. Compare forward-slashed + lowercased on both sides.
      const _norm = (s) => String(s || "").replace(/\\/g, "/").toLowerCase();
      const match = (video_id && cur.video_id && video_id === cur.video_id)
                 || (filepath && cur.filepath &&
                     _norm(filepath) === _norm(cur.filepath));
      if (!match) return;
      const api = window.pywebview?.api;
      if (!api?.browse_get_transcript) return;
      const res = await api.browse_get_transcript({
        video_id: cur.video_id || undefined,
        title: cur.title || "",
      });
      let segments = [];
      let sourceInfo = null;
      if (Array.isArray(res)) segments = res;
      else if (res && res.segments) {
        segments = res.segments;
        sourceInfo = res.source || null;
      }
      // when the retranscribe completes but the segment
      // fetch comes back empty (JSONL write failed, FTS ingest failed,
      // whisper produced nothing), the old transcript used to stay on
      // screen alongside a "complete" toast — user assumed the
      // retranscribe worked fine. Now explicitly replace with a
      // placeholder and show a warn toast so the failure is visible.
      if (!segments.length) {
        window.renderWatchView(cur, [], sourceInfo,
                               { skipVideoReload: true });
        window._showToast?.(
          "Re-transcription finished but produced no segments — check the log.",
          "warn");
        return;
      }
      const transcript = segments.map(seg => ({
        ts: (window._formatTs ? window._formatTs(seg.s) : ""),
        text: seg.t, words: seg.w, s: seg.s, e: seg.e,
      }));
      // Re-render transcript + source banner ONLY — skip the video
      // source reload + metadata drawer refresh. `skipVideoReload:true`
      // leaves the <video> element's src and playhead alone, so the
      // video keeps playing from wherever it was without the restart
      // saw in earlier versions.
      window.renderWatchView(cur, transcript, sourceInfo,
                             { skipVideoReload: true });
      window._showToast?.("Re-transcription complete — transcript updated.", "ok");
    } catch (e) { /* ignore */ }
  };

  /** Render the Watch view: loads the real video file into <video> and
   * builds per-word transcript spans with (s, e) timestamps for karaoke.
   *
   * transcript items: { s, e, t, w:[{w,s,e},...], ts } — s/e in seconds.
   * source: { source: "whisper"|"yt_captions_punct"|"yt_captions_raw"|"unknown", raw: "..." }
   * `video.filepath` is used to request a file:// URL from the backend.
   *
   * Render mode: single continuous flowing body (no per-segment divs,
   * no [timestamp] inline prefixes) — matches ArchivePlayer. A source
   * banner above the body tells the user whether the transcript came
   * from Whisper or YouTube auto-captions, and for auto-captions offers
   * an inline "Re-transcribe with Whisper" link for better accuracy.
   */
  window.renderWatchView = function (video, transcript, sourceInfo, opts) {
    const title = document.getElementById("watch-title");
    const meta = document.getElementById("watch-meta");
    const tr = document.getElementById("watch-transcript");
    const vEl = document.getElementById("watch-video");
    const ph = document.getElementById("watch-video-placeholder");
    if (!title || !meta || !tr) return;

    // `opts.skipVideoReload`: set by _onRetranscribeComplete so the
    // <video> element isn't re-sourced (which would restart playback
    // from 0). We only need to refresh the transcript + source banner
    // when the retranscribe for the currently-playing video finishes.
    const skipVideoReload = !!(opts && opts.skipVideoReload);

    title.textContent = video.title || "Video Title";
    const parts = [];
    if (video.channel) parts.push(video.channel);
    if (video.uploaded) parts.push(video.uploaded);
    if (video.duration) parts.push(video.duration);
    if (video.views) parts.push(video.views + " views");
    meta.textContent = parts.join(" · ");

    // Stash for `_onRetranscribeComplete` — when the Python side finishes
    // a retranscribe, it pushes an event; the handler checks this ref
    // to decide whether the completed job matches what's on screen.
    window._watchCurrentVideo = video;

    // Repaint the Re-transcribe button to reflect whether THIS video
    // has an in-flight retranscribe. Without this, navigating from
    // Video A (mid-retranscribe) to Video B left B's button locked in
    // A's "Re-transcribing… X%" state, so the user couldn't queue B.
    try { window._syncWatchRetranscribeButton?.(); } catch {}

    if (!skipVideoReload) {
      _loadVideoSource(video, vEl, ph);
      _loadWatchMetadataDrawer(video);
    }

    tr.innerHTML = "";
    if (!transcript || transcript.length === 0) {
      tr.innerHTML = '<div style="color: var(--c-dim); font-style: italic;">No transcript available.</div>';
      _unbindKaraoke(vEl);
      // Clear caption cues too — previously a failed retranscribe left
      // the previous video's cues on the TextTrack so they kept
      // appearing as overlay while the still-playing video kept going
      // (audit: watchView.js:88-91).
      try { _setCueTrackFromTranscript(vEl, []); } catch { /* ignore */ }
      return;
    }

    const frag = document.createDocumentFragment();

    // Source banner — Whisper / YT auto-captions / unknown. Mirrors
    // ArchivePlayer's `_ytSourceBannerHTML`. Pass the actual transcript
    // so the banner can verify the "(punctuation restored)" claim
    // against real content — for legacy videos the source tag says
    // YT+PUNCTUATION (Transcript.txt was punct-restored) but the
    // per-segment data in the watch view was never punctuated, so the
    // banner used to lie.
    const bannerEl = _buildSourceBanner(sourceInfo, video, transcript);
    if (bannerEl) frag.appendChild(bannerEl);

    // Flatten every word across every segment into one continuous flowing
    // body. No per-segment div, no inline [timestamp] prefixes — matches
    // ArchivePlayer. The `seg` wrappers are kept for karaoke so the whole
    // block highlights while the active word inside it gets stronger styling.
    const body = document.createElement("div");
    body.className = "watch-transcript-body";
    const segEls = [];
    for (const seg of transcript) {
      const segEl = document.createElement("span");
      segEl.className = "seg";
      segEl.dataset.s = seg.s ?? 0;
      segEl.dataset.e = seg.e ?? 0;
      const words = Array.isArray(seg.words) && seg.words.length
        ? seg.words
        : (seg.w && seg.w.length ? seg.w : null);
      // Tokenize the segment text on whitespace. After the "Restore
      // transcript punctuation" pass runs, seg.text/seg.t holds the
      // punctuated paragraph form while the words array stays raw
      // (drives the video overlay's karaoke captions, which are
      // intentionally unpunctuated). When the token count matches the
      // words count we
      // render the punctuated tokens but keep the per-word timestamps —
      // user sees readable prose with full karaoke + click-to-seek.
      // If counts ever disagree (cap-flush rollover words, manual
      // edits, etc.) fall back to the raw words.
      const segText = seg.t || seg.text || "";
      const textTokens = segText.trim().split(/\s+/).filter(Boolean);
      // Per-word click handlers were causing memory pressure on long
      // videos — a 10k-word transcript allocated 10k closures that
      // GC eventually had to reap. Use event delegation instead:
      // one click listener on the body root walks up to the nearest
      // .word and seeks. Set up below, once per render.
      if (words && textTokens.length === words.length) {
        for (let i = 0; i < words.length; i++) {
          const wobj = words[i];
          const span = document.createElement("span");
          span.className = "word";
          span.dataset.s = wobj.s ?? 0;
          span.dataset.e = wobj.e ?? 0;
          span.textContent = textTokens[i] + " ";
          segEl.appendChild(span);
        }
      } else if (words) {
        for (const wobj of words) {
          const span = document.createElement("span");
          span.className = "word";
          span.dataset.s = wobj.s ?? 0;
          span.dataset.e = wobj.e ?? 0;
          span.textContent = (wobj.w || "") + " ";
          segEl.appendChild(span);
        }
      } else {
        const span = document.createElement("span");
        span.className = "word";
        span.dataset.s = String(seg.s ?? 0);
        span.textContent = segText + " ";
        segEl.appendChild(span);
      }
      // Trailing space between segments keeps words from running together.
      segEl.appendChild(document.createTextNode(" "));
      body.appendChild(segEl);
      segEls.push(segEl);
    }
    // Delegated click handler — one listener on body instead of N
    // per-word listeners. Walks up to nearest .word and seeks via
    // dataset.s. Replaces the previous closure-per-word pattern
    // that allocated thousands of listeners for long transcripts.
    body.addEventListener("click", (e) => {
      const wEl = e.target && e.target.closest && e.target.closest(".word");
      if (!wEl) return;
      const _s = parseFloat(wEl.dataset.s);
      if (Number.isFinite(_s)) _seekTo(vEl, _s);
    });
    frag.appendChild(body);
    tr.appendChild(frag);
    _bindKaraoke(vEl, tr, segEls);
    // Build (or refresh) the WebVTT caption track for the video overlay.
    // Uses the same per-word timing the karaoke loop reads from the DOM,
    // but delivered as VTTCues so the browser handles fullscreen display
    // for free — no custom-controls rebuild needed when the user goes
    // fullscreen on the <video> element.
    _setCueTrackFromTranscript(vEl, transcript);
    _applyCaptionPrefs(vEl);
  };

  /** Build the source-banner div for the Watch view transcript panel.
   * Returns a DOM element or null. Mirrors ArchivePlayer's
   * `_ytSourceBannerHTML` (static/app.js:1106) EXACTLY — same text,
   * same four cases, same link wording. The "unknown, no raw" case
   * returns null so no banner appears at all (ArchivePlayer returns
   * the empty string for that case). */
  function _buildSourceBanner(sourceInfo, video, transcript) {
    const src = (sourceInfo && sourceInfo.source) || "unknown";
    const raw = (sourceInfo && sourceInfo.raw) || "";

    // Sample the first few segments' text to see if the per-segment
    // data ACTUALLY has sentence punctuation. The source tag says
    // YT+PUNCTUATION for Transcript.txt content, but for many legacy
    // videos the per-segment text the watch view actually renders
    // wasn't punctuated. Downgrade the badge in that case so the
    // banner reflects what the user can see.
    const _hasContentPunct = (() => {
      if (!Array.isArray(transcript) || transcript.length === 0) return false;
      // Broader sample: a music-intro segment list ("♪♪♪", "[Music]")
      // followed by a punctuated body used to mis-label the banner
      // because only the first 8 segments were sampled (audit:
      // watchView.js:265-270). Sample up to ~50 segments at regular
      // strides across the whole transcript so legitimate punctuation
      // anywhere in the body is detected.
      const _n = transcript.length;
      const _stride = Math.max(1, Math.floor(_n / 50));
      let _sampled = "";
      for (let _i = 0; _i < _n; _i += _stride) {
        const _seg = transcript[_i];
        _sampled += " " + (_seg && (_seg.t || _seg.text || ""));
        if (_sampled.length > 4096) break; // cap regex input
      }
      return /[.,!?;:]/.test(_sampled);
    })();

    const banner = document.createElement("div");
    banner.className = "watch-src-banner";
    const dot = document.createElement("span");
    dot.className = "watch-src-dot";

    const buildRetranscribeLink = () => {
      const a = document.createElement("a");
      a.href = "#";
      a.className = "watch-retranscribe-link";
      a.textContent = "re-transcribe with Whisper";
      a.addEventListener("click", (ev) => {
        ev.preventDefault();
        const btn = document.getElementById("btn-watch-retranscribe");
        if (btn) btn.click();
      });
      return a;
    };

    if (src === "whisper") {
      banner.classList.add("whisper");
      banner.appendChild(dot);
      // Raw like "WHISPER small" or "WHISPER:small" — pull everything
      // after the first whitespace/colon as the model label.
      // Strip internal "+NO-PUNCT" diagnostic — users don't need to see
      // that tag in the displayed model name (issue #151).
      const parts = (raw || "").trim().split(/[\s:]+/);
      let model = parts.length > 1 ? parts.slice(1).join(" ") : "";
      model = model.replace(/\+no-?punct/gi, "").trim();
      const txt = document.createElement("span");
      txt.textContent = model
        ? `Whisper transcription — ${model.toLowerCase()} model`
        : "Whisper transcription";
      banner.appendChild(txt);
      return banner;
    }
    if (src === "yt_captions_punct" || src === "yt_captions_raw") {
      banner.classList.add("yt");
      banner.appendChild(dot);
      // Show "(punctuated)" whenever the rendered content actually
      // has sentence punctuation, regardless of which source tag the
      // file got at ingest. The legacy tag system distinguished
      // "punct restoration pass ran" vs "didn't run" — but modern YT
      // auto-cap arrives already punctuated and skips the restoration
      // pass, so a yt_captions_raw video like Vox May 2026 can have
      // fully punctuated text while a yt_captions_punct legacy video
      // has lowercase per-segment content. Basing the badge on visible
      // content reflects what the user sees instead of pipeline state.
      const label = _hasContentPunct
        ? "YouTube auto-captions (punctuated) — transcript is approximate · "
        : "YouTube auto-captions — transcript is approximate · ";
      banner.appendChild(document.createTextNode(label));
      banner.appendChild(buildRetranscribeLink());
      banner.appendChild(document.createTextNode(" for improved results"));
      return banner;
    }
    // Unknown. Per ArchivePlayer app.js:1140 — don't flag with a warning
    // since we genuinely don't know. Show a neutral tag if we have a raw
    // string, otherwise NOTHING at all.
    if (raw) {
      banner.classList.add("unknown");
      banner.appendChild(dot);
      banner.appendChild(document.createTextNode(`Source: ${raw}`));
      return banner;
    }
    return null;
  }

  // Load + render the Watch-view metadata drawer. Reads the aggregated
  // `.{ch_name} Metadata.jsonl` via the `browse_get_video_metadata` Api.
  // Matches YTArchiver.py:26750 _fetch_video_metadata display: description,
  // view_count, like_count, upload_date, top 50 comments.
  // Exposed via window.loadWatchMetadataDrawer so the Refresh-metadata
  // button (wired in app.js) can re-render the drawer in place after a
  // per-video re-fetch, instead of forcing a Back-and-reopen.
  window.loadWatchMetadataDrawer = (video) => _loadWatchMetadataDrawer(video);
  async function _loadWatchMetadataDrawer(video) {
    const drawer = document.getElementById("watch-meta-drawer");
    if (!drawer) return;
    const statsEl = document.getElementById("watch-meta-stats");
    const descEl = document.getElementById("watch-meta-description");
    const commentsEl = document.getElementById("watch-meta-comments");
    const countEl = document.getElementById("watch-meta-comments-count");
    // Reset state immediately so a slow fetch doesn't bleed previous video's data
    if (statsEl) statsEl.textContent = "";
    if (descEl) descEl.textContent = "Loading…";
    if (commentsEl) commentsEl.innerHTML = "";
    if (countEl) countEl.textContent = "";

    const api = window.pywebview?.api;
    if (!api?.browse_get_video_metadata) {
      if (descEl) descEl.textContent = "(Metadata unavailable in browser-preview mode)";
      return;
    }
    let res;
    try {
      res = await api.browse_get_video_metadata({
        filepath: video.filepath || "",
        video_id: video.video_id || "",
        title: video.title || "",
        channel: video.channel || "",
      });
    } catch (e) {
      if (descEl) descEl.textContent = "(Failed to load metadata.)";
      return;
    }
    if (!res?.ok || !res.meta) {
      if (descEl) descEl.textContent =
        "No metadata on disk for this video yet. Run 'Download metadata' on the channel to fetch it.";
      return;
    }
    const meta = res.meta;
    // Stats line
    if (statsEl) {
      const bits = [];
      if (meta.view_count) bits.push(`${Number(meta.view_count).toLocaleString()} views`);
      if (meta.like_count) bits.push(`${Number(meta.like_count).toLocaleString()} likes`);
      if (meta.upload_date && String(meta.upload_date).length === 8) {
        const d = meta.upload_date;
        bits.push(`Uploaded ${d.slice(0, 4)}-${d.slice(4, 6)}-${d.slice(6, 8)}`);
      }
      statsEl.textContent = bits.join(" · ");
    }
    // Description — scan for YouTube-style timestamps (M:SS, MM:SS,
    // H:MM:SS) and render each as a clickable span that seeks the
    // <video> element to that time. Matches YouTube's own
    // description-timestamp behavior. "timestamps in descriptions
    // are supposed to be clickable, just like youtube itself".
    if (descEl) {
      _renderDescriptionWithTimestamps(
        descEl,
        (meta.description || "").trim() || "(No description.)");
    }
    // Comments — top N by likes
    const comments = Array.isArray(meta.comments) ? meta.comments : [];
    if (countEl) {
      countEl.textContent = comments.length
        ? `${comments.length} top comment${comments.length === 1 ? "" : "s"}`
        : "No comments archived";
    }
    if (commentsEl) {
      commentsEl.innerHTML = "";
      const frag = document.createDocumentFragment();
      for (const c of comments) {
        const row = document.createElement("div");
        row.className = "watch-comment";
        const author = document.createElement("span");
        author.className = "watch-comment-author";
        author.textContent = c.author || "(anon)";
        const likes = document.createElement("span");
        likes.className = "watch-comment-likes";
        if (c.likes) likes.textContent = `${Number(c.likes).toLocaleString()} ▲`;
        const head = document.createElement("div");
        head.className = "watch-comment-head";
        head.append(author, likes);
        const body = document.createElement("div");
        body.className = "watch-comment-body";
        // Render comment body with clickable timestamps (same helper
        // as the description). Lots of YT comments carry "at 4:51 ..."
        // style timestamps the viewer expects to click.
        _renderDescriptionWithTimestamps(body, c.text || "");
        row.append(head, body);
        frag.appendChild(row);
      }
      commentsEl.appendChild(frag);
    }
  }

  // Render a description string with any YouTube-style timestamps
  // (M:SS, MM:SS, H:MM:SS, HH:MM:SS) converted to clickable seek
  // links. The click handler sets `#watch-video`.currentTime to the
  // parsed seconds and plays from there.
  //
  // Regex accepts any run of digits in the minute/hour positions so
  // jumpy descriptions like "1:22:07" and "7:03" both match. The
  // `(?<![\d:])` lookbehind prevents matches inside "3:14:15 PM" or
  // "ratio 2:30" when they sit next to another digit or colon.
  function _renderDescriptionWithTimestamps(el, text) {
    el.innerHTML = "";
    if (!text) return;
    // Split on newlines, render timestamps per line so line breaks
    // survive. textContent on a <span> inside a div preserves \n if
    // we use `white-space: pre-wrap` (which .watch-meta-description
    // already has) — but building paragraph nodes is cleaner.
    // Instead we use a single container and insert `\n` literally
    // between match-split fragments; CSS preserves the newlines.
    // Lookbehind-free variant. The old regex used `(?<![\d:])` which
    // throws at parse time on stale WebView2 / older Safari (audit:
    // watchView.js:457). We now do the boundary check manually:
    // strip matches whose preceding character is `[0-9:]`, so
    // "3:14:15 PM" and "ratio 2:30" still don't get auto-linked.
    const RX = /(\d{1,3}):(\d{2})(?::(\d{2}))?\b/g;
    let lastIdx = 0;
    let m;
    while ((m = RX.exec(text)) !== null) {
      const _prev = m.index > 0 ? text.charAt(m.index - 1) : "";
      if (_prev && (_prev === ":" || (_prev >= "0" && _prev <= "9"))) {
        // Skip this match — preceded by digit or colon. Advance one
        // char and retry so a later non-skipped match isn't missed.
        if (RX.lastIndex === m.index) RX.lastIndex++;
        continue;
      }
      // Pre-match text
      if (m.index > lastIdx) {
        el.appendChild(document.createTextNode(text.slice(lastIdx, m.index)));
      }
      const h = m[3] ? Number(m[1]) : 0;
      const mn = m[3] ? Number(m[2]) : Number(m[1]);
      const s = m[3] ? Number(m[3]) : Number(m[2]);
      // Sanity — "1:64" isn't a valid timestamp; skip it.
      if (mn > 59 || s > 59) {
        el.appendChild(document.createTextNode(m[0]));
      } else {
        const secs = h * 3600 + mn * 60 + s;
        const span = document.createElement("span");
        span.className = "desc-ts";
        span.dataset.t = String(secs);
        span.textContent = m[0];
        span.addEventListener("click", (e) => {
          e.preventDefault();
          const vEl = document.getElementById("watch-video");
          if (!vEl) return;
          try {
            // Clamp to duration so a malformed description
            // timestamp (e.g. "9:99:99" decoded as 35999s on a
            // 2-minute video) doesn't throw on Safari / older
            // Chromium (audit: watchView.js:480-485). Modern
            // Chrome silently clamps; clamp explicitly for cross-
            // browser consistency.
            const _dur = Number.isFinite(vEl.duration) ? vEl.duration : secs;
            vEl.currentTime = Math.min(secs, _dur);
            vEl.play().catch(() => {});
          } catch {}
        });
        el.appendChild(span);
      }
      lastIdx = m.index + m[0].length;
    }
    // Trailing text
    if (lastIdx < text.length) {
      el.appendChild(document.createTextNode(text.slice(lastIdx)));
    }
  }

  // Expand/collapse toggle for the metadata drawer. Wired once on boot —
  // survives across Watch-view renders.
  function _initWatchMetaDrawerToggle() {
    const head = document.getElementById("watch-meta-head");
    const body = document.getElementById("watch-meta-body");
    const arrow = document.getElementById("watch-meta-arrow");
    const drawer = document.getElementById("watch-meta-drawer");
    if (!head || !body || !arrow || !drawer) return;
    // Start expanded so description + comments are visible on first
    // load. Body has its own max-height scroll, so this doesn't crowd
    // out the transcript pane.
    body.style.display = "";
    arrow.innerHTML = "▾";
    head.addEventListener("click", () => {
      const collapsed = drawer.classList.toggle("collapsed");
      body.style.display = collapsed ? "none" : "";
      arrow.innerHTML = collapsed ? "▸" : "▾";
    });
  }
  // Fire once when watchView.js loads
  if (document.readyState !== "loading") _initWatchMetaDrawerToggle();
  else document.addEventListener("DOMContentLoaded", _initWatchMetaDrawerToggle);

  async function _loadVideoSource(video, vEl, ph) {
    if (!vEl) return;
    const fp = video.filepath || "";
    const api = window.pywebview?.api;
    // Race-token check: capture _watchOpenToken at entry so we can
    // detect "user navigated away during URL fetch". Before this fix,
    // a video would start playback in the background after the user
    // clicked Back during the load — the late `browse_video_url`
    // response would still set vEl.src + call play(), starting playback
    // in a hidden Watch view with no way to stop it short of returning
    // to the same video and pausing manually.
    const _entryToken = (typeof window._watchOpenToken === "number")
      ? window._watchOpenToken : 0;
    // Show a "Loading…" state on the placeholder while we await
    // browse_video_url. Without this the user sees a blank box
    // (or the previous video's stale state) between clicking the
    // thumbnail and the <video> element revealing — which feels
    // broken on a cold archive walk where the API can take a beat.
    // Rebuild the placeholder contents wholesale because the error
    // path further down also rewrites innerHTML and can leave the
    // sub-elements gone.
    if (ph && fp) {
      ph.style.display = "";
      ph.innerHTML =
        '<div class="watch-play-icon" style="visibility:hidden;">▶</div>' +
        '<div class="watch-placeholder-text" style="font-size:13px;color:var(--c-text);">'
        + '<span class="spinner-inline"></span>Loading…</div>';
      // Hide the <video> element so its previous src doesn't flash.
      vEl.style.display = "none";
    }
    let url = null;
    let errorDetail = "";
    if (fp && api?.browse_video_url) {
      try {
        const r = await api.browse_video_url(fp);
        if (r?.ok && r.url) url = r.url;
        else if (r && !r.ok) errorDetail = r.error || "unknown error";
      } catch (e) { errorDetail = String(e); }
    } else if (!fp) {
      errorDetail = "No video selected";
    }
    // CRITICAL: drop the late response on the floor if the user has
    // navigated away. Symptoms before this guard: clicking Back during
    // load → video starts playing in the hidden Watch view, audio plays
    // in the background, no UI affordance to stop it. Two checks:
    //   1. Token mismatch = user opened a different video.
    //   2. View no longer "watch" = user backed out / changed sub-view.
    const _stillOnSameVideo = (
      typeof window._watchOpenToken !== "number"
      || window._watchOpenToken === _entryToken);
    const _stillOnWatchView = (
      !window._browseState
      || window._browseState.view === "watch");
    if (!_stillOnSameVideo || !_stillOnWatchView) {
      // Make sure the element is fully torn down — without this, a
      // stale src from a previous load might still be in the element
      // and play when we navigate back. _stopWatchVideo already runs
      // in showView leaving-watch but it can't catch an in-flight
      // _loadVideoSource that resolves AFTER it ran.
      try {
        vEl.pause();
        vEl.removeAttribute("src");
        if (typeof vEl.load === "function") vEl.load();
      } catch { /* noop */ }
      return;
    }
    if (url) {
      // Stop the previous video's load explicitly before swapping src.
      // Without this, rapid back-to-back video clicks can leave the
      // old request holding the file handle on Z:\ DrivePool until
      // the local HTTP server times out (~30s) — at peak this
      // transiently leaks N file handles. Pause+removeAttribute+load
      // is the documented clean-teardown pattern.
      try {
        vEl.pause();
        vEl.removeAttribute("src");
        if (typeof vEl.load === "function") vEl.load();
      } catch { /* noop */ }
      vEl.src = url;
      vEl.style.display = "";
      if (ph) {
        ph.style.display = "none";
        // Reset placeholder DOM back to its default empty-state
        // structure so the NEXT empty/error state has the icon and
        // standard label to work with again.
        ph.innerHTML =
          '<div class="watch-play-icon">▶</div>' +
          '<div class="watch-placeholder-text">Select a video to play</div>';
      }
      // Volume: default 20% (matches OLD YTArchiver). Persists across
      // video switches via localStorage so the user's slider adjustment
      // survives — re-applied on every new video load so the HTMLMediaElement
      // doesn't reset to 100% on src change. Saves back on volume-change.
      _applyPersistedVolume(vEl);
      // Try autoplay (user has already clicked in the app, so this is allowed)
      vEl.play().catch(() => { /* user can click play */ });
    } else {
      // No playback possible — show placeholder with actionable error.
      // reported clicking a "(1)" duplicate that had a stale DB
      // entry (file missing from disk) — the old placeholder just said
      // "Select a video to play" which was misleading when a video IS
      // selected but the file is gone. Now it explains.
      vEl.src = "";
      vEl.style.display = "none";
      if (ph) {
        ph.style.display = "";
        if (errorDetail && errorDetail !== "unknown error") {
          const label = ph.querySelector(".placeholder-label") || ph;
          // Escape minimally for safety since `fp` could contain
          // anything; we only show the last path segment.
          const leaf = fp ? fp.split(/[\\/]/).pop() : "";
          label.innerHTML =
            `<div style="font-size:13px;color:var(--c-log-red);margin-bottom:4px;">` +
            `⚠ ${errorDetail}</div>` +
            (leaf
              ? `<div style="font-size:11px;color:var(--c-dim);">${leaf}</div>`
              : "") +
            `<div style="font-size:11px;color:var(--c-dim);margin-top:6px;">` +
            `The index entry may be stale — try Rescan archive.</div>`;
        }
      }
    }
  }

  // Read the saved volume (fallback to 0.2 = 20%) and apply it to the
  // video element. Also wires a one-time `volumechange` listener so the
  // user's slider moves get persisted — subsequent videos start at the
  // last value, not 20% again. A flag on the element prevents
  // double-wiring across re-binds.
  function _applyPersistedVolume(vEl) {
    let vol = 0.2;
    try {
      const saved = localStorage.getItem("ytarchiver_watch_volume");
      if (saved !== null) {
        const n = parseFloat(saved);
        if (Number.isFinite(n) && n >= 0 && n <= 1) vol = n;
      }
    } catch (e) { /* localStorage unavailable */ }
    try { vEl.volume = vol; } catch (e) { /* noop */ }
    if (!vEl.dataset.volHooked) {
      vEl.dataset.volHooked = "1";
      vEl.addEventListener("volumechange", () => {
        try { localStorage.setItem("ytarchiver_watch_volume", String(vEl.volume)); }
        catch (e) { /* noop */ }
      });
    }
  }

  function _seekTo(vEl, seconds) {
    if (!vEl || !vEl.duration) return;
    try {
      vEl.currentTime = Math.max(0, Number(seconds) || 0);
      vEl.play().catch(() => {});
    } catch { /* noop */ }
  }

  // Scroll an element (a .seg or word) into view WITHIN its transcript
  // container only — DOES NOT scroll any ancestor scroll container.
  // plain `scrollIntoView` walks up the parent chain and also
  // scrolls the outer .browse-view, which pushed the video out of
  // frame as karaoke followed along. This helper sets the container's
  // scrollTop directly using getBoundingClientRect deltas so the
  // calculation is offsetParent-independent.
  window._scrollTranscriptTo = function (container, target) {
    if (!container || !target) return;
    try {
      const cRect = container.getBoundingClientRect();
      const tRect = target.getBoundingClientRect();
      const topInScrollable =
        (tRect.top - cRect.top) + container.scrollTop;
      const y = topInScrollable
              - (container.clientHeight / 2)
              + (tRect.height / 2);
      container.scrollTo({
        top: Math.max(0, y),
        behavior: "smooth",
      });
    } catch (_e) { /* fallback: skip, don't throw */ }
  };

  // ── Karaoke: requestAnimationFrame loop (.txt millisecond timestamps)
  //
  // Previously `timeupdate` fired every ~250ms — fine at 1x, but at 2x
  // playback the highlight lagged a full word behind and short words
  // were skipped entirely. The rAF loop runs at the display refresh
  // rate (60fps) regardless of playback speed, so the active-word
  // marker tracks Whisper's millisecond-precision timestamps cleanly
  // even at fast forward.

  let _karaokeHandler = null;   // legacy: kept around for compat
  let _karaokeRafId = null;
  function _unbindKaraoke(vEl) {
    if (_karaokeRafId !== null) {
      try { cancelAnimationFrame(_karaokeRafId); } catch {}
      _karaokeRafId = null;
    }
    if (vEl && _karaokeHandler) {
      vEl.removeEventListener("timeupdate", _karaokeHandler);
    }
    _karaokeHandler = null;
  }

  function _bindKaraoke(vEl, trWrap, segEls) {
    _unbindKaraoke(vEl);
    if (!vEl || !segEls.length) return;
    let lastSegIdx = -1;
    let lastWordEl = null;

    const _tick = () => {
      const t = vEl.currentTime;
      if (t == null) return;
      // Binary-search segments for the one containing t
      const idx = _findSegIdx(segEls, t);
      if (idx !== lastSegIdx) {
        if (lastSegIdx >= 0 && segEls[lastSegIdx]) {
          segEls[lastSegIdx].classList.remove("active");
        }
        if (idx >= 0 && segEls[idx]) {
          segEls[idx].classList.add("active");
          // Auto-scroll transcript if toggle is on. CRITICAL: use
          // container-local scrollTop rather than `scrollIntoView` —
          // scrollIntoView walks up the parent chain and also scrolls
          // the outer `.browse-view` container, which pushes the video
          // off-screen as the karaoke follows along. "scrolls
          // the whole page down so you can't see the video ... we need
          // to scroll the transcription up to where the video is".
          // Compute the element's offset relative to the scrollable
          // transcript pane and set scrollTop directly so no ancestor
          // containers move.
          const follow = document.getElementById("watch-autofollow");
          if (follow?.checked && trWrap) {
            window._scrollTranscriptTo(trWrap, segEls[idx]);
          }
        }
        lastSegIdx = idx;
      }
      // Word highlight within the active segment
      if (idx >= 0 && segEls[idx]) {
        const seg = segEls[idx];
        const words = seg.querySelectorAll(".word");
        let newWordEl = null;
        for (const w of words) {
          const s = parseFloat(w.dataset.s || "0");
          const e = parseFloat(w.dataset.e || "0");
          if (s <= t && t <= e) {
            newWordEl = w;
            break;
          }
          if (s > t) break;
        }
        if (newWordEl !== lastWordEl) {
          if (lastWordEl) lastWordEl.classList.remove("active");
          if (newWordEl) newWordEl.classList.add("active");
          lastWordEl = newWordEl;
        }
      }
      // Schedule the next frame as long as the video is still loaded.
      // The unbind path cancels the in-flight rAF so this never leaks.
      if (vEl.isConnected) {
        _karaokeRafId = requestAnimationFrame(_tick);
      }
    };
    _karaokeRafId = requestAnimationFrame(_tick);
  }

  function _findSegIdx(segEls, t) {
    // Binary search segments by (s, e) bounds
    let lo = 0, hi = segEls.length - 1;
    while (lo <= hi) {
      const mid = (lo + hi) >> 1;
      const el = segEls[mid];
      const s = parseFloat(el.dataset.s || "0");
      const e = parseFloat(el.dataset.e || "0");
      if (t < s) hi = mid - 1;
      else if (t > e) lo = mid + 1;
      else return mid;
    }
    // Fall-through: return the last segment we've crossed (for gaps).
    // Bail with -1 when t is past the last segment's end so the
    // karaoke highlight doesn't get stuck on the last word forever
    // after the transcript ends (audit: watchView.js:781-792).
    if (segEls.length > 0) {
      const _lastE = parseFloat(segEls[segEls.length - 1].dataset.e || "0");
      if (Number.isFinite(_lastE) && t > _lastE) return -1;
    }
    let best = -1;
    for (let i = 0; i < segEls.length; i++) {
      const s = parseFloat(segEls[i].dataset.s || "0");
      if (s <= t) best = i;
      else break;
    }
    return best;
  }

  // ─── Video overlay captions (karaoke word-by-word) ─────────────────
  //
  // Uses the native TextTrack API + VTTCues so the browser draws the
  // caption itself. Benefits over a custom DOM overlay:
  //   • Fullscreen "just works" — native <video> fullscreen shows the
  //     cue track, an absolute-positioned sibling div would not.
  //   • Styling via `video::cue` with attribute selectors lets us
  //     swap size/background by flipping data-cap-* attributes on the
  //     <video> element — no per-cue restyle, no track rebuild.
  //
  // One TextTrack per video element, recycled across video changes:
  //   tracks created via addTextTrack can't be detached, so we stash
  //   the ref on the element and clear cues on each new transcript.
  function _ensureCapTrack(vEl) {
    if (!vEl) return null;
    if (vEl._capTrack) return vEl._capTrack;
    let t;
    try {
      t = vEl.addTextTrack("captions", "Overlay transcript", "en");
    } catch { return null; }
    vEl._capTrack = t;
    // Default to hidden until prefs apply (avoids a flash of unstyled
    // cues on first load when the user has the feature off).
    t.mode = "hidden";
    return t;
  }

  function _setCueTrackFromTranscript(vEl, transcript) {
    const t = _ensureCapTrack(vEl);
    if (!t) return;
    // Clear prior cues. The old `break` on removeCue throw left the
    // loop early at the first failure, stranding remaining cues from
    // the previous video — they then bled into the next video's
    // captions (visible as stale phrases overlaying unrelated
    // footage). With continue-on-throw + a bounded retry counter
    // we drain everything we can, accepting that some genuinely
    // un-removable cues stay (extremely rare, and at worst hidden
    // behind the new cues' time ranges).
    if (t.cues) {
      let _attempts = 0;
      while (t.cues.length && _attempts < 5000) {
        _attempts++;
        try {
          t.removeCue(t.cues[0]);
        } catch {
          // Skip this cue; try removing from the end instead so we
          // don't get stuck on cue[0] forever.
          try {
            t.removeCue(t.cues[t.cues.length - 1]);
          } catch {
            break;
          }
        }
      }
    }
    if (!Array.isArray(transcript)) return;
    const Cue = window.VTTCue || window.TextTrackCue;
    if (!Cue) return;
    for (const seg of transcript) {
      const words = (Array.isArray(seg.words) && seg.words.length)
        ? seg.words
        : (Array.isArray(seg.w) && seg.w.length ? seg.w : null);
      if (words) {
        for (const w of words) {
          const s = Number(w.s);
          const e = Number(w.e);
          const text = (w.w || "").trim();
          if (!text) continue;
          if (!Number.isFinite(s) || !Number.isFinite(e)) continue;
          if (e <= s) continue;
          try { t.addCue(new Cue(s, e, text)); } catch {}
        }
      } else {
        // Coarse fallback for transcripts without per-word timing
        // (e.g. unpunctuated YT captions where each "segment" is the
        // whole displayed line). One cue per segment is the right
        // granularity here — the karaoke loop also degrades to
        // segment-level highlighting in this case.
        const s = Number(seg.s);
        const e = Number(seg.e);
        const text = (seg.t || seg.text || "").trim();
        if (!text || !Number.isFinite(s) || !Number.isFinite(e) || e <= s) continue;
        try { t.addCue(new Cue(s, e, text)); } catch {}
      }
    }
  }

  // Read the cached prefs (set by app.js when the user changes the
  // toolbar selects, and on boot from settings_load) and apply them
  // to a freshly-rendered <video>. Called from renderWatchView so the
  // mode/size/bg survive video changes.
  function _applyCaptionPrefs(vEl) {
    if (!vEl) return;
    const p = window._captionPrefs || {};
    const size = p.size || "off";
    const bg = p.bg || "translucent";
    vEl.dataset.capSize = (size === "off") ? "" : size;
    vEl.dataset.capBg = bg;
    const t = vEl._capTrack;
    if (t) t.mode = (size === "off") ? "hidden" : "showing";
  }

  // Public setter — app.js calls this when the user changes a toolbar
  // select. We update the cache + apply to the currently-loaded video.
  window.setCaptionPref = function (key, value) {
    window._captionPrefs = window._captionPrefs || {};
    if (key === "size" || key === "bg") {
      window._captionPrefs[key] = value;
    }
    const vEl = document.getElementById("watch-video");
    if (vEl) _applyCaptionPrefs(vEl);
  };
})();
