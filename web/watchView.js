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

  const displayText = window.YT?.util?.displayText || ((s) => String(s ?? ""));

  function bridgeCall(method, ...args) {
    const fn = window.YT?.bridge?.bridgeCall;
    if (fn) return fn(method, ...args);
    return undefined;
  }

  function nativeBridgeUp() {
    return !!window.YT?.bridge?.isUp?.();
  }

  function _watchVideoIdentity(video) {
    if (!video) return "";
    if (video.video_id) return `id:${video.video_id}`;
    if (video.filepath) {
      return `file:${String(video.filepath).replace(/\\/g, "/").toLowerCase()}`;
    }
    return `fallback:${video.channel || ""}\u0000${video.title || ""}`;
  }

  /** Retranscribe completion hook — called by Python via evaluate_js
   * when a `transcribe_retranscribe` job finishes. If the completed
   * video is the one currently on screen, refetch the transcript and
   * re-render the Watch view (this flips the source banner from
   * "auto-captions — approximate" to "Whisper transcription"). Mirrors
   * the companion viewer's post-finish progress refresh. */
  window._onRetranscribeComplete = async function (payload) {
    let refreshToken = null;
    let refreshKey = "";
    const existingTranscriptKept = payload?.existing_transcript_kept === true;
    const keptTranscriptMessage =
      "Whisper found no speech — the existing transcript was kept.";
    try {
      const { video_id, filepath } = payload || {};
      // Clear the in-flight entry for THIS video so other watch-view
      // navigations stop showing its progress. Pass the video_id so
      // only the finished job is removed — multiple retranscribes can
      // be queued concurrently across different videos, and clearing
      // them all on one completion would mis-paint sibling buttons.
      try { window._retranscribeWatchClear?.(video_id || ""); } catch {}
      const wv = document.getElementById("view-watch");
      if (!wv || wv.hidden) return;
      const cur = window._watchCurrentVideo || null;
      if (!cur) return;
      refreshToken = (typeof window._watchOpenToken === "number")
        ? window._watchOpenToken : null;
      refreshKey = _watchVideoIdentity(cur);
      const stillShowingRefreshTarget = () => {
        const active = window._watchCurrentVideo || null;
        const tokenMatches = refreshToken === null
          || window._watchOpenToken === refreshToken;
        return tokenMatches
          && _watchVideoIdentity(active) === refreshKey
          && (!window._browseState || window._browseState.view === "watch");
      };
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
      if (!nativeBridgeUp()) return;
      const res = await bridgeCall("browse_get_transcript", {
        video_id: cur.video_id || undefined,
        title: cur.title || "",
        channel: cur.channel || "",
        filepath: cur.filepath || "",
      });
      // The user may have opened B while A's refreshed transcript was
      // loading. Never repaint A over B or announce A's completion as if it
      // applied to the video now on screen.
      if (!stillShowingRefreshTarget()) return;
      let segments = [];
      let sourceInfo = null;
      if (Array.isArray(res)) segments = res;
      else if (res && res.segments) {
        segments = res.segments;
        sourceInfo = res.source || null;
      }
      // A re-transcription can legitimately find no speech even though the
      // video already has a usable transcript (for example, saved captions).
      // The backend deliberately preserves that transcript. Refresh it when
      // available, but never replace it with an empty state or claim that
      // Whisper updated it.
      if (existingTranscriptKept) {
        if (segments.length) {
          const transcript = segments.map(seg => ({
            ts: (window._formatTs ? window._formatTs(seg.s) : ""),
            text: seg.t, words: seg.w, s: seg.s, e: seg.e,
          }));
          window.renderWatchView(cur, transcript, sourceInfo,
                                 { skipVideoReload: true });
        }
        window._showToast?.(keptTranscriptMessage, "ok");
        return;
      }
      // when the retranscribe completes but the segment
      // fetch comes back empty (JSONL write failed, FTS ingest failed,
      // whisper produced nothing), the old transcript used to stay on
      // screen alongside a "complete" toast — user assumed the
      // retranscribe worked fine. Now explicitly replace with a
      // placeholder and show a warn toast so the failure is visible.
      if (!segments.length) {
        // Carry tx_status so renderWatchView shows the right empty message.
        if (res && !Array.isArray(res) && res.tx_status) {
          cur.tx_status = res.tx_status;
        }
        window.renderWatchView(cur, [], sourceInfo,
                               { skipVideoReload: true });
        if (cur.tx_status === "no_speech") {
          // Benign: Whisper ran and the video genuinely has no speech.
          window._showToast?.("No speech detected — nothing to transcribe.", "ok");
        } else {
          window._showToast?.(
            "Re-transcription finished but produced no segments — check the log.",
            "warn");
        }
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
    } catch (e) {
      if (refreshToken !== null
          && (window._watchOpenToken !== refreshToken
              || _watchVideoIdentity(window._watchCurrentVideo) !== refreshKey
              || (window._browseState && window._browseState.view !== "watch"))) {
        return;
      }
      if (existingTranscriptKept) {
        // The durable transcript was preserved even if this one UI refresh
        // failed. Keep the current transcript on screen and report the actual
        // Whisper outcome instead of implying new text was saved.
        window._showToast?.(keptTranscriptMessage, "ok");
        return;
      }
      console.warn("_onRetranscribeComplete failed:", e);
      window._showToast?.(
        "Re-transcription finished but the view couldn't refresh; reopen the video.",
        "warn");
    }
  };

  /** Render the empty / no-speech transcript state into the panel.
   * Handles both "not yet transcribed" and "Whisper found no speech". */
  function _renderEmptyTranscript(tr, video, vEl) {
    const _noSpeech = !!(video && video.tx_status === "no_speech");
    tr.innerHTML = _noSpeech
      ? '<div class="watch-transcript-note">No speech detected — this video has no spoken audio to transcribe.</div>'
      : '<div class="watch-transcript-note">No transcript available.</div>';
    _unbindKaraoke();
    // Hide any stale overlay phrase that lingered from the previous video.
    try { vEl._capOverlay && vEl._capOverlay.classList.remove("show"); } catch { /* ignore */ }
  }

  // seconds -> "m:ss" / "h:mm:ss" for the paragraph gutter timestamps.
  function _fmtParaTs(sec) {
    sec = Math.max(0, Math.floor(Number(sec) || 0));
    const h = Math.floor(sec / 3600);
    const m = Math.floor((sec % 3600) / 60);
    const s = sec % 60;
    const mm = h ? String(m).padStart(2, "0") : String(m);
    return (h ? h + ":" : "") + mm + ":" + String(s).padStart(2, "0");
  }

  // Non-speech cue detection — bracketed "[Music]" / "[Applause]",
  // parenthesized "(laughs)", or musical-note runs "♪♪♪". Tagged so the
  // "Hide ♪" toggle can collapse them out of the reading flow.
  function _isNonSpeechToken(tok) {
    const t = (tok || "").trim();
    if (!t) return false;
    if (/^[[(][^\])]*[\])]$/.test(t)) return true;
    if (/^[♩♪♫♬♭♮♯]+$/.test(t)) return true;
    return false;
  }

  // Paragraph grouping thresholds. Captions often arrive in huge source
  // segments, so paragraphing works at sentence boundaries inside those
  // segments instead of only between segments.
  const _PARA_GAP = 2.0;
  const _PARA_TARGET_SPAN = 45;
  const _PARA_TARGET_SENTENCES = 4;
  const _PARA_TARGET_CHARS = 700;
  const _PARA_MIN_WORDS_FOR_PAUSE = 20;

  function _cleanTranscriptText(text) {
    return String(text || "").replace(/\s+/g, " ").trim();
  }

  function _isSentenceBoundaryText(text) {
    const t = _cleanTranscriptText(text);
    if (!t) return false;
    if (_isNonSpeechToken(t)) return true;
    return /[.!?…]["')\]]*$/.test(t);
  }

  function _paintRetranscribeControl(ctrl) {
    if (!ctrl) return;
    const vid = ctrl.dataset.videoId || "";
    const canStart = ctrl.dataset.canStart === "1";
    const inflight = window._inflightRetranscribes;
    const busy = !!(vid && inflight && inflight.has && inflight.has(vid));
    const rawState = busy ? inflight.get(vid) : 0;
    const state = busy && window._normalizeRetranscribeWatchState
      ? window._normalizeRetranscribeWatchState(rawState)
      : {
          pct: Math.max(0, Math.min(99, parseInt(rawState, 10) || 0)),
          phase: (parseInt(rawState, 10) || 0) > 0
            ? "transcribing" : "queued",
        };
    const pct = state.pct;
    const finalizing = state.phase === "finalizing";
    const paused = state.phase === "paused";
    const resuming = state.phase === "resuming";
    const needsAttention = state.phase === "needs_attention";
    const queued = state.phase === "queued";
    const link = ctrl.querySelector(".watch-retranscribe-link");
    const progress = ctrl.querySelector(".watch-retranscribe-progress");
    const fill = ctrl.querySelector(".watch-retranscribe-fill");
    const text = ctrl.querySelector(".watch-retranscribe-progress-text");
    const banner = ctrl.closest(".watch-src-banner");

    ctrl.classList.toggle("is-busy", busy);
    if (banner) banner.classList.toggle("is-progress-pinned", busy);
    ctrl.hidden = (!busy && !canStart);
    if (link) link.hidden = busy || !canStart;
    if (progress) {
      progress.hidden = !busy;
      progress.classList.toggle("is-needs-attention", needsAttention);
      progress.classList.toggle("is-paused", paused);
    }
    if (!busy) return;

    if (text) {
      if (finalizing) {
        const display = window._retranscribeWatchFinalizingDisplay
          ? window._retranscribeWatchFinalizingDisplay(state)
          : { text: "Finishing transcript…", title: "Finishing transcript" };
        text.textContent = display.text;
        text.title = display.title;
      } else if (needsAttention) {
        text.textContent = "Needs attention — retry in Processing";
        text.title = "Open Processing to retry this re-transcription.";
      } else if (paused) {
        text.textContent = "Paused — resume in Processing";
        text.title = "Open Processing when you are ready to resume.";
      } else if (resuming) {
        text.textContent = "Resuming transcription…";
        text.title = "Re-transcription is resuming.";
      } else if (queued) {
        text.textContent = "Queued";
        text.title = "Re-transcription is waiting in the Processing queue.";
      } else {
        text.textContent = pct > 0 ? `Whisper ${pct}%` : "Queued";
        text.title = pct > 0
          ? `Re-transcribing with Whisper, ${pct}%`
          : "Re-transcribe queued";
      }
    }
    if (fill) {
      const stopped = paused || needsAttention;
      const indeterminate = !stopped
        && (finalizing || resuming || queued || pct <= 0);
      fill.style.width = indeterminate
        ? "36%" : pct > 0 ? `${pct}%` : "0%";
      fill.classList.toggle("is-indeterminate", indeterminate);
      fill.classList.toggle("is-needs-attention", needsAttention);
    }
  }

  function _paintRetranscribeControls() {
    document.querySelectorAll(".watch-retranscribe-control")
      .forEach(_paintRetranscribeControl);
  }
  window._syncWatchRetranscribeBanner = _paintRetranscribeControls;

  /** Build the scrollable transcript body from segment data.
   * Returns { body, segEls, allWordEls, wordIndex }.
   *
   * Words are grouped into sentence-safe chunks, then into paragraphs by
   * silence gaps / size targets. Each paragraph gets a clickable gutter timestamp — turning
   * the old undifferentiated wall into a readable, seekable article. The
   * `.seg` / `.word` spans stay in document order inside `body`, so
   * karaoke (which reads segEls + querySelectorAll(".word")) is unaffected.
   *
   * Token-count reconciliation: when the punctuated-text token count
   * matches the raw word-timestamp count, punctuated tokens drive the
   * display while per-word timestamps stay intact for karaoke.
   * On mismatch (cap-flush rollover, manual edits) falls back to raw words. */
  function _buildTranscriptBody(transcript, vEl) {
    const body = document.createElement("div");
    body.className = "watch-transcript-body";
    const segEls = [];

    let curParaText = null;    // .para-text span currently being filled
    let paraStartSec = 0;      // start time of the current paragraph
    let prevEnd = null;        // end time of the previous timed chunk
    let prevEndReliable = false; // did the previous chunk carry a real end?
    let prevBoundarySafe = false;
    let paraChars = 0;
    let paraWords = 0;
    let paraSentences = 0;

    const startParagraph = (startSec) => {
      const para = document.createElement("div");
      para.className = "transcript-para";
      const ts = document.createElement("button");
      ts.type = "button";
      ts.className = "para-ts";
      ts.dataset.s = String(startSec);
      ts.textContent = _fmtParaTs(startSec);
      ts.title = "Jump to " + _fmtParaTs(startSec);
      ts.tabIndex = 0;
      ts.setAttribute("aria-haspopup", "menu");
      ts.setAttribute("aria-expanded", "false");
      ts.setAttribute("aria-keyshortcuts", "Shift+F10");
      const text = document.createElement("span");
      text.className = "para-text";
      para.appendChild(ts);
      para.appendChild(text);
      body.appendChild(para);
      curParaText = text;
      paraStartSec = startSec;
      paraChars = 0;
      paraWords = 0;
      paraSentences = 0;
    };

    for (const seg of transcript) {
      const segStart = Number(seg.s) || 0;
      const segEnd = Number(seg.e);
      const segEndReliable = Number.isFinite(segEnd) && segEnd > segStart;
      const words = Array.isArray(seg.words) && seg.words.length
        ? seg.words
        : (seg.w && seg.w.length ? seg.w : null);
      const segText = seg.t || seg.text || "";
      const textTokens = segText.trim().split(/\s+/).filter(Boolean);

      const wordItems = [];
      const addItem = (text, s, e) => {
        if (!text) return;
        const item = { text: String(text), s: Number(s), e: Number(e) };
        if (!Number.isFinite(item.s)) item.s = segStart;
        if (!Number.isFinite(item.e)) delete item.e;
        wordItems.push(item);
      };
      if (words && textTokens.length === words.length) {
        for (let i = 0; i < words.length; i++) {
          addItem(textTokens[i], words[i].s ?? segStart, words[i].e);
        }
      } else if (words) {
        for (const wobj of words) addItem(wobj.w || "", wobj.s ?? segStart, wobj.e);
      } else if (textTokens.length > 1) {
        // Last-resort fallback for text-only transcripts. If the segment has
        // an end time, spread words across it so paragraph timestamps remain
        // useful; otherwise every word clicks back to the segment start.
        const span = segEndReliable ? (segEnd - segStart) : 0;
        for (let i = 0; i < textTokens.length; i++) {
          const s = span ? segStart + (span * i / textTokens.length) : segStart;
          const e = span ? segStart + (span * (i + 1) / textTokens.length) : undefined;
          addItem(textTokens[i], s, e);
        }
      } else if (segText) {
        addItem(segText, seg.s ?? 0, seg.e);
      }
      if (!wordItems.length) continue;

      const renderChunk = (items, safeBoundary) => {
        if (!items.length) return;
        const first = items[0];
        const last = items[items.length - 1];
        const chunkStart = Number.isFinite(first.s) ? first.s : segStart;
        let chunkEnd = Number.isFinite(last.e) ? last.e : null;
        if (chunkEnd == null || chunkEnd <= chunkStart) {
          chunkEnd = Number.isFinite(last.s) && last.s > chunkStart
            ? last.s
            : (segEndReliable ? segEnd : chunkStart);
        }

        // Gap-based breaks only when the previous chunk had a trustworthy
        // end time. Some transcripts carry starts only; using the start as a
        // pseudo-end would shred text into tiny paragraphs.
        const gap = (prevEnd == null) ? Infinity
                  : (prevEndReliable ? (chunkStart - prevEnd) : 0);
        const paraAge = (prevEnd == null) ? 0 : Math.max(0, prevEnd - paraStartSec);
        const pauseBreak = prevBoundarySafe
          && gap >= _PARA_GAP
          && paraWords >= _PARA_MIN_WORDS_FOR_PAUSE;
        const sizeBreak = prevBoundarySafe
          && (paraAge >= _PARA_TARGET_SPAN
              || paraSentences >= _PARA_TARGET_SENTENCES
              || paraChars >= _PARA_TARGET_CHARS);
        if (!curParaText || pauseBreak || sizeBreak) {
          startParagraph(chunkStart);
        }

        const segEl = document.createElement("span");
        segEl.className = "seg";
        segEl.dataset.s = chunkStart;
        segEl.dataset.e = chunkEnd;
        let nonSpeech = 0, total = 0;
        const chunkTextParts = [];
        const addWord = (text, s, e) => {
          const span = document.createElement("span");
          span.className = "word";
          span.dataset.s = (s ?? 0);
          if (e != null) span.dataset.e = e;
          span.textContent = text + " ";
          if (_isNonSpeechToken(text)) { span.classList.add("tx-nonspeech"); nonSpeech++; }
          total++;
          segEl.appendChild(span);
        };
        for (const item of items) {
          chunkTextParts.push(item.text);
          addWord(item.text, item.s ?? chunkStart, item.e);
        }
        // A chunk that is ENTIRELY non-speech (a standalone "[Music]"
        // line) gets tagged too, so the toggle collapses the whole line.
        if (total > 0 && nonSpeech === total) segEl.classList.add("tx-nonspeech");
        // Trailing space between chunks keeps words from running together.
        segEl.appendChild(document.createTextNode(" "));
        curParaText.appendChild(segEl);
        segEls.push(segEl);

        const chunkText = chunkTextParts.join(" ");
        paraChars += chunkText.length + 1;
        paraWords += total;
        if (safeBoundary && !_isNonSpeechToken(chunkText)) paraSentences += 1;

        prevEndReliable = Number.isFinite(chunkEnd) && chunkEnd > chunkStart;
        prevEnd = prevEndReliable ? chunkEnd : chunkStart;
        prevBoundarySafe = !!safeBoundary;
      };

      let chunk = [];
      for (let i = 0; i < wordItems.length; i++) {
        const item = wordItems[i];
        chunk.push(item);
        const safeBoundary = _isSentenceBoundaryText(item.text);
        if (safeBoundary || i === wordItems.length - 1) {
          renderChunk(chunk, safeBoundary);
          chunk = [];
        }
      }
    }

    // Delegated click listener — one closure on the body instead of N
    // per-element closures. A paragraph timestamp seeks to the paragraph
    // start; otherwise the nearest word seeks via its dataset.s.
    body.addEventListener("click", (e) => {
      const tsEl = e.target && e.target.closest && e.target.closest(".para-ts");
      if (tsEl) {
        const ps = parseFloat(tsEl.dataset.s);
        if (Number.isFinite(ps)) _seekTo(vEl, ps);
        return;
      }
      const wEl = e.target && e.target.closest && e.target.closest(".word");
      if (!wEl) return;
      const _s = parseFloat(wEl.dataset.s);
      if (Number.isFinite(_s)) _seekTo(vEl, _s);
    });
    const allWordEls = Array.from(body.querySelectorAll(".word"));
    const wordIndex = new Map();
    for (let i = 0; i < allWordEls.length; i++) wordIndex.set(allWordEls[i], i);
    return { body, segEls, allWordEls, wordIndex };
  }

  /** Render the Watch view: loads the real video file into <video> and
   * builds per-word transcript spans with (s, e) timestamps for karaoke.
   *
   * transcript items: { s, e, t, w:[{w,s,e},...], ts } — s/e in seconds.
   * source: { source: "whisper"|"yt_captions_punct"|"yt_captions_raw"|"unknown", raw: "..." }
   * `video.filepath` is used to request a file:// URL from the backend.
   *
   * Render mode: single continuous flowing body (no per-segment divs,
   * no [timestamp] inline prefixes) — matches the companion viewer. A source
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

    // Ensure the pinned-overlay stage + overlay element exist around the
    // <video> before we (re)load the source or bind the karaoke loop.
    _ensureCapOverlay(vEl);

    // `opts.skipVideoReload`: set by _onRetranscribeComplete so the
    // <video> element isn't re-sourced (which would restart playback
    // from 0). We only need to refresh the transcript + source banner
    // when the retranscribe for the currently-playing video finishes.
    const skipVideoReload = !!(opts && opts.skipVideoReload);

    title.textContent = displayText(video.title || "Video Title");
    const parts = [];
    if (video.channel) parts.push(displayText(video.channel));
    if (video.uploaded) parts.push(video.uploaded);
    if (video.duration) parts.push(video.duration);
    if (video.views) parts.push(video.views + " views");
    meta.textContent = parts.join(" · ");

    // Stash for `_onRetranscribeComplete` — when Python finishes a
    // retranscribe, the handler checks this ref to decide whether the
    // completed job matches what's on screen.
    window._watchCurrentVideo = video;
    window._watchRenderedToken = window._watchOpenToken;
    const bookmarkButton = document.getElementById("btn-bookmark-now");
    if (bookmarkButton) {
      const canBookmark = !!video.video_id && !opts?.transcriptLoading;
      bookmarkButton.disabled = !canBookmark;
      bookmarkButton.title = opts?.transcriptLoading
        ? "Bookmark is available when the transcript check finishes"
        : canBookmark
        ? "Bookmark current segment (B)"
        : "Bookmark unavailable: this file has no YouTube ID";
    }

    // Repaint the Re-transcribe button to reflect whether THIS video
    // has an in-flight retranscribe. Without this, navigating from
    // Video A (mid-retranscribe) to Video B left B's button locked in
    // A's "Re-transcribing… X%" state, so the user couldn't queue B.
    try { window._syncWatchRetranscribeButton?.(); } catch {}

    if (!skipVideoReload) {
      _loadVideoSource(video, vEl, ph, opts?.playbackIntent);
      _loadWatchMetadataDrawer(video);
    }

    tr.innerHTML = "";
    if (opts?.transcriptLoading || opts?.transcriptError) {
      _unbindKaraoke();
      vEl?._capOverlay?.classList.remove("show");
      const note = document.createElement("div");
      note.className = "watch-transcript-note";
      note.setAttribute("role", "status");
      note.textContent = opts.transcriptLoading
        ? "Loading transcript… You can play the video while it loads."
        : `Couldn't load transcript: ${opts.transcriptError}`;
      tr.appendChild(note);
      return;
    }
    if (!transcript || transcript.length === 0) {
      _renderEmptyTranscript(tr, video, vEl);
      return;
    }

    const frag = document.createDocumentFragment();
    // Source banner — Whisper / YT auto-captions / unknown. Pass the
    // actual transcript so the banner can verify the "(punctuation
    // restored)" claim against real per-segment content.
    const bannerEl = _buildSourceBanner(sourceInfo, video, transcript);
    if (bannerEl) frag.appendChild(bannerEl);

    const { body, segEls, allWordEls, wordIndex } =
      _buildTranscriptBody(transcript, vEl);
    frag.appendChild(body);
    tr.appendChild(frag);

    // Apply the selected font before measuring the rolling overlay's lines.
    _applyCaptionPrefs(vEl);
    _bindKaraoke(vEl, tr, segEls, allWordEls, wordIndex);
  };

  /** Build the source-banner div for the Watch view transcript panel.
   * Returns a DOM element or null. Mirrors the companion viewer's source
   * banner — same text,
   * same four cases, same link wording. The "unknown, no raw" case
   * returns null so no banner appears at all (the companion returns
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

    const buildRetranscribeControl = (canStart) => {
      const wrap = document.createElement("span");
      wrap.className = "watch-retranscribe-control";
      wrap.dataset.videoId = (video && video.video_id) || "";
      wrap.dataset.canStart = canStart ? "1" : "0";
      const a = document.createElement("a");
      a.href = "#";
      a.className = "watch-retranscribe-link";
      a.setAttribute("role", "button");
      a.title = "Re-transcribe with Whisper for more accurate results";
      a.textContent = "Re-transcribe with Whisper";
      a.addEventListener("click", (ev) => {
        ev.preventDefault();
        const btn = document.getElementById("btn-watch-retranscribe");
        if (btn) btn.click();
      });
      const prog = document.createElement("span");
      prog.className = "watch-retranscribe-progress";
      prog.hidden = true;
      prog.setAttribute("role", "status");
      prog.setAttribute("aria-live", "polite");
      const progText = document.createElement("span");
      progText.className = "watch-retranscribe-progress-text";
      const bar = document.createElement("span");
      bar.className = "watch-retranscribe-bar";
      bar.setAttribute("aria-hidden", "true");
      const fill = document.createElement("span");
      fill.className = "watch-retranscribe-fill";
      bar.appendChild(fill);
      prog.appendChild(progText);
      prog.appendChild(bar);
      wrap.appendChild(a);
      wrap.appendChild(prog);
      _paintRetranscribeControl(wrap);
      return wrap;
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
      banner.appendChild(buildRetranscribeControl(false));
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
      // pass, so a raw-caption fixture from a later refresh can have
      // fully punctuated text while a yt_captions_punct legacy video
      // has lowercase per-segment content. Basing the badge on visible
      // content reflects what the user sees instead of pipeline state.
      const label = _hasContentPunct
        ? "YouTube auto-captions (punctuated) — transcript is approximate. "
        : "YouTube auto-captions — transcript is approximate. ";
      banner.appendChild(document.createTextNode(label));
      banner.appendChild(buildRetranscribeControl(true));
      return banner;
    }
    // Unknown source — do not flag it with a warning
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
  let _watchMetadataSeq = 0;
  window.loadWatchMetadataDrawer = (video) => _loadWatchMetadataDrawer(video);
  async function _loadWatchMetadataDrawer(video) {
    const drawer = document.getElementById("watch-meta-drawer");
    if (!drawer) return;
    const statsEl = document.getElementById("watch-meta-stats");
    const descEl = document.getElementById("watch-meta-description");
    const commentsEl = document.getElementById("watch-meta-comments");
    const countEl = document.getElementById("watch-meta-comments-count");
    const requestSeq = ++_watchMetadataSeq;
    const requestToken = (typeof window._watchOpenToken === "number")
      ? window._watchOpenToken : null;
    const requestKey = _watchVideoIdentity(video);
    const requestIsCurrent = () => {
      const tokenMatches = requestToken === null
        || window._watchOpenToken === requestToken;
      return requestSeq === _watchMetadataSeq
        && tokenMatches
        && _watchVideoIdentity(window._watchCurrentVideo) === requestKey;
    };
    // Reset state immediately so a slow fetch doesn't bleed previous video's data
    if (statsEl) statsEl.textContent = "";
    if (descEl) descEl.textContent = "Loading…";
    if (commentsEl) commentsEl.innerHTML = "";
    if (countEl) countEl.textContent = "";

    if (!nativeBridgeUp()) {
      if (descEl) descEl.textContent = "Video details aren't available because YTArchiver isn't ready yet.";
      return;
    }
    let res;
    try {
      res = await bridgeCall("browse_get_video_metadata", {
        filepath: video.filepath || "",
        video_id: video.video_id || "",
        title: video.title || "",
        channel: video.channel || "",
      });
    } catch (e) {
      if (!requestIsCurrent()) return;
      if (descEl) descEl.textContent = "(Failed to load metadata.)";
      return;
    }
    // Metadata A can finish after the user opens B, or after a manual
    // refresh for the same video starts. Only the newest matching request
    // may touch the shared drawer.
    if (!requestIsCurrent()) return;
    if (!res?.ok || !res.meta) {
      if (descEl) descEl.textContent =
        "No metadata on disk for this video yet. Run 'Download metadata' on the channel to fetch it.";
      return;
    }
    const meta = res.meta;
    // Fill the same header fields even when the entry point supplied only
    // a Search hit. Reuse the existing saved-details request and identity
    // guard; enrichment never changes the selected file or playback time.
    if (!video.duration && Number(meta.duration) > 0) {
      video.duration = window._formatTs?.(Number(meta.duration)) || "";
    }
    if (!video.views && Number.isFinite(Number(meta.view_count)) && meta.view_count != null) {
      video.views = Number(meta.view_count).toLocaleString(undefined,
        { notation: "compact", maximumFractionDigits: 1 });
    }
    if (!video.uploaded && /^\d{8}$/.test(String(meta.upload_date || ""))) {
      const date = String(meta.upload_date);
      const timestamp = Date.UTC(Number(date.slice(0, 4)), Number(date.slice(4, 6)) - 1,
        Number(date.slice(6, 8))) / 1000;
      video.uploaded = window._formatBrowseUploadAge?.(timestamp) || "";
    }
    const header = document.getElementById("watch-meta");
    if (header) header.textContent = [displayText(video.channel || ""), video.uploaded,
      video.duration, video.views ? `${video.views} views` : ""].filter(Boolean).join(" · ");
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
        const secs = Math.max(0, h * 3600 + mn * 60 + s);
        const span = document.createElement("span");
        span.className = "desc-ts";
        span.dataset.t = String(secs);
        span.textContent = m[0];
        span.addEventListener("click", (e) => {
          e.preventDefault();
          const vEl = document.getElementById("watch-video");
          if (!vEl) return;
          _seekTo(vEl, secs);
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

  async function _loadVideoSource(video, vEl, ph, requestedIntent) {
    if (!vEl) return;
    const playbackIntent = requestedIntent ?? _playbackIntent;
    const fp = video.filepath || "";
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
        '<div class="watch-play-icon watch-play-icon-hidden">▶</div>' +
        '<div class="watch-placeholder-text">'
        + '<span class="spinner-inline"></span>Loading…</div>';
      // Hide the <video> element so its previous src doesn't flash.
      vEl.hidden = true;
    }
    let url = null;
    let errorDetail = "";
    if (fp && nativeBridgeUp()) {
      try {
        const r = await bridgeCall("browse_video_url", fp);
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
      // A stale request has no ownership of the shared player's current source.
      return;
    }
    if (url) {
      // Stop the previous video's load explicitly before swapping src.
      // Without this, rapid back-to-back video clicks can leave the
      // old request holding the file handle on the archive drive until
      // the local HTTP server times out (~30s) — at peak this
      // transiently leaks N file handles. Pause+removeAttribute+load
      // is the documented clean-teardown pattern.
      try {
        vEl.pause();
        vEl.removeAttribute("src");
        if (typeof vEl.load === "function") vEl.load();
      } catch { /* noop */ }
      // Re-check token + view IMMEDIATELY before assigning src. The
      // earlier check at the start of this branch can pass, then the
      // teardown above yields the microtask, and a Back click in that
      // gap can run _stopWatchVideo before this assignment lands —
      // restarting playback in a hidden Watch view (audit:
      // watchView.js C25).
      const _stillOnSameVideo2 = (
        typeof window._watchOpenToken !== "number"
        || window._watchOpenToken === _entryToken);
      const _stillOnWatchView2 = (
        !window._browseState
        || window._browseState.view === "watch");
      if (!_stillOnSameVideo2 || !_stillOnWatchView2) {
        return;
      }
      // Surface decode/playback failures. Without this, a corrupt or
      // partially-downloaded file — or a codec the WebView can't decode —
      // can fail SILENTLY without the handler below, leaving the user
      // staring at a black box with no explanation. Wire once per <video> element.
      if (!vEl.dataset.errHooked) {
        vEl.dataset.errHooked = "1";
        vEl.addEventListener("error", () => {
          const err = vEl.error;
          // Ignore aborts (fired during our own teardown / src swaps) and
          // the empty-src state — only surface real decode/format failures.
          if (!err || err.code === 1 /* MEDIA_ERR_ABORTED */) return;
          if (!vEl.getAttribute("src")) return;
          const ph2 = document.getElementById("watch-video-placeholder");
          if (!ph2) return;
          const _esc = window._escapeHtml || (s => String(s ?? ""));
          const codeMsg = ({
            2: "a network error interrupted the video",
            3: "the video file appears to be corrupt or incomplete",
            4: "the file's format/codec isn't supported by the player",
          })[err.code] || "the video couldn't be played";
          vEl.hidden = true;
          ph2.style.display = "";
          const label2 = ph2.querySelector(".placeholder-label") || ph2;
          label2.innerHTML =
            `<div class="watch-video-error-title">⚠ Couldn't play this ` +
            `video — ${_esc(codeMsg)}.</div>` +
            `<div class="watch-video-error-detail watch-video-error-hint">` +
            `Try Redownload to refetch the file, or Rescan archive.</div>`;
        });
      }
      vEl.src = url;
      vEl.dataset.watchToken = String(_entryToken);
      vEl.hidden = false;
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
      // Opening a video from the grid should start playback immediately.
      // The race-token checks above prevent stale loads from starting in
      // the background after the user navigates away.
      if (playbackIntent === _playbackIntent && _watchIsVisible()) {
        vEl.play().catch(() => { /* user can click play if autoplay is blocked */ });
      }
    } else {
      // No playback possible — show placeholder with actionable error.
      // reported clicking a "(1)" duplicate that had a stale DB
      // entry (file missing from disk) — the old placeholder just said
      // "Select a video to play" which was misleading when a video IS
      // selected but the file is gone. Now it explains.
      vEl.src = "";
      vEl.hidden = true;
      if (ph) {
        ph.style.display = "";
        if (errorDetail && errorDetail !== "unknown error") {
          const label = ph.querySelector(".placeholder-label") || ph;
          // Escape both values before innerHTML. `fp`'s leaf is derived from
          // the YouTube title at download time and `errorDetail` is a backend
          // string — neither is trusted markup. (The comment used to promise
          // escaping that wasn't actually applied.)
          const _esc = window._escapeHtml || (s => String(s ?? ""));
          const leaf = fp ? _esc(fp.split(/[\\/]/).pop()) : "";
          label.innerHTML =
            `<div class="watch-video-error-title">` +
            `⚠ ${_esc(errorDetail)}</div>` +
            (leaf
              ? `<div class="watch-video-error-detail">${leaf}</div>`
              : "") +
            `<div class="watch-video-error-detail watch-video-error-hint">` +
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

  let _playbackIntent = 0;
  Object.defineProperty(window, "_watchPlaybackIntent", {
    get: () => _playbackIntent,
    configurable: true,
  });
  let _pendingSeekCleanup = null;
  function _watchIsVisible() {
    return window.YT.util.isElementVisible(document.getElementById("view-watch"));
  }
  window._cancelWatchPlaybackIntent = () => {
    ++_playbackIntent;
    _pendingSeekCleanup?.();
    _pendingSeekCleanup = null;
  };
  function _seekTo(vEl, seconds) {
    if (!vEl) return;
    _pendingSeekCleanup?.();
    _pendingSeekCleanup = null;
    const token = window._watchOpenToken;
    const intent = _playbackIntent;
    const target = Math.max(0, Number(seconds) || 0);
    const doSeek = () => {
      if (token !== window._watchOpenToken || intent !== _playbackIntent
          || !_watchIsVisible() || vEl.dataset.watchToken !== String(token)
          || vEl.readyState < 1) return false;
      const d = Number.isFinite(vEl.duration) ? vEl.duration : target;
      vEl.currentTime = Math.min(target, d);
      vEl.play().catch(() => {});
      return true;
    };
    try {
      if (!doSeek()) {
        const onMeta = () => {
          vEl.removeEventListener("loadedmetadata", onMeta);
          if (_pendingSeekCleanup === cleanup) _pendingSeekCleanup = null;
          try { doSeek(); } catch {}
        };
        const cleanup = () => vEl.removeEventListener("loadedmetadata", onMeta);
        _pendingSeekCleanup = cleanup;
        vEl.addEventListener("loadedmetadata", onMeta);
      }
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

  let _karaokeRafId = null;
  let _karaokeGen = 0;
  function _unbindKaraoke() {
    _karaokeGen += 1;
    const video = document.getElementById("watch-video");
    if (video) {
      video._capCleanup?.();
      video._capCleanup = null;
      video._capForceRefresh = null;
      video._capRollingCache = null;
      video._capOverlay?.classList.remove("show");
    }
    if (_karaokeRafId !== null) {
      try { cancelAnimationFrame(_karaokeRafId); } catch {}
      _karaokeRafId = null;
    }
  }

  function _bindKaraoke(vEl, trWrap, segEls, allWordEls, wordIndex) {
    _unbindKaraoke();
    if (!vEl || !segEls.length) return;
    const myGen = ++_karaokeGen;
    let lastSegIdx = -1;
    let lastWordEl = null;
    vEl._capRollingCache = null;
    vEl._capLayoutDirty = true;
    const _refresh = (force = false) => {
      if (myGen !== _karaokeGen) return;
      const layoutChanged = vEl._capLayoutDirty;
      if (layoutChanged) {
        _updateCaptionScale(vEl);
        vEl._capLayoutDirty = false;
        if (vEl._capRollingCache) vEl._capRollingCache.layoutDirty = true;
      }
      const t = vEl.currentTime;
      if (!Number.isFinite(t)) return;
      // Binary-search segments for the one containing t
      const idx = _findSegIdx(segEls, t);
      if (idx !== lastSegIdx) {
        if (lastSegIdx >= 0 && segEls[lastSegIdx]) {
          segEls[lastSegIdx].classList.remove("active");
        }
        if (idx >= 0 && segEls[idx]) {
          segEls[idx].classList.add("active");
          // Keep the transcript following playback. Use container-local
          // scrollTop rather than `scrollIntoView` —
          // scrollIntoView walks up the parent chain and also scrolls
          // the outer `.browse-view` container, which pushes the video
          // off-screen as the karaoke follows along.
          // Compute the element's offset relative to the scrollable
          // transcript pane and set scrollTop directly so no ancestor
          // containers move.
          if (trWrap) {
            window._scrollTranscriptTo(trWrap, segEls[idx]);
          }
        }
        lastSegIdx = idx;
      }
      // Active word within the active segment (null between words / in
      // gaps so the pinned overlay clears instead of freezing on a word).
      let newWordEl = null;
      if (idx >= 0 && segEls[idx]) {
        const words = segEls[idx].querySelectorAll(".word");
        for (const w of words) {
          const s = parseFloat(w.dataset.s || "0");
          const e = parseFloat(w.dataset.e || "0");
          if (s <= t && t <= e) { newWordEl = w; break; }
          if (s > t) break;
        }
      }
      const wordChanged = newWordEl !== lastWordEl;
      if (wordChanged) {
        if (lastWordEl) lastWordEl.classList.remove("active");
        if (newWordEl) newWordEl.classList.add("active");
        lastWordEl = newWordEl;
      }
      // Rolling captions have their own timing lookup, including brief gaps
      // across segment boundaries. Transcript highlighting remains exact.
      if (force || layoutChanged || wordChanged || (window._captionPrefs?.mode || "default") === "default") {
        _updateCapOverlay(vEl, newWordEl, allWordEls, wordIndex, force || layoutChanged);
      }
    };
    // Read currentTime again on every explicit refresh: the cached active
    // word can belong to the old position when seeking or changing a mode.
    vEl._capForceRefresh = () => {
      vEl._capLayoutDirty = true;
      _refresh(true);
    };
    const refresh = () => _refresh(true);
    const events = ["seeking", "seeked", "pause", "ended"];
    events.forEach(name => vEl.addEventListener(name, refresh));
    const progress = () => _refresh();
    vEl.addEventListener("timeupdate", progress);
    const resize = () => {
      if (myGen !== _karaokeGen) return;
      vEl._capLayoutDirty = true;
      _refresh();
    };
    vEl.addEventListener("loadedmetadata", resize);
    vEl.addEventListener("resize", resize);
    const observer = typeof ResizeObserver === "function" ? new ResizeObserver(resize) : null;
    observer?.observe(vEl);
    document.fonts?.ready.then(() => {
      if (myGen === _karaokeGen) vEl._capLayoutDirty = true;
    });
    vEl._capCleanup = () => {
      observer?.disconnect();
      events.forEach(name => vEl.removeEventListener(name, refresh));
      vEl.removeEventListener("timeupdate", progress);
      vEl.removeEventListener("loadedmetadata", resize);
      vEl.removeEventListener("resize", resize);
      vEl._capOverlay?.getAnimations({ subtree: true }).forEach(animation => animation.cancel());
    };
    const _tick = () => {
      _refresh();
      // Schedule the next frame as long as the video is still loaded.
      // The unbind path cancels the in-flight rAF so this never leaks.
      // Stop the rAF loop when the Watch view is hidden — it
      // otherwise runs at 60fps wastefully while user is on
      // another tab / view (audit: watchView.js H155).
      const _watchView = document.getElementById("view-watch");
      const _hidden = _watchView && _watchView.hidden;
      if (myGen === _karaokeGen && vEl.isConnected && !_hidden) {
        _karaokeRafId = requestAnimationFrame(_tick);
      }
    };
    _refresh(true);
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

  // ─── Pinned on-video overlay (replaces the native ::cue overlay) ───
  //
  // The overlay is a 3-column grid [1fr | auto | 1fr]: the CURRENT word
  // sits in the centre column and stays pinned to the video's horizontal
  // centre, while the previous / next words grow outward into the side
  // columns. The old native ::cue approach centred the whole
  // "prev current next" line, so the middle word drifted left/right
  // whenever the neighbours differed in width.
  //
  // The overlay + stage + fullscreen button are STATIC markup (in
  // partials/tab-browse.html), NOT built here. An earlier version created
  // the stage at runtime and reparented the live <video> into it, which
  // broke Chromium's native click-to-play/pause on the video body. This
  // helper now just grabs the static refs and wires the fullscreen button
  // once. Fullscreen targets the stage (not the bare <video>) so the
  // overlay rides along — a fullscreened <video> can't show sibling DOM.
  function _ensureCapOverlay(vEl) {
    if (!vEl) return null;
    if (vEl._capOverlay) return vEl._capOverlay;
    const stage = document.getElementById("watch-video-stage")
                  || vEl.closest(".watch-video-stage");
    vEl._capStage = stage;
    const ovl = document.getElementById("watch-cap-ovl");
    vEl._capOverlay = ovl;

    const fsBtn = document.getElementById("watch-fs-btn");
    if (fsBtn && stage && !fsBtn._wired) {
      fsBtn._wired = true;
      // CSS-based "fill the program window" — NOT the native Fullscreen API
      // and NOT OS fullscreen.
      //
      // The native API (stage.requestFullscreen) is unusable here: WebView2
      // promotes the playing <video> to a dedicated GPU surface and, once
      // the controls auto-hide on mouse-idle, composites ONLY that surface —
      // dropping the DOM caption overlay (and even native ::cue text) until
      // the mouse moves. Instead we just CSS-fill the stage to the program
      // window's content area (position:fixed; inset:0). The <video> stays an
      // ordinary composited DOM element (exactly like windowed mode), so the
      // caption overlay renders over it and survives the idle controls-hide —
      // and it keeps honoring the user's caption size, since it's the same
      // `.cap-ovl[data-cap-size]` element either way. The window itself is
      // left alone (no OS fullscreen / taskbar takeover — by design).
      const _escFs = (ev) => {
        if (ev.key === "Escape" && stage.classList.contains("cssfs")) {
          ev.preventDefault();
          ev.stopPropagation();
          _setFs(false);
        }
      };
      const _setFs = (on) => {
        if (on === stage.classList.contains("cssfs")) return;
        stage.classList.toggle("cssfs", on);
        document.documentElement.classList.toggle("watch-cssfs", on);
        if (on) document.addEventListener("keydown", _escFs, true);
        else document.removeEventListener("keydown", _escFs, true);
      };
      fsBtn.addEventListener("click", (e) => {
        // Don't let the click fall through to the video's native
        // click-to-play handler underneath.
        e.stopPropagation();
        _setFs(!stage.classList.contains("cssfs"));
      });
    }

    // Explicit click-to-play/pause on the video body. Chromium normally
    // gives <video controls> this for free, but in this WebView2 embed the
    // click-on-body toggle doesn't fire, so we wire it ourselves.
    //
    // Bulletproof against double-toggle: a debounce guard means that even
    // if the engine's own click-to-play DOES fire for the same physical
    // click, the two collapse into exactly one toggle. We deliberately do
    // NOT use an invisible click-catch overlay for this — a pointer-events
    // layer over the <video> would also swallow mousemove and stop the
    // native control bar from auto-showing on hover.
    //
    // Clicks on the bottom control-bar strip are ignored so the native
    // scrubber / play button / volume keep working untouched.
    if (vEl && !vEl._clickToggleWired) {
      vEl._clickToggleWired = true;
      let _lastToggle = 0;
      vEl.addEventListener("click", (e) => {
        const rect = vEl.getBoundingClientRect();
        if (!rect.height) return;
        const CONTROL_STRIP = 48; // native control bar height (approx)
        if (e.clientY >= rect.bottom - CONTROL_STRIP) return;
        const now = (window.performance && performance.now)
          ? performance.now() : 0;
        if (now && now - _lastToggle < 300) return; // collapse double-fire
        _lastToggle = now;
        if (vEl.paused) vEl.play().catch(() => {});
        else vEl.pause();
      });
    }
    return ovl;
  }

  const _CAPTION_HOLD = 0.7;
  let _captionMeasureContext;
  window._seekWatchTo = seconds => _seekTo(document.getElementById("watch-video"), seconds);

  function _updateCaptionScale(vEl) {
    const rect = vEl.getBoundingClientRect();
    // Size choices are proportions of a 640 x 360 player, independent of
    // monitor size, UI font preferences, and the video's encoded resolution.
    // Retain the last usable geometry while hidden or waiting for metadata.
    if (Number.isFinite(rect.width) && Number.isFinite(rect.height)
        && rect.width > 0 && rect.height > 0) {
      vEl._capScale = Math.min(rect.width / 640, rect.height / 360);
      vEl._capPlayerWidth = rect.width;
    }
    vEl._capOverlay?.style.setProperty("--cap-scale", String(vEl._capScale || 1));
  }

  function _rollingCaptionCache(vEl, allWordEls) {
    let cache = vEl._capRollingCache;
    if (!cache || cache.words !== allWordEls) {
      // Sort a separate timing view so overlapping or out-of-order segments
      // cannot reveal future words. Saved transcripts and their DOM stay intact.
      const timeline = allWordEls.map(el => ({
        text: el.textContent.trim(), start: Number(el.dataset.s),
        end: Number.parseFloat(el.dataset.e),
        segmentEnd: Number.parseFloat(el.parentElement?.dataset.e),
      })).filter(word => word.text && Number.isFinite(word.start))
        .sort((a, b) => a.start - b.start);
      timeline.forEach((word, i) => {
        if (!Number.isFinite(word.end) || word.end < word.start) {
          const next = timeline[i + 1]?.start;
          word.end = Math.min(next > word.start ? next : word.start + 0.5,
            word.segmentEnd > word.start ? word.segmentEnd : word.start + 0.5,
            word.start + 1);
        }
      });
      cache = { words: allWordEls, timeline, layout: null, renderKey: null };
      vEl._capRollingCache = cache;
    }
    if (!cache.layout || cache.layoutDirty) {
      cache.layoutDirty = false;
      const style = getComputedStyle(vEl._capOverlay.children[1]);
      const fontPx = Number.parseFloat(style.fontSize) || 13;
      const font = `${style.fontStyle} ${style.fontWeight} ${fontPx}px ${style.fontFamily}`;
      const scale = vEl._capScale || 1;
      const width = Math.max(1, Math.min(
        (vEl._capPlayerWidth || 640) * 0.86 - 14 * scale, fontPx * 26));
      const key = `${font}:${width}:${scale}`;
      if (cache.layout?.key !== key) {
        _captionMeasureContext ||= document.createElement("canvas").getContext("2d");
        if (_captionMeasureContext) _captionMeasureContext.font = font;
        const measure = text => _captionMeasureContext
          ? _captionMeasureContext.measureText(text).width : text.length * fontPx * 0.6;
        const lines = [];
        let line = null;
        let block = 0;
        cache.timeline.forEach((word, i) => {
          const previous = cache.timeline[i - 1];
          const gap = previous && word.start - previous.end > _CAPTION_HOLD;
          if (gap) { block += 1; line = null; }
          const text = line ? `${line.text} ${word.text}` : word.text;
          const measured = measure(text);
          if (!line || measured > width) {
            line = { first: i, last: i, block, text: word.text, width: measure(word.text) };
            lines.push(line);
          } else {
            line.last = i;
            line.text = text;
            line.width = measured;
          }
          word.line = lines.length - 1;
        });
        cache.layout = { key, fontPx, width, scale, lines };
        cache.renderKey = null;
        cache.lineKey = null;
      }
    }
    return cache;
  }

  function _renderRollingCaption(vEl, allWordEls, force) {
    const ovl = vEl._capOverlay;
    const cache = _rollingCaptionCache(vEl, allWordEls);
    const { timeline, layout } = cache;
    const t = vEl.currentTime;
    let lo = 0, hi = timeline.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (timeline[mid].start <= t) lo = mid + 1;
      else hi = mid;
    }
    const index = lo - 1;
    const word = timeline[index];
    if (!word || vEl.ended || t > word.end + _CAPTION_HOLD) {
      ovl.classList.remove("show");
      cache.renderKey = null;
      cache.lineKey = null;
      return;
    }
    const cell = ovl.children[1];
    const key = `${layout.key}:${index}`;
    if (!force && cache.renderKey === key && ovl.classList.contains("show")) return;
    if (force) {
      cell.getAnimations({ subtree: true }).forEach(animation => animation.cancel());
      cell.querySelector('[data-row="outgoing"]')?.remove();
    }
    if (cache.renderKey !== key) {
      const current = layout.lines[word.line];
      const candidate = layout.lines[word.line - 1];
      const previous = candidate?.block === current.block ? candidate : null;
      const oldCurrent = cell.querySelector('[data-row="current"]');
      const lineKey = `${layout.key}:${current.first}`;
      const spoken = timeline.slice(current.first, index + 1).map(item => item.text).join(" ");
      if (cache.lineKey === lineKey && oldCurrent) {
        // A fast speaker can add words during the scroll. Preserve the rows
        // and their animations while extending the incoming line's text.
        oldCurrent.firstChild.textContent = spoken;
        cache.renderKey = key;
        ovl.classList.add("show");
        return;
      }
      const canRoll = previous && cache.lineKey && !force && !vEl.paused && !vEl.seeking
        && oldCurrent?.dataset.line === String(previous.first)
        && !window.matchMedia("(prefers-reduced-motion: reduce)").matches;
      const outgoing = canRoll ? cell.querySelector('[data-row="previous"]') : null;
      cell.getAnimations({ subtree: true }).forEach(animation => animation.cancel());
      cell.style.setProperty("--cap-row-height", `${layout.fontPx * 1.25 + 4 * layout.scale}px`);
      const rows = [previous, current].map((line, row) => {
        const span = row === 0 && canRoll ? oldCurrent : document.createElement("span");
        span.className = "cap-ovl-line";
        span.dataset.row = row === 0 ? "previous" : "current";
        span.dataset.line = line ? String(line.first) : "";
        // Reserve the complete line's measured width. Its spoken prefix grows
        // from one fixed position; future words never enter the rendered DOM.
        const scale = line ? Math.min(1, layout.width / Math.max(1, line.width)) : 1;
        span.style.width = line ? `${Math.min(layout.width, line.width) + 14 * layout.scale}px` : "0px";
        span.style.fontSize = `${layout.fontPx * scale}px`;
        span.replaceChildren();
        if (line) {
          const text = document.createElement("span");
          text.className = "cap-ovl-text";
          text.textContent = row === 0 ? line.text : spoken;
          span.appendChild(text);
        }
        return span;
      });
      cell.replaceChildren(...rows);
      if (canRoll) {
        const distance = layout.fontPx * 1.25 + 6 * layout.scale;
        const nextLine = layout.lines[word.line + 1];
        const nextStart = nextLine ? timeline[nextLine.first].start : Infinity;
        const playbackRate = Math.max(0.1, Number(vEl.playbackRate) || 1);
        // Finish before the next line arrives, including accelerated playback
        // and a narrow player where one long word can occupy a whole row.
        const duration = Math.max(1, Math.min(250, (nextStart - t) * 800 / playbackRate));
        const slide = (element, from, to) => element.animate([
          { transform: `translate(-50%, ${from}px)` },
          { transform: `translate(-50%, ${to}px)` },
        ], { duration, easing: "ease-out" });
        rows.forEach(row => slide(row, distance, 0));
        // A third row exists only while leaving the clipped two-row window.
        // No animation is allowed to grow the viewport into a third line.
        if (outgoing?.textContent) {
          outgoing.dataset.row = "outgoing";
          cell.appendChild(outgoing);
          const animation = slide(outgoing, 0, -distance);
          animation.finished.then(() => outgoing.remove(), () => outgoing.remove());
        }
      }
      cache.renderKey = key;
      cache.lineKey = lineKey;
    }
    ovl.classList.add("show");
  }

  // `default` selects rolling captions. Explicit one/three-word choices
  // keep their centered active word while the overlay remains enabled.

  function _updateCapOverlay(vEl, curEl, allWordEls, wordIndex, force = false) {
    const ovl = vEl && vEl._capOverlay;
    if (!ovl) return;
    const p = window._captionPrefs || {};
    const on = p.size && p.size !== "off";
    const mode = p.mode || "default";
    if (!on || mode !== "default") {
      ovl.getAnimations({ subtree: true }).forEach(animation => animation.cancel());
      ovl.querySelector('[data-row="outgoing"]')?.remove();
    }
    if (!on) { ovl.classList.remove("show"); return; }
    const i = wordIndex ? wordIndex.get(curEl) : -1;
    const has = (j) => allWordEls && j >= 0 && j < allWordEls.length;
    ovl.dataset.capMode = mode;
    if (mode === "default") {
      if (ovl.children[0].firstChild) ovl.children[0].textContent = "";
      if (ovl.children[2].firstChild) ovl.children[2].textContent = "";
      _renderRollingCaption(vEl, allWordEls, force);
      return;
    }
    if (vEl._capRollingCache) {
      vEl._capRollingCache.renderKey = null;
      vEl._capRollingCache.lineKey = null;
    }
    if (!curEl) { ovl.classList.remove("show"); return; }
    const prevEl = (mode === "phrase3" && has(i - 1)) ? allWordEls[i - 1] : null;
    const nextEl = (mode === "phrase3" && has(i + 1)) ? allWordEls[i + 1] : null;
    ovl.children[0].textContent = prevEl ? prevEl.textContent.trim() : "";
    delete ovl.children[1].dataset.cueKey;
    ovl.children[1].textContent = curEl.textContent.trim();
    ovl.children[2].textContent = nextEl ? nextEl.textContent.trim() : "";
    ovl.classList.add("show");
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
    const mode = p.mode || "default";
    const ovl = vEl._capOverlay;
    if (ovl) {
      ovl.dataset.capSize = (size === "off") ? "" : size;
      ovl.dataset.capBg = bg;
      ovl.dataset.capMode = mode;
      if (size === "off") ovl.classList.remove("show");
      vEl._capLayoutDirty = true;
    }
  }

  // Public setter — app.js calls this when the user changes a toolbar
  // select. We update the cache + apply to the currently-loaded video.
  window.setCaptionPref = function (key, value) {
    window._captionPrefs = window._captionPrefs || {};
    if (key === "size" || key === "bg" || key === "mode") {
      window._captionPrefs[key] = value;
    }
    const vEl = document.getElementById("watch-video");
    if (!vEl) return;
    _applyCaptionPrefs(vEl);
    // Repaint the overlay right away so size/mode/bg changes are visible
    // even while the video is paused.
    try { vEl._capForceRefresh && vEl._capForceRefresh(); } catch { /* ignore */ }
  };
})();
