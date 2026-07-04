/* ═══════════════════════════════════════════════════════════════════════
   logs.js — YTArchiver log rendering (hot path)

   Log line data format:
     segments = [[text, tag?], [text, tag?], ...]
     tag = string class name (e.g. "simpleline_pink") OR array of classes OR null

   Matches tkinter's tag system: each segment of a line carries one or
   more tags; CSS classes on <span> produce the colors.
   ═══════════════════════════════════════════════════════════════════════ */

(function () {
  "use strict";

  // Scroll state per log container (user-scrolled = don't auto-snap to bottom)
  const scrollState = new WeakMap();

  // Progress tags that should replace their previous emission instead of
  // appending a new line each tick. Matches YTArchiver's log_box.insert
  // pattern where whisper_progress / encode_progress lines replace in-place.
  const INPLACE_TAGS = new Set([
    "whisper_progress", "whisper_pct", "whisper_dots",
    "encode_progress", "encode_pct", "encode_dots",
    "startup_loading",
  ]);

  function _inplaceKind(segments) {
    // Classify the line as "whisper" or "encode" or "startup" if its tag
    // is a known progress family; the log-batch renderer replaces the
    // previous line of the same kind instead of appending a new one.
    //
    // PRIORITY-ORDERED MULTI-PASS. The old single-pass scan was
    // order-sensitive WITHIN a single tag list: a segment tagged
    // `["dim", "whisper_job_N", "tx_done_vid"]` would hit the
    // `whisper_job_` check on tag #2 and return that — even though
    // tx_done_ is supposed to win. Result: the transcribe-done line
    // resolved to `whisper_job_N` while the placeholder resolved to
    // `tx_done_vid`, so the done emit couldn't find its placeholder
    // and appended fresh, leaving the "⏳ Transcription queued…"
    // line visible alongside the "✓ Transcription" line.
    //
    // Fix: separate scan per prefix, in priority order. Check ALL
    // segments × ALL tags for `tx_done_` first. If none found, fall
    // back to `whisper_job_`, then `sync_row_`, then `dlrow_`, etc.
    const segs = segments || [];
    const allTags = [];
    for (const seg of segs) {
      if (!seg) continue;
      const tag = seg[1];
      if (Array.isArray(tag)) {
        for (const t of tag) if (t) allTags.push(t);
      } else if (tag) {
        allTags.push(tag);
      }
    }

    // Priority order for per-job / per-row markers.
    const priorityPrefixes = [
      "tx_done_",
      "meta_done_",
      "compress_done_",
      "dwnld_done_",
      "whisper_job_",
      "sync_row_",
      "dlrow_",
    ];
    for (const prefix of priorityPrefixes) {
      for (const t of allTags) {
        if (t.startsWith(prefix)) return t;
      }
    }

    // Per-workflow "sticky at bottom" active-status lines.
    for (const t of allTags) {
      if (t === "redwnl_active") return t;
      if (t === "metadata_active") return t;
      if (t === "compress_active") return t;
      if (t === "reorg_active") return t;
      // Video ID backfill — two progress phases that replace in-
      // place: "Fetched N videos from YouTube catalog..." counter,
      // then the "[N/M] matched K so far..." match-loop counter.
      // Both cleared via clear_line when the final summary emits.
      if (t === "backfill_progress") return t;
      // Views/likes refresh per-channel transitional lines —
      // "Refreshing X...", "N video(s) have updated counts...",
      // and "[N/M] fetching metadata..." all share this kind so
      // each replaces the previous in-place. Cleared via
      // clear_line when the final per-channel summary emits.
      if (t === "views_refresh_progress") return t;
      // Comments refresh sticky [N/total] progress line — replaces
      // in-place per video so the user sees progress on long
      // channels (e.g. David Pakman = 2086 videos). Cleared via
      // clear_line when the channel finishes or pauses.
      if (t === "comments_refresh_active") return t;
    }

    // Pass 2 — prefix-family matches (shared in-place families)
    for (const t of allTags) {
      if (t.startsWith("whisper_")) return "whisper";
      if (t.startsWith("encode_")) return "encode";
      if (t.startsWith("startup_")) return "startup";
    }
    return null;
  }

  // Should this log line stay pinned at the bottom of the main log?
  //
  // Whisper progress lines carry `whisper_pct` / `whisper_progress`
  // tags. We pin them ONLY when no sync pass is running — i.e. when
  // the user manually triggered a single retranscribe from the Browse
  // tab and is watching their mini-log for the result. Pinning keeps
  // the live "Transcribing… X%" line in the last N lines the mini-log
  // mirrors.
  //
  // During a sync pass, pinning is wrong: the progress line drifts to
  // the bottom of the log, away from the "[N/total] Channel" header
  // and the "✓ <Video Title>" line it belongs to. The user sees
  // "Transcription (Whisper small, took 1min 15sec)" at the bottom
  // with no idea which video it goes with. Inline rendering keeps
  // the progress line with its video.
  function _isPinToBottom(segments) {
    if (!Array.isArray(segments)) return false;
    // Sync running → never pin transcribe progress. Keep it inline so
    // it stays with its channel + video context in the log. Read the
    // batch-cached result first (set in _logBatch) to avoid an O(M)
    // sync-queue walk per line (audit: logs.js H198).
    try {
      if (window.__yt_batch_anySyncRunning === true) return false;
      if (window.__yt_batch_anySyncRunning === false) { /* fall through */ }
      else if (typeof window._anySyncRunning === "function"
          && window._anySyncRunning()) {
        return false;
      }
    } catch (_e) { /* fall through */ }
    // ALSO never pin a line that carries a `tx_done_<vid>` marker.
    // That marker means the line belongs to a specific per-video slot
    // (originally seeded by sync.py as the " — ⏳ Transcription queued…"
    // placeholder under the channel's video row). If sync finishes
    // mid-Whisper and the next progress tick is the FIRST emit after
    // _anySyncRunning() flips to false, the old behavior pulled the
    // progress line out of its row-62 slot and moved it to the log
    // bottom — and every subsequent inplace replacement (including the
    // final "— ✓ Transcription" done line) followed it down there.
    // The tx_done_ marker is the explicit "I belong here, not at the
    // bottom" signal; honor it.
    for (const seg of segments) {
      if (!Array.isArray(seg) || seg.length < 2) continue;
      const tag = seg[1];
      const tags = Array.isArray(tag) ? tag : [tag];
      for (const t of tags) {
        if (typeof t === "string" && t.startsWith("tx_done_")) return false;
      }
    }
    for (const seg of segments) {
      if (!Array.isArray(seg) || seg.length < 2) continue;
      const tag = seg[1];
      const tags = Array.isArray(tag) ? tag : [tag];
      for (const t of tags) {
        if (t === "whisper_pct" || t === "whisper_progress") return true;
      }
    }
    return false;
  }

  // Remove the in-place "Loading…" line once startup has genuinely completed.
  // Called by the backend via evaluate_js once the background sweep finishes.
  // Also re-mirrors mini logs so they don't keep showing the stale Loading line.
  window.clearStartupLine = function () {
    const el = document.getElementById("main-log");
    if (!el) return;
    el.querySelectorAll('.log-line[data-inplace="startup"]').forEach(n => n.remove());
    if (typeof window._mirrorMiniLogs === "function") window._mirrorMiniLogs();
  };

  // Two-slot startup indicator — backend drives via
  // window._setIndicator(slot, text|null) where slot is "sweep" or
  // "preload". Sweep slot carries disk-scan / indexing progress;
  // preload slot carries the browse-tab preload progress. They can
  // both run in parallel, so both slots may be visible at once.
  // Auto-dismiss timers keyed by slot, so a long-lived "Browse preload
  // complete: …" doesn't stick on-screen forever. Cleared whenever new
  // text comes in, so progress updates aren't pre-emptively hidden.
  const _indicatorTimers = {};
  window._setIndicator = function (slot, text) {
    const el = document.getElementById("browse-preload-slot-" + slot);
    if (!el) return;
    if (_indicatorTimers[slot]) {
      clearTimeout(_indicatorTimers[slot]);
      _indicatorTimers[slot] = null;
    }
    if (!text) {
      el.hidden = true;
      el.textContent = "";
    } else {
      el.hidden = false;
      el.textContent = text;
      // Terminal "complete" lines fade after 45s. Active progress
      // ("Preloading …", "Scanning …") stays visible.
      if (/(?:^|\b)(?:complete|done)(?:[:.!-]|\s*$)/i.test(text)) {
        _indicatorTimers[slot] = setTimeout(() => {
          const current = document.getElementById(
            "browse-preload-slot-" + slot);
          if (!current) return;
          current.hidden = true;
          current.textContent = "";
          _indicatorTimers[slot] = null;
        }, 45000);
      }
    }
  };
  // Back-compat shim for any caller still using the pre-split name.
  window._setPreloadIndicator = function (text) {
    window._setIndicator("sweep", text);
  };

  function tagClasses(tag) {
    if (!tag) return "";
    if (Array.isArray(tag)) {
      return tag.map(t => "t-" + t).join(" ");
    }
    return "t-" + tag;
  }

  function buildLine(segments, lineClass) {
    const line = document.createElement("div");
    line.className = lineClass ? "log-line " + lineClass : "log-line";
    for (const seg of segments) {
      if (!seg) continue;
      const text = seg[0] == null ? "" : String(seg[0]);
      const tag = seg[1];
      const span = document.createElement("span");
      const cls = tagClasses(tag);
      if (cls) span.className = cls;
      span.textContent = text;
      line.appendChild(span);
    }
    return line;
  }

  function isAtBottom(el) {
    const gap = el.scrollHeight - el.clientHeight - el.scrollTop;
    return gap < 4;
  }

  function maybeSnapToBottom(el) {
    const st = scrollState.get(el) || {};
    if (!st.userScrolled) {
      el.scrollTop = el.scrollHeight;
    }
  }

  function wireUserScrollDetection(el) {
    if (!el || scrollState.has(el)) return;
    const st = { userScrolled: false };
    scrollState.set(el, st);

    // Build the "↓ Latest" floating affordance once, parented to the frame.
    const frame = el.parentElement;
    let jumpBtn = null;
    if (frame && !frame.querySelector(".log-jump-latest")) {
      const pos = getComputedStyle(frame).position;
      if (pos === "static") frame.style.position = "relative";
      jumpBtn = document.createElement("button");
      jumpBtn.className = "log-jump-latest";
      jumpBtn.type = "button";
      jumpBtn.textContent = "\u2193 Latest";
      jumpBtn.title = "Scroll to newest (auto-follow)";
      jumpBtn.style.display = "none";
      jumpBtn.addEventListener("click", (ev) => {
        ev.stopPropagation();
        st.userScrolled = false;
        el.scrollTop = el.scrollHeight;
        jumpBtn.style.display = "none";
      });
      frame.appendChild(jumpBtn);
    } else if (frame) {
      jumpBtn = frame.querySelector(".log-jump-latest");
    }
    const refreshBtn = () => {
      if (!jumpBtn) return;
      jumpBtn.style.display = st.userScrolled && !isAtBottom(el) ? "" : "none";
    };

    // Single scroll handler covers every input method (wheel, scrollbar
    // drag, keyboard PgUp/End, touchpad momentum). Rule: if we're NOT at
    // the bottom after a scroll event, the user has scrolled away — stop
    // auto-following. Reaching the bottom again re-enables follow.
    //
    // Programmatic `el.scrollTop = el.scrollHeight` also fires a scroll
    // event, but it always lands at bottom → isAtBottom=true → doesn't
    // flip the flag. Safe.
    el.addEventListener("scroll", () => {
      st.userScrolled = !isAtBottom(el);
      refreshBtn();
    });
  }

  function _snapIfFollowing(el) {
    const st = scrollState.get(el) || {};
    if (!st.userScrolled) el.scrollTop = el.scrollHeight;
  }

  function _insertTrimWarning(el) {
    const existing = el.querySelector(".log-line.log-trim-warn");
    if (existing) existing.remove();
    const warn = buildLine([[
      " \u26A0 Log trimmed \u2014 older lines removed to keep it scrollable.",
      "dim",
    ]]);
    warn.classList.add("log-trim-warn");
    el.insertBefore(warn, el.firstChild);
  }

  function _trimMainLog(el) {
    const cap = 8000;
    const keep = 5000;
    if (el.childElementCount <= cap) return;
    const toRemove = el.childElementCount - keep;
    for (let i = 0; i < toRemove; i++) el.removeChild(el.firstChild);
    _insertTrimWarning(el);
  }

  // ─── Public API ──────────────────────────────────────────────────────

  /** Build one grid-aligned activity-log row.
   * Coloring rule (matches the real YTArchiver screenshot): ONLY the
   * [Kind] tag and the primary "N replaced / N fetched / N downloaded"
   * cell get the tag color. Time, channel, em-dashes, skipped, errors,
   * took all stay default. */
  // Inline regex-highlight map for autorun history cells — mirrors
  // YTArchiver.py:22290-22369 _insert_hist_line inline-color rules:
  // "N downloaded" → green
  // "N transcribed" → blue
  // "N replaced" → chartreuse (redwnl)
  // "N compressed" → purple
  // "N moved" → orange (reorg)
  // "N fetched" → pink
  // "N skipped" → amber (uses c-log-sum)
  // "N errors?" → red
  // Only match non-zero counts — "0 skipped" / "0 errors" should stay dim
  // default color (matches YTArchiver.py _insert_hist_line behavior: tags
  // are only applied when the count is > 0).
  // Each pattern has a numeric form AND a checkmark form. The checkmark
  // form fires when a cell represents exactly 1 of something (
  // polish: "if it's just one, can we change the numbers to checkmarks").
  // `\b` won't assert a word boundary before \u2713 (non-word char), so
  // the checkmark regex drops the leading boundary and relies on the
  // label's `\b` on the right side to bound the match.
  const _HIST_HILITE = [
    [() => /\b([1-9]\d*)\s+(downloaded|new)\b/gi, "t-hist_green"],
    [() => /(\u2713)\s+(downloaded|new)\b/gi, "t-hist_green"],
    [() => /\b([1-9]\d*)\s+(transcribed|captions?)\b/gi, "t-hist_blue"],
    [() => /(\u2713)\s+(transcribed|captions?)\b/gi, "t-hist_blue"],
    [() => /\b([1-9]\d*)\s+(replaced|remade)\b/gi, "t-hist_redwnl"],
    [() => /(\u2713)\s+(replaced|remade)\b/gi, "t-hist_redwnl"],
    [() => /\b([1-9]\d*)\s+(compressed)\b/gi, "t-hist_compress"],
    [() => /(\u2713)\s+(compressed)\b/gi, "t-hist_compress"],
    [() => /\b([1-9]\d*)\s+(moved|reorged)\b/gi, "t-hist_reorg"],
    [() => /(\u2713)\s+(moved|reorged)\b/gi, "t-hist_reorg"],
    // Optional-word slot (?:\w+\s+)? catches "N IDs backfilled" or
    // "N comments refreshed" — the in-use phrase forms where a noun
    // sits between the count and the verb. Without this, those
    // phrases matched nothing and rendered in the default color
    // instead of pink.
    [() => /\b([1-9]\d*)\s+(?:\w+\s+)?(fetched|refreshed|metadata|backfilled)\b/gi, "t-hist_pink"],
    [() => /(\u2713)\s+(?:\w+\s+)?(fetched|refreshed|metadata|backfilled)\b/gi, "t-hist_pink"],
    [() => /\b([1-9]\d*)\s+skipped\b/gi, "t-hist_skipped"],
    [() => /\b([1-9]\d*)\s+errors?\b/gi, "t-hist_error"],
    // Warning-but-not-error states: "N unresolved" (ID backfill
    // couldn't match) and "N ambiguous" (title-match hit multiple
    // candidates). Amber/orange matches the "heads up, look at
    // this but it's not broken" semantic the user wanted.
    [() => /\b([1-9]\d*)\s+(unresolved|ambiguous)\b/gi, "t-hist_skipped"],
  ];

  function _buildHistCell(text, extra, colored, tagCls) {
    // Slow-path builder that tokenizes text against _HIST_HILITE and emits
    // per-match coloured <span>s so the plain-dark default cell shows
    // "4 downloaded" green + "2 skipped" amber + "1 error" red inline.
    const s = document.createElement("span");
    const baseCls = ((colored ? tagCls : "") + " " + (extra || "")).trim();
    if (baseCls) s.className = baseCls;
    if (text == null || text === "") return s;

    // Build an index of (start, end, cls) ranges. Each pass creates
    // fresh global regexes so lastIndex cannot leak between calls.
    const ranges = [];
    for (const [makeRx, cls] of _HIST_HILITE) {
      const rx = makeRx();
      let m;
      while ((m = rx.exec(text)) !== null) {
        ranges.push({ start: m.index, end: m.index + m[0].length, cls });
        if (m.index === rx.lastIndex) {
          rx.lastIndex++;
        }
      }
    }
    if (!ranges.length) { s.textContent = String(text); return s; }
    ranges.sort((a, b) => a.start - b.start || a.end - b.end);
    // De-overlap: keep the first, drop any that starts before previous end
    const kept = [];
    let cursor = -1;
    for (const r of ranges) {
      if (r.start >= cursor) { kept.push(r); cursor = r.end; }
    }
    // Walk the string and split into text + span chunks
    let pos = 0;
    for (const r of kept) {
      if (r.start > pos) s.appendChild(document.createTextNode(text.slice(pos, r.start)));
      const m = document.createElement("span");
      m.className = r.cls;
      m.textContent = text.slice(r.start, r.end);
      s.appendChild(m);
      pos = r.end;
    }
    if (pos < text.length) s.appendChild(document.createTextNode(text.slice(pos)));
    return s;
  }

  function buildActivityRow(entry) {
    if (entry.segments && !entry.cells) {
      return buildLine(entry.segments, entry.alt ? "hist_row_alt" : "");
    }
    const c = entry.cells || {};
    const tag = c.row_tag || "";
    const tagCls = tag ? "t-" + tag : "";
    const line = document.createElement("div");
    // Row-kind class lets the grid template adapt per activity kind
    // (e.g. [ReDwnl] has only 3 counts + took; [Dwnld] has 4 counts +
    // took). Without per-kind grid overrides, unused count cells
    // reserve their minmax widths and push adjacent cells out.
    const _kindSlug = String(c.kind || "").replace(/[^A-Za-z0-9]/g, "");
    const _kindCls = _kindSlug ? " hist-row-" + _kindSlug : "";
    // Content-driven empty-cell markers — let CSS collapse the column +
    // its leading dash to 0 when there's nothing in it, so width-
    // constrained [Metdta] rows (which leave tertiary empty) don't show
    // a wide gap with floating dashes in the middle.
    const _noSecondary = !c.secondary || !String(c.secondary).trim();
    const _noTertiary  = !c.tertiary  || !String(c.tertiary).trim();
    const _emptyCls = (_noSecondary ? " hist-no-secondary" : "") +
                      (_noTertiary  ? " hist-no-tertiary"  : "");
    line.className = "log-line" + (entry.alt ? " hist_row_alt" : "") +
                     _kindCls + _emptyCls;
    // `row_id` lets the backend retroactively update a previously-
    // emitted row (e.g. a [Dwnld] row that fired with `0 transcribed`
    // while Whisper was still running — the transcribe-complete hook
    // re-emits with the same row_id and the merged counts). When set,
    // `_logBatch` finds any existing `[data-row-id="..."]` element in
    // the activity log and swaps it in place instead of appending.
    if (c.row_id) line.dataset.rowId = c.row_id;
    const cell = (text, extra, colored) => {
      const s = document.createElement("span");
      const cls = ((colored ? tagCls : "") + " " + (extra || "")).trim();
      if (cls) s.className = cls;
      s.textContent = text == null ? "" : String(text);
      return s;
    };
    // ONLY kind + primary take the row tag color.
    // Secondary / tertiary / errors cells get inline-regex highlighting instead.
    // `tertiary` is the new 3rd count cell added for consolidated [Dwnld]
    // rows — `N metadata`. Classic rows (Trnscr, Metdta, etc.) leave it
    // empty so the grid just renders a blank cell there.
    // Em-dash separators between cells. To keep [Metdta] rows clean
    // (their secondary/tertiary cells are empty), only emit the dash
    // when the cell to the RIGHT has content \u2014 that way an empty
    // secondary doesn't print "\u2014 \u2014" floating in the middle. Grid
    // columns still exist (so vertical alignment across row kinds
    // stays consistent); only the dash glyph appears conditionally.
    const dash = (hasNext) =>
      cell(hasNext ? "\u2014" : "", "hist-col-dash", false);
    const _hasSecondary = !!(c.secondary && String(c.secondary).trim());
    const _hasTertiary  = !!(c.tertiary  && String(c.tertiary).trim());
    const _hasErrors    = !!(c.errors    && String(c.errors).trim());
    const _hasTook      = !!(c.took      && String(c.took).trim());

    line.appendChild(cell(`[${c.kind || ""}]`, "hist-col-kind", true));
    line.appendChild(cell(c.time_date || "", "hist-col-time", false));
    line.appendChild(dash(true));  // always show before channel
    line.appendChild(cell(c.channel || "", "hist-col-channel", false));
    line.appendChild(dash(true));  // always show before primary
    line.appendChild(cell(c.primary || "", "hist-col-num", true));
    line.appendChild(dash(_hasSecondary));
    line.appendChild(_buildHistCell(c.secondary || "", "hist-col-num", false, tagCls));
    line.appendChild(dash(_hasTertiary));
    line.appendChild(_buildHistCell(c.tertiary || "", "hist-col-num", false, tagCls));
    line.appendChild(dash(_hasErrors));
    line.appendChild(_buildHistCell(c.errors || "", "hist-col-num", false, tagCls));
    line.appendChild(dash(_hasTook));
    line.appendChild(cell(c.took || "", "hist-col-took", false));
    return line;
  }

  /** Replace the whole activity log with a list of entries. */
  window.renderActivityLog = function (entries) {
    const el = document.getElementById("activity-log");
    if (!el) return;
    wireUserScrollDetection(el);
    // replaceChildren is cheaper than innerHTML="" for a large
    // activity log — innerHTML triggers a full HTML re-parse pass
    // even for empty string (audit: logs.js:474).
    if (typeof el.replaceChildren === "function") {
      el.replaceChildren();
    } else {
      el.innerHTML = "";
    }
    entries.forEach((entry) => {
      el.appendChild(buildActivityRow(entry));
    });
    // First render — force scroll to bottom so the newest row is visible.
    // `maybeSnapToBottom` alone can fire before the browser has laid out
    // the new rows, leaving scrollTop at 0. Delay via rAF + one more tick.
    maybeSnapToBottom(el);
    requestAnimationFrame(() => {
      // Re-check userScrolled before each snap. The setTimeout fires
      // 30ms later — if the user scrolled up in that window (mouse
      // wheel right after the first render), the old code still
      // yanked them to the bottom (audit: logs.js:493).
      const _st1 = scrollState.get(el);
      if (!_st1 || !_st1.userScrolled) el.scrollTop = el.scrollHeight;
      setTimeout(() => {
        const _st2 = scrollState.get(el);
        if (!_st2 || !_st2.userScrolled) el.scrollTop = el.scrollHeight;
      }, 30);
    });
  };

  /** Append a single activity log entry.
   * mirror the main-log cap/keep behavior so the activity
   * log doesn't grow unbounded. User windows often stay open for days;
   * without a cap this DOM node accumulates until the layout engine
   * starts struggling.
   *
   * 2026-05-14: bumped cap from 4000/2500 → 30000/25000. With 105+
   * channels syncing multiple times a day, plus per-channel metadata
   * rows + thumbnail refetch rows, 2500 lines = maybe one day of
   * activity. User asked for "way larger" — 30k holds weeks of
   * structured rows without the layout engine breaking a sweat
   * (rows are static text + classes, no event listeners).
   */
  function _trimActivityLog(el) {
    const cap = 30000, keep = 25000;
    if (el.childElementCount > cap) {
      let toRemove = el.childElementCount - keep;
      if (toRemove % 2 === 1) toRemove++;
      for (let i = 0; i < toRemove; i++) el.removeChild(el.firstChild);
    }
  }

  function _applyActivityParity(row, previousRow) {
    try {
      const prevAlt = previousRow
        && previousRow.classList.contains("hist_row_alt");
      if (prevAlt) row.classList.remove("hist_row_alt");
      else row.classList.add("hist_row_alt");
    } catch (_e) { /* non-fatal */ }
  }

  window.appendActivityLog = function (entry) {
    const el = document.getElementById("activity-log");
    if (!el) return;
    wireUserScrollDetection(el);
    const row = buildActivityRow(entry);
    el.appendChild(row);
    // don't trust entry.alt — the backend can lose track
    // after long sessions, leaving same-color rows adjacent. Compute
    // parity from the actual prior row in the DOM each append.
    _applyActivityParity(row, row.previousElementSibling);
    _trimActivityLog(el);
    maybeSnapToBottom(el);
  };

  // ── Mini-log mirror ─────────────────────────────────────────────────
  // Project rule: "MINI LOGS SHOULD ALWAYS SHOW EXACTLY WHAT THE MAIN LOG
  // SHOWS. NO DISCREPANCIES." Instead of maintaining a parallel append
  // buffer (which drifts when the main log removes lines in-place, e.g.
  // clearStartupLine, whisper_progress replacements), we snapshot the last
  // N lines of the main log into each mini after every mutation.
  const MINI_LOG_IDS = ["subs-mini-log", "browse-mini-log",
                        "recent-mini-log", "health-mini-log",
                        "settings-mini-log"];
  const MINI_LINES = 5;

  // cache DOM refs across calls. mirrorMiniLogs is
  // called after every emit during active sync — re-running 5×
  // getElementById per call is wasteful (~5 lookups × hundreds of
  // emits/sec during active sync). The cache invalidates if any ref's
  // isConnected goes false (DOM teardown).
  const _miniDom = { main: null, minis: {} };
  function _miniGet(id) {
    // Cache nulls too so we don't re-query getElementById every
    // emit for mini-logs that don't exist in this DOM (audit:
    // logs.js L130). Uses `in` check to distinguish "not yet
    // looked up" from "looked up and null".
    if (id in _miniDom.minis) {
      const el = _miniDom.minis[id];
      if (el === null) return null;
      if (el.isConnected) return el;
    }
    const el2 = document.getElementById(id);
    _miniDom.minis[id] = el2;
    return el2;
  }

  // Coalesce mirror calls into one per animation frame. Without this,
  // every line append during heavy sync output (hundreds of emits/sec)
  // re-clones 5 last main-log children into every mini-log — a lot of deep
  // clones + attribute strips per emit, a documented WebView2 jank
  // source (audit: logs.js C31). rAF batching collapses a burst into
  // one mirror per frame.
  let _miniScheduled = false;
  function _doMirrorMiniLogs() {
    let main = _miniDom.main;
    if (!main || !main.isConnected) {
      main = document.getElementById("main-log");
      _miniDom.main = main;
    }
    if (!main) return;
    const all = main.children;
    const start = Math.max(0, all.length - MINI_LINES);
    for (const id of MINI_LOG_IDS) {
      const m = _miniGet(id);
      if (!m) continue;
      // Rebuild mini as an exact slice clone of the last N main lines
      m.innerHTML = "";
      for (let i = start; i < all.length; i++) {
        // Strip the data-inplace attribute from the clone. Without
        // this, a subsequent inplace-replace emit (e.g. whisper
        // progress tick) would find BOTH the main-log line AND the
        // mini-log clone via the same `data-inplace="<kind>"`
        // selector and replace whichever it hit last — producing
        // doubled lines or the done-line landing in the wrong log.
        // strip every data-* attribute that participates
        // in in-place selectors (not just data-inplace). If a future
        // feature adds another selector-attracting dataset key, the
        // mini-log clones would silently start colliding with it
        // again. Enumerating the attribute list and removing any
        // data-* makes the clones truly inert.
        const clone = all[i].cloneNode(true);
        // Collect to a separate array first — DOM mutation during
        // NamedNodeMap iteration skips entries.
        // Bug [106]: also strip aria-* and role so screen readers don't
        // announce the same line twice (once from the main log, once
        // from this mini-log clone).
        const _toRemove = [];
        for (const _a of clone.attributes) {
          if (_a.name.startsWith("data-")
              || _a.name.startsWith("aria-")
              || _a.name === "role"
              // `id` would duplicate across main + 4 mini logs,
              // violating HTML spec uniqueness and breaking
              // getElementById lookups (audit: logs.js L120).
              || _a.name === "id") {
            _toRemove.push(_a.name);
          }
        }
        for (const _n of _toRemove) clone.removeAttribute(_n);
        m.appendChild(clone);
      }
    }
  }
  function mirrorMiniLogs() {
    if (_miniScheduled) return;
    _miniScheduled = true;
    const _fire = () => {
      _miniScheduled = false;
      try { _doMirrorMiniLogs(); } catch (_e) { /* non-fatal */ }
    };
    if (typeof window.requestAnimationFrame === "function") {
      window.requestAnimationFrame(_fire);
    } else {
      setTimeout(_fire, 16);
    }
  }
  window._mirrorMiniLogs = mirrorMiniLogs;

  /** Bulk render main log (initial). */
  window.renderMainLog = function (lines) {
    const el = document.getElementById("main-log");
    if (!el) return;
    wireUserScrollDetection(el);
    const frag = document.createDocumentFragment();
    for (const segs of lines) {
      frag.appendChild(buildLine(segs));
    }
    el.innerHTML = "";
    el.appendChild(frag);
    maybeSnapToBottom(el);
    mirrorMiniLogs();
  };

  /** Append a single line to the main log (hot path — called from Python). */
  window.appendMainLog = function (segments) {
    const el = document.getElementById("main-log");
    if (!el) return;
    wireUserScrollDetection(el);
    el.appendChild(buildLine(segments));
    _trimMainLog(el);
    maybeSnapToBottom(el);
    mirrorMiniLogs();
  };

  /** Append to a mini log (by element id). NO-OP — kept for backward-compat.
   * Mini logs are now a pure mirror of the last 5 main-log lines.
   * kept as a no-op shim rather than removed so any legacy
   * call sites don't ReferenceError. Caller code at
   * web/app.js:7377-7380 will eventually be removed — this just
   * absorbs for now. */
  window.appendMiniLog = function () {};

  /** Clear a log container.
   * also reset the scrollState.userScrolled flag for the
   * cleared element so the next appendMainLog auto-scrolls to the
   * bottom. Without this, "Clear log" while scrolled up left the
   * element pinned to a non-existent scroll position — new lines
   * arrived but the log looked frozen.
   */
  window.clearLog = function (id) {
    const el = document.getElementById(id);
    if (!el) return;
    el.innerHTML = "";
    try {
      const st = scrollState.get(el);
      if (st) {
        st.userScrolled = false;
        el.scrollTop = 0;
      }
    } catch {}
    // If the main log was cleared, clear the mirrors too.
    if (id === "main-log") mirrorMiniLogs();
  };

  /** Batched log delivery from Python. Called by LogStreamer.
   * payload = { main: [ [segments], [segments], ... ],
   * activity: [ { cells, alt }, ... ] }
   * Special segment tags signal in-place replacement:
   * - "whisper_progress" or "encode_progress" → replace the previous
   * line carrying that same tag (so progress bar ticks in place). */
  window._logBatch = function (payload) {
    if (!payload) return;
    // Process payload.main in ORDER. Previously control-channel
    // segments were filtered out in a pre-pass that ran BEFORE any
    // regular emissions were rendered — meaning a `clear_line`
    // control emitted AFTER a new inplace line in the same batch
    // could only clear DOM from PRIOR batches, not the sibling line
    // in its own batch.
    //
    // Reported symptom: redownload's `_clear_active()` fires right
    // after the last iteration's `_emit_active()`. Both emits land
    // in the same 60ms LogStreamer batch. The old pre-pass cleared
    // whatever was in DOM (from batch N-1, already gone), then
    // rendered the new active line, then dropped the clear_line —
    // which never saw the new active line. Result: active line
    // survived into the final "=== Redownload complete ===" footer
    // section.
    //
    // Fix: inline the control processing into the render loop, so
    // each control runs AT ITS POSITION and can clear elements
    // already added to `frag` in this same pass.
    if (Array.isArray(payload.main) && payload.main.length) {
      const el = document.getElementById("main-log");
      if (el) {
        // Wire user-scroll detection on first batch. Without this, the
        // scrollState map never gets an entry for #main-log, `userScrolled`
        // is undefined, and `!undefined === true` makes every batch
        // auto-scroll to the bottom — snapping away from the user's
        // read position.
        wireUserScrollDetection(el);
        const frag = document.createDocumentFragment();
        // Cache _anySyncRunning() ONCE at batch start so the per-line
        // `_isPinToBottom` calls don't all re-walk the sync queue —
        // O(N×M) per batch where M is queue size (audit: logs.js
        // H198). Stash on a global the per-line check reads.
        try {
          window.__yt_batch_anySyncRunning = (
            typeof window._anySyncRunning === "function"
            && window._anySyncRunning());
        } catch (_e) {
          window.__yt_batch_anySyncRunning = false;
        }
        let _batchHasPinned = false;
        for (const segs of payload.main) {
          if (!Array.isArray(segs) || segs.length === 0) continue;
          // Control-line handling — run in order, then skip render.
          if (segs.length === 1 && Array.isArray(segs[0])
              && segs[0][1] === "__control__") {
            try {
              const data = JSON.parse(segs[0][0] || "{}");
              // use CSS.escape on data.marker when building
              // the selector. If a marker ever contains special CSS
              // chars (", ], \, etc.) the raw interpolation produces
              // an invalid selector and querySelectorAll throws —
              // killing the entire log batch and breaking in-place
              // replacements downstream. CSS.escape is browser-native.
              // Dispatch the yt-control event FIRST so listeners
               // observe the removal as a clean transition. Old order
               // was remove → dispatch, which let a listener that
               // also handled clear_line attempt a second removal on
               // an already-detached node (audit: logs.js:746).
              window.dispatchEvent(new CustomEvent("yt-control", {
                detail: data,
              }));
              if (data.kind === "clear_line" && data.marker) {
                // Remove matching `data-inplace` elements from BOTH
                // the committed DOM AND the in-progress fragment —
                // otherwise a same-batch clear can't see the sibling
                // inplace line that was just added above it.
                // Escape EVERY CSS-attribute-value metacharacter so a
                // marker containing `(`, `)`, `[`, `*`, `:`, etc.
                // doesn't break the selector. The previous partial
                // fallback only handled `"`, `\`, `]` (audit:
                // logs.js H202).
                const _esc = (typeof CSS !== "undefined" && CSS.escape)
                  ? CSS.escape(data.marker) : String(data.marker)
                    .replace(/[^\w-]/g, (c) =>
                      "\\" + c.charCodeAt(0).toString(16) + " ");
                const sel = `.log-line[data-inplace="${_esc}"]`;
                frag.querySelectorAll(sel).forEach((n) => n.remove());
                el.querySelectorAll(sel).forEach((n) => n.remove());
              }
            } catch (e) {
              console.error("control payload parse failed:", e);
            }
            continue;
          }
          const line = buildLine(segs);
          // Surface whisper_pct percentage onto the watch-view
          // Re-transcribe button — but only for the video this line
          // belongs to. The `tx_done_<vid>` marker tag rides every
          // transcribe progress emit (transcribe/legacy.py
          // _emit_progress), so we read the video_id out of it and let
          // the update function decide whether THAT video is on screen.
          // Without the video_id gate, Video A's progress used to paint
          // Video B's button when the user navigated away mid-transcribe.
          try {
            if (window._inflightRetranscribes
                && window._inflightRetranscribes.size > 0) {
              let _lineVid = "";
              let _pctStr = "";
              for (const _sg of segs) {
                if (!Array.isArray(_sg) || _sg.length < 2) continue;
                const _tag = _sg[1];
                const tags = Array.isArray(_tag) ? _tag : [_tag];
                if (!_lineVid) {
                  for (const t of tags) {
                    if (typeof t === "string" && t.startsWith("tx_done_")) {
                      _lineVid = t.slice("tx_done_".length);
                      break;
                    }
                  }
                }
                if (!_pctStr && tags.includes("whisper_pct")) {
                  const _m = String(_sg[0] || "").match(/(\d+)\s*%/);
                  if (_m) _pctStr = _m[1];
                }
              }
              if (_lineVid && _pctStr
                  && window._retranscribeWatchUpdateProgress) {
                window._retranscribeWatchUpdateProgress(_pctStr, _lineVid);
              }
            }
          } catch (_pe) { /* non-fatal */ }
          // Mark transcribe progress lines so they pin at the bottom of
          // the log (see post-append pin-move below). The browse-tab
          // mini-log mirrors the last N main-log lines — without
          // pinning, the active "Transcribing X%" line gets pushed up
          // by unrelated sync output and disappears from the mini-log
          // even though it's still the most relevant thing happening.
          if (_isPinToBottom(segs)) { line.dataset.pinBottom = "1"; _batchHasPinned = true; }
          const inplaceKind = _inplaceKind(segs);
          if (inplaceKind) {
            line.dataset.inplace = inplaceKind;
            // Look for the prior line of this kind in BOTH the committed
            // log AND the in-progress fragment. Without checking the
            // fragment, a single 60ms LogStreamer batch that contains
            // both a placeholder ("Adding punctuation...") and its
            // replacement ("[1/1] done") would append BOTH — the fragment
            // hasn't been flushed to `el` yet, so `el.querySelectorAll`
            // doesn't see the placeholder that arrived earlier in the
            // same batch. The frag match wins when present because it
            // is newer than anything already in `el`.
            const allInFrag = frag.querySelectorAll(
              `.log-line[data-inplace="${inplaceKind}"]`);
            const lastInFrag = allInFrag[allInFrag.length - 1];
            if (lastInFrag) {
              lastInFrag.replaceWith(line);
              continue;
            }
            const allInEl = el.querySelectorAll(
              `.log-line[data-inplace="${inplaceKind}"]`);
            const lastInEl = allInEl[allInEl.length - 1];
            if (lastInEl) {
              lastInEl.replaceWith(line);
              continue;
            }
          }
          frag.appendChild(line);
        }
        el.appendChild(frag);
        // Move every line marked `data-pin-bottom="1"` to the end of
        // `el` so they stay visually pinned at the bottom of the log
        // (and at the end of the slice the mini-log mirrors). New
        // non-pinned lines in this batch were appended above; pull the
        // pinned ones back down. Inplace-replacement still works
        // because the replacement line lands in the same DOM position,
        // and this pass re-pulls it to the end on the next batch.
        if (_batchHasPinned) {
          const _pinned = el.querySelectorAll('.log-line[data-pin-bottom="1"]');
          if (_pinned.length) {
            for (const _p of _pinned) el.appendChild(_p);
          }
        }
        // Same scroll-freeze / trim behavior as appendMainLog.
        _trimMainLog(el);
        _snapIfFollowing(el);
        // Mirror the last N main-log lines into every mini log (Subs / Browse
        // / Recent / Health / Settings). One snapshot per batch — not per line —
        // so mini logs track main exactly.
        mirrorMiniLogs();
      }
    }
    if (Array.isArray(payload.activity) && payload.activity.length) {
      const el = document.getElementById("activity-log");
      if (el) {
        // Same wiring fix as main-log above — without this the activity
        // log would auto-snap on every batch.
        wireUserScrollDetection(el);
        const frag = document.createDocumentFragment();
        // backend can lose track of alt across long sessions
        // (every emit_activity defaults alt=false). The single-append
        // path in appendActivityLog applies a parity override after
        // appendChild, but the batched path here never did — so any
        // metadata row arriving via _logBatch ended up with no
        // hist_row_alt class, breaking zebra striping after the first
        // few backend-painted rows. Track parity from the last row
        // in DOM and toggle as we append.
        // audit LG-1: also check the in-progress fragment for a row
        // with the same row_id before adding a duplicate. Without
        // this, when a [Dwnld] and its retroactive update arrive in
        // the same batch, the DOM lookup misses (new row isn't in
        // DOM yet, only in frag), and both rows are appended.
        // CSS.escape availability fallback — same pattern as the
        // clear_line handler above (audit: logs.js:899,909). Old code
        // assumed CSS.escape; on stale WebView2 builds that's
        // undefined, and any row_id-bearing activity row threw and
        // killed the rest of the batch.
        const _escRid = (s) =>
          (typeof CSS !== "undefined" && CSS.escape)
            ? CSS.escape(s) : String(s).replace(/["\\\]]/g, "\\$&");
        for (const entry of payload.activity) {
          const row = buildActivityRow(entry);
          const rid = row.dataset.rowId;
          if (rid) {
            // In-place replacement: if a prior row with this id is
            // already in the DOM, swap it rather than appending a
            // duplicate. Used by the [Dwnld] retroactive update when
            // a slow transcribe finishes after the row first emitted
            // with `0 transcribed`. Preserve the existing row's alt
            // class so the parity sequence doesn't shift.
            const existing = el.querySelector(
              `.log-line[data-row-id="${_escRid(rid)}"]`);
            if (existing) {
              const wasAlt = existing.classList.contains("hist_row_alt");
              if (wasAlt) row.classList.add("hist_row_alt");
              else row.classList.remove("hist_row_alt");
              existing.replaceWith(row);
              continue;
            }
            // Also search the fragment for a same-batch predecessor.
            const pending = frag.querySelector(
              `.log-line[data-row-id="${_escRid(rid)}"]`);
            if (pending) {
              const wasAlt = pending.classList.contains("hist_row_alt");
              if (wasAlt) row.classList.add("hist_row_alt");
              else row.classList.remove("hist_row_alt");
              pending.replaceWith(row);
              continue;
            }
          }
          // Append path — derive parity from the true previous row,
          // including same-batch rows staged in the fragment.
          _applyActivityParity(row, frag.lastElementChild || el.lastElementChild);
          frag.appendChild(row);
        }
        el.appendChild(frag);
        _trimActivityLog(el);
        _snapIfFollowing(el);
      }
    }
  };

})();
