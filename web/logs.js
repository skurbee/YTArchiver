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
    }

    // Pass 2 — prefix-family matches (shared in-place families)
    for (const t of allTags) {
      if (t.startsWith("whisper_")) return "whisper";
      if (t.startsWith("encode_")) return "encode";
      if (t.startsWith("startup_")) return "startup";
    }
    return null;
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
  window._setIndicator = function (slot, text) {
    const el = document.getElementById("browse-preload-slot-" + slot);
    if (!el) return;
    if (!text) {
      el.hidden = true;
      el.textContent = "";
    } else {
      el.hidden = false;
      el.textContent = text;
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
    [/\b([1-9]\d*)\s+(downloaded|new)\b/gi, "t-hist_green"],
    [/(\u2713)\s+(downloaded|new)\b/gi, "t-hist_green"],
    [/\b([1-9]\d*)\s+(transcribed|captions?)\b/gi, "t-hist_blue"],
    [/(\u2713)\s+(transcribed|captions?)\b/gi, "t-hist_blue"],
    [/\b([1-9]\d*)\s+(replaced|remade)\b/gi, "t-hist_redwnl"],
    [/(\u2713)\s+(replaced|remade)\b/gi, "t-hist_redwnl"],
    [/\b([1-9]\d*)\s+(compressed)\b/gi, "t-hist_compress"],
    [/(\u2713)\s+(compressed)\b/gi, "t-hist_compress"],
    [/\b([1-9]\d*)\s+(moved|reorged)\b/gi, "t-hist_reorg"],
    [/(\u2713)\s+(moved|reorged)\b/gi, "t-hist_reorg"],
    // Optional-word slot (?:\w+\s+)? catches "N IDs backfilled" or
    // "N comments refreshed" — the in-use phrase forms where a noun
    // sits between the count and the verb. Without this, those
    // phrases matched nothing and rendered in the default color
    // instead of pink.
    [/\b([1-9]\d*)\s+(?:\w+\s+)?(fetched|refreshed|metadata|backfilled)\b/gi, "t-hist_pink"],
    [/(\u2713)\s+(?:\w+\s+)?(fetched|refreshed|metadata|backfilled)\b/gi, "t-hist_pink"],
    [/\b([1-9]\d*)\s+skipped\b/gi, "t-hist_skipped"],
    [/\b([1-9]\d*)\s+errors?\b/gi, "t-hist_error"],
    // Warning-but-not-error states: "N unresolved" (ID backfill
    // couldn't match) and "N ambiguous" (title-match hit multiple
    // candidates). Amber/orange matches the "heads up, look at
    // this but it's not broken" semantic the user wanted.
    [/\b([1-9]\d*)\s+(unresolved|ambiguous)\b/gi, "t-hist_skipped"],
  ];

  function _buildHistCell(text, extra, colored, tagCls) {
    // Slow-path builder that tokenizes text against _HIST_HILITE and emits
    // per-match coloured <span>s so the plain-dark default cell shows
    // "4 downloaded" green + "2 skipped" amber + "1 error" red inline.
    const s = document.createElement("span");
    const baseCls = ((colored ? tagCls : "") + " " + (extra || "")).trim();
    if (baseCls) s.className = baseCls;
    if (text == null || text === "") return s;

    // Build an index of (start, end, cls) ranges.
    // audit F-3: always reset rx.lastIndex at the END of the loop too
    // (via try/finally). If an error thrown mid-iteration leaves
    // lastIndex dirty, the NEXT call's rx.exec starts at that stale
    // offset — highlights would land at wrong offsets and paint the
    // wrong text. Guarding with try/finally makes the reset
    // unconditional.
    const ranges = [];
    for (const [rx, cls] of _HIST_HILITE) {
      try {
        let m;
        rx.lastIndex = 0;
        while ((m = rx.exec(text)) !== null) {
          ranges.push({ start: m.index, end: m.index + m[0].length, cls });
          if (m.index === rx.lastIndex) rx.lastIndex++;
        }
      } finally {
        rx.lastIndex = 0;
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
    line.className = "log-line" + (entry.alt ? " hist_row_alt" : "") + _kindCls;
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
    line.appendChild(cell(`[${c.kind || ""}]`, "hist-col-kind", true));
    line.appendChild(cell(c.time_date || "", "hist-col-time", false));
    line.appendChild(cell("\u2014", "hist-col-dash", false));
    line.appendChild(cell(c.channel || "", "hist-col-channel", false));
    line.appendChild(cell("\u2014", "hist-col-dash", false));
    line.appendChild(cell(c.primary || "", "hist-col-num", true));
    line.appendChild(_buildHistCell(c.secondary || "", "hist-col-num", false, tagCls));
    line.appendChild(_buildHistCell(c.tertiary || "", "hist-col-num", false, tagCls));
    line.appendChild(_buildHistCell(c.errors || "", "hist-col-num", false, tagCls));
    line.appendChild(cell(c.took || "", "hist-col-took", false));
    return line;
  }

  /** Replace the whole activity log with a list of entries. */
  window.renderActivityLog = function (entries) {
    const el = document.getElementById("activity-log");
    if (!el) return;
    wireUserScrollDetection(el);
    el.innerHTML = "";
    entries.forEach((entry) => {
      el.appendChild(buildActivityRow(entry));
    });
    // First render — force scroll to bottom so the newest row is visible.
    // `maybeSnapToBottom` alone can fire before the browser has laid out
    // the new rows, leaving scrollTop at 0. Delay via rAF + one more tick.
    maybeSnapToBottom(el);
    requestAnimationFrame(() => {
      el.scrollTop = el.scrollHeight;
      setTimeout(() => { el.scrollTop = el.scrollHeight; }, 30);
    });
  };

  /** Append a single activity log entry.
   * audit E-18: mirror the main-log cap/keep behavior so the activity
   * log doesn't grow unbounded. User windows often stay open for days;
   * without a cap this DOM node accumulates until the layout engine
   * starts struggling. cap=4000 / keep=2500 gives 1500 lines of
   * headroom between trims.
   */
  window.appendActivityLog = function (entry) {
    const el = document.getElementById("activity-log");
    if (!el) return;
    wireUserScrollDetection(el);
    el.appendChild(buildActivityRow(entry));
    const cap = 4000, keep = 2500;
    if (el.childElementCount > cap) {
      const toRemove = el.childElementCount - keep;
      for (let i = 0; i < toRemove; i++) el.removeChild(el.firstChild);
      // audit LG-2: re-compute zebra-stripe parity on the kept
      // rows so the alternating background stays coherent after
      // a trim. Without this, removing an even number of rows from
      // the top is fine, but removing odd N leaves the visible
      // log with two adjacent same-background rows at the old
      // trim boundary.
      try {
        const rows = el.querySelectorAll(".log-line");
        for (let i = 0; i < rows.length; i++) {
          if (i % 2 === 1) rows[i].classList.add("hist_row_alt");
          else rows[i].classList.remove("hist_row_alt");
        }
      } catch (_e) { /* non-fatal */ }
    }
    maybeSnapToBottom(el);
  };

  // ── Mini-log mirror ─────────────────────────────────────────────────
  // Project rule: "MINI LOGS SHOULD ALWAYS SHOW EXACTLY WHAT THE MAIN LOG
  // SHOWS. NO DISCREPANCIES." Instead of maintaining a parallel append
  // buffer (which drifts when the main log removes lines in-place, e.g.
  // clearStartupLine, whisper_progress replacements), we snapshot the last
  // N lines of the main log into each mini after every mutation.
  const MINI_LOG_IDS = ["subs-mini-log", "browse-mini-log",
                        "recent-mini-log", "settings-mini-log"];
  const MINI_LINES = 5;

  function mirrorMiniLogs() {
    const main = document.getElementById("main-log");
    if (!main) return;
    const all = main.children;
    const start = Math.max(0, all.length - MINI_LINES);
    for (const id of MINI_LOG_IDS) {
      const m = document.getElementById(id);
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
        // audit F-2: strip every data-* attribute that participates
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
              || _a.name === "role") {
            _toRemove.push(_a.name);
          }
        }
        for (const _n of _toRemove) clone.removeAttribute(_n);
        m.appendChild(clone);
      }
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
    // Trim if we get ridiculous (tkinter caps ~8000 lines). keep=5000
    // gives 3000 lines of growth headroom before the next trim fires,
    // preventing the "⚠ Log trimmed" warning from chattering every
    // few seconds during heavy sync output (at keep=6000 the next
    // 2001 lines triggered another trim visibly often).
    const cap = 8000;
    const keep = 5000;
    if (el.childElementCount > cap) {
      const toRemove = el.childElementCount - keep;
      for (let i = 0; i < toRemove; i++) el.removeChild(el.firstChild);
      // Drop a one-time warning at the top of the new log window so the
      // user knows older lines got trimmed. Matches YTArchiver.py:1222
      // inline trim marker. Re-emits periodically if trimming continues.
      const existing = el.querySelector(".log-line.log-trim-warn");
      if (existing) existing.remove();
      // audit F-4: dropped the trailing \n — buildLine emits a <div>
      // per segment and the literal \n was just noise in the cell
      // (collapsed to whitespace by the default white-space rules,
      // but still inflated the character count of this one entry).
      const warn = buildLine([[" \u26A0 Log trimmed \u2014 older lines removed to keep it scrollable.", "dim"]]);
      warn.classList.add("log-trim-warn");
      el.insertBefore(warn, el.firstChild);
    }
    maybeSnapToBottom(el);
    mirrorMiniLogs();
  };

  /** Append to a mini log (by element id). NO-OP — kept for backward-compat.
   * Mini logs are now a pure mirror of the last 5 main-log lines.
   * audit E-20: kept as a no-op shim rather than removed so any legacy
   * call sites don't ReferenceError. Caller code at
   * web/app.js:7377-7380 will eventually be removed — this just
   * absorbs for now. */
  window.appendMiniLog = function () {};

  /** Clear a log container.
   * audit E-19: also reset the scrollState.userScrolled flag for the
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
        for (const segs of payload.main) {
          if (!Array.isArray(segs) || segs.length === 0) continue;
          // Control-line handling — run in order, then skip render.
          if (segs.length === 1 && Array.isArray(segs[0])
              && segs[0][1] === "__control__") {
            try {
              const data = JSON.parse(segs[0][0] || "{}");
              // audit E-22: use CSS.escape on data.marker when building
              // the selector. If a marker ever contains special CSS
              // chars (", ], \, etc.) the raw interpolation produces
              // an invalid selector and querySelectorAll throws —
              // killing the entire log batch and breaking in-place
              // replacements downstream. CSS.escape is browser-native.
              if (data.kind === "clear_line" && data.marker) {
                // Remove matching `data-inplace` elements from BOTH
                // the committed DOM AND the in-progress fragment —
                // otherwise a same-batch clear can't see the sibling
                // inplace line that was just added above it.
                const _esc = (typeof CSS !== "undefined" && CSS.escape)
                  ? CSS.escape(data.marker) : String(data.marker)
                    .replace(/["\\\]]/g, "\\$&");
                const sel = `.log-line[data-inplace="${_esc}"]`;
                frag.querySelectorAll(sel).forEach((n) => n.remove());
                el.querySelectorAll(sel).forEach((n) => n.remove());
              }
              window.dispatchEvent(new CustomEvent("yt-control", {
                detail: data,
              }));
            } catch (e) {
              console.error("control payload parse failed:", e);
            }
            continue;
          }
          const line = buildLine(segs);
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
        // Same scroll-freeze / trim behavior as appendMainLog
        const cap = 8000, keep = 5000;
        if (el.childElementCount > cap) {
          const toRemove = el.childElementCount - keep;
          for (let i = 0; i < toRemove; i++) el.removeChild(el.firstChild);
          // Inline trim warning, same pattern as appendMainLog.
          const existing = el.querySelector(".log-line.log-trim-warn");
          if (existing) existing.remove();
          // audit F-4: dropped the trailing \n — buildLine emits a <div>
      // per segment and the literal \n was just noise in the cell
      // (collapsed to whitespace by the default white-space rules,
      // but still inflated the character count of this one entry).
      const warn = buildLine([[" \u26A0 Log trimmed \u2014 older lines removed to keep it scrollable.", "dim"]]);
          warn.classList.add("log-trim-warn");
          el.insertBefore(warn, el.firstChild);
        }
        const st = scrollState.get(el) || {};
        if (!st.userScrolled) el.scrollTop = el.scrollHeight;
        // Mirror the last N main-log lines into every mini log (Subs / Browse
        // / Recent / Settings). One snapshot per batch — not per line —
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
        // audit LG-1: also check the in-progress fragment for a row
        // with the same row_id before adding a duplicate. Without
        // this, when a [Dwnld] and its retroactive update arrive in
        // the same batch, the DOM lookup misses (new row isn't in
        // DOM yet, only in frag), and both rows are appended.
        for (const entry of payload.activity) {
          const row = buildActivityRow(entry);
          const rid = row.dataset.rowId;
          if (rid) {
            // In-place replacement: if a prior row with this id is
            // already in the DOM, swap it rather than appending a
            // duplicate. Used by the [Dwnld] retroactive update when
            // a slow transcribe finishes after the row first emitted
            // with `0 transcribed`.
            const existing = el.querySelector(
              `.log-line[data-row-id="${CSS.escape(rid)}"]`);
            if (existing) { existing.replaceWith(row); continue; }
            // Also search the fragment for a same-batch predecessor.
            const pending = frag.querySelector(
              `.log-line[data-row-id="${CSS.escape(rid)}"]`);
            if (pending) { pending.replaceWith(row); continue; }
          }
          frag.appendChild(row);
        }
        el.appendChild(frag);
        const st = scrollState.get(el) || {};
        if (!st.userScrolled) el.scrollTop = el.scrollHeight;
      }
    }
  };

  /** Render the Subs tab channel table. */
  window.renderSubsTable = function (rows, totalLabel) {
    const tbody = document.getElementById("subs-table-body");
    if (!tbody) return;
    window._subsAllRows = rows; // keep a copy for the filter
    _renderSubsFiltered(rows);
    const totalEl = document.getElementById("subs-total-size");
    if (totalEl && totalLabel) totalEl.textContent = totalLabel;
    // Re-apply the Avg column visibility in case this is the first render
    // (class would otherwise only be applied on Settings open / change).
    // Default undefined -> show, matching legacy behavior.
    const tbl = document.getElementById("subs-table");
    if (tbl && window._subsShowAvg === false) tbl.classList.add("hide-avg-col");
    // feature F7: the new tbody rows have no row-selected classes, so
    // the bulk-actions bar should hide itself on re-render. Invoke the
    // bar updater if it's been wired.
    const bar = document.getElementById("subs-bulk-bar");
    if (bar) bar.hidden = true;
  };

  function _renderSubsFiltered(rows) {
    const tbody = document.getElementById("subs-table-body");
    if (!tbody) return;
    tbody.innerHTML = "";
    const frag = document.createDocumentFragment();
    for (const r of rows) {
      const tr = document.createElement("tr");
      // Per-channel "Sync now" is accessed via right-click → Sync now
      // (matches the original tkinter version). No inline hover button.
      // A chartreuse dot after the channel name flags an unfinished
      // resolution redownload (`_redownload_progress.json` present).
      const dot = r._pending_redownload
        ? ' <span class="sub-redwnl-dot" title="Unfinished redownload">\u25CF</span>'
        : "";
      // Stash pending-redownload metadata on the TR so the right-click
      // menu in app.js can switch "Redownload at..." to "Continue
      // Redownload at 480p" without a backend roundtrip. ALSO stash
      // the clean channel name because `.col-folder` now contains
      // the name PLUS the dot indicator span, so `.textContent`
      // reads "Channel Name ●" instead of just "Channel Name",
      // which made `chan_redownload({name: "Channel Name ●"}, ...)`
      // fail the name lookup silently.
      tr.dataset.channelName = r.folder || "";
      if (r._pending_redownload) {
        tr.dataset.pendingRedownload = "1";
        if (r._redownload_res) tr.dataset.redownloadRes = r._redownload_res;
      }
      tr.innerHTML = `
        <td class="col-folder">${escapeHtml(r.folder)}${dot}</td>
        <td>${escapeHtml(r.res)}</td>
        <td>${escapeHtml(r.min)}</td>
        <td>${escapeHtml(r.max)}</td>
        <td class="col-mark">${escapeHtml(r.compress)}</td>
        <td class="col-mark">${escapeHtml(r.transcribe)}</td>
        <td class="col-mark">${escapeHtml(r.metadata)}</td>
        <td>${escapeHtml(r.last_sync)}</td>
        <td>${escapeHtml(r.n_vids)}</td>
        <td>${escapeHtml(r.size)}</td>
        <td>${escapeHtml(r.avg_size || "—")}</td>
      `;
      frag.appendChild(tr);
    }
    tbody.appendChild(frag);
  }

  window._applySubsFilter = function (query) {
    const all = window._subsAllRows || [];
    const q = (query || "").toLowerCase().trim();
    if (!q) { _renderSubsFiltered(all); return; }
    const filtered = all.filter(r => (r.folder || "").toLowerCase().includes(q));
    _renderSubsFiltered(filtered);
  };

  // Toggle visibility of the Avg filesize column on the Subs table.
  // Wired from Settings ("Show Avg filesize in subs tab"). CSS handles
  // the actual hide via `.hide-avg-col th:last-child, .hide-avg-col td:last-child`.
  // Cached on window._subsShowAvg so future renders respect the choice.
  window._applySubsAvgVisibility = function (show) {
    window._subsShowAvg = !!show;
    const tbl = document.getElementById("subs-table");
    if (!tbl) return;
    tbl.classList.toggle("hide-avg-col", !show);
  };

  /** Render the Recent tab downloads. Dispatches between the legacy
   * table ("list") and the thumbnail grid ("grid") based on the
   * user's setting (cached on window._recentViewMode, set by
   * window._applyRecentViewMode — default "list"). */
  window.renderRecentTable = function (rows) {
    window._recentAllRows = rows || [];
    _dispatchRecent(window._recentAllRows);
  };

  window._applyRecentFilter = function (query) {
    const all = window._recentAllRows || [];
    const q = (query || "").toLowerCase().trim();
    const filtered = !q ? all : all.filter(r =>
      (r.title || "").toLowerCase().includes(q) ||
      (r.channel || "").toLowerCase().includes(q)
    );
    _dispatchRecent(filtered);
  };

  // Flip which view is visible + pick the matching renderer. Called at
  // boot via the runtime_info seed and live whenever the Settings radio
  // changes. Remembers the choice on window so filter re-applies can
  // dispatch without needing to re-read the setting each time.
  window._applyRecentViewMode = function (mode) {
    const m = (mode === "grid") ? "grid" : "list";
    window._recentViewMode = m;
    const listFrame = document.getElementById("recent-list-frame");
    const gridFrame = document.getElementById("recent-grid-frame");
    if (listFrame) listFrame.style.display = (m === "list") ? "" : "none";
    if (gridFrame) gridFrame.style.display = (m === "grid") ? "" : "none";
    // Re-render the current dataset through the newly-active view so
    // switching modes doesn't leave the other view empty.
    if (Array.isArray(window._recentAllRows)) {
      _dispatchRecent(window._recentAllRows);
    }
  };

  function _dispatchRecent(rows) {
    if (window._recentViewMode === "grid") {
      _renderRecentFilteredGrid(rows);
    } else {
      _renderRecentFiltered(rows);
    }
  }

  function _renderRecentFiltered(rows) {
    const tbody = document.getElementById("recent-table-body");
    if (!tbody) return;
    tbody.innerHTML = "";
    const frag = document.createDocumentFragment();
    for (const r of rows) {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td class="col-title" title="${escapeAttr(r.title)}">${escapeHtml(r.title)}</td>
        <td class="col-channel">${escapeHtml(r.channel)}</td>
        <td class="col-time">${escapeHtml(r.time)}</td>
        <td class="col-length right">${escapeHtml(r.duration)}</td>
        <td class="col-size right">${escapeHtml(r.size)}</td>
      `;
      // Stash the full row data so handlers can reach filepath/video_id/etc.
      tr.dataset.filepath = r.filepath || "";
      tr.dataset.videoId = r.video_id || "";
      tr.dataset.title = r.title || "";
      tr.dataset.channel = r.channel || "";
      tr.title = "Double-click to play in Watch view";
      // Double-click opens the video in the embedded Watch view with transcript
      tr.addEventListener("dblclick", (e) => {
        e.preventDefault();
        const v = {
          title: tr.dataset.title,
          channel: tr.dataset.channel,
          filepath: tr.dataset.filepath,
          video_id: tr.dataset.videoId,
          duration: r.duration || "",
          uploaded: r.time || "",
        };
        // Switch to Browse tab + Watch view (call the helper in app.js).
        if (typeof window._openVideoInWatch === "function") {
          window._openVideoInWatch(v);
        } else if (v.filepath && window.pywebview?.api?.browse_open_video) {
          // Fallback — external player
          window.pywebview.api.browse_open_video(v.filepath);
        }
      });
      frag.appendChild(tr);
    }
    tbody.appendChild(frag);

    // Row click selection — supports Ctrl/Shift multi-select
    const allRows = [...tbody.querySelectorAll("tr")];

    // Show/hide Clear list + Delete File buttons based on rows
    const hasItems = allRows.length > 0;
    const clearBtn = document.getElementById("btn-clear-recent");
    const delBtn = document.getElementById("btn-delete-file");
    if (clearBtn) clearBtn.style.display = hasItems ? "" : "none";
    if (delBtn) delBtn.style.display = "none"; // revealed when a row is selected

    let lastClickedIdx = -1;
    allRows.forEach((tr, idx) => {
      tr.addEventListener("click", (e) => {
        if (e.ctrlKey || e.metaKey) {
          // Toggle this row's selection
          tr.classList.toggle("row-selected");
        } else if (e.shiftKey && lastClickedIdx >= 0) {
          // Range-select from last clicked to this one
          const [a, b] = [Math.min(lastClickedIdx, idx), Math.max(lastClickedIdx, idx)];
          allRows.forEach((r, i) => {
            if (i >= a && i <= b) r.classList.add("row-selected");
          });
        } else {
          allRows.forEach(r => r.classList.remove("row-selected"));
          tr.classList.add("row-selected");
        }
        lastClickedIdx = idx;
        const any = tbody.querySelectorAll("tr.row-selected").length;
        if (delBtn) {
          delBtn.style.display = any ? "" : "none";
          delBtn.textContent = any > 1 ? `Delete ${any} files` : "Delete File";
        }
      });
    });
  };

  // ─── Recent grid renderer ───────────────────────────────────────────
  //
  // Thumbnail-card view of recent downloads — visually matches the video
  // grid inside a channel (same _buildVideoCard helper). Selection isn't
  // exposed on cards (no multi-select) because the Browse grid doesn't
  // have it either; users who need bulk-delete can flip back to List.
  //
  // Click behavior mirrors the Browse grid:
  // - single-click → open in Watch view (embedded player + transcript)
  // - double-click → open in external player (VLC / system default)
  //
  // The Delete File button depends on selection and is therefore hidden
  // in this mode; Clear list remains functional.
  function _renderRecentFilteredGrid(rows) {
    const grid = document.getElementById("recent-grid");
    if (!grid) return;
    grid.innerHTML = "";
    // _buildVideoCard was made public earlier in this file; fall back to
    // a plain-text row if somehow it's missing so the view isn't blank.
    const build = window._buildVideoCard;
    const frag = document.createDocumentFragment();
    for (const r of rows) {
      const v = {
        title: r.title || "",
        channel: r.channel || "",
        filepath: r.filepath || "",
        video_id: r.video_id || "",
        duration: r.duration || "",
        uploaded: r.uploaded || r.time || "",
        size_bytes: r.size_bytes || 0,
        thumbnail_url: r.thumbnail_url || "",
        // Recent is a cross-channel view — force the channel line on so
        // every card identifies its channel (Browse cards don't need it).
        show_channel: true,
      };
      if (!build) {
        const d = document.createElement("div");
        d.className = "video-card";
        d.textContent = v.title;
        frag.appendChild(d);
        continue;
      }
      // onVideoClick → open Watch view (same path the list view uses on dblclick).
      const onClick = (vv) => {
        if (typeof window._openVideoInWatch === "function") {
          window._openVideoInWatch(vv);
        } else if (vv.filepath && window.pywebview?.api?.browse_open_video) {
          window.pywebview.api.browse_open_video(vv.filepath);
        }
      };
      const card = build(v, onClick);
      frag.appendChild(card);
    }
    grid.appendChild(frag);

    // Button visibility — Clear list follows "has items", Delete File is
    // hidden in grid mode since there's no card-selection UX.
    const hasItems = rows.length > 0;
    const clearBtn = document.getElementById("btn-clear-recent");
    const delBtn = document.getElementById("btn-delete-file");
    if (clearBtn) clearBtn.style.display = hasItems ? "" : "none";
    if (delBtn) delBtn.style.display = "none";
  }

  /** Render the queue popovers for Sync Tasks + GPU Tasks. */
  window.renderQueues = function (queues) {
    renderTaskList("sync-tasks-body", queues.sync, "No sync tasks queued.", "sync");
    renderTaskList("gpu-tasks-body", queues.gpu, "No GPU tasks queued.", "gpu");
    _updateBadge("badge-sync", (queues.sync || []).length);
    _updateBadge("badge-gpu", (queues.gpu || []).length);
  };

  function _updateBadge(id, n) {
    const el = document.getElementById(id);
    if (!el) return;
    if (!n || n <= 0) {
      el.hidden = true;
      el.textContent = "0";
      return;
    }
    el.hidden = false;
    el.textContent = n > 99 ? "99+" : String(n);
  }

  // In-memory queue state so drag-to-rearrange can update order.
  const _queueState = { sync: [], gpu: [] };
  // Exposed so context menus elsewhere (Subs tab) can check whether a
  // channel is currently queued / running and label menu items dynamically.
  // Mirrors OLD's dynamic-label mutation (YTArchiver.py:5596 _chan_ctx_menu).
  window._queueStateSnapshot = () => ({
    sync: _queueState.sync.slice(),
    gpu: _queueState.gpu.slice(),
  });
  // Convenience: does `channelName` have a sync queued? (running or queued)
  window._queueHasSyncForChannel = (channelName) => {
    const n = (channelName || "").toLowerCase();
    if (!n) return null;
    const match = (t) => {
      const s = String(t?.name || t?.url || "").toLowerCase();
      return s.includes(n) ? t.status || "queued" : null;
    };
    for (const t of _queueState.sync) {
      const s = match(t);
      if (s) return s; // "running" | "queued"
    }
    return null;
  };
  // Convenience: does a GPU task (transcribe/encode/compress) reference this channel?
  window._queueHasGpuForChannel = (channelName) => {
    const n = (channelName || "").toLowerCase();
    if (!n) return null;
    for (const t of _queueState.gpu) {
      const s = String(t?.name || t?.title || "").toLowerCase();
      if (s.includes(n)) return t.status || "queued";
    }
    return null;
  };

  function renderTaskList(bodyId, list, emptyText, queueKind) {
    const body = document.getElementById(bodyId);
    if (!body) return;
    _queueState[queueKind] = (list || []).slice();
    paintTaskList(body, _queueState[queueKind], emptyText, queueKind);
  }

  function paintTaskList(body, list, emptyText, queueKind) {
    body.innerHTML = "";
    if (!list || list.length === 0) {
      body.innerHTML = `<div class="queue-empty">${emptyText}</div>`;
      return;
    }
    list.forEach((t, i) => {
      const row = document.createElement("div");
      const statusCls = t.status || "queued";
      row.className = `queue-task-row ${statusCls}`;
      row.draggable = true;
      row.dataset.idx = i;
      row.dataset.queue = queueKind;

      const stateGlyph =
        statusCls === "running" ? "\u25B6" :
        statusCls === "paused" ? "\u275A\u275A" :
                                  "\u25CB";

      // Color the verb (Downloading/Transcribing/Metadata) in tag color
      const nameHtml = colorizeTaskName(t.name);

      // Cycling dots after the active task's name ("..."/".. "/". ") —
      // pure CSS animation via ::after content keyframes. Matches
      // YTArchiver.py:20131 _active_label cycling dots.
      const dotsSpan = statusCls === "running" ? '<span class="queue-task-dots"></span>' : "";

      // X button hidden for the running row — that item lives in
      // current_sync / current_gpu, NOT in queues.sync / queues.gpu.
      // An index-based delete on the running row would silently drop
      // the next-queued item (the one that visually slid up to slot 0
      // after the running row's translation). For running rows the
      // user should use the right-click context menu's Skip / Cancel
      // actions instead.
      const closeBtnHtml = statusCls === "running"
        ? ""
        : '<button class="queue-task-close" title="Remove">&times;</button>';

      row.innerHTML = `
        <span class="queue-task-index">${i + 1}.</span>
        <span class="queue-task-state ${statusCls}">${stateGlyph}</span>
        <span class="queue-task-name"></span>${dotsSpan}
        ${closeBtnHtml}
      `;
      row.querySelector(".queue-task-name").innerHTML = nameHtml;

      row.querySelector(".queue-task-close")?.addEventListener("click", (e) => {
        e.stopPropagation();
        const popoverIdx = Number(row.dataset.idx);
        const removed = _queueState[queueKind][popoverIdx];
        // The X is a per-ROW action. Translate popover index ->
        // backend queue index (the popover prepends current_sync /
        // current_gpu as the running row, so any 'running' rows
        // before our position need to be subtracted off — that
        // position doesn't exist in the backend's queues list).
        let runningBefore = 0;
        for (let j = 0; j < popoverIdx; j++) {
          if ((_queueState[queueKind][j] || {}).status === "running") {
            runningBefore++;
          }
        }
        const queueIdx = popoverIdx - runningBefore;
        _queueState[queueKind].splice(popoverIdx, 1);
        paintTaskList(body, _queueState[queueKind], emptyText, queueKind);
        // Original code passed only a URL / path, which deleted EVERY
        // queue entry sharing that identifier (e.g. one X click on a
        // metadata-refresh row also dropped the download row for the
        // same channel because both shared the channel URL).
        // Fix: prefer the index-based remove API (queues_*_remove_at)
        // with identity guard. Falls back to the legacy URL-based API
        // only on backends that don't expose the new method.
        if (!window.pywebview?.api || !removed) return;
        const api = window.pywebview.api;
        if (queueKind === "sync") {
          if (api.queues_sync_remove_at) {
            api.queues_sync_remove_at(queueIdx,
              removed.url || "",
              removed.channel_name || removed.name || "");
          } else if (api.queues_sync_remove) {
            api.queues_sync_remove(removed.url || removed.channel_name
                                    || removed.name || "");
          }
        } else if (queueKind === "gpu") {
          // Coalesced "Transcribe {ch} (N videos)" row → bulk-remove
          // (drop all siblings). Single rows use index-based API.
          const isBulk = !!removed.bulk_id && (removed.bulk_count || 0) > 1;
          if (isBulk && api.queues_gpu_remove_bulk) {
            api.queues_gpu_remove_bulk(removed.bulk_id);
          } else if (api.queues_gpu_remove_at) {
            api.queues_gpu_remove_at(queueIdx,
              removed.path || "",
              removed.bulk_id || "");
          } else if (api.queues_gpu_remove) {
            api.queues_gpu_remove(removed.path || removed.bulk_id
                                   || removed.id || removed.name || "");
          }
        }
      });

      // Right-click menu on queue rows: skip / move-to-top / cancel-or-remove
      // Mirrors YTArchiver.py:20570-20584 (sync) + 21441-21455 (gpu) — each
      // destructive action pops a confirm, matching the old app's askyesno flow.
      row.addEventListener("contextmenu", (ev) => {
        ev.preventDefault();
        ev.stopPropagation();
        const idx = Number(row.dataset.idx);
        const api = window.pywebview?.api;
        const items = [];
        const taskLabel = (t.name || t.title || t.url || "this task").toString().slice(0, 60);
        // "Skip this job" — only meaningful for the currently-running item.
        // Old app: messagebox.askyesno("Skip Current", "Cancel the current job
        // and move to the next one in queue?")
        if (statusCls === "running") {
          items.push({ label: "Skip this job",
            action: async () => {
              const ok = await (window.askConfirm
                ? window.askConfirm("Skip current job",
                    "Cancel the current job and move to the next one in queue?",
                    { confirm: "Skip", danger: true })
                : Promise.resolve(confirm(
                    "Cancel the current job and move to the next one in queue?")));
              if (!ok) return;
              if (queueKind === "sync") api?.sync_skip_current?.();
              else api?.gpu_skip_current?.();
            }});
        }
        items.push(
          { label: "Move to top",
            action: () => {
              if (idx <= 0) return;
              const [taken] = _queueState[queueKind].splice(idx, 1);
              _queueState[queueKind].unshift(taken);
              paintTaskList(body, _queueState[queueKind], emptyText, queueKind);
              if (queueKind === "sync" && api?.queues_sync_reorder)
                api.queues_sync_reorder(taken?.url || taken?.name || "", 0);
              else if (queueKind === "gpu" && api?.queues_gpu_reorder)
                api.queues_gpu_reorder(taken?.id || taken?.path || taken?.name || "", 0);
            }},
          // Old app: messagebox.askyesno("Remove from Queue", ...) line 20382
          { label: statusCls === "running" ? "Cancel task" : "Remove from queue",
            cls: "danger",
            action: async () => {
              const title = statusCls === "running" ? "Cancel task" : "Remove from queue";
              const msg = statusCls === "running"
                ? `Cancel "${taskLabel}"?\n\nThe current job will stop.`
                : `Remove "${taskLabel}" from the queue?`;
              const ok = await (window.askConfirm
                ? window.askConfirm(title, msg, { confirm: title, danger: true })
                : Promise.resolve(confirm(msg)));
              if (!ok) return;
              row.querySelector(".queue-task-close")?.click();
            }},
        );
        if (window.showContextMenu) window.showContextMenu(ev.clientX, ev.clientY, items);
      });

      // Drag-and-drop (HTML5)
      row.addEventListener("dragstart", (e) => {
        row.classList.add("drag-src");
        e.dataTransfer.effectAllowed = "move";
        e.dataTransfer.setData("text/plain", String(i));
      });
      row.addEventListener("dragend", () => {
        row.classList.remove("drag-src");
        body.querySelectorAll(".drag-target-above, .drag-target-below")
            .forEach(el => el.classList.remove("drag-target-above", "drag-target-below"));
      });
      row.addEventListener("dragover", (e) => {
        e.preventDefault();
        const rect = row.getBoundingClientRect();
        const halfway = rect.top + rect.height / 2;
        row.classList.toggle("drag-target-above", e.clientY < halfway);
        row.classList.toggle("drag-target-below", e.clientY >= halfway);
      });
      row.addEventListener("dragleave", () => {
        row.classList.remove("drag-target-above", "drag-target-below");
      });
      row.addEventListener("drop", (e) => {
        e.preventDefault();
        const srcIdx = Number(e.dataTransfer.getData("text/plain"));
        const dstIdx = Number(row.dataset.idx);
        if (Number.isNaN(srcIdx) || srcIdx === dstIdx) return;
        const rect = row.getBoundingClientRect();
        const below = e.clientY >= rect.top + rect.height / 2;
        const list = _queueState[queueKind];
        const [moved] = list.splice(srcIdx, 1);
        let insertAt = dstIdx;
        if (srcIdx < dstIdx) insertAt -= 1;
        if (below) insertAt += 1;
        list.splice(insertAt, 0, moved);
        paintTaskList(body, list, emptyText, queueKind);
        // Phase 6: notify Python of reorder
      });

      body.appendChild(row);
    });
  }

  function colorizeTaskName(name) {
    // Color the action verb in its tag color — mirrors YTArchiver's
    // log palette so Downloading=green, Metadata=pink, Transcribing=blue,
    // Redownloading=chartreuse, Encoding/Compressing=purple, Moving/Reorg=orange.
    // (Hex values live in styles.css as var(--c-log-*).)
    // Both present-continuous (running) and plain-verb (queued) forms.
    // Longer verbs listed first so "Redownloading" isn't matched by "Download".
    const verbs = [
      ["Redownloading", "qv-redwnl"], // chartreuse #c7e64f
      ["Redownload", "qv-redwnl"],
      ["Downloading", "qv-sync"], // green #3dd68c
      ["Download", "qv-sync"],
      ["Transcribing", "qv-trans"], // blue #6cb4ee
      ["Transcribe", "qv-trans"],
      ["Metadata", "qv-meta"], // pink #e87aac
      ["Compressing", "qv-compress"], // purple #c084fc
      ["Compress", "qv-compress"],
      ["Encoding", "qv-compress"],
      ["Encode", "qv-compress"],
      ["Moving", "qv-reorg"], // orange #ff8c42
      ["Reorg", "qv-reorg"],
      ["Syncing", "qv-sync"],
      ["Sync", "qv-sync"],
    ];
    for (const [verb, cls] of verbs) {
      if (name.startsWith(verb)) {
        const rest = name.slice(verb.length);
        return `<span class="${cls}">${escapeHtml(verb)}</span>${escapeHtml(rest)}`;
      }
    }
    return escapeHtml(name);
  }

  /** Legacy tree renderer — no-op since Browse switched to the channel
      grid layout. Kept so existing call sites don't error. */
  window.renderBrowseTree = function (_rows) { /* no-op */ };

  // ─── Browse tab: YouTube-style 3-view flow ───────────────────────────

  // Deterministic gradient from a string (first letter → hue).
  function gradientFor(name) {
    const s = (name || "").trim();
    const first = (s[0] || "?").toUpperCase();
    const codepoint = first.charCodeAt(0);
    const hue = (codepoint * 47) % 360;
    const hue2 = (hue + 40) % 360;
    return `linear-gradient(135deg, hsl(${hue}, 55%, 28%) 0%, hsl(${hue2}, 60%, 18%) 100%)`;
  }

  /** Render the Channels grid (Browse tab landing view).
   *
   * YouTube-style: banner fills the card, circular PFP overlaps
   * bottom-left, name + stats in a dark-gradient overlay below.
   * Images use <img loading="lazy"> so 100+ channels don't fetch all
   * thumbnails on mount (fixes scroll lag the user reported).
   */
  window.renderChannelGrid = function (channels, onChannelClick) {
    const grid = document.getElementById("channel-grid");
    if (!grid) return;
    grid.innerHTML = "";
    const frag = document.createDocumentFragment();
    for (const c of channels) {
      const name = c.folder || c.name || "";
      const vids = c.n_vids || c.video_count || "—";
      const size = c.size || "";
      const first = (name[0] || "?").toUpperCase();

      const bannerUrl = c.banner_url || "";
      const avatarUrl = c.avatar_url || "";
      // Banner priority: explicit banner > avatar zoomed-to-fill > gradient.
      const bannerSrc = bannerUrl || avatarUrl || "";

      const card = document.createElement("div");
      card.className = "channel-card";
      if (!bannerSrc) {
        // Pure-gradient fallback gets the tinted bg directly.
        card.style.background = gradientFor(name);
      }
      if (typeof c.transcription_pending === "number") {
        card.dataset.pendingTx = String(c.transcription_pending);
      }
      if (typeof c.metadata_pending === "number") {
        card.dataset.pendingMeta = String(c.metadata_pending);
      }

      const bgImg = bannerSrc
        ? `<img class="channel-card-bg" src="${bannerSrc}" loading="lazy" decoding="async" alt="" />`
        : "";
      const avatarImg = avatarUrl
        ? `<img class="channel-avatar" src="${avatarUrl}" loading="lazy" decoding="async" alt="" />`
        : "";
      const letterHtml = (!bannerSrc && !avatarUrl)
        ? `<div class="channel-letter">${escapeHtml(first)}</div>`
        : "";

      card.innerHTML = `
        ${bgImg}
        ${letterHtml}
        <div class="channel-card-overlay">
          <div class="channel-card-name"></div>
          <div class="channel-card-meta"></div>
        </div>
        ${avatarImg}
      `;
      card.querySelector(".channel-card-name").textContent = name;
      card.querySelector(".channel-card-meta").textContent =
        `${vids}${vids && vids !== "—" ? " videos" : ""}${size ? " \u00b7 " + size : ""}`;

      // Swap to gradient if the banner image fails to load.
      const bgEl = card.querySelector(".channel-card-bg");
      if (bgEl) {
        bgEl.addEventListener("error", () => {
          bgEl.remove();
          card.style.background = gradientFor(name);
          if (!avatarUrl && !card.querySelector(".channel-letter")) {
            const d = document.createElement("div");
            d.className = "channel-letter";
            d.textContent = first;
            card.insertBefore(d, card.firstChild);
          }
        }, { once: true });
      }

      card.addEventListener("click", () => onChannelClick && onChannelClick(c));
      frag.appendChild(card);
    }
    grid.appendChild(frag);

    // Prefetch banner + avatar images in the background, throttled to a
    // few parallel fetches at a time. By the time the user scrolls past
    // the first viewport, the later cards' images are already in the
    // browser cache and decoded — no more pop-in. Runs on idle time so
    // it never blocks main-thread scrolling.
    _prefetchChannelArt(channels);
  };

  let _prefetchQueue = [];
  let _prefetchActive = 0;
  // 2 concurrent is enough \u2014 more than this stacks decode work on
  // the main thread and creates the very lag we're trying to prevent.
  const PREFETCH_MAX_CONCURRENT = 2;
  // Only prefetch enough cards to fill the first couple of viewports.
  // Past that, let `<img loading="lazy">` pull the rest on demand as
  // the user scrolls. Prefetching all 100+ banners up-front floods the
  // decoder and makes scroll janky for the first 10-20 seconds.
  const PREFETCH_LIMIT = 40;

  function _prefetchChannelArt(channels) {
    // audit E-21: explicitly reset the module-level queue at the
    // top so a second renderChannelGrid call doesn't accidentally
    // append (safe no-op today — the assignment below replaces —
    // but defensive against any future refactor that changes the
    // assignment to a push).
    _prefetchQueue = [];
    const first = channels.slice(0, PREFETCH_LIMIT);
    const urls = [];
    for (const c of first) {
      if (c.banner_url) urls.push(c.banner_url);
    }
    for (const c of first) {
      if (c.avatar_url && c.avatar_url !== c.banner_url) urls.push(c.avatar_url);
    }
    _prefetchQueue = urls;
    for (let i = 0; i < PREFETCH_MAX_CONCURRENT; i++) _pumpPrefetch();
  }

  function _pumpPrefetch() {
    if (!_prefetchQueue.length) return;
    if (_prefetchActive >= PREFETCH_MAX_CONCURRENT) return;
    const url = _prefetchQueue.shift();
    if (!url) return;
    _prefetchActive++;
    const img = new Image();
    img.decoding = "async";
    img.fetchPriority = "low";
    const done = () => {
      _prefetchActive--;
      // Schedule the next pump inside an idle callback so decodes
      // only run when the main thread isn't busy rendering scrolls.
      const next = () => _pumpPrefetch();
      if (typeof requestIdleCallback === "function") {
        requestIdleCallback(next, { timeout: 500 });
      } else {
        setTimeout(next, 100);
      }
    };
    img.addEventListener("load", done, { once: true });
    img.addEventListener("error", done, { once: true });
    img.src = url;
  }

  /** Render the Videos grid (inside a channel). */
  function _buildVideoCard(v, onVideoClick) {
    const card = document.createElement("div");
    card.className = "video-card";
    card.dataset.filepath = v.filepath || "";
    card.dataset.videoId = v.video_id || "";
    card.dataset.title = v.title || "";
    // Use a real <img> tag for thumbnails rather than CSS
    // `background: url(...)` — the image tag is far more forgiving of
    // http://127.0.0.1 URLs through pywebview's webview, and failed loads
    // trigger a clean `onerror` swap to the gradient placeholder.
    const hasThumb = !!v.thumbnail_url;
    const imgTag = hasThumb
      ? `<img class="video-thumb-img" src="${v.thumbnail_url}" alt=""
              loading="lazy" decoding="async" />`
      : "";
    card.innerHTML = `
      <div class="video-thumb" style="${hasThumb ? '' : `background: ${gradientFor(v.title)};`}">
        ${imgTag}
        ${hasThumb ? '' : '<span>&#9654;</span>'}
        <span class="video-duration-badge"></span>
      </div>
      <div class="video-card-body">
        <div class="video-card-title"></div>
        <div class="video-card-channel"></div>
        <div class="video-card-meta"></div>
      </div>
    `;
    // Swap to gradient placeholder if the thumb fails to load
    const imgEl = card.querySelector(".video-thumb-img");
    if (imgEl) {
      imgEl.addEventListener("error", () => {
        const tf = imgEl.parentElement;
        if (tf) {
          tf.style.background = gradientFor(v.title);
          imgEl.remove();
          const ph = document.createElement("span");
          ph.innerHTML = "&#9654;";
          tf.insertBefore(ph, tf.firstChild);
        }
      }, { once: true });
    }
    card.querySelector(".video-duration-badge").textContent = v.duration || "";
    card.querySelector(".video-card-title").textContent = v.title || "";
    // Channel line — opt-in via v.show_channel so contexts like the
    // Recent grid (many channels mixed together) get it, while the
    // Browse video grid (already scoped to one channel) doesn't show
    // a redundant channel name on every card.
    const chEl = card.querySelector(".video-card-channel");
    if (chEl) {
      if (v.show_channel && v.channel) {
        chEl.textContent = v.channel;
        chEl.style.display = "";
      } else {
        chEl.style.display = "none";
      }
    }
    const metaParts = [];
    // Pretty-print the upload date as "Nov 15, 2025" instead of the raw
    // "2025-11-15" that the backend emits. Accepts any ISO-ish YYYY-MM-DD
    // or date-time string; unparseable values fall back to verbatim.
    if (v.uploaded) metaParts.push(_fmtCardDate(v.uploaded));
    if (v.size_bytes) metaParts.push(_fmtBytes(v.size_bytes));
    if (v.views) metaParts.push(v.views + " views");
    if (v.tx_status === "transcribed") metaParts.push("transcribed");
    card.querySelector(".video-card-meta").textContent = metaParts.join(" \u00b7 ");
    card.addEventListener("click", () => onVideoClick && onVideoClick(v));

    // (Hover-enlarge preview removed in v49.7 — intrusive on a dense
    // grid; covered the adjacent cards and interrupted normal browsing
    // flow without adding real information.)

    card.addEventListener("dblclick", (e) => {
      e.preventDefault();
      e.stopPropagation();
      if (v.filepath && window.pywebview?.api?.browse_open_video) {
        window.pywebview.api.browse_open_video(v.filepath);
      }
    });
    return card;
  }
  // Public alias so other renderers (e.g. renderRecentGrid) can reuse the
  // same card builder without duplicating markup + hover-preview logic.
  window._buildVideoCard = _buildVideoCard;

  function _yearOf(v) {
    // Prefer upload_ts (ms epoch from added_ts*1000); fall back to r.year
    if (typeof v.upload_ts === "number" && v.upload_ts > 0) {
      return new Date(v.upload_ts).getFullYear();
    }
    if (v.year) return Number(v.year);
    // Last resort: try to parse 'uploaded' (e.g. "2024-05-20")
    const m = (v.uploaded || "").match(/\b(19|20)\d{2}\b/);
    return m ? Number(m[0]) : null;
  }

  window.renderVideoGrid = function (videos, onVideoClick, opts) {
    const grid = document.getElementById("video-grid");
    if (!grid) return;
    grid.innerHTML = "";
    const groupByYear = !!(opts && opts.groupByYear);
    // `groupByMonth` only makes sense nested inside `groupByYear`;
    // without a year context, "March" alone is ambiguous across years.
    // Automatically treat month-grouping as implying year-grouping.
    const groupByMonth = !!(opts && opts.groupByMonth);
    const useYear = groupByYear || groupByMonth;

    // Lazy-load: on channels with 500+ videos, rendering all thumbnails at
    // once causes jank + high memory. Match YTArchiver.py:28108 (fires at
    // 85% scroll) by batching 60 cards per append cycle with an
    // IntersectionObserver sentinel.
    // Mirrors OLD `_grid_check_load_more` / `_grid_build_cards(reset=False)`.
    const BATCH = 60;
    const LAZY_THRESHOLD = 120; // only lazy-load when enough videos to matter
    const useLazy = !useYear && videos.length > LAZY_THRESHOLD;

    if (useLazy) {
      grid.classList.remove("video-grid-grouped");
      let cursor = 0;
      const appendBatch = () => {
        const end = Math.min(cursor + BATCH, videos.length);
        const frag = document.createDocumentFragment();
        for (let i = cursor; i < end; i++) {
          frag.appendChild(_buildVideoCard(videos[i], onVideoClick));
        }
        // Remove old sentinel if present, append batch, add new sentinel
        const oldSentinel = grid.querySelector(".video-grid-sentinel");
        oldSentinel?.remove();
        grid.appendChild(frag);
        cursor = end;
        if (cursor < videos.length) {
          const sentinel = document.createElement("div");
          sentinel.className = "video-grid-sentinel";
          sentinel.textContent = `\u2026 ${videos.length - cursor} more, scroll to load`;
          grid.appendChild(sentinel);
          _observeGridSentinel(sentinel, appendBatch);
        }
      };
      appendBatch();
      return;
    }

    if (!useYear) {
      grid.classList.remove("video-grid-grouped");
      const frag = document.createDocumentFragment();
      for (const v of videos) frag.appendChild(_buildVideoCard(v, onVideoClick));
      grid.appendChild(frag);
      return;
    }

    // Group in the order they arrive (caller already sorted).
    const buckets = new Map(); // year -> array of videos
    const order = []; // year order of first occurrence
    const unknown = [];
    for (const v of videos) {
      const y = _yearOf(v);
      if (y == null) { unknown.push(v); continue; }
      if (!buckets.has(y)) { buckets.set(y, []); order.push(y); }
      buckets.get(y).push(v);
    }
    if (unknown.length) { buckets.set("?", unknown); order.push("?"); }

    // Render: grid becomes a single column of <section>s each containing its
    // own internal grid of cards. CSS handles the visual gaps.
    grid.classList.add("video-grid-grouped");
    const frag = document.createDocumentFragment();
    for (const y of order) {
      const vids = buckets.get(y);
      const section = document.createElement("section");
      section.className = "video-grid-year-section";
      section.dataset.year = String(y);
      const head = document.createElement("header");
      head.className = "video-grid-year-head";
      const arrow = document.createElement("span");
      arrow.className = "vgy-arrow";
      arrow.textContent = "\u25BE"; // ▾
      const label = document.createElement("span");
      label.className = "vgy-label";
      label.textContent = (y === "?" ? "Unknown" : String(y));
      const count = document.createElement("span");
      count.className = "vgy-count";
      count.textContent = `(${vids.length})`;
      head.append(arrow, label, count);
      section.appendChild(head);

      const inner = document.createElement("div");
      inner.className = "video-grid-year-inner";
      if (groupByMonth && y !== "?") {
        // Second-level grouping: month within year. Month order
        // follows the caller's sort — videos arrive already sorted
        // so the first month we see is the newest (or oldest).
        const mBuckets = new Map();
        const mOrder = [];
        const mUnknown = [];
        for (const v of vids) {
          const m = _monthOf(v);
          if (m == null) { mUnknown.push(v); continue; }
          if (!mBuckets.has(m)) { mBuckets.set(m, []); mOrder.push(m); }
          mBuckets.get(m).push(v);
        }
        if (mUnknown.length) { mBuckets.set("?", mUnknown); mOrder.push("?"); }
        for (const m of mOrder) {
          const mVids = mBuckets.get(m);
          const mSec = document.createElement("section");
          mSec.className = "video-grid-month-section";
          mSec.dataset.month = String(m);
          const mHead = document.createElement("header");
          mHead.className = "video-grid-month-head";
          const mArrow = document.createElement("span");
          mArrow.className = "vgy-arrow";
          mArrow.textContent = "\u25BE";
          const mLabel = document.createElement("span");
          mLabel.className = "vgy-label";
          mLabel.textContent = (m === "?" ? "Unknown" : _MONTH_NAMES[m] || `Month ${m}`);
          const mCount = document.createElement("span");
          mCount.className = "vgy-count";
          mCount.textContent = `(${mVids.length})`;
          mHead.append(mArrow, mLabel, mCount);
          mSec.appendChild(mHead);
          const mInner = document.createElement("div");
          mInner.className = "video-grid-year-inner";
          for (const v of mVids) mInner.appendChild(_buildVideoCard(v, onVideoClick));
          mSec.appendChild(mInner);
          mHead.addEventListener("click", () => {
            const collapsed = mSec.classList.toggle("collapsed");
            mArrow.textContent = collapsed ? "\u25B8" : "\u25BE";
          });
          inner.appendChild(mSec);
        }
      } else {
        for (const v of vids) inner.appendChild(_buildVideoCard(v, onVideoClick));
      }
      section.appendChild(inner);

      head.addEventListener("click", () => {
        const collapsed = section.classList.toggle("collapsed");
        arrow.textContent = collapsed ? "\u25B8" : "\u25BE"; // ▸ : ▾
      });

      frag.appendChild(section);
    }
    grid.appendChild(frag);
  };

  const _MONTH_NAMES = {
    1: "January", 2: "February", 3: "March", 4: "April",
    5: "May", 6: "June", 7: "July", 8: "August",
    9: "September", 10: "October", 11: "November", 12: "December",
  };

  function _monthOf(v) {
    // Prefer the explicit `month` field (set from DB row's year/month
    // columns, which come from the yyyy/mm folder structure on disk);
    // fall back to parsing from upload_ts epoch if month is missing.
    if (v && typeof v.month === "number" && v.month >= 1 && v.month <= 12) {
      return v.month;
    }
    if (v && v.upload_ts) {
      try {
        const d = new Date(Number(v.upload_ts));
        if (!Number.isNaN(d.getTime())) return d.getMonth() + 1;
      } catch {}
    }
    return null;
  }

  function _fmtBytes(b) {
    if (!b) return "";
    const units = ["B","KB","MB","GB","TB"];
    let i = 0, v = b;
    while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
    return `${v.toFixed(i >= 2 ? 1 : 0)} ${units[i]}`;
  }

  // Pretty-print a date string (accepts YYYY-MM-DD or any Date-parseable
  // form) as e.g. "Nov 15, 2025". Returns the original string unchanged
  // if it can't be parsed so existing displays never regress to "NaN".
  const _MON_ABBR = ["Jan","Feb","Mar","Apr","May","Jun",
                     "Jul","Aug","Sep","Oct","Nov","Dec"];
  function _fmtCardDate(s) {
    if (!s) return "";
    // Prefer a strict YYYY-MM-DD match so we don't trip on Date's
    // timezone-shifted parsing of bare ISO dates (which can roll
    // "2025-11-15" into Nov 14 on negative-UTC offsets).
    const m = String(s).match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (m) {
      const y = Number(m[1]);
      const mo = Number(m[2]);
      const d = Number(m[3]);
      if (mo >= 1 && mo <= 12 && d >= 1 && d <= 31) {
        return `${_MON_ABBR[mo - 1]} ${d}, ${y}`;
      }
    }
    // Fallback — hand off to Date() for other formats (ISO with time, etc.)
    const dt = new Date(s);
    if (!isNaN(dt.getTime())) {
      return `${_MON_ABBR[dt.getMonth()]} ${dt.getDate()}, ${dt.getFullYear()}`;
    }
    return String(s);
  }

  // Shared IntersectionObserver so big channels don't make thousands of them.
  let _gridIO = null;
  const _gridSentinelCallbacks = new WeakMap();
  function _observeGridSentinel(sentinel, cb) {
    if (!_gridIO) {
      _gridIO = new IntersectionObserver((entries) => {
        for (const e of entries) {
          if (e.isIntersecting) {
            const fn = _gridSentinelCallbacks.get(e.target);
            if (fn) {
              _gridIO.unobserve(e.target);
              _gridSentinelCallbacks.delete(e.target);
              fn();
            }
          }
        }
      }, { rootMargin: "400px" });
    }
    _gridSentinelCallbacks.set(sentinel, cb);
    _gridIO.observe(sentinel);
  }

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
      // bug H-10: when the retranscribe completes but the segment
      // fetch comes back empty (JSONL write failed, FTS ingest failed,
      // whisper produced nothing), the old transcript used to stay on
      // screen alongside a "complete" toast — user assumed the
      // retranscribe worked fine. Now explicitly replace with a
      // placeholder and show a warn toast so the failure is visible.
      if (!segments.length) {
        window.renderWatchView(cur, [], sourceInfo,
                               { skipVideoReload: true });
        window._showToast?.(
          "Re-transcription finished but produced no segments \u2014 check the log.",
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
      window._showToast?.("Re-transcription complete \u2014 transcript updated.", "ok");
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
    meta.textContent = parts.join(" \u00b7 ");

    // Stash for `_onRetranscribeComplete` — when the Python side finishes
    // a retranscribe, it pushes an event; the handler checks this ref
    // to decide whether the completed job matches what's on screen.
    window._watchCurrentVideo = video;

    if (!skipVideoReload) {
      _loadVideoSource(video, vEl, ph);
      _loadWatchMetadataDrawer(video);
    }

    tr.innerHTML = "";
    if (!transcript || transcript.length === 0) {
      tr.innerHTML = '<div style="color: var(--c-dim); font-style: italic;">No transcript available.</div>';
      _unbindKaraoke(vEl);
      return;
    }

    const frag = document.createDocumentFragment();

    // Source banner — Whisper / YT auto-captions / unknown. Mirrors
    // ArchivePlayer's `_ytSourceBannerHTML`.
    const bannerEl = _buildSourceBanner(sourceInfo, video);
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
      if (words) {
        for (const wobj of words) {
          const span = document.createElement("span");
          span.className = "word";
          span.dataset.s = wobj.s ?? 0;
          span.dataset.e = wobj.e ?? 0;
          span.textContent = (wobj.w || "") + " ";
          span.addEventListener("click", () => _seekTo(vEl, wobj.s ?? 0));
          segEl.appendChild(span);
        }
      } else {
        const span = document.createElement("span");
        span.className = "word";
        span.textContent = (seg.t || seg.text || "") + " ";
        span.addEventListener("click", () => _seekTo(vEl, seg.s ?? 0));
        segEl.appendChild(span);
      }
      // Trailing space between segments keeps words from running together.
      segEl.appendChild(document.createTextNode(" "));
      body.appendChild(segEl);
      segEls.push(segEl);
    }
    frag.appendChild(body);
    tr.appendChild(frag);
    _bindKaraoke(vEl, tr, segEls);
  };

  /** Build the source-banner div for the Watch view transcript panel.
   * Returns a DOM element or null. Mirrors ArchivePlayer's
   * `_ytSourceBannerHTML` (static/app.js:1106) EXACTLY — same text,
   * same four cases, same link wording. The "unknown, no raw" case
   * returns null so no banner appears at all (ArchivePlayer returns
   * the empty string for that case). */
  function _buildSourceBanner(sourceInfo, video) {
    const src = (sourceInfo && sourceInfo.source) || "unknown";
    const raw = (sourceInfo && sourceInfo.raw) || "";

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
      const parts = (raw || "").trim().split(/[\s:]+/);
      const model = parts.length > 1 ? parts.slice(1).join(" ") : "";
      const txt = document.createElement("span");
      txt.textContent = model
        ? `Whisper transcription \u2014 ${model.toLowerCase()} model`
        : "Whisper transcription";
      banner.appendChild(txt);
      return banner;
    }
    if (src === "yt_captions_punct") {
      banner.classList.add("yt");
      banner.appendChild(dot);
      banner.appendChild(document.createTextNode(
        "YouTube auto-captions (punctuation restored) \u2014 transcript is approximate \u00b7 "));
      banner.appendChild(buildRetranscribeLink());
      banner.appendChild(document.createTextNode(" for improved results"));
      return banner;
    }
    if (src === "yt_captions_raw") {
      banner.classList.add("yt");
      banner.appendChild(dot);
      banner.appendChild(document.createTextNode(
        "YouTube auto-captions \u2014 transcript is approximate \u00b7 "));
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
  async function _loadWatchMetadataDrawer(video) {
    const drawer = document.getElementById("watch-meta-drawer");
    if (!drawer) return;
    const statsEl = document.getElementById("watch-meta-stats");
    const descEl = document.getElementById("watch-meta-description");
    const commentsEl = document.getElementById("watch-meta-comments");
    const countEl = document.getElementById("watch-meta-comments-count");
    // Reset state immediately so a slow fetch doesn't bleed previous video's data
    if (statsEl) statsEl.textContent = "";
    if (descEl) descEl.textContent = "Loading\u2026";
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
      statsEl.textContent = bits.join(" \u00b7 ");
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
        if (c.likes) likes.textContent = `${Number(c.likes).toLocaleString()} \u25B2`;
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
    const RX = /(?<![\d:])(\d{1,3}):(\d{2})(?::(\d{2}))?\b/g;
    let lastIdx = 0;
    let m;
    while ((m = RX.exec(text)) !== null) {
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
            vEl.currentTime = secs;
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
    // Start collapsed — transcript should be the dominant surface.
    drawer.classList.add("collapsed");
    body.style.display = "none";
    head.addEventListener("click", () => {
      const collapsed = drawer.classList.toggle("collapsed");
      body.style.display = collapsed ? "none" : "";
      arrow.innerHTML = collapsed ? "\u25B8" : "\u25BE";
    });
  }
  // Fire once when logs.js loads
  if (document.readyState !== "loading") _initWatchMetaDrawerToggle();
  else document.addEventListener("DOMContentLoaded", _initWatchMetaDrawerToggle);

  async function _loadVideoSource(video, vEl, ph) {
    if (!vEl) return;
    const fp = video.filepath || "";
    const api = window.pywebview?.api;
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
    if (url) {
      vEl.src = url;
      vEl.style.display = "";
      if (ph) ph.style.display = "none";
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
            `\u26a0 ${errorDetail}</div>` +
            (leaf
              ? `<div style="font-size:11px;color:var(--c-dim);">${leaf}</div>`
              : "") +
            `<div style="font-size:11px;color:var(--c-dim);margin-top:6px;">` +
            `The index entry may be stale \u2014 try Rescan archive.</div>`;
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

  // ── Karaoke: bind a single timeupdate handler per video load ──

  let _karaokeHandler = null;
  function _unbindKaraoke(vEl) {
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

    _karaokeHandler = () => {
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
    };
    vEl.addEventListener("timeupdate", _karaokeHandler);
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
    // Fall-through: return the last segment we've crossed (for gaps)
    let best = -1;
    for (let i = 0; i < segEls.length; i++) {
      const s = parseFloat(segEls[i].dataset.s || "0");
      if (s <= t) best = i;
      else break;
    }
    return best;
  }

  function escapeHtml(s) {
    return String(s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }
  function escapeAttr(s) {
    return escapeHtml(s);
  }
})();
