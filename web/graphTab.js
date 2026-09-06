/**
 * web/graphTab.js — Browse > Graph sub-mode.
 *
 * Owns the Chart.js word-frequency graph, word cloud, CSV export,
 * click-to-drill into search, _graphChart state.
 *
 * Depends on: util.js, bridge.js, vendor/chart.umd.min.js.
 * Loaded BEFORE app.js (which calls YT.graph.init() during boot).
 *
 * Public surface:
 *   YT.graph.init()                  — wire up the Graph sub-view
 *   YT.graph.populateChannels()      — populate the channel <select>
 *   YT.graph.draw()                  — re-render from current form state
 *   YT.graph.drillIntoSearch(...)    — used internally + by Word Cloud span click
 *
 * Back-compat globals (legacy app.js boot still calls these):
 *   window.initGraphView, window.populateGraphChannels, window.drawGraph
 */
(function () {
  "use strict";
  function bridgeCall(method, ...args) {
    const fn = window.YT?.bridge?.bridgeCall;
    if (fn) return fn(method, ...args);
    return undefined;
  }

  function nativeBridgeUp() {
    return !!window.YT?.bridge?.isUp?.();
  }

  window.YT = window.YT || {};
  const YT = window.YT;
  // Hard fallback that ACTUALLY escapes. This IIFE loads before app.js and
  // captures escapeHtml at module-eval time; if YT.util weren't populated yet
  // the old `|| (s => String(s))` fallback was a no-op, leaving channel names
  // (YT-derived) flowing raw into <option> innerHTML. The fallback now escapes.
  const escapeHtml = (YT.util && YT.util.escapeHtml) || window._escapeHtml
    || (s => String(s ?? "").replace(/[&<>"']/g,
        c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c])));

  // ─── Browse > Graph sub-mode (word frequency + word cloud) ───────────
  // Mirrors YTArchiver.py Graph/Frequency sub-mode: 3 chart types (Line,
  // Bar, Word Cloud), per-1000-segments Normalize toggle, click-graph-to-
  // drill-into-search, and CSV export of the plot data.
  let _graphChart = null;
  let _graphLastData = null; // last-rendered { labels, values | series, bucket, word, channel }
  let _graphType = "line"; // current chart type
  let _graphRequestSeq = 0;

  function initGraphView() {
    // Re-init guard so a second invocation doesn't double-wire
    // every click handler (audit: graphTab.js H218).
    if (window._graphInited) return;
    window._graphInited = true;
    const btn = document.getElementById("btn-graph-run");
    if (btn) btn.addEventListener("click", drawGraph);
    // Chart-type buttons
    const typeBtns = document.querySelectorAll(".chart-type-btn");
    typeBtns.forEach(b => {
      b.addEventListener("click", () => {
        // Invalidate a query started for the old chart type before changing
        // modes. A late line response must not be interpreted as word-cloud
        // data (or vice versa).
        _graphRequestSeq++;
        typeBtns.forEach(x => {
          x.classList.remove("active");
          x.setAttribute("aria-pressed", "false");
        });
        b.classList.add("active");
        b.setAttribute("aria-pressed", "true");
        _graphType = b.dataset.type || "line";
        // Re-render on type switch. Word Cloud is independent of the Line/Bar
        // query (it needs no word) and previously showed "No words." until the
        // user clicked Plot — so when switching TO it, re-use a cached cloud if
        // we have one, otherwise fetch it now. Line/Bar only re-render when the
        // cache actually holds series data (not a cloud), since they require a
        // word in the box.
        if (_graphType === "wordcloud") {
          if (_graphLastData && Array.isArray(_graphLastData.cloud)) {
            const emptyEl = document.getElementById("graph-empty");
            if (emptyEl) emptyEl.textContent = "";
            drawGraphFromData(_graphLastData);
          } else {
            drawGraph();
          }
        } else if ((document.getElementById("graph-word")?.value || "").trim()) {
          // Line/Bar with a word present: re-query so the switch reflects the
          // CURRENT word/channel form state, not the last-plotted snapshot.
          // (Redrawing the cached series here is what made edits to Word or
          // Channel look ignored when you only toggled the chart type.)
          drawGraph();
        } else if (_graphLastData && !_graphLastData.cloud) {
          // No word typed but a cached series exists — redraw it so the
          // Line<->Bar toggle still works without forcing a re-type.
          const emptyEl = document.getElementById("graph-empty");
          if (emptyEl) emptyEl.textContent = "";
          drawGraphFromData(_graphLastData);
        } else {
          // Cold start / empty box: drawGraph() toasts "Enter a word to plot."
          drawGraph();
        }
        _syncGraphWordField();
      });
    });
    _syncGraphWordField();
    // Normalize toggle re-runs the query (the divisor is server-side).
    document.getElementById("graph-normalize")?.addEventListener("change", () => {
      if ((document.getElementById("graph-word")?.value || "").trim()) {
        drawGraph();
      }
    });
    // Channel scope change auto-replots (parity with the Plot button) so the
    // chart reflects the selected channel without a manual Plot click.
    document.getElementById("graph-channel")?.addEventListener("change", () => {
      if (_graphType === "wordcloud"
          || (document.getElementById("graph-word")?.value || "").trim()) {
        drawGraph();
      }
    });
    // Enter in the Word field plots — fire the Plot button itself so Enter and
    // Plot can never drift apart.
    document.getElementById("graph-word")?.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        document.getElementById("btn-graph-run")?.click();
      }
    });
    // CSV export
    document.getElementById("btn-graph-export-csv")?.addEventListener("click", _exportGraphCsv);
    document.getElementById("btn-graph-drill")?.addEventListener(
      "click", _drillSelectedGraphPoint);
  }

  function _graphDestroy() {
    if (_graphChart) {
      try { _graphChart.destroy(); } catch {}
      _graphChart = null;
    }
  }

  function _clearGraphResult() {
    _graphLastData = null;
    _graphDestroy();
    _hideWordCloud();
    _clearGraphDrillControls();
  }

  function _clearGraphDrillControls() {
    const controls = document.getElementById("graph-drill-controls");
    const select = document.getElementById("graph-drill-select");
    if (controls) controls.hidden = true;
    if (select) {
      select.innerHTML = "";
      select._graphDrillChoices = [];
    }
  }

  function _populateGraphDrillControls(data) {
    const controls = document.getElementById("graph-drill-controls");
    const select = document.getElementById("graph-drill-select");
    if (!controls || !select) return;
    const labels = Array.isArray(data?.labels) ? data.labels : [];
    const choices = [];
    if (Array.isArray(data?.series) && data.series.length) {
      data.series.forEach((series, datasetIndex) => {
        labels.forEach((label, index) => {
          choices.push({
            word: series?.word || data.word || "",
            label,
            bucket: data.bucket || "month",
            channel: data.channel || null,
            value: Array.isArray(series?.values) ? series.values[index] : "",
            datasetIndex,
            index,
          });
        });
      });
    } else {
      labels.forEach((label, index) => {
        choices.push({
          word: data?.word || "",
          label,
          bucket: data?.bucket || "month",
          channel: data?.channel || null,
          value: Array.isArray(data?.values) ? data.values[index] : "",
          datasetIndex: 0,
          index,
        });
      });
    }
    select.innerHTML = "";
    choices.forEach((choice, index) => {
      const option = document.createElement("option");
      option.value = String(index);
      const word = choice.word ? `“${choice.word}” — ` : "";
      option.textContent = `${word}${choice.label}: ${choice.value ?? ""}`;
      select.appendChild(option);
    });
    select._graphDrillChoices = choices;
    controls.hidden = choices.length === 0;
  }

  function _drillSelectedGraphPoint() {
    const select = document.getElementById("graph-drill-select");
    const index = Number(select?.value);
    const choice = Number.isInteger(index)
      ? select?._graphDrillChoices?.[index] : null;
    if (!choice) return;
    _drillIntoSearch(
      choice.word, choice.label, choice.bucket, choice.channel);
  }

  // In Word-cloud mode the Word field is ignored (the cloud is a global
  // top-words view), so grey it out + add a tooltip explaining why; it's
  // re-enabled for Line/Bar where a word is required.
  function _syncGraphWordField() {
    const wordEl = document.getElementById("graph-word");
    if (!wordEl) return;
    if (wordEl.dataset.defaultPlaceholder === undefined) {
      wordEl.dataset.defaultPlaceholder = wordEl.placeholder || "";
    }
    const cloud = _graphType === "wordcloud";
    wordEl.disabled = cloud;
    // Swap the placeholder too (not just disabled + hover tooltip) so the
    // "ignored here" state is visible at a glance, not only on hover.
    wordEl.placeholder = cloud
      ? "(ignored in word-cloud mode)"
      : wordEl.dataset.defaultPlaceholder;
    wordEl.title = cloud
      ? "Word cloud shows the most-spoken words across the whole scope — the Word field is ignored here."
      : "";
  }

  async function drawGraph() {
    const wordEl = document.getElementById("graph-word");
    const chanEl = document.getElementById("graph-channel");
    const bucketEl = document.getElementById("graph-bucket");
    const emptyEl = document.getElementById("graph-empty");
    const canvas = document.getElementById("graph-canvas");
    if (!canvas || !wordEl) return;

    const requestId = ++_graphRequestSeq;
    const requestType = _graphType;
    const requestIsCurrent = () =>
      requestId === _graphRequestSeq && requestType === _graphType;
    const word = (wordEl.value || "").trim();
    const bucket = bucketEl?.value || "month";
    const channel = (chanEl && chanEl.value !== "All") ? chanEl.value : null;
    const normalize = !!document.getElementById("graph-normalize")?.checked;

    // Word Cloud doesn't require a word — it's "what are the most-spoken
    // words overall?". Line/Bar both need a word.
    if (requestType !== "wordcloud" && !word) {
      _clearGraphResult();
      if (emptyEl) emptyEl.textContent = "Enter a word to plot.";
      window._showToast?.("Enter a word to plot.", "warn");
      return;
    }

    if (!nativeBridgeUp()) {
      _clearGraphResult();
      if (emptyEl) emptyEl.textContent = "The graph isn't ready yet. Try again in a moment.";
      return;
    }

    _clearGraphResult();
    if (emptyEl) emptyEl.textContent = "Querying\u2026";

    // Word Cloud path
    if (requestType === "wordcloud") {
      // Method-existence check — kept on direct api because the YT.api
      // proxy resolves every name to a function and so can't express
      // "is this endpoint actually wired on the backend?". Preserves the
      // distinct "not wired yet" message instead of a generic toast.
      if (!window.pywebview?.api?.browse_word_cloud) {
        _clearGraphResult();
        if (emptyEl) emptyEl.textContent = "Word cloud backend not wired yet.";
        return;
      }
      let cloud;
      try {
        const outcome = await window.YT.bridge.catalogRead(
          "graph",
          () => bridgeCall("browse_word_cloud", channel, 120),
          {
            label: "graph data",
            onStatus: (status) => {
              if (status.phase !== "done" && requestIsCurrent() && emptyEl) {
                emptyEl.textContent = status.text;
              }
            },
          });
        if (outcome.stale) return;
        cloud = outcome.value;
      }
      catch (e) {
        if (requestIsCurrent()) {
          _clearGraphResult();
          if (emptyEl) emptyEl.textContent = "Error: " + e;
        }
        return;
      }
      if (!requestIsCurrent()) return;
      if (!cloud?.ok || !Array.isArray(cloud.words) || !cloud.words.length) {
        _clearGraphResult();
        if (emptyEl) emptyEl.textContent = cloud?.error || "No words found.";
        return;
      }
      if (emptyEl) emptyEl.textContent = "";
      const samplingLabel = cloud.sampling?.label || "";
      _renderWordCloud(cloud.words, samplingLabel);
      _graphLastData = {
        cloud: cloud.words,
        channel,
        graphType: requestType,
        samplingLabel,
      };
      return;
    }

    // Line / Bar path
    // Method-existence check kept on direct api (see word-cloud note above).
    if (!window.pywebview?.api?.browse_graph) {
      _clearGraphResult();
      if (emptyEl) emptyEl.textContent = "The graph isn't ready yet. Try again in a moment.";
      return;
    }
    let data;
    try {
      const outcome = await window.YT.bridge.catalogRead(
        "graph",
        () => bridgeCall("browse_graph", word, channel, bucket, normalize),
        {
          label: "graph data",
          onStatus: (status) => {
            if (status.phase !== "done" && requestIsCurrent() && emptyEl) {
              emptyEl.textContent = status.text;
            }
          },
        });
      if (outcome.stale) return;
      data = outcome.value;
    }
    catch (e) {
      if (requestIsCurrent()) {
        _clearGraphResult();
        if (emptyEl) emptyEl.textContent = "Error: " + e;
      }
      return;
    }
    if (!requestIsCurrent()) return;
    if (data?.error) {
      _clearGraphResult();
      if (emptyEl) emptyEl.textContent = data.error;
      return;
    }
    // backend reports `backfill_pending` when week-bucket
    // plots are requested before upload_ts backfill finishes. Surface
    // this so the user knows sparse week data is incomplete, not empty.
    if (bucket === "week" && data?.backfill_pending) {
      window._showToast?.(
        `${data.backfill_pending.toLocaleString()} video(s) still indexing \u2014 ` +
        `week plot may be incomplete until backfill finishes.`, "warn");
    }
    if (!data?.labels?.length) {
      if (emptyEl) emptyEl.textContent = `No occurrences found.`;
      _clearGraphResult();
      return;
    }
    if (emptyEl) emptyEl.textContent = "";

    _graphLastData = Object.assign({}, data,
      { word, channel, bucket, normalize, graphType: requestType });
    drawGraphFromData(_graphLastData, requestType);
  }

  function drawGraphFromData(data, graphType = _graphType) {
    if (!data) return;
    const canvas = document.getElementById("graph-canvas");
    if (!canvas) return;

    // Word cloud uses its own renderer, not Chart.js
    if (graphType === "wordcloud") {
      _clearGraphDrillControls();
      _renderWordCloud(data.cloud || [], data.samplingLabel || "");
      return;
    }
    _populateGraphDrillControls(data);
    _hideWordCloud();

    const chartWords = Array.isArray(data.series) && data.series.length
      ? data.series.map(series => series.word).filter(Boolean).join(", ")
      : (data.word || "word frequency");
    canvas.setAttribute(
      "aria-label",
      `${graphType === "bar" ? "Bar" : "Line"} chart for ${chartWords}. `
        + "Use the Chart point controls below to inspect a bucket in Search.");
    if (typeof Chart === "undefined") return;

    _graphDestroy();
    const palette = ["#6cb4ee", "#e87aac", "#3dd68c", "#c7e64f",
                     "#c084fc", "#ff8c42", "#38d9e0"];

    let datasets;
    if (Array.isArray(data.series) && data.series.length) {
      datasets = data.series.map((s, i) => ({
        label: `"${s.word}"`,
        data: s.values,
        borderColor: palette[i % palette.length],
        backgroundColor: graphType === "bar"
          ? palette[i % palette.length] + "aa"
          : palette[i % palette.length] + "28",
        tension: 0.25,
        fill: graphType === "line",
        pointRadius: 3,
        pointHoverRadius: 5,
        pointHitRadius: 14,
        pointBackgroundColor: palette[i % palette.length],
      }));
    } else {
      datasets = [{
        label: `"${data.word}"${data.channel ? " in " + data.channel : ""}` +
               (data.normalize ? " (per 1k)" : ""),
        data: data.values,
        borderColor: "#6cb4ee",
        backgroundColor: graphType === "bar"
          ? "rgba(108, 180, 238, 0.75)"
          : "rgba(108, 180, 238, 0.15)",
        tension: 0.25,
        fill: graphType === "line",
        pointRadius: 3,
        pointHoverRadius: 5,
        pointHitRadius: 14,
        pointBackgroundColor: "#6cb4ee",
      }];
    }

    _graphChart = new Chart(canvas.getContext("2d"), {
      type: graphType,
      data: { labels: data.labels, datasets: datasets },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        // Deferred resize prevents Chart.js from getting stuck at
        // height=0 when the browser throttles layout during window
        // drag. graph "collapses and never recovers" when the
        // window is squished narrow.
        resizeDelay: 50,
        animation: { duration: 300 },
        // A one-bucket series used to pin its only point to the chart edge,
        // where the canvas clipped most of its click target. Center that
        // special case, keep a little breathing room, and let Chart.js choose
        // the nearest point when the click is just outside the visible dot.
        layout: { padding: { left: 10, right: 10 } },
        interaction: { mode: "nearest", intersect: false },
        scales: {
          x: {
            // Preserve Chart.js's normal bar spacing while centering the
            // otherwise edge-clipped one-point line-series case.
            offset: graphType === "bar"
              || (Array.isArray(data.labels) && data.labels.length === 1),
            ticks: { color: "#a0aabb" },
            grid: { color: "#2a2c30" },
          },
          y: { ticks: { color: "#a0aabb" }, grid: { color: "#2a2c30" }, beginAtZero: true,
               title: {
                 display: true,
                 text: data.normalize ? "Matching segments per 1,000 segments" : "Matching transcript segments",
                 color: "#a0aabb",
               } },
        },
        plugins: {
          legend: { labels: { color: "#dde1e8" } },
          tooltip: { backgroundColor: "#1c1e21",
                     titleColor: "#dde1e8", bodyColor: "#dde1e8" },
        },
        // Click-to-drill: clicking a data point jumps to Search tab with
        // the word + the bucket's date range filled in.
        onClick: (evt, els) => {
          if (!els || !els.length) return;
          const idx = els[0].index;
          const label = data.labels[idx];
          // A multi-word plot stores the original comma-separated input in
          // data.word. Drill with the clicked dataset's word first; otherwise
          // every line opened Search for the whole comma-separated string.
          const word = data.series?.[els[0].datasetIndex]?.word
            || data.word || "";
          _drillIntoSearch(word, label, data.bucket || "month", data.channel);
        },
      },
    });
    // Belt-and-suspenders: Chart.js's built-in ResizeObserver occasionally
    // fails to re-measure after the container shrinks to a very small
    // size and then grows back. We attach our OWN observer on the
    // wrapper and explicitly call chart.resize() whenever its rect
    // changes. Wrapped in rAF so rapid drag events coalesce into one
    // resize per frame. Also listens on window resize as a fallback
    // for environments where ResizeObserver misfires entirely.
    try {
      const wrap = canvas.parentElement;
      if (wrap && !wrap._graphResizeObs) {
        let raf = 0;
        const kick = () => {
          cancelAnimationFrame(raf);
          raf = requestAnimationFrame(() => {
            try { _graphChart && _graphChart.resize(); } catch {}
          });
        };
        if (typeof ResizeObserver === "function") {
          const ro = new ResizeObserver(kick);
          ro.observe(wrap);
          wrap._graphResizeObs = ro;
        }
        // Window-resize as a second safety net.
        // gate the listener add with a flag on `wrap` the
        // same way the ResizeObserver is gated, so we don't accumulate
        // N listeners after N chart-type switches. Each
        // drawGraphFromData call used to add ANOTHER resize listener,
        // so after 10 switches 10 handlers fired on every window
        // resize — noticeable perf hit on wide monitors.
        if (!wrap._graphWinResize) {
          window.addEventListener("resize", kick);
          wrap._graphWinResize = true;
        }
      }
    } catch (e) { /* resize-safety is best-effort */ }
  }

  // Switch to the Search view and pre-fill the query + year range so the
  // user sees the exact segments behind a clicked graph point. Mirrors
  // YTArchiver.py:30507 _on_graph_click.
  function _drillIntoSearch(word, bucketLabel, bucket, channel) {
    if (!word) return;
    // Activate the Search sub-view within Browse. NOTE: the sub-view switcher
    // is a .submode-btn[data-submode="..."]; there is no [data-view] element,
    // so the old selector silently no-op'd and the drill ran the search into a
    // still-hidden Search view (looked like "clicking a point does nothing").
    document.querySelector('.tab[data-tab="browse"]')?.click();
    document.querySelector('.submode-btn[data-submode="search"]')?.click();
    const q = document.getElementById("search-query");
    const yf = document.getElementById("search-year-from");
    const yt = document.getElementById("search-year-to");
    const scope = document.getElementById("search-scope");
    const inTranscripts = document.getElementById("search-in-transcripts");
    const inTitles = document.getElementById("search-in-titles");
    if (q) q.value = word;
    // Graph points are calculated exclusively from transcript segments, so
    // their drill-down must search the same source regardless of whatever
    // content mode the user last chose in Search. Use transcript-only here:
    // retaining title matches would mix unrelated video-title hits into the
    // evidence behind the plotted point. Dispatch change so the visible
    // filter state and the hidden compatibility scope stay in sync.
    if (inTranscripts) inTranscripts.checked = true;
    if (inTitles) inTitles.checked = false;
    inTranscripts?.dispatchEvent(new Event("change", { bubbles: true }));
    inTitles?.dispatchEvent(new Event("change", { bubbles: true }));
    if (scope) scope.value = "all";
    // Preserve the exact clicked month or ISO week for the Search request.
    // The visible year fields stay populated as a useful summary.
    const m1 = /^(\d{4})$/.exec(bucketLabel || "");
    const m2 = /^(\d{4})-(\d{2})$/.exec(bucketLabel || "");
    const mw = /^(\d{4})-W(\d{2})$/.exec(bucketLabel || "");
    let range = null;
    if (yf) yf.value = "";
    if (yt) yt.value = "";
    if (m1) { if (yf) yf.value = m1[1]; if (yt) yt.value = m1[1]; }
    else if (m2) {
      if (yf) yf.value = m2[1];
      if (yt) yt.value = m2[1];
      const start = new Date(Number(m2[1]), Number(m2[2]) - 1, 1);
      const end = new Date(Number(m2[1]), Number(m2[2]), 1);
      range = { fromTs: start.getTime() / 1000, toTs: end.getTime() / 1000 };
    } else if (mw) {
      const jan4 = new Date(Number(mw[1]), 0, 4);
      const mondayOffset = (jan4.getDay() + 6) % 7;
      const start = new Date(Number(mw[1]), 0,
        4 - mondayOffset + (Number(mw[2]) - 1) * 7);
      const end = new Date(start);
      end.setDate(end.getDate() + 7);
      if (yf) yf.value = String(start.getFullYear());
      if (yt) yt.value = String(new Date(end.getTime() - 1).getFullYear());
      range = { fromTs: start.getTime() / 1000, toTs: end.getTime() / 1000 };
    }
    window._searchExactDateRange = range
      ? { ...range, label: String(bucketLabel), query: String(word) }
      : null;
    window._paintSearchDateFilter?.();
    window._setSearchSelectedChannels?.([]);
    if (scope && channel) {
      // Set the hidden compat select so the legacy path picks up the
      // single-channel scope.
      for (const opt of scope.options) {
        if (opt.value === "channel") { scope.value = "channel"; break; }
      }
      // Also seed the new multi-select state so the modern doSearch
      // path (which prefers _searchSelectedChannels over the legacy
      // shim) actually scopes to this channel. The Graph view passes
      // either the channel folder or the channel name; try to resolve
      // the folder by matching either against the Subs cache.
      try {
        const rows = window._subsAllRows || [];
        const match = rows.find(r => r.folder === channel || r.name === channel);
        const folder = match?.folder || channel;
        window._setSearchSelectedChannels?.([folder]);
        const wrap = document.getElementById("search-channel-multi");
        if (wrap && wrap._searchSelected) {
          wrap._searchSelected.clear();
          wrap._searchSelected.add(folder);
          // Update the visible label without re-opening the panel.
          const label = document.getElementById("search-channel-label");
          if (label) label.textContent = match?.name || folder;
          const allCb = document.getElementById("search-channel-all");
          if (allCb) allCb.checked = false;
        }
      } catch { /* drill-in is best-effort UI sugar */ }
    }
    // Fire the search
    setTimeout(() => document.getElementById("btn-search-run")?.click(), 80);
  }

  // Word cloud renderer — no Chart.js; just positions span elements sized
  // proportional to their frequency. Matches YTArchiver.py's matplotlib
  // word-cloud conceptually but DOM-based so we don't pull another lib.
  function _renderWordCloud(words, samplingLabel = "") {
    _graphDestroy();
    _clearGraphDrillControls();
    const canvas = document.getElementById("graph-canvas");
    const wrap = canvas?.parentElement;
    if (!wrap) return;
    // Hide the canvas, show the cloud overlay
    canvas.style.display = "none";
    let cloud = document.getElementById("graph-wordcloud");
    if (!cloud) {
      cloud = document.createElement("div");
      cloud.id = "graph-wordcloud";
      cloud.className = "graph-wordcloud";
      wrap.appendChild(cloud);
    }
    cloud.style.display = "";
    cloud.innerHTML = "";
    if (samplingLabel) {
      const samplingHint = document.createElement("div");
      samplingHint.className = "browse-hint graph-wordcloud-hint";
      samplingHint.textContent = samplingLabel;
      cloud.appendChild(samplingHint);
    }
    const cleanWords = Array.isArray(words)
      ? words.map(w => ({
          word: String(w?.word || "").trim(),
          count: Math.max(1, Number(w?.count) || 1),
        })).filter(w => w.word)
      : [];
    if (!cleanWords.length) {
      cloud.innerHTML = '<div class="browse-empty">No words.</div>';
      return;
    }
    // Scale font sizes between 12px and 52px based on rank.
    const counts = cleanWords.map(w => w.count);
    const max = Math.max(...counts);
    const min = Math.min(...counts);
    // Degenerate-data hint: when every word ties at the same count
    // the cloud renders as one uniform size with no rank cue. Make
    // the user aware that frequency ordering is arbitrary (audit:
    // graphTab.js H220).
    if (max === min && cleanWords.length > 1) {
      const hint = document.createElement("div");
      hint.className = "browse-hint graph-wordcloud-hint";
      hint.textContent = `All ${cleanWords.length} words tied at ${max} `
        + `occurrences — frequency ordering is arbitrary.`;
      cloud.appendChild(hint);
    }
    const palette = ["#6cb4ee", "#e87aac", "#3dd68c", "#c7e64f",
                     "#c084fc", "#ff8c42", "#38d9e0", "#dde1e8"];
    for (let i = 0; i < cleanWords.length; i++) {
      const w = cleanWords[i];
      const ratio = max === min ? 0.5 :
        (Math.log(w.count) - Math.log(min)) / (Math.log(max) - Math.log(min));
      const size = 12 + Math.round(ratio * 40);
      const span = document.createElement("span");
      span.className = "wc-word";
      span.textContent = w.word;
      span.style.fontSize = size + "px";
      span.style.color = palette[i % palette.length];
      span.title = `${w.word} \u2014 ${(w.count).toLocaleString()} occurrence${w.count === 1 ? "" : "s"}`;
      span.setAttribute("role", "button");
      span.tabIndex = 0;
      // Activate = seed the Word field with this word and re-plot as line.
      const activate = () => {
        const wordEl = document.getElementById("graph-word");
        if (wordEl) wordEl.value = w.word;
        // Switch back to line chart
        document.querySelector('.chart-type-btn[data-type="line"]')?.click();
      };
      span.addEventListener("click", activate);
      span.addEventListener("keydown", (event) => {
        if (event.key !== "Enter" && event.key !== " ") return;
        event.preventDefault();
        activate();
      });
      cloud.appendChild(span);
    }
  }

  function _hideWordCloud() {
    const cloud = document.getElementById("graph-wordcloud");
    if (cloud) cloud.style.display = "none";
    const canvas = document.getElementById("graph-canvas");
    if (canvas) canvas.style.display = "";
  }

  // Export the last-plotted data as CSV. Works for Line / Bar (labels +
  // values), or Word Cloud (word, count).
  async function _exportGraphCsv() {
    if (!_graphLastData) {
      window._showToast?.("Plot something first.", "warn");
      return;
    }
    let rows = [];
    if (_graphLastData.cloud) {
      const samplingLabel = String(_graphLastData.samplingLabel ||
        "Limited sample: word frequencies use at most the oldest 500,000 " +
        "transcript segments, not the complete archive.");
      rows.push(["SAMPLE SCOPE", samplingLabel]);
      rows.push([]);
      rows.push(["word", "count"]);
      const cloudRows = Array.isArray(_graphLastData.cloud)
        ? _graphLastData.cloud : [];
      for (const w of cloudRows) {
        const word = String(w?.word || "").trim();
        if (!word) continue;
        rows.push([word, Math.max(1, Number(w?.count) || 1)]);
      }
    } else if (Array.isArray(_graphLastData.series) && _graphLastData.series.length) {
      const labels = Array.isArray(_graphLastData.labels)
        ? _graphLastData.labels : [];
      const series = _graphLastData.series.filter(s => Array.isArray(s.values));
      rows.push(["bucket", ...series.map(s => s.word || "")]);
      const rowCount = Math.min(labels.length, ...series.map(s => s.values.length));
      for (let i = 0; i < rowCount; i++) {
        rows.push([labels[i], ...series.map(s => s.values[i] ?? "")]);
      }
    } else {
      rows.push(["bucket", _graphLastData.word || "count"]);
      const labels = Array.isArray(_graphLastData.labels)
        ? _graphLastData.labels : [];
      const values = Array.isArray(_graphLastData.values)
        ? _graphLastData.values : [];
      const rowCount = Math.min(labels.length, values.length);
      for (let i = 0; i < rowCount; i++) {
        rows.push([labels[i], values[i] ?? ""]);
      }
    }
    if (!_graphLastData.cloud && rows.length) {
      rows[0].push("units", "channel_scope", "normalization", "bucket_size", "counting_rule");
      const context = [
        _graphLastData.normalize ? "Matching segments per 1,000 segments" : "Matching transcript segments",
        _graphLastData.channel || "All channels",
        _graphLastData.normalize ? "Per 1,000 segments" : "Off",
        _graphLastData.bucket || "month",
        "Matching indexed transcript segments; Search removes duplicates and may use different dates when upload dates are missing",
      ];
      for (let i = 1; i < rows.length; i++) rows[i].push(...context);
    }
    const csv = rows.map(r => r.map(c => {
      const s = String(c ?? "");
      return (s.includes(",") || s.includes('"') || s.includes("\n"))
        ? `"${s.replace(/"/g, '""')}"` : s;
    }).join(",")).join("\n");

    const fname = _graphLastData.cloud ? "wordcloud.csv" :
      `graph_${(_graphLastData.word || "data").replace(/[^\w-]+/g, "_")}.csv`;
    if (nativeBridgeUp()) {
      try {
        const res = await bridgeCall("save_text_to_file", fname, csv);
        if (res?.ok) window._showToast?.("CSV saved.", "ok");
        else if (!res?.cancelled) {
          window._showToast?.(res?.error || "Save failed.", "error");
        }
      } catch (error) {
        window._showToast?.(
          `Save failed: ${error?.message || error}`, "error");
      }
    } else {
      // Browser preview fallback
      const blob = new Blob([csv], { type: "text/csv" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url; a.download = fname; a.click();
      setTimeout(() => URL.revokeObjectURL(url), 1000);
    }
  }

  // Populate the Graph's channel dropdown from the DB
  async function populateGraphChannels() {
    const sel = document.getElementById("graph-channel");
    if (!sel) return;
    if (!nativeBridgeUp()) return;
    const paint = (chans) => {
      if (!Array.isArray(chans)) return;
      const previous = sel.value;
      sel.innerHTML = '<option value="All">All</option>' +
        chans.map(c => `<option value="${escapeHtml(c.name || c.folder)}">${escapeHtml(c.name || c.folder)}</option>`).join("");
      if ([...sel.options].some(option => option.value === previous)) {
        sel.value = previous;
      }
    };
    // The Channels screen already owns a useful subscription snapshot. Use
    // it immediately instead of making the graph dropdown wait for artwork
    // and aggregate counts it never displays.
    const cached = window._browseState?.channels || [];
    if (cached.length) {
      paint(cached);
      return;
    }
    sel.innerHTML = '<option value="All">Loading channels…</option>';
    try {
      const outcome = await window.YT.bridge.catalogRead(
        "graph-channels",
        () => bridgeCall("browse_list_channels"),
        {
          label: "graph channels",
          onStatus: (status) => {
            if (status.phase !== "done" && sel.options.length) {
              sel.options[0].textContent = status.text;
            }
          },
        });
      if (!outcome.stale && Array.isArray(outcome.value)) paint(outcome.value);
    } catch (e) { paint([]); }
  }

  YT.graph = {
    init: initGraphView,
    populateChannels: populateGraphChannels,
    draw: drawGraph,
    drillIntoSearch: _drillIntoSearch,
  };
  // Back-compat globals — app.js boot() and other modules call these.
  window.initGraphView = initGraphView;
  window.populateGraphChannels = populateGraphChannels;
  window.drawGraph = drawGraph;
  window._drillIntoSearch = _drillIntoSearch;
})();
