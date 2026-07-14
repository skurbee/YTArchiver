# Changelog

YTArchiver release notes. Format roughly follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) — though
internally we still use a per-push single-decimal counter (`vX.Y`)
rather than full SemVer. Each version below describes what changed
since the previous one.

## v82.1 - 2026-07-13

### Changed
- **Cheap YouTube captions stay in the sync queue.** Already-punctuated local captions are parsed, written, and indexed during download finalization instead of waiting behind Processing.
- **Processing now receives only model-backed transcription work.** Captions that need punctuation restoration and videos that need Whisper are routed through the Processing queue and continue to honor its Auto and pause controls.
- **Compression remains ordered and durable.** Automatic compression is queued only after transcription succeeds, is visible as its own Processing task, and retains its follow-up settings across app restarts.

### Validation
- Backend smoke suite passed: 378 tests.
- Frontend JavaScript syntax and generated HTML freshness checks passed.
- Caption routing, pending counters, activity counts, and compression follow-up persistence have focused regression coverage.
- Built with Python 3.13 using `YTArchiver.spec`.

## v82.0 - 2026-07-13

### Fixed
- **Archive sweeps no longer deadlock each other.** Startup, automatic, and manual sweep requests now share a single-flight gate, so two full-archive writers cannot race and exhaust SQLite's busy timeout.
- **Stable channels no longer generate thousands of no-op writes.** Availability reconciliation updates only catalog rows whose missing/available state actually changed instead of rewriting every video seen during a channel walk.
- **Download-history migration no longer monopolizes the index.** The one-time legacy recency migration runs off the Browse request path, commits in small batches, and records durable completion in SQLite.

### Validation
- Backend smoke suite passed: 371 tests.
- Frontend JavaScript syntax and generated HTML freshness checks passed.
- On the 36 GB production index, the first patched migration's longest writer hold was 0.9 seconds versus 91.7 seconds before the fix; a clean restart produced no writer-lock stalls during a 60-second probe.
- Built with Python 3.13 using `YTArchiver.spec`.

## v81.9 - 2026-07-13

### Fixed
- **Manual thumbnails render reliably.** Existing local thumbnail sidecars now ship with the initial Manual page instead of racing later bridge callbacks, and visible Manual cards use eager local-image loading so tab and sort changes cannot strand thumbnails in Chromium's lazy queue.
- **Persisted processing tasks start after restart.** Starting or resuming the Processing queue now reconstructs the worker's runtime list from the durable queue, preserves all transcription and compression execution fields, and keeps the recovery journal synchronized instead of deleting its newly restored state.
- **Completed Whisper jobs no longer appear stuck at 99%.** The UI reports a finalization phase after recognition, and transcript indexing uses an independent SQLite writer so unrelated long index work cannot hold the completed job indefinitely.
- **Processing feedback is consistent.** Finished transcriptions emit one completion row, popover buttons show one tooltip, and pausing uses the app's standard timestamped pause-log style.

### Validation
- Backend smoke suite passed: 370 tests.
- Frontend JavaScript syntax and generated HTML freshness checks passed.
- Live testing covered Manual thumbnail rendering, persisted queue recovery, Whisper processing, completion logging, pause logging, and popover tooltips.
- Built with Python 3.13 using `YTArchiver.spec`.

## v81.8 - 2026-07-13

### Fixed
- **Manual Downloads opens immediately without dropping thumbnails.** The initial Manual request no longer waits for ffprobe duration checks or local-thumbnail generation. Indexed cards render first, early background thumbnail/duration results are merged into the fresh page instead of being overwritten, and session-specific thumbnail URLs are no longer reused after restart.
- **Manual video lengths are durable.** Loose folder-discovered videos are promoted into the catalog when their local duration is resolved, legacy probes run in a bounded four-worker backfill, and new one-off downloads persist yt-dlp's known duration during registration instead of discarding it.
- **Tray Quit no longer leaves a hazed, unresponsive window.** Quit now hides the window before durable queue, subprocess, server, and index cleanup runs in the background; shutdown cleanup is also guarded against running twice.
- **Browse recency reflects completed downloads.** Recently Downloaded ordering now uses a dedicated completed-download timestamp instead of index discovery time, so rescanning old files cannot make them look newly downloaded.
- **Partial and stale catalog entries stay out of Browse.** Temporary, zero-byte, missing, and interrupted artifacts are rejected or quarantined, while delete and archive reconciliation paths clean up stale catalog state.
- **Thumbnail and duration repairs refresh Browse correctly.** Metadata thumbnail operations invalidate the affected Browse cache, and local duration values remain available from the catalog while missing values backfill safely.

### Validation
- Manual first-page backend call returned 60 of 149 real archive rows in 0.669 seconds.
- Backend smoke suite passed: 364 tests.
- Frontend JavaScript syntax and generated HTML freshness checks passed.
- Built with Python 3.13 using `YTArchiver.spec`.

## v79.3 - 2026-06-29

### Fixed
- **Watch fullscreen transcript overlays stay visible after controls fade.** The Watch view now uses CSS-based fullscreen plus a WebView2 compositor guard so the transcript overlay remains visible when the mouse is idle and native video controls disappear.
- **Duration-filtered videos no longer retry forever.** Videos intentionally skipped by yt-dlp match filters, such as channel minimum-duration rules, are removed from `failed_video_ids` instead of being retried on every sync.
- **Generated app shell is back in sync.** The shipped HTML now includes the Launch-at-boot settings controls, the Browse > Manual view, and the Manual view script include that already existed in the source partials.

### Validation
- Backend smoke suite passed.
- Frontend JavaScript syntax checks passed for all web modules.
- Built with Python 3.13 using the PyInstaller spec file.

## v79.1 - 2026-06-28

### Fixed
- **First-time channel pauses now stay in the queue.** Pausing a new channel download during its first sync no longer makes the task look finished or disappear from the queue.
- **Startup indexing yields to active downloads.** Browse preload and new-file indexing now back off while sync or GPU work is active, reducing first-download stalls on large archives.
- **Browse preload avoids caching partial channel loads.** Low-priority preload work now retries cleanly after being interrupted instead of storing incomplete channel state.

### Changed
- **API mixins continue moving behind service boundaries.** Queue, transcription, onboarding, redownload, and thumbnail paths now prefer injected application services with compatibility fallbacks.

### Validation
- Focused backend smoke tests passed for sync pause behavior, low-priority Browse preload, index maintenance, AppServices mixin slices, and related compile checks.
- Built with Python 3.13 using the PyInstaller spec file.

## v78.9 - 2026-06-22

### Fixed
- **Single-video downloads now bind to the actual saved file more reliably.** The one-off download path now trusts yt-dlp's reported output paths before falling back to title/id guessing, which prevents successful downloads from being reported as missing when yt-dlp sanitizes or trims filenames differently than YTArchiver predicted.
- **Single-video download failures are more readable in Simple mode.** The old terse "file missing after download" message is replaced with a plain explanation that the download finished but YTArchiver could not find the saved video, with clear next steps.
- **Autorun no longer starts Sync Subbed over an active one-off download.** Active single-video downloads now count as busy work for the autorun scheduler, so an interval fire skips/rearms instead of overlapping with the Download tab job.

### Validation
- `ArchiveMixinTests` and `AutorunTests` passed.
- `py_compile` passed for the touched backend, app, and smoke-test files.
- Built with Python 3.13 using the PyInstaller spec file.

## v78.7 — 2026-06-19

### Fixed
- **Large-channel Browse loading is faster and less blocking.** Channel browse data now does less redundant
  disk/index work during large-channel opens, reducing the long stalls seen when opening very large channels.
- **Simple-mode logs are cleaner.** Thumbnail sweep misses are no longer surfaced as raw backend warning
  jargon during normal sync output.
- **Compression is safer.** If the original video's duration cannot be verified, compression now refuses to
  replace the archive copy and keeps a `.compressed` output for manual review.
- **Delete cleanup reports partial failures.** If a video file is deleted but index cleanup fails, the bridge
  now returns a clear partial-failure result instead of a misleading full success.
- **Sidecar deletion is more conservative.** Video delete cleanup no longer removes arbitrary same-stem
  `.txt` files or visible user image/caption files.
- **Folder reorganization can be cancelled.** Reorg now tracks its own running/cancel state and sync start
  refuses to begin while a reorg is active.
- **Single-video archive binding is safer.** The fallback that binds a finished one-off download to a file now
  requires the candidate filename to be corroborated by video id or title.

### Added / Improved
- **Recent/Subs context menus are keyboard accessible.** Selected rows can open their context menu via the
  Context Menu key or Shift+F10, and the shared menu supports focus, arrow navigation, Enter/Space, Escape,
  and ARIA menu roles.
- **Repair and maintenance passes are less wasteful.** Caption repair reuses one temp directory per pass and
  scans large JSONL files in chunks; orphan caption cleanup now has safer guards.
- **Config-write hardening has started.** Several autorun, sync, comments-refresh, and Recent cleanup writes
  now use the shared config transaction path to reduce lost-update races.
- **Tray icon resilience improved.** The system tray icon now refreshes itself if Windows drops the icon during
  a long-running session.

## v78.6 — 2026-06-13

### Fixed
- **Search → Watch now shows the transcript.** Opening a video from a search
  result could land on a "No transcript available" Watch pane even though the
  transcript was clearly in the index (and rendered fine in the result
  preview). The Watch loader now resolves the transcript the way the search and
  bookmark paths do, including a fallback for older entries whose segments
  aren't keyed by video id.
- **Word-frequency graph: the month axis no longer shows "YYYY-00".** Segments
  with no month (videos filed by year only) produced an invalid `2024-00` tick
  label; those buckets now show the year alone.
- **Graph: clicking a point drills into search again.** The "Click a point to
  drill into search" action targeted a stale element and silently did nothing;
  it now switches to Search, pre-fills the word, and constrains the year to the
  clicked bucket.
- **Add channel: the folder name fills correctly when typing a URL.** Typing
  (rather than pasting) a channel URL left the auto-derived folder name stuck on
  the first character. It now keeps deriving the full handle as you type, until
  you edit the name yourself.
- **Transcribed % no longer rounds up to 100%.** A channel at 99.6% displayed
  "(100%)"; it now floors so an incomplete channel never reads as done.

### Added / Improved
- **The graph re-plots when you change the chart type, channel, or word**
  instead of leaving a stale chart until you press Plot; Enter in the word field
  now plots.
- **Word-cloud mode** now makes clear the word field is ignored.
- **"Remove channel" is disabled during an active sync or processing**, with a
  tooltip explaining why, so a removal can't race a running job.
- **Metadata table:** the per-channel "Views" / "Comments" columns are
  relabeled "Views refreshed" / "Comments refreshed" so it's clear they show the
  last-refresh time, not a count.
- **The "add channels in the Subs tab" hint is fully clickable**, not just its
  button.

## v78.1 — 2026-06-07

### Fixed
- **Back from a search result returns to your search.** Opening a video from
  the Search results (or the reader pane) and then pressing Back dropped you
  on the channel grid instead of your results. Watch now remembers where it
  was entered from (Search / Videos / Bookmarks / Graph) and returns there.
- **Standalone (one-off) downloads now get a transcript on disk.** Manually
  transcribing a loose video wrote only to the search index, not to disk. It
  now writes a conjoined `… Transcript.txt` (plus a hidden segment sidecar)
  right next to the video, the same as channel videos.
- **Graph: Normalize + Week no longer plots all zeros.** Per-week
  normalization was dividing by year-keyed totals that never matched, so
  every bar came out zero. Week totals are now keyed per ISO-week.
- **Emptied channels repopulate.** A channel whose files were deleted
  wouldn't re-download because the global download-archive still listed the
  IDs. An "Entire channel" sync of an empty folder now bypasses that archive
  and refills from scratch.
- **Hidden sidecars are now bulletproof.** Sidecar files (metadata JSON,
  thumbnails, segment data, etc.) are reliably flagged hidden so a folder
  shows only the videos and the transcript file — fixing a case where a
  metadata file with an unusual name stayed visible. Adds a **Settings →
  Hidden sidecars → Scan & repair** tool to clean an existing archive in one
  pass.
- **Edit-channel panel no longer keeps stale fields.** The from-date and
  compress options from a previously-opened channel could linger; the form
  now fully resets on every open.
- **Concurrency hardening:** removing a channel can no longer race a live
  sync's writes, metadata sidecar writes are serialized per file, and
  start-up locks are pre-initialized to prevent rare double-spawns.

### Added / Improved
- **Realign thumbnails** now streams per-channel progress to the log, has a
  live **Stop** control, and reports a completion summary for the survey
  pass (previously it ran silently with no feedback).
- **Filter box on the Videos list** — filter the entire archive by title or
  channel name (server-side, so it matches across every page).
- **One-off downloads can fetch metadata + a thumbnail** (optional checkbox)
  so a single download isn't a bare video file. Both sidecars are kept
  hidden.
- **Add-channel auto-fills the folder name** from the channel URL's handle.

## v77.9 — 2026-06-06

### Fixed
- **Browse → Videos refreshes on return.** The Videos grid renders once and
  didn't re-query when you left and came back to it, so a download that
  finished while you were on another tab wouldn't appear until you changed
  the sort. Returning to the Browse tab now re-checks the first page and
  re-renders only when it actually changed — no flash or scroll jump when
  nothing was added.

## v77.8 — 2026-06-06

### Fixed
- **Activity log stays visible once it has content.** The activity-log
  pane auto-hides while empty, but rows added at runtime (download / sync
  activity) weren't re-triggering that check — so a pane that started
  empty stayed hidden even after it filled up (you could "Clear activity"
  but never see it). It now re-evaluates visibility on any content change.

## v77.7 — 2026-06-05

Rollup of everything since the last public release. Highlights:

### Added
- **New maintenance tools** (Settings → Tools): back-fill missing video
  durations (local ffprobe, no network), repair legacy auto-caption
  transcripts, restore sentence punctuation to transcripts, scan & fix
  transcript/index drift, and reset a channel's sync state. Each shows a
  dry-run / confirm step before doing anything.
- **Fix-missing-video-IDs tool** (Settings → Metadata) to backfill IDs for
  older archived videos from their `.info.json` sidecars.
- **Search year-range filter** with auto-apply and inverted-range handling.

### Fixed
- **Sync no longer stalls after a large channel downloads.** Post-download
  maintenance (folder-size recount + caption/thumbnail cleanup) used to run
  inline and walk the entire channel folder on every download — several
  minutes on very large channels. The size recount now uses an indexed
  lookup, and the cleanup passes run in the background, so the pass moves on
  immediately.
- **Unreachable videos give up after 3 tries.** A video that repeatedly
  can't be reached (network/CDN timeout) is now retried for three syncs,
  then skipped and recorded — instead of erroring on every sync forever.
  The strike count is stored independently so a concurrent write can't lose
  it.
- **Reliable video-ID capture at download.** Newly downloaded videos always
  record their ID (with an `.info.json` sidecar fallback), so metadata,
  search, and the Browse views work for them. The disk sweep also backfills
  the ID for any older entry that's missing one.
- **Search matches hyphenated / punctuated terms.** Queries like
  "well-known", "co-op", or ones containing quotes or "%" now return
  results instead of silently matching nothing — in both the Search box and
  the word-frequency Graph. The `AND` / `OR` / `NOT` / `"phrase"` / `word*`
  operators are unchanged.
- **Graph:** switching the chart type back from Word Cloud to Line/Bar now
  re-renders correctly, and the weekly bucket plots real per-week data.
- **Watch → Back** returns to where you opened the video from (Search,
  Graph, or Bookmarks) instead of always the Videos grid.
- **Bookmarks** save the clicked transcript moment's real timestamp instead
  of 0:00.
- **Browse** sidebar shows the transcript-segment count; the Download tab's
  resolution defaults to the configured default resolution.
- Single-URL downloads use the same network-timeout bounds as the sync path.

### Changed
- "Rescan archive" now confirms before running (it mutates the index).
- Adding a channel rejects illegal characters in the folder name.
- About dialog wording normalized to "YTArchiver".

## v76.2 — 2026-06-03

### Added
- **Auto-sync timing modes.** Choose between a *timer* (counts down a
  fixed interval from the last run) and *clock-aligned* (fires at fixed
  wall-clock times, e.g. on the hour). The next-run indicator updates live.
- **Channel sort options in Browse.** Sort the channel grid
  alphabetically or by most-recently-downloaded.
- **Global "Videos" view.** Browse and sort every archived video in one
  place — by date added, upload date, view count, or like count — with
  lazy loading so large archives stay responsive.

### Changed
- **Settings now save automatically.** The Save button is gone; every
  field persists the moment you change it (selects and checkboxes on
  change, the folder fields when you pick a folder, the disk-staleness
  field on commit), with a small "Saved" confirmation in the footer. An
  invalid staleness value now reverts instead of silently saving.
- **"GPU tasks" renamed to "Processing tasks"** across the UI and tray,
  since the queue covers more than GPU work.

### Fixed
- **A downloaded video's YouTube ID is now reliably captured at download
  time.** Closed the gaps where a freshly-downloaded file could be
  recorded without its ID (which broke thumbnails and metadata refetch):
  the ID is bound to the file through yt-dlp's own download metadata when
  filename matching misses, an explicitly-provided ID is no longer
  rejected, and a routine re-scan can no longer overwrite a known ID with
  a blank one. If a file genuinely can't be matched, it's logged loudly
  rather than silently dropping the ID.
- **Settings dropdowns no longer show a horizontal scrollbar** at narrow
  window widths — the menu grows to fit its options instead of clipping
  them behind a scrollbar.

## v75.8 — 2026-06-03

### Added
- **Onboarding now surfaces YouTube sign-in status.** The setup wizard
  shows whether Firefox is installed and whether it holds YouTube cookies,
  and explains up front that YTArchiver authenticates to YouTube via
  Firefox cookies — so users learn about a missing/expired sign-in during
  setup rather than hitting a cryptic error at first download.

### Changed
- **Cookie source is now Firefox-only.** Chromium browsers
  (Chrome/Brave/Edge/Vivaldi/Opera) are no longer auto-selected for cookie
  extraction — their cookie stores are app-bound-encrypted on Windows and
  can't be read, which previously surfaced as a confusing "could not get
  chrome cookies" error at download time on machines without Firefox. The
  app now uses Firefox cookies if present, a user-provided `cookies.txt`
  (dropped in the app's config folder) if not, or runs unauthenticated
  (public content) otherwise.

## v75.7 — 2026-06-03

Restores first-run onboarding and lands a batch of UI/correctness fixes.

### Added
- **First-run setup wizard.** A guided first-launch flow: pick an
  archive folder, then install the required download tools (yt-dlp +
  ffmpeg/ffprobe, fetched into an app-managed folder that's added to
  PATH) and, optionally, the AI-transcription stack (Python 3.11 +
  faster-whisper + torch, with a CUDA build when a GPU is present).
  Re-openable any time from **Settings ▸ Tools ▸ "Run setup again"**.
  This replaces the dependency-install onboarding that was lost in an
  earlier rewrite, where a fresh machine only saw "missing dependency"
  errors with no in-app way to install them.

### Fixed
- **Browse ▸ Search "Year" filter did nothing.** The from/to year
  inputs now constrain both transcript and title results.
- **Title search treated `%` and `_` as wildcards** — they now match
  literally (proper LIKE-escaping).
- **Result count was silently capped** with no hint; it now shows
  "N+ (capped)" when more matches exist than are displayed.
- **Settings ▸ Tools ▸ About** threw a script error instead of showing
  version/info.
- **Transcript-drift "Scan & fix" channel picker** listed every entry
  as "(channel name missing)"; channels now populate correctly.
- **Index Statistics** could spin "loading…" indefinitely on large
  archives; it now falls back gracefully after a grace period.
- **Bookmark CSV export** wrote an unsubstituted placeholder in the
  "created" column; it now writes a real timestamp.
- **Per-video "Redownload"** showed "undefinedp" and reused the
  whole-channel dialog; it now has a proper per-video resolution picker.
- **Browse ▸ Channels** could show only one channel on entry until the
  filter was toggled; the grid now resets to show all channels.
- Zero-result searches left the previous transcript in the reader pane.
- "1 matches" → "1 match" (pluralization).
- "Open folder in Explorer" relabeled to "Open folder" (the OS default
  file manager is used, which isn't always Explorer).
- Channel-art thumbnails and the art-cache marker file now get the
  hidden attribute consistently.
- Full-backup **Export** now defaults to a `.zip` filter, matching the
  Restore dialog.

## v75.3 — 2026-05-23

Conservative ship: small set of verified user-visible fixes on top of
v74.8. A larger audit pass was deferred when a regression surfaced
that couldn't be cleanly isolated; the un-shipped audit work remains
on a local branch for future re-landing.

### Fixed
- **Fully-synced channels reported a spurious "— 1 error" summary
  on every pass.** yt-dlp returns exit code `101` when
  `--break-on-existing` aborts a multi-playlist iteration, which
  happens on every fully-synced channel-root URL (the root expands
  to Videos + Shorts as a multi-playlist). The classifier now
  recognizes `101` as a normal exit alongside `0` and `1`.
- **"yt-dlp crashed with no output" diagnostic** demoted from Simple
  mode to Verbose-only — these crashes are almost always transient
  (network blip, cookie expiry) and the channel row's "— N error"
  summary stays visible either way so the failure isn't invisible.
- **"⏳ Metadata queued…" placeholder under-indented by 5 spaces**
  relative to its eventual "✓ Metadata downloaded" replacement.
  Now aligned with both the replacement line and the existing
  "Transcription queued…" sibling.

### Changed (internal)
- Removed five vestigial sub-queue lists (`reorg`, `video`,
  `transcribe`, `redownload`, `metadata`) from `QueueState`. They
  were initialized, persisted, loaded, and counted, but no code
  outside `queues.py` ever appended to or popped from them.
  Redownload / transcribe / metadata-refresh tasks all ride the
  `sync` queue with a `kind=` discriminator. ~50 lines of dead
  state removed.
- `QueueState.add_listener` + `_notify` snapshot now both acquire
  `_lock` so the rest of the class's "all shared mutable state
  goes through the lock" invariant is consistent.
- `_push_indicator` uses `json.dumps` for JS string encoding
  rather than a manual `\\` / `'` escape chain — handles control
  characters in folder names safely.
- `pyproject.toml`'s `version` field bumped in sync with
  `backend/version.py`'s `APP_VERSION`; a comment notes that
  `version.py` is the authoritative source.
- `YTArchiver.spec` comment about the Python 3.11 venv location
  no longer references the long-removed `backend/transcribe.py`
  (the module is now `backend/transcribe/`).

## v74.6 — 2026-05-20

### Fixed
- **Browse > Search**: when both Transcripts and Video Title legs ran,
  each result row's `[title]`/`[transcript]` source badge was leaking
  as escaped angle-bracket HTML instead of rendering as a styled pill.
  The badge was concatenated as a raw `<span>` into `r.snippet`, but
  the per-row renderer's XSS hardening treats every non-`<mark>` chunk
  as literal text. Now built as a real DOM node in the render loop.
- **Sync pause/resume**: when the user paused mid-pass while an inline
  metadata fetch was queued, the task would bail out silently and never
  re-enqueue on resume — the `⏳ Metadata queued…` placeholder stayed
  in the log forever and the activity-log metadata counter for that
  video stuck at 0. The task now poll-waits on `pause_event` in 0.5s
  slices (cancel-checked) so it resumes naturally when the user clicks
  Resume and the done-line replaces the placeholder.

## v72.x – v73.x — Continued decomposition + bug audit

Continuation of the v71.x organization series, plus a deep
user-facing bug audit before pushing.

### Changed (structural splits)
- **`backend/sync/core.py`** (3,199 → 2,089 lines): extracted
 `sync_all.py` (multi-channel batch orchestrator) and
 `sync_helpers.py` (pure file/format helpers).
- **`web/indexControls.js`** (1,503 → 183 lines): the mislabeled
 three-concern merge got split — `metadataTab.js` (Settings →
 Metadata refresh-status table), `settingsInfra.js` (Settings
 sub-tab nav + Archive Roots), and `indexControls.js` now just
 the Index sub-tab. Also moved the orphaned `filterCurrentView`
 function into `browseView.js` where it's actually called.
- **`backend/transcribe/core.py`** (2,420 → 1,960 lines): extracted
 `helpers.py` (path/title/duration helpers, `find_python311`) and
 `punct_manager.py` (PunctuationManager subprocess wrapper).
- **`backend/index.py`** (2,508 → 1,498 lines): extracted
 `index_search.py` (FTS5 + title search), `index_graph.py` (word
 frequency / bucket totals), `index_bookmarks.py` (bookmark CRUD),
 `index_maintenance.py` (sweep / prune / FTS rebuild).
- **`web/styles.css`** (5,075 → 288 lines): split into 8 themed
 sheets loaded in cascade order — settings, download-controls,
 logs, tabs-data, browse, browse-grids, watch, dialogs.
- **`web/index.html`** is now a build artifact assembled at boot
 from `index.template.html` + 7 partials (`tab-*.html`,
 `popovers.html`, `dialogs.html`, `modals.html`). The
 `backend/html_assembler.py` helper rebuilds it on every launch
 if any partial has been modified.
- **`backend/metadata/refresh.py`** (1,368 → 33 lines): now a
 re-export shim. The three big refresh functions live in
 `refresh_views.py`, `refresh_comments.py`, `refresh_fetch.py`;
 lazy proxies into `core.py` live in `_refresh_proxies.py`.
- **`web/settingsTab.js`** (1,133 → 580 lines): 4 dialog wirings
 extracted to their own files — `driftScanDialog.js`,
 `compressDryRunDialog.js`, `repairCaptionsDialog.js`,
 `punctRestoreDialog.js`.
- **`web/app.js`** (685 → 146 lines): extracted `uxPolish.js`
 (tooltip + defocus), `removeChannel.js`, `queuePending.js`,
 `refreshSizes.js`, `smallInits.js`. App.js is now a clean
 boot orchestrator.

### Fixed (audit + bug-hunt findings)
- **Search blocked by sweep / ingest**: read-only queries in
 `index_search.py` and other modules were acquiring the writer
 lock `_db_lock`, queueing behind long-running sweeps. Swapped
 11 read paths to use the dedicated `_reader_open` + `_reader_lock`
 pattern. Browse Search now returns in ~50ms even while a sweep
 is running.
- **Transcribe / auto-caption ID lookup blocked by sweep**: the
 `_extract_video_id` fallback in `transcribe/helpers.py` and
 `transcribe/transcribe_vtt.py` was on the writer lock; switched
 to reader.
- **`get_segment_context` blocked by sweep**: clicking any search
 result hit this code path on the writer lock.
- **Critical: `_JOB_COUNTER` was lost in the transcribe split.**
 `TranscribeManager._transcribe_one` used `global _JOB_COUNTER`
 but `_JOB_COUNTER` moved to `helpers.py`; every transcribe job
 crashed with `NameError`. Fixed by mutating through the helpers
 module reference (`_h._JOB_COUNTER += 1`).
- **Critical: `register_video` undefined in `index_maintenance.py`**.
 The startup sweep called bare `register_video(...)` but the name
 was never imported. Every new video the sweep encountered would
 crash. Fixed to `_idx.register_video(...)`.
- **Missing `sqlite3` and `os` imports in `index_search.py` and
 `index_graph.py`**: `except sqlite3.Error:` and `os.path.isfile`
 would have thrown `NameError` on first error / first call.
- **Settings persistence**: 4 watch-view preferences
 (`transcript_font_size`, `transcript_pane_width`,
 `caption_overlay_size`, `caption_overlay_bg`) were either
 write-only or silently dropped by `settings_save`. Now all
 round-trip cleanly through config.
- **Clear button → activity-log visibility refresh** wasn't firing
 because of a bare-name call in a different IIFE
 (`syncActivityLogVisibility()` → `window._syncActivityLogVisibility?.()`).
- **Recent tab "Delete File" button stayed visible after tab
 switch**: `chrome.js` referenced the wrong button id
 (`recent-delete-btn` instead of `btn-delete-file`).
- **`edit-res-reset` button (↻ Reset resolution to default) had no
 handler at all** — clicking it did nothing. Now wires the
 dropdown back to its default-selected option.
- **`initSubsContextMenu`, `initQueueAutoCheckboxes`, `initGraphView`
 never ran** — app.js called them bare-name from its IIFE but
 they lived in other IIFEs (silent ReferenceErrors swallowed by
 the surrounding `try/catch`). Sub right-click menu, GPU/Sync Auto
 checkboxes, and Graph tab were all broken. Each is now exposed
 via `window.*` and called through that.
- **`syncSubbed.js` `_inFlight` reference broke clicks**: the
 Sync Subbed and Pause click handlers were wrapped in an
 undefined `_inFlight(...)`. Inlined a local copy.
- **Boot-time UI lock — video clicks froze while Settings → Index
 stats warmed up**. `get_index_db_stats` runs three multi-second
 aggregate queries (`COUNT(*)` on a multi-million-row segments
 table, plus two duration `SUM`s) and was holding the shared
 `_reader_lock` for the entire duration. That lock serializes every
 other reader in the app — browse-grid clicks, watch-view transcript
 loads, recent-tab actions — so launching the app appeared to lock
 up until the stats query finished. The slow stats path now opens
 its own dedicated read-only SQLite connection; SQLite WAL supports
 concurrent readers across separate connections, so the UI stays
 responsive throughout boot.
- **"Scanning disk" indicator appeared seconds late**. The
 `startup_ready` bridge call (which kicks off Stage 2's disk walk
 + indicator) was the LAST step in the initial seed chain, so the
 indicator couldn't show until six earlier sequential bridge calls
 had returned. Moved it to fire first, in parallel with the rest of
 the seed.
- **"Last Full Sync" label took up to a minute to populate**. The
 ticker's first tick ran at `DOMContentLoaded`, before pywebview
 injected its API; the call silently returned and the next tick was
 60 seconds later. Now retries on `pywebviewready` + a short poll
 fallback, matching the pattern other UI restore paths already use.

### Changed (UI polish)
- **Sync log: nest metadata + transcription rows under the video
 they belong to**. When a sync catches a new download, the
 "Metadata downloaded" and "Transcription (…)" rows now indent
 six spaces so they read as children of the parent " — ✓ Title
 (size)" video row instead of being visually at the same level as
 the channel header. Standalone retranscribes (Watch view,
 Transcribe File, drift retranscribe) keep the original
 single-space indent because they don't have a parent video row.
- **Transcription done line lands at its video row even when sync
 ends mid-Whisper**. Previously, if a long Whisper transcription was
 still running after "Pass complete" emitted, the next progress
 tick saw `_anySyncRunning()` flip to false, tripped the
 pin-to-bottom heuristic, and yanked the line out of its
 per-video slot. The "— ✓ Transcription (…)" done line then
 followed it to the log bottom instead of replacing its
 placeholder under the channel's video row. Pin-to-bottom now
 honors the `tx_done_<vid>` marker — any line tied to a specific
 video slot stays where it belongs.
- **"Punctuation model loaded (CUDA)" log line hidden in Simple
 mode**. It's a one-time subprocess-startup diagnostic with no
 per-job marker, so in Simple mode it ended up orphaned at the
 log bottom after the per-video transcription block had already
 finished. Verbose mode still shows it.
- **yt-dlp's transient "Retrying (1/10)" notices no longer flagged
 as errors**. When yt-dlp hits a network hiccup mid-download
 (partial read, server 5xx, etc.) it logs a `Got error: …
 Retrying (N/M)…` line and tries again on its own — almost always
 successfully. The sync's red-error classifier was matching these
 as real failures, scaring the user with a red line and bumping
 the per-channel error counter even when the download completed
 fine on retry. Lines containing `Retrying (N/M)` now render
 dimmed and don't increment the error count. A real final
 failure (no "Retrying" suffix) still surfaces as a red error.

### Removed
- **`tests/` directory**. The import + re-export smoke suite was
 added during the v71.x package-decomposition pass to catch the
 bug shape that bit during file splits (missing imports, deleted
 helpers behind re-export proxies, lost `__init__.py` symbols).
 Now that the splits have stabilized, the suite was never run
 automatically (no CI, no pre-commit hook) and just shipped as
 dead weight. The `[tool.pytest.ini_options]` block and the
 `pytest` dev-dependency in `pyproject.toml` were also removed.

### Fixed (sync log join)
- **DLTRACK orphan warning no longer fires when yt-dlp sanitizes
 intermediate Destination paths differently from the merged
 output**. When a video title contains a Windows-illegal char
 (`"`, `:`, `?`, `|`, etc.), yt-dlp may strip it entirely from
 the intermediate `.fNNN` track filenames but render it as a
 fullwidth Unicode substitute (`＂`, `：`, `？`, `｜`) in the
 final merged `.mp4`. That meant the path key stored under the
 Destination-derived intermediate path didn't match the DLTRACK
 lookup that used the merged-output path. Now the `[Merger]`
 line cross-stamps `_path_to_counter` with the merged path
 pointing to the same counter, so the DLTRACK join finds it
 cleanly. A "youngest pending counter" last-resort fallback was
 also added — if path, basename, AND video-ID lookups ever all
 miss in some unforeseen edge case, the counter for the only
 pending download is necessarily this video.
- **If a DLTRACK orphan ever does occur, the diagnostic line is
 now hidden in Simple mode**. The previous `[ytarchiver.backend.
 sync.core] DLTRACK orphan: …` warning was visible in both
 modes; demoted to a debug-level message so Simple mode users
 don't see internal book-keeping noise when nothing actually
 failed (the download still succeeded, just the in-place log-row
 replacement fell back to a fresh marker). Verbose mode still
 surfaces the diagnostic with a ⚠ prefix.

### Added
- **Search result sorting** (Browse → Search): new Sort dropdown
 with Relevance (default), Newest, Oldest, By channel, By title.
 Changing the dropdown re-runs the search automatically.
- Build-time HTML assembler (`backend/html_assembler.py`) so
 `index.html` can be split into partials while still loading as
 a single file at runtime.

### Privacy / public-content cleanup
- LICENSE copyright holder set to GitHub username.
- CHANGELOG / docs / code comments scrubbed of personal-name
 references and absolute paths to the developer's machine.
- Companion-display integration (sync-progress JSON writer) renamed
 to generic terms throughout the codebase; the module is now
 `sync/display_push.py`.

## v71.x — Internal-code-organization series

This stretch was a deep cleanup pass after the codebase had accreted
to a single 10,200-line `web/app.js` plus large legacy.py files in
each backend package.

### Added
- `LICENSE` (MIT)
- `requirements.txt` with pinned dep ranges
- `ruff.toml` linter config + applied 1,090 safe auto-fixes
- `tests/test_imports.py` — pytest smoke suite (125 assertions, ~0.3s)
- `docs/` folder grouping `ARCHITECTURE.md` / `BUILD.md` / `PROJECT_MAP.md`
- `CONTRIBUTING.md` cross-references between docs
- `__all__` lists in each `core.py` (sync, transcribe, metadata)
- External-tool startup toast when `yt-dlp` / `ffmpeg` missing on PATH
- `CHANGELOG.md` (this file)

### Changed
- **`web/app.js` cut from 10,218 → ~685 lines** (~93% reduction). Code
 redistributed across 35+ focused modules under `web/`:
 - **Boot / shell:** `chrome.js`, `shortcuts.js`, `queueBlink.js`,
 `logContextMenu.js`, `seedLogs.js`, `missingFolders.js`
 - **Tabs:** `downloadUrl.js`, `downloadDragDrop.js`, `clearButton.js`,
 `editChannel.js`, `syncSubbed.js`, `autoSync.js`, `liveDrawer.js`,
 `columnSort.js`, `columnWidth.js`, `recentContextMenu.js`,
 `browseView.js`, `browseContent.js`, `browseSearch.js`,
 `browseContextMenus.js`, `bookmarks.js`, `watchActions.js`
 - **Settings:** `settingsTab.js`, `indexControls.js`, `aboutDialog.js`,
 `diagnosticsDialog.js`, `manualTranscribe.js`, `queuePopovers.js`,
 `autorunHistory.js`
 - **Modals + dialogs:** `modals.js`, `toasts.js`, `dropdown.js`,
 `appDialogs.js`, `redownloadSampleModal.js`
 - **Shared state:** `browseState.js` (canonical `window._browseState`)
 - **Browse helpers:** `graphTab.js`, `logMode.js`, `scanArchive.js`,
 `activityLogVis.js`
- **`web/logs.js` cut from 2,921 → ~895 lines** (~69% reduction).
 Log rendering stayed; everything else split out:
 - `watchView.js` (~886 lines) — embedded video + transcript karaoke
 + WebVTT caption overlay + metadata drawer. Owns `renderWatchView`,
 `_onRetranscribeComplete`, `loadWatchMetadataDrawer`, `setCaptionPref`,
 plus the rAF karaoke loop and all caption-track plumbing.
 - `browseGrids.js` (~521 lines) — Channel grid (Browse landing) +
 Video grid (inside a channel) with year/month grouping and
 lazy-load batching. Owns `renderChannelGrid`, `renderVideoGrid`,
 `_buildVideoCard` (shared with the Recent grid).
 - `tables.js` (~327 lines) — Subs channel table + Recent
 list/grid views. Owns `renderSubsTable`, `renderRecentTable`,
 `_applySubsFilter`, `_applyRecentFilter`, `_applyRecentViewMode`.
 - `queueRender.js` (~412 lines) — Sync / GPU task popover row
 builder, drag-reorder, right-click skip/cancel, verb-color tagging.
 Owns `renderQueues`, `_queueStateSnapshot`, `_anySyncRunning`,
 `_queueHasSyncForChannel`, `_queueHasGpuForChannel`. (The
 popover open/close behavior was already in `queuePopovers.js`.)
- Backend `legacy.py` files renamed to `core.py` in `sync/`, `transcribe/`,
 `metadata/` packages (the "we never finished the refactor" smell).
- Inline patch-history cruft scrubbed — 406 prefix removals across 68
 files (e.g. `Patch N (vXX.Y): foo` → `foo`, `Mirrors OLD YTArchiver.py:
 NNNN` cross-references dropped).
- `docs/PROJECT_MAP.md` + `web/README.md` updated to reflect package
 splits and the new JS module surface.

### Fixed
- **Retranscribe queueing behind startup sweep** — `transcribe_retranscribe`
 and 4 other API endpoints were using the writer `_db_lock` for simple
 reads; switched to the dedicated reader connection so clicks no longer
 queue behind `ingest_jsonl` writers.
- **Retranscribe stuck at 99% during sweep** — `sweep_new_videos` now
 yields while a GPU job is actively running. Without the yield, sweep's
 per-file writes on its independent connection still competed with the
 active retranscribe for SQLite's single-writer slot at the file level.
- **`transcribe_vtt.py` undefined imports** — missing `import time`,
 `_startupinfo`, and `_bump_transcription_pending` (extraction artifacts
 that crashed every video on the auto-captions fast-path).
- **`metadata/thumbnails_ops.py`** — `load_config()` called but not
 imported; would crash on "Realign misplaced thumbnails."
- **`compress.py` + `metadata/core.py`** — `Tuple` / `Callable` used in
 type hints but not imported (lazy-eval saved at runtime, but linter and
 any `from __future__ import annotations` removal would crash).
- **`sync/__init__.py` re-exports** — `_fmt_duration` and
 `_ROW_EMIT_PASS_ID` weren't surfaced, breaking `sync_one_channel`
 flow with `ImportError`.
- **GPU launch-pause vs new task** — launch-time pause (when restored
 queue items existed) was blocking new user-initiated tasks even with
 the GPU Auto checkbox on. New tasks now auto-clear the launch-pause.
- **Removed queue item coming back as active** — `queues_gpu_remove*`
 now also drops the matching job from `TranscribeManager._jobs`, so a
 removed pending retranscribe can't reappear as the running task when
 the worker pops the next item.
- **Whisper model picker swap-then-no-fire** — `swap_model` was firing
 unconditionally (killing the worker subprocess + emitting a log line)
 even when the picked model matched the currently-loaded one. Now
 no-op when same model.
- **Watch view shows progress for wrong video** — when retranscribing
 Video A and navigating to Video B mid-job, B's button was locked
 showing A's percentage. Now tracked per-video via
 `window._inflightRetranscribes` Map; button reflects only the on-screen
 video's state.
- **`transcribe_retranscribe` silent failures** — wrapped the bridge
 await chain in try/catch so a pywebview timeout surfaces as a visible
 toast instead of an unhandled promise rejection.

### Project layout

```
YTArchiver/
├── main.py # entry point
├── backend/ # Python — see docs/PROJECT_MAP.md
├── web/ # ~35 JS modules + index.html + styles.css
├── docs/ # architecture, build, project map
├── tests/ # pytest smoke suite
├── README.md CONTRIBUTING.md CHANGELOG.md LICENSE
├── requirements.txt ruff.toml YTArchiver.spec
└── (gitignored: CLAUDE.md, Classic Tkinter ver/, GitHub Desktop/, dist/)
```

## Pre-v70 history (imported from separate doc)

The version-per-push counter ran without a structured changelog before
the v70 cleanup series. The block below is auto-imported from the
offline dev-history dashboard generated on — covers every
push from v0.x through v64.7. There's a small gap between v64.7 (the
last captured push) and v70.0 (the start of the cleanup series above);
those few days only exist in `git log`.

<!-- AUTO-IMPORTED FROM Dev History HTML. -->
<!-- Spans v0.x through v64.7. Small gap between v64.7 and v70.0. -->

---
#### v64.7 — Fix YT auto-caption parser dropping rolled-in words and bleeding next-segment words into prior segments; add a "Repair YT auto-captions" tool to retroactively fix already-archived videos.

```
Two parser bugs were producing visible artifacts in the Watch-view
transcript for every YouTube-auto-captioned video:

1) Untagged "rollover" words at the start of each continuation cue
 got silently dropped from the per-word array.

 YouTube auto-cap VTTs deliver each new spoken word as an
 untagged prefix before the first <c> tag in a continuation cue,
 e.g.:

 00:00:02.639 --> 00:00:06.150
 Okay, so today we're driving to southern
 New<00:00:02.879><c> Jersey</c>

 The Step-1b prefix-extraction was gated to cue_idx == 0, so on
 every continuation cue the lead word ("New" here) was thrown
 away. Visible as missing words throughout the transcript:
 `New`, `>> Convertible?`, `for`, `aggressive,`, `weeks`,
 `the`, `receiving`, `this`, `because`, `across`, etc. Roughly
 one word per cue boundary disappeared.

 The fix removes the gate. The same prefix logic now runs on
 every cue with <c> tags — the leading untagged token is
 captured at cue_s as the genuine new word.

2) Adjacent segments duplicated each other's first words.

 Step 3 attached per-word timestamps to merged segments using a
 ±0.5s slop window (`seg.end + 0.5` / `seg.start - 0.5`). With
 YouTube's 10-20ms-precision timestamps, that slop pulled the
 next segment's first 1-3 words into the prior segment's array.
 Visible as duplications at segment boundaries:

 "...southern Jersey heading to | heading to a data center"

 The fix tightens both bounds to strict `[seg.start, seg.end)`.
 Each word now belongs to exactly one segment.

New Tool: Settings → Tools → "YT auto-captions: Repair…"

Re-parses YT auto-captions for already-archived videos to fix the
per-word array in place. Hits YouTube once per video, runs in the
background, writes directly to the JSONL sidecar and the segments
DB (column-only UPDATE on `words` — FTS index, row IDs, and
bookmarks all preserved). Whisper-transcribed videos auto-skipped
via the (SOURCE) tag in the Transcript.txt header.

Modal offers:
 - Scope: All channels (default) or one specific channel
 - Dry-run: fetch + parse only, don't write
 - Include YT+PUNCTUATION: skipped by default (re-parsing may
 lose punctuation a prior punct pass restored)

Refresh a video's Watch view to see the repair land — no app
restart needed.

Bug only affected the per-word `words` array. The aggregated
Transcript.txt (used for search) was correct already.
```

#### v64.6 — Fix .fNNN intermediate path leak; per-video "Refresh metadata" right-click; loading spinners on Browse tabs.

```
Three things this build.

1) Transcribe "file not found" errors when DLTRACK fires mid-merge.

 Caught two SNL "Weekend Update" videos in the morning sync with
 log lines like:

 Transcribe: file not found:
 …\Weekend Update： Mr. On Blast Speaks His Mind Again Without
 Holding Back - SNL.f135.mp4

 The actual merged .mp4 was on disk (17.6 MB), but transcribe got
 handed the .f135 intermediate path — which yt-dlp's merger had
 since deleted. Root cause was in backend/sync.py:_scan_recent_video,
 the last-resort fallback used when [Merger] / Destination parsing
 can't hand us a final path. It looks for the most recent
 .mp4/.mkv/.webm in the channel folder ctime'd within 10 min — and
 it didn't filter yt-dlp's per-track intermediates. If DLTRACK
 fired during the brief window where both `video.f135.mp4` (about
 to be deleted) and the merged `video.mp4` (just written) existed,
 scan could pick the intermediate. Then merger cleanup deletes it
 and the transcribe worker wakes up to a dead path.

 Fix: scan now skips any filename matching `.fNNN.` or `.fNNN-X.`
 before the extension. Both Mr. On Blast and the Xi Jinping
 Weekend Update were re-transcribed manually after the fix.

2) Per-video "Refresh metadata" on the grid right-click menu.

 The Watch view already had a "Refresh metadata" button — fetch
 fresh views/likes/description/top comments for one video via
 yt-dlp and write back to the channel's aggregated Metadata.jsonl.
 Surfaced it on the right-click context menu of every video card
 too (Browse > channel grid AND Browse > Recent grid), in the
 "fetch / regenerate" group between Show in Explorer and
 Transcribe now. Works for both refreshing existing metadata and
 filling in missing entries — fetch_single_video_metadata writes
 a fresh row when none exists.

 Toast feedback: "Refreshing metadata…" while in flight, then
 "Metadata refreshed." or the backend's error string on completion.
 Wrapped in try/catch with console logging so any bridge-level
 exception surfaces as an error toast instead of silent failure.

3) Loading spinners on Browse > Channels and Browse > Recent.

 During the 5–10s startup window where seedLogs is still pulling
 channel list + recent downloads from the backend, switching to
 Browse left the user staring at a fully blank tab — no header
 row, no message, nothing. Easy to think the app had broken.

 Added a centered CSS spinner + "Loading channels…" /
 "Loading recent downloads…" placeholder inside the empty grid
 containers. Renderers all start with `el.innerHTML = ""` on
 their first call, so the placeholder is naturally cleared the
 moment real data lands — no JS state to manage.
```

---
#### v64.5 — Settings: clearer helper text under "Video downloads".

```
The helper text below the single-video downloads folder said
"Where single-video downloads (URL bar on the Download tab) land
by default. Same caveat: only affects new downloads." — the
"Same caveat" was a hand-off from the Archive root description's
"existing videos stay where they are" warning, which doesn't
meaningfully apply to single-video downloads (there's no
channel-folder structure to migrate). The dangling reference
just made the line confusing.

Replaced with: "Default save location for single-video downloads
(URL bar on the Download tab)."
```

#### v64.4 — Manual-transcription done line now includes title + channel.

```
Reported from the player view: when a transcription runs OUTSIDE a
download flow (player-view Re-transcribe, "Transcribe File",
drift-scan retranscribe, Queue Pending / Transcribe All on a
channel, folder walk), the done line just read

 — ✓ Transcription (Whisper large-v3, took 49sec, 7.7x realtime)

with no clue WHICH video had just finished. Inside the download
flow that's fine — the "Downloaded — <title> — <channel>" line is
emitted by sync.py directly above it, so the user has context. Out
of that flow there's nothing above to anchor it to.

Fix: added a `from_download` flag (default False) to
TranscriptionManager.enqueue(). Only the sync.py download enqueue
passes from_download=True. Every other call site (transcribe_enqueue,
transcribe_folder, transcribe_retranscribe, chan_transcribe_all,
Queue Pending, drift-scan retranscribe path) keeps the default and
gets the new augmented line:

 — ✓ Transcription — Video Title — Channel Name (Whisper large-v3, took 49sec, 7.7x realtime)

Title and channel render in white (matches the download done line's
title color); em-dashes match the existing dim style. If channel is
unknown (loose file via "Transcribe File…" that isn't indexed), the
line collapses to "— ✓ Transcription — Video Title (...)" — single
em-dash, no trailing empty cell.

Same augmentation applied to the chunked path (>2h videos) for
consistency.

Flag is journaled to the pending-jobs file so a crash-mid-transcribe
recovers with the correct styling for the resumed job.
```

#### v64.3 — Comments-refresh log: fixed-width columns + always-shown OK.

```
Two issues reported on a comments-refresh pass:

1. Rows didn't stack vertically — the count column ("9 comments
 refreshed", "183 comments refreshed", "355 comments refreshed")
 was left-aligned, so the label position shifted with the count
 width and the trailing "(took …)" cell drifted left or right
 row-to-row.

2. "N OK" cell only appeared when N > 0. Most channels showed no
 OK at all; only the one channel with non-zero unchanged
 videos had it. Looked like the column was missing.

Fix:

- Count and OK counts are now right-aligned to 4 chars so the
 "comments refreshed" and "OK" labels start at the same column
 across every row.
- "N OK" is always shown (even at 0) whenever any fetch happened
 — matches the activity-tab row, which already had this pattern.
- "no videos in scope" and "comments refresh failed" pad to the
 same combined width as the count+OK pair, so "(took …)" lands
 in the same column on all row variants.
- Mixed result with errors: appends "N Errors" after the OK cell;
 pushes (took …) right for that row only — accepted since errors
 are rare enough to warrant the visual break.

Matching cosmetic change on the activity-tab side: secondary cell
wording flipped from "X unchanged" → "X OK" so both surfaces use
the same word for the same number.

Semantics unchanged: "comments refreshed" still counts every
successful API hit, "OK" counts the subset where the comments
dict was byte-identical to last time. The two overlap — they're
not separate buckets that sum to total-checked. Worth a future
rename if anyone reads them as separate, but no behavior change
here.
```

#### v64.2 — Watch view: rename "Captions" controls to "Overlay transcript".

```
Verbiage cleanup — the toolbar label that controlled the
word-by-word overlay was "Captions:" (with size/style selects
following it), which read like a generic captions toggle rather
than the very specific "show the current transcript word
superimposed on the playing video" feature it actually is.
Renamed:
 - Toolbar label: "Captions:" → "Overlay transcript:"
 - Size select tooltip: "Caption size" → "Overlay text size"
 - BG select tooltip: "Caption background style" → "Overlay
 background style"
 - TextTrack label registered with the <video> element (visible
 in the browser's native captions menu if the user opens it):
 "Word captions" → "Overlay transcript"

No functional change; CSS classes (`.tx-hint`, data-cap-size/bg),
settings keys (`caption_overlay_size`, `caption_overlay_bg`), and
internal IDs are unchanged to avoid churn.
```

#### v64.1 — Watch view: dedup multi-ingest transcripts.

```
Two regressions surfaced after v64.0's reader-path migration let
clicks succeed during the startup sweep:

- Transcript text and karaoke caption both showed every line twice
 (cues stacked at the same playback time).
- Source banner above the transcript ("Whisper transcription" /
 "YouTube auto-captions") was missing.

Not caused by v64.0 — caused by pre-existing data state where a
video has segments rows under more than one `jsonl_path` (e.g.,
a combined `.Channel Transcript.txt` and a year-split
`.Channel 2024 Transcript.txt` were both ingested at some point).
```

#### v63.9 and earlier never exposed this because the lock contention

```
meant clicks during the sweep timed out before rendering anything.

Fix in two matching spots:

- `get_segments` (backend/index.py) — when the caller passes only
 video_id (the Watch-view click path), pick the canonical
 jsonl_path first via `GROUP BY jsonl_path ORDER BY MAX(id) DESC
 LIMIT 1` (the most-recent ingest), then fetch only that
 jsonl_path's segments. Duplicates collapse; the user sees the
 most-recent ingest's text, which is what you'd want after a
 retranscribe.

- `_classify_transcript_source`'s video_id→jsonl_path lookup
 (main.py) — added `ORDER BY s.id DESC` to the LEFT JOIN so the
 classifier resolves to the SAME jsonl_path get_segments picks.
 Before this, plain `LIMIT 1` could land on a stale ingest's
 directory, miss the active Transcript.txt header, and return
 source=unknown — losing the banner. Now both functions agree
 on which ingest is canonical.

Doesn't clean up the duplicate rows themselves — that's a
separate housekeeping pass if you want to reclaim the space.
```

#### v64.0 — Watch view: video clicks no longer block on startup sweep.

```
Reported: clicking a video during the startup "Preloading Browse
tab" / "Indexing new files" stage hung for up to three minutes
before the player rendered. No download or sync task was running —
just the startup work.

Root cause: `get_segments` (the SQL fetch behind every Watch click)
was using the writer connection `_open()` and acquiring `_db_lock`.
The startup sweep holds `_db_lock` during its FTS-ingest writes
(many seconds per transcript), so every Watch click queued behind
the next ingest commit. Multiple back-to-back ingests stacked into
multi-minute waits.

The fix is the same migration `list_videos_for_channel` and the
Recent-tab path already had: switch `get_segments` to
`_reader_open()` and drop the `with _db_lock:` block. The reader
connection is a separate SQLite connection in WAL mode, so it
reads concurrently with the sweep's ongoing writer transaction
without ever touching `_db_lock`.

Same migration applied to `_classify_transcript_source`'s
video_id→jsonl_path lookup in main.py, which had the same
bottleneck pattern.

Net: Watch clicks during startup now go straight through; only the
browser's own video-file fetch remains on the critical path.
```

#### v63.9 — Watch view: loading state shown for ALL navigation paths.

```
Reported: clicking a video card in the Channels/Videos browse grid
flashed the literal HTML placeholders ("Video Title", "Channel ·
upload date · duration", "Select a video to play", empty transcript
pane) for several seconds whenever the user beat the transcript
preload — looked like a fully blank, broken player.

Root cause: the v63.4 loading-state paint was wired only inside
`_openVideoInWatch`, but four other entry paths set
`_browseState.currentVideo` and call `showView("watch")` directly,
bypassing the helper:

 - video-grid card click (the main browse path — main offender)
 - search-result hit click
 - search-jsonl segment click
 - filtered-grid card click

Fix: extracted the paint into a `_paintWatchLoadingState(video)`
helper and called it from `showView("watch")` itself. Every path
routes through showView, so all four bypassing handlers get
identical treatment without each having to remember the call.
The duplicate block inside `_openVideoInWatch` was removed since
showView covers it now.

The reset was also extended to clear the description/comments
drawer body. The drawer is open by default since v63.7 — without
this, the previous video's comments stayed visible until the
new video's metadata drawer load fired.
```

#### v63.8 — Watch view: word-by-word caption overlay on the video.

```
New feature: the currently-active transcript word can be displayed
as a caption overlaid on the playing video, in addition to (or
instead of) the existing transcript-pane highlight. Driven by two
new selects in the watch-actions toolbar:

 Captions: [Off / Small / Medium / Large]
 Style: [Translucent / Outline / None]

Defaults are Off (Translucent), so the feature is opt-in. Sizes
are 16/26/40px. Translucent draws the word over a 72%-opaque black
pill; Outline drops the background and adds a multi-direction
text-shadow rim; None is pure white text.

Implementation uses the native <video> TextTrack API + VTTCue:
one cue per word, using the same (s, e) timing the karaoke loop
reads from the DOM. The browser handles cue rendering itself, so
going fullscreen on the <video> element keeps the overlay visible
without any custom-controls rebuild — the original concern when
designing this. A DOM overlay sibling would have been simpler but
would vanish in native fullscreen.

Styling is keyed off `data-cap-size` and `data-cap-bg` attributes
on the <video> element with `video::cue` selectors, so changing
size or style flips an attribute and doesn't rebuild the track.
Cues are recycled per video change (clear + refill the same
TextTrack — addTextTrack creates a new track per call, which
would accumulate without cleanup).

Preferences persist via settings_save (caption_overlay_size,
caption_overlay_bg) and localStorage. Set once, survive restart.
```

#### v63.7 — Watch view tweaks: drawer-open default, hint restyle, per-video metadata refresh, toolbar wrap.

```
Four small player-section changes:

1. The Description & comments drawer now opens by default. Was
 collapsed since it landed — the rationale at the time was "keep
 the transcript dominant" — but in practice the extra click on
 every video got old. The drawer body still has its own
 `max-height: 420px` scroll so it can't crowd out the transcript
 pane.

2. The `?` keyboard-shortcuts hint in the transcript header was a
 `btn btn-ghost btn-thin` and read as a clickable action sitting
 next to Aa- / Aa+. Re-tagged as `<span class="tx-hint">` — dim
 16px circle, `cursor: help`, no click handler. Tooltip still
 lists the shortcuts on hover.

3. New "Refresh metadata" button in the watch-view action row, next
 to Re-transcribe. Runs a synchronous per-video yt-dlp fetch (top
 50 comments, view/like counts, description) and writes back to
 the channel's aggregated `.{ch} Metadata.jsonl`, then re-renders
 the drawer in place. Faster and more targeted than queuing a
 full-channel views/likes refresh when you just want fresh counts
 for the video you're watching.

 New Api method: `browse_refresh_video_metadata({filepath,
 video_id, title, channel})` in main.py — delegates to
 `backend.metadata.fetch_single_video_metadata(refresh=True)` and
 returns the canonical on-disk entry via
 `browse_get_video_metadata`. Frontend exposes
 `window.loadWatchMetadataDrawer` so the button handler can
 re-trigger the existing drawer-render path instead of forcing a
 Back-and-reopen round-trip.

4. The `.watch-actions` toolbar (Open in player / Bookmark /
 Redownload / Re-transcribe / Refresh metadata / Speed / Follow
 playback) used to be `flex-wrap: nowrap` with `overflow-x:
 auto` — at narrow window widths this turned into a horizontal
 scrollbar, which felt broken. Switched to `flex-wrap: wrap` with
 `row-gap: 6px`. Individual buttons keep their per-element
 `white-space: nowrap` so labels never break mid-word — the old
 "Open\nin\nplayer" failure mode is prevented at the per-button
 level instead of by forbidding row wrap entirely.
```

#### v63.6 — Activity log: collapse empty secondary/tertiary cells.

```
At narrow window widths the [Metdta] activity rows had a wide
gap in the middle and stray-looking em-dashes because the tertiary
count cell (used by [Dwnld] for "N metadata", unused by classic
rows) still held its 98px-minimum grid track even when empty. The
earlier "dash only when next has content" JS logic suppressed the
visible dash glyph but didn't shrink the grid track — so the gap
persisted and the adjacent dashes drifted around inside it.

Fix is content-driven:

- `buildActivityRow` in web/logs.js now tags rows with
 `hist-no-secondary` and/or `hist-no-tertiary` when those cells
 are empty.

- web/styles.css adds three new grid overrides keyed off those
 classes (one each, plus the compound case). The empty count cell
 AND its leading em-dash separator collapse to 0 width, freeing
 their reserved space to be redistributed across the 1fr tracks
 (channel / primary / errors / took). Column count stays at 14 so
 per-kind overrides still line up.

Effect: rows now read "[Metdta] time — channel — primary —
secondary — errors — took" with no internal dead space. At
narrower widths the primary cell ("17 comments refreshed") has
more room before ellipsis kicks in.
```

#### v63.5 — Metadata-kind rows: pink brackets instead of green.

```
After v63.4 landed the condensed comments-refresh line, the
`[N/total]` brackets at the start of each row were still rendering
green (sync_bracket color), which conflicts with the convention that
green is reserved for downloads. Metadata tasks should be pink
throughout — brackets included.

Fix touches three layers:

1. `_sync_row_emit` now accepts a `bracket_tag` parameter (default
 "sync_bracket" — preserves existing behavior for download rows).
 The tag is forwarded to `_bracket_segments` so the `[`, `/`, `]`
 punctuation picks up the requested color.

2. The channel-iteration loop now computes `_ch_kind` BEFORE the
 live-row emit (was after — couldn't kind-color the live row).
 For metadata-family kinds (metadata, metadata_comments,
 videoid_backfill) it sets `_row_bracket = "meta_bracket"` and
 `_row_name_tag = "simpleline_pink"`, then passes those to the
 live-row emit. The channel name appears pink too while work is
 in flight.

3. Every `_sync_row_emit` call site inside the three metadata-kind
 branches (paused / done / failed for each of metadata,
 metadata_comments, videoid_backfill — 9 sites total) now also
 passes `bracket_tag=_row_bracket`. Without this the done row's
 in-place replacement would revert the brackets to green.

Done row's channel name stays white per the original spec
("everything else white, errors red"). Only the live (in-flight)
row uses pink for the name.
```

#### v63.4 — Comments-refresh log row: condensed + tooltip fix + Clear button move.

```
Four small UI cleanups landed together.

COMMENTS-REFRESH LOG ROW — three lines collapse into one rich line.

Before:
 [1/67] Jimmy Kimmel — 285 comments refreshed · 1 errors
 — Refreshing comments for Jimmy Kimmel (last 365d)...
 — Jimmy Kimmel: comments refreshed — 285 ok, 1 errors (took 30m 2s)

After:
 [1/67] Jimmy Kimmel (last 365d) — 285 comments refreshed · 0 OK · 1 Errors · (took 30m 2s)

The done-row replaces the live `[1/67] Jimmy Kimmel` in place once the
channel finishes. Color discipline: `[ / ] ( ) — ·` all pink; the
channel name, scope, count text, and "took" white; error count red.
refresh_channel_comments no longer emits its own preamble or summary
— the sync loop owns the single-line presentation now. Implementation
added rich-segment support to _sync_row_emit: name and summary can each
be a list of [text, tag] pairs for multi-color output. String form
still works for the dozens of other callers.

SYNC TASKS POPOVER — comments-refresh tasks no longer say "Download".

The Sync Tasks list showed queued comments-refresh items as "Download
ChannelName" because _task_label_sync had no case for the
metadata_comments kind. They now read "Metadata comments — ChannelName"
and pick up the pink qv-meta color class (matches the "Metadata check"
rows), so users no longer mistake them for video downloads.

GLOBAL PAUSE BUTTON — fix doubled tooltip.

Hovering the pause button showed two tooltips stacked: the custom
styled bubble and the browser's native title tooltip. The custom
tooltip system migrated `title` → `data-tooltip` on mouseover, but
the 700ms blink-tick that paints the pause-button state was
re-setting `title` directly. Mouseover only fires on entry, so the
re-added title sat there until the user left and came back — the
browser happily rendered its native version on top of the custom one.
Fix: paint state writes to `data-tooltip` directly and removes
`title`, so the native tooltip can never be triggered.

TOOLBAR LAYOUT — Clear button moved next to Pause.

Row 3 had: `Auto-sync: [▼] [Clear ▼] (countdown)` — the Clear
dropdown wedged itself between Auto-sync's label/select pair and
broke the visual grouping. Moved Clear to Row 2 right after the
global Pause button. Row 3 is now `Auto-sync: [▼] (countdown)`
with the label and select directly adjacent.
```

#### v63.3 — Comments-refresh activity row: fill the awkward gap.

```
Per-channel [Metdta] rows for comments refresh were rendering as
"813 comments refreshed — — — 0 errors — took 1h 25m"
with an obvious empty stretch between the count and the error cell.
Root cause: the "X unchanged" value was being placed in the
TERTIARY column to match the views/likes refresh convention (which
reserves SECONDARY for "X new"). Comments has no "new" concept,
so secondary was always empty — that's the gap.

Fix: put "X unchanged" in SECONDARY for comments rows. Show it
consistently (even "0 unchanged") whenever the row actually
refreshed videos, so the column stays populated row-to-row.

New row format:
"[Metdta] 8:09am, May 16 — HasanAbi — 813 comments refreshed
 — 0 unchanged — 0 errors — took 1h 25m"
```

#### v63.2 — Download line cleanup + Cancel-task no longer drops the next channel.

```
Two issues from a screenshot review of the running app.

DOWNLOAD CONFIRMATION LINE — drop the channel name
---------------------------------------------------
Per-video done line was "— ✓ <title> — <channel> (NNN MB)". The
channel name was already shown in the [Dwnld] header line directly
above the block, so repeating it on every video done line was just
noise. Now reads "— ✓ <title> (NNN MB)".

CANCEL TASK ON RUNNING ROW — silently dropped the NEXT-popped channel
---------------------------------------------------------------------
Right-clicking the running task in the Sync Tasks popover and
choosing "Cancel task" was supposed to drop the in-flight job and
let the next queued channel run. Instead, the cancellation silently
ALSO dropped the channel that was at slot #2 — without it ever
being processed.

Root cause: sync_skip_current sets both _sync_cancel (kills yt-dlp)
and _sync_skip (tells the worker "advance, don't break the pass").
When sync_channel returned after the cancelled job, both flags were
still set. The worker then popped the NEXT channel and immediately
hit the cancel-check at the top of the iteration, which interpreted
the still-set flags as "skip THIS popped channel" — so the freshly
popped channel was logged as "skipped" without ever running.

Fix: clear cancel+skip at the top of every iteration BEFORE the
next sync_pop. The cancel-during-pause path is unchanged (those
flags get set after this clear runs).
```

---
#### v62.4 — Still-on-YT card rounding + transcript source classifier fixes.

```
Two follow-up bug fixes after the deep audit sweep.

STILL-ON-YT TOTALS CARD (rounding bug)
--------------------------------------
Card showed "100.0%" while per-row entries showed 99.8% / 99.9% for
channels with known removed videos. The card's percentage was being
computed correctly (sum of `id_total` and `removed_from_yt` across
all rows, gated by `last_views_refresh_ts >= REMOVED_DETECTION_SINCE`),
but `p.toFixed(1)` rounded 99.98% (101,968 / 101,988 with 20 removed)
UP to "100.0" — the per-row visibly showed sub-100% only because
small-base channels (e.g. 516/517) amplify a single removal into
0.2% of the total, while at archive-wide scale the 20 removals fall
into the second decimal place.

Fix: when toFixed(1) would round to "100.0" but the real value is
< 100, use toFixed(2) instead. Card now shows "99.98%" with sub-text
"101,968 / 101,988 · 20 removed" — the numerator/denominator already
carried the signal but the user had to mentally diff the two
numbers; the explicit "20 removed" makes it unambiguous.

TRANSCRIPT SOURCE BANNER (classifier bug)
-----------------------------------------
Watch view dropped the source banner ("YouTube auto-captions —
transcript is approximate · re-transcribe with Whisper for improved
results") for any video whose title contained a comma. Reproduced on
voidzilla's "i tried getting scammed, instead i got the t1 phone".

Root cause: `_classify_transcript_source` parsed the Transcript.txt
header line `===(title), (date), (time), (SOURCE)===` by doing
`body.split(",")` and taking `parts[0]` as the title. The voidzilla
title's internal comma split it into `(i tried getting scammed` +
` instead i got the t1 phone)` — the head_title compare against the
normalized request title failed, no header matched, classifier
returned unknown, `_buildSourceBanner` returned null for unknown
with empty raw → banner silently disappeared.

Fix: anchor the parse on the END of the header line with a regex
that matches the trailing `(date), (time), (SOURCE)` triple (none of
which can contain commas). The title field then absorbs whatever
comes before, including commas and even parens. The legacy
comma-split path stays as a fallback for any non-canonical header
shape we haven't seen yet.

Bonus verification: re-ran the classifier against the actual
voidzilla Transcript.txt — now correctly returns
{source: "yt_captions_raw", raw: "YT CAPTIONS"} → banner renders.
```

#### v62.2 — Large UX sweep + watch lifecycle hardening.

```
Big multi-day batch. v58.1 (last release) → v62.2 covers ~5,600 lines
of changes spread across UI polish, dialog system, Simple/Verbose
mode cleanup, watch view lifecycle, settings, metadata refresh,
toast handling.

Topics, in roughly order of user impact:

WATCH LIFECYCLE (critical bug)
------------------------------
Race-token check inside _loadVideoSource. Symptom before: user clicks
a video, the slow URL-fetch starts, user gets impatient and clicks
Back, then the URL fetch resolves and `vEl.src = url; vEl.play()`
fires anyway — video starts playing in the hidden Watch view with
no UI to stop it short of returning to the same video to pause.
Fix: _loadVideoSource captures _watchOpenToken at entry, checks it
+ _browseState.view === "watch" after the await. If either fails,
tears the element down (pause + removeAttribute + load) and bails.
Required exposing _watchOpenToken + _browseState on window so the
logs.js function could read them.

Also: title / channel / Loading-spinner now paint IMMEDIATELY on
click (before the transcript fetch) so the user gets feedback
instantly. Previously the literal HTML placeholders ("Video Title",
"Channel · upload date · duration") stayed visible until the fetch
resolved (potentially seconds).

ACTIVITY LOG
------------
- Em-dash separators between every column.
- Even column widths (channel was 2.5fr, others 0.8-1fr; all 1fr now).
- Alt-row color bumped from #101520 (invisible) to #1c2433.
- Retention bumped from 100 → 10k entries (config + JS).
- [Metdta] rows show 'N unchanged' / 'N new' in FIXED column slots
 so values align vertically across rows regardless of which optional
 columns are populated. Previously 'unchanged' jumped between
 secondary and tertiary depending on whether 'new' was present.
- '1 errors' → '1 error' pluralization.

DIALOG SYSTEM
-------------
- askChoice auto-compacts to a single row [Action][Cancel] when
 there's only one action button (no more two same-size red buttons
 stacked vertically).
- X-close dialog: [Cancel] [Close to tray] [Quit] with red Quit
 styling. Cancel button works.
- 'Metadata Already Downloaded' dialog no longer has two stacked
 Cancel buttons (removed the redundant choice).
- Modal backdrop opacity 0.6 → 0.45 (felt like a security warning).
- Paused-state cancel dialog: 'Clear queue' on left (red), 'Never
 mind' on right (ghost gray).
- Close dialog body text no longer indents weirdly mid-wrap.

TOOLTIP SYSTEM
--------------
Custom-tooltip helper now migrates title→data-tooltip on EVERY
mouseover instead of just the first. Was only migrating once,
which meant elements whose title was dynamically rewritten (e.g.
the global pause button's blink-state tick at 700ms intervals)
re-added the title attribute and got BOTH the custom bubble AND
the native browser tooltip side-by-side.

SUBS TABLE
----------
- 'Compres' → 'Compress' (typo in column header).
- Compress / Transcribe / Metadata column headers AND per-cell
 cells now have hover tooltips explaining what the A / ✓ / — marks
 mean. Cell-level tip is value-specific ("A ✓ = auto-transcribe ON
 and channel fully transcribed", "— -5 = 5 videos behind", etc.).

SETTINGS
--------
- 'When closing window' preference moved from Appearance to General.
- Bug fix: custom dropdown widget now repaints after settings_load.
 Setting sel.value programmatically updated the hidden <select> but
 the visible trigger label stayed on the HTML default. Result: a
 user with close_behavior=quit saved in config saw the dropdown
 showing 'Ask each time' (the default selected option) instead of
 the actual value. Same bug affected all four Settings dropdowns
 (Default resolution, Auto-transcribe model, Log mode, When closing
 window) but only showed up when the saved value diverged from the
 HTML default.
- Sub-nav active tab: green-tinted bg + brighter text + 3px accent
 border. Previously a near-invisible underline only.

DIAGNOSTICS DIALOG
------------------
Stops breaking short values mid-character. Was using
`word-break: break-all` which broke "(17.36 GB)" into "(17.36 G"
newline "B)". Switched to `overflow-wrap: anywhere` which only
breaks when nothing else fits.

INDEX STATS CARD
----------------
New animated spinner (.spinner-inline reusable class) replaces the
static 'loading…' text on the Segments / Hours of video / Index DB
size rows during the multi-second slow DB query. Felt dead before.

WATCH TRANSCRIPT
----------------
Aa- / Aa+ font-size buttons actually work now. Were wired correctly
via a CSS variable, but a child element (.watch-transcript-body)
had a hardcoded font-size: 13px that blocked inheritance through
to the rendered word spans.

SIMPLE vs VERBOSE MODE
----------------------
Reported issue: "Verbose mode is supposed to literally spit out
every piece of information we possibly can at the user. In simple
mode, however, everything needs to be incredibly readable and
clean. No complex jargon. no backend function name calling."

Audit + fixes:
- Backend error messages cleaned of tool-name jargon (yt-dlp,
 Whisper, ffmpeg, output_dir, bulk-stats, fast-fetch, spawn failed,
 Backfill / Compress prefixes) for Simple mode. Technical details
 preserved on a parallel 'dim'-tagged line visible only in Verbose.
 18 sites rewritten.
- Verbose mode beefed up in compress.py (full ffmpeg cmd, duration
 probe result, codec verification, bitrate calcs, encoder flags),
 reorg.py (per-video folder-routing decisions, mtime sources,
 skip reasons), metadata.py (per-video diff trace during bulk
 refresh — old→new view counts + decision), redownload.py (full
 yt-dlp cmd, format selection, url, output path).
- yt-dlp stderr captured on bulk-stats failure (was DEVNULL'd) and
 emitted on Verbose-only 'dim' lines so users in Verbose mode can
 actually debug failures.
- 'Bulk-stats returned no data for X' wording (jargon) replaced with
 Simple-mode 'Initial check unsuccessful for X — trying per-video
 lookup…' + Verbose-only follow-up with the technical detail.

METADATA REFRESH
----------------
Auto-retry bulk-stats with canonical /channel/UC.../videos URL when
the @handle form fails. yt-dlp 2026.03.17 + youtubetab:skip=webpage
can't resolve some channel @handles ("Failed to resolve url" error),
but the same channel works fine via channel-ID. The skip=webpage
arg is REQUIRED for bulk view counts (without it every count is
"NA"), so we can't just drop it. Now: on empty bulk result for an
@handle URL, spend 1 extra yt-dlp call to resolve channel_id, then
retry with /channel/UC.../videos. Saves ~25 minutes per affected
channel per refresh by avoiding the per-video fallback path.

Coverage display rewrite: 'metadata: 512/512 ✓ (1 stale)' instead
of the misleading '513/512 (0 missing)' format where the X/Y ratio
didn't add up (orphan metadata files for deleted videos were
inflating the X count).

TOAST / ERROR HANDLING
----------------------
_sanitizeErrorMsg now handles non-string inputs (Error objects,
rejected fetches, plain objects). Extracts .message / .error /
.detail / .msg fields before falling back. Final guard against
"[object Object]" / "[object Promise]" reaching the user.

MISC
----
- .spinner-inline reusable CSS class for any inline loading state.
- Watch action button row scrolls horizontally at narrow widths
 instead of wrapping individual button labels mid-word.
```

---
#### v58.1 — Drop visible orphan progress lines (simple-mode log clean-up).

```
Symptom: visible "Downloading #N 100%" orphan lines persisting
between real done-lines. Example:

 [4/84] Dr Insanity — 1 new video
 — Downloading #0 100% <-- orphan, never replaced
 — Downloading #1 100% <-- orphan, never replaced
 — ✓ Transcription ...
 — ✓ Metadata downloaded

Two distinct sources, both fixed:

CAUSE 1 — U-98 visible "#N" fallback
------------------------------------
When the progress handler couldn't resolve a title for the
current dlrow counter (yt-dlp emits progress for sidecars /
metadata / pre-flight BEFORE the main-video Destination line),
it emitted a visible "Downloading #N" placeholder. Every channel
with a fast first-tick produced a "#0" orphan that no done-line
ever replaced.

The original "UI looks stuck" risk U-98 was trying to address
turned out to be theoretical — in practice the main-video
Destination fires before any visible progress for the actual
video, so the lookup succeeds for real video progress. Sidecars
/ metadata are fast enough that dropping their ticks is invisible.

Reverted: drop the tick silently when title is unresolvable.

CAUSE 2 — DLTRACK orphan path's wrong-counter clear
---------------------------------------------------
When DLTRACK fired for a video and path-match failed (sidecar-only
Destination, slash/casing mismatch), the orphan path tried to clear
`dlrow_{_DLROW_COUNTER}` to clean up. But _DLROW_COUNTER may have
already advanced to a DIFFERENT video that's actively downloading
— clearing it wiped THAT video's progress row.

Removed the wrong-counter clear. Replaced with a post-channel
sweep at the end of sync_channel: iterate every dlrow we created
(`_path_to_counter.values()`) and emit clear_line for any not in
`_closed_dlrows`. Catches every genuinely-orphaned row regardless
of counter race.

Verbose mode unchanged — still gets every progress tick + line as
before. This is purely the simple-mode log getting closer to the
intended one-line-per-video pattern (per channel, regardless of
whether path-match succeeded for each video).
```

#### v58.0 — Sweep gets its own DB connection (unblocks sync during boot).

```
ROOT CAUSE
----------
The startup sweep_new_videos was holding the Python-level _db_lock
during the disk walk + ingest. Every per-file register_video /
ingest_jsonl call grabbed and released the same lock that sync.py's
DLTRACK handler also needs to register newly-downloaded videos.

Visible symptom: a download could finish (yt-dlp at 100%) and then
sit as "Downloading 100%" for 8+ minutes while the sweep churned
through. Preload and transcribe's FTS-ingest were stuck in the same
queue. The whole DB layer wedged behind a single sequential sweep.

WHY WAL WASN'T HELPING
----------------------
WAL mode was already enabled on the shared connection but useless
because the entire app funneled through ONE sqlite3.Connection plus
the global _db_lock. WAL only buys concurrency across DIFFERENT
connections. With one connection + Python lock wrapping every
call site, every DB op serialized.

FIX
---
- New _open_independent() returns a fresh sqlite3.Connection
 (separate from the shared _conn). Same WAL + synchronous PRAGMAs,
 longer 30s timeout (vs 10s on the shared) since it'll be making
 lots of writes back to back.
- register_video and ingest_jsonl gained an optional _conn_override
 parameter. When supplied, they use that connection AND skip
 _db_lock (via contextlib.nullcontext). When not, original
 behavior preserved (shared conn + lock).
- sweep_new_videos opens its own connection and passes it through
 to its register_video / ingest_jsonl calls. The shared connection's
 schema-init still runs first via _open() so PRAGMAs and tables
 exist. The sweep's connection is closed at the end (best-effort).

EFFECT
------
Sweep's many brief writes still go to the same DB file, but
SQLite's per-write lock (much finer-grained than the Python lock)
handles serialization. Sync's DLTRACK register_video calls now
interleave instead of queueing behind the entire sweep. Multiple
readers (preload, browse) run concurrently with the sweep via WAL's
reader-snapshot semantics. The 8-minute visible "Downloading 100%"
hang at boot should be gone.
```

#### v57.9 — User-Action Audit Parts 1-3: 12 contract-mismatch fixes.

```
Patched 12 of 14 confirmed bugs from a different KIND of audit than
the static-review audits. This audit started from the user (every
clickable element), wrote down what they expected to happen, then
traced the handler + backend chain end-to-end. Catches contract
bugs that static review misses (the X-multi-delete bug from yesterday
is the canonical example).

CRITICAL — silent failure / state corruption (4)
------------------------------------------------
- U-1 queue drag-drop cross-queue contamination: drag payload now
 JSON {queueKind, idx} so a Sync row dropped on a GPU row gets
 refused with a toast instead of corrupting the wrong queue's
 state via wrong-index splice.
- U-2 drag-reorder backend notify: calls queues_sync_reorder /
 queues_gpu_reorder after the local splice. Was reordering
 visually for one frame, then snapping back on the next backend
 push because the backend never knew.
- U-3 video_delete_file API added in main.py. The Browse grid
 right-click Delete file silently failed because the bridge call
 had no backing method. New method mirrors recent_delete_file's
 sidecar cleanup (audit F-24 list) plus drops the index DB row.
- U-4 video_redownload API added in main.py. The Watch view
 Redownload button silently failed for the same reason. New
 method looks up the video's channel via the index DB and
 delegates to backend/redownload.py.

HIGH — action runs but does the wrong thing / fires in wrong context (5)
------------------------------------------------------------------------
- U-5 edit-channel dup check: subs_check_duplicate now takes an
 optional exclude_identity parameter so the channel being edited
 isn't flagged as a duplicate of itself. Frontend now runs the
 check on Edit too; was Add-only.
- U-6 misleading "Try anyway" override removed. Backend rejects
 real duplicates regardless, so offering it as a button lied to
 the user. Replaced with a hard-block info dialog that tells the
 user to resolve the conflict.
- U-7 Whisper model hot-apply: settings_save now calls
 TranscribeManager.swap_model when the model changed, so the next
 job uses the new model. Was only applying after a full app
 restart — config persisted, but the manager's loaded subprocess
 kept using the OLD model.
- U-8 modal-aware keyboard shortcut gate: when an askq backdrop is
 open, every shortcut except Esc/Enter is blocked. Ctrl+S during
 a confirm dialog was firing Sync Subbed mid-confirmation.
- U-9 number-key tab switch (1-4) also blocked while modal open
 (same gate as U-8). Was leaving the modal floating over the
 wrong tab and breaking modal exclusivity.

MEDIUM — UX gaps / opaque feedback / risky defaults (4)
-------------------------------------------------------
- U-10 askDanger focuses Cancel by default. Enter no longer
 triggers the destructive action. Non-danger dialogs keep
 Confirm focused so the common-case "yes" flow stays one-key fast.
- U-11 backup restore preview. New import_full_backup_preview()
 reads the ZIP manifest read-only and returns file list + sizes
 + dates. Frontend renders a confirmation modal with the
 contents BEFORE committing the restore. import_full_backup()
 now accepts an optional zip_path argument so the
 preview-confirmed path is passed straight in without re-opening
 the file picker.
- U-13 channels_import surfaces per-skip reasons in a modal
 grouped by reason ("already subscribed", "missing URL",
 "URL doesn't look like a YouTube link", etc.) instead of a bare
 "5 skipped" count. Backend already returned skipped_reasons;
 frontend was discarding them.
- U-14 archive root + video downloads fields now have help text
 explaining the change only affects NEW downloads, not existing
 files. CSS .edit-gh class added for spanning hint paragraphs
 under settings fields.

NOT A BUG (skipped)
-------------------
- U-12 (auto_index_* fields not loaded into General tab Settings):
 they live in the Index sub-view, not General. They ARE loaded
 via the Index sub-view's own loadSavedAuto() on boot, and the
 General-tab Save handler doesn't include them in payload, so
 there's no overwrite risk. Audit conflated the two views.

Verified: build is 31 MB (correct = pywebview bundled). Wrong-
Python build would be 28.8 MB and pop the "requires pywebview"
dialog at launch.
```

#### v57.8 — Queue X-click no longer multi-deletes duplicate rows.

```
Bug: clicking X on a queued sync row deleted MULTIPLE rows when
the queue had duplicates (e.g. the same channel queued for both
a download AND a metadata refresh). Same problem on the GPU queue.

ROOT CAUSE
----------
queues.sync_remove(url) and gpu_remove(task_id) used list-
comprehension filters that dropped EVERY row matching the
identifier:
 self.sync = [c for c in self.sync if c.get("url") != url]
The X click is a per-row action, so even first-match would be
wrong (would delete the FIRST duplicate when the user clicked
the second). Index-based removal with an identity guard is the
correct semantic.

FIX
---
- New queues.sync_remove_at(idx, expected_url, expected_name)
 and queues.gpu_remove_at(idx, expected_path, expected_bulk_id)
 remove exactly the slot at that index. Identity guard refuses
 the delete if the slot doesn't match the expected hint so a
 race-shifted queue can't drop the wrong row.
- New Api wrappers queues_sync_remove_at / queues_gpu_remove_at
 in main.py.
- logs.js X-click handler passes the BACKEND queue index (popover
 display index minus the count of running rows shown above —
 current_sync / current_gpu live in their own fields, not in
 queues.sync / queues.gpu).
- X button hidden on the running row entirely. That row's item
 isn't in queues.* at all, so an index-based delete on it would
 silently drop the next-queued item. For running items the user
 should use the right-click context menu's Skip / Cancel.
- Legacy URL-based remove updated to first-match-only as well so
 the name-fallback in main.py stops multi-deleting too.
```

---
#### v57.7 — Metadata tab loading fix + universal "Xm YYs" duration format.

```
Bundles two local-build iterations (v57.6 + v57.7) into one push.

UNIVERSAL "FOLD INTO Xm YYs" RULE
---------------------------------
Per the design rule: "always fold into Xm XXs" / "this rule should apply
everywhere". Bare "201s" should never appear in the UI.

- utils.py: new format_elapsed(secs) helper. Rules:
 * < 60s -> "Xs" ("47s")
 * < 1h -> "Xm YYs" ("3m 21s", zero-padded)
 * >= 1h -> "Xh Ym YYs" ("1h 5m 03s")
- metadata.py heartbeat uses it. The "(201s)" elapsed marker on
 the metadata-refresh in-place line becomes "(3m 21s)".
- Frontend mirrors the same formatter (JS _fmtElapsed) so the
 Loading... counter follows the same rule.

METADATA TAB LOADING (root cause)
---------------------------------
The Settings > Metadata table sometimes appeared hung at "Loading..."
for 30+ seconds. Two issues, both fixed:

1. count_video_id_status ran 3 COUNT(*) queries against the 9M+ row
 videos table per channel. With 100+ channels that was 300+
 serialized queries holding the FTS DB lock — visibly hung when
 another op (sweep, ingest, startup-time backfill_upload_ts) was
 contending for the same lock.
2. The first attempted optimization used GROUP BY LOWER(channel) —
 which forces a full table scan because LOWER() defeats the
 existing idx_vid_channel index. Still slow.

Fix:
- New count_video_id_status_bulk() collapses to ONE GROUP BY
 channel query covering every channel.
- Critically uses GROUP BY channel (raw column) so idx_vid_channel
 actually serves the query. Case-folding happens in Python after
 the result lands — case-variant channel names are merged by
 summing counts under the lowercased key.
- main.py:get_channel_metadata_status uses the bulk path, falls
 back to the per-channel query only when a channel doesn't appear
 in the bulk lookup (rare case-drift).

LOADING... ENHANCEMENT
----------------------
So the table doesn't sit silent during a slow query (which can
still happen during boot when startup tasks hold the lock):

- Live elapsed counter ticks every second in the empty-row td:
 "Loading channels... (3s)"
- At > 10s: appends "querying the index DB...".
- At > 30s: explains "a startup task (backfill / sweep / preload)
 is likely holding the lock; this clears once the green
 'Browse preload complete' indicator appears" — points the user
 at the indicator that signals when it's safe.
- styles.css: dim/smaller .md-load-info span so the secondary
 status text doesn't compete with "Loading channels..." for
 visual weight.
```

#### v57.5 — Move "Browse tab preload complete" off the activity log.

```
The green "--- Browse tab preload complete (N channels . M videos
cached) ---" milestone no longer prints to the activity log.
It becomes the persistent dim-italic text in the existing
browse-preload indicator slot, replacing the live
"Preloading Browse..." line in the same spot when stage 3 finishes.

Implementation
--------------
- main.py: dropped the s.emit_text() green emit at end of stage 3.
 Captures n_ch / n_vids from index_summary into stage-local vars
 instead.
- After stage3_done.set(), wait 0.5s (longer than the animator's
 0.4s sleep cycle) so its loop definitely sees the flag and exits
 BEFORE we push the persistent completion text. Without this, the
 animator's last in-flight iteration could race-overwrite the text
 with a stale "Preloading Browse..." line.
- Removed the animator's own _push_indicator(slot, None) cleanup on
 loop exit — caller (post-stage-3 block) now owns the final
 indicator state. Sweep slot is hidden, preload slot gets the
 completion text.
- CSS unchanged: .preload-indicator already styled dim italic.
```

#### v57.4 — Pause-pending blink, metadata heartbeat, Index DB stats split.

```
Multi-session bundle (v56.9 -> v57.4) with three coordinated themes.
Released as a single GitHub tag because the local-build iterations
weren't pushed individually.

PAUSE-PENDING FEEDBACK
----------------------
- Resume button BLINKS when you click Pause but the worker is still
 finishing its current operation (e.g. mid-channel during a
 metadata refresh that takes minutes per channel). Goes solid
 Resume the moment the worker actually parks at its pause-wait.
- Held visible for a minimum 1.5s after the click so a fast
 pause-handshake (yt-dlp streaming output, per-line check fires
 within ~50ms) doesn't skip the user-visible feedback.
- Worker hooks added across every wait site that could keep the
 user waiting > 1 second:
 * sync.py: _wait_if_paused (between channels)
 * transcribe.py: outer between-jobs gate, per-chunk wait inside
 chunked transcription, per-segment wait inside the chunk's
 read loop
 * metadata.py: _flat_playlist_bulk_stats catalog walk,
 bulk_refresh_views_likes per-video re-fetch, fetch_metadata_for_videos
 outer + inner waits, backfill_video_ids per-file,
 refresh_channel_comments per-video
 * redownload.py: _fetch_yt_catalog walk, pause-on-entry,
 per-file pause between videos
- New "Paused at H:MMpm — <channel> (<operation>) — click Resume."
 + "Resumed at H:MMpm." log lines for every metadata wait site
 via _enter_pause_wait / _exit_pause_wait helpers. You can see
 pause take effect in the activity log, not just via the button.
- Fixed a related bug: metadata / metadata_comments / videoid_backfill
 kinds were not calling set_current_sync, so the Sync Tasks popover
 head row stayed empty during those passes — which broke the blink
 condition (paintBlinkState requires a running head row to compute
 sync_running=true). Now sets it for non-download kinds too.
- CSS: .btn-pause.pause-pending + .popover-footer-btn.pause-pending
 blink animation between green and blue at 1s cycle. Honors
 @media (prefers-reduced-motion: reduce) with a solid teal
 fallback so the state is still distinguishable without motion.

METADATA REFRESH HEARTBEAT
--------------------------
- bulk_refresh_views_likes spawns a daemon heartbeat thread that
 re-emits the in-place "Refreshing X..." line every 3 seconds with
 elapsed time + current sub-phase. No more silent freezes on cold
 yt-dlp startup or during the long re-fetch loop.
- Phases the heartbeat surfaces:
 * "fetching catalog from YouTube"
 * "matching local files"
 * "writing updated counts"
 * "re-fetching details for N updated videos"
 * "refreshing metadata [N/total]"
- Catalog count is folded into the heartbeat phase string:
 "Refreshing Bernie Sanders -- fetching catalog from YouTube
 . 2,500 videos in catalog (54s)"
 instead of a separate competing in-place line. One active line
 per channel rather than two side-by-side.
- _flat_playlist_bulk_stats accepts a progress_cb. When provided,
 it skips its own backfill_progress emit and calls the callback
 instead. Other callers (backfill_video_ids) unaffected.
- Heartbeat stops cleanly before the final clear_line + summary
 so it doesn't double-paint over the completion line.

INDEX STATISTICS PANEL
----------------------
- Backend split: archive_scan.index_summary() returns ONLY the
 fast basics (channels, videos, total size, transcribed %).
 The slow FTS DB queries (COUNT(*) over millions of segments +
 duration sum + DB file stat) moved to a separate
 archive_scan.index_db_stats() helper.
- main.py: new get_index_db_stats Api method.
- app.js Settings panel: renders index_summary basics IMMEDIATELY,
 then async-fetches index_db_stats and re-renders Segments /
 Hours of video / Index DB size when ready. Shows "loading..."
 for those rows in the interim.

STARTUP REGRESSION FIX
----------------------
This fix unblocks a startup hang introduced earlier in the day:
the boot sequence's get_index_summary step was running the slow
FTS queries synchronously. On a 9M+ segment / 16 GB DB the
queries took long enough that startup_ready() never fired ->
_setReady(true) never fired -> Sync Subbed stayed disabled ->
the 3-stage startup log (deps / disk scan / preload) never
printed. App appeared "stuck" even though it was technically
still running. The Index DB stats split above resolves it.
```

#### v56.8 — Audit sweep: ~50 verified bug fixes across backend + frontend.

```
Patches landed across critical, high, and medium severity buckets
after a deep multi-wave code audit. Every changed Python module
parses + imports cleanly; both JS files pass node --check. No
behavior change for happy paths — all fixes target silent failure
modes, edge cases, and missing error feedback.

CRITICAL
--------
- recent_delete_file now checks save_config return so a successful
 file deletion + failed config write doesn't leave a stale entry
 in recent_downloads pointing to a missing file.
- queue_auto_set returns failure when persistence fails (was
 returning {ok: True, enabled: ...} even on silent save failure;
 UI showed the toggle ON but the state didn't survive restart).
- _try_auto_captions no longer raises NameError on VTT parse
 failure (the fallback emit referenced self in a standalone
 function — the outer try/except swallowed the error so users
 silently fell through to Whisper instead of seeing the parse
 failure).
- Subs table refresh after channel-redownload-cancel uses the
 correct function name (typo: _refreshSubsTable -> refreshSubsTable).
- repair_metadata_mismatches.py: phase 1 commits deferred until
 phase 2 succeeds so a phase-2 crash rolls back cleanly instead
 of leaving the archive half-fixed.
- cleanup_dup_transcripts.py: argparse default=True on --dry-run
 cleaned up to make the mutually exclusive group behave as
 advertised.
- transcribe_translate.py: ffmpeg failure now surfaces explicit
 error instead of silently failing later in wave.open.

HIGH (~20)
----------
- set_batch_cooldown URL match normalizes trailing slash + scheme
 + www variants so the 72h cooldown actually persists on
 >100k-channel bootstrap runs (was silently no-op'ing for
 URLs that didn't string-match exactly).
- whisper_worker accepts per-job language override (was hardcoded
 to "en", forcing all transcriptions through English even for
 non-English content).
- transcribe pending journal persists bulk_total + bulk_index AND
 deletes the journal after recovery so a second crash doesn't
 re-enqueue the same jobs (previously bulk count metadata was
 lost on restart and recovery could double-enqueue).
- get_segments uses AND across multiple filters (was OR — when
 main.py passed both video_id AND jsonl_path, segments from
 other videos sharing a combined transcript path got mashed in).
- queue save_debounced re-arms the timer on each call so bursty
 edits coalesce correctly into one save AFTER the burst quiets
 (early-return on existing-timer was losing late edits).
- seen_filters lookup is now O(1) via a parallel lowercase set
 (previously rebuilt the entire lowercase set on every is_seen
 call — meaningful CPU on a thousands-entry filter file).
- redownload title fuzzy match NFC-normalizes both sides so
 non-ASCII titles ("Cafe" vs "Cafe" with composed/decomposed
 accents) match across composed/decomposed Unicode forms.
- channel_art surfaces partial download failures (failed avatar
 + successful banner now returns ok=True + partial=True instead
 of silently hiding the broken-image icon in Browse).
- archive_scan logs 0-byte phantom video files (previously
 silently dropped from counts; orphans on disk had no signal).
- compress emits the first progress sample regardless of % 5
 boundary so fast encodes don't appear stuck at 0% all the way
 through.
- autorun returns overdue_seconds alongside seconds_remaining so
 overdue tasks (system asleep past fire time, blocking modal)
 aren't masked as "0 seconds remaining".
- net.probe_once spawns the 3 host probes in parallel so a slow
 Cloudflare DNS doesn't block reaching youtube.com (could
 falsely report "network down" when YouTube was reachable).
- window_state visibility floor raised from 100x100 to 250x150 px
 so windows don't restore as a tiny ungrabbable sliver on a
 smaller external monitor.
- drift_scan dedup normalizes paths via normcase + normpath so
 case-drift on Windows ("C:\\Path" vs "c:\\path") doesn't inflate
 the distinct count.
- app.js: search-context-pagination + livestream button handlers
 wrapped in try/catch so API rejections show a toast instead of
 silently dying mid-flow.
- backfill_channel_art logs download failures + mtime-check errors
 instead of swallowing them (no more silent FAILs with no detail).
- cleanup_dup_transcripts whisper_score has a deterministic
 title-based tiebreaker so consecutive runs pick the same winner
 for tied groups (previously dict iteration order made it
 non-deterministic).

MEDIUM (~10)
-----------
- Legacy duration migration emits a dim warning so users see the
 sub-1-min upgrade instead of wondering why filtering changed.
- Punctuation timeout flag surfaces in the result dict so the
 source tag can distinguish timeout vs other failure vs success.
- list_videos_for_channel uses COLLATE NOCASE on the channel match
 so duplicate filtering catches case-drifted rows.
- search_fts returns [] on FTS error instead of [{"error": ...}],
 which would KeyError when callers iterated and accessed
 r["segment_id"] on the error sentinel.
- ingest_jsonl returns the actual inserted-row count, not the
 raw segment count from the JSONL (empty-text segments were
 inflating the reported "ingested" total).
- Settings Save button disables during the in-flight save so
 fast double-clicks don't queue duplicate writes.

WAVE 5 / POLISH
---------------
- sync.py emits a fallback "#N" progress label when Destination
 line for the counter hasn't been processed yet (instead of
 silently dropping the tick — UI looked stuck while a video
 was actually downloading).
- FTS index sync failure during transcribe is now a warning
 (was emit_dim — invisible in Simple log mode while the
 transcript silently went unindexed).
- upload_ts mtime fallback flagged in the row so the UI can
 indicate "estimated date" instead of treating it as real.
- compress redo-on-larger fallback defaults to idx=0 (most
 generous) on unknown quality instead of idx=1 (Average) which
 silently demoted the user further down.
- logs.js mini-log clones strip aria-* + role attributes so
 screen readers don't announce the same line twice.
- cleanup_dup_transcripts logs malformed JSONL lines so users
 know the file is partially corrupt instead of silently
 preserving them as-is.
- app.js compress dry-run summary uses null-coalescing on
 t.videos / t.hours so "undefined" doesn't render in the totals.

STATISTICS PANEL (Settings -> Index)
------------------------------------
- Segments and Hours of video now populate (were blank — backend
 didn't provide them; frontend rendered "--" for both).
- "Total size: 4.3 TB" replaced with "Index DB size: 15.8 GB"
 (the actual .db file size). The Index Statistics panel
 describes the searchable index, not the underlying archive —
 showing the 4 TB archive size in this context was misleading.
- Index DB size pulled from the live transcription_index.db
 file size on disk; segments + hours from the FTS DB
 (segments count + sum of videos.duration_s with fallback to
 MAX(segments.end_time) per video for rows where duration_s
 is NULL).
```

#### v56.7 — Backfill: in-place progress + high-confidence fuzzy escape.

```
SYNC TASKS POPOVER
------------------
Video-ID backfill items now label as "Metadata ID fix --
CHANNEL" so they color pink (metadata family) in the queue
popover instead of falling through to the generic green
"Download CHANNEL" label.

BACKFILL LOG POLISH
-------------------
The three progress phases (catalog fetch tick, transition,
per-file match tick) now share a single in-place marker
("backfill_progress"). Each emit replaces the previous line
instead of appending, so a 10k-channel backfill shows one
updating status line instead of a scrolling wall. A
clear_line control fires right before the final summary so
the transient counter doesn't persist next to it.

Final summary tag is no longer dim when resolved=0 but
already_set>0 (the "nothing new to resolve" case). Uses the
default white for that case, pink when new work happened,
and dim only if there was literally nothing on disk.

FUZZY MATCH: HIGH-CONFIDENCE NO-DATE ESCAPE
-------------------------------------------
When the date-confirmed fuzzy path finds nothing, accept a
match if exactly ONE candidate scores >= 0.95 SequenceMatcher
ratio. "Near-identical string" similarity at 0.95+ rarely
lands on the wrong video, so this salvages cases where the
file's mtime doesn't match upload_date (re-encode, missing
--mtime on legacy downloads, tool touch bumping mtime).
Ambiguity at the 0.95 threshold still declines -- the
conservative "rather have missing than wrong" principle is
preserved.
```

#### v56.5 — Video ID backfill: multi-strategy matching + UX polish.

```
Rewrote backfill_video_ids from exact-normalized-title-only
to a five-strategy resolver. All title-based strategies
require the candidate's upload_date to land within plus or
minus 1 day of the file's mtime -- "rather have missing info
than incorrect info". A test channel with 9,510 local files
that previously sat at 13% coverage resolved to 8,235 / 9,510
in under five minutes on the new resolver.

BACKFILL STRATEGIES (in order)
------------------------------
1. .info.json sidecar -- if yt-dlp wrote one next to the
 video, the id field is used directly. Zero network.
2. Exact normalized-title match against the current flat-
 playlist.
3. Substring title match -- one side contains the other with
 length ratio >= 0.7. Requires date agreement.
4. Date + duration match -- file mtime day -> upload_date;
 duration (within 2s) disambiguates same-day candidates.
5. Fuzzy title match via difflib.get_close_matches at 0.80
 cutoff. All candidates above the cutoff are date-filtered
 and the winner must be either the only one to pass date
 OR be 0.05 clear of the runner-up.

Everything that fails every strategy gets an
id_backfill_tried_ts stamp (new column) so the UI can tell
"tried unsuccessfully -- likely renamed or removed" from
"not yet attempted -- run Fix IDs".

PROGRESS TICKS
--------------
Large channels used to look frozen between "Resolving video
IDs..." and the final summary. Now:
 * "Fetched N videos from YouTube catalog..." every 500
 items or 5 seconds during the catalog fetch.
 * Transition line: "Catalog has N videos, matching M local
 file(s)..."
 * "[N/M] matched K so far..." every 200 files or 5 seconds
 during the match loop (only for channels > 1000 files).
All three visible in Simple mode.

SETTINGS > METADATA
-------------------
Video IDs column now shows a percentage instead of a raw
ratio. 100% = green checkmark, 90-99% = neutral, <90% =
orange warning triangle, 0% = red. Threshold is a single
tunable constant (ID_WARN_THRESHOLD). Hover tooltip breaks
down missing videos: "K tried unsuccessfully, Y not yet
attempted (run Fix IDs)" vs "all tried -- likely renamed or
removed from YouTube".

ACTIVITY LOG
------------
[Metdta] rows correctly render pink on reload-from-history
now -- the autorun-history tag detector was only matching
"fetched" / "refreshed", not "N IDs backfilled".

"N IDs backfilled" highlights pink; "N unresolved" / "N
ambiguous" highlights amber. The inline-regex cell colorizer
now allows an optional noun between the count and the verb
("N IDs backfilled", "N comments refreshed") so those phrase
forms finally get colored.

Dropped the hist-row-Metdta grid override. Metdta rows use
the default 10-column template so the Errors and Took cells
land at the same x-position as Dwnld rows.

Activity log resets to the default 3-row height when it
transitions from empty to populated (e.g. first emission
after a Clear). Previously an expanded drag height was
kept, leaving a 20-row empty frame around a single log line.

OTHER
-----
Refresh comments -- all channels: label dropped the "(30d)"
hint; popup choices are now 1 month (default) / 1 year / All
videos instead of 7/30/90/all.

Settings > Tools: removed the duplicate "Metadata (all
channels)" row -- those actions already live on the Metadata
tab.

Global Resume button now uses the Sync Subbed green (var(
--c-sync)) so the two primary-action greens on the top bar
stay visually consistent.

Bulk refresh-views emits an "[N/total] fetching metadata..."
tick every 25 videos or 20 seconds so long passes on large
channels don't look frozen.
```

#### v56.0 — Audit sweep: ~60 bug fixes + resume-on-restart.

```
Fourth deep audit round. 14 parallel audits catalogued ~200
potential issues; ~60 were confirmed bugs and fixed this
release.

SECURITY + DATA SAFETY
----------------------
Local fileserver rejects requests outside the archive-root
allowlist (previously served any absolute path on disk).
Command server CORS restricted to loopback origins; LAN
bind now requires an explicit opt-in env flag. Compress
verifies output codec is actually AV1 — NVENC silent-fallback
to HEVC/H.264 can no longer be promoted as a successful
compress. Reorg surfaces sidecar move failures instead of
silently orphaning metadata in the old folder. Whisper
refuses to write empty .txt/.jsonl when the worker returns
no text. FTS cascade-deletes on prune so search can't return
phantom hits. Retranscribe is now atomic across .txt +
.jsonl; one side failing rolls back the other.

SYNC RELIABILITY
----------------
URL normalization in the config-write pass so a saved vs
live `www.` / trailing-slash difference no longer silently
suppresses last_sync / initialized / sync_complete —
channels finally graduate to the fast-path on first
success. Narrowed the bare `except: pass` around that write
so disk / permission errors surface. Removing a subscription
clears its queued sync tasks. sync_complete default flipped
True → False for safer first-sync on imported / legacy
channels. duration_s column actually populated on register.

TRANSCRIPTION + METADATA
------------------------
Bulk refresh-views emits an [N/total] progress tick every
25 videos / 20s so long passes on large channels don't
appear frozen (primary reported symptom on the Bernie
Sanders test channel, 610 videos). CUDA → CPU fallback env
resets even if the fallback job crashes (prevents "stuck
slow" until app restart). Corrupt metadata-JSONL lines are
counted and warned instead of silently dropping entries.
view_count / like_count casts guarded so one bad row doesn't
abort the whole Browse grid. channel_transcription_stats
excludes duplicate-marked rows so "32 / 50 transcribed"
matches the visible grid count.

DOWNLOADS TAB
-------------
URL history written only on successful launch, not on
submit. URL canonicalized before dispatch (fragment +
unrelated params stripped). Target folder writability
probed before launching yt-dlp. Custom-name validation
rejects empty-after-sanitize input. DLTRACK parse bounds-
checked. yt-dlp `--continue` added to sync, single-video,
and redownload commands so partial .part files resume
across restarts.

UI POLISH
---------
Activity log first-open-after-clear opens at the default
3-row height instead of inheriting the user's last drag.
Metadata (all channels) row removed from Settings → Tools
(superseded by the dedicated Metadata tab). Global Resume
button now matches the Sync Subbed green. Activity-log
Metdta grid uses a fixed secondary column so the Errors
cell stays aligned regardless of whether "N unresolved"
is present (fixes the ragged alignment the user flagged in
screenshots). Z-index swap so right-click menus no longer
float above modal dialogs. Browse back button clears the
per-video filter on the way to the channel grid. Recent-tab
row-selected highlight cleared on tab switch. filterskip
lines correctly hidden in Simple mode. Live tickers
(deferred livestreams, last-sync label) pause when the
window is hidden.

STATE + SETTINGS
----------------
New "Reset channel sync state" action in Settings → Tools
clears initialized / sync_complete / init_complete /
batch_resume_index so a channel can be re-bootstrapped
without editing the config by hand. autorun_history
trimmed to 500 entries on save so the config file can't
grow unbounded.

PERFORMANCE
-----------
Compound index on videos(video_id, channel) for faster
prune + cross-channel dedup scans. Empty-text segments
skipped during FTS ingest; malformed word-level data
coerced to a safe default.

MINOR
-----
Case-insensitive dedup for seen-filter titles. Compound-
suffix sidecars (.en.vtt, .es.srt, etc.) now attach to their
video during reorg. Cancelled redownload cleans a broader
set of temp exts (.ytdl, .frag, .tmp in addition to .part).
Redownload progress file cleared on catalog-fetch failure
so a retry can't resume against a stale catalog mapping.
Redownload pause emits a "Still paused (Nm)…" tick every
minute so a long pause doesn't look like a hang. Queue-
changed notifications fired on transcribe bulk enqueue so
the GPU Tasks popover reflects fresh items immediately.
```

#### v55.7 — Activity-log parity for metadata tasks, row spacing fix.

```
Views/likes refresh, comments refresh, and video-ID backfill
now emit [Metdta] rows in the activity log, pink-styled to
match the existing classic [Metdta] entries. Previously only
the legacy full-metadata path emitted a row; the newer bulk
paths logged only to the main log and left the activity log
empty for long refresh passes. Consolidated activity-row emit
+ history persistence into one sync-worker helper so every
metadata path produces a single identical entry.

Row layout: added a hist-row-Metdta grid override that
collapses the always-empty tertiary cell and auto-sizes the
optional secondary cell. Prior layout left ~100px of dead
space between primary and errors when secondary was empty,
making the row look unfinished.

Metadata-tab live refresh: the Settings > Metadata table's
XXm-ago timestamps now update automatically when any
metadata-kind task completes. Previously they stayed on the
pre-pass values until the user clicked Reload.

Pass complete line readability: the body text (Pass complete:,
separators, errors when zero, skipped, took Ns) switched from
the muted header color (#a0aabb) to bright white (--c-text).
On typical displays the header color was reading as dim — the
whole line looked grayed out even though only the zero-count
verb was technically tagged dim. Zero-count verbs now render
white+bold instead of near-unreadable dim so "0 refreshed"
still looks like a successful completion.

More log jargon scrub across user-facing strings:
 Bulk-refreshing X → Refreshing X
 Flat-playlist returned no data → YouTube returned no video list
 bulk path returned nothing → fast refresh returned nothing
 bulk-stats spawn/read errors → generic phrasing
 Resolving video_ids for X (flat-playlist) → Resolving video IDs for X
 Resolved N video_id(s) by title match → Matched N video(s) by title
 checking channel playlist for id match by title →
 checking channel for title match
```

#### v55.3 — Log jargon cleanup + readable 0-verb Pass complete.

```
Pass complete zero-count lines ("0 refreshed", "0 IDs
backfilled", "0 downloaded", etc.) now render in the same
green styling as non-zero counts. Previously they were tagged
`dim` (--c-dim = #4a4f5a) which made the whole line near-
unreadable on the dark log background. A successful pass
that happened to find no work is still a successful pass —
should read that way.

Dropped implementation-detail jargon from every user-facing
log line. Simple-mode users shouldn't have to decode yt-dlp
flags or internal path names:
 "Bulk-refreshing X (flat-playlist)..." → "Refreshing X..."
 "Resolving video_ids for X (flat-playlist)..." → "Resolving
 video IDs for X..."
 "Resolved N video_id(s) by title match — backfilled into
 index." → "Matched N video(s) by title — saved their
 YouTube IDs."
 "Flat-playlist returned no data for X" → "YouTube returned
 no video list for X"
 "bulk path returned nothing" → "fast refresh returned
 nothing"
 "bulk-stats spawn failed" / "bulk-stats read error" →
 neutralized to generic phrasing
 "can't bulk-refresh" → "can't refresh"
 "video_id" → "YouTube ID" in fallback-resolution lines
 "checking channel playlist for id match by title" →
 "checking channel for title match"
```

#### v55.2 — Clear button positioning fix.

```
Clear button now sits directly next to the Auto-sync dropdown
instead of being shoved to the right edge of the row. The
autorun-countdown span sitting between them had a 140px
min-width, so even when auto-sync was Off (empty countdown
text) it still reserved dead space that pushed Clear to the
right. Reordered the row: Auto-sync select → Clear → countdown
span. The paired controls now read as a group and the countdown
claims its space to their right only when it has content to
display.
```

#### v55.1 — Metadata refresh tab, video ID backfill, dialog polish.

```
Built the Settings > Metadata tab: per-channel refresh status
dashboard with sortable columns (Videos / Video IDs / Views
refresh / Comments refresh) and a compact per-row action menu.
Default sort is oldest-refresh-first so stale channels float to
the top. Channel column ellipsis-truncates at narrow widths so
the action button is never clipped.

Fast bulk views/likes refresh path: one yt-dlp flat-playlist call
per channel instead of per-video --dump-json. Orders-of-magnitude
speedup (1h17m → under a minute on a 404-video channel). Only
videos whose counts actually changed get any further work.

Title-based video_id backfill for archives with no [id] brackets
in filenames (tkinter-era default layout). Per-channel "Fix IDs"
button + "Fix missing video IDs — all channels" bulk action. DB
gets updated transparently; subsequent bulk refreshes hit the
fast path.

Comments refresh is now a separate per-channel action with
7d / 30d / 90d / all scope picker — cleaner than bundling into
views/likes, and faster when you don't need comment updates.

Dialog layout: action buttons on top row (green by default),
Cancel alone on the bottom row (red). No more mixed-row
wrapping on narrow dialogs.

Pass-complete log line reports the right verb per kind:
 "X refreshed" for views/likes refresh
 "X comments refreshed" for comments
 "X IDs backfilled" for video_id backfill
 "X downloaded" for regular sync
=== brackets are green+bold. Per-segment tag coloring on the
channel summary line (name + labels white, numbers pink).

Sync pass banner labels the pass kind: "=== Views/likes
refresh starting (N channels) ===" etc.

Bootstrap-only channel-level timestamp stamping: a brand-new
channel add shows "today" in the Metadata tab; incremental
syncs that trickle in a few new videos don't falsely stamp
the whole channel as "just refreshed".

Fixed: orphan 🔍 icon on Browse sub-views (Recent, Search,
Graph, Bookmarks, Watch) — the whole filter wrap now hides,
not just the input element.

Fixed: fetch_single_video_metadata signature mismatch in the
bulk-refresh secondary fetch path. Previous arg order had
`stream` in the title_hint slot and passed a nonexistent
`cancel_event` kwarg, so every per-video re-fetch raised
TypeError and got swallowed. Full fetches now actually run.

Many smaller audit fixes across backend + web (29 files,
~5k lines of changes).
```

---
#### v53.9 — 60-bug audit sweep + 3 new features.

```
Deep audit of every subsystem (sync, transcribe, metadata, index,
redownload, queues, frontend, boot flow) looking for issues in the
same class as the #134/#135/#136 reports: buttons that don't match
their label, state written but never read, silent feature failures,
UI going stale after backend changes. 65 findings total; 60 real
bugs fixed, 5 agent claims re-verified as already-correct and
withdrawn.

CRITICAL (3):

 C-1 sync_channel's last_sync + initialized + sync_complete
 writes referenced an undefined 'now' inside a
 try/except-pass. The NameError was swallowed silently
 every time a sync actually downloaded anything — so
 those flags never persisted, and every subsequent sync
 re-walked the channel catalog from scratch instead of
 using --break-on-existing. One-line fix
 (now = datetime.now() before the write), massive impact.

 C-2 transcription_pending counter leaked to -N on any
 Whisper error path (OOM, missing Python 3.11 venv,
 ffmpeg failure, etc.). Added a try/finally in the
 transcribe worker loop that drains unconditionally,
 with a `_pending_decremented` sentinel on the job dict
 so the success path doesn't double-decrement.

 C-3 livestreams.drop() was only wired to the UI Ignore
 button, so the deferred-livestreams journal grew
 unbounded. Now fires automatically in sync's post-
 download hook when a previously-deferred stream
 successfully downloads.

HIGH (12):

 H-1 Edit-channel compress dropdowns (batch size, quality
 level, output resolution) were write-only: save path
 captured them, load path didn't, so opening and saving
 any channel stomped stored values with HTML defaults.
 Fixed the load path to populate all three from the
 channel dict and added batch_size to the save payload.

 H-2 Compress: orphan temp file left in _TEMP_COMPRESS/ when
 os.replace failed (file locked by VLC preview, AV, etc.).
 temp_cleanup.py skips non-empty temp dirs, so it
 accumulated indefinitely. Now cleaned up inline.

 H-3 Redownload: cancelled mid-download left .part /
 intermediate files in _REDOWNLOAD_TEMP/. End-of-run
 rmdir only clears empty dirs. Added a cancel-branch
 sweep that force-removes anything matching the video_id
 or ending in .part.

 H-4 Metadata summary now reports 'N refreshed' and 'N errors'
 alongside 'N new' (both were silently dropped), and tags
 the summary red when errors > 0.

 H-5 chan_cancel_redownload invalidated the cache but didn't
 push refreshSubsTable, so the chartreuse continue-
 redownload dot stayed visible until a tab switch. Now
 pushes immediately.

 H-6 withdrawn — metadata.py:712 downloads thumbnails
 unconditionally after every fetch, so refresh mode
 already re-downloads missing thumbnails. Agent misread.

 H-7 metadata_queue_all auto-fires the sync worker
 regardless of the autorun_sync flag. Previously gated
 on autorun=True, so users with autorun off saw
 "Queued metadata for N channels" and then nothing
 happened — tasks sat parked forever.

 H-8 Reorg shared-sidecar bug: when two primary videos share
 a stem (X.mp4 + X.mkv, produced by aborted+retried
 yt-dlp downloads), moving one's sidecars orphaned the
 other. Added a _has_video_sibling check; when present,
 shared sidecars are copied rather than moved so both
 destinations get their metadata.

 H-9 Compress presets extended with 1440 + 2160 entries.
 Previously any res not in the preset table silently
 fell back to 1080p bitrates — 4K output was under-
 bitrated by ~40%.

 H-10 Retranscribe with empty segments (JSONL write failed,
 FTS ingest failed, Whisper produced nothing) used to
 leave the stale transcript on screen alongside a
 "complete" toast. Now re-renders with a placeholder +
 warning toast.

 H-11 Recent-tab delete refreshes the table immediately so
 deleted rows vanish instead of lingering until tab
 switch.

 H-12 channel_art return always includes avatar_path +
 banner_path keys (null when missing), so downstream
 <img> renders don't break when only one of the two
 was successfully fetched.

MEDIUM (14): queue auto-toggle push, subs_undo cache
invalidation, activity-log Clear UI notify, FTS rebuild
repopulates indexed_files so the unindexed banner clears,
playback speed persistence, from_date+date_after sync,
sync_start_all returns total_queued, grid metadata banner
clears on teardown, chan_transcribe_pending checks save result,
transcript source tag uses 'WHISPER:unknown' instead of
guessing, thumbnail-only refetches counted separately, Graph
Week bucket surfaces backfill_pending, register_video ingests
JSONL inline so Watch doesn't open empty.

 M-12 withdrawn — range↔mode mapping already consistent at
 both the save and load paths.

LOW (15): heal guard 'or'→'and', stable [i/total] denominator
across pauses, live-detect ID-extract warning, extended
benign-yt-dlp-ERROR allowlist, per-video metadata error lines,
auto-captions parse warning, +NO-PUNCT source-tag suffix on
punct failure, net monitor documents 60s recovery expectation,
resolution picker whitelist, bookmark note revert-on-failure,
auto-index counter migrated to `downloads_since_last_index`,
platform-stable activity-log day format, sample-confirm
countdown pauses on mouse hover.

 L-9 / L-11 withdrawn — existing confirmation + spin-stop
 logic was already correct.

SUPPLEMENTAL (9): 8 previously-silent try/except-pass blocks
with real side effects now emit dim diagnostic lines so
failures are visible instead of swallowed. Folder-rename-on-
update rolls back config if OS rename fails (previously
created disk/config ghost folders). remove_channel with
delete_files=True refuses while sync is actively writing to
that channel (no more rmtree racing yt-dlp). Empty-result
first syncs no longer stamp initialized=True and lock the
channel into fast-path mode forever.

 S-7 withdrawn — `write_blocked` is already surfaced to the UI
 in three places (app.js:4249, 4696, 6422).

DOWNLOAD-TAB / FIRST-LAUNCH (12):

 W-1 Single-video downloads previously vanished after
 landing on disk — the DLTRACK line was emitted but
 never parsed. Now the post-run hook parses it and
 calls register_video + _record_recent_download +
 livestreams.drop + pushes a Recent-tab refresh, so
 downloaded videos appear in Browse / Recent / Search
 immediately like channel-sync downloads do.

 W-2 url_history (saved all along) now populates a <datalist>
 autocomplete dropdown under the Download-tab URL input.

 W-3 About + Diagnostics dialogs had no UI buttons to open
 them. Added under Settings → Tools → "Info & Self-check".

 W-4 set_parent_folder verifies write access
 (mkdir/rmdir probe) before committing the path —
 unwritable paths no longer silently break future syncs.

 W-5 Tray → Show Window also calls Win32
 SetForegroundWindow on Windows so a hidden-behind-
 other-apps window comes to the front.

 W-6 Download tab "Save to" warns if the picked path is
 outside the archive root (files there wouldn't appear
 in Browse/Search since the scanner only walks
 output_dir).

 W-7 Single-video downloads now surface known yt-dlp error
 patterns (members-only, private, region-locked, cookies
 missing) as a visible toast instead of burying the
 error in the dim stdout dump.

 W-8 Download-tab resolution / YT-title / date-file /
 add-date preferences persist across sessions via
 localStorage.

 W-9 _should_batch_limit semantics documented in-place so
 future edits don't get confused by the flag names.

 W-10 channels_import returns skipped_reasons[] so the user
 knows which entries were rejected and why.

 W-11 GPU Tasks popover gains a mid-queue whisper-model-swap
 dropdown (the JS was fully coded but the <select>
 element didn't exist in HTML).

 W-12 Dead initParentFolderPicker legacy code replaced with
 a noop shim.

NEW FEATURES:

 F5 Watch view jump-to-next-hit. Opening a video from a
 search result passes the query through; the transcript
 Find box auto-populates; Enter / Shift+Enter cycles
 between hits in this specific video. Saves the round-
 trip back to search results.

 F7 Subs tab bulk operations. Ctrl/Shift-click multi-select
 + a bulk-actions bar appears when ≥2 rows are selected:
 Change resolution, Toggle auto-transcribe, Queue metadata
 refresh, Delete (with keep-files / delete-files choice).
 Backed by three new API methods (subs_bulk_update,
 subs_bulk_delete, subs_bulk_queue_metadata) all
 whitelisted so no load-bearing fields can be wiped.

 F8 Compress dry-run savings. Settings → Tools → Compress →
 "Dry-run savings…" opens a modal showing per-channel
 and grand-total projected disk savings at each quality
 tier for a user-chosen output resolution. Reads duration
 + size from the index DB; zero compression actually
 runs. Answers "is it worth enabling compress globally?"
 before committing.

--------------------------------------------------------------------------------
```

#### v53.8 — Disk-cache healing + metadata refresh visibility (issues #134/#135/#136).

```
Issue #134 — Subs table blank Size / # Vids after redownload.
Reported: after a channel goes through a redownload, its Size and
# Vids columns show em-dash, and the blank state survives both the
folder-size rescan button and an app restart. the user's cache had
four affected channels (Bernie Sanders, Branch Education,
Decoder with Nilay Patel, Doomscroll Podcast) — each entry
contained only a `sweep_fingerprint` key, no num_vids / size_bytes.

Root cause: the old invalidate path popped the cache entry but
didn't repopulate it. Later, sweep_new_videos used
`_fp_cache.setdefault(ch_url, {})` then wrote the fingerprint —
creating an empty row for any missing entry. The startup disk walk
is staleness-gated (24 h), so on restart the "missing-entry" check
found the fingerprint-only row and skipped it. Subs table read 0/0
and rendered em-dash forever.

Fix: three layers.
 1. archive_scan.invalidate_channel now pops AND spawns a daemon
 thread that calls update_disk_cache_for_channel to refill the
 row with fresh num_vids / size_bytes — mirrors the classic
 _invalidate_channel_disk_cache behaviour.
 2. backend.index.sweep_new_videos only stamps the fingerprint
 onto rows that already contain num_vids or size_bytes. A
 missing row is left missing so the "needs scan" detector picks
 it up; a one-off extra walk next sweep is preferable to
 permanent invisible corruption.
 3. New archive_scan.heal_malformed_cache_entries() runs at boot
 inside stage-2 disk walk. It drops rows that lack
 num_vids / size_bytes (anything that was already corrupted by
 the old behaviour) and forces do_walk = True when any were
 dropped, bypassing the 24 h staleness gate. The Subs table is
 also pushed a refreshSubsTable() at the end of stage-2 so the
 numbers show up immediately instead of waiting for the user
 to switch tabs. Verified on the user's live cache: the four
 malformed entries were dropped + rebuilt correctly
 (SenatorSanders → 1394 videos / 33.2 GB,
 BranchEducation → 41 / 9.7 GB,
 decoderpod → 60 / 26.9 GB,
 doomscrollpodcast → 46 / 25.4 GB).

Issue #135 — Rescan archive button placement.
Reported: the "Rescan archive" button in the Browse toolbar feels
out of place next to per-channel nav controls; belongs on the
Settings Tools tab with the other archive-wide maintenance actions
(yt-dlp update, channel/backup import-export, bulk metadata).

Fix: removed the button from the Browse toolbar. Added a new
"Archive" row to Settings → Tools with the Rescan archive button.
Kept button id `btn-scan-archive` unchanged so the existing
`initScanArchive()` click handler finds it without JS changes.

Issue #136 — "Refresh views/likes" button looked broken.
Reported: clicking "Refresh views/likes" seemed to do nothing
different from "Queue all metadata" — checks metadata, skips the
rest, doesn't update anything visibly.

Root cause: the backend was actually refreshing (every on-disk
video re-hit, counts merged in, JSONL rewritten), but none of the
UI surfaces reflected it.
 - The sync task row summary line read only `fetched` from the
 result dict and said "up to date" when `fetched == 0`, ignoring
 `refreshed` entirely.
 - Per-video log line used the static label "Metadata" regardless
 of refresh state, so on-screen it was indistinguishable from a
 normal scan.
 - The confirm dialog claimed "this skips channels/videos that
 already have fresh metadata" — the exact opposite of what
 refresh does.

Fix: sync.py now reads `_refreshed` alongside `_fetched` and emits
"N refreshed" as the summary (pink tag) when fetched is zero but
refreshed is non-zero. metadata.py's per-video log line emits
"Refresh — title" when `refresh and vid_id in existing` (vs.
"Metadata" / "Thumbnail" for the other cases). The confirm dialog
text was rewritten to describe what refresh actually does: every
on-disk video re-hit, previously-failed retries cleared, one
yt-dlp call per video so it's slow on large archives.

--------------------------------------------------------------------------------
```

---
#### v53.7 — Watch view narrow-width graceful stacking.

```
Reported: at ~880px window width (and anything narrower), the
Watch view's video element disappeared entirely — only the
transcript was visible.

Root cause: .watch-layout had a responsive breakpoint at
max-width:900px that set `grid-template-columns: 1fr` but didn't
adjust `grid-template-rows`. The rows stayed at `1fr` (single
row). With two grid children in a 1-col / 1-row grid, the second
child (transcript) auto-flowed into an implicit row. The parent's
fixed `height:100%` + `overflow:hidden` on #view-watch squeezed
that implicit row, hiding the video entirely behind the
transcript which occupied the explicit 1fr row.

Fix: two proper stacking tiers for narrow widths.

 @media (max-width: 900px)
 grid-template-columns: 1fr
 grid-template-rows: auto minmax(200px, 1fr)
 .watch-video-wrap {
 max-height: 40vh; /* cap video so transcript gets space */
 overflow-y: auto;
 }

 @media (max-width: 520px)
 #view-watch { overflow-y: auto }
 grid-template-rows: auto auto
 height: auto; min-height: 100%
 .watch-video-wrap { max-height: none }

At the 900px tier, the video renders at its natural aspect-ratio
height (capped at 40vh) above a transcript that fills the
remainder with a 200px minimum. At 520px and narrower, the entire
Watch view pane becomes scrollable as a document instead of
cramming two panes into a tiny dual-viewport.
```

#### v53.6 — Deferred livestreams drawer: tighter log + retry dropdown + Ignore.

```
Reported: the [Live] Deferred log line and deferred-livestreams
drawer looked sloppy. Also wanted a retry-cadence dropdown and a
per-entry Ignore button.

Log line cleanup (backend/sync.py)
- Pre-fix: dumped 140 chars of yt-dlp's raw stderr verbatim:
 [Live] Deferred is_upcoming: [download] Title is live! does
 not pass filter (!is_live & !is_upcoming & duration>?180 & …
- Post-fix: one readable line:
 [Live] Deferred — <clean title> (upcoming premiere | currently live).
- current_title (the most recent Destination-line filename stem)
 often still carried yt-dlp's `.fNNN-N` format-selector suffix,
 so the drawer showed titles like "Foo.f140-4". Strip it before
 storing in the deferred journal.
- Also split the log wording by detected state: "is_upcoming" /
 "premieres in" / "scheduled" / "starts in" / "will begin" →
 "upcoming premiere"; anything else → "currently live".

Ignore support (backend/livestreams.py)
- Added permanent-ignore set at
 %APPDATA%\\YTArchiver\\ytarchiver_livestream_ignore.json.
- ignore(video_id) adds the ID + drops from deferred.
- defer() now checks is_ignored() first and silently no-ops for
 ignored IDs, so future sync passes can't re-add them.

Drawer snooze (backend/livestreams.py)
- snooze_drawer(seconds) stamps snooze_until_ts to
 ytarchiver_livestream_drawer.json.
- drawer_state() returns {snooze_until_ts, now_ts, visible}.
- UI's refreshDeferredLivestreams() polls drawer_state first —
 keeps drawer hidden while snooze is active; 30s poll interval
 picks up snooze expiry automatically.

New js_api methods (main.py)
 livestreams_ignore(video_id)
 livestreams_snooze(seconds)
 livestreams_drawer_state()

UI changes (web/index.html, web/app.js, web/styles.css)
- "Retry all" button → "Retry ▾" dropdown:
 Now — kicks Sync Subbed (existing)
 in 24 hours — snoozes drawer 24h
 in 1 week — snoozes drawer 7d
- Each drawer row gets a new "Ignore" button alongside × (drop).
 Confirm dialog before permanently skipping. Differentiated by:
 × = forget for now (yt-dlp may re-detect + re-add on next sync)
 Ignore = permanent skip (won't re-add regardless)
- Styling: dropdown menu positioned under the Retry button, card
 background + border, hover highlight. Ignore button has a thin
 border to distinguish it from the × glyph.
```

#### v53.5 — Same-batch control ordering + live [ReDwnl] activity push.

```
(A) "Redownloading: <channel>..." active line was surviving into
the "=== Redownload complete ===" footer section even though
`_clear_active()` was being called right before the footer. Root
cause was the _logBatch filter in logs.js:
 - It ran control segments in a pre-pass against committed DOM
 ONLY, then dropped them.
 - When `_clear_active()` and the last iteration's
 `_emit_active()` landed in the same 60ms LogStreamer batch,
 the sequence was:
 [clear_line redwnl_active] (from _emit_active's 1st step)
 [new active line] (from _emit_active's 2nd step)
 [clear_line redwnl_active] (from _clear_active)
 [footer line]
 - Pre-pass ran both clear_line controls against DOM before any
 of the batch's new lines had rendered. The second clear saw
 nothing (new active wasn't in DOM yet), dropped itself.
 - Render pass then appended the new active line. Footer
 followed. Active line stuck.

Fix: rewrote _logBatch to process payload.main in a single
linear pass. Each item is either a control (execute, skip render)
or a regular emission (render into fragment). When a clear_line
control fires, it now removes matching `data-inplace` elements
from BOTH the in-progress fragment AND the committed DOM. A
clear_line that comes AFTER a new inplace line in the same batch
correctly removes it.

(B) No [ReDwnl] row appeared in the live activity log immediately
after a redownload completed, despite other recently-completed
channels' rows being visible. Root cause: the [ReDwnl] emit path
only called `autorun.append_history_entry(line)` which writes to
`config['autorun_history']` for persistence but does NOT push to
the running UI. [Dwnld]'s `emit_consolidated_auto_row` does both
paths (`stream.emit_activity()` for live UI + `append` for
persist). The [ReDwnl] finalization was missing the live push —
the row only appeared on next app launch when
`autorun_history_entries_for_ui` re-read from config.

Fix: added a `stream.emit_activity({...})` call alongside the
existing append. Cells map matches the grid layout
`autorun_history_entries_for_ui` builds from the persisted line
(primary / secondary / tertiary / errors='' / took), with
row_tag='hist_redwnl' when n_done > 0 for the chartreuse color.
```

#### v53.4 — Redownload polish: mtime match, gap fixes, activity-log layout.

```
Rolls up the morning's redownload-related fixes.

Match improvements (backend/redownload.py)
- Added _build_metadata_index() that loads the channel's
 aggregated .{ch} Metadata.jsonl files into three lookup tables:
 by_title, by_date (keyed by YYYYMMDD upload_date), by_id.
- _match_files_to_ids now has a 5-tier priority:
 (1) [VIDEOID] token in filename
 (2) Exact title match in current YT catalog
 (3) Exact title match in LOCAL metadata
 — catches videos YouTube has renamed since download; local
 filename still reflects the old title stored in our JSONL
 (4) Mtime-date match against local metadata's by_date index
 — yt-dlp's --mtime stamps file mtime = YT upload date, so
 YYYYMMDD -> video_id is a bijection for ≤1-video-per-day
 channels. Higher-frequency channels fall back to substring
 match within that day's candidates only.
 (5) Substring fallback against current YT catalog (existing)
- Reported symptom: "25/41 matched is terrible" for a low-frequency
 channel. The 16 missing files were rename-drifts caught by tier 3.

Loop gap fixes (backend/redownload.py)
- Two code paths in the main redownload loop were `continue`-ing
 without emitting any log line:
 - "vid in done" (resumed-pass entries already redownloaded in
 a prior session)
 - _download_one returning None (download failure)
- Each silent continue left a gap in the [N/total] sequence.
 Reported: numbers jumped 1294 -> 1300 -> 1314 with no log
 between.
- Both now emit a visible [N/total] one-liner: dim
 "— already done." for resumed-skip, red "— download failed."
 for errors. Each also calls _emit_active() so the sticky
 active-status line stays at the log bottom.

Completion dot clear (main.py)
- redownload_channel deletes _redownload_progress.json on
 successful completion, which is what drives the chartreuse
 pending-redownload dot in the Subs table. But the Subs table
 snapshot was already cached client-side and no signal was
 pushed to refresh it — dot persisted until the user manually
 switched tabs.
- _run_redownload_one's finally block now calls
 evaluate_js("if (window.refreshSubsTable) window.refreshSubsTable();")
 so the UI re-fetches subs data and the dot clears immediately.

Activity-log layout (backend/ytarchiver_config.py, web/logs.js,
web/styles.css)
- [ReDwnl] history rows have 3 counts (replaced · skipped ·
 errors) while [Dwnld] has 4 (downloaded · transcribed ·
 metadata · errors). The grid template has 4 count columns;
 for [ReDwnl] the 3rd column (tertiary, reserved for [Dwnld]'s
 metadata count) was empty at minmax(98px, 1fr) — creating a
 visible ~98px gap between "skipped" and "errors".
- Parser's 4-bullet branch now has a `kind == "ReDwnl"` check
 that packs all 3 counts into primary / secondary / tertiary
 and leaves the errors cell empty (instead of the reverse).
- buildActivityRow adds a `hist-row-{kind}` class per row.
- Added a CSS override for .hist-row-ReDwnl that collapses the
 unused `errors` column from minmax(60px, 0.8fr) to 0. With
 that the "took" cell snaps tight to the counts instead of
 floating out by 60px+.
```

#### v53.0 — Fix _inplaceKind priority scan + orphan-fallback clear.

```
Two related log-placement bugs reported in a single screenshot
during a Vox channel sync. Same class of issue at root —
multi-tag inplace-kind resolution.

(A) "Downloading 100%" row stuck next to the "✓ done" line
instead of being replaced. Happens when the DLTRACK handler
falls through to the orphan marker path (`dlrow_orphan_<vid>`),
which it does when both the literal and basename path lookups
miss `_path_to_counter`. Transient os.path.isfile races, or
encoding mismatches between yt-dlp's Merger-line path and the
Destination-stored key, can cause this. the user's failing video
had a fullwidth question mark `？` (U+FF1F) in the filename —
yt-dlp sanitizes `?` to `？` because `?` is invalid on Windows.
That specific character is fine in principle; the issue is the
fall-through path had no cleanup. Orphan-marked done line
emitted fresh at the log bottom while the progress row with
dlrow_<N> stayed stuck in the DOM at its original position.

Fix: when falling through to orphan marker, the DLTRACK handler
now also emits a clear_line control for
dlrow_<current _DLROW_COUNTER>. That removes the stuck progress
row from the DOM. The counter is also added to _closed_dlrows
so any late progress ticks get dropped.

(B) "Transcription queued…" placeholder not replaced by the
"✓ Transcription (took Xsec)" done line — both visible in the
log. _inplaceKind in web/logs.js iterated segment-by-segment,
and within each tag returned on the FIRST prefix match found.
For a transcribe-done segment tagged
`["dim", "whisper_job_N", "tx_done_<vid>"]`, iteration went:
tag "dim" (no prefix match), tag "whisper_job_N" (matches
`whisper_job_`) → return `"whisper_job_N"`. Never looked at
`"tx_done_<vid>"`. Placeholder resolved to `tx_done_<vid>`,
done resolved to `whisper_job_N` → different kinds, no inplace
match, done appended fresh below the placeholder.

Fix: rewrote _inplaceKind to flatten ALL tags across ALL
segments into one list first, then scan in priority order
(tx_done_ first, then whisper_job_, sync_row_, dlrow_, then
active markers, then prefix families). Also reordered backend
transcribe.py tag lists to put _tx_tag first as belt-and-
suspenders for any client still running the old _inplaceKind.
```

---
#### v52.9 — Most Viewed sort: title-match fallback for NULL video_id rows. (Also rolls up v52.8's frontend view_count preserve fix.)

```
Reported: the "Most Viewed" dropdown in Browse rendered the exact
same order as "Newest" — looked like the sort did nothing.

Two nested bugs, fixed across two builds.
```

#### v52.8 fix (frontend): app.js's row-mapping in loadVideosFor()

```
had `view_count: 0` hardcoded in the shape. Any value the backend
returned was discarded. sortCurrentVideos's comparator (b.view_count
- a.view_count) always returned 0 → stable sort → no reorder.
Preserved r.view_count in the mapping.
```

#### v52.9 fix (backend, the real root cause): list_videos_for_channel

```
in backend/index.py enriches each row's view_count via a
_fetch_meta() lookup keyed on video_id. But the transcription_index
DB's `video_id` column is NULL for videos indexed via folder-scan
imports — common for older channels added before the video_id
column was consistently populated. Verified: 0 out of 398 videos
in one test channel had a video_id. Every enrichment lookup
missed → view_count=0 across the board → Most Viewed sort was a
no-op regardless of the frontend fix.

_fetch_meta now builds TWO indices from the aggregated
Metadata.jsonl file: the existing video_id→entry AND a new
title→entry (loose normalization: lowercase + whitespace
collapse). The enrichment loop first tries video_id; on miss,
falls back to title match. Videos with NULL video_id in the DB
still get their view_count populated.

Also in v52.8: `like_count` preserved in the frontend mapping
alongside view_count / views, for future display.
```

#### v52.7 — Row-ID-aware [Dwnld] persist + [ReDwnl] history entry on finish.

```
Two related issues, both about activity-log persistence.

(1) Duplicate [Dwnld] rows in the activity log. the user screenshotted
a single download that produced two rows:
 [Dwnld] 10:21pm, Apr 21 — Seth Meyers — 1 downloaded 0 transcribed ✓ metadata 0 errors took 17s
 [Dwnld] 10:21pm, Apr 21 — Seth Meyers — 1 downloaded ✓ transcribed ✓ metadata 0 errors took 58s
These should be ONE row transitioning from "0 transcribed" to
"✓ transcribed" as Whisper finishes.

Root cause: `emit_consolidated_auto_row` is called twice for the
same download — once when sync_channel ends (transcribed=0,
because Whisper is still running), and again when the transcribe
worker's `_flush_batch_stats` pops the pending registry entry and
retroactively re-emits with the final transcribed count.

The LIVE DOM update via `data-row-id` worked correctly — logs.js
queries `[data-row-id="<id>"]` and swaps the element in place.
But the `append_history_entry(line)` call inside the same function
was completely oblivious to row_id. It wrote a fresh config line
each time. On next `renderActivityLog` (app restart, tab swap
that triggers a rerender, etc.), both persisted lines were
rendered as two separate rows with no dedup.

Fixed by introducing `_HIST_INDEX_BY_ROW_ID: Dict[row_id, int]` +
a new `_persist_row_history(row_id, line)` helper in sync.py.
First emit for a given row_id APPENDS to `autorun_history` and
records the list index. Retroactive emit OVERWRITES that same
slot. Trim-cap handling shifts tracked indices so they stay
valid after config-list trimming.

(2) Cancelling a redownload left no activity-log entry. Classic
YTArchiver's `_record_redownload_finish` (YTArchiver.py:22678)
persisted a [ReDwnl] row on BOTH full completion and cancel,
telling the user how many videos got replaced before the cancel
hit. Overhaul never ported this.

Fixed by adding a `time.time()` start tracker at the top of
`redownload_channel` and emitting a `[ReDwnl]` history line
after the `=== Redownload complete ===` footer. Fires on both
paths — full completion AND cancel. Format matches classic's
output exactly:
 [ReDwnl] 11:26pm, Apr 21 — Channel — 23 replaced · 0 skipped · 0 errors · took 45s

When `n_done > 0` the row tag is `hist_redwnl` (chartreuse) per
existing `_hist_tag_for_kind` logic.
```

#### v52.6 — Fix four log placement bugs.

```
All four observed in a single Simple-mode screenshot during a
multi-channel sync pass. Different root causes, grouped into one
release.

(A) "Downloading 100%" row stuck next to the ✓ done line instead
of being replaced. Only happened on video titles containing
internal quote marks — e.g. `Trump Says He Will Reach Iran Peace
Deal "the Nice Way or the Hard Way"`. yt-dlp's merger line
formats as `[Merger] Merging formats into "PATH"` with PATH
wrapped in double-quotes. The old non-greedy regex `"(.+?)"`
stopped at the first internal quote inside the title, truncating
the captured path. `_path_to_counter.get(truncated_path)` then
missed, basename fallback missed too (different basenames after
truncation), and the DLTRACK handler fell through to the unique
`dlrow_orphan_<vid>` marker. Orphan = fresh line at log bottom,
leaving the 100% progress row stuck above.

Fixed by switching to greedy `"(.+)"\s*$` anchored to end-of-
line. Captures the full quoted path even with internal quotes.

(B) "✓ Transcription (took Xsec)" landing under the WRONG
channel. Sync.py reserves a `tx_done_<vid>` placeholder at
DLTRACK time so the async transcribe-done line lands in the
right spot regardless of where the sync pass is when Whisper
finishes. But the placeholder emit had BOTH segments tagged
`dim` — primary tag `dim` is in VERBOSE_ONLY_TAGS, so Simple
mode's `_line_is_verbose_only` filter returned True and dropped
the whole line before it ever hit the DOM. When the async
transcribe-done emit later fired with the same tx_done_<vid>
marker, its inplace-replace queried the DOM, found no
placeholder to replace, and appended at log bottom — under
whichever channel sync was currently processing (typically 1-3
channels later in the pass).

Fixed by changing the placeholder tags to `whisper_bracket`
(for the em-dash + hourglass glyphs) and `simpleline` (for the
text body). Neither is in VERBOSE_ONLY_TAGS, so the placeholder
survives Simple mode and the done line reliably replaces it.

(C, D) "Transcribing — Loading Whisper model (small) on GPU..."
and "✓ Whisper model loaded (small, CUDA)" landing under the
wrong channel, persisting in the log forever. Both are one-time
subprocess-spawn diagnostics that fire when Whisper's venv boots
up — which is async relative to the sync pass. Neither line had
an inplace marker, so both appended at whatever log position
the sync cursor happened to be at when Whisper spawned. Usually
that was 2-3 channels after the video that actually queued the
job, since model load takes several seconds.

Fixed by adding `transcribe_using` to VERBOSE_ONLY_TAGS (the tag
the "Loading Whisper model" line already uses), and changing the
"model loaded" emit to use `transcribe_using` as its primary tag
on both segments. In Simple mode both lines are silently
filtered. In Verbose mode they still show for diagnostics (slow
Whisper startup, CUDA fallback detection).
```

#### v52.5 — Audit-driven bug sweep (~20 fixes) + Cancel Redownload feature.

```
Ran a full parallel-agent audit of the codebase. ~90 findings
surfaced, deduped to ~30 real bugs. This release tackles the
21 that were either simple (surgical one-liners) or high-value
(data integrity). Remaining items are tracked locally.

NEW FEATURE: Cancel Redownload button

Red ✖ Cancel button appears next to ↻ Continue Redownload in the
edit channel panel whenever a channel has a pending redownload.
Confirms via dialog, then:
 1. Fires _sync_cancel if the channel's redownload is currently
 running (pipeline exits at next chunk boundary).
 2. Drops any queued entries for this channel from the internal
 _redwnl_pending list.
 3. Removes from queues.sync so the Sync Tasks popover row
 disappears.
 4. Deletes _redownload_progress.json so the Subs chartreuse
 dot + right-click "Continue" option both clear.
 5. Invalidates archive-scan cache + refreshes Subs table.
Returns {was_running, was_queued, progress_removed} so the toast
can report exactly what happened.

Data integrity

 - Redownload extension-swap uses two-phase commit: rename
 original aside → os.replace → drop aside on success, roll
 back on failure. Prevents file loss when yt-dlp's output
 container differs from the original (rare but destructive).

 - Late-progress-clobbering-done fix: yt-dlp occasionally
 dribbles a final "[download] 100% of X in Y" stdout line
 AFTER the `after_video:DLTRACK:::` print has fired. The late
 tick was re-emitting the progress line with the same dlrow_N
 marker, inplace-replacing the ✓ done line we had just
 emitted. Users saw a stuck "Downloading Title 100%" with no
 checkmark on the first of two videos in a channel. Added a
 _closed_dlrows set populated by the DLTRACK handler; progress
 branch drops any tick targeting a closed dlrow.

 - sync_all now skips kind=redownload tasks. They're drained by
 a separate Api._redwnl_worker. Previously both workers popped
 the same queue entries and sync_all mis-processed them as
 regular downloads (user saw "[1/N] ChannelName — no new
 videos" while the real redownload was running in the
 popover).

 - Redownload tasks excluded from the sync_all starting-total +
 dynamic-total calculations so "Sync pass starting (N
 channels)" stays accurate when redownloads are queued
 alongside.

Log UX

 - Active status lines re-emit after every completed/skip entry
 within the same iteration so the "active is always last" rule
 holds. Pre-fix, the active line from _emit_active() sat above
 later-emitted completed entries. Redownload was the most
 visible — metadata/compress/reorg follow the same pattern
 (preemptively audited; only redownload needed fixing because
 the others already re-emit at top of next iteration fast
 enough that the window is invisible).

 - Bracket-coloring split across all four "active" module logs
 (redownload, metadata, compress, reorg): only [/] + the label
 word render in the module's signature color; numbers + channel
 names stay white. Matches the visible per-item log pattern
 the user already had elsewhere.

 - "Metadata downloaded" line: em-dash + checkmark + "Metadata"
 in pink; "downloaded" in white. Color discipline is "subject
 colored, verb white".

 - Download progress percentage surfaces in Simple mode via the
 "— Downloading <title> NN%" row, in-place replaced every tick,
 replaced cleanly by the final ✓ done line. Pre-fix the dim /
 dlprogress_pct tags got filtered in Simple mode and the
 Downloading line sat unchanged until done.

 - Stale _DLROW_COUNTER guards: progress emits drop when no
 path-to-counter entry matches; DLTRACK done emits fall back
 to literal-lookup → basename-lookup → unique orphan marker
 (never to the current _DLROW_COUNTER which may point at the
 NEXT video if yt-dlp started extracting ahead of the previous
 video's merge).

 - Cookie sign-out detection in sync.py's yt-dlp stdout loop:
 matches "sign in to confirm", "cookies are missing|invalid",
 "failed to extract any player response" + "sign in", and the
 classic "error: cookie extract|sign in" pattern. Emits a red
 █ banner ONCE per sync pass explaining what happened. Classic
 had this; overhaul was silent, so Firefox cookies could stay
 stale for weeks with no visible feedback.

 - Pause UX cleanup: single "Sync paused at H:MMpm — click
 Resume." line instead of the previous three. The /streams
 pass no longer launches when the main pass was just paused.

 - Log trim warning stops chattering (keep=5000 provides 3000
 lines of headroom between trims).

 - Mini-log clones strip data-inplace so inplace-replace emits
 don't cross-match between main and mini log containers.

 - Caption-fetch and thumbnail-download failures emit dim
 diagnostic lines instead of silently swallowing. Cookie /
 auth errors during caption fetch are now visible ("Caption
 fetch blocked: <error>").

 - Disk-error watchdog "Permission denied" regex requires a
 filesystem-context keyword (writ/output/file/disk/save) —
 no more false positives pausing all workers on age-gate /
 member-only / auth rejection.

State / persistence

 - Window geometry sanitized on save + load. Coords outside
 +/- 10000 reset to None (pywebview centers). Sizes < 400x300
 or > 20000x20000 reset to defaults (1100x780). Reported:
 window had ended up at Windows' minimized-parking coordinate
 (-32000, -32000) while reporting normal state; next launch
 faithfully restored off every display.

 - Recent tab rows sorted by download_ts descending BEFORE the
 200-row slice. Fresh entries can no longer be hidden by 200
 older entries from an out-of-order insertion history.

 - Subs sparse-payload update now mirrors folder→name when only
 folder was edited, matching the existing name→folder logic.
 Pre-fix, renaming a channel's folder left the display name
 stale in Subs / Browse / tray tooltip.

 - Subs edit dirty-check includes folder_override in the `cur`
 object so it mirrors the `snap` shape. Update button is no
 longer dirty on panel open.

 - Subs undo-remove is a stack (max 50) instead of a single
 slot. Consecutive removes can all be undone in LIFO order.
 Failed restores re-push onto the stack for retry.

 - _migrate_pending_tx_ids gated behind
 _migration_v2_pending_tx_ids flag — runs once per config,
 skipped on subsequent load_config calls.

 - download_ts falls back to None instead of 0, and
 _fmt_time_ago early-returns on falsy input. Eliminates
 "54 years ago" display for rows with missing timestamps.

 - archive_single_video per-URL in-flight set prevents duplicate
 yt-dlp processes when the user mashes the button on the
 same URL. Different URLs still parallel.

Redownload chain (also part of this pass)

 - Multiple Continue Redownload clicks queue on an internal
 _redwnl_pending list and drain sequentially through a
 single worker. Previously the 2nd+ clicks silently errored
 with "Sync pipeline already running".

 - chan_redownload now clears QueueState.sync_paused +
 gpu_paused on worker start so the global Pause/Resume button
 doesn't stay stuck on "Resume" when a new redownload fires.

UI polish

 - Reorg KeyError on month outside 1-12 (corrupted upload_date)
 degrades to "00 Unknown" folder instead of crashing the
 whole pass mid-move.

 - Reorg progress emits every 10 files (was 25) so channels
 with <25 videos actually show midway progress.

 - Toast auto-dismiss timers pause on document.visibilityState
 === "hidden" and resume when visible. Tabbing away and back
 no longer flashes a burst of dismissals.

 - Welcome modal Browse button disables while pick_folder is in
 flight; picker errors surface via toast instead of a silent
 catch.

 - Watch video pause on tab switch flushes audio buffer via
 currentTime = currentTime. Eliminates the 1-3 second audio
 lag after switching tabs while a video was playing.

 - set_log_mode persists to disk BEFORE mutating self._config.
 Save-failure (e.g. permission denied) no longer leaves
 in-memory config and disk diverged.

 - TranscribeManager.current_model() public accessor replaces
 main.py's private-attr reach into _transcribe._model.

 - bookmark_list() returns {ok: True, rows: [...]} to match
 bookmark_add/remove/update_note shape. JS caller back-compat
 handles both shapes (old list + new wrapper).

Build

 - YTArchiver.spec's datas list now auto-bundles ffprobe.exe
 when present in the project root (same treatment as
 yt-dlp.exe and ffmpeg.exe). Safety net for future clean-
 machine deployments where ffprobe isn't on PATH.
```

#### v51.8 — Chain multiple Continue Redownload requests sequentially.

```
Reported: right-clicking "Continue Redownload at X" on a channel
with the chartreuse pending-redownload dot worked the FIRST time
(task appeared in the Sync Tasks popover and ran), but every
subsequent right-click did nothing. Channels 2..N got silently
dropped.

Root cause: chan_redownload in main.py was doing
 if self.sync_is_running():
 return {"ok": False, "error": "Sync pipeline already running"}
Redownload reuses self._sync_thread, so once the first redownload
was active, sync_is_running() returned True and any new redownload
request got rejected. The JS call site didn't await / didn't show
a toast, so from the UI side it looked like nothing happened.

Refactor: introduced self._redwnl_pending (list) + a lock on the
Api instance. chan_redownload now:
 - If a regular (non-redownload) sync is running, still refuse
 (existing guard, correct behavior).
 - If a redownload is running, APPEND to _redwnl_pending AND
 mirror into queues.sync_enqueue so the queued redownloads
 show in the Sync Tasks popover alongside the running one.
 Returns {ok: true, queued: true}.
 - If nothing is running, spawns a single worker thread that
 drains _redwnl_pending sequentially, calling the newly-
 extracted _run_redownload_one(ch, folder, new_res,
 scope_label) helper per item. Previously the per-item
 logic lived as a closure `_run` inside chan_redownload;
 lifting it out lets the worker loop over multiple items
 without re-spawning threads.

JS Continue Redownload action is now async and awaits the
response to show a toast: "Redownload started: <channel>" on
direct start, "Queued redownload of <channel>" when chained.
app.js cache-buster bumped to v=59.
```

#### v51.7 — Pause log cleanup + orphan-safe DLTRACK done emit.

```
Two bugs reported in quick succession.

(1) Pause emitted three log lines for one click:
 ⏸ Paused — stopping current download.
 ⏸ Paused — stopping current download.
 ⏸ Sync paused at 5:38pm — click Resume.

sync_channel runs TWO yt-dlp subprocesses per channel — one for
/videos (main pass) and one for /streams (past livestreams). When
pause fires mid-main-pass, the main yt-dlp's stdout loop emitted
the "stopping current download" line, terminated the subprocess,
and broke. But the outer for-_pass_idx loop in sync_channel only
checked cancel_event, not pause_event — so it iterated to the
/streams pass, launched a fresh yt-dlp, which hit the pause
check on its very first stdout iteration and emitted a SECOND
"stopping current download" line. Then sync_all's outer loop
popped the next channel and _wait_if_paused emitted the "Sync
paused at H:MMpm" line. Three lines from one click.

User report: "just the bottom one is fine." Removed the "stopping
current download" emit entirely — pause is now surfaced only
by _wait_if_paused's single "Sync paused at H:MMpm" line. Also
added a pause_event check to the for-_pass_idx loop so /streams
doesn't launch when the main pass was just paused.

(2) Stuck "Downloading 100%" line for one of two videos in the
same channel — the done ✓ line didn't replace the progress row.
Root cause: yt-dlp started extracting video 2 while video 1's
merge was still finalizing, bumping _DLROW_COUNTER to N2. When
video 1's DLTRACK fired, the done emit's key lookup on
_path_to_counter sometimes missed (path formatting quirks:
slashes, casing, abs-vs-rel) and fell back to the current
_DLROW_COUNTER which was N2 — routing video 1's done line onto
video 2's progress row, orphaning video 1's progress as a stuck
"Downloading 100%".

Fix: match order is now literal -> case-insensitive basename
match across all announced paths -> unique orphan marker
(dlrow_orphan_<videoID>). Never fall back to the current
_DLROW_COUNTER, which can point at the WRONG video at this
point in the stream. Worst case the done line appears fresh
at the log bottom, tolerable and much safer than cross-wiring
rows.
```

#### v51.5 — Sanitize saved window geometry (Windows -32000 parking trap).

```
Reported today: YTArchiver was open and showed up in Alt-Tab, but
clicking it / hovering its taskbar thumbnail did nothing. The window
was painted off every display. Probed via user32 EnumWindows +
GetWindowRect — the window was at (-32000, -32000) with size
640x480. That's the coordinate Windows parks minimized windows at,
but IsIconic was returning False, so the overhaul's window-state
saver captured (-32000, -32000) as "current position" and wrote it
to ytarchiver_config.json under window_state.{x,y}. Launch restored
the window to that position faithfully → invisible on every monitor
but listed as "visible" by Windows.

Fix: _sanitize_geometry in backend/window_state.py runs on both
save and load. Coordinates outside +/- 10000 get reset to None
(pywebview centers on primary display). Sizes smaller than 400x300
or larger than 20000x20000 get reset to DEFAULT_STATE. Applied on
SAVE so bad values can't land in config; applied on LOAD so any
already-stored bad values get scrubbed the first time a pre-v51.5
config is read by a v51.5+ build.

Immediate recovery on the user's machine was done via a PowerShell
MoveWindow call to relocate the window to (0, 0) with a 1600x1000
size; that position auto-saved to config so the next launch is
already sane. v51.5 prevents the trap from re-occurring.
```

#### v51.4 — Download progress pct + Metadata split color + cookie banner.

```
Three fixes rolled into one build.

(1) Download progress bar in Simple mode. The "— Downloading <title>"
line now updates in place with a green percentage suffix on every
yt-dlp tick ("— Downloading Title 45.2%"), replaced cleanly by the
final ✓ done line. Pre-v51.2 the progress ticks were tagged
dim/dlprogress_pct (both in VERBOSE_ONLY_TAGS), so Simple mode
filtered them out entirely and the Downloading line sat unchanged
until the final done line arrived. Now the tick re-emits the same
3-segment Downloading shape with a green pct appended, tagged with
non-verbose tags so Simple mode shows it.

First attempt at this (v51.2) had a bug where some progress ticks
fired with a stale _DLROW_COUNTER value from a prior video/channel,
producing ghost "Downloading 100%" lines with no title that
in-place replaced a PREVIOUS channel's done line way up in the
log. Fixed in v51.4: progress emits are dropped entirely when the
counter can't be resolved to a title via the per-channel
path→counter map. Better to skip a tick than corrupt the log.

(2) Metadata done line color. Was white in original, then
over-corrected to all pink in v51.2. Final split: em-dash +
checkmark + "Metadata" pink (subject), "downloaded" white (verb).

(3) Firefox / browser cookie sign-out detection. Classic YTArchiver
emitted a red block telling the user to sign back in to YouTube
when yt-dlp surfaced a cookie-extract / sign-in error; the overhaul
was silent. Added detection in sync.py's yt-dlp stdout loop that
matches any of: "sign in to confirm", "cookies are missing",
"cookies are invalid", "failed to extract any player response"
with "sign in", or the classic "error: cookie extract/sign in"
pattern. Fires once per sync pass. Reported: Firefox had been
signed out of YouTube for weeks with no log feedback.
```

#### v51.1 — Auto-off sync/gpu: enqueue without firing + blink gate.

```
Two related bugs with the Auto checkboxes on Sync Tasks and GPU
Tasks. Reported: clicking "Sync Subbed" with Sync Auto unchecked
still fired the worker immediately instead of just queueing; and
when a download kicked a transcribe job into the GPU queue while
GPU Auto was off, the GPU Tasks button started blinking even
though nothing was actually processing — it just had an item
parked in the queue.

sync_start_all now checks autorun_sync at the top. If Auto is off
AND this is a fresh "Sync Subbed" click (add_downloads_from_config
=True), it enqueues every subbed channel but returns without
spawning the worker thread. Response payload gets a new
started=False + queued=N shape so the UI can differentiate.

_on_queue_changed now gates the blink state on the autorun flags.
"Working" (head-of-queue status=="running") always blinks — if a
job IS running, the button must show it regardless of auto state
(e.g., user clicked Start manually). But "alive" (worker thread
running / jobs queued) only counts when Auto is ON. So a parked
queue with Auto off shows items in the popover + count badge but
leaves the button idle-grey.

queue_auto_set("sync", True) now spins up the sync worker when
the queue has pending items — symmetric with existing GPU behavior.
So the flow "queue everything with Auto off → change mind → flip
Auto on" starts draining automatically without a second click.

JS toast on the Sync Subbed button now branches on response.started:
"Queued N channels. Start manually or enable Auto." (warn) vs the
existing "Sync started." (ok).

(claude randomly stopped adding to this file for a few days. idk why)
```

#### v47.1 — Strip PII from all tracked source.

```
Inline code comments across main.py, backend/, and web/ carried
developer-identifying information: a real name used as "User report: " /
"Rule: " attribution, a full "C:/Users/<name>/..." path in
the YTArchiver.spec build-command comment, and a handful of
personally-archived channel names in case-study comments.

All of it was rewritten into generic language (reported / noted /
users / a user's channel, etc.). The spec file's Python path is
now `py -3.13`. No runtime-behavior changes — comments only.

GitHub release v47.0 had the PII; it was deleted and replaced with
this clean v47.1 release. Historical commits in the GitHub repo
still carry those strings though, so a git history rewrite +
force-push is the only way to fully erase them from the public
record.
```

#### v47.0 — Watch-view state survives tab-swap + retroactive whisper model.

```
Pausing a video, switching to another top-level tab, then coming
back to Browse no longer shows an empty Watch placeholder. The
video element's src + playhead are preserved across the tab swap;
only playback is paused. Full unload (removeAttribute("src") +
load()) still runs on intentional "done watching" exits — the
Back button, Library sidebar click, or any navigate-via-showView
transition — since those mean the user is done with the video.

Retroactive whisper model fill in the Watch-view source banner.
```

#### v46.0 through v46.5 had a bug that wrote "(WHISPER)" to the

```
Transcript.txt header instead of "(WHISPER SMALL)" or
"(WHISPER:large-v3)", so the model name was literally not in the
file. Classic YTArchiver always wrote the model; v46.6+ does too.
For entries missing the model info, _classify_transcript_source
now substitutes the user's current default whisper model from
config so the banner reads e.g. "Whisper transcription - small
model" instead of just "Whisper transcription". Best-effort guess
since the real model used at the time wasn't recorded;
re-transcribe on v46.6+ to write the accurate tag into the file.

Diagnostic log: transcribe.py now prints the source_tag it writes
to the main log (Verbose mode). Lets you verify new re-transcripts
carry the model name in their header without opening the .txt
file manually.
```

#### v46.7 — Watch-view polish pass (rolls up v46.4 \u2192 v46.7 local builds).

```
Karaoke auto-follow no longer scrolls the outer .browse-view out
from under the video. The "scroll to active word" helper now sets
container.scrollTop directly via getBoundingClientRect deltas
instead of calling scrollIntoView (which walks every overflow:auto
ancestor). Plus the Watch layout is capped at viewport height so
the transcript pane is the only scrollable child and the video +
controls stay pinned at the top.

Description / views / likes / comments visible again in the Watch
drawer. Regression from the layout cap: the drawer was a third
grid child that got implicit-row-clipped by the new overflow:hidden.
Moved it inside the video column so it stacks below the playback
controls; .watch-video-wrap scrolls internally for tall descriptions
+ 50+ comments so the column's content stays accessible without
breaking the outer cap.

Re-transcribe from Watch view no longer restarts playback. The
refresh path used to call renderWatchView which re-sourced the
<video> element via _loadVideoSource, kicking the playhead back
to 0. Added skipVideoReload flag so _onRetranscribeComplete only
touches the transcript DOM + source banner.

Manual whisper model picker (one-off re-transcribe modal) no
longer mutates the Settings default. transcribe_swap_model now
takes persist=True/False; the askWhisperModel dialog passes False
so a one-off pick doesn't change what auto-transcribes use.

Whisper banner shows the model name again ("Whisper transcription
large-v3 model"). whisper_worker's ok response doesn't carry a
"model" field, so _write_outputs now falls back to self._model
when building the (WHISPER:xyz) tag that ends up in
Transcript.txt's header. New re-transcripts will show the model;
older entries tagged plain (WHISPER) still render as "Whisper
transcription" with no model qualifier.

Clickable timestamps in descriptions + comments. New
_renderDescriptionWithTimestamps helper scans for M:SS / MM:SS /
H:MM:SS / HH:MM:SS patterns and wraps each in a .desc-ts span with
a click handler that seeks the <video>. Negative lookbehind
prevents false matches on datetimes like "3:14:15 PM" or ratios
"2:30" when adjacent to other digits.

[Trnscr] activity row from a Watch-view retranscribe now carries
the channel name. main.py looks up the channel from the index DB
by filepath at enqueue time and passes it through to the
transcribe worker.

Stuck 99% "Transcribing" progress line no longer persists after
the done line lands. _inplaceKind (the tag classifier) was doing
a single-pass scan that matched the generic "whisper_" prefix
before the per-job "whisper_job_N" tag, so progress and done
lines classified as different kinds and the done line couldn't
replace the progress line in-place. Two-pass scan now: per-job
/ per-row kinds first, prefix-family fallback second.
```

#### v46.3 — CRITICAL: fix video seeking (HTTP Range support).

```
The local file server that the Watch-view <video> tag streams from
was ignoring the Range header browsers send when you click the seek
bar or drag the playhead. Without 206 Partial Content responses and
an `Accept-Ranges: bytes` hint, WebView2 couldn't jump mid-stream
and the playhead snapped back to its current position on every seek
attempt. Any build from v46.0 through v46.2 had this bug.

Fix in backend/local_fileserver.py:
 - protocol_version = "HTTP/1.1" (required for 206)
 - Accept-Ranges: bytes on every successful response
 - _parse_range() handles bytes=X-Y, bytes=X-, bytes=-N
 - do_GET serves 206 + Content-Range on Range requests
 - do_HEAD added for browsers that probe before seeking
 - 416 with Content-Range: bytes */SIZE on malformed ranges
 - Silent ConnectionError handling for mid-scrub client aborts
```

#### v46.2 — Watch-view retranscribe polish + stuck progress line fix.

```
Stuck 99% progress line in the mini log after a transcription
completes is fixed. Root cause was in the frontend _inplaceKind
classifier: segments tagged ["whisper_bracket", "whisper_job_N"]
matched the generic "whisper_" prefix first, so progress + done
lines ended up with different data-inplace kinds and the done line
couldn't replace the progress line. Reworked to a two-pass scan:
per-job / per-row unique kinds (whisper_job_N, sync_row_N, dlrow_N)
win over the family-prefix fallback.

Watch view now refreshes its transcript + source banner on its own
when a retranscribe completes for the currently-viewed video. Slash
normalization + case-insensitive compare on both filepaths so a
backslash-vs-forward-slash mismatch doesn't drop the refresh.
Playhead position + play/pause state are preserved across the
re-render — the banner flips from "YT auto-captions (approximate)"
to "Whisper transcription" without restarting playback.

[Trnscr] activity-log row from a Watch-view retranscribe now shows
the channel name. Was rendering as em-dash because the Api call
didn't pass `channel=` to the transcribe queue. main.py now looks
up the channel from the FTS index DB by filepath at enqueue time.
```

#### v46.0 — pywebview overhaul graduates. UI rebuilt on an HTML/CSS/JS

```
frontend served by pywebview, same yt-dlp / Whisper / ffmpeg / FTS5
backend as before. The Tkinter build is archived as the "Tkinter
legacy ver" release on GitHub for anyone who prefers the original UI.

Major UI shifts from classic:
 - Browse tab consolidates Channels / Search / Graph / Bookmarks /
 Recent behind one sidebar. Recent has a thumbnail-grid view as
 well as the legacy list.
 - Graph tab supports Year / Month / Week buckets (Week uses the
 new videos.upload_ts column derived from file mtimes).
 - Settings split into General / Performance / Appearance / Tools /
 Index sub-tabs.

Activity log consolidation:
 - Download + transcribe + metadata for each channel collapse into
 a single [Dwnld] row per sync pass:
 [Dwnld] 12:44pm, Apr 20 — Channel — N downloaded · N transcribed
 · N metadata · 0 errors · took 13s
 - When transcribed or metadata is exactly 1, the count renders as a
 ✓ instead of a digit (the user's single-video polish). `downloaded`
 stays numeric. New grid cell for metadata; row max width bumped
 to 1800px so wide monitors don't truncate.
 - Race fix: sync_channel reads the transcribe manager's per-channel
 batch stats synchronously at its end and marks "sync active" so a
 fast auto-captions flush can't fire a stray [Trnscr] row first.

Recent-tab live refresh: new downloads + thumbnails now re-push the
grid/list without needing an app restart.

UTF-8 titles: curly apostrophes / em-dashes survive the yt-dlp
subprocess now. Sets PYTHONIOENCODING / PYTHONUTF8 / LC_ALL / LANG
in the subprocess env and reads stdout in bytes mode with a
UTF-8-first / cp1252-fallback decoder — yt-dlp.exe's frozen Python
bootstrap wasn't always respecting the env vars alone.

Dark tray context menu: uxtheme.dll ordinal 135/136 ForceDark applied
before pystray spawns, so the right-click menu follows the app theme
instead of Windows's light default.

Graph stability: min-height floor on the canvas wrap so the plot
doesn't collapse to zero height when the window is squished narrow
(and then fail to recover when expanded). Belt-and-suspenders
ResizeObserver + window resize listener calls chart.resize() as
Chart.js's internal observer occasionally misses.

Autorun countdown: waits for the current sync to finish before
restarting. "Waiting for queue..." when a manual sync is mid-flight
instead of a stale ticking countdown.

Column-width persistence: Subs tab column widths saved per user +
restored across relaunch (pywebviewready race fix).

URL sanitization: paste-a-video-URL-into-channel-field nudge panel
handoff (+ existing reverse nudge on the Download tab). Normalizes
@handle / /@handle / bare handle / channel/UC... / user/name on save.
```

---
#### v42.2 — Redownload log: .mp4 white, truncation ellipsis yellow

```
Follow-up tweak to v42.1's redownload line coloring. After living with it,
the user wanted the `.mp4` extension to render white (like the title) rather
than chartreuse, since the colored brackets and slash already carry the
task identity. If the filename was too long and got truncated to
`first 47 chars + "..."`, those three trailing dots now render in the
chartreuse redownload color so the truncation indicator stands out.

Now:
 [ / ] → chartreuse (redwnl task color)
 numbers inside brackets → white
 video title → white
 .mp4 → white (was chartreuse in v42.1)
 truncation "..." → chartreuse

Implementation: in the `_is_redwnl` branch of `_segmented_insert`, the
extension segment now inserts with `dl_white` instead of `_base_tag`.
Added a fallback regex for the truncated case (no extension match) that
splits the text on the trailing `...` and colors the ellipsis with
`_base_tag`.
```

#### v42.1 — Redownload task lines match the download/transcribe coloring style

```
The per-item lines in the redownload task (simple mode) looked like:
 [352/2850] The Verge Mobile Show 019 - October 2nd, 2012.mp4
but every character was chartreuse — numbers, title, extension all the
same color as the [ ] brackets. User report: the video title should be white;
only the brackets, slash, and file extension should be color-matched.
That matches how the download, transcribe-done, and metadata lines
already render (white title, colored decorators).

Now:
 [ / ] and .mp4 → chartreuse (redwnl task color)
 numbers inside brackets → white
 video title → white

Implementation: extended the existing `_segmented_insert` helper. It
already had a white-title branch for meta_bracket lines — added a
parallel branch for redwnl_bracket that splits the after-bracket text
on the `.ext` suffix so only the extension keeps the task color.
```

#### v42.0 — Trim restore-pause notice (too verbose)

```
v41.9's startup notice was:
 ⏸ Sync queue restored — PAUSED on launch. Click Resume to start processing.
User report: too verbose; last word should be PAUSED.
Now:
 ⏸ Sync queue restored — PAUSED

(Version rolls v41.9 → v42.0 per the single-decimal versioning rule.)
```

#### v41.9 — Pause is visible at task entry, not after preliminary work

```
Auto-pause-on-restore behavior is preserved (the user confirmed via the
prompt: "Keep current behavior — always auto-pause on any restored
items"). What changes is WHEN the pause becomes visible to the user.

PROBLEM: the user showed a screenshot where a Redownload task on The Verge
(480p) ran through "Found 6005 videos. Matched 2850 files. Checking the
first 10 at 480p..." and then immediately "⏸ Redownload paused at
6:02pm." with the bottom anim showing "[1/2850] PAUSED: The Verge".
He hadn't clicked pause. From his POV, the task scanned the catalog
for several minutes then spontaneously paused itself.

ROOT CAUSE: pause_event was auto-set at startup when the queue was
restored (the existing "must explicitly hit resume" behavior), but
the per-task pause checks lived DEEP inside the per-video loop. The
task did all its preliminary work (catalog enumeration, file scan,
title-to-ID matching) BEFORE checking — so the pause only showed up
several minutes in, looking like it came out of nowhere.

FIX (two parts):

 1. Restore-time visibility. When sync_pipeline_restored auto-sets
 pause_event at launch, also log:
 ⏸ Sync queue restored — PAUSED on launch. Click Resume to
 start processing.
 So the user knows immediately, not implicitly.

 2. Entry-time pause check on every long-running sync-pipeline
 worker. Tasks now check pause_event at the START before doing
 any visible preliminary work. Pauses for the full duration if
 paused, then runs cleanly when resumed.

 Worker entry-points that gained the check this round:
 - Sync (_sync_worker, line ~19036)
 - Reorganize (_run_reorganize_auto worker, line ~8069)
 - Transcribe (_start_transcription's _worker, line ~14394)
 - Redownload (_backlog_redownload_channel's _worker, line ~10469
 — added in earlier work today)

 Metadata (_run_metadata_download, line ~12468) already had this
 check, so no change there.

NET EFFECT: launch with restored items → clear pause notice immediately
in the log → click Resume → tasks proceed cleanly without ever showing
a surprise mid-flow pause again.
```

#### v41.8 — Anim PAUSED check picks the right pause flag per task source

```
Symptom the user showed in a screenshot: bottom-pinned anim said
"[0/0] PAUSED: Apple Explained" even though channels were happily
ripping through the GPU queue.

Root cause: _simple_anim_tick was always reading the GLOBAL pause_event
flag when deciding whether to render PAUSED. pause_event governs the
SYNC pipeline (Sync, Metadata, Reorg). GPU-worker tasks (transcribe /
encode triggered from the GPU queue) use a SEPARATE flag, _gpu_pause.
pause_event has nothing to do with them.

If pause_event happened to be left set from an earlier sync action
(e.g. user clicked Pause on the sync queue, then later cleared the
sync queue without un-pausing), every subsequent GPU-driven task
would incorrectly display [N/M] PAUSED: X in the anim even though
the worker was running normally.

FIX: when the current task's mode is "transcribe" or "compress" AND
_gpu_running is True, check _gpu_pause instead of pause_event. All
other modes (sync, metadata, reorg) keep using pause_event since
those ARE sync-pipeline tasks and the global pause is correct.
```

#### v41.7 — Whisper trumps auto-captions, no more dual transcript entries

```
Three coordinated fixes that together enforce the "one transcription
per video, Whisper beats YT" rule end-to-end.

1. BULK-QUEUE LOG SPAM
 _queue_all_transcriptions used to emit one "Added X to GPU-tasks
 queue" line per channel — 103 channels = 103 redundant log lines
 above a single summary. _add_to_gpu_queue now respects _quiet=True
 for the success log; the bulk caller passes _quiet=True. Summary
 line gains a leading em-dash so the existing simpleline_green +
 leading-em-dash painter rule paints it blue (transcription
 identity). Net result: one tidy line per bulk action, not 103.

2. AUTO-TRANSCRIBE OVERWRITING WHISPER (the real bug the user reported)
 Symptom: a video previously retranscribed with Whisper via the
 client viewer was getting auto-captioned AGAIN on subsequent sync
 runs, silently overwriting the Whisper transcript. The auto-
 transcribe-after-sync flow was treating the file as "not yet
 transcribed" and re-processing it through the YT-captions path.

 Root cause: file stems on Windows use fullwidth replacements
 (？/：/／) for forbidden characters, but transcript headers may use
 either form depending on whether the title came from YouTube's API
 (ASCII) or from the filename (fullwidth). The _start_transcription
 skip check ("is this fname already in already_done?") was a literal
 string compare — so the fullwidth ？ filename stem missed the ASCII
 ? .txt entry written by the client retranscribe. File fell into
 files_to_process → auto-captions ran → Whisper overwritten.

 Fix (two parts):
 a) NFKC-normalize both sides of the skip check (and the
 _jsonl_needed check). NFKC collapses fullwidth ？/：/／ to
 ASCII so `？` and `?` compare equal.
 b) _run_retranscribe_job now writes new entries with the
 FILESYSTEM-safe filename stem as the title (was: DB title,
 which was usually the YouTube API ASCII form). v41.4's
 purge-by-video_id removes the OLD entry whatever its title
 was, so the file ends up with ONE entry whose title matches
 the on-disk filename exactly. No more dual-title entries.

3. METADATA-PREP-LINE FIGHTING (carry-over noted in changelog)
 "Fetching channel playlist..." metadata prep line had been
 inserting at tk.END, which made it RACE the bottom-pinned
 simplestatus anim ("Transcribing: X··") for the absolute-bottom
 position. Fixed in v41.6 by inserting at simplestatus[0]; pink
 em-dash added for metadata identity.

END-STATE CONTRACT for transcripts going forward:
 - One .txt entry per video
 - One .jsonl segment group per video
 - Whisper retranscribes cleanly REPLACE YT auto-captions in place
 - Files stay "transcribed"-flagged across future sync passes
 - Future re-transcribes will not create dual entries

Existing dual entries on disk (created pre-v41.4) remain cleanable
via cleanup_dup_transcripts.py. Today already cleaned: LaurieWired
CPU video, HasanAbi "huge change" (twice — second time appeared
between earlier cleanup and v41.7 ship).
```

#### v41.6 — Metadata prep line: insert above simplestatus + pink em-dash

```
Two fixes to the transient "Fetching channel playlist..." /
"Date-resolving N video(s)..." line that the metadata sweep emits
during long Pass-4 channel enumerations.

PROBLEM 1: WAS INSERTING AT tk.END
Both that line AND the bottom-pinned simplestatus anim ("Transcribing:
Saturday Night Live··" or similar) wanted to live at the very bottom
of the log. The metadata one inserted at tk.END; the simplestatus tick
every 500ms ALSO re-inserted at tk.END. Result: the two lines kept
flipping order, looking like they were "fighting" for the bottom spot.

FIX: now inserts at log_box.tag_ranges("simplestatus")[0] (i.e., just
ABOVE the anim line) so the anim stays at the absolute bottom and the
metadata prep line sits one line up. Falls back to tk.END if no
simplestatus is present (no concurrent transcribe/sync). Same
pattern _tx_scan already uses for transcription's own prep lines.

PROBLEM 2: WAS TAGGED PLAIN GREY 'scanline'
No task-identity color, so the line looked detached from the metadata
task whose state it was reporting.

FIX: 3-segment insert — 2-space indent (scanline-tagged), em-dash
with simpleline_pink overlay (gives the dash the metadata-task pink),
then the rest (scanline-tagged). All three segments still carry the
scanline tag so the next call's delete-then-reinsert finds the whole
line and replaces it in-place.

Visual end state when sync + transcribe + metadata are all running:
 \u2014 Fetching channel playlist... 12,291 titles (149s) <- pink dash
 Transcribing: Saturday Night Live\u00b7\u00b7 <- bottom anim
```

#### v41.5 — Auto-transcribe-after-sync skips full channel scan

```
the user noticed that on large channels (Internet Historian, ~1300 pages
of catalog) the auto-transcribe-after-sync flow was kicking off a
full yt-dlp --flat-playlist enumeration ("Scanning YouTube catalog
page 220...") just to build its title->ID map — 5-10 minutes of
scanning that's totally unnecessary when sync ALREADY has the
freshly-downloaded video_ids sitting in _last_run_counts.

The auto-metadata-after-sync path was already doing the right thing
here (it threads video_ids into the queue item and the metadata task
has a fast path for "I already know which IDs to fetch"). The
transcribe path was missing the same plumbing.

FIX:
 1. Sync's auto-transcribe block (line ~6848) now reads the list of
 freshly-downloaded video_ids from _last_run_counts and includes
 it as `video_ids` in the GPU queue item.
 2. GPU worker (line ~21745) forwards `item.get("video_ids")` to
 _start_transcription.
 3. _start_transcription (line ~14315 + 14555) gained a new optional
 video_ids parameter. When set:
 - Looks up (title, filepath) for each video_id in YTArchiver's
 own DB and pre-seeds yt_title_to_id[file_stem] = vid_id.
 - New `_all_files_seeded` flag — true when every file in the
 batch has a seeded ID. When true, skips BOTH the per-file
 YT search (small batches) AND the channel-wide playlist
 fetch (large batches) entirely. Goes straight to caption-
 fetching for files that have IDs.
 4. Falls through to existing search/fetch logic for any unseeded
 files (e.g. retranscribe flows that don't carry sync's IDs).

Net effect for the sync->transcribe pipeline: zero channel-wide
enumeration when sync just downloaded a bounded number of new videos.
The 5-10 minute Scanning YouTube catalog phase is gone for
auto-transcribe-after-sync runs.
```

#### v41.4 — Retranscribe replace: purge by video_id, not just title

```
Bug discovered while testing the ArchivePlayer (beta) viewer's
re-transcribe-with-Whisper flow. After the GPU finished re-transcribing
HasanAbi "This is huge change..." (3 dots — the current filename),
the viewer kept showing the OLD YT-captions transcript. Reason: the
jsonl + txt files now contained BOTH the old and new entries side by
side, and the loader picked the first one it found.

Root cause: _replace_jsonl_entry and _replace_txt_entry purged stale
entries by EXACT TITLE MATCH ONLY. The OLD YT-captions entry was
titled "This is huge change.." (TWO dots — written when the file was
named differently) and the new Whisper entry came in with title
"This is huge change..." (THREE dots — current filename). Match
failed, old entry survived, new entry got APPENDED. Same video_id
on both, two distinct titles, both in the file.

Likely affects more than just this one HasanAbi video — anywhere a
channel's filename has drifted at any point (trailing dots, fullwidth
vs ASCII chars, length truncation, manual renames), the same bug would
have left stale duplicates whenever a re-transcribe ran.

FIX:
 _replace_jsonl_entry now purges entries matching (title OR video_id),
 returning the SET of distinct titles it found for this video so
 callers know what extra cleanup the txt needs.
 _replace_txt_entry accepts an `extra_titles_to_remove` set and
 purges all matching headers — not just the canonical one. Date /
 duration provenance is captured from the first removed header so the
 new entry inherits the original recording context.
 Both call sites (_run_retranscribe_job worker AND the right-click
 Browse-tab _on_retranscribe worker) flipped to call jsonl FIRST,
 then feed the discovered titles into the txt replacer.

Future re-transcribes will properly wipe-and-replace. Existing
duplicates already on disk get cleaned up either by re-transcribing
again, or via the one-time scan/clean script
(cleanup_dup_transcripts.py) added alongside this release.

Bonus viewer-side band-aid (in ArchivePlayer beta, separate codebase):
the transcript loader now prefers Whisper-style segment groups (those
with word-level `words` arrays) when multiple groups share a single
video_id. Lets the viewer show the correct transcript for already-
duplicated entries without waiting for a re-transcribe to clean up.
```

#### v41.3 — Cmd receiver binds to 0.0.0.0 (LAN-accessible)

```
Foundational change for cross-machine ArchiveBrowser usage. The HTTP
command receiver at port 9855 now binds to 0.0.0.0:9855 (all interfaces)
instead of 127.0.0.1:9855 (localhost only). Effect: a client viewer
running on a different LAN machine — e.g. a laptop in another room
pointed at the family archive — can now hit the host's YTArchiver to
trigger re-transcribe, repair, and status commands.

Trust model: local network only, no auth token. Anyone who can reach
port 9855 on this machine can issue commands. Acceptable for a home
LAN; if you ever need to lock back down (multi-tenant box, exposed
network), set env var YTARCHIVER_CMD_BIND=127.0.0.1 to restore the
previous localhost-only behavior.

Adds a startup log line surfacing the bind so there's no mystery
about which mode the receiver is in:
 [cmd] Receiver listening on 0.0.0.0:9855 (LAN-accessible)
or
 [cmd] Receiver listening on 127.0.0.1:9855 (localhost only)

PAIRED VIEWER CHANGES (in ArchiveBrowserWithYTTest, separate codebase):
 - New YTARCHIVER_HOST env var (default 127.0.0.1). Client viewers
 on other machines should set this to the host's IP or hostname
 (e.g. "media-server" or "192.168.0.42"). The host's own viewer
 keeps the default since YTArchiver is right there on localhost.
 - All ytarchiver helpers (_ytarchiver_is_running, _ytarchiver_ping,
 _ytarchiver_url, the retranscribe forwarder, the gpu-status
 forwarder, the repair forwarder) now route to the configured
 host instead of hardcoded 127.0.0.1.
 - Status banner now REPLACES the "transcript is approximate"
 warning when a re-transcribe is queued/running for the current
 video. Shows queue position ("next up" / "N jobs ahead"), live
 whisper progress percentage, and "(GPU queue paused)" suffix
 when the host's GPU queue is paused. Auto-restores the warning
 if the job clears without completing.
```

#### v41.2 — Fix simple-mode log filter + channel art yt-dlp invocation

```
Two bugs caught right after v41.1 shipped overnight.

THE LOG FILTER BUG: A guard at line 713 in the log() function (gated
by `if _is_simple_mode:`) had an allowlist of "tags that are allowed
to render in simple mode" — anything not in the list got an early
return without writing to log_box. The list included simpleline,
simpleline_green, simpleline_blue, simpleline_pink, summary, header,
red, simpledownload, pauselog, etc. — but NOT the three task-identity
tags I introduced in v40.9 / v41.0:
 - simpleline_redwnl (redownload)
 - simpleline_compress (compress backlog + regular compress)
 - simpleline_reorg (reorganize)
So every log call using these tags was silently dropped in simple mode
the entire time those features existed. The visible symptom was the
one the user hit: a redownload backlog task on MoistCr1TiKaL advanced
its bottom-pinned [N/total] counter from 1 to 2500+ but produced
ZERO per-video log lines. The counter advances because it reads from
_simple_anim_state directly (not via the log filter), so the bottom
anim line happily updates while every per-video log call gets dropped.
Fix: added the three missing tags to the allowlist.

THE CHANNEL ART BUG: v41.1's _fetch_channel_art used the yt-dlp invocation
 yt-dlp --skip-download --no-warnings --playlist-items 0 \\
 --print "%(thumbnails)j" --cookies-from-browser firefox <url>
This produces NO OUTPUT because --print fires per item but
--playlist-items 0 means no items are enumerated. Result: the helper
saw empty stdout, returned silently, and the .ChannelArt/ folder got
created but never populated. the user noticed: ran metadata on every
channel overnight, refreshed the test viewer, no avatars anywhere.
Switched to:
 yt-dlp --skip-download --no-warnings \\
 --dump-single-json --flat-playlist --playlist-items 0 \\
 --cookies-from-browser firefox <url>
This dumps the channel's own JSON. Its `thumbnails` array contains
both the avatar (id=avatar_uncropped) and the banner
(id=banner_uncropped) which we then download as before.

Plus a one-time backfill_channel_art.py script that walks every
channel in the YTArchiver config and downloads avatar+banner using
the fixed invocation. Already run: 102 channels populated, 1 skipped
(manual sanity test from earlier), 0 failures.
```

#### v41.1 — Date-aware title-to-ID matcher + channel art grab

```
Major bug fix in the metadata sweep, plus a new feature for the viewer.

THE BUG: For channels that upload multiple videos with the same title
(Jimmy Kimmel "Guillermo at the Oscars" annual, weekly recap shows like
David Pakman's "Top Clips!", MoistCr1TiKaL's "Worst Game of the Year"),
the title-to-ID resolver was first-wins. Pass 4's _batch_map stored
norm_title -> video_id and ignored every subsequent video with the same
normalized title. So all 12 "Guillermo at the Oscars" files (2013-2026)
ended up with video_id 34yhz7v5FBM (the 2013 one), and metadata fetched
for that ID got attributed to all of them. Same upload_date, same view
count, same description, same thumbnail across all 12 files.

THE FIX: All three resolver passes now use date proximity to disambiguate.
 - Pass 4 (channel playlist match): _batch_map now stores
 norm_title -> [(vid_id, upload_date), ...] (every candidate). When
 multiple files have the same normalized title, each one matches to
 the candidate whose upload_date is closest to its file mtime. Any
 best-match with >365 days drift is rejected outright (the wrong
 video) and the file falls through to date-based matching in Pass 6.
 - Pass 7 (individual yt-dlp search): the yt-dlp `ytsearch1:` fallback
 occasionally returns a fuzzy match that shares a substring of the
 query but is an entirely different video. Now compares the search
 result's upload_date against the file mtime and rejects assignment
 if drift >365d.
 - Pass 2 (DB title shortcut): if the same title has multiple distinct
 video_ids in the DB (the same-titled-video bug already manifested),
 skips the shortcut and defers to the date-aware passes.

THE REPAIR: A standalone repair_metadata_mismatches.py script was run
against the DB to fix the existing damage from this bug. Scope:
 - 133 duplicate video_id groups across 22 channels
 - 121 repaired (winner kept by mtime closest to upload_date,
 losers cleared for re-resolution)
 - 12 clear-all (video_id wrong for ALL files in the group;
 cleared everyone, will re-resolve from scratch)
 - 194 wrong assignments cleared total
 - 276 orphan metadata entries removed across 188 jsonl files
 (entries whose video_id was no longer claimed by any file in the
 same folder, e.g. a 2026 video's metadata sitting in a 2008/.jsonl
 because the 2008 file briefly held that ID)
 - Orphaned thumbnails in .Thumbnails/ folders deleted alongside
Affected channels (top): Jimmy Kimmel (31 dup groups), David Pakman
(18), MoistCr1TiKaL (15), Bernie Sanders (14), HasanAbi (9), Two
Minute Papers (8), Stephen Colbert (8), John Michael Godier (7),
Apple Explained (6), and 13 channels with 1-4 each.

After upgrading to v41.1, the 22 affected channels need a metadata
refresh to re-resolve the cleared rows with the now-fixed matcher.
Right-click on each → "Refresh metadata".

CHANNEL ART (new feature): Every metadata sweep now also grabs the
channel's avatar + banner via yt-dlp and stores them in a hidden
<channel>/.ChannelArt/ folder (avatar.jpg + banner.jpg). Skip
re-fetch if both files exist and are <30 days old. Failures never
block the sweep (best-effort). Used by the ArchiveBrowser viewer
to render real channel avatars on the channel grid instead of
the colored letter placeholder.
```

---
#### v41.0 — Backlog log lines: task-colored em-dashes throughout

```
Follow-up to v40.9's em-dash audit. the user pointed out that during a
redownload task, MANY of the prep-flow log lines were still defaulting
to plain white "simpleline" instead of carrying the chartreuse-yellow
redownload identity color. Same problem on the compress side, just less
visible because compress jobs run alone more often.

The fix is per-task instead of per-message: every log call inside
_backlog_redownload_channel was retagged simpleline_redwnl with a leading
em-dash, every log call inside _backlog_compress_channel was retagged
simpleline_compress with a leading em-dash. The painter already knew how
to colorize the em-dash given the right base tag (lines 909-914) — the
bug was just that these calls never passed the right tag.

Lines affected:
 Redownload backlog (_backlog_redownload_channel ~line 10219+):
 - "Found N local file(s)." (10340)
 - "Redownload cancelled before YouTube fetch." (10343)
 - "Fetching YouTube video list for ID matching..." (10349)
 - "No videos returned with cookies — retrying without cookies..." (10366)
 - "Retrying YouTube video list fetch..." (10376)
 - "Redownload cancelled during YouTube fetch." (10381)
 - "Found N video(s) on YouTube." (10388)
 - "Redownload cancelled after YouTube fetch." (10391)
 - "Scanning YouTube catalog (page X)..." (10306)
 - "No video files found." (10337)
 - "No files to process after matching." (10464)
 - "Resuming — N video(s) already redownloaded, M remaining." (10474)
 - "All files already redownloaded." (10476)
 - "Matched N file(s). Starting redownload at Xp..." (10489)
 - "Matched N file(s). Resuming redownload at Xp..." (10491)
 - "Matched N file(s). Checking the first 10 at Xp..." (10493)

 Compress backlog (_backlog_compress_channel ~line 9510+):
 - "No video files found." (9549)
 - "Found N local video file(s)." (9552)
 - "Scanning YouTube catalog (page X)..." (9638)
 - "Found N video(s) on YouTube." (9656)
 - "No files to process after matching." (9722)
 - "Matched N file(s) for backlog processing." (9725)
 These four were previously simpleline_redwnl from the v40.9 session
 where I had the wrong task identified — corrected here so compress
 runs show green (simpleline_compress) end-to-end.

 Regular compress (_compress_channel ~line 9168+):
 - "No target video files found for this batch." (9200)
 - "No video files found to compress." (9214)
 Standardized to simpleline_compress with em-dash too, so a triggered
 compress job has a consistent identity from the first message.

Net effect: a running redownload looks chartreuse-yellow throughout, a
running compress looks green throughout, regardless of which sub-step
is currently logging. Easier to spot which task each line belongs to
when several are running.

Side note (not in this release, but related): the prod ArchivePlayer
exe was rebuilt + redeployed to Z:. Continue Watching dedup (one card
per show) and the new defensive PRAGMA integrity_check + auto-REINDEX
on startup were both already in source but the deployed exe was from
before those edits — explained why The Boys was still showing 3 cards.
```

#### v40.9 — Re-transcribe complete log line + simple-mode em-dash audit

```
Re-transcribe complete line was using its own bespoke format
("Re-transcription complete: <title> (Xs)") with simpleline_green
tag and a leading newline. Switched to the same painter-friendly
shape regular MT completion uses: "[1/1] <title> — done (Xs)" with
simpleline_blue tag. Painter recognizes the pattern and colors
the brackets blue (trans_bracket), title white (dl_white), and
the trailing " — done (...)" blue. No leading newline anymore so
the line sits flush under the progress line it replaced.

Simple-mode em-dash audit. A handful of log lines were missing
their task-identity em-dash color, making it hard to glance-scan
which task a line belonged to when multiple were running:
- "Fetching YouTube video list for ID matching..." at lines 9577
 and 10349 (both transcription resolver paths) now lead with
 " — " instead of " ", picking up the blue em-dash via the
 painter's simpleline + leading-em-dash rule.
- "Found N local file(s)." at line 10340 same treatment.
- "All videos already transcribed!" / "All videos already
 transcribed." (lines 14238 / 14256) now lead with " — ✓"
 so the em-dash gets blued (painter's simpleline_green +
 leading-em-dash rule) while the ✓ + text stay green.
- Redownload result lines at 10866 / 10868 ("✓ 32.6 MB →
 109.3 MB (235% larger)") moved from simpleline_green to
 simpleline_redwnl AND prefixed with an em-dash. simpleline_redwnl's
 chartreuse foreground (C_LOG_REDWNL) now applies to the whole
 line including the em-dash, matching the redownload task identity.
```

#### v40.8 — Quieter re-transcribe logging in simple mode

```
Follow-up to v40.7. The new _run_retranscribe_job worker was
printing the same verbose header that _run_manual_transcription
uses — a `===` banner plus "RE-TRANSCRIBE: <title>", "Model:
<model>", and "Transcribing with Whisper (<model>)..." lines.
In simple mode that's too much noise; the queue-added line and
Whisper progress line are already the meaningful events.

Wrapped those four lines with `if not _is_simple_mode`. Simple
mode now goes:

 — Added M.T. <title> to GPU-tasks queue
 — ✓ Whisper model loaded (<model>, CUDA).
 [1/1] Transcribing "<title>", 47%...

Verbose mode is unchanged.
```

#### v40.7 — Fix: /cmd/retranscribe was writing transcripts to the wrong files

```
Critical fix. Caught by the user after the first end-to-end test of
model-selection retranscribe from the viewer: the transcript refresh
toast fired, but both the Browse tab's source indicator and the
viewer's source banner kept showing the old "YT+PUNCTUATION" tag —
meaning the new Whisper output never actually landed in the files
the two apps read.

Root cause: v40.5's /cmd/retranscribe enqueued an mt-type item like
{"type": "mt", "file_path": ...}, which goes through
_run_manual_transcription. That function saves output via
`open(out_path, "w").write(text)` where out_path is a standalone
`<video>.txt` next to the .mp4. That file is a dead-end — nothing
reads it. The per-year Transcript.txt and Transcript.jsonl that the
Browse tab + external viewers read stayed untouched.

Fix:
- /cmd/retranscribe now joins videos → segments to pick up the
 stored jsonl_path, derives the matching .txt path by stripping
 the leading dot and swapping extension (same logic as
 _tp_panel._get_txt_path). Falls back to scanning the video's
 parent folder for " Transcript.jsonl" / " Transcript.txt" if
 the video has no segments yet.
- Builds the queue item as
 {"type": "mt", "file_path": ..., "model": ...,
 "retranscribe": True, "title": ..., "video_id": ...,
 "txt_path": ..., "jsonl_path": ...}.
- GPU worker's mt branch now has three sub-cases: folder-based MT
 (unchanged), retranscribe (NEW: routes to _run_retranscribe_job),
 single-file MT (unchanged — _run_manual_transcription).
- _run_retranscribe_job reproduces the Browse-tab _on_retranscribe
 worker body at module scope: _whisper_transcribe → capture BOTH
 text and segments, _whisper_punct_fixup, then
 _tp_panel._replace_txt_entry (updates the per-year .txt header
 to "(WHISPER <MODEL>)" and body to the new text),
 _tp_panel._replace_jsonl_entry (swaps the per-video segments
 block in the per-year .jsonl), and _tp_index_file to refresh
 the FTS search index immediately.

Result: retranscription via the command API now touches the same
files as the Browse tab's right-click. Source detection in the
viewer flips from "YouTube auto-captions" to "Whisper
transcription — <model>" on the next transcript fetch, and FTS
search starts returning the new Whisper content right away.
```

#### v40.6 — Model selection + GPU/Whisper status API for viewer integration

```
Follow-up to v40.5. Adds everything the external viewer needs to
show a proper "re-transcribing now" experience instead of a
fire-and-forget button.

/cmd/retranscribe extended:
- Now accepts {channel, video_id, model?} where model is one of
 tiny / small / medium / large-v3. Validates the value and
 returns 400 for anything else. If omitted, the GPU worker falls
 back to _whisper_model_choice like it always did.
- mt branch of _gpu_worker now reads item.get("model") and, if
 present, assigns to _whisper_model_choice for this job only.
 Mirrors the transcribe-branch pattern already in place at line
 21324, just extended to manual-transcription items.

New /cmd/gpu-status endpoint (GET):
- Returns current running GPU item (file_path, file_name, type,
 model, label) plus the pending queue (same shape, minus
 progress fields).
- Whisper progress: parses _whisper_dots["pct_str"] with a regex
 so viewers get a 0-100 number. Also includes whisper_active
 bool and the _whisper_counter batch {idx, total} for channel-
 level transcriptions.
- gpu_paused bool so viewers can say "queued but paused" instead
 of just "queued".
- Cheap read-only — polled every ~3 s by the viewer's watch view
 while open. Locks only briefly to snapshot the queue.

Viewer-side changes (in the ArchiveBrowserWithYTTest project, not
YTArchiver — but the APIs above are what they need):
- Model picker modal replaces the fire-and-forget link. User
 chooses tiny/small/medium/large-v3 before sending.
- Watch view polls /cmd/gpu-status every 3s. If the watched video
 is the current running job, shows a "Re-transcribing with
 Whisper (small) — 47%" banner with a live progress bar. If it's
 queued, shows "Queued for re-transcription — N ahead". If GPU
 is paused, banner annotates that too.
- When the current job transitions away from the watched video
 (re-transcription finished), viewer waits 1.2 s for the file
 write, refetches transcript + source, and if source category
 changed (e.g. yt_captions_punct → whisper) re-renders the
 watch view in place. Saves and restores the playback position
 so the user doesn't lose their spot.

Minor viewer bug fixes batched in:
- /api/youtube/retranscribe forwarder on the viewer side was
 still requiring "year" in the body from v40.5 (leftover from
 the earlier API shape). Dropped — YTArchiver looks up by
 (channel, video_id) now. Also now forwards the model param
 through.
- Source banner in the transcript panel was using display:flex
 which caused the inline "re-transcribe with Whisper" link to
 stack vertically in a 380px sidebar. Switched to display:block
 with inline-block dot; text flows naturally now.
- Right-click context menu on YT cards and search results now
 includes a Re-transcribe item that pops the same model picker.
```

#### v40.5 — Integration API, thumbnail dedup, pause-state fixes

```
Large push adding a localhost command API so an external viewer
project (ArchiveBrowserWithYTTest) can drive YTArchiver's existing
flows. Built alongside the viewer's integrity-check feature that
surfaced a pile of real archive-level data corruption in the
process.

New: localhost command receiver (stdlib http.server on port 9855,
daemon thread, started right before root.mainloop). Bound to
127.0.0.1 only — host-only by network layer, no separate auth.
 - GET/POST /cmd/ping — returns version, hostname, pid, current
 GPU-queue depth. Used by the viewer as a liveness probe.
 - POST /cmd/retranscribe {channel, video_id} — looks up the
 video in the index DB, then calls _add_to_gpu_queue with
 type='mt'. Identical code path to the Browse tab's right-click
 "Transcribe single file" action.
 - POST /cmd/repair-orphans {items:[{channel,year,filename},...]}
 — for each .mp4 path, clears date_resolve_failed_ts /
 date_resolve_failed_mtime / search_failed_ts /
 id_resolve_failed_ts / metadata_fetch_failed_ts so the row is
 eligible for another multi-pass resolve attempt. Caller is
 expected to queue metadata afterwards. Fixed mid-flight: the
 DB filepath lookup was hitting empty because config["output_dir"]
 is stored with forward slashes but videos.filepath uses all
 backslashes. Wrapped the os.path.join in os.path.normpath so
 slash-style mismatches resolve.
 - POST /cmd/repair-duplicates {items:[{video_id, locations}]}
 — for each duplicate video_id, reads the canonical title from
 the videos table for that id, finds the location whose
 filename prefix matches (normalized title compare), keeps it,
 deletes the rest. If no location's title matches, keeps the
 newest by mtime so at least one artifact survives. Thumbnails
 only — never touches the .mp4.
 - POST /cmd/repair-mismatches {items:[{channel, year,
 thumbnail_filename}]} — deletes each thumbnail whose embedded
 [video_id] disagrees with metadata.jsonl. Next metadata pass
 re-downloads the correct one.

Thumbnail downloader dedup (the root cause of 38 duplicate-
video_id cases the viewer's first integrity scan surfaced across
the archive):
- Before writing a new thumbnail, _download_thumbnail now scans
 the target .Thumbnails folder for any existing file containing
 [<video_id>]. If found under a different title prefix (because
 YouTube renamed the video since we last fetched), the existing
 file is renamed to match the new title instead of creating a
 second copy. Same video_id now maps to at most one thumbnail
 file per year folder.

Pause-state fixes. Four separate bugs all around the same area,
all surfaced while testing the /cmd/retranscribe integration:
- _sync_task_finished was unconditionally clearing pause_event
 whenever no task was running. Meant any state transition that
 reached _sync_task_finished silently wiped the user's manual
 pause. Removed the clear. Disk-error auto-resume has its own
 explicit pause_event.clear() at line 2418 so that path is
 unaffected.
- _add_to_metadata_queue's "auto-start if nothing else running"
 guard now also checks pause_event.is_set(). Previously, adding
 metadata to a paused sync queue fired the task immediately
 because the guard only looked at running-flags, which are all
 False during a pause.
- _start_metadata_task's re-queue-at-head condition now also
 checks pause_event. Belt-and-suspenders: even if some other
 caller somehow bypasses the first guard and reaches the task
 start, the item re-queues instead of running while paused.
- _save_queue_state_now now writes "sync_paused": pause_event.is_set()
 to the queue-state file, matching the existing gpu_paused pattern.
 _load_queue_state restores it — AND sets pause_event if any
 sync-pipeline items were restored, even if sync_paused wasn't
 explicitly true. Rule: launching with items in queue never
 auto-starts; the user has to explicitly hit Start or Resume.
- Both Start buttons (_global_start_all and the task-list popup's
 _start_sync_queue) now explicitly clear pause_event (and
 _gpu_pause for global) before calling _process_next_queued.
 Otherwise with the new guards, clicking Start while paused
 would just re-queue the top item and nothing would happen.

Behaviour changes for anyone paying attention:
- Queues restored on launch now show as paused. Start/Resume to
 run them. Previously they'd just sit (same result, different
 visual).
- Items added while paused stay queued. Previously metadata
 adds would fire right away.
- Explicit pause now survives across task completions. Previously
 it got silently cleared on every task finish.

Unrelated investigation outcome: a viewer-side integrity scan
across the whole archive found 38 duplicate [video_id] thumbnail
filenames, 18 thumbnail/metadata mismatches, and 491 orphan .mp4
files (most of them bodycam-style channels where the date-based
resolver legitimately failed — short or too-generic filenames,
multiple uploads on the same date with ambiguous title overlap).
Prevention now landed via the thumbnail dedup; existing dupes
cleaned up by /cmd/repair-duplicates. Orphans need a full
metadata re-queue to give the resolver another crack at them.
```

#### v40.4 — Pre-release bug sweep

```
Full line-by-line audit across 33k lines done in two parallel-agent
passes. Safe fixes applied, behavior-visible choices deferred to
explicit approval. Nothing on this list changes how a feature works
for users who don't hit the specific edge case being fixed.

Threading / race conditions:
- _gpu_worker was missing _whisper_model_choice in its global
 declaration. The queue item's requested model was being written
 to a shadow local and silently discarded — queued transcriptions
 that asked for a specific model (instead of inheriting the
 global) were actually running with whatever the global had last
 been set to. Added the global decl so the assignment takes.
- Three config writes (whisper_model updates in 2 transcription
 worker paths + the GPU Tasks dialog path) were mutating the
 shared config dict and calling save_config without holding
 config_lock. Could theoretically corrupt config.json if another
 thread saved at the same instant. Wrapped all three with
 `with config_lock:`. config_lock is an RLock so there's no
 deadlock risk on re-entry.
- Browse tab Actions button: if you rapidly switched between two
 videos that both required a background "is the video file
 actually there?" check, a stale thread from the first selection
 could enable the button AFTER the second selection had disabled
 it. Added a "is this check still about the currently-selected
 video?" guard so only the right thread's result wins. Mirrors
 the pattern already used in the Play button check.

Off-by-one / math:
- Whisper chunk split formula: `if dur % _MAX_SEG_SECS > 1 else 0`
 returned ceiling-minus-one for videos whose length modulo the
 chunk cap was exactly 1 second. A 61-second clip with a
 30-second cap was splitting into 2 chunks of 30.5s instead of
 3 chunks (30 + 30 + 1). Changed >1 to >0 at all three sites so
 the chunks always stay at or below the cap.

Error handling / resource lifecycle:
- SQLite connection in the stale-index check worker didn't close
 on exception (the .close() sat after the query block, not in a
 finally). If any of the queries raised, the connection and the
 WAL lock on the db file leaked. Wrapped with try/finally plus
 None-guard.
- Fixed the same pattern in the TranscriptionPanel stats worker.
- Added timeout=10 to the ffmpeg -version and yt-dlp --version
 probes, and timeout=600 to the tray-dependency pip install.
 The startup version checks could theoretically hang on a
 corrupt exe or a stalled network drive.
- Added a dedicated concurrent.futures.TimeoutError branch in the
 captions prefetch handler so a 120s timeout logs as "Timed out
 after 120s" instead of the generic "Failed:".
- Dim log line now appears when an invalid batch size is entered
 in channel settings, explaining that the default (20) is being
 used instead. Previously it silently fell back with no feedback.

Month / date parsing:
- Upload-date month parse in the folder-placement fix step now
 handles non-numeric or out-of-range month values gracefully
 (falls back to "Unknown Month" rather than letting ValueError
 propagate and abandon the rename).
- Browse-grid date_str parse now bounds-checks the month before
 indexing into _TP_MONTH_NAMES. A bad month digit falls back to
 a year-only date string rather than the exception handler.

UI / log polish:
- Fixed a long-standing issue where the mini-log strips on the
 Subs / Recent / Browse tabs did not color the "active" status
 lines correctly for Redownload, Compress, or Reorg modes. The
 sync function's tag priority list and the mini-log's tag
 configuration list were both missing the simplestatus_redwnl /
 _compress / _reorg variants (plus the matching simpleline_*
 and *_bracket tags for history rows). Added all of them. Now
 the chartreuse Redownloading / purple Compressing / orange
 Reorganizing status lines mirror across to the mini logs the
 same way sync's green line already did.
- Recent tab "Downloaded" column now sorts the same direction as
 every other column. Was inverted (`reverse=not reverse`) — a
 leftover from a previous fix that's now obsolete. Clicking the
 header once sorts newest-first, clicking again sorts
 oldest-first, matching user expectation.
- Harmonized the three different "Install Whisper AI" dialog
 wordings. Batch variant that already showed a count of affected
 videos remained the template; single-file and folder variants
 now use the same "is needed for transcription" phrasing and
 same "Whisper requires ~2.5 GB" line.
- Standardized "Redownload" (no hyphen) across every user-facing
 string. Menu items, dialog titles, confirmation bodies, log
 messages. Was a mix of "Re-download" and "Redownload" — the
 code counts overwhelmingly favored the no-hyphen form so we
 went with that throughout.
- Renamed "Un-Organize Folder" menu item to "Un-Org. Folder" so
 it visually matches the sibling "Org. Folder by Year" /
 "Org. Folder by Year/Month" items in the same context menu.
 Internal queue labels and dialog body text still use the full
 "Un-Organize" spelling.
- Year-range stat in the transcript panel now shows "N/A"
 instead of literal "2024 – None" when only one of MIN/MAX year
 is populated in the index.
- Channel count in the stats label now uses thousands separator
 (consistent with the segments and videos counts).

Dead code / cleanup:
- Removed two duplicate module-level initializations and three
 redundant local `import shutil` statements inside exception
 handlers (global shutil is already imported at line 13).
- Simplified `1 <= (month or 0) <= 12` to `1 <= month <= 12` at
 six sites — the `month is not None` check immediately before
 already makes the `or 0` fallback unreachable.

Subprocess wait / cleanup paths:
- Added `wait(timeout=10)` after `kill()` at the backlog-compress
 yt-dlp path so a stuck process gets reaped instead of leaked.
- Added `wait(timeout=5)` after `kill()` on the yt-dlp update
 check path for the same reason.
- Added `on_chan_list_select(event=None)` default so the several
 call sites that pass None are explicit about it.
```

#### v40.3 — Activity log column alignment, pause log timestamps, UX polish

```
- Activity log columns now align vertically across all entries. Each
 column (count, label, skipped count, label, errors, duration) is
 center-aligned in a uniform width computed from the widest cell in
 that column. Old 3-column entries (legacy Metdta) right-align so
 their shared existing/errors/duration columns still line up with
 newer 4-column rows. Widths adapt when the program is resized.
- All user-initiated pause/resume log entries now include the time.
 Previously Compression, Backlog, and Redownload entries showed
 only "Compression paused." / "Compression resuming..." with no
 timestamp — they now match sync/metadata/transcription format:
 "⏸ Compression paused at H:MMam/pm." and "▶ Compression resumed
 at H:MMam/pm..."
- Removed the blank line the pause entries were inserting above
 themselves in the log.
- Fixed the taskbar/tray icon still spinning after all tasks were
 paused. The task buttons correctly hold their solid "paused"
 colour, but the tray spinner kept animating because the blink
 system held it open. Blue tray spin now freezes on the base icon
 while sync is paused, mirroring the existing behaviour for GPU.
- Redownload sample-confirm popup ("check first 10 then ask") now
 auto-continues after 5 minutes if left unattended. A visible
 "Auto-continuing in M:SS..." countdown ticks down under the
 stats. Clicking Continue, Change Resolution, Cancel, or the X
 cancels the timer.
```

---
#### v40.2 — Fix Browse tab crash under heavy I/O, queue drag, UI polish

```
- Fixed critical crash: Browse tab locked up permanently when navigating
 while a redownload task was running. Root cause was os.path.getmtime()
 calls on the main thread blocking the UI under disk I/O contention.
- Removed all blocking file I/O from the main-thread grid card builder.
- Skipped expensive mtime loop for "All Channels" scope (dateless videos
 are filtered out anyway — no point statting thousands of files).
- Added abort checks to the mtime and thumbnail scanning loops so
 navigating away cancels stale background work promptly.
- Fixed date_str edge case where non-standard upload_date formats left
 date_str empty, triggering unnecessary mtime fallback.
- Fixed queue drag-to-reorder not working when batch sync entries were
 present (collapsed display rows broke the position-based index math).
- Fixed drag-down not landing at the correct position (off-by-one from
 a bad post-pop index adjustment).
- Redownload status now shows as a bottom-pinned animated line (like
 sync/metadata/compress) instead of a placeholder in the activity log.
 Eliminates orphaned "running..." lines after crashes.
- Cleaned up orphaned redownload history lines on startup.
- Whisper model choice now persists across restarts. GPU tasks restored
 from queue no longer re-prompt for model selection.
- Right-click "Transcribe Channel" on already-transcribed channels now
 skips the organization dialog and follows the existing layout.
```

#### v40.1 — Fix startup freeze from DB lock contention

```
- Fixed ~10 second UI freeze ("Not Responding") during startup caused
 by slow aggregate queries (COUNT DISTINCT on 8.7M-row segments table)
 holding _db_lock and blocking the main thread.
- Moved _ensure_videos_populated to background preload thread so it
 never blocks the UI.
- Slow stats/index-freshness queries now use separate read-only DB
 connections instead of competing for _db_lock with disk scan and
 grid preload threads.
- Redownload activity log placeholder now shows "⏸ paused" / "▶ running..."
 status in real time when pause is toggled.
```

#### v40.0 — Fix Browse tab sort stability for same-day videos

```
- Browse tab "Newest" sort now uses file mtime as a tiebreaker within
 the same day. Previously, switching sort modes and back (e.g. Most
 Popular → Newest) would shuffle same-day videos into a different
 order because they all shared the same YYYYMMDD sort key.
- Both the initial load sort and the re-sort function are fixed.
```

#### v39.9 — Auto-index after downloads, background grid preload

```
- New opt-in "Auto-update index" setting in the Index tab: after a
 configurable number of downloads (default 10), automatically runs
 an incremental index rebuild in the background.
- Browse tab grid data is now silently preloaded for all channels
 after startup. Opening the Browse tab and clicking any channel
 is instant — no more "Loading..." spinner.
- Grid cache limit increased from 20 to 200 scopes.
- "All Channels" preloads first since it's the most common first click.
- Fix blank grid when navigating to a preloaded channel for the first
 time (canvas sizing on initial cache-hit render).
- Startup log lines unified to --- format; new "Browse tab load
 complete" log when all channels are preloaded.
```

#### v39.8 — Log coloring tweaks for punctuation model and download lines

```
- "Loading punctuation model..." now shows a blue em dash prefix
 instead of solid blue text, matching other transcription log lines.
- "Punctuation model loaded (CUDA)" line now uses white text instead
 of green; the blue em dash was already correct.
- Download completion lines (✓ lines) now have a green em dash prefix.
 Only the em dash, checkmark, and file size are green; title, channel,
 and date are white for readability.
```

#### v39.7 — Recursive manual folder scan, redownload scoping fix

```
- Manual folder transcription now scans subfolders recursively when
 counting video files, matching the actual transcription worker.
 Previously folders-within-folders returned "No video/audio files found".
- Fix UnboundLocalError in redownload worker: the sample-popup
 "Change Resolution" branch assigned new_res inside _worker(), causing
 Python to treat it as an unbound local throughout the function.
 Added nonlocal declaration.
```

#### v39.6 — Metadata pending counter, playlist pause, verbiage fix

```
- Add metadata_pending counter mirroring transcription_pending: Subs
 tab metadata column now shows ✓ -N when videos need metadata.
 Counter decrements per successful fetch so partial completions
 (e.g. pause after 3 of 6) are reflected accurately.
- Queue Pending button now queues both transcription and metadata
 channels with pending videos.
- Playlist fetch phase ("Fetching channel playlist...") now shows
 "will pause after this step" when pause is requested, and enters
 pause loop after the subprocess completes.
- Change "use Refresh Metadata to retry" to "video likely deleted"
 for permanently unmatchable videos.
```

---
#### v39.5 — Log styling consistency pass

```
- Fix em-dash-only coloring for GPU queue addition lines: transcription
 (blue) and compression (compress color) were coloring the entire line
 instead of just the em-dash character. Now matches metadata (pink)
 behavior — only the — is colored, rest is default white.
- Standardize all Sync List queue addition headers to consistent
 "Added to Sync List: [action] [channel]" format across sync, reorg,
 redownload, and standalone transcription.
- Normalize reorg completion footer from "--- ... ---" to "=== ... ==="
 to match sync and redownload completion style.
- Revert last session's unauthorized git-only divergences: GPU queue
 header format, removed model selection, restored reindex popup,
 added GPU completion message.
```

#### v39.4 — Fix stale pause lines persisting through clear log

```
- Clear log was unconditionally preserving all pause/resume lines even
 after tasks finished. Now only preserves them if a task is actively
 paused (pause_event set AND a task running).
```

#### v39.3 — Metadata pause fix, direct-ID metadata fast path

```
- Metadata queue now respects pause between channels. Previously
 channels that already had metadata returned instantly, chaining the
 next channel without ever checking pause_event.
- Auto-metadata after sync now passes the downloaded video IDs directly
 to the metadata worker, which fetches metadata for just those videos
 without enumerating the entire channel playlist. Turns a multi-minute
 playlist scan into a few-second direct fetch.
```

#### v39.2 — Fix Most Viewed sort, add refresh-all metadata option

```
- Fix silent crash in Most Viewed sort: yt-dlp metadata stores null
 for view_count on some videos. dict.get("view_count", 0) returns
 None (not 0) when key exists with null value, crashing the sort
 comparator. Fixed with `or 0` coercion in enrichment + sort.
- All Videos right-click menu now shows two options:
 "Queue all for Metadata" (existing) and "Refresh all (update
 views/likes)" which re-fetches counts without re-downloading comments.
```

#### v39.1 — Fix All Channels view, column indicators, tree collapse

```
All Channels view:
- Fix hang caused by ffprobe fallback running on 90K+ videos (skipped
 for __all__ view)
- Fix breadcrumb showing "__all__" instead of "All Channels"
- Add per-channel loading progress (Loading metadata... 14/102 — name)
- Filter out dateless/orphaned videos from All view
- Skip tree preexpand for __all__ (no title nodes to expand)

Subs tab:
- Remove broken color emoji (tkinter can't render them), use plain text
- Apply same indicator style to Metadata and Compress columns (was only
 Transcribe): blank if off, ✓ if done, A ✓ if auto+done

Browse tree:
- Revert broken custom +/- indicators to working defaults
- Add generation guard to preexpand callbacks so stale delayed callbacks
 can't re-open nodes the user just collapsed

Sort:
- Most Viewed sort uses .get() for view_count to prevent silent crash
```

#### v39.0 — Verbose debug mode overhaul, Browse UI improvements, UX polish

```
Verbose log mode:
- All previously suppressed info now shown: DLTRACK lines, yt-dlp nag
 messages, filter re-skips, ffmpeg commands + raw stderr, Whisper CUDA
 fallback reasons, channel count stderr, yt-dlp re-download commands
- Throttles relaxed: enum progress every 100 (was 500), download
 progress unthrottled, scan progress every item (was every 25),
 metadata scan every file (was every 10%), download count every DL
 (was every 10)
- Log cap raised to 50k lines (was 20k), with trim notice
- ~30 silent except:pass blocks now log errors: queue state save, sync
 progress, redownload progress, ffprobe functions, whisper cache
 writes, folder-fix moves, batch callbacks, and more
- Whisper [idx/total] counter now shown in both modes
- Simple mode completely unchanged (verified all ~50 edits)

Browse tab:
- "All Channels" aggregate view at top of tree with year/month folders,
 cross-channel metadata + thumbnail loading, channel name on cards
- Duration badge ffprobe fallback for videos without metadata
- Sort selection now persists when switching between channel/subfolder
- Larger +/- expand/collapse indicators (custom 14px images)
- Loading screen shows "Loading All Channels..." for aggregate view

Search tab:
- Matched terms bracket-highlighted in snippet column: [match]
- Pulsing indeterminate progress bar during search

Subs tab:
- Transcription column: emoji color indicators (🟢 complete, 🟡 pending),
 "A" prefix for auto-transcribe, removed redundant "Done" text

Video player:
- Seek bar now supports click-to-jump (not just drag)

UI:
- Log mode selector changed from radio buttons to dropdown
```

#### v38.9 — Fix metadata resolution gaps + Pass 6b timeout scaling

```
Three fixes:
1. Title normalization: unsafe chars (*, /, ", :, |) now replaced with
 spaces instead of stripped, matching yt-dlp's filename sanitization.
 Fixes mismatches like "9/11" → "911" vs "9 11" and "P*do" → "pdo"
 vs "p do" that prevented metadata matching on renamed videos.
2. Pass 6b timeout: now scales with candidate count (30s per video,
 120s min) instead of flat 30-minute deadline. Added --socket-timeout
 30 so yt-dlp doesn't hang indefinitely on unreachable videos.
3. Pass 6 exclusion: zero-candidate rows (video deleted/private on
 YouTube, no data exists for that upload date) now properly marked
 as date_resolve_failed. Previously these re-ran the full pipeline
 on every metadata fetch — now they're skipped after first failure.
```

#### v38.8 — Code quality refactor: extract shared helpers, eliminate duplication

```
Full-codebase sanity check (32,700 lines reviewed). Consolidated ~56
duplicated call sites into 11 shared helper functions. No behavior
changes — purely structural cleanup for maintainability.

New helpers: _ensure_videos_tab, _channel_folder_name, _any_task_running,
_resolve_upload_date, _format_duration_hms, _normalize_yt_title,
_drain_skip_and_advance. Also reused existing _load_archived_ids,
_is_partial_file, _ffprobe_duration at sites that were re-implementing
them inline. Refactored _simple_anim_tick from 7 copy-paste blocks to
data-driven rendering. Net: -122 lines (139 added, 261 removed).
```

---
#### v38.7 — Fix WinError 206 from uncapped URL list

#### v38.6 removed the cap on Pass 6b candidates but still passed every

```
URL as a command-line argument to yt-dlp. Channels with hundreds of
candidates (e.g. Bernie Sanders) exceeded Windows' ~32K char command-
line limit, causing [WinError 206]. Fixed by writing URLs to a temp
batch file and using yt-dlp's --batch-file flag instead. Temp file
is cleaned up in the finally block regardless of success/failure.

Should have caught this before shipping v38.6 — removing a cap on
a list that gets serialized onto a command line is an obvious
platform-limit issue on Windows.
```

#### v38.6 — Remove Pass 6b cap (one-shot legacy/migration cleanup)

```
Reframed Pass 6b from "bounded recurring optimization" to what it
actually is: a ONE-SHOT legacy/migration cleanup that runs until
every stuck row is rescued, then disappears forever.

WHY THE CAP WAS WRONG:
Pass 6b only fires for rows that entered the DB with NULL
video_id. For anyone downloading exclusively through YTArchiver,
that never happens — the DLTRACK handler parses the video_id out
of yt-dlp's --print output and record_download() writes it to
the DB at the moment the file hits disk. Pass 1 then resolves
every new row instantly, _need_search stays empty, and Passes
4-6b are all skipped via their gates.

Pass 6b only exists to clean up the legacy cases:
 (a) Stragglers from older YTArchiver versions that didn't
 write video_id at download time — the user's 1,007 stuck rows
 (b) Files imported from other archive tools
 (c) Manually-dropped files picked up by the browse-tab
 scanner (register_video with filepath but no video_id)

In all three cases, the goal is to resolve the row ONCE, write
video_id to the DB, and have Pass 1 skip it forever after. The
per-run cap of 30 candidates was treating Pass 6b like a recurring
optimization that needed runtime bounding, which was wrong — it
was dragging out a finish-and-disappear cleanup over 13+ runs for
big channels instead of just completing in one run.

CHANGES:
- Removed _MAX_6B = min(30, max(5, len(_need_search) * 3)) cap.
 Pass 6b now processes every plausible candidate from the fuzzy
 pre-filter in one go.

- Bumped _6b_deadline 600s (10 min) → 1800s (30 min) to give
 large legacy cleanups room to complete without mid-run timeout.
 The deadline still exists as runaway subprocess protection;
 normal progress is gated on cancel_event and yt-dlp stdout EOF.

- The fuzzy ≥50 pre-filter stays unchanged. Without it, channels
 like HasanAbi (thousands of undated videos, tens of stuck rows)
 would fetch dates for the entire channel bucket, which is pure
 waste. With the pre-filter, Pass 6b only fetches candidates
 whose title is at least in the ballpark of an unresolved local
 row — cost scales with actual work, not with channel size.

- Reframed the Pass 6b block comment with the new mental model
 so a future edit doesn't re-introduce the cap under the wrong
 assumption. Explicitly notes that fresh-user flows never hit
 this pass and that the cap removal is zero-cost for them.

EXPECTED IMPACT FOR THE 1,007 STUCK ROWS:
- 52 of 57 affected channels had ≤30 stuck rows and would clear
 in Run 1 even with the old cap — still resolve in Run 1 now.
- 5 big channels (EWU Bodycam 365, Two Minute Papers 198,
 Midwest Safety 113, Dr Insanity 41, HasanAbi 35) previously
 needed 2-13 runs. With the cap gone, they should clear in a
 single run (bounded only by yt-dlp fetch rate).
- Remaining after full convergence: ~14 rows (metadata_fetch_
 failed — deleted/private/region-locked, permanent ceiling).
```

#### v38.5 — Log message polish + stale comment cleanup (cosmetic)

```
Post-v38.4 sanity sweep. No behavioral changes — just a handful of
inconsistencies between the "clean simple mode" direction the user set
for v38.4 and the actual log output / code comments.

- PASS 6a — simple-mode success message no longer exposes the
 library name. Was: "— Fuzzy-matched N video ID(s) via thefuzz."
 Now: "— Matched N video ID(s) by title." This matches the
 cleaned-up Pass 6b summary ("— Matched N video(s) by upload
 date.") for consistent user-facing language.

- PASS 6b — red error paths no longer leak internal phase names.
 Was: "⚠ Pass 6b: per-ID date fetch timed out." / "⚠ Pass 6b:
 per-ID fetch failed: {e}"
 Now: "⚠ Date fetch timed out." / "⚠ Date fetch failed: {e}"
 These only fire on failure/timeout, but they're shown in simple
 mode (red tag), so they should match the same no-jargon rule.

- STALE COMMENT — Pass 6a docstring said "collect matches ≥ 85"
 but the actual threshold was lowered to 75 in v38.4. Updated to
 match. No code change.

- STALE COMMENT — Smart cap comment said "3 unresolved rows → max
 10 fetches" but the formula min(30, max(5, 3*3)) gives 9, not
 10. Corrected to "max 9 fetches (floor of 5)".

- STALE REFERENCE — The batch-resolve comment mentioned populating
 _yt_by_date and _yt_all_list, but _yt_all_list doesn't exist in
 the code (pre-dates a refactor). Removed the dangling reference.

- VERBOSE DIM LOG — Pass 6b diagnostic line changed "undated
 entries" → "no-date entries". This line is dim-only (verbose
 mode), so the user never sees it in simple mode, but kept the "no
 internal jargon" theme consistent for when verbose mode is on.

BEHAVIORAL HEADS-UP (from the sanity sweep):
- Initial concern was that the 1,007 stuck rows might already be
 flagged with date_resolve_failed_ts from v38.0-v38.3's Pass 6,
 which would filter them out before Pass 6b could rescue them on
 a normal metadata run.
- Live DB query result: ZERO rows are currently flagged. The
 stuck rows never reached Pass 6's "ambiguous match" branch (the
 only place that writes the flag) because _candidates was always
 empty (yt-dlp returns NA dates universally, so everything ends
 up in the "" bucket and Pass 6 can't find any dated candidates).
- Net: v38.4's Pass 6b WILL naturally process all 1,007 stuck rows
 on the next normal metadata run. No refresh needed.

PRE-PUSH HOOK FIX:
- Local-only .git/hooks/pre-push was blocking tag-only pushes
 after the main branch push completed (both refs show v38.5, so
 the hook said "version didn't bump"). Added a tag-only skip
 check: if every ref being pushed is refs/tags/*, skip the
 version checks entirely. Tag pushes can't change APP_VERSION, so
 enforcing the bump rule on them was a false positive.
```

#### v38.4 — Smarter Pass 6b + lower Pass 6a threshold + cleaner logs

```
- ROOT DIAGNOSIS: yt-dlp's --flat-playlist mode now returns
 upload_date=NA on every channel (confirmed by direct testing on
 2kliksphilip, 3kliksphilip, Branch Education — all return NA for
 upload_date and timestamp in flat mode). This means Pass 6's
 date-based matcher has effectively been broken channel-wide
 because _yt_by_date never gets populated with dated buckets.
 Pass 6a (fuzzy title matching) picks up the slack but it has to
 work harder, and Pass 6b (per-ID date fetch fallback) was
 running too aggressively.

- PASS 6a — lowered fuzzy threshold from 85 to 75.
 With dates gone, Pass 6a is now the primary resolver for rows
 that lack video_id. A stricter 85 threshold meant more rows
 fell through to the expensive Pass 6b per-ID fetch. 75 is still
 confident enough for distinct titles but catches more minor
 drift (emoji strip, case differences, punctuation variations).

- PASS 6b — pre-filter candidates by fuzzy score before fetching.
 The "" (undated) bucket can contain hundreds of videos for a
 channel (since flat-playlist is always NA). v38.3 fetched up to
 50 candidate dates blindly, which was wasteful when only a
 handful of local rows actually needed matching. v38.4 pre-scores
 every "" bucket candidate against every unresolved local row
 via fuzz.token_sort_ratio and keeps only candidates with score
 >= 50. Those are "plausible matches worth the fetch cost" — still
 below Pass 6a's confident 75 threshold but above noise.

- PASS 6b — smart cap scales with unresolved row count.
 Old fixed cap of 50 replaced with min(30, max(5, len(need_search) * 3)).
 3 unresolved rows -> max 10 fetches. 10 unresolved rows -> max
 30 fetches. Never more than 30 (roughly 90s of per-ID fetches).

- USER-FACING LOGS cleaned up for simple mode:
 * Scanline now reads "Matching N video(s) by upload date... X/Y (Zs)"
 instead of "Fetching real dates... X/Y (Zs)". Clearer intent,
 no "undated candidates" terminology that was confusing.
 * Final summary reads "— Matched N video(s) by upload date."
 instead of "— Resolved N video ID(s) via per-ID date lookup."
 * "Pass 6b: capping fetch to 50 of 446 undated candidates." is
 moved from simpleline_pink to dim so it only shows in verbose
 mode. The simple-mode user never sees internal pass jargon.
```

#### v38.3 — Metadata resolver: Pass 6b per-ID date fetch as robust fallback

```
- METADATA RESOLVER — Pass 6b for channels that return NA dates
 Backup pass for the edge case where --flat-playlist returns
 upload_date=NA for every video on a channel (caught Branch
 Education's 7 emoji-titled videos initially). v38.2's fixes
 (lowered Pass 4 threshold + "" sentinel bucket + Pass 6a fuzzy)
 already handle this via title-based fuzzy matching, and it was
 confirmed working on Branch Education while v38.3 was being
 written — the bulk metadata run reached that channel in queue
 order and Pass 6a matched all 7 rows at 100%.
 But Pass 6a relies on thefuzz.token_sort_ratio matching local
 filename tokens to YT titles. If the local filename is truly
 different from the YT title (beyond what token_sort can handle —
 channel renames, manual file renames, heavy encoding drift,
 non-ASCII corruption, etc.), fuzzy will miss. For those cases,
 Pass 6b uses the file's mtime directly against real YT upload
 dates, which is the "if we know the file's date and the channel
 uploads ≤1/day just match them" approach that's been the goal
 all along.
 Trigger: Pass 6b runs INSIDE the existing
 `if _yt_by_date and not cancel_event.is_set():` block, AFTER
 the Pass 6 date-based loop, WHEN:
 * _need_search is still non-empty (rows remain unresolved)
 * _yt_by_date has a "" sentinel bucket (undated candidates)
 * ch_url is available
 * cancel_event is not set
 Algorithm:
 1. Gather (vid, title) pairs from _yt_by_date[""] filtered
 against _date_known (exclude already-resolved IDs). Cap at
 50 entries as a safety net against pathological channels.
 2. Build URL list: "https://www.youtube.com/watch?v={vid}"
 for each candidate.
 3. Invoke yt-dlp in batch mode WITHOUT --flat-playlist using
 --skip-download --print "%(id)s|||%(upload_date)s|||%(timestamp)s"
 with all URLs in one invocation. Non-flat mode is slower
 but returns real upload dates (confirmed via empirical test:
 ~22 seconds for 7 candidates on Branch Education).
 4. Stream stdout, parse each line, build vid -> YYYYMMDD dict.
 Update scanline every second with progress counter.
 10-minute deadline as timeout backstop.
 5. For each remaining unresolved local row:
 a. Get file_date from os.path.getmtime(filepath).
 b. Walk candidate dates, collect those within ±1 day of
 file_date (timezone safety, same tolerance as Pass 6
 proper).
 c. If exactly 1 candidate matches: direct assignment.
 d. If multiple candidates match (multi-upload-per-day
 channels): use fuzz.token_sort_ratio as tiebreaker with
 a more lenient 70 threshold since date has already
 narrowed the field.
 e. If match found: append to _resolved_rows, strip from
 _need_search, clear ALL failure flags on the row
 (search_failed_ts, id_resolve_failed_ts,
 date_resolve_failed_ts, date_resolve_failed_mtime).
 6. Log summary: "— Resolved N video ID(s) via per-ID date
 lookup." in simpleline_pink (visible in simple mode).
 Bounded cost:
 * Only runs when Pass 6a and Pass 6 date-based have both left
 unresolved rows behind.
 * Only fetches candidates that are still in the "" bucket and
 not yet in _date_known.
 * Capped at 50 per-ID fetches per metadata run.
 * Most runs will skip Pass 6b entirely (no undated candidates
 to fetch).
 Live scanline format (simple mode only, updated once per second):
 "Fetching real dates... 12/14 (17s)"
 Error handling: try/except around the yt-dlp call, timeout
 enforcement, cleanup_process(_6b_proc) in finally. Failures log
 a red warning and continue — never kill the whole metadata run.
```

#### v38.2 — Metadata resolver: handle undated videos + always run Pass 4

```
- METADATA RESOLVER — two related fixes for Branch Education's 7
 emoji-titled videos, which had been stuck through v37.8 → v38.1
 despite every "this should fix it" attempt. Post-mortem diagnosis
 by directly querying the DB + simulating yt-dlp against the live
 channel revealed the real root cause.

 THE PROBLEM (finally, for real):
 All 7 rows had search_failed_ts AND id_resolve_failed_ts set but
 date_resolve_failed_ts = NULL. v38.1's cleanup pass was a no-op
 because the flag it cleared was never set in the first place.
 The reason Pass 6 never set it: when Pass 6's date-based matcher
 ran with wrong mtimes, _yt_by_date.get(wrong_date) returned [],
 and the "no candidates" branch was a plain `continue` without
 marking the row as failed.

 Deeper: even after FixDates fixed the mtimes, Pass 6 still failed.
 Why? Running yt-dlp --flat-playlist against this channel returns
 upload_date = "NA" for EVERY video (all 48 of them, 0 dated).
 The Pass 4 and Pass 6 stdout loops both had this pattern:
 if _resolved_date:
 _yt_by_date.setdefault(_resolved_date, []).append((vid, t))
 Undated videos were silently DROPPED. Since Pass 6a's fuzzy
 matcher builds its candidate list by flattening
 _yt_by_date.values(), those 48 videos were invisible to fuzzy
 matching too. And Pass 6 date-based obviously can't match
 dateless entries. So neither pass could resolve them.

 On top of that, Pass 4 was gated on `len(_need_search) > 10` —
 for small channels like this one (7 unresolved rows), Pass 4
 never ran at all, leaving Pass 6 to run its own fetch with the
 same drop-undated bug.

 THE FIXES (2):

 1. Lowered the Pass 4 threshold from `len(_need_search) > 10` to
 `_need_search` (any unresolved rows). The playlist fetch is
 cheap and guarantees _yt_by_date / the full title→id map are
 populated for Pass 6 / Pass 6a regardless of channel size,
 avoiding the second fetch path entirely.

 2. In BOTH the Pass 4 stdout loop and the Pass 6 stdout loop,
 added an `else` branch for undated videos that stashes them
 under a "" sentinel key:
 else:
 _yt_by_date.setdefault("", []).append((vid_id, title))
 Pass 6a's flatten picks up this bucket via
 _yt_by_date.values(), so fuzzy matching sees every video
 regardless of whether yt-dlp returned a date. Pass 6's
 date-based matcher looks up specific "YYYYMMDD" keys and
 naturally ignores the "" bucket, so there's no risk of
 mismatching a dateless video against the wrong local file.

 Also cleaned up the Pass 6a success path to clear
 id_resolve_failed_ts alongside search_failed_ts,
 date_resolve_failed_ts, and date_resolve_failed_mtime for
 consistency — once a row has a video_id, none of those flags
 are meaningful anymore.

 SIMULATION CONFIRMED:
 Before shipping, ran the fix logic against the live DB + live
 yt-dlp output. All 7 stuck files scored 100% fuzz match against
 their correct YT videos via token_sort_ratio. The 34 already-
 resolved rows stay resolved, the 7 stuck rows pick up their
 correct IDs, and the metadata fetch runs against the 7 new IDs.
- CODE COMMENT CLEANUP
 Scrubbed personal references from code comments throughout
 YTArchiver.py — removed channel-specific examples ("David Pakman",
 "3kliksphilip"), tool-specific references ("FixDates v5"), and
 path-specific examples from the slash-mismatch dedupe comment.
 Going forward, all commit messages, release notes, and code
 comments stay generic per the public-content-privacy rule.
```

#### v38.1 — Stale date-failure flag invalidation when file mtime changes

```
- METADATA RESOLVER — mtime-aware date_resolve_failed_ts invalidation
 Root cause of Branch Education's 7 emoji-titled videos staying
 stuck with no metadata even after running FixDates v5:
 date_resolve_failed_ts was a one-way latch. When the videos first
 went through metadata resolution their mtimes were wrong (all set
 to , the download date) and Pass 6 couldn't find any YT
 videos uploaded on that date, so it marked them all as permanently
 "date-failed". the user then ran FixDates v5 to correct the mtimes
 to the real YouTube upload dates — but the failure flag was still
 set. On subsequent metadata runs:
 * The pre-check's _pc_date_failed count hit 7
 * _pc_effective dropped to 34 (41 - 7)
 * The "all metadata covered" fast-exit fired because
 _pc_covered (34) >= _pc_with_ids (34) and the JSONL entry
 count covered _pc_effective
 * Pass 6 and Pass 6a (fuzzy matching) never got a second chance
 Fix: invalidate the stale flag by comparing the stored mtime to
 the live file mtime.
 Schema change:
 * New column date_resolve_failed_mtime REAL on the videos table
 — stores the file's mtime at the time date-resolution failed
 New cleanup pass at the top of _run_metadata_download:
 * Loads every row in the current scope (channel ± year/month)
 with date_resolve_failed_ts IS NOT NULL AND video_id IS NULL
 * For each, reads the file's current mtime
 * If stored mtime is NULL (pre-v38.1 row), or differs from
 the current mtime by more than 1 second (fs granularity /
 float precision tolerance), queues the row for clearing
 * Batch-clears date_resolve_failed_ts AND date_resolve_failed_mtime
 for all queued rows in one UPDATE ... WHERE id IN (...)
 * Logs "— Cleared N stale date-failure flag(s) (file mtime changed)."
 in simpleline_pink (visible in simple mode) so the user can see
 the invalidation fired
 Updated failure setter at the end of Pass 6 to ALSO store the
 current _mtime value alongside date_resolve_failed_ts, so future
 invalidation can compare correctly. _mtime is already computed
 earlier in the same loop iteration from os.path.getmtime, so no
 extra stat call is needed.
 Updated every success path (Pass 6a fuzzy match, Pass 6 direct
 date match, and the refresh=True clear-all) to also clear the
 new date_resolve_failed_mtime column, keeping the schema tidy
 (no orphan mtime values for rows with no failure flag).
 Migration-friendly: every existing row with date_resolve_failed_ts
 set has NULL stored mtime because the column didn't exist before.
 The cleanup treats NULL as "changed" and clears the flag, so on
 the first v38.1 run every previously-stuck row gets a fresh shot.
 Branch Education's 7 will then fall through to Pass 6a (thefuzz
 emoji matching from v37.9/v38.0) or Pass 6's "1 candidate on this
 date" direct match since the channel uploads ≤1/day.
```

#### v38.0 — Live prep scanline for metadata download on large channels

```
- METADATA DOWNLOAD — live status line during ID resolution
 Big channels like David Pakman were sitting for 10+ minutes on
 "Metadata: preparing X..." with no visible progress in simple
 mode, because every internal log call in the resolution pipeline
 used the "dim" tag which is filtered out of simple mode. The
 heavy lifting (yt-dlp --flat-playlist fetching 3000+ titles,
 JSONL transcript walk, individual ytsearch per video,
 date-based fallback) happened entirely silently.
 Added two helpers inside _run_metadata_download:
 * _update_meta_prep_line(text) — in-place scanline update that
 reuses the existing "scanline" tag (so the later "Scanning
 metadata files X/Y" line naturally takes over). 5-second
 grace period: the first call within 5 seconds of function
 entry is a no-op. This means fast channels (<5s total prep)
 see zero extra output — behavior is unchanged there.
 * _clear_meta_prep_line() — clears the scanline before early
 returns (pre-check "all metadata covered" shortcut; no-rows
 shortcut after Pass 6).
 Instrumented phase boundaries:
 * Pre-check: "Checking existing metadata (N video(s))..."
 * Pass 3: "Scanning transcripts for N unresolved ID(s)..."
 * Pass 4: "Fetching channel playlist for N video(s)..."
 * Pass 5: "Searching YouTube for N video(s)..."
 * Pass 6: "Date-resolving N video(s) — fetching channel dates..."
 Live counters inside the two yt-dlp stdout loops (Pass 4 and
 Pass 6): each iteration computes elapsed time and calls
 _update_meta_prep_line once per second with the current title/
 entry count, so the user sees a growing number every second
 instead of a motionless line. The existing per-500-title dim
 log calls are left intact for verbose mode.
 Simple mode only — verbose mode is completely untouched. The
 "scanline" tag is the same one "Scanning metadata files X/Y"
 already uses, so when the scanning phase starts it seamlessly
 replaces whatever prep message was last shown.
 Visual example for a big channel:
 Metadata: preparing David Pakman...
 Fetching channel playlist... 2,340 titles (47s) ← live
 Date-resolving... 1,890 entries (21s) ← live
 Scanning metadata files 145/200 ← existing
 — Scanned 200 metadata files.
```

#### v37.9 — Port FixDates' thefuzz-based fuzzy matching into metadata resolver + bulk-queue log spam fix

```
- METADATA RESOLVER — thefuzz.token_sort_ratio fuzzy matching
 Videos with heavy emoji titles (🛠️⚙️💻 etc.) were breaking the
 custom _norm_title / word-overlap matching in the metadata
 pipeline. The old FixDates v5.py Re-Dater tool handles these
 perfectly via thefuzz.token_sort_ratio — it matched all 7 of
 Branch Education's emoji-titled videos at 100% in a dry run.
 Ported that exact algorithm into _run_metadata_download as a new
 "pass 6a" that runs AFTER _yt_by_date is populated (from
 batch-resolve or pass 6's own fetch) and BEFORE the existing
 date-based match loop.
 * Flatten _yt_by_date into a single list (excluding resolved IDs)
 * Score each unresolved local file's base filename against every
 YT title using fuzz.token_sort_ratio (score ≥ 85 threshold)
 * Sort potential matches by score descending
 * Greedy-assign each file to its best unused YT video
 Resolved rows get search_failed_ts and date_resolve_failed_ts
 cleared and are stripped from _need_search before the date-based
 loop runs.
 Log message: " — Fuzzy-matched N video ID(s) via thefuzz."
 (pink em-dash, visible in simple mode).
 thefuzz import is wrapped in try/except so the app degrades
 gracefully if the package is missing. thefuzz, thefuzz.fuzz, and
 rapidfuzz added to YTArchiver.spec's hiddenimports so pyinstaller
 bundles them into the exe.

- BULK METADATA QUEUE — single summary line instead of spam
 When queuing metadata download for all channels via the Browse
 tab's bulk action, the log was spamming one "Added metadata
 download to sync-tasks queue" line per channel (102 identical
 lines for a full subscription list). Added a _quiet=False kwarg
 to _add_to_metadata_queue — the bulk caller now passes _quiet=True
 so per-channel lines are suppressed and emits a single summary:
 " — Added metadata download to queue for 102 channel(s)"
 Single-channel callers still get their per-channel line as before.
```

#### v37.8 — Date-based metadata resolution now runs on search-failed rows

```
- Videos with heavy emoji titles (e.g. 🛠️⚙️💻) break YouTube's search
 ranking, so the title-based individual search fails on them and
 marks them search_failed_ts. The date-based resolver (pass 6) was
 supposed to rescue these via file mtime → channel upload-date
 matching, but two bugs prevented it from running in practice:

 1. Pre-check was subtracting _pc_search_failed from the effective
 total, letting the "all videos already have metadata" early-exit
 bail before the pipeline ran. Result on Branch Education: 7
 videos with no video_id and no metadata, log saying "all 41
 videos already have metadata".
 2. Pass 5's _already_searched_ids filter mutated _need_search in
 place. Pass 6 iterates _need_search, so it never saw the
 search-failed rows even when the early-exit DID fall through.

- Fixes:
 * New date_resolve_failed_ts column tracks when date-based has
 actually been attempted and failed (ALTER TABLE migration).
 * Pre-check now subtracts date_resolve_failed_ts rows instead of
 search_failed_ts — rows are only "permanently unreachable" once
 every method has been exhausted.
 * _need_search_full captured before pass 5's filter so pass 6 can
 see the complete unresolved list including search-failed rows.
 * Pass 6 builds its own candidate list and reassigns _need_search
 so existing downstream code works unchanged.
 * Pass 6 success path clears both search_failed_ts AND
 date_resolve_failed_ts on resolve.
 * Pass 6 failure path sets date_resolve_failed_ts so future runs
 fast-path through the pre-check.
 * refresh=True also clears date_resolve_failed_ts.
 * Pre-check log message is now honest: reports
 "X videos have metadata. Y unmatchable (all resolution methods
 exhausted — use Refresh Metadata to retry)" instead of the
 previous lie "all N videos already have metadata".
```

#### v37.7 — Grid view context menu: dim entries use state="normal" instead of "disabled"

```
- Items like "Redownload..." and "Open Video — YouTube" in the grid
 thumbnail right-click menu were rendered with state="disabled" when
 their action wasn't available. On Windows, tk.Menu's state="disabled"
 uses the OS's native disabled rendering which COMPLETELY IGNORES our
 disabledforeground setting — that's why it looked wrong no matter
 what color was set in the previous attempt (v37.6).
- The subs-tab channel context menu avoids this by using state="normal"
 with foreground=C_DIM and a no-op command, which Tk fully respects.
- Added an _add_dim(label) helper inside _grid_on_rightclick that adds
 a no-op entry, then entryconfig(idx, foreground=C_DIM,
 activeforeground=C_DIM) so hover doesn't brighten the dimmed text
 back up. Replaced all three state="disabled" branches with
 _add_dim() calls.
```

#### v37.6 — Grid view context menu style matches subs tab menu

```
- The right-click menu on Browse grid thumbnails was using a bright blue
 accent hover (#4a9eff) with white text and a 1px border outline, which
 looked jarring next to the subs tab channel context menu's muted
 dark-grey hover. Copied the subs-tab menu's style verbatim:
 * activebackground: #4a9eff → #2e3035 (subtle dark grey)
 * activeforeground: "white" → _TP_FG (same as normal — no jump on hover)
 * bd: 1 → 0 (no border outline)
```

#### v37.5 — Visual identity overhaul, sync ordering fix, DB dedupe, transcribe simplification

```
── VISUAL IDENTITY OVERHAUL ────────────────────────────────────────────────
- Each task type now has its own color throughout the log + activity history.
 * Redownload: chartreuse #c7e64f (was sharing green with Manual/Auto sync)
 * Compress: purple #c084fc (was uncolored)
 * Reorg: orange #ff8c42 (was uncolored)
- Brackets [N/M], em-dashes, and label keywords (Transcribing:, Metadata:,
 Refreshing:) are now color-coded per task — not just em-dashes.
- Pinned live "active" status line now fires for transcribe / metadata /
 compress / reorg (was only sync + redownload before).
- Activity log row colors fixed: ReDwnl is now chartreuse instead of
 duplicate green, Cmprss is now purple, Reorg has orange handling.

── NEW FEATURES ────────────────────────────────────────────────────────────
- AUTO CHECKBOX on Sync Tasks popup — mirror of GPU's Auto checkbox. When
 enabled, the next queued sync-pipeline item auto-starts when the current
 one finishes (e.g. auto-metadata after a sync). Stored as autorun_sync.
- SAMPLE-AND-CONFIRM popup for redownload — after the first 10 successful
 replacements, shows a dialog with the average size delta and three
 buttons: Continue / Change Resolution / Cancel. Skipped on resume or
 for batches ≤ 10.
- UPDATE TRANSCRIPTION INDEX popup BATCHED — was firing per-channel during
 a queued batch transcribe. Now fires once at the end of the queue with
 a combined message ("Transcriptions for 5 channels are complete").
- SMALL-BATCH TRANSCRIBE OPTIMIZATION — for ≤10 new files on a channel,
 skips the slow yt-dlp --flat-playlist enumeration (5–10 min on big
 channels) and uses per-file YouTube search instead (~30 s for 10 files).

── BUG FIXES ───────────────────────────────────────────────────────────────
- DB SLASH-MISMATCH DEDUPE: _scan_channel_disk_info was inserting
 un-normalized filepaths. With output_dir set with forward slashes,
 os.path.join produced mixed-slash paths that bypassed the
 UNIQUE COLLATE NOCASE constraint. Found and merged 6190 duplicate file
 pairs in the DB on startup; the scanner now normalizes via
 os.path.normpath + NFC. One-time _dedupe_slash_mismatch_videos pass
 runs on _ensure_videos_populated, gated by a sentinel row.
- SYNC TASKS ORDERING: _single_worker kept _sync_running=True while
 calling _process_next_queued, which silently re-queued any metadata
 task instead of dispatching it. Plus _process_next_queued had no
 handler for "video" source so video items in _queue_order were
 skipped entirely and the legacy _process_video_dl_queue() fallback
 fired out of order. Both fixed; the popup order now matches execution.
- AUTO-METADATA-AFTER-SYNC wasn't actually firing without a manual click.
 Same root cause as the ordering bug above (_start_metadata_task seeing
 _sync_running=True).
- COLDFUSION [5/1] COUNTER: pre-count was skipping _fetch_failed_ids but
 the actual fetch loop wasn't, so the counter showed [5/1] instead of
 [5/5] and previously-failed videos got re-fetched on every run. Fetch
 loop now mirrors the pre-count's skip logic.
- AI-DRIVR DUPLICATE: _TEMP_COMPRESS files (compression intermediates)
 were being indexed as videos. Both _scan_channel_disk_info and the
 Browse panel scanner now exclude _TEMP_COMPRESS and _BACKLOG_TEMP.
- WHISPER PROGRESS vs ACTIVE LINE ORDERING: the pinned "Transcribing:
 Channel···" line was getting pushed above the whisper progress line on
 every animation tick. log_simple_status rewritten so simplestatus is
 always re-inserted at tk.END after restoring whisper progress.
- PINNED STATUS LINE DEAD BOTTOM: was only starting AFTER the matching
 phase, leaving the bottom dead during the (slow) scanning/YT-fetch
 phases. Now starts immediately when a transcribe job begins, showing
 "Transcribing: Channel···" until counts are known, then
 "[N/M] Transcribing: Channel···".
- [Trnscr] activity row was showing done_count + _prior_done (cumulative
 across the channel's lifetime). Now shows just this run's count.
- EM-DASH COLORS in colored task lines: _segmented_insert was bailing
 out when a _bk_tag was set but no [N/M] was in the line, leaving
 em-dashes uncolored. Fixed; "Transcribing:" / "Metadata:" /
 "Refreshing:" keywords also now get the matching color.

── SIMPLE-MODE LOGGING SLIMDOWN ────────────────────────────────────────────
- TRANSCRIBE: removed === borders + the entire TRANSCRIPTION SUMMARY
 block. Replaced with a single line:
 " — ✓ Channel transcribed. Completed N - Errors E - Took T"
 (blue em-dash, green ✓, green count). The verbose phase messages
 (Scanning local → Found N → Fetching YT → matched) collapse into one
 updating "scanline" that disappears once the matched-count is known.
- METADATA: pink em-dash format throughout.
 Old: "Metadata: Channel — N to fetch (M already done)."
 New: " — N to fetch (M already done)."
 Per-item lines also get the pink em-dash prefix. Completion line:
 " — Metadata for Channel download complete. N New"
- [Metdta] activity row: "skipped" column → "existing" column. The 28k
 "skipped" number was reading like an error; "existing" is neutral
 and the row now renders without the alarming amber highlight.
- WHISPER SETUP: suppressed three redundant lines in simple mode:
 "N video(s) need Whisper AI transcription", "Using Whisper model: X",
 "Selected Whisper model: X". Kept the "✓ Whisper model loaded".
- PER-ITEM CAPTION FETCH: dropped the leading "Transcribing" word:
 "[1/1] \"Title\" - fetching captions..." (was "[1/1] Transcribing \"Title\" - ...")
 Lowercased the active pinned label TRANSCRIBING: → Transcribing:.

── CMPRSS ROW ALIGNMENT ────────────────────────────────────────────────────
- The "Batch X" segment was inserted between channel name and stats,
 breaking column alignment with the other rows. Removed (the batch
 number was meaningless for single-video auto-compress runs anyway).
- Added missing "skipped" column. _compress_channel already tracks files
 that ffprobe says are already compressed; just needed to thread the
 count into _record_compression.

── PINK EM-DASH for "Added metadata download to sync-tasks queue" ──────────
- Was: "=== Added to Sync List: Download X Metadata ===" (header style).
- Now: " — Added metadata download to sync-tasks queue" (pink em-dash).
```

---
#### v37.4 — Fix duplicate year nodes in Browse tree (UI race condition)

```
- Browse tree was occasionally showing duplicate year entries under a
 channel (e.g. ColdFusion → 2010, 2011, ..., 2026, 2010, 2011, ...).
 Verified via direct read-only DB query that the underlying data is
 clean integer years — this was a UI-level race, not a data bug.
- Root cause: when the user collapsed and re-expanded a channel while
 its background year query thread was still running, _on_browse_open
 saw the "Loading..." placeholder (tagged the same as the initial
 dummy) and spawned a second query thread. Both threads eventually
 called _apply and each inserted a full set of year nodes.
- Fix: added _browse_loading_iids tracking set. Expand handlers bail
 early if the iid is already loading; async _apply handlers discard
 from the set in a finally block. Also added a "node already has
 non-placeholder children, skip insert" defensive guard inside each
 _apply as secondary protection against any remaining races.
```

#### v37.3 — Persist metadata-fetch failures

```
- Videos where yt-dlp --dump-json fails (deleted/private/region-locked/
 age-restricted) were getting re-fetched on every single metadata run
 because nothing persisted the failure. Now marked with a new
 metadata_fetch_failed_ts column and skipped on subsequent runs until
 the user explicitly hits refresh.
- Example: a channel reporting "506 processed, 502 already had metadata,
 4 failed" on run 1 will report "all 506 videos already have metadata"
 on run 2 and exit the pre-check immediately.
- Refresh clears all three failure columns together (search_failed_ts,
 id_resolve_failed_ts, metadata_fetch_failed_ts) for a true clean slate.
```

#### v37.2 — Simple-mode metadata scan visual polish

```
- Metadata scan progress no longer spams "Scanning metadata files... N/M"
 on a new line for every 10% step in simple log mode. Now it uses an
 in-place updating single line during the scan.
- When the scan completes, the transient line is cleared and replaced
 with a persistent "— Scanned N metadata files." summary (pink em-dash
 prefix, matching other metadata lines).
- Suppressed the "Grouping N video(s) by folder..." line in simple mode.
- Verbose mode keeps both the original per-10% progress and the grouping
 line for users who want detailed output.
```

#### v37.1 — Hotfix: date-resolve tuple unpacking

```
- The v37.0 metadata pipeline refactor changed _need_search tuples to
 5-tuples (added row_id as first element), but missed updating a
 generator expression at the date-resolve prefetch check that still
 unpacked 4 values. Metadata downloads crashed with "too many values
 to unpack (expected 4)" whenever rows remained unresolved after the
 individual search phase.
- One-line fix; all v37.0 improvements still in place.
```

#### v37.0 — Blink-driven tray + metadata scan optimization + emoji title fix

```
- Tray icon spin and tooltip now derive from the sync/GPU blink state
 instead of scattered _tray_start_spin / _tray_stop_spin calls in every
 task handler. Fixes "YT Archiver — Idle" showing up in the taskbar
 tooltip while sync tasks are running. Reorg tasks now also set a
 proper tooltip (they were the only task type that didn't).
- Metadata ID resolution refactored into a 3-pass staged pipeline:
 Pass 1 (free): existing video_ids + filename pattern match
 Pass 2 (cheap): segments table + videos table SQL lookups
 Pass 3 (expensive): JSONL folder walk — last resort only
 For large channels, this means metadata runs no longer walk every
 transcript JSONL file just to resolve 1-2 missing video_ids.
- New id_resolve_failed_ts column marks rows whose ID couldn't be found
 after a full JSONL walk. Subsequent runs skip these orphans so a
 single permanent orphan row no longer triggers a full folder scan
 every metadata run. Cleared on manual refresh.
- Fixed emoji/unicode video titles causing individual yt-dlp search to
 re-fire every single run. The search_failed_ts mark UPDATE was
 matching by filepath, but unicode normalization drift between
 yt-dlp stdout and os.walk output meant the UPDATE silently matched
 0 rows and the fail mark never stuck. All metadata-pipeline UPDATEs
 now target rows by primary key id instead — bulletproof.
- register_video and _scan_archive now NFC-normalize filepaths on
 insert so new downloads are always stored in canonical form,
 preventing duplicate rows from unicode differences going forward.
```

---
#### v36.9 — Fix auto-transcribe not queuing to GPU Tasks list

```
- Auto-transcribe was running Phase A inline during sync, so it never
 appeared in the GPU Tasks list and couldn't be paused independently
- Now queues the full transcription as a proper GPU task, giving the
 user pause/cancel control and making the GPU Tasks button blink
```

---
#### v36.4 — Restyle grid sort dropdown + fix label order

```
- Replace ugly tk.OptionMenu with button+popup menu matching the
 Actions dropdown style
- Fix "Sort:" / dropdown order so it reads left to right
```

#### v36.3 — Revert browse tree preload (caused duplicate year nodes)

```
- Reverted the preload-on-click feature from v36.2 — the async
 populate method raced with <<TreeviewSelect>> events and created
 duplicate year nodes in the tree
```

#### v36.2 — Skip previously-searched metadata videos + fix disabled menu color

```
- Videos that individual yt-dlp search couldn't resolve (deleted/private)
 are now remembered via search_failed_ts — skipped instantly on future
 runs instead of re-searching ~8s each (fixes the recurring 597 loop)
- Pre-check early exit accounts for search-failed videos so the pipeline
 doesn't re-enter unnecessarily
- Refresh mode clears search_failed_ts to allow re-trying everything
- Fix disabled menu text color in browse right-click menus (use _TP_DIM
 instead of hardcoded #666b75)
- Preload browse tree children on channel click without visually expanding
```

#### v36.8 — Fix metadata progress logs + task menu height

```
- Progress logs were using whisper_progress tag which silently fails —
 changed to simpleline so "Scanning metadata files..." actually shows
- Task menu +1 line height to prevent last item clipping
```

#### v36.6 — Task menu cutoff fix + metadata progress logging

```
- Fix task menu last item text being cut off (bottom padding on both
 Sync Tasks and GPU Tasks popups)
- Add progress logging during silent metadata phases: "Scanning metadata
 files... X/Y" during pre-count, "Grouping N videos..." during grouping
- Fix Unicode slash in date-based resolve normalizer (same ⧸ bug)
```

#### v36.5 — Fix 597 David Pakman videos re-searching every restart

```
- Root cause: all 597 had Unicode big solidus (⧸, U+29F8) in titles —
 a filesystem-safe replacement for /. Batch resolve stripped / but NOT ⧸,
 so file titles never matched YouTube titles. Search sent ⧸ to YouTube
 which returned nothing. Every restart repeated the same 597 searches.
- Added U+29F8 and U+FF0F to title normalization strip regex
- Individual search replaces ⧸ back to / before querying YouTube
```

#### v36.1 — Persist metadata video IDs across restarts + disk cache update

```
- Commit resolved video IDs frequently during metadata download so
 progress survives program restarts (was only committing at the end,
 so restarting lost all ~600 search results and repeated the cycle)
- Batch resolve: commit immediately after matching
- Individual search: commit every 10 successful finds + at end
- Update disk cache after metadata download so channel list shows
 "Done" instead of "Auto" without needing a restart
```

#### v36.0 — Pre-expand browse tree when grid loads

```
- Clicking a channel now pre-expands the tree hierarchy in the background
 while the thumbnail grid loads. Clicking a video opens instantly — no
 more "Loading..." delay requiring a second click.
```

#### v35.9 — Log completion after re-transcription

```
- Clear stale whisper_progress line from log on retranscribe completion
- Log green "Re-transcription complete" or red failure message
- Previously log stayed stuck on "Re-indexing..." forever
```

#### v35.8 — Fix mini log sync: wrong colors, stale content, missing tags

```
- Full audit found 9 discrepancies between main log and mini logs
- Sync fast-path now compares actual content, not just end index —
 fixes progress updates not reaching mini logs
- Created shared _MINI_LOG_TAGS list used by all 3 mini logs
- Browse mini log had wrong blue (#7eb8da vs #6cb4ee) on 11 tags,
 wrong summary (#999 vs #f5a623), wrong simplestatus, wrong update colors
- Added missing tags: dim, scanline, dlprogress
- Fixed simplestatus_green bold mismatch
- update_sep/update_head added to _ALL_LOG_TAGS for sync detection
- Retranscribe bypasses broken whisper_progress handler, updates log directly
```

#### v35.6 — Fix log progress stuck at 0% during retranscribe

```
- _whisper_transcribe's whisper_progress log calls were silently failing
 (complex 90-line tag handler), then deleting status handler's updates
- Suppress _whisper_transcribe log when progress_cb is provided (retranscribe)
- _browse_retranscribe_status now directly updates log_box on main thread
 with simple delete+insert, bypassing the broken handler entirely
```

#### v35.4 — Stale transcript after closing player

```
- Closing the player after retranscription now refreshes the browse
 viewer so it shows the updated transcript immediately
```

#### v35.3 — Show retranscribe progress in transcript notice area

```
- Moved progress back to transcript area (above body text) instead of
 title bar — title stays as video title during transcription
- Notice text is replaced and all word char offsets are adjusted by the
 delta so highlights and click-to-seek stay accurate during transcription
```

#### v35.2 — Fix retranscribe status breaking word mapping + click guard

```
- Retranscribe progress was inserting text at "1.0" in the transcript
 widget, shifting all char offsets — highlights and clicks pointed to
 wrong positions during transcription. Now uses the title label.
- New _player_reload_transcript() rebuilds transcript text and word
 mappings without restarting VLC — video keeps playing uninterrupted
- Click handler now ignores clicks in the notice area (before first
 word) instead of seeking to 0:00 via nearest-word fallback
```

#### v35.1 — Fix transcript click/highlight cascade failure on compound tokens

```
- Root cause: trailing punctuation set included hyphen, so matching "5"
 in "5-10%" consumed the "-" and advanced the cursor past it. The next
 token "-10" couldn't match at its correct position, matched at a
 duplicate hundreds of words later — misaligning everything after it.
- Remove hyphen from trailing punctuation advancement set
- Relax left boundary check for hyphen-prefixed Whisper tokens
 (e.g. "-selling", "-on", "-10" from compound word splits)
- Add 500-char search distance guard to prevent cascade failures
- Clean up dead code (_player_segs, _player_prev_tag)
```

#### v35.0 — Player transcript improvements and highlight fix

```
- Changed YT captions notice to "Youtube Auto Captions — re-transcribe
 with Whisper for improved results"
- Show live "Transcribing - X%" progress in player transcript area
 during re-transcription
- Auto-reload player after re-transcription completes (preserves
 playback position, no manual close/reopen needed)
- Fix word highlight matching for Whisper split decimal tokens
 (e.g. "6.3" tokenized as "6" + ".3" — leading dot prevented match)
```

---
#### v34.9 — Queue Sync Subbed when metadata is running + dim style fixes

```
- start_sync_all() didn't check _metadata_running, so Sync Subbed fired
 concurrently with metadata download instead of queueing. Added guard.
- "Already in Sync List" and "No Videos — Sync First" context menu items
 now use the same dim style as the org folder items (foreground=C_DIM)
 instead of tkinter's native disabled grey.
```

#### v34.8 — Queue sync when metadata download is running

```
- Four pipeline guards checked sync/reorg/redownload but not metadata,
 allowing sync to fire concurrently with metadata download instead of
 queueing. Fixed in context menu sync, menu label, sync_single_channel,
 and tab-switch button refresh.
```

#### v34.7 — Fix startup freeze from slow queries + heavy I/O

```
- _scan_channel_disk_info was reading every Transcript.txt line at startup
 for all 100 channels (millions of lines of I/O). Reverted to fast
 directory-level check; Browse tab scanner handles per-video accuracy.
- Replaced COUNT(*) FROM (SELECT DISTINCT channel, title) subquery
 (227 seconds on 8M rows) with COUNT(DISTINCT title) using the existing
 title index (6 seconds). 99.6% accurate, no more DB lock starvation.
```

#### v34.6 — Exclude no-speech videos from un-indexed warning

```
- Warning checks now only count videos with tx_status='transcribed' when
 comparing against the segments table. Videos with no speech or no captions
 have nothing to index and no longer trigger the "rebuild index" warning.
```

#### v34.5 — Fix index stats undercounting videos without video_id

```
- All video count queries used COUNT(DISTINCT video_id) which excluded
 ~6,300 videos with empty video_id — showed 86,377 instead of 92,698
 and triggered a false "6,505 un-indexed" warning. Fixed in all 4
 locations to count unique (channel, title) pairs instead.
```

#### v34.2 — Fix metadata playlist timeout for large channels

```
- Batch playlist fetch timeout was based on videos needing ID resolution
 instead of total channel size — for David Pakman (598 needing search
 out of 28,000 total), timeout was 5 min when the full playlist needs
 10+ min. Now scales with total channel video count, up to 30 min max.
 Once resolved, IDs persist in the DB so subsequent restarts skip the
 playlist fetch entirely.
```

#### v34.1 — Fix JSONL backfill dead loop + metadata restart redundancy

```
- JSONL backfill was stuck in a dead loop: when all videos were already
 transcribed but missing searchable .jsonl entries, the early return
 prevented the backfill from ever running. The log message "will generate
 next transcription run" was a lie — next run hit the same early return.
 Now continues to YouTube fetch + backfill when entries are needed. This
 should close the ~10k gap between transcribed videos and indexed videos.
- Metadata restart: video IDs resolved via batch playlist were saved to the
 DB but never read back on restart — the resolver only checked segments
 table, JSONL files, and filenames. Added videos table lookup so resolved
 IDs persist across restarts, eliminating the 500-700 video playlist
 timeout + individual search on every app launch.
```

#### v34.0 — Fix per-video transcription status detection

```
- tx_status was falsely marking ~4,000+ videos as "transcribed" based on
 directory-level presence of any transcript file — if a folder had even one
 transcript, every video in it was marked done. Now reads each Transcript.txt
 and checks per-video titles against the actual transcript entries
- Reverse reconciliation: on scan, videos previously marked "transcribed" that
 have no transcript entry are corrected back to "pending" and the channel's
 transcription_pending counter is updated so re-transcription catches them
- Fixed in all three code paths: Subs tab scan, Browse tab scanner +
 reconciliation, and post-transcription status update (prompt_reindex)
```

#### v33.9 — Whisper fallback model on crash, fix title % coloring, internet-pause metadata

```
- When Whisper process crashes on a file, automatically retries with large-v3
 (or medium) as a fallback model before skipping — fixes rare CTranslate2
 model-specific crashes where one model hard-crashes on audio that another
 handles fine
- Fix transcription progress percentage coloring: video titles containing "%"
 (e.g. "38% of Americans...") no longer steal the green color from the actual
 transcription progress — now always colors the last percentage in the line
- Add internet-down check to per-video metadata fetch loop — metadata
 operations now properly pause when internet is lost instead of continuing
 to fire requests
```

---
#### v33.8 — Early-exit for complete channels, date-resolve timestamp fallback

```
- Re-running metadata download on an already-complete channel now exits
 near-instantly — pre-reads JSONL metadata before the search pipeline
 and skips batch-resolve/individual search/date-resolve entirely when
 all videos are already covered
- Fix date-based elimination not capturing upload dates: yt-dlp's
 flat-playlist mode often returns upload_date as "NA", now falls back
 to converting the Unix timestamp field to a date
- Added diagnostic log showing date capture rate during batch-resolve
 (e.g. "487/500 with dates") for visibility into date-resolve coverage
```

#### v33.7 — Date-based elimination fallback for missing metadata

```
- New final-stage ID resolver: when batch title matching and individual
 YouTube search both fail, groups unresolved videos by upload date (file
 mtime) and cross-references against the channel's YouTube uploads for
 that day
- Single-candidate dates get a direct match; multi-candidate dates attempt
 fuzzy title matching but skip anything ambiguous (prefers missing data
 over incorrect data)
- Checks +/- 1 day to handle timezone discrepancies between file dates
 and YouTube upload dates
- Existing batch-resolve now also captures upload dates from the same
 yt-dlp call (no extra network cost), feeding the date resolver
 automatically
- Re-queuing metadata download for a channel will now pick up previously
 unresolvable videos via this new fallback
```

#### v33.6 — Batch-resolve video IDs for metadata downloads

```
- Metadata downloads for channels where videos lack [VIDEO_ID] in filenames
 now fetch the entire channel playlist in one bulk call (~2-5 min) instead
 of searching YouTube one video at a time (~8s each, days for large channels)
- Matches local video titles against YouTube titles using normalized comparison
 (NFKC unicode, strip unsafe chars, collapse whitespace, case-insensitive)
- Falls back to individual ytsearch1: only for titles that don't match the
 bulk playlist — typically a small handful instead of thousands
```

---
#### v33.5 — Fix search dates for year-only folders, responsive graph toolbar

```
- Fix Browse > Search dates showing just "2021" instead of "2021-05" for
 channels with year folders but no month subfolders — now derives month
 from companion video file mtime (which is set to YouTube upload date)
- Fix Browse > Graph toolbar clipping at narrow window widths — controls
 now dynamically reflow into two rows when space is tight, single row
 when wide enough
```

#### v33.4 — Fix temp file cleanup regex and queue drag-reorder

```
- Fix _PARTIAL_FRAG_RE and _is_partial_file to catch yt-dlp fragment files
 with dash-suffixed format codes (e.g. .f140-7.m4a) that slipped through
- Fix all 6 queue processors to respect drag-to-reorder: pass preferred key
 from _queue_order so each processor pops the correct item, not always first
```

#### v33.3 — Log fixes, scroll lock, DB performance, metadata queue persistence

```
Log:
- Fix 'no speech detected' lines missing blue brackets
- Fix auto-scroll snapping back when user scrolls up slightly (tighten
 threshold so only scrolling to the very bottom re-engages lock)
- Fix '...' in video titles being incorrectly colored blue (only color
 actual truncation ellipsis, not dots in the original title)
Database:
- Skip metadata JSONL files during DB index builds (always 0 segments)
- Add missing indexes on jsonl_path and video_id for large-scale performance
- Optimize full rebuild: drop+recreate tables instead of row-by-row DELETE
- Fix metadata downloads failing when DB still initializing on startup
Queue:
- Fix metadata queue losing the running channel on pause+restart
 (channel was popped before processing and never saved back to disk)
```

---
#### v33.1 — 3 targeted fixes from deep audit pass 3

```
- Fix _scan_archive _scan_running flag never reset on early return,
 permanently disabling the Scan Archive button for the session
- Fix metadata JSON parsing failing silently when yt-dlp emits non-JSON
 warnings/notices before the JSON dump
- Fix frequency chart click-to-search producing comma-separated FTS query
 that never matches (changed to space-separated)
```

---
#### v33.0 — Deep audit pass 2: 55 fixes across reliability, threading, performance

```
Critical:
- Fix _show_in_explorer name collision crashing right-click on Recent tab
- Eliminate Whisper model race condition during concurrent re-transcription
- Atomic punctuation sweep writes (prevent transcript data loss on crash)
- Fix register_video TOCTOU race with atomic UPSERT
High:
- Don't hold config_lock during filesystem checks on disconnected drives
- Cache _fetch_video_title (was blocking yt-dlp parser 3s per call)
- Cap _walk_cache at 5 entries to prevent unbounded memory growth
- Protect _gpu_current_item writes with proper lock
- Add _local_archived_set.add() for members-only/private/scheduled videos
- Gate start_download on transcribe/metadata running state
- Add internet outage blocking to metadata search loop
- Fix frequency chart data race between background thread and main thread
- Add _scan_archive concurrency guard
Medium/Low:
- O(log N) prefix matching in compress/redownload (was O(N) linear scan)
- Pre-filter _scan_existing_transcripts (skip non-header lines before regex)
- Fix _merge_anim ghost callbacks on rapid merges
- Move bulk transcript deletion to background thread (prevent UI freeze)
- Cap browse find highlights at 500 matches
- PhotoImage cleanup on grid cache eviction (prevent memory leak)
- Manual folder transcription now recurses into subdirectories
- Fix metadata complete log lines stuck at bottom when GPU paused
- 30+ additional threading, edge case, and cleanup fixes
```

#### v32.9 — Deep audit: 36 fixes across bugs, threading, performance, reliability

```
Critical:
- Fix .part substring match that could delete real videos (e.g. "apartment_tour.mp4")
- Fix Whisper stale queue drain ordering that caused 15-minute transcription hangs
- Fix archive sanity check condition that never actually triggered
- Fix backfill permanently marking videos "no captions" on transient network errors
High:
- Add stdin redirect to prevent yt-dlp from hanging on interactive prompts
- Move save_config inside config_lock (7 locations) to prevent race conditions
- Add atexit handler to kill Whisper/punct/ffmpeg subprocesses on crash
- Move disk retry check to background thread (prevents UI freeze on disconnected drives)
- Fix scrollbar debounce cross-widget interference
- Remove per-video JSONL fsync (major HDD performance improvement for transcription)
- Track enum/retranscribe subprocesses in active_processes for clean shutdown
- Fix internet monitor duplicate thread race with proper locking
- Fix prefetch executor shutdown race with temp directory cleanup
- Add retry cap (5 attempts + backoff) to save_config thread spawning
Medium/Low:
- Atomic Whisper cache writes (write-to-tmp + os.replace)
- Dispatch browse thumbnail display to main thread (tkinter thread safety)
- Input validation for resolution, duration, compress batch fields
- Video ID regex now ASCII-only (was matching Unicode word chars)
- Don't persist empty disk cache during rescan (crash-safe rebuild)
- Release icon.ico file handle after loading
- Add fsync to disk cache saves for consistency
- Module-level dot animation constant, padding edge case fix
- Clean up redundant exception clauses
```

#### v32.8 — Browse tab title search, mini log bracket fix

```
- Add video title search bar in Browse tab header (next to "All Videos")
- Searches across all channels, results shown in grid with channel names
- Multi-word search: each word matched independently (AND logic)
- "Searching..." loading indicator while query runs
- Back button returns to search results after viewing a transcript
- Fix mini log bracket coloring: bracket tags now take priority over base tags
- Add yellow warning icon next to search bar when index is stale
```

#### v32.7 — Fix em-dash position and Searching line truncation

```
- "Skipped (ID already belongs to another video)" em-dash now appears before the message, not after
- Searching line title truncation now uses _trunc_pad_title to align with other log lines
```

#### v32.6 — Fix metadata searching em-dash color

```
- Bracket-prefix em-dash now context-aware: pink for metadata lines, blue for transcription lines
- Was incorrectly using blue for metadata searching lines
```

#### v32.5 — Fix log line coloring for Skipped, Punctuation, and Searching lines

```
- "Skipped (ID already belongs to another video)" now shows only a pink — instead of entire line pink
- "Punctuation model loaded" line now has a blue — prefix (matching Whisper format)
- Searching lines during metadata ID resolution now have a blue — prefix
- Bracket-prefix rendering updated to color em-dashes blue in pre-bracket text
```

#### v32.4 — Color-code metadata and transcription log lines

```
- Metadata lines with — now get pink em-dash coloring to denote metadata action
- [N/N] Searching: lines get pink bracket and label coloring
- Skipped (ID already belongs...) lines now pink
- Loading punctuation model line now blue (transcription action)
```

#### v32.3 — Fix metadata pause, search visibility in simple mode

```
- Add pause support to metadata YouTube-search loop (was unresponsive to pause during ID lookups)
- Make [X/Y] Searching and Skipped lines visible in simple mode (no more long silent gaps)
- Remove bumper space above === Metadata Complete === lines
```

#### v32.2 — Metadata thumbnail fixes, YouTube search for missing video IDs

```
- Search YouTube to find video IDs for videos without [VIDEO_ID] in filename
- Channel matching uses URL comparison (handles renamed channels like penguinz0)
- Normalize channel name comparison (handles "AI-DRIVR" vs "AI DRIVR")
- Deduplicate grid entries when multiple videos share the same video_id
- Fix metadata [X/Y] counter to only count videos that actually need fetching
- Add progress logging throughout metadata download (preparing, resolving, searching)
- Fix metadata choice dialog not appearing for split-year channels
- Add metadata choice dialog to Subs tab right-click menu
- Invalidate grid thumbnail cache after metadata download completes
- Fix browse mini-log header color to match main log
- Improve disabled menu item text visibility in right-click context menus
- Segments table fallback for resolving video IDs in grid display
```

#### v32.1 — Professional audit: 30 fixes across all 29,200 lines

```
- Config corruption now shows a warning dialog instead of silently resetting
- Fixed sync race condition in _start_queued (_sync_running gap)
- Auto-worker now supports Skip Current (was completely missing)
- Auto-worker only updates last_sync if work was actually done
- _gpu_actively_encoding flag guaranteed to reset via try/finally
- _fix_file_dates prefix matching optimized from O(N) to O(log N) via bisect
- _check_internet now closes response (was leaking sockets every 5 seconds)
- Queue state save now locks before deep-copying volatile dicts
- GPU queue deduplicates on load to prevent accumulation across crashes
- sanitize_folder now catches Windows reserved names (CON, PRN, AUX, etc.)
- Extracted _show_in_explorer helper (replaced 3 duplicated implementations)
- Extracted _format_bytes helper (replaced duplicated size formatting)
- Bookmark dialogs now have dark title bar and center on parent window
- VLC fallback returns UI to browse section before opening external player
- Folder transcription now shows pause/resume status like channel transcription
- JSONL read no longer unnecessarily unhides/re-hides files on Windows
- Warning log lines properly suppressed in Simple mode
- _VIDEO_EXTS shadowing in TranscriptionPanel renamed to _TP_VIDEO_EXTS
- Icon handles freed after SendMessageW, window dimensions clamped on restore
- Placeholder text reentrance guard, deepcopy in ctx menu edit, autorun_gpu lock
```

#### v32.0

```
- Add overlay tags (meta_bracket, trans_bracket, dl_white, sync_bracket, trans_dots)
 to mini-log sync so metadata and transcription lines render with correct colors
 on Subs/Recent/Browse tabs
- Keep browse grid loading overlay visible 1.8s longer so thumbnails load before
 grid is shown
```

#### v31.9 — Major browse grid overhaul: canvas-native rendering, thumbnail matching, navigation

```
- Rewrote grid to use canvas-native items instead of embedded frames (eliminates scroll tearing)
- Loading overlay with progress feedback during channel load
- Fixed duplicate videos caused by case-sensitive filepath UNIQUE constraint (COLLATE NOCASE)
- Title-based thumbnail fallback with Unicode NFKC normalization (fullwidth chars, trailing dots)
- JSONL video_id backfill for videos without [ID] in filename
- Clickable breadcrumb navigation (Channel / Year / Month)
- Back button to return from transcript view to grid
- Right-click context menu on grid cards (Play, YouTube, Explorer, Delete)
- Fixed grid click navigation for split_years/months channels (chained tree expansion)
- Scroll-to-load-more for channels with 500+ videos
- Thread-safe PhotoImage creation (moved to main thread)
- Delete file now removes video from grid view immediately
```

#### v31.8 — Fix channel-level sort (newest/oldest/most viewed)

```
- When viewing a channel at root level with split_years, metadata was only
 loaded from a nonexistent root JSONL file. Now merges metadata from all
 per-year/month JSONL files so upload_date and view_count are available
 for sorting. Also fetches mtimes for all dateless videos instead of just
 the first page.
```

---
#### v31.7

```
- Metadata lines now subtract prefix length from truncation width so "..." aligns
 at the same column as transcription lines
```

#### v31.6

```
- Fix title truncation alignment: all progress lines use same fixed width
- Removed dynamic _MAX_LINE_WIDTH approach that caused misalignment
```

#### v31.5

```
- Record [Trnscr] activity log entry when all videos already transcribed (0 count)
- Fix [Trnscr] tag to only color blue when transcribed count > 0
```

#### v31.4

```
- Skip YouTube playlist fetch when all videos already transcribed
- Backfill runs naturally next time there are new videos
```

#### v31.3

```
- Fix grid thumbnails not loading for split_years channels at root level
- Now scans all year/month .Thumbnails directories
```

#### v31.2

```
- Fix progress line title truncation alignment using dynamic prefix-based width
- Done-line suffix extends past truncation point instead of shrinking title
```

#### v31.1 — Gold Master Audit: 27 optimizations and bug fixes

```
Performance:
- Remove config_lock from _write_snapshot, skip save/restore when no progress tags
- O(n) string join instead of O(n^2) concat in _sort_transcript_entries
- Player poll slows to 500ms when paused, targeted word highlight removal
- SQLite WAL journal mode, cap VTT overlap detection at 20 words
- Replace busy-wait lock loop with single acquire(timeout=60)
Bug fixes:
- Companion file matching requires '.' after base name
- Fix save_config inside config_lock, drain stdout before proc.wait()
- Model dialog race condition guards, _whisper_starting protected by lock
- Search button reset on crash, bookmark refresh None guard, double .get() fix
UI polish:
- Tooltip screen-edge clamping, Escape key on missing channel dialog, dark title bar
Code cleanup:
- Removed dead code blocks, redundant imports, debounce wrapped in try/except
```

#### v31.0

```
- Page-by-page progress during large YouTube playlist fetches
- Scale playlist fetch timeout with channel size (5min base + 30s/1000 videos)
- Threaded stdout/stderr readers for responsive cancel/timeout
- Fix browse grid thumbnail resize: hide during drag, clean rebuild after release
- Fix blank space on right when narrowing browse grid
```

#### v30.9

```
- Grid resize no longer destroys/recreates cards — regrid in-place
- Thumbnails reuse existing photos during drag, reload at correct size after 400ms
- Debounced scrollregion, popup repositioning, autorun history refit
- Escape key closes edit channel UI from any widget on Subs tab
```

#### v30.8

```
- Fix explorer /select opening Documents folder (quote paths with spaces)
- Fix browse tab duplicate entries (dedup by normalized filepath + fuzzy title)
- Fix register_video creating dupes, DB cleanup for duplicate rows
- Browse tree preserves channel expansion state across refreshes
- Navigate-to-video uses async retry, metadata queue alphabetically ordered
- Log formatting fixes: metadata title width, transcription colors, scan symbols
```

#### v30.7

```
- Convert 10 traceback.print_exc() to log() with "dim" tag (verbose only)
- Catch sqlite3.OperationalError in _tp_open_db ALTER TABLE
- Merge double os.walk into single pass in index builder
- Fix _VIDEO_EXTS: list to tuple so str.endswith() works
```

#### v30.6

```
- Add _update_global_pause_btn_sync() at all 9 task-start locations
- Fix overrideredirect popup not resizing when content changes
```

#### v30.5 — Post-audit sweep: 6 fixes

```
- Add timeout=300 + kill fallback to all 5 yt-dlp proc.wait() calls
- Use os.walk filenames directly instead of redundant os.listdir
- Add jitter to caption backoff sleep
- Replace any(endswith) with str.endswith(tuple)
- Add encoding='utf-8' to check_directory_writable
```

#### v30.4 — Golden Master Audit: 24+ bug fixes

```
Critical/High: DB lock protection, whisper model race, session_totals locking,
download count atomicity, sync queue race, punctuation serialization,
reindex DB safety, prefix match false positives
Medium: edit channel URL lookup, tree node collapse, cancel event guard,
retry state thread-safety, GPU popup parent chain, grid nav retry
UI: metadata/transcription log alignment, blue dots, Enter/Escape channel edit
```

#### v30.3 — Golden Master Audit Batch 1: 20 fixes

```
CRIT: save_config inside config_lock; winfo_width off main thread
HIGH: disk cache key fix, session_totals lock, disk_error_lock, local_archived_set
MED: negative time clamp, geometry regex for negative monitors, trunc_pad guard
PERF: log line count throttle, word cloud stop-words, BICUBIC grid thumbs
SEC: strip FTS5 operators from search, followlinks=False on os.walk
UX: spacebar play/pause binding, last-sync 30-day display
```

#### v30.2 — Golden Patch #3: 36 fixes from full professional audit

```
- Redownload folder path, retranscribe flag, grid date parsing, double DB close
- Cancel event clearing, audio-only res-check crash, whisper model restore
- Forward reference stubs, deterministic seen-filter trim, video_id counts
- Cached disk size, O(n*m) queue check fix, bigram set hoisted out of loop
- Atomic JSONL replacement, browse viewer keyboard navigation
- GPU queue encode task label fix, metadata truncation alignment
```

---
#### v30.1 — Golden Audit #2: 17 fixes

```
Bugs: _redownload_hist_idx NameError, atomic write for transcript sorting,
proc.wait() double-timeout guard, json.loads guard on subprocess output,
os.path.getmtime guard, non-deepcopy channel list iteration
Correctness: consolidate 14 _VIDEO_EXTS definitions to single constant
Robustness: ASCII fast-path for _display_width, archive sanity check,
deep-copy volatile refs, compiled _FFMPEG_TIME_RE, re-hide metadata JSONL
UX: fix player transcript cursor, dark dialogs, disable non-functional
context menu items, log warning on VLC fallback failure
```

#### v29.9

```
- Fix metadata log color coding: only color brackets/label in pink, not whole line
```

#### v29.8

```
- Add _punct_lock protecting all punctuation subprocess access
- Move _tray_spin_active writes inside _tray_spin_lock
- Add pink color for metadata activity log and per-video log lines
- Reorder per-video metadata log format, align columns, fix centering
```

#### v29.7 — Global pause/resume/start button

```
- Unified button next to Sync Subbed for start/pause/resume of sync + GPU tasks
- Canvas-drawn play/pause icons with blue/green color states
```

#### v29.6

```
- Add Metadata column to subs list
- Add Download Metadata right-click option
- Fix browse DB after reorganize
```

#### v29.5

```
- Fix browse thumbnail persistence on deselect
- Show thumbnail without metadata
```

#### v29.4

```
- Replace per-row widget approach with single Text widget for Sync/GPU task popups
- Fix mousewheel scrolling, badge click-through, shutdown hang/HWND error
```

#### v29.3

```
- Add [Metdta] activity log entry for metadata download completion
- Add pause/resume log messages for metadata with tray spinner
- Fix sync tasks popup scroll
```

#### v29.2

```
- Metadata re-download dialog: Check for New / Refresh Counts / Cancel
- Refresh Counts mode re-fetches to update view/like/comment counts
- Right-click "All Videos" to queue metadata for all channels
- Fix cancel button confirmation dialog
```

---
#### v29.1

```
- Adaptive UI queue flush: 4ms when draining, 250ms when idle
- Mini-log sync: 250ms active, 1s idle
- Remove update_idletasks() from button handler and queue panels
- Grid photo cache: LRU eviction at 20 scopes
```

#### v29.0

```
- Auto-download metadata option in channel add/edit UI
- Prompt to enable auto-metadata after full download
- Sort options: Newest / Oldest / Most Viewed
- Grid cache per channel for instant restore
- Instant UI response on channel click with Loading state
- Generation counter prevents stale grid data
- Browse tree expand: DB queries moved to background threads
- Lazy mtime fetching: only stat files for current page
- Fix blank transcript and grid reload on collapse
- Metadata download with description, views, comments, thumbnails
- YouTube-style video grid view with responsive 1-4 column layout
- Pull-up metadata drawer (description, views, comments overlay)
- Fix transcript performance: single active_word tag instead of thousands
- YT caption accuracy notice with clickable re-transcribe link
- Loading overlay on Browse tab until disk scan completes
```

#### v28.0

```
- Browse tree respects channel folder organization settings (split_years/months)
- Channels with no folder org show flat video list
```

#### v27.9

```
- Build Browse tab UI eagerly after DB preload (no more loading flash)
- Fix sync tasks badge ghost count
```

#### v27.8

```
- Right-justify duration column in transcription log (fixed 10-char width)
```

#### v27.7

```
- Display-width-aware title alignment using East Asian Width
- Normalize smart quotes/dashes to ASCII for column counting
```

#### v27.6

```
- Fix transcription log alignment: title — (duration) — done with fixed-width pad
```

#### v27.5

```
- Align video duration right before the — done marker regardless of title length
```

#### v27.4

```
- Fix drag-to-reorder bugs in GPU/Sync queue popups
- Force full rebuild after drag to fix stale labels/closures

- Fix transcription done-line alignment in simple mode (separate duration from title)
```

#### v27.3

```
- Fix chunked transcription recursive splitting bug (chunks after first had wrong duration)
- Lower Whisper chunking threshold from 6hrs to 2hrs
```

---
#### v27.1

```
- Incremental treeview updates, popup hide/show caching, deferred tab refreshes
- Browse tab DB preload, queue flush/badge/save debouncing, SQLite batch commits
```

#### v27.0

```
- Fix chunked transcription progress cleanup (stale "Section 1/5, 100%")
- Add _clear_whisper_progress() after browse re-transcribe completes
```

#### v26.9

```
- Increased _MAX_TITLE_DISPLAY from 40 to 55 for wider log lines
- Captions path shows file duration and realtime ratio in done lines
```

#### v26.8

```
- Hide ffmpeg console window during chunk extraction
- Per-section progress lines indented under main video line
- Per-section completion summary with duration, elapsed, x realtime
- Pause/resume support between chunks
- Add "Follow" checkbox to embedded player for auto-scroll transcript
```

#### v26.7

```
- Aggressive process kill with psutil tree-kill on Whisper stalls
- 3s cooldown after kill, skip file on second stall
- Auto-chunk files over 6h into 2-hour segments with 30s overlap
- Offset timestamps and deduplicate overlap segments

- First-launch dependency setup dialog (detects missing deps, one-click install)
```

#### v26.6

```
- Thread Browse tab to eliminate UI freezes
- Background threads for DB queries, os.path.getmtime, .txt file reads
- Batch JSONL path lookup (single IN query instead of N queries)
- Generation counter prevents stale results
```

#### v26.5

```
- Clear sync progress file on app close (fixes companion display stale status)
```

#### v26.4

```
- Try without Firefox cookies first (faster), fall back on failure
- Pipeline prefetch: start fetching next video's captions while current processes
```

#### v26.3

```
- Browse tree auto-refreshes when video finishes downloading
- Fix Play Video failing when yt-dlp channel name differs from config name
```

---
#### v26.2

```
- Actions dropdown replacing Re-transcribe button (Re-transcribe + Re-download)
- Resolution picker dialog for single-video and folder-level redownloads
- Display video resolution via ffprobe next to title
- Play button enables instantly from DB filepath
```

#### v26.1

```
- Videos sorted by upload date (file mtime) instead of alphabetically
- Month separator lines in browse tree, fixed month parsing
- Schema bump forces DB rescan, auto-detect Python 3.11 path
```

#### v26.0

```
- Recent tab "Play Video" navigates to Browse tab with embedded player
- Show in Explorer and Delete File in Browse context menu
- Video ID extraction from yt-dlp filenames, backfill DB rows
- Channel name mismatch reconciliation, Scan Archive button fix
```

#### v25.9

```
- Fix matplotlib bundling in exe (pyinstaller Python version mismatch)
```

#### v25.8

```
- Sidebar header "LIBRARY", section header "All Videos"
- VLC scrubber no longer fights with poll loop while dragging
- Mini log mirroring added to Browse tab

- Rename 'Transcriptions Tab' to 'Browse Tab'
```

#### v25.7

```
- Browse tab shows all channels/videos regardless of transcription status
- New videos table tracks all videos in DB
- Play Video button in transcript viewer opens embedded VLC player
- Right-click folders for Transcribe/Re-transcribe options
- Responsive buttons collapse to icons when panel is narrow
- Scan Archive button for manual rescan

- Background internet monitor: pauses all ops on outage, auto-resumes on restore
- Playlist fetches detect if internet dropped mid-fetch and re-fetch from scratch
```

---
#### v25.6

```
- Atomic writes for transcript replacement and redownload progress
- JSONL append auto-repairs truncated lines from prior crashes
- Config saved before UI refresh on channel add/edit
- Whisper stall retries once automatically
- Sync worker safety net, tray spin guard fix, GPU Tasks error tracebacks
```

#### v25.5

```
- Delete transcriptions matches both "Transcript" and "Transcription" filenames
- Index build warns about orphan folders with .txt but no .jsonl
```

#### v25.4

```
- Fix re-transcribe hanging after 100% (status updates for punctuation/re-indexing)
- Timeout-based db_lock to avoid deadlock with background indexer
```

#### v25.3

```
- Fix re-transcribe JSONL write failing on hidden files (normalize path, chmod fallback)
```

#### v25.2

```
- Suppress transient corrupt JSONL warnings in simple mode (verbose only)

- Re-transcribe button sizing fix (padx instead of fixed width)
- Move _find_video_file check to background thread, add title index on segments table

- Fix re-transcribe: unhide .jsonl before writing, fix button resize

- Fix re-transcribe not working (variable name bug) + add progress % to button

- Fix re-transcribe button freezing app (dialog on main thread, not joined thread)
```

#### v25.0

```
- Add Re-transcribe button to Browse Transcriptions tab
- Downloads audio from YouTube if no local file, replaces old entry, re-indexes DB
```

#### v24.9

```
- Fix IndexError in VTT word-timestamp backtrack loop
- Fix JSONL backfill retrying for NO AUDIO DATA entries (writes placeholders)
- Fix punctuation sweep re-running on already-punctuated entries
- Fix missing null check after spawn_yt_dlp in redownload
- Per-item exception handling in all process kill loops
```

---
#### v24.8

```
- Suppress raw yt-dlp errors in simple mode (silently counted, shown in summary)
- Green brackets for Filtered channels
- Colored bracket prefixes for sync (green) and transcription (blue) log lines
- Harmonize autosync with manual sync: queue population, log text, notifications
- Auto-download yt-dlp and ffmpeg when missing, VLC download dialog
- Fix orphaned bracket characters in transcription done lines
- Disk scan logging: dim start, green completion
- Fix stale transcription checkmarks for cached channels
- Clear stale transcription_complete flag when transcripts missing from disk
- Handle scheduled livestreams as filtered, add filtered counts to summaries
- Fix --break-on-existing disabled after interrupted sync on large channels
- Fix 14 bugs: race conditions, cleanup leaks, extension mismatch, injection hardening
- Fix transcript timestamps, reprocessing loop, and tray icon
- Fix seek-to-timestamp (proportional char mapping), remove 2500+ duplicate JSONL entries
- Split oversized segments (>30s), fix Python falsy bug with empty lists
- Add fast-path for fully-synced channels in autosync (_quick_check_new_uploads)
- Fix stale-whisper false positives: add source field to JSONL entries
```

---
#### v24.7

```
- Fix VTT word timestamps and stale-whisper repeat processing loop
- Parse per-word timestamps from YouTube VTT <c> tags
- Fix JSONL re-processing loop: per-line error handling, segment splitting
```

#### v24.6

```
- Fix 6 bugs from deep code review of Sonnet patches
- Fix log clearing during autosync, bracket colors, queue display, whisper progress
- Auto-sync Sync Tasks shows current channel instead of empty
- Bracket coloring: segmented insertion instead of post-hoc tag manipulation
- Fix queue showing duplicate entries during batch sync
```

#### v24.5

```
- Fix stale Whisper re-transcription: Whisper fallback in backfill
- Purge stale Whisper JSONL entries before writing new ones
- Word-alignment browse position map instead of time-proportional guessing
- Stale Whisper detection via segment length + missing word timestamps
- Browse double-click word opens Local/YT video at timestamp
- Fix single-instance: silent focus instead of error, release mutex on close
- Fix Whisper timestamp/video_id so right-click open-to-timestamp works

- Transcription Parser (EARLY BETA) — graphing and parsing initial work
```

#### v24.4

```
- Fix frequency Plot/Clear button toggle (auto-revert on new text)
- Word cloud without search term (random segment sampling)
- Default search sort: newest to oldest
```

#### v24.3

```
- Fix tab styling visibility (dark mode backgrounds with visible borders)
- Lazy-load browse tree on expand instead of loading entire tree
- Async stats queries via background threads
- Database indexes on channel, year, month
```

#### v24.2

```
- Add word cloud chart type, inline find in browse viewer, search within viewer
```

#### v24.1

```
- Add Browse, Bookmarks, async search, copy/bookmark context menu, export frequency CSV
- Fix filtered count bug, tab borders, column resize, queue-all dialog
- Timestamp offset fix, normalize tooltip, multi-channel frequency, auto-plot

- Transcription Parser (EARLY BETA): initial graphing and parsing
```

---
#### v23.9

```
- Fix spinner during pause, retry errored videos on disk restore
```

---
#### v23.8

```
- Background temp cleanup on startup, app update checker
```

#### v23.7

```
- Green running status, redownload auto-resume, continue button fix, tray pause fix
```

#### v23.6

```
- Activity log for redownload jobs with live in-progress placeholder
```

#### v23.5

```
- Verbose mode expanded: active channel line, DLTRACK detail, Whisper queue depth,
 page enumeration, file move counters, backlog yt-dlp streaming, running totals
```

#### v23.4

```
- Skip redownload if already at target quality (pre-ffprobe for numeric, post for best)
```

#### v23.3

```
- Clear log preserves pause/resume lines, Whisper loading next indicator
- Fix redownload log gap, add Errno 9 to disk error patterns
- Auto-sync waits for queue, fix auto-sync reorg guard
```

---
#### v23.2

```
- GPU popup edge detection, TRANSCRIBING banner colors, [SKIP] two-tone coloring
```

#### v23.1

```
- Activity log alignment: center channel names, pad stat columns, ellipsis truncation
```

#### v23.0

```
- Fix repeat [SKIP] via title-based persistent dedup
```

#### v22.9

```
- Silence repeat [SKIP] for already-archived filter-rejected videos
```

#### v22.8

```
- Show 'Sync Subbed' label in activity log for auto sync-subbed runs
```

#### v22.7

```
- Fix Sync-Subs activity log label, archive live stream IDs to prevent repeat itemization
```

#### v22.6

```
- Align activity log columns, increase alternating row contrast
```

---
#### v22.5

```
- Fix transcript sort receiving string instead of list
- Fix _gpu_actively_encoding not declared global
- Fix _current_sync_ch not declared global
- Fix save_config race condition (live mutable dict to background thread)
- Fix whisper process cleanup on startup failure
- Fix bare except catching KeyboardInterrupt/SystemExit
```

#### v22.4

```
- Fix red dot blink when GPU paused, SYNCING line grey flash
- Sync Subbed itemized activity log entries
```

#### v22.3

```
- Suppress [SKIP] spam for live/upcoming videos each sync run
- Reduce _prefetch_livestreams scan from 30 to 5 videos
```

#### v22.2

```
- Fix JSONL migration flag not set for new channels
- Fix idx counter duplicate display, remove dead code
```

#### v22.1

```
- Fix activity log entries running together (missing newlines), hide text cursor
```

#### v22.0

```
- Activity log inline coloring (white text, blue Trnscr, green Manual/Auto, amber skipped)
```

#### v21.9

```
- Fix tray red spin race condition, tray stop ignoring gpu_running
- Fix sync blocked message, fix CHANNEL SYNC COMPLETE spam
- Fix simple mode [x/n] counter
```

#### v21.8

```
- Dark-mode missing channel dialog, fix right-click hang on slow drive
- Fix redownload queue not restoring after restart
- Fix Continue Redownload button needing two clicks
```

---
#### v21.7

```
- Fix redownload simple mode: white [x/x]+channel in status bar, suppress Downloading line
```

#### v21.6

```
- Single-instance lock, yt-dlp update timeout, redownload queue persistence

- Rename project from 'YT Archiver' to 'YTArchiver'
```

#### v21.5

```
- Fix whisper_prefix/whisper_title tags missing from mini-logs

- Update resolution control and add redownload ability
```

#### v21.4

```
- Fix tray icon fallback, redownload queue during GPU transcription
- Resolution refresh scan, Whisper Phase B indentation bug
```

#### v21.3

```
- Fix [X/X] gap in simple mode done lines (remove rjust padding)
```

#### v21.2

```
- Write whisper cache incrementally during Phase A (restarts skip re-checked files)
```

#### v21.1

```
- Fix counter gap in simple mode progress display
```

#### v21.0

```
- Fix fullwidth char display gap + Whisper cache not written in GPU Tasks mode
```

#### v20.9

```
- Normalize transcription progress line widths, fix Whisper queue count display
```

#### v20.8

```
- Fix GPU task list scroll garbling, fix [X/X] caption counter
- Add whisper queue count in simple mode
```

#### v20.7

```
- Fix double-click column header, simple mode Whisper log lines
- Add Whisper pending cache
```

#### v20.6

```
- Fix pause line stuck after skip/cancel
- Silence per-video Whisper queue logs in simple mode
- Auto-caption retry improvements (cookie fallback, rate-limit backoff, temp-dir wipe)
```

#### v20.5

```
- Fix Whisper queue log itemization and auto-caption reliability after pause/resume
```

#### v20.4

```
- Smaller queue pending button, clickable total size label for rescan
- Fix QUEUED not showing after startup restore
```

#### v20.3

```
- Fix 'Queued' not showing in transcription column
- Add 'Queue Pending Transcriptions' button
- Inline YT auto-captions after download (only queue GPU task if Whisper needed)
```

#### v20.2

```
- Fix redownload showing Running in Transcribed column
- Fix size not updating after redownload
```

#### v20.1

```
- Fix refresh button position next to resolution dropdown
- Fix redownload channel doubling in sync tasks
```

#### v20.0

```
- Fix Sync Resumed line staying at bottom in simple mode
- Fix shorts/live filtered video IDs not being archived (re-scanned every sync)
```

---
#### v19.9

```
- Remove per-channel header line during queued sync
```

#### v19.8

```
- Fix SYNCING grey flash, Sync Subbed button width oscillation
- Replace res refresh button with icon
```

#### v19.7

```
- Fix queue-batch sync: correct [i/total] counter
- Suppress per-channel summary/notification, show one final summary
```

#### v19.6

```
- Fix refresh button gap, atomic archive writes, disk-full patterns
- Redownload cancel_event clear, corrupt queue state fix
```

#### v19.5

```
- Fix caption tank on pause/resume or cancel/restart
```

#### v19.4

```
- Fix whisper progress line turning all-blue on pause
- Restore queue immediately on startup instead of after all checks
```

#### v19.3

```
- Dark-theme dialog for "Delete channel folder?" prompt
- Video URL redirect in Add Channel panel (detect video URL, nudge to Download tab)
```

#### v19.2

```
- Add resolution refresh button and redownload size-diff logging

- Fix resolution redownload: non-blocking fetch, cookie fallback, fuzzy match
```

#### v18.10

```
- Fix simpleline/simpleline_green appearing below SYNCING status anchor

- Fix whisper progress formatting, SYNCING flash, backfill pause support

- Fix transcribing-line width cap and bottom-anchoring

- Fix SYNCING/ENCODING line colors and transcribing line indentation

- Fix auto-caption log format in simple mode, shorten [Trnscrb] to [Trnscr]
```

#### v18.5

```
- Fix auto-captions regression on pause/resume (missing cookies in yt-dlp calls)

- Transcription UI: active line color splits + x realtime on done lines
```

#### v18.3

```
- Fix Whisper transcription stuck forever at 0%
 - stderr pipe deadlock (CTranslate2 writing to piped fd 2)
 - Reader thread race condition (old daemon threads stealing stdout lines)
 - "starting" heartbeat sent before model.transcribe()
 - Process failure no longer marks videos as "no speech detected"

- Fix 4 issues: hide URL column, hover fix, column width persistence, Whisper stuck
```

---
#### v17.7

```
- Move redownload to Sync-Tasks queue, auto-pause on disk write errors with 5-min retry
- Route resolution redownload through GPU Tasks queue

- Fix JSONL backfill order, shorten [Trnscrb] label
```

#### v17.3

```
- Fix whisper progress jitter, channel summary spam, resolution redownload dialog
```

#### v17.2

```
- Fix transcription done lines not persisting in simple mode log
- Fix "filterskip " trailing space never matching

- Add per-video progress indicator to JSONL backfill generation
```

---
#### v16.1

```
- Fix log ordering, pause anchoring, compression UI, duration format, log size limit
```

#### v16.0

```
- Fix GPU tray icon for encodes, log scroll for GPU queue messages, MT dedup edge cases
- Dark mode yes/no dialog for compress/backlog prompts

- Fix compress-after-download: batch numbering, file targeting, mtime preservation
```

#### v15.9

```
- Fix log auto-scroll: _log_user_scrolled flag prevents fighting the scroll
- Fix encode batch numbering offset when restoring from saved queue
- Add skip current GPU task with right-click confirmation dialog
- Fix right-click skip getting stuck, log snapping to bottom during progress updates
```

#### v15.6

```
- Fix log bouncing/jitter: insert-in-place instead of delete-then-insert-at-END
- Window position saved/restored on close/launch
- JSONL log line no longer fights with SYNCING animation

- Cleaned up .jsonl transcription output
```

#### v15.3

```
- JSONL backfill shows live X/76 counter, resume skips already-processed titles
```

#### v15.1

```
- Hidden JSONL timestamp files alongside .txt transcripts
- VTT segment parsing + Whisper segment data for word-level timestamps
- Backfill for already-done channels: re-fetches VTT to get timestamps
- Punctuation sweep on existing .txt entries
- Fix popup fast-path bypass of pause check
```

#### v15.0

```
- Fix log visual shifting (scroll freeze during batch operations)
- Fix transcription count resets after restart (track _prior_done)
- Add wrap="none" to log (lines clipped instead of wrapping)
- Fix paused task still animated, sync task order reversed on relaunch
- Fix Whisper punctuation density check (3+ marks AND 1/200 density)
```

---
#### v14.8

```
- Added punctuation pass on Whisper transcribed videos
- Multiple crash fixes and stability improvements
```

#### v14.7

```
- Fix GPU Tasks pause/cancel buttons showing for sync-queued items
- Added _transcribe_sync_controlled flag to distinguish sync vs GPU transcription
```

#### v14.4

```
- Crash fix attempts
```

#### v14.3

```
- Replace all root.after(0, ...) calls from worker threads
- Pause clears transient display lines before pause message appears
```

#### v14.1

```
- Bugfix
```

#### v14.0

```
- Bugfix
```

#### v13.7

```
- Bugfix
```

#### v13.5

```
- Bugfix
```

---
#### v13.4

```
- Fixed flaky pause button
```

#### v13.3

```
- Fix pause button not appearing in sync tasks queue on relaunch
- Changed compress after download settings and UI
```

#### v13.2

```
- Visual bugfixes
```

#### v13.1

```
- Visual bug fix
```

#### v13.0

```
- Small visual bug fixes
```

#### v12.8

```
- Small bugfixes
```

#### v12.7

```
- Switched to faster-whisper, visual bug fixes
```

#### v12.5

```
- Fixed flicker on Sync/GPU tasks menus
- Fixed 'active' lines in log fighting for bottom position
```

#### v12.4

```
- Fix Whisper took-time gap, 40-char title truncation on Whisper progress
- Active task shows "-ing" verbiage with animated dots
- Whisper progress line preserved when sync logs content
- GPU Pause/Cancel buttons no longer create empty frame gap
- Cancel messages specify Syncs vs GPU Tasks
```

#### v12.3

```
- Improved parsing for older livestreams
```

#### v12.2

```
- Dark title bar in all popup dialogs
- Removed X to close Sync-Tasks menu
- Configurable batch size for bulk channel compressions
```

#### v11.9

```
- Fix crash on opening manual transcription menu from GPU tasks list
```

#### v11.8

```
- Various improvements
```

#### v11.1 — HQ Compressor mode fully functional

```
- Download at high res then compress locally for better quality/size ratio
- Retroactively apply new settings to already downloaded videos
- Processes in batches of 20: download, compress, replace
```

#### v10.6

```
- Split queue into Sync Tasks and GPU Tasks (run independently)
- Transcriptions and re-encodes go to GPU Tasks queue
- Auto-transcribe adds new downloads to GPU Tasks
- (WIP) Compressor mode: download higher quality, re-encode, replace
```

---
#### v9.9

```
- CUDA utilization for punctuation model on YT captions
- Single channel sync summaries correctly show duration-filtered skips
```

#### v9.8

```
- Fix Whisper model timeout vs user selection logging
- Merged "using Whisper" and progress lines in simple mode ([idx/total] prefix)
- Fix YouTube title matching (fullwidth Unicode normalization for yt-dlp filenames)
```

#### v9.7

```
- Simple mode line purging: completed videos only show "done" line
- Elapsed time on both caption and Whisper done lines
- Windows Runtime Toast notifications with "YT Archiver" title
- Transcription recorded in sync log/autorun history
- Fix queue bug where second sync gets re-queued
```

#### v9.6

```
- Bugfix and model selection option when transcribing
```

#### v9.5

```
- Fix deadlock in _process_next_queued() (non-reentrant lock acquired twice)
```

#### v9.4

```
- Unified queue ordering via _process_next_queued() (respects insertion order)
- Drag-to-reorder works for all item types, queue save/restore persists order
- Immediate "Checking channel..." feedback after SYNCING header
```

#### v9.3

```
- Sync-subbed / manual sync queueing (checks transcribe_running before starting)
- Red spinning tray icon for transcriptions
```

#### v9.2

```
- Changed order of transcriptions in .txt file to chronological
```

#### v9.1

```
- Distinct queue labels: "Add to job queue" vs "Queue transcription"
```

#### v9.0

```
- Version bump
```

---
#### v8.9

```
- Fix Whisper verbose=True crashing JSON protocol (stdout pipe collision)
- Progress capture: intercept Whisper print() calls, parse timestamps, send JSON %
- Redirect sys.stderr to dummy StringIO to swallow tqdm/warnings
```

#### v8.8

```
- Whisper progress % via stderr timestamp parsing
- "Adding punctuation..." log line before _punctuate_text() runs
- Pause during Whisper: progress shows "will pause after this file..."
```

#### v8.7

```
- Fix punctuation model crash in exe (sys.stdout/stderr are None)
- Fix terminal windows flashing during transcription (missing startupinfo on 8 calls)
```

#### v8.6

```
- Clear selection when switching away from Recent tab
- Transcript headers include source info
```

#### v8.5

```
- Punctuation model (deepmultilingualpunctuation + transformers)
- Direct pipeline API with correct aggregation_strategy
- ~0.23s per transcript on CPU, loads once, reused for all files
- Fix pause/cancel buttons disappearing (_cancel_safety_net guard)
- Fix manual sync + sync-subbed running simultaneously (_job_generation counter)
- Green vs spinning circle taskbar icon fix
- Non-CUDA fallback (skip Whisper, auto-captions + punctuation still works)
```

#### v8.2

```
- Version bump (v7.7 to v8.2)
```

---
#### v7.7

```
- Version bump
```

#### v7.6

```
- Version bump
```

#### v7.5

```
- Version bump
```

#### v7.4

```
- Version bump
```

#### v7.2

```
- Bug patch
```

---
