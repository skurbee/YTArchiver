# YTArchiver — Project Map

A guide to the runtime files, their responsibilities, and the main paths
through the application. Test suites and supporting assets are grouped by
purpose. For change and build procedures, see [CONTRIBUTING.md](CONTRIBUTING.md)
and [BUILD.md](BUILD.md).
---

## What YTArchiver is

YTArchiver is a desktop app that maintains a local video
archive of YouTube channels. You give a list of
channels; it periodically syncs each one, downloads any new videos
via `yt-dlp`, transcribes them with Whisper, fetches their metadata,
and stores everything in a structured folder tree on disk. The point
is to own a permanent, offline-accessible copy of content that might
disappear in the future.

A SQLite catalog and transcript index make the archive searchable across channels,
including full-text search inside transcripts. The UI shows you each
channel's history, lets you browse / play videos in an embedded
player with karaoke-style word highlighting on the transcript, and
graphs things like word-frequency over time across the archive.

## How it's built

The app is Python + an embedded web UI. Python runs the heavy lifting
(yt-dlp, ffmpeg, Whisper, SQLite, file I/O); the UI is plain HTML +
CSS + JavaScript rendered inside a `pywebview` window. The two sides
talk through a tiny bridge: JavaScript calls `window.pywebview.api.<method>(...)`
to invoke Python methods, and Python pushes streaming updates back to
JS by calling `window._logBatch(...)` and a handful of other globals.
The Python host owns the application services and the native WebView2 window.
The UI uses the pywebview bridge rather than a Flask application server.
Separate HTTP services provide local media playback and a command API.
The command server defaults to loopback, allows unauthenticated GET status
reads, and requires a token for POST actions. Worker subprocesses perform
transcription and external-tool work.

The exe is built with PyInstaller (see `YTArchiver.spec`). Whisper
runs in its own Python 3.11 environment, with CPU/CUDA dependency locks kept
separate from the Python 3.13 main application.

---

## Top-level files

### `main.py`
The entry point. Defines the `Api` class, which is the single object
exposed to JavaScript via `pywebview`. Backend operations such as adding a
channel, starting a sync, loading transcripts, and deleting bookmarks are
methods on this class. Playback seeking and other local UI interactions
remain in JavaScript.

Also handles app startup: single-instance mutex, config loading, tray
icon, log streamer setup, autorun scheduler, transcription manager,
window state restore, and signal handling for clean shutdown.

**Key things to find inside:**
- `class Api` — the JS bridge, composed from `backend/api_mixins/`.
  Its constructor connects shared services, managers, queues, and callbacks;
  most feature endpoints live in the mixins.
- `backend/version.py` — the single source of truth for `APP_VERSION` and
  `APP_VERSION_DATE`.
- `main()` and the final `if __name__ == "__main__":` block — initialize
  the host, create the pywebview window, and start its event loop.
- Startup stages — load cached library state first, then run supervised
  background catalog and disk checks that yield to foreground work.

### `YTArchiver.spec`
PyInstaller "recipe". Tells PyInstaller how to bundle `main.py` plus
every backend module, the `web/` folder, the icon, and the whisper
worker script into a single `dist/YTArchiver.exe`. The normal build path is
the verified gate in `scripts/check.ps1`, not a bare PyInstaller command.

### Toolchain, dependency, and release files

- `.python-version` and `.nvmrc` pin the Python and Node versions used by the
  local gate and Windows CI.
- `requirements/*.lock` contains exact, hash-verified locks for the app,
  build/test tools, and CPU/CUDA worker environments.
- `package.json` and `package-lock.json` pin the Playwright browser-test
  harness; `playwright.config.js` controls headless browser selection and output.
- `pyproject.toml` owns Ruff configuration. `.gitignore` and `.gitattributes`
  define generated-file exclusions and repository text handling.
- `THIRD_PARTY_NOTICES.md` and `licenses/` carry the notices and license text
  packaged with the executable.
- `.github/workflows/quality.yml` runs the same Windows quality gate used
  locally and uploads the verified executable as a CI artifact.
- `docs/ARCHITECTURE.md`, `docs/PROJECT_MAP.md`, `docs/CONTRIBUTING.md`, and
  `docs/BUILD.md` describe design, file ownership, contribution, and builds;
  `docs/CHANGELOG.md` records release notes. `docs/requirements.txt` is a
  broad dependency reference; reproducible builds use `requirements/*.lock`.

See [`BUILD.md`](BUILD.md) for the complete command and gate stages.

### `icon.ico`
The window icon and tray icon, used by both `pywebview` and `pystray`.
Bundled into the exe via the spec file.

---

## `backend/` — the Python package

The application's Python modules and worker entry points. The presence of
`__init__.py` is what makes Python treat this folder as an importable
package (so `from backend.sync import sync_channel` works).

### `__init__.py`
Contains a short package overview. The detailed file inventory and current
ownership boundaries are described below.

### `api_mixins/`  ·  feature endpoints on `main.Api`

`__init__.py` exports the mixin classes; `_shared.py` holds shared imports and
compatibility helpers. Endpoint ownership is divided as follows:

- `archive_mixin.py`, `index_mixin.py` — archive discovery/rescans and index operations.
- `subs_mixin.py`, `channel_mixin.py` — subscriptions, defaults, and channel actions.
- `sync_mixin.py`, `queue_mixin.py` — sync admission, worker lifecycle, and queue commands.
- `browse_mixin.py`, `recent_mixin.py`, `bookmark_mixin.py` — library lists,
  transcript/file resolution, recent items, and bookmarks.
- `video_mixin.py`, `media_ops_mixin.py`, `redownload_mixin.py` — video actions,
  media tools, and redownload workflows.
- `metadata_mixin.py`, `thumbnail_mixin.py`, `transcribe_mixin.py` — metadata,
  thumbnails, and transcription controls.
- `backup_mixin.py`, `trash_mixin.py` — app-state backup/restore and recoverable Trash.
- `diagnostics_mixin.py` — diagnostic reports and cancellable integrity-preview jobs.
- `settings_mixin.py`, `info_mixin.py`, `onboarding_mixin.py` — preferences,
  runtime information, and first-run setup.
- `startup_mixin.py`, `window_mixin.py`, `livestreams_mixin.py` — startup helpers,
  native-window operations, and deferred livestream controls.

Feature mixins share `self.services` and the managers wired by `main.Api`.
Use the service or repository owning the data when tracing a mutation.

### `archive_scan.py`  ·  filesystem reality check
Walks the channel folder tree on disk and produces per-channel video
counts, total sizes, and recency stats. The Subs and Browse tabs
use these saved stats alongside catalog data. Has an on-disk JSON cache so the
walk doesn't have to happen every time the UI repaints.

**Key functions:** `scan_channel_folder`, `scan_all_channels`,
`enrich_channels_with_stats`, `cache_coverage`, `publish_scan_stats`,
`index_summary`, `index_db_stats`, `archive_totals`.

`index_summary` describes saved scans of current subscriptions and includes
explicit scanned/total-channel coverage. `index_db_stats` reads the full
catalog independently. A partial cache must never stand in for the full
catalog's video count. Startup schedules a disk walk when coverage is incomplete,
even when the previous scan timestamp is recent; only complete coverage earns
a completed-scan timestamp.

### `auto_backup.py`  ·  scheduled backups + the archive's info folder
Maintains `<archive root>/YTArchiver Info/`: a generated
`ABOUT THIS ARCHIVE.txt` documenting every file convention in the
archive, a copy of the running exe (frozen builds), and scheduled
full-state backup ZIPs (config, subscriptions, download-ID archive,
queue and recovery sidecar, filters, activity history, independently saved
bookmarks/notes, and a database snapshot when available). Bookmarks are kept
even when the full database snapshot is omitted. The ZIP manifest records its
creation time and resource checksums. Owns `build_backup_zip` — the single zip-writing core
that the Health tab's manual Export (backup_mixin) also calls. The
newest 4 scheduled backups are kept; older scheduled ZIPs are removed
automatically. Manual backup exports are not part of this rotation.

**Key classes/functions:** `class AutoBackupScheduler` (daemon timer,
"off"/"daily"/"weekly"/"monthly" via `auto_backup_interval`),
`run_backup`, `build_backup_zip`, `backup_file_entries`,
`refresh_info_folder`.

### `autorun.py`  ·  recurring background sync
Schedules sync passes using interval timers or clock-aligned times, including
configurable anchors for the 12-hour and daily schedules. Also
uses `activity_history.py` for durable activity-log history. It reports the
next scheduled time, running state, and any workload delaying a due run.

**Key classes/functions:** `class AutorunScheduler` (the timer-driven
job), `append_history_entry`, `clear_history`, `format_history_entry`.

### `channel_art.py`  ·  avatars and banners
Downloads each channel's avatar + banner once, caches them in a
`.ChannelArt/` subfolder, and creates thumbnail versions for the UI.

**Key functions:** `fetch_channel_art`, `avatar_path_for`,
`banner_path_for`, `ensure_banner_thumb`, `ensure_avatar_thumb`.

### `channel_cache.py`  ·  "have we seen this video before?"
A per-channel cache of every video ID ever returned by yt-dlp for that
channel. The sync uses this as a fast-path so it doesn't have to
re-walk a 2,000-video channel's full catalog every time you sync.

**Key functions:** `get_cached_ids`, `set_cached_ids`, `append_ids`,
`clear`, `counts`.

### `cmd_server.py`  ·  localhost HTTP shim
A tiny HTTP server (Python `http.server`) that external tools can use without
driving the GUI. It binds to localhost by default; a non-loopback bind requires
both `YTARCHIVER_CMD_BIND` and `YTARCHIVER_CMD_ALLOW_LAN=1`. GET status routes
are unauthenticated. POST actions require the installation token through
`X-Auth-Token` or the token query parameter. `main.py` registers ping,
GPU-status, and retranscription routes.

**Key functions:** `start_server`, `stop_server`, `register_handler`.

### `compress.py`  ·  re-encode old downloads
Drives ffmpeg's AV1 NVENC encoder at a configured quality and resolution,
validates the output, and promotes it over the original. Used by the per-channel
"compress" toggle to save disk over time.

**Key functions:** `compress_video` (one file), `compress_videos_batch`
(many), `find_ffmpeg`, `get_bitrate`, `get_video_duration`,
`get_video_codec`.

### `disk_watch.py`  ·  is the drive still there?
Watches the configured archive root for disconnect / write-protection.
If it goes away mid-sync the app pauses gracefully instead of corrupting state.

**Key classes/functions:** `class DiskErrorMonitor`,
`_check_directory_writable`.

### `drift_scan.py`  ·  catch silent drift
Compares transcript records in a channel's text and JSONL sidecars. Reports
entries present in only one representation and uses catalog IDs to resolve
identity where possible; ambiguous same-title matches cannot be repaired
automatically. It also reports global FTS integrity-check failures. The
legacy `fts_phantoms` result key aliases an unhealthy-index count, not a count
of missing video files. Used by the drift-check tools in Health.

**Key functions:** `scan_channel`, `apply_channel`,
`rebuild_fts_index`.

### `integrity_scan.py`  ·  read-only cross-store preview

Inspects explicit archive, config, database, and queue paths without changing
them. SQLite is opened immutable/read-only. The report compares FTS and
transcripts, legacy and normalized catalog links, saved-media IDs, queue and
transcription recovery records, folder overrides, activity history, and
migration state. It reports proposed repairs but deliberately has no repair
entry point. `api_mixins/diagnostics_mixin.py` wraps it in a supervised job
with progress, elapsed time, cancellation, and a final report for the UI.

**Key functions:** `scan_integrity`, `run_integrity_scan`.

### `index.py`  ·  the SQLite database (entry module)
THE central data store. Every downloaded video gets a row here with
its title, channel, upload date, duration, file path, transcription
state, and metadata-fetch state. Browse / Search / Videos / Graph
all read from this DB.

Also stores transcript SEGMENTS (one row per Whisper segment per
video) so full-text search can pinpoint matches inside transcripts.

This file owns the connection management (`_open` / `_reader_open`),
schema, and the most-called read/write functions. Specialized query
families live in sibling modules (`index_search.py`, `index_graph.py`,
`index_bookmarks.py`, `index_maintenance.py`) which `index.py`
re-exports for back-compat.

**Key functions (still in this file):** `register_video`,
`mark_video_transcribed`, `ingest_jsonl`, `list_recent_videos`,
`list_videos_for_channel`, `list_all_videos`, `find_thumbnail`,
`new_videos_in_last_n_days`, `channel_transcription_stats`,
`get_segments`, `get_segment_context`, `summary`.

**Connection primitives:** `_open` (shared writer), `_reader_open`
(separate shared reader), `_open_independent` (a connection owned by the
calling operation), `_interactive_reader` and `_bounded_sql` (bounded lock
and SQL waits for interactive reads).

### `catalog_repository.py`  ·  logical videos and physical media

Owns the additive normalized projection beside the legacy `videos` table.
`logical_videos` represents content identity and `media_files` represents each
known path/copy. Identity comes from a YouTube ID, then a normalized path,
then an explicit legacy row ID; title text is never used as identity.

The staged migration creates and verifies a separate legacy-catalog backup,
projects old rows, compares deterministic digests, and enables normalized
reads only after equivalence passes. Compatibility triggers mark identities
dirty when an older executable writes `videos`; `CatalogConnection` reconciles
those identities in the same transaction as the next compatible catalog commit.

**Key classes/functions:** `CatalogConnection`, `CatalogStatus`,
`create_verified_legacy_catalog_backup`, `verify_legacy_catalog_backup`,
`reconcile_catalog`, `reconcile_dirty_catalog`, `catalog_status`,
`normalized_reads_enabled`.

### `index_search.py`  ·  FTS5 + title search
Owns the Browse > Search backend.

When normalized reads are enabled, both title and transcript results resolve
through logical video identity and its canonical available media path. This
prevents duplicate physical copies from multiplying search results.

**Key functions:** `search_video_titles` (LIKE-based titles), `search_fts`
(FTS5 MATCH over transcript segments), `_sanitize_fts_query`.
Interactive title/transcript queries use bounded database access. Transcript
resolution validates catalog identity before accepting filename-derived IDs.

### `index_graph.py`  ·  word-frequency graphing
Powers Browse > Graph.

**Key functions:** `bucket_totals`, `top_words`,
`graph_word_frequency`, `graph_multi`, `graph_channel_overlay`,
`backfill_upload_ts`,
`list_all_channels_in_db`.

### `index_bookmarks.py`  ·  bookmark CRUD
Contains the index-backed bookmark operations.
Writes use the admitted shared writer with bounded waits and duplicate-save
protection. The list query filters title, channel, and note before its result
limit is applied.

**Key functions:** `bookmark_add`, `bookmark_list`, `bookmark_remove`,
`bookmark_update_note`.

### `index_maintenance.py`  ·  archive sweep + prune + FTS rebuild
Owns catalog sweeps, missing-file reconciliation, and FTS rebuild work.
The bridge mixins admit and supervise these operations for startup and
Health's archive/search-index controls.

**Key functions:** `sweep_new_videos`, `prune_missing_videos`,
`rebuild_fts_index`.

### `html_assembler.py`  ·  build web/index.html at boot
Reads `web/index.template.html` and every
`<!-- @include partials/X.html -->` marker, then writes the assembled
`web/index.html`. It compares generated bytes to the existing artifact and
only rewrites when content changed.

**Key function:** `assemble_index_html(web_dir)`.

### `livestreams.py`  ·  defer "not downloadable yet" videos
Detects when yt-dlp returns a "video unavailable / livestream not
started" error and stashes the URL in a deferred list. The drawer
in the lower-right shows what's pending; the next sync retries them.

**Key functions:** `defer`, `drop`, `ignore`, `is_ignored`,
`list_deferred`, `drawer_state`, `snooze_drawer`,
`line_looks_live`.

### `local_fileserver.py`  ·  serve local files to the embedded page
The pywebview page can't load `file://` URLs reliably, so this is a
localhost HTTP server (random port, allowlist of allowed roots) that
serves local media, captions, channel artwork, and thumbnails to the embedded
page. Requests are tokenized and restricted to allowed roots/files.

**Key functions:** `set_allowed_roots`, `start_server`, `stop_server`,
`url_for`.

### `log_stream.py`  ·  Python → JS log pipe
Backend code writes log "segments" (a tuple of text + style tag) to
the `LogStreamer`, which batches them and pushes them
into JS via `window._logBatch(payload)`. This is the bus that the
Sync Log, mini-logs, and activity rows ride on.

**Key classes/functions:** `class LogStreamer` (the bus),
`emit`, `emit_text`, `emit_simple`, `emit_dim`, `emit_error`,
`emit_header`, `emit_activity`, `_line_is_verbose_only` (the
simple-mode filter that hides chatty output).

### `metadata/`  ·  views / likes / comments refresh  ·  package
yt-dlp metadata refresh pipeline for downloaded videos. Pulls view counts,
like counts, comments, and descriptions; updates metadata sidecars and the
catalog. Health > Library owns the channel-information table and bulk tools;
individual video actions also use this package.

Package layout (`metadata/__init__.py` preserves the public compatibility surface):
- `core.py` — title-match strategies + bulk-stats pipeline
- `fetcher.py` — per-video / per-batch yt-dlp metadata fetch
- `refresh.py` — re-export shim
- `refresh_views.py` — `bulk_refresh_views_likes`
- `refresh_comments.py` — `refresh_channel_comments`
- `refresh_fetch.py` — `fetch_channel_metadata`
- `_refresh_proxies.py` — lazy proxies into core.py for the three
  refresh modules above
- `normalize.py`, `scan.py`, `thumbnails_ops.py` — text utils,
  metadata-row scanning, thumbnail housekeeping
- `io.py` — metadata JSONL paths and validated sidecar reads/writes
- `refresh_state.py` — successful refresh timestamps on channel records
- `manual_backfill.py` — candidate matching for manual videos missing IDs;
  ambiguous matches are saved for review instead of assigned automatically

**Key functions:** `fetch_single_video_metadata`,
`fetch_metadata_for_videos` (batch), `bulk_refresh_views_likes`,
`count_thumbnail_status_bulk`, `count_video_id_status_bulk`,
`sweep_missing_thumbnails`, `realign_misplaced_thumbnails`.

### `services/`  ·  explicit state and lifecycle boundaries  ·  package

The gradual replacement for implicit cross-mixin `self._*` ownership in
`main.Api`. New code should depend on a named repository or domain service and
receive shared instances through `self.services`; `AppServices` stays a thin
dependency holder.

Package layout:
- `app_services.py` — `AppServices`, the long-lived dependency container
- `config_repository.py` — config load/replace/serialized mutation contract
- `queue_repository.py` — atomic queue/resuming commits and corruption
  preservation (used by `QueueState`)
- `sidecar_store.py` — locked, validated, staged sidecar reads/writes and
  durable multi-store reconciliation markers
- `event_bus.py` — `BridgeEventBus`, safely serialized Python-to-JS dispatch
- `file_ops.py` — managed-root containment and recoverable destructive actions
- `job_supervisor.py` — register-before-start background ownership,
  admission, checkpoint, bounded join, and exact force-stop
- `managed_work.py` — small adapters for supervised API/startup work
- `channel_leases.py` — atomic channel-alias and archive-wide leases
- `channel_transactions.py` — crash-recoverable folder/config transaction
  journal
- `restore_coordinator.py` — validated, staged, rollback-capable state restore

**Key classes/functions:** `AppServices`, `ConfigRepository`,
`QueueRepository`, `BridgeEventBus`, `JobSupervisor`,
`begin_reconciliation`, `channel_leases`, `start_managed_task`,
`safe_remove_file`, `safe_rmtree_channel_folder`.

### `net.py`  ·  am I online?
Tiny TCP-connect probe (`probe_once`) and a background monitor that
sets a flag when the network goes down. Other modules call
`block_if_down` to pause work when there's no connectivity instead
of failing every download retry.

**Key functions:** `probe_once`, `start_monitor`, `block_if_down`.

### `provenance.py`  ·  "Embed file tags" backfill
Retrofits the archive's existing files with the embedded identity new
downloads get. Phase A upgrades legacy Transcript.txt headers with the
`(youtu.be/<id>)` field (ids matched from the sibling `.jsonl`,
unambiguous titles only). Phase B stream-copy remuxes each known-ID
MP4 (`ffmpeg -map 0 -c copy -movflags +faststart`) to embed title /
channel / upload-date / watch-URL tags — no re-encode, atomic
tmp+replace, mtimes preserved. A ledger in APPDATA makes re-runs skip
already-tagged files. Runs as a sync-queue task (kind `provenance`),
so it inherits pause / resume / cancel.

**Key functions:** `embed_provenance_archive` (queue-task body),
`_upgrade_txt_file`, `_embed_one`, `_mp4_worklist`.

### `punct_worker.py`  ·  punctuation restoration subprocess
This subprocess runs a punctuation model that
reads in raw text and writes out punctuated + capitalized text.
Stays alive between transcribe jobs so the model only loads once.

The standalone script reads JSON requests from stdin and writes responses
to stdout. The
`PunctuationManager` class in `transcribe/punct_manager.py` manages it.

### `queues.py`  ·  Sync + GPU task queues
Two persistent queues backed by `ytarchiver_queue.json`: the Sync
queue (yt-dlp downloads) and the GPU queue (Whisper transcriptions +
ffmpeg compressions). The Sync Tasks / GPU Tasks popups in the
header render these.

`QueueState` owns the in-memory state machine. File parsing and commits are
delegated to `services/queue_repository.py`, which preserves malformed input
as a sidelined backup and never replaces the last known-good file with an
invalid object. Full-library sync runs also retain a ledger of their exact
task IDs, so pause/restart resumes the same batch and only a successfully
completed batch advances the global "Last Full Sync" timestamp.

**Key classes/functions:** `class QueueState` (enqueue, pop, remove, reorder,
clear, current-running tracking, pause flags, UI payload formatting),
`QueueRepository` (main/resuming load and atomic commit).

### `redownload.py`  ·  replace media at a chosen resolution
Per-video and per-channel redownload actions use this module. It
finds the existing file, identifies its current resolution via
ffprobe, and re-fetches via yt-dlp at the selected target. A lower target
can deliberately reduce resolution; "at target" checks account for that.

**Key functions:** `redownload_channel`, `_fetch_yt_catalog`,
`_match_files_to_ids`, `_ffprobe_height`, `_already_at_target`,
`_download_one`.

### `reorg.py`  ·  shuffle into year/month folders
Once a video's upload_date is known, move it into the right
`YYYY/MM Month/` subfolder. The Subs settings let you turn on
year-split, month-split, or both.

**Key functions:** `reorg_channel`, `fix_file_dates` (sets file
mtime to upload date), `_move_video`, `_cleanup_empty_dirs`,
`_date_from_info_json`.

### `seen_filters.py`  ·  remember title filters
If a duration / regex filter rejects "Episode 47", we remember that
in a persistent set so the next sync doesn't waste a yt-dlp call
re-filtering the same title.

**Key functions:** `is_seen`, `mark_seen`, `clear`, `count`.

### `subs.py`  ·  the Subs CRUD
Channel-subscription create / read / update / delete. Validates a
YouTube channel URL, normalizes its shape, prevents duplicates, and
applies defaults to new channel records.
Channel roots are normalized without forcing a `/videos` tab, allowing
streams-only channels. `channel_identity.py` owns durable YouTube channel-ID
verification and recovery from changed handles.

The Add Channel UI's default switches come from `SubsMixin.subs_get_defaults`:
Auto-metadata and Auto-transcribe on, Compress off, unless an explicit saved
default overrides them. Existing channel records retain their own settings.

**Key functions:** `normalize_channel_url`, `validate_channel_url`,
`fetch_channel_display_name`, `add_channel`, `update_channel`,
`remove_channel`, `get_channel`, `list_channels`,
`ensure_videos_suffix`, `streams_url`.

### `sync/`  ·  the central download path  ·  package
THE single most important area of the backend. Wraps `yt-dlp` as a
subprocess for each channel sync, parses its stdout line by line,
emits log lines through the LogStreamer, and dispatches inline
metadata + transcribe jobs as each video completes.

Package layout (`sync/__init__.py` preserves the public compatibility surface):
- `core.py` — `sync_channel`, the per-channel orchestration giant
- `sync_all.py` — `sync_all`, the multi-channel batch coordinator
- `download_commit.py` — validates durable final media and performs the one
  catalog-registration commit for completed downloads
- `sync_helpers.py` — small file/format helpers (`_hide_sidecar_win`,
  `_sweep_orphan_vtts`, `_scan_recent_video`, `_resolve_final_mp4`,
  `_fmt_duration`, `_fmt_size`)
- `log_rows.py` — activity-log row emission + persistence
  (`emit_consolidated_auto_row`, `emit_metadata_activity_row`,
  `_sync_row_emit`, `_persist_row_history`)
- `quickcheck.py` — fast "are there new uploads?" probe using channel roots,
  including channels that only have a Live tab
- `options.py` — normalized per-channel sync options and match filters
- `ytdlp_proc.py` — yt-dlp executable lookup, cookies, format strings, and batch-file helpers
- `ytdlp_events.py` — yt-dlp output parsing helpers
- `ytdlp_session.py` — process launch, watchdog, finish/cleanup helpers
- `recent_track.py` — recent-download history tracking (`_record_recent_download`)
- `active_state.py` — in-flight sync-channel tracking + metadata-changed hook
- `display_push.py` — sync-progress JSON writes for a companion display

**Key functions:**
- `sync_channel` — the central function. Walks one channel, spawns
  yt-dlp, parses every output line, manages the per-video Downloading-
  line lifecycle, handles cookie / livestream / archive-skip / Merger
  / DLTRACK events.
- `commit_download` — rejects missing/empty/partial media and returns one
  explicit `DownloadCommitResult` for durable media plus registration.
- `sync_all` — top-level batch sync across all subscribed channels.
- `build_format_string` — turns a resolution preference (e.g. "1080")
  into the right yt-dlp format selector.
- `sanitize_folder`, `channel_folder_name` — Windows-safe folder
  name from a channel record.
- `set_sync_active` / `clear_sync_active` / `is_sync_active` /
  `is_any_sync_active` — the "is this channel being synced right
  now?" flag used by other workers to coordinate.
- `emit_consolidated_auto_row`, `emit_metadata_activity_row` —
  the consolidated `[Dwnld] N downloaded · M transcribed` activity-
  log row format.
- `_record_recent_download` — appends to the recent-download history.
- `prefetch_channel_total`, `quick_check_new_uploads` — the fast
  "are there any new videos?" check without a full channel walk.

### `temp_cleanup.py`  ·  delete .part / .ytdl leftovers
On startup, sweep the channel tree for partial files left behind by
cancelled / crashed yt-dlp invocations.

**Key functions:** `is_partial_file`, `cleanup_folder`,
`startup_cleanup_temps`.

### `transcribe/`  ·  Whisper manager  ·  package
Owns the transcription pipeline:

1. Already-saved, already-punctuated captions can commit inline after a
   recovery marker is persisted. Unfinished work is promoted to the ordinary
   Processing queue using the same durable task identity.
2. Queued jobs normally try saved/fetched YouTube captions before Whisper.
   A successful caption path includes durable sidecar/index commits and
   recovery cleanup. Cancellation and partial/failed commits remain explicit
   outcomes requiring the appropriate recovery policy.
3. Jobs without usable captions run faster-whisper in the Python 3.11 worker
   with CUDA or CPU, optionally restore punctuation, and commit transcript
   text and JSONL. Explicit Whisper re-transcription bypasses caption reuse.

Package layout (`transcribe/__init__.py` preserves the public compatibility surface):
- `core.py` — `TranscribeManager` + worker loop
- `job_execution.py` — explicit worker outcomes and cancel/defer/shutdown
  policy after file-changing work stops
- `helpers.py` — pure helpers (path/title resolution, `find_python311`,
  `_extract_video_id`, `_bump_transcription_pending`,
  `_resolve_transcript_paths`, `_ffprobe_duration`, chunk constants)
- `punct_manager.py` — `PunctuationManager` subprocess wrapper
- `paths.py` — aggregate/per-video transcript naming and JSONL sidecar paths
- `transcribe_vtt.py` — fast-path: `_try_auto_captions`,
  `_fetch_captions_via_ytdlp`, `_parse_vtt`
- `transcribe_files.py` — file I/O: `_write_jsonl_entry`,
  `_write_transcript_entry`, `_replace_jsonl_entry`,
  `_replace_txt_entry`

**Key classes/functions:**
- `class TranscribeManager` — the worker thread that consumes the
  GPU queue.
- `TranscriptionJobExecutor`, `WorkerOutcome`, `execution_decision` — the
  result contract that decides terminal removal versus retry/pause.
- `class PunctuationManager` — manages the `punct_worker.py`
  subprocess.
- `find_python311` — discovers the Whisper environment.
- `_try_auto_captions`, `_fetch_captions_via_ytdlp`, `_parse_vtt`
  — the fast-path.
- `_write_jsonl_entry`, `_write_transcript_entry` — append to the
  selected JSONL sidecar (word-timestamp data) and transcript text;
  paths may aggregate a channel's videos or belong to one manual download.
- `_replace_jsonl_entry`, `_replace_txt_entry` — surgical replace
  for re-transcribe.

### `tray.py`  ·  Windows system-tray icon
pystray-driven tray icon with a context menu, animated spinner during
sync, badge overlay for pending tasks, and "On top" toggle.

**Key classes/functions:** `class TrayController` (start / stop / set
tooltip / set badge / start / stop spin / set autorun menu).

### `utils.py`  ·  shared low-level helpers
Compatibility exports and remaining helpers reused across modules: subprocess env setup, byte
decoding with cp1252 fallback, time/size/duration formatting, disk
space check, process kill helper, ffprobe-based "is this video
already compressed?" check.

**Key functions:** `utf8_subprocess_env`, `decode_subprocess_line`,
`format_bytes`, `format_duration_hms`, `format_elapsed`,
`format_enc_size`, `fmt_time_ago`, `norm_ascii`,
`check_directory_writable`, `check_disk_space`, `kill_process`,
`ffprobe_is_compressed`, `try_find_by_title`,
`try_locate_moved_file`.

### `whisper_worker.py`  ·  Python 3.11 transcription subprocess
The actual faster-whisper invocation, kept in its own process and on
its own Python version so the main app can stay on 3.13. Reads JSON
job descriptions from stdin and writes transcript results (with word-
level timestamps) to stdout.

Standalone worker script with a stdin-reader thread and explicit result
messages. Includes the segment-cap re-segmentation logic that makes the
karaoke transcript viewer behave on long monologues.

### `window_state.py`  ·  remember window geometry
Persists the pywebview window size, position, maximized/on-top state,
splitter position, and column widths
inside `ytarchiver_config.json` (under the `window_state` key, so it
roams with the user's other settings) on close, restores on launch.

**Key functions:** `load_window_state`, `save_window_state`,
`_sanitize_geometry`.

### `ytarchiver_config.py`  ·  the user-settings file
Reads / writes `%APPDATA%\YTArchiver\ytarchiver_config.json`, which
holds every user setting: archive root, subscribed channels, autosync
interval, log mode, recent downloads, etc. The single source of truth
for "what does this user have configured".

Also formats the data for UI consumption (channels-for-Subs-table,
recent-download history, autorun-history-for-Activity-log).

**Key functions:** `load_config`, `save_config`, `config_file_exists`,
`config_is_writable`, `backup_config_on_start`,
`channels_for_subs_ui`, `recent_for_ui`,
`autorun_history_entries_for_ui`, `append_pending_tx_id`,
`remove_pending_tx_id`.

### Supporting backend modules

| Files | Responsibility |
|---|---|
| `activity_history.py` | Stable-ID activity history in its own durable store; reads legacy formats during migration. |
| `archive_capacity.py` | Archive-drive free-space warnings. |
| `channel_identity.py` | Learn permanent `UC…` channel IDs and verify handle recovery before changing saved URLs. |
| `subscriber_counts.py` | Background recovery and caching of channel subscriber counts. |
| `youtube_session.py` | Shared authentication/rate-limit failure handling and visible pause state. |
| `youtube_traffic.py` | Persistent operation budgets, spacing, admission reservations, and traffic projections; a yt-dlp launch is an operation rather than an exact HTTP-request count. |
| `deps_installer.py` | Optional external-tool and worker-environment installation helpers. |
| `process_runner.py` | Child-process registry and supervised yt-dlp/ffmpeg execution. |
| `proc_utils.py`, `subprocess_util.py` | Subprocess environment, decoding, Windows launch flags, and process helpers. |
| `executor_utils.py`, `pause_helpers.py` | Bounded thread-pool submission and shared pause/cancel coordination. |
| `fs_search.py` | Canonical media-extension sets, partial-file detection, and channel file walkers. |
| `fs_safety.py`, `fs_attrs.py` | Containment, atomic I/O, disk checks, sidecar cleanup, and Windows visibility attributes. |
| `fmt_utils.py`, `view_format.py` | Shared byte/time/duration formatting and UI-specific display formats. |
| `text_utils.py` | Canonical title normalization and matching helpers. |
| `log.py` | Python logging adapter for the UI log stream. |
| `thumbnails.py` | Validated thumbnail downloads, atomic cache writes, and status helpers. |
| `repair_captions.py` | Re-fetch and repair saved YouTube caption word/timestamp data. |
| `punct_restore.py`, `punct_alignment.py` | Restore punctuation in existing transcripts and align punctuated text with word timing. |
| `trash_manager.py` | Validated Trash listing, restoration, and permanent deletion through recovery manifests and opaque entry IDs. |
| `trash_retention.py` | Retention timing, workload deferral, and lifecycle of automatic Trash cleanup. |
| `taskbar_overlay.py` | Native Windows taskbar overlay, separate from the system-tray icon. |
| `version.py` | Application version and version date used by runtime and build verification. |

---

## `web/` — the frontend

Plain HTML / CSS / JS with a small HTML assembly step and no JavaScript
transpilation. The whole UI is
rendered inside the pywebview window using the embedded Edge WebView2
on Windows.

### `index.template.html`, `partials/`, and `index.html`

Edit the template or a partial, then regenerate `index.html` through
`backend/html_assembler.py`. The generated page is packaged with the app;
`scripts/check_generated_html.py` checks that it matches its sources.

The template defines:
- Header strip (title + version)
- Four default visible tabs: Download / Browse / Health / Settings. The
  `data-tab="subs"` element is hidden in the template and retained for the
  optional Dense Subs compatibility view, controlled by its saved preference.
- Tab panels and their included partials
- Floating overlays (modals, context menu, drawers, popups)
- Script tags loading foundation modules, feature controllers, renderers,
  and finally `app.js` in dependency order

The partials own these surfaces:

- `tab-download.html` — URL entry, download options, queues, and activity/main logs.
- `tab-subs.html` — Dense Subs table and the shared Add/Edit Channel controls.
- `tab-browse.html` — Channels, Videos, Manual, Recent, Search, Graph,
  Bookmarks, Trash, and Watch views.
- `tab-health.html` — Overview, Library, and Backups sections.
- `tab-settings.html` — application preferences and archive-folder configuration.
- `popovers.html` — queue and status popovers.
- `dialogs.html`, `modals.html` — feature dialogs and shared modal structures.
- `onboarding.html` — first-run setup overlay.

### `styles.css` + `styles-*.css`
All visual styling. Dark theme. CSS variables (`:root` block in
`styles.css`) define the color palette so theming is centralized.
Split into themed sheets that load in cascade order:

- `styles.css` — `:root` vars, base, header, tab row,
  tab panels
- `styles-settings.css` — Settings and Health pages
- `styles-download-controls.css` — Download tab controls
- `styles-logs.css` — Activity log + main log + tag classes
- `styles-tabs-data.css` — Subs table, data panels, queue popovers
- `styles-browse.css` — Browse tab framing + sub-modes
- `styles-browse-grids.css` — Channel + Video grids
- `styles-watch.css` — Watch view + captions + drawer
- `styles-dialogs.css` — Dark dialogs + toasts + modals
- `styles-onboarding.css` — first-run setup overlay

### Frontend module split

`app.js` and `logs.js` were originally large monoliths. Both have been
decomposed into focused single-concern
files. Loaded in order by `index.html` and stitched together at
runtime through `window.*` published handles. See the **Frontend
modules** section below for what each one does.

### `app.js`
The bootstrap + tab init orchestrator that's left after extraction.
A small `boot()` function calls every feature module's init function
in dependency order. The IIFE wrapper exposes `window._trackBootObserver`
so feature modules can attach MutationObservers to the same beforeunload
cleanup pool.

Exports a handful of `window.<name>` functions that Python calls via
`evaluate_js(...)` — but the heavy ones (renderSubsTable,
renderQueues, renderWatchView, renderChannelGrid,
renderVideoGrid, _onRetranscribeComplete, etc.) now live in the
extracted modules.

### `logs.js`
After extraction, focused entirely on log rendering — the Python →
JS log pipe. Owns:
- `window._logBatch(payload)` — entry point Python pushes log
  segments into. Inserts log lines into the main log and mini-logs.
- `window.appendMainLog`, `window.renderActivityLog`,
  `window.renderMainLog` — direct-render entry points.
- `_inplaceKind` — the in-place-replace key resolver, which lets
  consecutive lines tagged `dlrow_5` replace each other in the same
  DOM position (so a Downloading row turns into a ✓ done row at the
  same spot, instead of stacking).

### Frontend modules (extracted from app.js + logs.js)

Each file is a self-contained IIFE that publishes its public surface
through `window.<name>`. The order in `index.html` matters because
later modules read earlier modules' globals.

**Foundation (loaded first):**
- `util.js` — `escapeHtml`, `escapeAttr`, `_formatTs`,
  `onceIdempotent`; namespaced as `YT.util.*`.
- `bridge.js` — `window.pywebview.api` shim + `bridgeCall(method,
  ...)` helper, readiness, admission/result handling, and queued catalog
  reads with per-screen stale-response guards and loading state.
- `eventState.js` — stable named-topic `publish` / `subscribe` / `snapshot`
  owner for shared bridge-pushed state. `window.setQueueState` publishes the
  `queue-state` topic instead of being repeatedly wrapped by consumers.
- `browseState.js` — declares `window._browseState` early so
  extracted modules close over the same object.

**Shell + chrome:**
- `chrome.js` — header strip, tab buttons, view switcher.
- `shortcuts.js` — global keyboard shortcuts.
- `navigationHistory.js` — Browse navigation history and back/forward handling.
- `commandPalette.js` — searchable application commands and destinations.
- `statusBar.js` — current Sync/Processing/index state, errors, and YouTube traffic limits.
- `queueBlink.js` — pause/resume button + queue badge state machine.
  Subscribes to `queue-state` and `queue-payload` independently of the status
  bar.
- `dropdown.js` — custom select widget used in toolbars.
- `contextMenu.js` — generic right-click menu used everywhere.
- `logContextMenu.js` — log-line right-click (copy / open URL / etc).
- `toasts.js` — `window._showToast(text, kind)`.
- `modals.js` — `askConfirm`, `askDanger`, `askQuestion`,
  `askChoice`, `askTextInput`, including multiline note entry.
- `uxPolish.js` — shared tooltip and focus behavior.
- `onboarding.js` — first-run setup state, validation, and transitions.

**Rendering modules (the heavy ones):**
- `queueRender.js` — Sync / GPU task popover row builder.
  Drag-reorder, right-click skip/cancel, verb-color tagging.
  Publishes `renderQueues`, `_queueStateSnapshot`.
- `queuePopovers.js` — open/close behavior of the popover containers
  themselves (anchor, outside-click close, Escape).
- `tables.js` — Subs channel table.
  Publishes `renderSubsTable`, `_applySubsFilter`,
  `_applySubsAvgVisibility`.
- `browseGrids.js` — Channel grid (Browse landing) + Video grid
  (inside a channel) with year/month grouping and lazy-load batching.
  Publishes `renderChannelGrid`, `renderVideoGrid`, `_buildVideoCard`
  (also reused by the archive-wide Videos and Manual grids).
- `watchView.js` — Embedded video player + transcript karaoke +
  timed DOM caption overlay + metadata drawer.
  Publishes `renderWatchView`, `loadWatchMetadataDrawer`,
  `_onRetranscribeComplete`, `setCaptionPref`.
  Playback and metadata can become usable while transcript loading continues;
  late transcript responses are guarded against replacing a newer video.
  The YT-style overlay reveals timed words on the bottom row and smoothly rolls
  completed lines through a fixed two-row viewport. Its measured line layout
  is cached and rebuilt when the caption font or player dimensions change.
  X-small through Large scale text and spacing with the player in all modes,
  including paused playback and window fullscreen; hidden geometry retains
  the last usable scale. YT Style is the startup and Off-to-on default;
  explicitly selected word modes remain active while the overlay stays on.

**Per-feature controllers (one file per UI feature):**
- `downloadUrl.js`, `downloadDragDrop.js` — download URL bar +
  drag-and-drop ingestion.
- `clearButton.js`, `syncSubbed.js` — log clearing and the primary Sync Subbed action.
- `editChannel.js` — shared Browse dialog/Dense Subs Add/Edit Channel controller,
  asynchronous defaults, dirty tracking, and post-add single-channel sync prompt.
  Add/reset defaults are Auto-metadata on, Auto-transcribe on, Compress off;
  saved defaults and existing-channel values take precedence when loaded.
- `removeChannel.js`, `queuePending.js`, `refreshSizes.js` — channel removal,
  pending-transcription queuing, and saved-size refresh controls.
- `autoSync.js`, `liveDrawer.js` — autorun controls + livestreams
  drawer.
- `columnSort.js`, `columnWidth.js` — Subs table column sort + resize.
- `browseContextMenus.js` — right-click menus on Browse cards.
- `browseView.js`, `browseContent.js`, `browseSearch.js`, `videosView.js` —
  Browse-tab view switching, content rendering, search, and the archive-wide
  Videos grid. Search owns the explicit Play/Open in Watch action and the
  visible date filter carried over from Graph.
- `manualView.js`, `manualReview.js` — Manual Downloads listing/filtering and
  review of ambiguous video-ID candidates.
- `bookmarks.js`, `watchActions.js` — bookmark listing/filtering, pending-save
  feedback, note editing, and Watch actions.
- `trashView.js` — Trash filtering, selection, restore, and permanent-delete workflows.
- `graphTab.js` — Chart.js word-frequency graphs, Search drill-down, and CSV
  export with units, channel scope, normalization, and bucket context.
- `settingsTab.js`, `settingsInfra.js` — preferences, Settings/Health navigation,
  and additional archive folders.
- `healthOverview.js` — read-only status cards and links to the relevant Health tools.
- `indexControls.js` — Health > Library catalog statistics and search-index controls.
  The catalog video count comes from `get_index_db_stats`; saved scan totals
  have separate labels and explicit current-channel coverage.
- `metadataTab.js` — Health > Library channel-information table, filtering,
  sorting, missing-ID visibility, and metadata/thumbnail actions.
- `scanArchive.js` — archive rescan progress and completion feedback.
- `diagnosticsDialog.js` — diagnostic output and cancellable integrity-scan preview.
- `driftScanDialog.js`, `compressDryRunDialog.js`, `repairCaptionsDialog.js`,
  `punctRestoreDialog.js`, `provenanceDialog.js` — maintenance previews and
  their progress/action dialogs.
- `aboutDialog.js`, `manualTranscribe.js`, `autorunHistory.js`, `logMode.js` —
  About, manual transcription, activity history, and log presentation controls.
- `activityLogVis.js`, `seedLogs.js`, `missingFolders.js` —
  miscellaneous helpers.
- `appDialogs.js`, `redownloadSampleModal.js` — modal dialogs.
- `smallInits.js` — last-full-sync ticker, Subs filter, splitter persistence,
  and archive-rescan completion handling.

### `vendor/chart.umd.min.js`
Vendored Chart.js library. Renders the bar / line charts in the
Browse > Graph view. Third-party, do not edit.

---

## Tests and release tooling

### `tests/`

Python regression modules cover the backend by domain and change family,
including normalized catalog identity, integrity preview, queue/config
repositories, download/transcription boundaries, and durable sidecar storage.
The Windows gate runs each Python test module in a fresh interpreter with
disposable app-data directories so process-lifetime state cannot leak between
unrelated modules.

Frontend tests have two layers:

- `tests/test_frontend*.js` — fast Node regression tests for isolated modules.
- `tests/frontend/browser/` — Playwright tests that load the real generated
  `web/index.html` with a deterministic `window.pywebview.api` stub. They cover
  modal safety, startup bridge timing, exact queue identity, stale async Watch
  responses, hidden-player shortcuts, backend error handling, and isolated
  event-state subscribers. Health coverage tests distinguish full catalog
  counts from partial saved scans; library-polish tests exercise filters,
  explicit playback, bookmark feedback/notes, and Graph export context.

Useful focused entry points include `test_health_archive_count_coverage.py`,
`test_startup_disk_count_recovery.py`, `test_physical_ui_library_reads.py`,
`test_physical_ui_bookmark_writes.py`, and `test_ui_polish_sync_scope.py`.
The browser fixture in `tests/frontend/browser/fixtures.js` supplies synthetic
bridge responses; browser assertions do not by themselves prove real-archive
performance or successful external downloads.

`tests/release/test_release_guardrails.py` verifies the guardrails themselves:
dependency locks, generated HTML, bridge scanning, privacy scanning, version
ownership, CI stages, x64 PE parsing, and packaged notices.

### `scripts/`

- `check.ps1` — authoritative Windows gate and clean verified build
- `lock_dependencies.ps1` — validates or intentionally refreshes lock files
- `import_check.py` — compiles backend code and `main.py`, then imports backend
  modules and runtime dependencies in disposable app-data directories;
  `main.py` and the Whisper/punctuation worker entry points are not imported
- `check_generated_html.py` — fails when generated `index.html` is stale
- `check_bridge_contract.py` — reports frontend bridge calls with no Python API
- `repository_scan.py` — blocks known secret and publication-privacy patterns
- `verify_build.py` — verifies x64 PE structure, version resources, and required
  files inside the PyInstaller executable
- `smoke.py` — older focused smoke helper; it can regenerate a stale HTML
  artifact. The full Windows gate remains the build/validation entry point.
- `check.sh` — Git Bash compatibility wrapper that delegates to `check.ps1`;
  use the PowerShell entry point directly for the isolated workflow.

The CI workflow in `.github/workflows/quality.yml` invokes
`scripts/check.ps1 -Bootstrap -RequireCleanTree`, so local and hosted release
checks use the same implementation.

---

## How library counts differ

- Health's **Search index** card (`index.summary`) and **Videos in catalog**
  (`archive_scan.index_db_stats`) count available logical videos across the
  full catalog. Multiple physical copies are represented separately in
  `media_files` and do not multiply that logical count.
- **Saved channel scan** comes from disk-cache entries for current
  subscriptions. Its scanned/total-channel coverage describes whether those
  subtotals cover every current channel; it does not cover every possible
  additional archive or manual-download folder.
- A direct `.mp4` file count has a different scope: it counts physical MP4s
  under the chosen folder, excludes other supported formats, and includes
  duplicate copies. Compare both scope and units before treating differing
  values as missing media.
- `indexControls.js` keeps database values loading until the database reply,
  displays unavailable values on errors, and labels saved scan totals
  independently. `healthOverview.js` flags incomplete saved scan coverage.

## How a sync actually works (end-to-end)

Helpful to trace:

1. The user clicks **Sync Subbed**. `web/syncSubbed.js` handles the action
   through the bridge's `sync_start_all` endpoint.
2. `SyncMixin.sync_start_all` checks admission and protects worker startup
   against concurrent requests. A full-library request enqueues persistent
   download tasks for current subscriptions. With Auto off, the request can
   stage the queue for a later Start instead of immediately running it.
3. The supervised sync worker calls `backend.sync.sync_all(...)`, whose
   implementation in `sync/sync_all.py` drains the persistent Sync queue.
   This queue can contain downloads and other kinds of channel work.
4. Download tasks resolve their current channel record and call
   `sync/core.py:sync_channel`. Queued single-channel requests use the same
   persistence and worker lifecycle without claiming a full-library run.
5. `sync_channel` checks channel identity, applies channel options, and
   launches yt-dlp through shared process/traffic controls. The stored channel
   root and the streams pass support archived livestreams as well as uploads.
6. As yt-dlp writes lines to stdout, `sync_channel` parses each one:
   `[youtube] VIDID:` → track current video id;
   `[download] Destination:` → emit "Downloading <title>" log row;
   `[download] 50%` → update that row in place;
   `[Merger] Merging formats into "X.mp4"` → capture final path;
   `DLTRACK:::...` (a custom `--print` template we inject) → identify
   the final output. `download_commit.commit_download` verifies that the file
   is durable and performs one catalog registration before the UI reports a
   successful completion.
7. When enabled, per-video metadata work refreshes saved information and
   writes sidecars through the shared durable store. Shared YouTube traffic
   and session controls apply to metadata requests too.
8. When transcription is enabled, `TranscribeManager` can commit already-saved,
   already-punctuated captions inline after persisting a recovery marker.
   Work requiring models, failed/unfinished caption commits, or unfinished
   follow-up compression uses the ordinary Processing queue/recovery path.
   Queued transcription normally tries captions before the Python 3.11 Whisper
   worker and returns an explicit outcome for completion/retry policy.
9. Download and processing progress update a consolidated activity row;
   `activity_history.py` preserves durable activity records independently
   of whole-config saves.
10. When the channel finishes, the consolidated row is finalized,
    optional channel-art refresh runs, optional .vtt cleanup runs,
    and config is updated with `last_sync` timestamp.
11. Each full-library task records its outcome against the saved batch ledger.
    Only successful completion of that batch advances **Last Full Sync**.
    Cancelled, failed, or removed tasks and single-channel runs cannot present
    themselves as a completed full pass. The autorun scheduler tracks its next
    due time and any workload that postpones it.

Every step above writes log segments through the `LogStreamer`, which
batches them and pushes them into JS via
`window._logBatch(...)`, which then inserts them into the main log
and the mini-logs.

## How a Whisper transcription works (end-to-end)

1. A finished download requests transcription for an auto-transcribe channel,
   or the user requests it from Browse.
2. `TranscribeManager` first persists recovery state. Eligible saved,
   already-punctuated captions can finish inline; other work is coordinated
   with the shared persistent `QueueState` GPU lane and current/recovery records.
3. A queued job normally tries saved/fetched captions through
   `_try_auto_captions`. Parsing VTT is only preparation: success requires
   durable transcript/JSONL/index commits and cleanup of recovery state.
   Cancellation and partial/failed commits preserve recovery rather than
   silently continuing into another append. Explicit Whisper re-transcription
   skips this caption path.
4. When captions are unavailable or Whisper was explicitly requested, the
   manager spawns (or reuses) the Python 3.11 subprocess running `faster-whisper`.
   The subprocess receives the media path in a JSON message and transcribes it.
5. Whisper returns segments with word-level timestamps. The worker
   caps long segments (see `whisper_worker.py`)
   so the karaoke viewer stays usable.
6. When punctuation is enabled, text is sent to the `punct_worker.py`
   subprocess and aligned back to timed segments.
7. The manager uses `transcribe/paths.py` and `transcribe_files.py` to write
   transcript text and its hidden JSONL sidecar. Subscribed-channel paths can
   aggregate multiple videos; manual downloads use per-video transcript paths.
   Validated atomic commits and reconciliation markers protect multi-store updates.
8. It registers / updates the video and ingests transcript segments in the
   SQLite index, which immediately makes the transcript searchable in the UI.
9. Throughout, progress updates flow back to the UI via the log
   stream so the user sees percent-complete inline in the main log.

---

## Reading order

To learn the codebase, read in this order:

1. **This document**
2. **`docs/ARCHITECTURE.md`** — service ownership, persistence, and lifecycle boundaries.
3. **`main.py`** and **`backend/api_mixins/`** — service wiring and the
   JS-callable feature surface.
4. **`backend/sync/core.py`** — the heart of the app. `sync_channel`
   builds the yt-dlp command, loops over stdout, handles each line
   type. (Submodules under `sync/` are mostly extracted helpers — read
   them only when their concern matters.)
5. **`backend/index.py`** + **`backend/catalog_repository.py`** — the database
   underneath user-visible Browse/Search and the logical-video/physical-media
   projection.
6. **`backend/services/`** — persistence, lifecycle, lease, and restore
   ownership boundaries.
7. **`web/index.template.html`**, **`web/partials/`**, **`web/bridge.js`**,
   **`web/eventState.js`**, and **`web/app.js`** — page sources, async bridge
   state, shared events, and boot order.
8. **`backend/transcribe/core.py`** + **`backend/whisper_worker.py`**
   — the transcription pipeline, only if you need to touch it.
9. **`scripts/check.ps1`** — the executable definition of done for a change.

