# YTArchiver — Project Map

A tour of every file in this repo, what it does, and the
important functions inside it.
---

## What YTArchiver is

YTArchiver is a desktop app that maintains a local video
archive of YouTube channels you subscribe to. You give a list of
channels; it periodically syncs each one, downloads any new videos
via `yt-dlp`, transcribes them with Whisper, fetches their metadata,
and stores everything in a structured folder tree on disk. The point
is to own a permanent, offline-accessible copy of content that might
disappear from YouTube.

A small SQLite index makes everything searchable across all channels,
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
There's no Flask, no HTTP, no two-process split — the UI literally
runs inside the Python process, so the bridge is just function calls.

The exe is built with PyInstaller (see `YTArchiver.spec`). Whisper
runs in its own subprocess on Python 3.11 because its CUDA wheels
don't exist for Python 3.13 (which is what the main app uses).

---

## Top-level files

### `main.py`  ·  ~7,500 lines
The entry point. Defines the `Api` class, which is the single object
exposed to JavaScript via `pywebview`. Every user-facing action the
UI can trigger — adding a channel, starting a sync, scrubbing through
a transcript, deleting a bookmark — is a method on this class.

Also handles app startup: single-instance mutex, config loading, tray
icon, log streamer setup, autorun scheduler, transcription manager,
window state restore, and signal handling for clean shutdown.

**Key things to find inside:**
- `class Api` — the JS bridge. Hundreds of methods, organized into
  sections by comment headers (Subs, Browse, Watch, Settings, etc).
- `APP_VERSION` constant near the top — gets bumped on every release.
- Bottom of file: `if __name__ == "__main__":` block — initializes
  everything in order, creates the pywebview window, starts the
  main loop.

### `YTArchiver.spec`
PyInstaller "recipe". Tells PyInstaller how to bundle `main.py` plus
every backend module, the `web/` folder, the icon, and the whisper
worker script into a single `dist/YTArchiver.exe`. Build with
`py -3.13 -m PyInstaller YTArchiver.spec`.

### `icon.ico`
The window icon and tray icon, used by both `pywebview` and `pystray`.
Bundled into the exe via the spec file.

---

## `backend/` — the Python package

Every server-side module the app uses at runtime. The presence of
`__init__.py` is what makes Python treat this folder as an importable
package (so `from backend.sync import sync_channel` works).

### `__init__.py`
Empty file in terms of code, but contains a docstring overview of
every module in the package and how they relate. **Read this first**
if you want a one-shot mental model of the backend.

### `archive_scan.py`  ·  filesystem reality check
Walks the channel folder tree on disk and produces per-channel video
counts, total sizes, and recency stats. The Subs and Browse tabs
render directly from these stats. Has an on-disk JSON cache so the
walk doesn't have to happen every time the UI repaints.

**Key functions:** `scan_channel_folder`, `scan_all_channels`,
`enrich_channels_with_stats`, `index_summary`, `archive_totals`.

### `autorun.py`  ·  recurring background sync
Schedules sync passes on a recurring interval (every X minutes). Also
owns the activity-log history that's written to `config.autorun_history`
and rendered in the Settings tab's history view.

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
A tiny HTTP server (Python `http.server`) bound to localhost that
external tools can hit to read app state without going through the
GUI. Endpoints return version, current sync job, etc.

**Key functions:** `start_server`, `stop_server`, `register_handler`.

### `compress.py`  ·  re-encode old downloads
Drives ffmpeg to re-encode older videos to AV1 / HEVC at lower
bitrates, replacing the original in place. Used by the per-channel
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
Compares each channel's files on disk against the SQLite index and
the per-channel `Transcript.txt`. Flags missing files, orphan
transcript entries, and FTS-search "phantoms" (rows that no longer
have a real file). Used by the maintenance pass in Settings.

**Key functions:** `scan_channel`, `apply_channel`,
`rebuild_fts_index`.

### `index.py`  ·  the SQLite database
THE central data store. Every downloaded video gets a row here with
its title, channel, upload date, duration, file path, transcription
state, and metadata-fetch state. Browse / Search / Recent / Graph
all read from this DB.

Also stores transcript SEGMENTS (one row per Whisper segment per
video) so full-text search can pinpoint matches inside transcripts.

**Key functions:** `register_video`, `mark_video_transcribed`,
`ingest_jsonl` (loads a transcript JSONL into the segment table),
`list_recent_videos`, `list_videos_for_channel`, `get_segments`,
`get_segment_context`, `search_video_titles`, `search_fts`,
`graph_word_frequency`, `graph_multi`, `bookmark_add`/`_list`/
`_remove`/`_update_note`, `bucket_totals`, `top_words`,
`summary`, `sweep_new_videos`.

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
serves the archive's .mp4 / .vtt / .txt files to the embedded video
player and transcript viewer.

**Key functions:** `set_allowed_roots`, `start_server`, `stop_server`,
`url_for`.

### `log_stream.py`  ·  Python → JS log pipe
Backend code writes log "segments" (a tuple of text + style tag) to
the `LogStreamer`, which batches them every ~60ms and pushes them
into JS via `window._logBatch(payload)`. This is the bus that the
Sync Log, mini-logs, and activity rows ride on.

**Key classes/functions:** `class LogStreamer` (the bus),
`emit`, `emit_text`, `emit_simple`, `emit_dim`, `emit_error`,
`emit_header`, `emit_activity`, `_line_is_verbose_only` (the
simple-mode filter that hides chatty output).

### `metadata.py`  ·  views / likes / comments refresh
yt-dlp metadata refresh pipeline for already-downloaded videos. Pulls
view counts, like counts, comments, and descriptions; writes a sidecar
`.txt` and updates the index DB. The Settings > Metadata tab drives
this.

**Key functions:** `fetch_single_video_metadata`,
`fetch_metadata_for_videos` (batch), `bulk_refresh_views_likes`,
`count_thumbnail_status_bulk`, `count_video_id_status_bulk`,
`sweep_missing_thumbnails`, `realign_misplaced_thumbnails`.

### `net.py`  ·  am I online?
Tiny TCP-connect probe (`probe_once`) and a background monitor that
sets a flag when the network goes down. Other modules call
`block_if_down` to pause work when there's no connectivity instead
of failing every download retry.

**Key functions:** `probe_once`, `start_monitor`, `block_if_down`.

### `punct_worker.py`  ·  punctuation restoration subprocess
Whisper outputs ALL CAPS or all-lowercase raw text with no commas /
periods. This subprocess runs a HuggingFace punctuation model that
reads in raw text and writes out punctuated + capitalized text.
Stays alive between transcribe jobs so the model only loads once.

**No top-level functions** — it's a standalone script that reads
JSON requests from stdin and writes responses to stdout. The
`PunctuationManager` class in `transcribe.py` is what manages it.

### `queues.py`  ·  Sync + GPU task queues
Two persistent queues backed by `ytarchiver_queue.json`: the Sync
queue (yt-dlp downloads) and the GPU queue (Whisper transcriptions +
ffmpeg compressions). The Sync Tasks / GPU Tasks popups in the
header render these.

**Key classes/functions:** `class QueueState` (the state machine —
load, save, enqueue, pop, remove, reorder, clear, current-running
tracking, pause flags, UI payload formatting).

### `redownload.py`  ·  fetch existing video at higher res
Right-click a video and pick "Redownload at 1080p" — this module
finds the existing file, identifies its current resolution via
ffprobe, and re-fetches via yt-dlp at the new target.

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

**Key functions:** `normalize_channel_url`, `validate_channel_url`,
`fetch_channel_display_name`, `add_channel`, `update_channel`,
`remove_channel`, `get_channel`, `list_channels`,
`ensure_videos_suffix`, `streams_url`.

### `sync.py`  ·  the central download path  ·  ~3,200 lines
THE single most important file in the backend. Wraps `yt-dlp` as a
subprocess for each channel sync, parses its stdout line by line,
emits log lines through the LogStreamer, and dispatches inline
metadata + transcribe jobs as each video completes.

**Key functions:**
- `sync_channel` — the giant central function. Walks one channel,
  spawns yt-dlp, parses every output line, manages the per-video
  Downloading-line lifecycle, handles cookie / livestream / archive-
  skip / Merger / DLTRACK events.
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
- `_record_recent_download` — appends to the Recent tab's list.
- `prefetch_channel_total`, `quick_check_new_uploads` — the fast
  "are there any new videos?" check without a full channel walk.

### `temp_cleanup.py`  ·  delete .part / .ytdl leftovers
On startup, sweep the channel tree for partial files left behind by
cancelled / crashed yt-dlp invocations.

**Key functions:** `is_partial_file`, `cleanup_folder`,
`startup_cleanup_temps`.

### `transcribe.py`  ·  Whisper manager  ·  ~3,000 lines
Owns the transcription pipeline. Two paths:
1. **Fast path:** if YouTube has captions, just download those via
   yt-dlp and parse the VTT — no Whisper needed.
2. **Slow path:** queue a Whisper job in the GPU subprocess (Python
   3.11 → faster-whisper → CUDA), then run punctuation restoration
   on the output, then write the per-video `.txt` and the merged
   channel `Transcript.txt`.

**Key classes/functions:**
- `class TranscribeManager` — the worker thread that consumes the
  GPU queue.
- `class PunctuationManager` — manages the `punct_worker.py`
  subprocess.
- `find_python311` — discovers the Whisper environment.
- `_try_auto_captions`, `_fetch_captions_via_ytdlp`, `_parse_vtt`
  — the fast-path.
- `_write_jsonl_entry`, `_write_transcript_entry` — append to the
  per-video JSONL (for word-timestamp data) and the channel-wide
  `Transcript.txt`.
- `_replace_jsonl_entry`, `_replace_txt_entry` — surgical replace
  for re-transcribe.

### `tray.py`  ·  Windows system-tray icon
pystray-driven tray icon with a context menu, animated spinner during
sync, badge overlay for pending tasks, and "On top" toggle.

**Key classes/functions:** `class TrayController` (start / stop / set
tooltip / set badge / start / stop spin / set autorun menu).

### `utils.py`  ·  shared low-level helpers
Common helpers reused across modules: subprocess env setup, byte
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

**No top-level functions** — it's a standalone script. Includes the
30-second segment-cap re-segmentation logic that makes the karaoke
transcript viewer behave on long monologues.

### `window_state.py`  ·  remember window geometry
Persists the pywebview window size, position, and last-active tab to
`%APPDATA%\YTArchiver\window_state.json` on close, restores on
launch.

**Key functions:** `load_window_state`, `save_window_state`,
`_sanitize_geometry`.

### `ytarchiver_config.py`  ·  the user-settings file
Reads / writes `%APPDATA%\YTArchiver\ytarchiver_config.json`, which
holds every user setting: archive root, subscribed channels, autosync
interval, log mode, recent downloads, etc. The single source of truth
for "what does this user have configured".

Also formats the data for UI consumption (channels-for-Subs-table,
recent-downloads-for-Recent-tab, autorun-history-for-Activity-log).

**Key functions:** `load_config`, `save_config`, `config_file_exists`,
`config_is_writable`, `backup_config_on_start`,
`channels_for_subs_ui`, `recent_for_ui`,
`autorun_history_entries_for_ui`, `append_pending_tx_id`,
`remove_pending_tx_id`.

---

## `web/` — the frontend

Plain HTML / CSS / JS — no React, no build step, no transpilation.
What's in source is what runs in the browser. The whole UI is
rendered inside the pywebview window using the embedded Edge WebView2
on Windows.

### `index.html`
The single page. Defines:
- Header strip (title + version)
- Tab row (Subs / Browse / Settings)
- Three tab panels — each is a full screen of UI
- Floating overlays (modals, context menu, drawers, popups)
- Script tags loading Chart.js, then `logs.js`, then `app.js`

The top-of-file comment in this file lists every section so a new dev
can navigate.

### `styles.css`  ·  ~3,000 lines
All visual styling. Dark theme. CSS variables at the top define the
color palette so theming is centralized. Sections are grouped by tab.

### `logs.js`
The log-rendering and log-streaming logic that's separate from the
rest of app.js for clarity. Owns:
- `window._logBatch(payload)` — entry point Python pushes log
  segments into. Inserts log lines into the main log and mini-logs.
- `window.appendMainLog`, `window.renderActivityLog`,
  `window.renderMainLog` — direct-render entry points.
- `_inplaceKind` — the in-place-replace key resolver, which lets
  consecutive lines tagged `dlrow_5` replace each other in the same
  DOM position (so a Downloading row turns into a ✓ done row at the
  same spot, instead of stacking).

### `app.js`  ·  ~10,800 lines
The single frontend script for everything else. Wrapped in an IIFE
so it doesn't leak globals. The top-of-file comment lists all 12
functional areas; key ones:
- Tab system + splitter
- Subs tab (channel list, add / edit / remove)
- Browse tab (channel grid, search, graphs, bookmarks, recent,
  the embedded watch view with karaoke transcript)
- Settings tab (per-channel + global toggles, metadata refresh)
- Queue popups + mini-logs
- The welcome / first-launch flow
- `seedLogs()` — startup data pull from the Python bridge

Exports a handful of `window.<name>` functions that Python calls via
`evaluate_js(...)` (renderSubsTable, renderActivityLog, etc).

### `vendor/chart.umd.min.js`
Vendored Chart.js library. Renders the bar / line charts in the
Browse > Graph view. Third-party, do not edit.

---

## How a sync actually works (end-to-end)

Helpful to trace:

1. User clicks the green **Sync Subbed** button in the Subs tab.
2. `app.js` calls `window.pywebview.api.start_sync_all()`.
3. `main.py`'s `Api.start_sync_all` spawns a background thread that
   calls `backend.sync.sync_all(...)`.
4. `sync_all` iterates over every channel and calls `sync_channel`
   for each one.
5. `sync_channel` builds a yt-dlp command and `Popen`s it.
6. As yt-dlp writes lines to stdout, `sync_channel` parses each one:
   `[youtube] VIDID:` → track current video id;
   `[download] Destination:` → emit "Downloading <title>" log row;
   `[download] 50%` → update that row in place;
   `[Merger] Merging formats into "X.mp4"` → capture final path;
   `DLTRACK:::...` (a custom `--print` template we inject) → confirm
   the video is fully merged. Replace the Downloading row with a
   "✓ <title>" done row. Submit an inline metadata task + transcribe
   task for this video.
7. Inline metadata task fires immediately via a single-worker
   ThreadPoolExecutor (so we don't hammer YouTube). It refreshes
   views / likes / comments and writes the metadata sidecar.
8. Transcribe task enqueues onto the GPU queue. The
   `TranscribeManager` worker picks it up, tries auto-captions first,
   falls back to Whisper on the 3.11 subprocess.
9. After every video, `sync_channel` writes a `[Dwnld]` row to the
   activity log (consolidated across the channel's videos so far).
10. When the channel finishes, the consolidated row is finalized,
    optional channel-art refresh runs, optional .vtt cleanup runs,
    and config is updated with `last_sync` timestamp.
11. When `sync_all` finishes ALL channels, the autorun scheduler
    decides when to fire again (or not, if autorun is off).

Every step above writes log segments through the `LogStreamer`, which
batches them every ~60ms and pushes them into JS via
`window._logBatch(...)`, which then inserts them into the main log
and the mini-logs.

## How a Whisper transcription works (end-to-end)

1. A finished download submits a transcribe task — either inline from
   `sync_channel` (auto-transcribe channels) or from a right-click
   menu in Browse.
2. The task enqueues onto `TranscribeManager`'s internal queue.
3. The worker thread pops the next task. If YouTube has captions for
   this video (`_try_auto_captions`), the fast path downloads the
   VTT, parses it, and we're done — no Whisper at all.
4. Otherwise, the worker spawns (or reuses) the Python 3.11 subprocess
   that runs `faster-whisper`. The subprocess receives the video
   path via a JSON message and runs whisper transcription on it.
5. Whisper returns segments with word-level timestamps. The worker
   re-segments anything longer than 30 seconds (see `whisper_worker.py`)
   so the karaoke viewer stays usable.
6. The raw text gets shipped to the `punct_worker.py` subprocess for
   punctuation + capitalization restoration.
7. The worker writes a per-video `.jsonl` sidecar (word-level
   timestamps) and appends an entry to the channel's merged
   `Transcript.txt`. It also registers / updates the video row in
   the SQLite index, which immediately makes the transcript
   searchable in the UI.
8. Throughout, progress updates flow back to the UI via the log
   stream so the user sees percent-complete inline in the main log.

---

## Reading order

to learn codebase, read in this order:

1. **This document**
2. **`backend/__init__.py`** — one-line summary of every backend
   module, all in one place.
3. **`main.py`** top section — the `Api` class is how the UI talks
   to everything else; skim its method names to see the surface.
4. **`backend/sync.py`** — the heart of the app. Understand its structure
   (build yt-dlp command, loop over stdout, handle each line type).
5. **`backend/index.py`** — the database underneath everything user-
   visible. Know the `register_video` / `list_videos_for_channel` /
   `search_fts` shape.
6. **`web/index.html`** + **`web/app.js`** header — the page
   structure + section map are at the top of each file.
7. **`backend/transcribe.py`** + **`backend/whisper_worker.py`** —
   the transcription pipeline, only if you need to touch it.

