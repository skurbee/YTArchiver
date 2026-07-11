# YTArchiver Architecture

This document describes how YTArchiver fits together. For "how do I add a
feature", see [`CONTRIBUTING.md`](CONTRIBUTING.md). For "how do I
build the exe", see [`BUILD.md`](BUILD.md). For the JS API surface
specifically, see [`../backend/api_mixins/README.md`](../backend/api_mixins/README.md).
For the frontend, see [`../web/README.md`](../web/README.md).

## High-level overview

YTArchiver is a desktop app that:
1. Downloads videos from subscribed YouTube channels via `yt-dlp`.
2. Transcribes them via local Whisper (GPU, CUDA).
3. Restores punctuation via a separate transformer model.
4. Indexes everything into SQLite + FTS5 for full-text search.
5. Provides a pywebview UI for browsing, searching, and managing the archive.

Single user, Windows desktop only, no cloud sync. The archive root
is user-configurable (`output_dir` in config) and the app makes no
assumption about the drive layout beyond "it's a writeable local
filesystem that supports atomic `os.replace` for same-directory
renames" — see "DrivePool assumption" below.

## Process model

One process. Multiple threads:

```
Main thread (pywebview event loop)
│
├── JS bridge worker pool (pywebview-managed)
│   └── invokes Api methods on demand
│
├── Sync worker thread (Api._sync_thread)
│   └── runs sync.sync_all → sync.sync_channel per channel
│       └── spawns yt-dlp subprocess(es) per channel
│
├── GPU worker thread (TranscribeManager._worker_thread)
│   └── manages persistent Whisper subprocess
│       (Python 3.11 subprocess, lives across many videos)
│
├── Punctuation worker thread (PunctuationManager._worker)
│   └── manages persistent punctuation subprocess
│
├── Autorun scheduler thread (AutorunScheduler._thread)
│   └── triggers sync_all on cadence
│
├── Tray thread (TrayController._thread)
│   └── pystray icon + menu
│
├── HTTP servers (one thread each)
│   ├── cmd_server (port 9855) — ArchivePlayer integration
│   └── local_fileserver (random port) — video/thumb serving
│
└── ad-hoc workers
    └── redownload, compress batch, drift_scan, etc.
```

All child subprocesses (yt-dlp, ffmpeg, ffprobe, whisper, punct) are
tracked by `ProcessRegistry` (`backend/process_runner.py`) added in
Patch 3, so shutdown cleanly kills them via `registry.kill_all()`
without needing psutil child-scanning.

## State persistence

| What | Where | Format |
|------|-------|--------|
| Config | `%APPDATA%\YTArchiver\ytarchiver_config.json` | JSON |
| Queue state | `%APPDATA%\YTArchiver\ytarchiver_queue.json` | JSON (debounced 2s) |
| Index | `%APPDATA%\YTArchiver\transcription_index.db` | SQLite + FTS5 |
| Per-video metadata | `<channel>\.<channel> Metadata.jsonl` | JSONL (hidden) |
| Per-segment transcripts | `<channel>\<year>\<month>\.<channel> Transcript.jsonl` | JSONL (hidden) |
| Aggregated transcripts | `<channel>\<year>\<month>\<channel> Transcript.txt` | Plain text |
| Thumbnails | `<channel>\.Thumbnails\<title> [<vid>].jpg` | JPEG (hidden) |
| Auth token | `%APPDATA%\YTArchiver\cmd_token` | Random URL-safe token |
| Config backups | `%APPDATA%\YTArchiver\backups\config_YYYY-MM-DD_HHMMSS.json` | JSON |
| Window state | inside `ytarchiver_config.json` (`window_state` key) | JSON |
| Channel cache | `%APPDATA%\YTArchiver\ytarchiver_channel_ids.json` | JSON |
| Provenance ledger | `%APPDATA%\YTArchiver\provenance_ledger.jsonl` | JSONL (files already tagged by Embed File Tags) |
| Archive info folder | `<archive root>\YTArchiver Info\` | ABOUT txt + exe copy + scheduled backup ZIPs |

## Sync pipeline (per video)

```
yt-dlp downloads .mp4 + .info.json
     │
     ▼
sync.sync_channel parses DLTRACK lines from yt-dlp stdout
     │  emits "Downloaded" log row, registers download_ts
     │
     ▼
metadata.fetch_single_video_metadata (async via _meta_exec)
     │  fetches views/likes/comments/thumbnail
     │
     ▼
transcribe.TranscribeManager.enqueue
     │  jobs queued for the persistent Whisper subprocess
     │
     ▼
whisper_worker.py: Whisper transcribes audio → JSONL segments
     │
     ▼
transcribe._transcribe_one: writes to .txt + atomic .jsonl
     │  Patch 1 (v66.5): writes are now atomic via .tmp + replace
     │
     ▼
PunctuationManager.punctuate (Python 3.11 subprocess)
     │  restores punctuation on the raw transcript
     │
     ▼
index.register_video + index.ingest_jsonl
     │  populates videos table + FTS5 segments
     │
     ▼
Browse/Search now finds the video.
```

## JS ↔ Python bridge

- **Pull (JS → Python)**: `pywebview.api.<method>(...)` invokes a method
  on `Api` (in `main.py`) or one of its mixins (`backend/api_mixins/`).
- **Push (Python → JS)**: `self._window.evaluate_js("window.<funcName>(...)")`.

The Python-side bridge runs on a pool of worker threads. Long-running
handlers (file walks, ffprobe, yt-dlp probes) must offload to a
background thread or the UI freezes. See [`../backend/api_mixins/README.md`](../backend/api_mixins/README.md)
"Threading" section.

## Notable design decisions

- **Generated frontend shell**: `web/index.html` is assembled from
  `web/index.template.html` plus `web/partials/*.html` by
  `backend/html_assembler.py`. Edit the template/partials first, then
  regenerate. Browse > Videos is a grid-only, lazy-loaded archive view
  owned by `web/videosView.js`.

- **Atomic file writes**: every JSONL/config write goes through
  `.tmp` + `fsync` + `os.replace`. A crash mid-write never corrupts the
  destination. Added incrementally as bugs surfaced.

- **Single-file aggregated transcripts**: per-channel `.txt` is the
  user-facing artifact (greppable). The hidden `.jsonl` is the
  machine-readable per-segment store. Both must stay in sync — see
  `_replace_jsonl_entry` / `_replace_txt_entry` for the retranscribe
  surgical-swap pattern.

- **Persistent worker subprocesses**: Whisper and the punctuation model
  each take 5-30s to load. Holding them open across many videos amortizes
  that cost. The worker protocol is JSON-line over stdin/stdout — see
  `backend/whisper_worker.py`.

- **In-line metadata fetch on download**: instead of a separate sweep
  pass, `sync.sync_channel` spawns a single-worker `ThreadPoolExecutor`
  that fetches metadata for each downloaded video while the next
  download is in flight. Hides latency.

- **Same-filesystem atomicity**: `.tmp + os.replace` writes assume the
  destination directory and the `.tmp` file live on the same filesystem.
  This is the standard Unix-rename pattern and holds for any
  single-volume archive root (NTFS, ext4, APFS) and for pooled-drive
  filesystems that proxy renames transparently (StableBit DrivePool,
  Storage Spaces, etc.).

## Where things might surprise you

- `backend/api_mixins/_shared.py` does `import *` of stdlib + backend
  modules. This isn't a layering trick — it's a "global namespace" for
  every mixin file. See [`../backend/api_mixins/README.md`](../backend/api_mixins/README.md).

- Some functions exist in two places (e.g. `_norm_title` in
  `transcribe.py` AND `repair_captions.py` AND `metadata.py`). They
  have subtly different semantics by design. Patch 1 created
  `text_utils.normalize_title` as the canonical version; future patches
  will collapse the rest.

- `print()` calls in some modules go to a dropped stdout in PyInstaller
  builds. Patch 4 (v66.8) replaced the load-bearing ones with `_log.*`;
  any remaining ones are deliberate (boot-time before logger is up).

- `swallowed (...): {e}` log lines (DEBUG level) are intentional — see
  `backend/log.py:swallow()`. Means "this exception was caught and the
  surrounding code can continue without it." Verbose-mode only.

## Patch history (high level)

| Patch | Theme |
|-------|-------|
| 1 | Critical bug fixes (data integrity, security, deadlocks) |
| 2 | Helper consolidation (`text_utils`, `subprocess_util`, `fs_search`) |
| 3 | `ProcessRegistry` + `YtDlpRunner` (centralized subprocess lifecycle) |
| 4 | `config_transaction()` + error-handling discipline |
| 5 | `metadata.py` decomposition (`metadata/io.py` extracted) |
| 6 | `transcribe.py` decomposition (`transcribe/paths.py` extracted) |
| 7 | `ytarchiver_config.py` decomposition (`view_format.py` extracted) |
| 8 | `api_mixins/` contract documentation (`api_mixins/README.md`) |
| 9 | Frontend documentation (`web/README.md`) |
| 10 | Contributor docs (this file, `CONTRIBUTING.md`, `BUILD.md`) |
| 11–13 | `web/app.js` decomposition into ~40 single-concern modules |
| 14 | `sync/core.py` split — `sync_all` + `sync_helpers` extracted |
| 15 | `indexControls.js` split — `metadataTab.js`, `settingsInfra.js` extracted |
| 16 | `transcribe/core.py` split — `helpers.py`, `punct_manager.py` extracted |
| 17 | `index.py` split — `index_search.py`, `index_graph.py` extracted |
| 18 | `styles.css` split into 6 themed sheets |
| 19 | `index.html` split into template + 7 partials, assembled at boot |
| 20 | `index.py` second split — `index_bookmarks.py`, `index_maintenance.py` |
| 21 | `styles-browse.css` split — `styles-browse-grids.css`, `styles-watch.css` |
| 22 | `metadata/refresh.py` split per refresh-kind (views / comments / fetch) |
| 23 | `styles-settings.css` split — `styles-download-controls.css` extracted |
| 24 | `settingsTab.js` split — 4 dialog modules (drift / compress / repair / punct-restore) |

Each patch was designed to be self-contained and leave the app in a
shippable state.
