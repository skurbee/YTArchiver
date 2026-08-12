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
renames" — see "pooled-filesystem assumption" below.

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
tracked by `ProcessRegistry` (`backend/process_runner.py`), so shutdown
cleanly kills them via `registry.kill_all()` without needing psutil
child-scanning.

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
     │  sidecar writes use .tmp + replace for atomicity
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
  filesystems that proxy renames transparently (pooled storage,
  Storage Spaces, etc.).

## Where things might surprise you

- `backend/api_mixins/_shared.py` does `import *` of stdlib + backend
  modules. This isn't a layering trick — it's a "global namespace" for
  every mixin file. See [`../backend/api_mixins/README.md`](../backend/api_mixins/README.md).

- A few feature-specific title-normalization helpers remain because they have
  subtly different semantics. New shared behavior belongs in
  `text_utils.normalize_title`.

- `print()` calls in some modules go to a dropped stdout in PyInstaller
  builds. Runtime diagnostics should use `_log.*`; remaining prints are
  limited to boot-time code that runs before the logger is available.

- `swallowed (...): {e}` log lines (DEBUG level) are intentional — see
  `backend/log.py:swallow()`. Means "this exception was caught and the
  surrounding code can continue without it." Verbose-mode only.
