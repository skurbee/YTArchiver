# YTArchiver Architecture

This document describes the current system boundaries and the rules that keep
archive state recoverable. For feature-development guidance, see
[`CONTRIBUTING.md`](CONTRIBUTING.md). For the executable workflow, see
[`BUILD.md`](BUILD.md). The bridge and frontend have their own references in
[`../backend/api_mixins/README.md`](../backend/api_mixins/README.md) and
[`../web/README.md`](../web/README.md).

## High-level overview

YTArchiver is a single-user Windows desktop app. It downloads subscribed
YouTube channels with `yt-dlp`, transcribes media locally, stores portable
sidecars beside the archive, and indexes videos and transcript segments in
SQLite/FTS5. A plain JavaScript frontend runs inside pywebview and calls a
Python API object through the pywebview bridge.

The archive root is user-configurable. File commits assume that a staging file
created beside its target can be installed with a same-directory
`os.replace`.

## Process and ownership model

The UI and backend share one process, with several owned threads and child
processes:

```
Main thread (pywebview event loop)
|
+-- JS bridge worker pool (pywebview-managed)
|   `-- invokes Api mixin methods
|
+-- Sync owner
|   `-- sync_all -> sync_channel -> yt-dlp/ffmpeg child processes
|
+-- Transcription owner
|   `-- persistent Python 3.11 Whisper child process
|
+-- Punctuation owner
|   `-- persistent Python 3.11 punctuation child process
|
+-- Schedulers and UI helpers
|   +-- autorun scheduler
|   +-- tray controller
|   +-- local command server
|   `-- allowlisted local media server
|
`-- Managed maintenance work
    `-- redownload, compression, reorganization, repair, and scans
```

`backend/services/job_supervisor.py` is the common admission and lifecycle
registry for long-lived background owners. New work is registered before its
thread starts. Shutdown or restore closes admission, asks each owner to
checkpoint, waits with a bounded deadline, and force-stops only the exact
remaining owner. Child processes are tagged and tracked by
`ProcessRegistry` in `backend/process_runner.py`.

Mutating jobs also use the process-wide leases in
`backend/services/channel_leases.py`. A channel lease covers its stable URL
and folder aliases; an archive-wide lease excludes every channel mutation.
This prevents sync, reorganization, restore, and maintenance from changing
the same resources at once. Folder/config operations use durable journals in
`channel_transactions.py` and `restore_coordinator.py` so startup can finish
or roll back an interrupted multi-resource change.

## State and persistence owners

| State | Owner | Storage |
|---|---|---|
| Configuration | `ConfigRepository` via `AppServices` | `%APPDATA%\YTArchiver\ytarchiver_config.json` |
| Sync/GPU queues | `QueueState` + `QueueRepository` | `ytarchiver_queue.json` and `ytarchiver_queue_resuming.json` |
| Catalog and transcript index | `index.py` + `catalog_repository.py` | `transcription_index.db` (SQLite + FTS5) |
| Saved channel counts and sizes | `archive_scan.py` | `ytarchiver_disk_cache.json` |
| Bookmarks and notes | `index_bookmarks.py` | `bookmarks` table in the index database; separate resource in app-state backups |
| Metadata sidecars | metadata services + `sidecar_store.py` | hidden per-channel JSONL |
| Transcript sidecars | transcription services + `sidecar_store.py` | hidden per-segment JSONL and visible aggregate text |
| Thumbnails | thumbnail services | hidden `.Thumbnails` files |
| Activity and provenance state | their domain services + `sidecar_store.py` | JSON/JSONL under app data or the archive |
| Window state | configuration repository | `window_state` inside configuration |

`ConfigRepository` owns load, replace, and serialized read-modify-write
operations. `QueueRepository` owns parsing, atomic main/resuming commits, and
corruption preservation; malformed queue files are moved aside rather than
silently overwritten. Domain code should ask those repositories for state
instead of opening the persistence files itself.

### Queue recovery and completion

Pending lists and active tasks are separate: `current_sync` and `current_gpu`
own running work, while the resuming records preserve that work across an
interruption. Stable `task_id` values identify the same task across queue,
worker, and recovery transitions. Restoring a queue does not by itself start
its worker. Once recovered work is durably returned to its lane, its old
resuming slot must be cleared so the next launch cannot resurrect it.

A full-library sync also persists an explicit checklist of required task IDs
in `full_sync_batch`. Only successful completion of that whole checklist,
followed by durable clearing of its current/recovery slots, can publish the
full-sync completion time. An empty queue, a single-channel sync, cancelled
work, or a removed task cannot stand in for a completed full-library sync.
Pause/defer retains the task's identity and its outstanding checklist entry.
These transitions live in [`queues.py`](../backend/queues.py); the sync
service and [`sync_mixin.py`](../backend/api_mixins/sync_mixin.py) consume
their results.

## Startup and saved-count recovery

The frontend validates the complete runtime installation status before opening
first-time setup. Error objects and incomplete bridge replies stay on the startup
retry path. Failed seed steps can retry after `pywebviewready`, including when
that event arrives during the failing attempt; successful recovery clears the
startup-data and connection warnings while retaining unrelated boot issues.

[`main.py`](../main.py) runs local startup stages sequentially under one
supervised owner. Initial checks make the UI ready to accept work; slower
archive scans continue in the background:

1. Refresh saved channel counts when the scan is stale, has never completed,
   or has missing, malformed, or old-format records for current subscriptions.
2. Sweep configured channel folders and additional archive roots for catalog
   and transcript updates, yielding to foreground Browse, sync, and processing
   work.
3. Retry the count scan once after the sweep if foreground work interrupted
   its first attempt. Keep the startup progress indicator alive through this
   retry, then run subscriber-count backfill.

A cancelled/deferred count scan does not publish its partial walk. Successful
counts are merged into the latest cache, preserving newer per-channel results
and subscriber metadata. The global freshness timestamp advances only after
publication succeeds and coverage of the current subscriptions is complete.
The scan's age alone is never proof of completeness. Auto-sync and trash
retention receive separate readiness signals from startup checks and indexing.

### Count scopes

Health exposes several related measurements; they must not be substituted for
one another:

| Measurement | Source and scope |
|---|---|
| Videos in catalog | `archive_scan.index_db_stats()` counts available logical videos across the catalog, selecting one canonical media copy per identity. |
| Saved channel scan | `archive_scan.index_summary()` totals usable saved records for current subscription URLs, excluding orphan cache entries. It reports `scan_complete`, `scanned_channels`, and `total_channels`. |
| Files in saved scan | Physical files in those channel records; multiple copies can represent one logical video. |
| Channel information coverage | `get_channel_metadata_status()` combines subscription metadata with catalog ID/transcript coverage restricted to current channel keys and the main archive. |

`cache_coverage()` reads saved records without scanning folders or querying
the catalog. A valid zero-video record is complete; an absent record is not a
zero-video channel. Duplicate subscription URLs are counted once for coverage
and saved totals. The frontend labels partial or unknown scan coverage rather
than presenting its subtotal as the full catalog. A failed catalog statistics
query returns an explicit error so an unavailable count is not displayed as
an empty library.

The saved scan groups known IDs within each channel and retains unknown files
by path. The full-catalog count groups identities across the library. Raw
`videos`, `media_files`, or `logical_videos` row counts can include physical
duplicates or records without available media and are not interchangeable
with either displayed video total.

## Normalized catalog

The original `videos` table is retained as the compatibility write contract.
The normalized projection adds:

```
legacy videos row(s)
        |
        | identity: YouTube ID, otherwise normalized path,
        | never a guessed title
        v
logical_videos 1 -------- * media_files
        |
        `-- one searchable logical record and a canonical available file
```

`backend/catalog_repository.py` owns this projection. A logical video models
content identity; `media_files` models each physical copy or path. This keeps
duplicate files from multiplying title or transcript search results, while
still preserving every known physical file.

Migration is additive and staged. Before the first normalized migration of a
non-empty legacy catalog, the app writes a separate backup of the legacy
`videos` table and verifies its row count, digest, and SQLite `quick_check`.
The migration then installs the new schema, copies/project rows, compares
deterministic projections, and only enables normalized reads after equivalence
passes. Compatibility triggers on `videos` mark identities dirty; they do not
attempt a second write model. `CatalogConnection.commit()` reconciles those
dirty identities in the same SQLite transaction as the legacy write.

Executables that use the legacy table can continue to read and write it. A
later normalized-catalog open detects those writes, rebuilds the affected
projection, compares it, and only then resumes normalized reads.

FTS transcript segments remain in their existing tables. Normalized title and
FTS result queries select a logical record/canonical media path so multiple
physical copies do not duplicate user-visible results.

Per-channel Browse uses a channel-scoped canonical selection. A valid copy
must remain visible within its own channel even when the library's primary
copy is elsewhere. The shared selection helpers are
`canonical_videos_cte_sql()` and `channel_videos_cte_sql()` in
[`index.py`](../backend/index.py).

## Sidecar durability

`backend/services/sidecar_store.py` is the common boundary for metadata,
transcript, provenance, caption-repair, and reorganization sidecars. It:

1. serializes writers per target path;
2. reads existing content without treating unreadable data as empty;
3. creates a unique staging file beside the target;
4. flushes and `fsync`s the staged bytes;
5. reads them back and validates their format;
6. installs them with `os.replace`; and
7. preserves the old target on any pre-replace failure.

Operations that must update more than one store first create a durable
reconciliation marker. Each committed store is checked off independently;
the marker is removed only when every store is committed. After interruption,
the marker records exactly which parts still need reconciliation instead of
leaving an ambiguous half-update.

## Download and transcription commit boundaries

`backend/sync/download_commit.py` is the single registration boundary for a
finished download. It rejects missing, empty, temporary, or partial media and
returns a `DownloadCommitResult` that records whether durable media and index
registration both succeeded. Existing-file, ID-less, and normal download
paths all use this contract, so a download is not announced as committed when
the final media or catalog registration failed.

`backend/transcribe/job_execution.py` gives each queued transcription/compression
operation an explicit `WorkerOutcome`. The executor converts exceptions and
invalid legacy return values into a known result; the owner then applies
cancel, defer, and shutdown signals after file-changing work has stopped.
Terminal removal and retry/pause decisions are made from that result rather
than from a collection of implicit sentinels.

## Sync pipeline (per video)

```
yt-dlp downloads and merges media
        |
        v
download_commit validates final non-empty media
        |
        +-- failure -> keep an explicit failed result; do not register
        |
        `-- success -> register once in the legacy catalog
                            |
                            `-- same transaction reconciles normalized catalog
        |
        +-- fetch metadata through durable sidecar writers
        +-- eligible saved captions -> inline durable sidecar/index commit
        `-- other transcription work -> queue processing
                            |
                            v
                 explicit WorkerOutcome
                            |
                            +-- durable transcript/JSONL sidecars
                            `-- transcript segment ingest into SQLite/FTS5
```

### Captions, Whisper, and Watch

[`transcribe/core.py`](../backend/transcribe/core.py) can finish already-saved,
already-punctuated native captions without loading a model or entering the
ordinary Processing queue. It first persists a recovery marker; failures or
unfinished follow-up compression promote the same durable task into normal
recovery rather than losing the work.

The queued transcription path normally tries saved/fetched YouTube captions
before Whisper. [`transcribe_vtt.py`](../backend/transcribe/transcribe_vtt.py)
distinguishes unavailable captions, cancellation, successful commits, and
partial/failed commits. A partial caption write requires recovery; it must
not silently fall through into a second transcript append. An explicit
Whisper re-transcription bypasses the caption fast path. Punctuation is an
optional shared worker, and transcript source tags distinguish Whisper, raw
YouTube captions, and captions processed for punctuation. `no_speech` is a
durable terminal classification, distinct from a transcribed video or a
pending retry.

Watch starts media and saved-detail loading independently of transcript
retrieval. A slow or unavailable transcript must not block playback. The
transcript response is checked against the current video before repainting;
its arrival does not reload the media or reset the requested seek position.
Caption/karaoke bindings are cleared when changing videos or showing loading
and error states. See [`browseContent.js`](../web/browseContent.js) and
[`watchView.js`](../web/watchView.js).

The on-video overlay offers one-word, three-word, and YT-style rolling modes.
Rolling captions retain the existing `default` preference value. A separate,
chronologically sorted word view and cached font-measured line layout reconstruct
the previous completed line and the current spoken prefix at any playback time.
The fixed two-row viewport clips a transient outgoing row during smooth rolls;
word arrivals do not restart the animation. Seeks and preference/layout changes
refresh the view immediately, and a speech gap beyond the short hold interval
clears the previous context. This changes presentation only, not saved transcripts.

The overlay's X-small, Small, Medium, and Large steps are 6, 13, 20, and 26
CSS pixels at a 640-by-360 player. Text, padding, row spacing, outlines, and
scroll distances use the smaller of the player width/640 and height/360 ratios.
Resize observation and media metadata/resize events update all modes while
paused as well as playing; hidden or zero-sized players retain their last usable
scale. The player box controls this scale independently of the application font
size and encoded video resolution. Size and background choices are restored.
Mode starts in YT Style at launch and on each Off-to-on transition, ignoring
previously saved modes. Explicit one/three-word choices remain active through
size changes, resizing, and video changes while enabled. Startup hydration does
not save fallback preferences over the stored settings, and a late response is
ignored after a user interacts with any caption control.

## Metadata and authored state

[`backend/metadata/`](../backend/metadata/) separates sidecar I/O, scans,
remote fetches, views/likes and comment refreshes, and thumbnail operations.
Reloading saved metadata status is different from contacting YouTube.
`get_channel_metadata_status(force=True)` recounts channel files and catalog
coverage, publishing refreshed disk counts; the UI's recount action also
requests fresh thumbnail coverage. Remote refresh actions use their own
queued operations and completion timestamps.

Bookmarks are authored state even though their table resides in the search
database. [`index_bookmarks.py`](../backend/index_bookmarks.py) uses the
admitted catalog writer and an explicit transaction with bounded lock/SQL
waits. Identical retries are deduplicated, and success is returned after
commit. The frontend keeps a pending save visible and suppresses overlapping
identical requests. Bookmark title/channel/note filtering happens before the
result limit, so an older matching bookmark is not hidden merely because it
is outside the first unfiltered page.

## App-state backups and restore

Manual export and scheduled backup share
[`auto_backup.build_backup_zip()`](../backend/auto_backup.py). They save app
state, not the downloaded media/transcript archive. Queue resources come
from one coherent `QueueState` snapshot rather than independent reads of the
main queue and its faster-moving resuming file. A versioned manifest records
creation time, resource sizes/hashes, and whether the index was included.

An included index uses SQLite's backup API for a consistent WAL-aware
snapshot. The index is omitted at or above the code-level size cap; it can
be rebuilt from archived material. Bookmarks and notes are
always exported separately as `ytarchiver_bookmarks.json`, including when the
large index is omitted. Scheduled backups run while the app is open, retain
the newest four scheduled ZIPs, and do not rotate arbitrary manual exports.

[`restore_coordinator.py`](../backend/services/restore_coordinator.py)
validates and stages an archive before replacing live state, quiesces owned
work, and uses its journal/rollback resources to recover interruption. If a
backup has bookmarks but no index, restore seeds the authored state into a
database whose catalog can be rebuilt. For an older backup containing neither,
restore preserves current bookmarks after quiescing instead of discarding
them or carrying a stale catalog into the restored configuration. A failure
to preserve those bookmarks aborts restore before live replacement.

## Read-only integrity preview

`backend/integrity_scan.py` is a diagnostic engine, not a repair engine. Its
caller must pass explicit archive, config, database, and queue paths. The
database is opened with SQLite's immutable read-only URI, and the scanner does
not create directories, replace files, or apply any proposed repair.

The returned report covers input readability, SQLite/FTS integrity,
transcript/index agreement, canonical catalog links, same-title collisions,
saved-media/download-archive agreement, queue and transcription recovery
records, folder overrides, activity history, and catalog migration state.
Every finding contains a proposed next action, but `preview_only` remains true.
Any future repair command must be a separate reviewed operation with a
verified backup taken first.

The interactive deep-check flow is a supervised background job exposed by
`integrity_scan_start`, `integrity_scan_state`, and `integrity_scan_cancel` in
[`diagnostics_mixin.py`](../backend/api_mixins/diagnostics_mixin.py). The UI
polls phase, progress, elapsed time, and the eventual report; cancellation is
cooperative between work items and during SQLite checks. Starting or polling
this diagnostic does not apply its proposed repairs. The synchronous
`integrity_scan_preview` endpoint remains available for compatibility.

## JS/Python bridge and frontend state

- **Request (JS -> Python):** `pywebview.api.<method>(...)` invokes `Api` or
  one of its mixins.
- **Push (Python -> JS):** backend code calls a stable `window.<function>`
  endpoint through `evaluate_js`/`BridgeEventBus`.
- **Shared frontend state:** `web/eventState.js` owns named topics through
  `publish`, `subscribe`, and `snapshot`.

`window.setQueueState` is one stable bridge endpoint. It publishes
`queue-state`; `queueRender.js` publishes `queue-payload`; queue indicators
and the status bar subscribe independently. A broken or removed subscriber
cannot replace the bridge callback or prevent another subscriber from
receiving the update. Log indicator state follows the same named-topic model.

Long-running bridge handlers must still offload work because pywebview invokes
Python methods on bridge worker threads. See the threading rules in
[`../backend/api_mixins/README.md`](../backend/api_mixins/README.md).

Catalog reads use frontend request generations to ignore stale results and
separate foreground Browse/Search work from diagnostic/background lanes.
Interactive search/transcript reads also have bounded backend lock/SQL waits;
the slow Health aggregates use a dedicated SQLite connection so they do not
hold the shared reader lock throughout the calculation. These mechanisms are
not a replacement for offloading a genuinely long-running operation.

## Local HTTP boundaries

The pywebview bridge remains the application's primary API. Two separate
HTTP helpers serve narrower purposes:

- [`local_fileserver.py`](../backend/local_fileserver.py) serves allowlisted
  archive media and thumbnails on a random loopback port. File requests need
  a per-run token; canonical path checks enforce allowed roots or explicitly
  allowed individual files, and an empty allowlist fails closed. Byte-range
  responses let the Watch video element seek without loading the whole file.
- [`cmd_server.py`](../backend/cmd_server.py) exposes a small companion-tool
  command registry. It defaults to loopback; non-loopback binding requires
  both the bind setting and an explicit LAN opt-in. Read-only status endpoints
  and authenticated mutations are distinct: POST commands require the
  installation token and are subject to a bounded request body.

Neither helper is a general replacement for the bridge or permission to
serve arbitrary filesystem content. Their shutdown hooks are part of app
cleanup, alongside managed workers and registered child processes.

## Verification and release guardrails

The Windows quality gate is `scripts/check.ps1`; CI runs the same gate from
`.github/workflows/quality.yml`. Toolchain versions are pinned in
`.python-version` and `.nvmrc`, and Python dependency locks are exact and
hash-verified.

The gate runs, in order:

1. dependency lock validation;
2. Ruff, compile, and import checks;
3. isolated Python test files with warnings-as-errors and coverage;
4. JavaScript syntax and Node regression tests;
5. Playwright behavior tests against the real assembled frontend with a
   deterministic pywebview bridge stub;
6. generated-HTML and JS/Python bridge-contract checks;
7. repository privacy/secret checks; and
8. a clean PyInstaller build plus x64 PE, version-resource, and packaged-data
   verification.

The gate fingerprints the source tree before and after running and fails if a
check unexpectedly changes it. See [`BUILD.md`](BUILD.md) for commands and
locked-environment bootstrap details.

## Notable design rules

- `web/index.html` is generated from `web/index.template.html` and
  `web/partials/*.html`. Edit the source template/partial, regenerate, and
  run the generated-HTML check.
- Title text is presentation data, not identity. Prefer a video ID; when one
  is unavailable, use a normalized media path or an explicit legacy row ID.
- New shared dependencies belong in a named repository/service and are
  injected through `AppServices`; the container itself should remain thin.
- Runtime diagnostics use the project logger. `print()` is reserved for the
  small amount of boot-time code that runs before logging is available.
- A few feature-specific title-normalization helpers intentionally have
  different semantics. New shared behavior belongs in
  `text_utils.normalize_title`.
