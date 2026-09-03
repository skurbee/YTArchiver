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
| Sync/GPU queues | `QueueState` + `QueueRepository` | `ytarchiver_queue.json` and `_resuming.json` |
| Catalog and transcript index | `index.py` + `catalog_repository.py` | `transcription_index.db` (SQLite + FTS5) |
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

## Normalized catalog

The original `videos` table is retained as the rollback contract. Patch 5
adds a normalized projection:

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

Patch 4 and earlier executables can therefore continue to read and write the
legacy table. A later Patch 5 open detects those writes, rebuilds the affected
normalized rows, compares them, and only then resumes normalized reads.

FTS transcript segments remain in their existing tables. Normalized title and
FTS result queries select a logical record/canonical media path so multiple
physical copies do not duplicate user-visible results.

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

`backend/transcribe/job_execution.py` gives each transcription/compression
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
        `-- enqueue transcription work
                            |
                            v
                 explicit WorkerOutcome
                            |
                            +-- durable transcript/JSONL sidecars
                            `-- transcript segment ingest into SQLite/FTS5
```

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
