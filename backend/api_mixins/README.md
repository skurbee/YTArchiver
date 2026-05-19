# api_mixins/ — JS-Callable API Surface

This package contains the methods that the pywebview JS frontend can call via
`pywebview.api.<method>(...)`. They're split across 22 mixin classes that
`main.py`'s `Api` class composes via multiple inheritance:

```python
class Api(InfoMixin, StartupMixin, WindowMixin, SettingsMixin, ..., BookmarkMixin):
    def __init__(self): ...
```

## Why mixins (and the caveat)

Originally one giant `Api` class in `main.py`. Mixins were extracted per-feature
so individual files stay browsable. The split is along **feature boundaries**,
not strict layers — every mixin is part of the same logical god-object that's
glued together by `main.py`.

That means mixins **read and write `self.<attr>` attributes that other mixins
own**. The contracts are implicit and listed below for reference. A clean
refactor would replace this with explicit service injection, but for now the
pattern is: write a method, attach to a mixin, document any new `self.<attr>`
dependencies here.

## Shared `self` attributes (the implicit contract)

Set by `main.py` `Api.__init__`. Every mixin can rely on these existing.

| Attribute              | Owner       | Read by                                 | Notes |
|------------------------|-------------|-----------------------------------------|-------|
| `self._config`         | main        | almost every mixin                      | Cached config dict; may be stale — see Patch 4 `config_transaction` for atomic RMW |
| `self._window`         | main        | every mixin that emits JS events        | pywebview window; `None` before `set_window` |
| `self._log_stream`     | main        | most mixins                             | `LogStreamer` for tagged log emit |
| `self._queues`         | main        | sync_mixin, queue_mixin, channel_mixin  | `QueueState` (in `backend/queues.py`) |
| `self._transcribe`     | main        | transcribe_mixin, channel_mixin, sync_mixin | `TranscribeManager` (in `backend/transcribe/core.py`, re-exported via `backend.transcribe`) |
| `self._sync_thread`    | main        | sync_mixin, channel_mixin               | Currently-running sync worker thread or None |
| `self._sync_cancel`    | main        | sync_mixin                              | `threading.Event` to cancel sync |
| `self._sync_pause`     | main        | sync_mixin                              | `threading.Event` |
| `self._sync_skip`      | main        | sync_mixin                              | `threading.Event` |
| `self._redwnl_pending` | main        | channel_mixin, sync_mixin               | List of pending redownload tasks |
| `self._redwnl_lock`    | main        | channel_mixin, sync_mixin               | Lock protecting `_redwnl_pending` |
| `self._autorun`        | main        | settings_mixin                          | `AutorunScheduler` |
| `self._disk_mon`       | main        | settings_mixin                          | `DiskErrorMonitor` |
| `self._tray`           | main        | tray-aware mixins                       | `TrayController` |

## `_shared.py` and the star import

Every mixin starts with `from ._shared import *`. That gives it access to
stdlib aliases (`os`, `json`, `threading`, etc.) and backend module aliases
(`sync_backend`, `metadata_backend`, etc.) without each file having to repeat
those imports.

The star import is a code smell — explicit imports per mixin would be cleaner.
That's a future refactor. For now, treat `_shared.py` as "the global namespace
this package operates in" and add new aliases there if multiple mixins need them.

## Adding a new method

1. Pick the mixin whose feature area matches your method (e.g. channel-related
   → `channel_mixin.py`; metadata-related → `metadata_mixin.py`).
2. Add the method to that class. It must be named consistently with the
   existing methods in the file.
3. If you need a new `self.<attr>` from `main.py`, add it to `Api.__init__`
   in `main.py` AND document it in the table above.
4. If your method blocks (`subprocess.run`, file I/O on a slow disk), offload
   to a background thread before returning — the JS bridge thread is shared
   across all calls and blocking it freezes the UI.
5. Add the method to the JS caller (`web/app.js`, etc.) so it actually gets used.

## Threading: when to background a method

The pywebview JS bridge invokes Python on the JS-result-waiting thread. Long
operations on that thread freeze the UI. Rule of thumb:

- **Synchronous OK** (< 100ms): config reads, queue inspections, simple DB
  lookups.
- **Background required**: any `subprocess.Popen`, any file walk, any
  network call, any ffprobe/yt-dlp invocation, anything that holds a DB
  writer lock.

For background work, use:

```python
def my_long_method(self, payload):
    def _worker():
        # do the work
        # push results back via self._window.evaluate_js(...)
    threading.Thread(target=_worker, daemon=True, name="yta-my-feature").start()
    return {"ok": True, "queued": True}  # tell the UI we started
```

## Patch history

- Patch 1: critical bug fixes across the codebase.
- Patch 2: helper consolidation (`subprocess_util`, `fs_search`, `text_utils`).
- Patch 3: `ProcessRegistry` + `YtDlpRunner` for subprocess lifecycle.
- Patch 4: `config_transaction()` for atomic RMW on config.
- Patch 8: this document. Mixin contracts made explicit (above).
