"""Filesystem safety utilities: disk checks, containment, atomic I/O, sidecar cleanup (split from utils.py)."""
from __future__ import annotations

import contextlib
import glob
import json
import os
import shutil
from typing import Any

from .fs_attrs import _file_has_hidden_attribute
from .log import get_logger

_log = get_logger(__name__)


def check_directory_writable(path: str) -> bool:
    """Can we create + delete a probe file inside `path`? True if yes."""
    if not path:
        return False
    try:
        if not os.path.isdir(path):
            return False
        # clean up any stale probe files from a previous
        # run (crashed process, antivirus-blocked unlink, etc.) before
        # writing a new one. Without this, the archive root accumulates
        # `.yta_probe_<PID>` litter over time.
        try:
            for _f in os.listdir(path):
                if _f.startswith(".yta_probe_"):
                    try: os.remove(os.path.join(path, _f))
                    except OSError: pass
        except OSError:
            pass
        probe = os.path.join(path, f".yta_probe_{os.getpid()}")
        with open(probe, "w", encoding="utf-8") as f:
            f.write("ok")
        try: os.remove(probe)
        except OSError: pass
        return True
    except OSError:
        return False


def check_disk_space(path: str, required_bytes: int) -> bool:
    """True if `path`'s filesystem has at least `required_bytes` free."""
    if not path or required_bytes <= 0:
        return True
    try:
        free = shutil.disk_usage(path).free
        return free >= int(required_bytes)
    except (OSError, ValueError) as exc:
        _log.warning("disk space probe failed for %r: %s", path, exc)
        return False


def is_within_managed_roots(path: str) -> bool:
    """True if `path` resolves to a location under one of the archive roots
    this app manages: the global output_dir, any per-channel output_dir, and
    the tp_archive_roots (index-only roots). Used to gate destructive
    os.remove calls that originate from the JS bridge (the trust boundary),
    so a crafted/compromised filepath can't drive a delete outside the
    archive. Fail-closed: returns False when no roots are configured or the
    path can't be resolved. realpath is used on both sides so a symlink
    can't tunnel out of an allowed root.
    """
    try:
        from .ytarchiver_config import load_config
        cfg = load_config() or {}
    except Exception:
        return False
    roots: list[str] = []
    _g = (cfg.get("output_dir") or "").strip()
    if _g:
        roots.append(_g)
    # (Channels don't store their own output_dir — their folders nest under
    # the global output_dir added above, so no per-channel root is needed.)
    for _r in (cfg.get("tp_archive_roots") or []):
        if _r:
            roots.append(str(_r))
    if not roots:
        return False
    try:
        target = os.path.normcase(os.path.realpath(path)).rstrip("/\\")
    except (ValueError, OSError):
        return False
    if not target:
        return False
    for _root in roots:
        try:
            nr = os.path.normcase(os.path.realpath(_root)).rstrip("/\\")
        except (ValueError, OSError):
            continue
        if not nr:
            continue
        if target == nr or target.startswith((nr + os.sep, nr + "/")):
            return True
    return False


def sampled_files_equal(path_a: str, path_b: str, sample: int = 1 << 20) -> bool:
    """Best-effort 'are these the same file' check: equal size + up to three
    1MB content windows (head, mid, tail). Used before treating one file as a
    duplicate of another and deleting/replacing the source. CONSERVATIVE — any
    read error or size mismatch returns False (not equal), so a caller never
    deletes on uncertainty. Three windows (vs head+tail only) guard the rare
    'same size, identical head+tail, different middle' collision. Single source
    of truth shared by redownload (replace) and reorg (dedup-delete) so the
    delete path isn't weaker than the replace path (audit: sampled_files_equal).
    """
    try:
        sz = os.path.getsize(path_a)
        if sz != os.path.getsize(path_b):
            return False
    except OSError:
        return False
    try:
        with open(path_a, "rb") as _a, open(path_b, "rb") as _b:
            if _a.read(sample) != _b.read(sample):
                return False
            if sz > sample * 3:
                _mid = sz // 2 - sample // 2
                _a.seek(_mid); _b.seek(_mid)
                if _a.read(sample) != _b.read(sample):
                    return False
                _tail = sz - sample
                _a.seek(_tail); _b.seek(_tail)
                if _a.read(sample) != _b.read(sample):
                    return False
            elif sz > sample * 2:
                _tail = sz - sample
                _a.seek(_tail); _b.seek(_tail)
                if _a.read(sample) != _b.read(sample):
                    return False
    except OSError:
        return False
    return True


def delete_video_sidecars(filepath: str) -> None:
    """Best-effort cleanup of sidecar files next to a video.

    Removes `.jsonl`, `.info.json`, `.description`, `.live_chat.json`,
    `.srt`, hidden image thumbnails, and language-coded caption variants
    (.*.vtt, .*.srt, .*.ttml). Used by recent_delete_file and
    video_delete_file.

    Errors per sidecar are swallowed — the primary contract is "the
    main file is gone"; a leaked sidecar is non-fatal.

    yt-dlp emits a wider sidecar set than the original
    narrow list captured. Keep this list in sync with what yt-dlp
    actually writes.
    """
    if not filepath:
        return
    base = os.path.splitext(filepath)[0]
    # Do not delete arbitrary same-stem `.txt`: it can be a visible
    # Transcript.txt or a user note. Image siblings are also only safe to
    # remove when the app has already hidden them as sidecars.
    _basic_exts = (".jsonl", ".info.json", ".description",
                   ".live_chat.json", ".srt")
    _hidden_image_exts = (".jpg", ".jpeg", ".webp", ".png")
    for ext in _basic_exts:
        sc = base + ext
        try:
            if os.path.isfile(sc):
                os.remove(sc)
        except OSError:
            pass
    for ext in _hidden_image_exts:
        sc = base + ext
        try:
            if os.path.isfile(sc) and _file_has_hidden_attribute(sc):
                os.remove(sc)
        except OSError:
            pass
    # Language-coded caption variants (en, en-orig, en-US, es, …).
    # Glob avoids enumerating every language code yt-dlp might emit.
    # `glob.escape` is required — titles routinely contain bracket
    # metacharacters like "[Live]" or "[Remastered]" which would
    # otherwise be interpreted as glob character classes, causing the
    # pattern to silently match nothing and leak orphan caption files.
    _base_glob = glob.escape(base)
    for pat in (_base_glob + ".*.vtt", _base_glob + ".*.srt",
                _base_glob + ".*.ttml"):
        try:
            for _hit in glob.glob(pat):
                try: os.remove(_hit)
                except OSError: pass
        except Exception as e:
            _log.debug("swallowed: %s", e)


def load_json_safe(path, default: Any = None) -> Any:
    """Load JSON from `path`. Return `default` on any failure — missing
    file, malformed JSON, OS error.

    Use for state files where "missing or corrupt = start from defaults"
    is the desired behavior (queue.json, channel cache, drawer state).
    Callers that need to distinguish missing-vs-corrupt should check
    `os.path.exists(path)` themselves before calling.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError, ValueError):
        return default


@contextlib.contextmanager
def atomic_write(path, mode: str = "w", encoding: str = "utf-8"):
    """Atomic-replace context manager.

    Yields a file handle pointing to a `.tmp` sibling of `path`. On
    successful exit, fsyncs and atomically renames over `path`. On
    exception inside the block, removes the `.tmp` so the original is
    untouched, then re-raises.

    Usage:
        with atomic_write("state.json") as f:
            json.dump(data, f)

    Why atomic: a crash or power loss during a plain `open(..., 'w')`
    truncates the destination to whatever bytes happened to flush. With
    `.tmp` + `os.replace()`, the original file remains intact until the
    rename completes — and rename is atomic at the filesystem layer on
    both Windows (NtSetInformationFile) and POSIX.
    """
    if "a" in mode:
        raise ValueError("atomic_write does not support append mode")
    path = os.fspath(path)
    # Use mkstemp for a UNIQUE temp filename per writer. Old `path +
    # ".tmp"` collided when two threads wrote the same path
    # concurrently — second open truncated the first's in-flight tmp,
    # then both raced on os.replace and the loser committed half-
    # flushed content over the winner (audit: utils.py:525-577).
    import tempfile as _tempfile
    _dir = os.path.dirname(path) or "."
    _stem = os.path.basename(path) + "."
    fd, tmp = _tempfile.mkstemp(prefix=_stem, suffix=".tmp", dir=_dir)
    try:
        os.close(fd)
    except OSError:
        pass
    open_kwargs: dict[str, Any] = {"mode": mode}
    if "b" not in mode:
        open_kwargs["encoding"] = encoding
    f = open(tmp, **open_kwargs)
    success = False
    try:
        yield f
        try:
            f.flush()
            os.fsync(f.fileno())
        except OSError:
            pass
        success = True
    finally:
        try:
            f.close()
        except Exception as e:
            _log.debug("swallowed: %s", e)
        if success:
            try:
                os.replace(tmp, path)
            except Exception:
                try: os.unlink(tmp)
                except OSError: pass
                raise
        else:
            try:
                os.unlink(tmp)
            except OSError:
                pass
