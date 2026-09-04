"""
deps_installer — first-run dependency setup for YTArchiver.

Restores the onboarding that was lost in the tkinter -> pywebview migration.
The old tkinter build had a "Setup - Install Dependencies" dialog that
downloaded yt-dlp + ffmpeg and pip-installed the helper packages. The
migrated app only *reports* missing deps (diagnostics_mixin.check_dependencies)
and never offered to install anything, so a brand-new machine just saw red
"[Deps] N missing" log lines. This module is the install half.

Two tiers:

  CORE binaries (small, required for downloading to work at all):
    - yt-dlp.exe          (GitHub latest release)
    - ffmpeg.exe/ffprobe.exe (gyan.dev release-essentials zip)
  installed into an app-managed bin dir (%APPDATA%/YTArchiver/bin) that
  `ensure_bin_on_path()` prepends to PATH at boot. Every existing
  shutil.which("yt-dlp") / find_yt_dlp() / find_ffprobe() call then
  resolves with no other changes.

  WHISPER stack (large, optional - for GPU transcription):
    - official Python 3.11 (silent per-user install to the location
      find_python311() already checks first)
    - faster-whisper + transformers + torch (CUDA build if an NVIDIA GPU
      is present, CPU build otherwise) installed into that Python 3.11.

Every long operation accepts a `progress` callback -- a 1-arg function
that receives a dict {phase, pct, msg, status}. All functions are
re-runnable and never raise to the caller (they return {"ok": bool, ...}).
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
import uuid
import zipfile
from collections.abc import Callable
from pathlib import Path
from urllib.parse import urlparse

from .log import get_logger
from .process_runner import supervise_streaming_process
from .ytarchiver_config import APP_DATA_DIR

_log = get_logger(__name__)

Progress = Callable[[dict], None]

# ── download sources ────────────────────────────────────────────────────
_YTDLP_URL = "https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp.exe"
_FFMPEG_ZIP_URL = "https://www.gyan.dev/ffmpeg/builds/ffmpeg-release-essentials.zip"
# Pinned Python 3.11 (last 3.11 with a Windows installer at time of writing).
_PY311_VERSION = "3.11.9"
_PY311_URL = f"https://www.python.org/ftp/python/{_PY311_VERSION}/python-{_PY311_VERSION}-amd64.exe"
# All dependency mutations share one process-wide lock.  The lock is
# re-entrant because the whisper installer calls the Python installer while
# holding it.  This prevents two onboarding clicks from racing downloads,
# swaps, or pip against the same managed locations.
_INSTALL_LOCK = threading.RLock()
_BIN_SWAP_JOURNAL_NAME = ".bin-swap.json"


# ── integrity helpers ────────────────────────────────────────────────────
def _fetch_text(url: str, timeout: int = 30) -> str:
    """Fetch a small text resource (checksums, manifests). Raises on failure."""
    req = urllib.request.Request(url, headers={"User-Agent": "YTArchiver-Setup"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read(1 << 20).decode("utf-8", errors="replace")


def _verify_sha256(path: Path, expected_hex: str) -> None:
    """Verify *path* SHA-256 matches *expected_hex*.

    Deletes the file and raises RuntimeError on mismatch so the caller can
    return {"ok": False, "integrity_error": True} rather than executing a
    potentially tampered artifact.
    """
    expected = str(expected_hex or "").strip().lower()
    if (len(expected) != 64
            or any(ch not in "0123456789abcdef" for ch in expected)):
        raise RuntimeError(f"missing or malformed SHA-256 for {path.name}")
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    actual = h.hexdigest().lower()
    if actual != expected:
        try:
            path.unlink()
        except OSError:
            pass
        raise RuntimeError(
            f"integrity check failed for {path.name}: "
            f"expected {expected[:16]}…, got {actual[:16]}…"
        )


# ── small helpers ─────────────────────────────────────────────────────────
def _bin_swap_journal_path() -> Path:
    return APP_DATA_DIR / _BIN_SWAP_JOURNAL_NAME


def _write_bin_swap_journal(payload: dict) -> None:
    """Durably publish dependency-swap recovery state."""
    APP_DATA_DIR.mkdir(parents=True, exist_ok=True)
    target = _bin_swap_journal_path()
    tmp = target.with_name(f"{target.name}.tmp-{uuid.uuid4().hex}")
    try:
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, target)
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass


def _swap_child(name: object, prefix: str) -> Path | None:
    """Resolve one journal child without permitting path traversal."""
    if not isinstance(name, str) or not name.startswith(prefix):
        return None
    if os.path.basename(name) != name:
        return None
    return APP_DATA_DIR / name


def _directory_has_files(path: Path) -> bool:
    try:
        return path.is_dir() and any(child.is_file() for child in path.iterdir())
    except OSError:
        return False


def _safe_mtime(path: Path) -> float:
    """Return an orphan backup's mtime without letting one bad entry abort recovery."""
    try:
        return path.stat().st_mtime
    except OSError:
        return -1.0


def _recover_interrupted_bin_swap() -> None:
    """Restore a usable managed tool directory after an interrupted swap.

    The live directory is renamed before the staged directory can take its
    place, so a power loss between those two atomic renames otherwise leaves
    the next launch with no tools.  The journal identifies the exact backup
    and verified stage.  A conservative orphan-backup fallback also repairs
    state left by older builds that predated the journal.
    """
    APP_DATA_DIR.mkdir(parents=True, exist_ok=True)
    live = APP_DATA_DIR / "bin"
    journal_path = _bin_swap_journal_path()
    journal = None
    if journal_path.is_file():
        try:
            with open(journal_path, encoding="utf-8") as handle:
                loaded = json.load(handle)
            if isinstance(loaded, dict):
                journal = loaded
        except (OSError, ValueError) as exc:
            _log.error("dependency swap journal is unreadable: %s", exc)

    if journal is not None:
        stage = _swap_child(journal.get("stage"), ".bin-stage-")
        backup = _swap_child(journal.get("backup"), ".bin-backup-")
        had_existing = bool(journal.get("had_existing"))
        phase = journal.get("phase")
        try:
            if phase == "committed" and _directory_has_files(live):
                # Only the durable committed phase proves that a non-empty
                # live directory is the verified staged toolset.
                if stage is not None and stage.exists():
                    shutil.rmtree(stage)
                if backup is not None and backup.exists():
                    shutil.rmtree(backup)
                journal_path.unlink(missing_ok=True)
                return

            if had_existing and backup is not None and backup.is_dir():
                # In prepared/backup_moved (and unknown) phases, live may be a
                # partial commit or rollback. The exact journal backup remains
                # authoritative even when that partial directory has files.
                if live.exists():
                    shutil.rmtree(live)
                os.replace(backup, live)
                if stage is not None and stage.exists():
                    shutil.rmtree(stage)
                journal_path.unlink(missing_ok=True)
                return

            if _directory_has_files(live):
                # No recoverable prior toolset exists. Keep the only usable
                # live directory, but never take this branch while an intact
                # journal backup is available.
                if stage is not None and stage.exists():
                    shutil.rmtree(stage)
                journal_path.unlink(missing_ok=True)
                return

            if live.exists():
                # Only an empty directory can be present here (for example an
                # older boot recreated it after the crash).
                live.rmdir()

            if (not had_existing and stage is not None
                    and _directory_has_files(stage)):
                # No live toolset existed before this install. The journal is
                # written only after validation, so this stage is safe to
                # promote when the commit rename was interrupted.
                os.replace(stage, live)
                journal_path.unlink(missing_ok=True)
                return
        except OSError as exc:
            _log.error("dependency swap recovery failed: %s", exc)
            return

    # Legacy fallback: old interrupted swaps had no journal. Restore a backup
    # only when live is absent/empty; never replace a non-empty live toolset.
    if not _directory_has_files(live):
        backups = sorted(
            (p for p in APP_DATA_DIR.glob(".bin-backup-*") if p.is_dir()),
            key=_safe_mtime,
            reverse=True,
        )
        for backup in backups:
            if not _directory_has_files(backup):
                continue
            try:
                if live.exists():
                    live.rmdir()
                os.replace(backup, live)
                _log.warning(
                    "recovered managed tools from interrupted swap backup %s",
                    backup.name)
            except OSError as exc:
                _log.error("legacy dependency backup recovery failed: %s", exc)
            break


def _emit(progress: Progress | None, phase: str, msg: str,
          pct: float | None = None, status: str = "running") -> None:
    """Best-effort progress notification. Never raises."""
    if progress is None:
        return
    try:
        progress({"phase": phase, "pct": (round(pct) if pct is not None else None),
                  "msg": msg, "status": status})
    except Exception as e:  # pragma: no cover - UI callback must never break install
        _log.debug("progress callback raised (ignored): %s", e)


def _no_window():
    """(startupinfo, creationflags) that suppress a console window on Windows."""
    if os.name != "nt":
        return None, 0
    si = subprocess.STARTUPINFO()
    si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    return si, getattr(subprocess, "CREATE_NO_WINDOW", 0)


def managed_bin_dir() -> Path:
    """%APPDATA%/YTArchiver/bin — where we drop downloaded yt-dlp/ffmpeg.
    Created on demand."""
    with _INSTALL_LOCK:
        _recover_interrupted_bin_swap()
        d = APP_DATA_DIR / "bin"
        try:
            d.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            _log.debug("could not create bin dir %s: %s", d, e)
        return d


def ensure_bin_on_path() -> str:
    """Append the managed bin dir to this process's PATH (idempotent).

    Call this once, EARLY in boot, before any shutil.which() / dependency
    probe runs. Returns the bin dir path. Safe to call repeatedly.

    APPEND (not prepend) so a user's own system yt-dlp/ffmpeg keeps winning
    — the managed dir is a *fallback* that fills gaps on machines that have
    nothing installed. shutil.which() re-reads PATH + the filesystem on each
    call, so binaries downloaded here mid-session are still found next call.
    """
    d = str(managed_bin_dir())
    cur = os.environ.get("PATH", "")
    parts = cur.split(os.pathsep) if cur else []
    if d not in parts:
        os.environ["PATH"] = (cur + os.pathsep + d) if cur else d
    return d


def _download(url: str, dest: Path, progress: Progress | None,
              phase: str, label: str) -> None:
    """Stream `url` to `dest` (atomic via .part), reporting % when the
    server gives a Content-Length. Raises on failure."""
    tmp = dest.with_suffix(dest.suffix + ".part")
    if urlparse(url).scheme.lower() not in ("http", "https"):
        raise ValueError(f"Unsupported download URL scheme: {url}")
    _emit(progress, phase, f"Downloading {label}…", 0)
    req = urllib.request.Request(url, headers={"User-Agent": "YTArchiver-Setup"})
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            total = int(resp.headers.get("Content-Length") or 0)
            got = 0
            chunk = 1024 * 256
            with open(tmp, "wb") as f:
                while True:
                    buf = resp.read(chunk)
                    if not buf:
                        break
                    f.write(buf)
                    got += len(buf)
                    if total > 0:
                        _emit(progress, phase,
                              f"Downloading {label}… "
                              f"{got // (1024*1024)}/"
                              f"{total // (1024*1024)} MB",
                              got * 100.0 / total)
                    else:
                        _emit(progress, phase,
                              f"Downloading {label}… "
                              f"{got // (1024*1024)} MB")
        os.replace(tmp, dest)
    except Exception:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        raise


# ── probing ───────────────────────────────────────────────────────────────
def _which(name: str) -> str | None:
    return shutil.which(name) or shutil.which(name + ".exe")


def _find_python311() -> str | None:
    """Locate Python 3.11 via the same logic the transcribe runtime uses."""
    try:
        from .transcribe.helpers import find_python311
        return find_python311()
    except Exception as e:
        _log.debug("find_python311 import/call failed: %s", e)
        return None


def detect_gpu() -> dict:
    """Probe for an NVIDIA GPU via nvidia-smi. {ok, name}."""
    try:
        si, cf = _no_window()
        r = subprocess.run(
            ["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"],
            capture_output=True, text=True, timeout=6,
            startupinfo=si, creationflags=cf)
        if r.returncode == 0 and r.stdout.strip():
            return {"ok": True, "name": r.stdout.strip().splitlines()[0].strip()}
    except Exception as e:
        _log.debug("nvidia-smi probe failed: %s", e)
    return {"ok": False, "name": ""}


def firefox_cookie_status() -> dict:
    """Detect whether Firefox is present and holds YouTube cookies.

    YTArchiver authenticates yt-dlp via **Firefox** cookies. Chromium
    browsers (Chrome/Brave/Edge/…) use app-bound cookie encryption on
    Windows that yt-dlp can't read — so on a machine without Firefox the
    cookie probe falls through to Chrome and downloads fail with a
    "could not get chrome cookies" error. This lets onboarding warn up
    front instead of at first download.

    Returns {installed, has_yt_cookies, signed_in, profile, detail}.
    """
    res = {"installed": False, "has_yt_cookies": False, "signed_in": False,
           "expired_auth_cookies": False, "profile": "", "detail": ""}
    try:
        appdata = os.environ.get("APPDATA") or ""
        prof_dir = Path(appdata) / "Mozilla" / "Firefox" / "Profiles"
        if not prof_dir.is_dir():
            res["detail"] = "Firefox not installed"
            return res
        res["installed"] = True
        cookie_dbs = sorted(prof_dir.glob("*/cookies.sqlite"))
        if not cookie_dbs:
            res["detail"] = "Firefox found, but no profile cookies yet"
            return res
        import sqlite3 as _sql
        # Cookie names that indicate an actual signed-in YouTube/Google
        # session (vs. just having visited youtube.com).
        AUTH = ("__Secure-3PSID", "__Secure-1PSID", "SID", "SAPISID",
                "SSID", "LOGIN_INFO")
        for db in cookie_dbs:
            try:
                # immutable=1 → read even while Firefox holds the DB open,
                # without taking locks (it won't change under us).
                uri = db.as_uri() + "?mode=ro&immutable=1"
                con = _sql.connect(uri, uri=True, timeout=2.0)
                try:
                    schema_version = con.execute(
                        "PRAGMA user_version").fetchone()[0]
                    # Firefox 142 / cookies DB schema 16 changed ``expiry``
                    # from Unix seconds to Unix milliseconds. Comparing a
                    # 13-digit value to time.time() seconds makes every
                    # schema-16+ auth cookie look unexpired indefinitely.
                    expiry_now = int(time.time() * (
                        1000 if schema_version >= 16 else 1))
                    yt_n = con.execute(
                        "SELECT COUNT(*) FROM moz_cookies "
                        "WHERE host LIKE '%youtube.com%'").fetchone()[0]
                    ph = ",".join("?" * len(AUTH))
                    auth_total = con.execute(
                        "SELECT COUNT(*) FROM moz_cookies WHERE "
                        "(host LIKE '%youtube.com%' OR host LIKE '%google.com%') "
                        f"AND name IN ({ph})", AUTH).fetchone()[0]
                    auth_n = con.execute(
                        "SELECT COUNT(*) FROM moz_cookies WHERE "
                        "(host LIKE '%youtube.com%' OR host LIKE '%google.com%') "
                        f"AND name IN ({ph}) "
                        "AND (expiry=0 OR expiry>?)",
                        (*AUTH, expiry_now)).fetchone()[0]
                finally:
                    con.close()
                if yt_n > 0:
                    res["has_yt_cookies"] = True
                    res["profile"] = db.parent.name
                if auth_total > 0 and auth_n == 0:
                    res["expired_auth_cookies"] = True
                    res["profile"] = db.parent.name
                if auth_n > 0:
                    res["signed_in"] = True
                    res["has_yt_cookies"] = True
                    res["profile"] = db.parent.name
                    break  # a signed-in profile is the best answer
            except Exception as e:
                _log.debug("firefox cookie db read failed (%s): %s", db, e)
                continue
        if res["signed_in"]:
            res["detail"] = "signed into YouTube in Firefox"
        elif res["expired_auth_cookies"]:
            res["detail"] = (
                "Firefox YouTube sign-in cookies expired - sign in again")
        elif res["has_yt_cookies"]:
            res["detail"] = "Firefox has YouTube cookies (sign-in not detected)"
        else:
            res["detail"] = "no YouTube cookies — sign into YouTube in Firefox"
        return res
    except Exception as e:
        _log.debug("firefox_cookie_status failed: %s", e)
        res["detail"] = "could not check Firefox cookies"
        return res


def _whisper_ready(py311: str | None) -> bool:
    """True when the Python 3.11 worker can import both worker stacks."""
    if not py311 or not os.path.isfile(py311):
        return False
    try:
        si, cf = _no_window()
        r = subprocess.run(
            [
                py311,
                "-c",
                "import faster_whisper, torch, transformers; "
                "from transformers import pipeline",
            ],
            capture_output=True, text=True, timeout=60,
            startupinfo=si, creationflags=cf)
        return r.returncode == 0
    except Exception as e:
        _log.debug("whisper import probe failed: %s", e)
        return False


def probe(check_whisper_import: bool = False) -> dict:
    """Snapshot of dependency state for the onboarding UI.

    `check_whisper_import` runs a (slowish) Python 3.11 import test; the
    wizard only needs that occasionally, so it's opt-in.
    """
    ensure_bin_on_path()
    ytdlp = _which("yt-dlp")
    ffmpeg = _which("ffmpeg")
    ffprobe = _which("ffprobe")
    py311 = _find_python311()
    gpu = detect_gpu()
    whisper_ok = False
    if check_whisper_import:
        whisper_ok = _whisper_ready(py311)
    try:
        cookies = firefox_cookie_status()
    except Exception as e:
        _log.debug("cookie status failed: %s", e)
        cookies = {"installed": False, "has_yt_cookies": False,
                   "signed_in": False, "detail": "check failed"}
    return {
        "bin_dir": str(managed_bin_dir()),
        "ytdlp": {"ok": bool(ytdlp), "path": ytdlp or ""},
        "ffmpeg": {"ok": bool(ffmpeg), "path": ffmpeg or ""},
        "ffprobe": {"ok": bool(ffprobe), "path": ffprobe or ""},
        "python311": {"ok": bool(py311), "path": py311 or ""},
        "whisper": {"ok": whisper_ok,
                    "checked": check_whisper_import,
                    "detail": "Whisper + punctuation imports OK" if whisper_ok
                              else ("Python 3.11 found - packages not verified"
                                    if py311 else "Python 3.11 not found")},
        "gpu": gpu,
        # YouTube auth: Firefox cookies (Chromium not supported on Windows).
        "cookies": cookies,
        # Convenience: are the must-haves for downloading present?
        "core_ok": bool(ytdlp and ffmpeg and ffprobe),
    }


# ── core installers (yt-dlp + ffmpeg) ──────────────────────────────────────
def _checksum_token(text: str, filename: str | None = None) -> str:
    """Return a strict SHA-256 token from a checksum sidecar."""
    wanted = filename.lower() if filename else None
    for raw_line in str(text or "").splitlines():
        parts = raw_line.strip().split()
        if not parts:
            continue
        if wanted is not None:
            if len(parts) < 2:
                continue
            published_name = os.path.basename(parts[-1].lstrip("*")).lower()
            if published_name != wanted:
                continue
        token = parts[0].lower()
        if (len(token) == 64
                and all(ch in "0123456789abcdef" for ch in token)):
            return token
        raise RuntimeError("checksum sidecar contains a malformed SHA-256")
    target = f" for {filename}" if filename else ""
    raise RuntimeError(f"checksum sidecar has no SHA-256{target}")


def _prepare_bin_stage() -> Path:
    """Create a same-volume copy of the managed bin directory."""
    APP_DATA_DIR.mkdir(parents=True, exist_ok=True)
    bin_dir = APP_DATA_DIR / "bin"
    stage = Path(tempfile.mkdtemp(prefix=".bin-stage-", dir=APP_DATA_DIR))
    try:
        if bin_dir.is_dir():
            shutil.copytree(bin_dir, stage, dirs_exist_ok=True)
        for artifact in stage.glob("*.part"):
            artifact.unlink(missing_ok=True)
        (stage / "_ffmpeg_dl.zip").unlink(missing_ok=True)
        return stage
    except Exception:
        shutil.rmtree(stage, ignore_errors=True)
        raise


def _validate_staged_tools(stage: Path, names: set[str]) -> None:
    missing = []
    for name in sorted(names):
        candidate = stage / name
        try:
            valid = candidate.is_file() and candidate.stat().st_size > 0
        except OSError:
            valid = False
        if not valid:
            missing.append(name)
    if missing:
        raise RuntimeError(f"staged toolset missing {', '.join(missing)}")


def _swap_managed_bin(stage: Path) -> Path:
    """Replace the managed toolset, rolling back if the swap cannot finish."""
    bin_dir = APP_DATA_DIR / "bin"
    backup = APP_DATA_DIR / f".bin-backup-{uuid.uuid4().hex}"
    had_existing = bin_dir.exists()
    journal = {
        "version": 1,
        "phase": "prepared",
        "stage": stage.name,
        "backup": backup.name,
        "had_existing": had_existing,
        "created_at": time.time(),
    }
    _write_bin_swap_journal(journal)
    try:
        if had_existing:
            os.replace(bin_dir, backup)
            journal["phase"] = "backup_moved"
            # This write is part of the swap, not preparation. If it fails,
            # restore the just-moved live directory immediately rather than
            # waiting for startup recovery while bin/ is absent.
            _write_bin_swap_journal(journal)
        os.replace(stage, bin_dir)
        journal["phase"] = "committed"
        _write_bin_swap_journal(journal)
    except Exception as swap_error:
        rollback_error = None
        if had_existing and backup.exists():
            try:
                if bin_dir.exists():
                    shutil.rmtree(bin_dir)
                os.replace(backup, bin_dir)
            except Exception as exc:  # pragma: no cover - catastrophic disk fault
                rollback_error = exc
        if rollback_error is not None:
            raise RuntimeError(
                f"toolset swap failed ({swap_error}); rollback also failed "
                f"({rollback_error})") from swap_error
        try:
            _bin_swap_journal_path().unlink(missing_ok=True)
        except OSError:
            pass
        raise
    if backup.exists():
        try:
            shutil.rmtree(backup)
        except OSError as exc:
            _log.warning("could not remove dependency backup %s: %s", backup, exc)
    try:
        _bin_swap_journal_path().unlink(missing_ok=True)
    except OSError as exc:
        _log.warning("could not remove dependency swap journal: %s", exc)
    return bin_dir


def _stage_ytdlp(stage: Path, progress: Progress | None) -> dict:
    dest = stage / "yt-dlp.exe"
    _download(_YTDLP_URL, dest, progress, "ytdlp", "yt-dlp")
    _emit(progress, "ytdlp", "Verifying yt-dlp integrity…", None)
    sums_url = _YTDLP_URL.replace("/yt-dlp.exe", "/SHA2-256SUMS")
    expected = _checksum_token(_fetch_text(sums_url), "yt-dlp.exe")
    _verify_sha256(dest, expected)
    _validate_staged_tools(stage, {"yt-dlp.exe"})
    return {"ok": True, "path": str(dest)}


def _stage_ffmpeg(stage: Path, progress: Progress | None) -> dict:
    zip_path = stage / "_ffmpeg_dl.zip"
    _download(_FFMPEG_ZIP_URL, zip_path, progress, "ffmpeg", "ffmpeg")
    _emit(progress, "ffmpeg", "Verifying ffmpeg integrity…", None)
    expected = _checksum_token(_fetch_text(_FFMPEG_ZIP_URL + ".sha256"))
    _verify_sha256(zip_path, expected)
    _emit(progress, "ffmpeg", "Extracting ffmpeg…", None)
    wanted = {"ffmpeg.exe", "ffprobe.exe"}
    found: set[str] = set()
    with zipfile.ZipFile(zip_path, "r") as zf:
        for member in zf.namelist():
            base = os.path.basename(member)
            if base in wanted:
                with zf.open(member) as src, open(stage / base, "wb") as dst:
                    shutil.copyfileobj(src, dst)
                found.add(base)
    zip_path.unlink(missing_ok=True)
    if wanted - found:
        raise RuntimeError(f"zip missing {', '.join(sorted(wanted - found))}")
    _validate_staged_tools(stage, wanted)
    return {"ok": True, "path": str(stage / "ffmpeg.exe")}


def _integrity_failure(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(token in msg for token in (
        "checksum", "sha-256", "integrity check", "hash mismatch"))


def install_ytdlp(progress: Progress | None = None, force: bool = False) -> dict:
    """Install yt-dlp through a verified staging directory."""
    with _INSTALL_LOCK:
        ensure_bin_on_path()
        existing = _which("yt-dlp")
        if not force and existing:
            _emit(progress, "ytdlp", "yt-dlp already present.", 100, "ok")
            return {"ok": True, "skipped": True, "path": existing}
        stage = None
        try:
            stage = _prepare_bin_stage()
            result = _stage_ytdlp(stage, progress)
            bin_dir = _swap_managed_bin(stage)
            stage = None
            path = str(bin_dir / "yt-dlp.exe")
            _emit(progress, "ytdlp", "yt-dlp installed.", 100, "ok")
            return {**result, "path": path}
        except Exception as exc:
            _log.warning("yt-dlp install failed: %s", exc)
            integrity = _integrity_failure(exc)
            label = "integrity check" if integrity else "install"
            _emit(progress, "ytdlp", f"yt-dlp {label} failed: {exc}",
                  status="error")
            return {"ok": False, "error": str(exc),
                    **({"integrity_error": True} if integrity else {})}
        finally:
            if stage is not None:
                shutil.rmtree(stage, ignore_errors=True)


def install_ffmpeg(progress: Progress | None = None, force: bool = False) -> dict:
    """Install ffmpeg + ffprobe through a verified staging directory."""
    with _INSTALL_LOCK:
        ensure_bin_on_path()
        if not force and _which("ffmpeg") and _which("ffprobe"):
            _emit(progress, "ffmpeg", "ffmpeg already present.", 100, "ok")
            return {"ok": True, "skipped": True}
        stage = None
        try:
            stage = _prepare_bin_stage()
            result = _stage_ffmpeg(stage, progress)
            bin_dir = _swap_managed_bin(stage)
            stage = None
            path = str(bin_dir / "ffmpeg.exe")
            _emit(progress, "ffmpeg", "ffmpeg + ffprobe installed.", 100, "ok")
            return {**result, "path": path}
        except Exception as exc:
            _log.warning("ffmpeg install failed: %s", exc)
            integrity = _integrity_failure(exc)
            label = "integrity check" if integrity else "install"
            _emit(progress, "ffmpeg", f"ffmpeg {label} failed: {exc}",
                  status="error")
            return {"ok": False, "error": str(exc),
                    **({"integrity_error": True} if integrity else {})}
        finally:
            if stage is not None:
                shutil.rmtree(stage, ignore_errors=True)


def install_core(progress: Progress | None = None, force: bool = False) -> dict:
    """Stage, verify, then commit the complete core toolset as one unit."""
    with _INSTALL_LOCK:
        ensure_bin_on_path()
        if (not force and _which("yt-dlp") and _which("ffmpeg")
                and _which("ffprobe")):
            state = probe()
            _emit(progress, "core", "Core tools already present.", 100, "ok")
            skipped = {"ok": True, "skipped": True}
            return {"ok": True, "ytdlp": dict(skipped),
                    "ffmpeg": dict(skipped), "state": state}

        stage = None
        ytdlp_result: dict = {"ok": False, "error": "not attempted"}
        ffmpeg_result: dict = {"ok": False, "error": "not attempted"}
        try:
            stage = _prepare_bin_stage()
            ytdlp_result = _stage_ytdlp(stage, progress)
            ffmpeg_result = _stage_ffmpeg(stage, progress)
            _validate_staged_tools(
                stage, {"yt-dlp.exe", "ffmpeg.exe", "ffprobe.exe"})
            _swap_managed_bin(stage)
            stage = None
            _emit(progress, "ytdlp", "yt-dlp installed.", 100, "ok")
            _emit(progress, "ffmpeg", "ffmpeg + ffprobe installed.", 100, "ok")
            state = probe()
            ok = bool(state.get("core_ok"))
            _emit(progress, "core",
                  "Core tools ready." if ok else "Some core tools missing.",
                  100, "ok" if ok else "error")
            return {"ok": ok, "ytdlp": ytdlp_result,
                    "ffmpeg": ffmpeg_result, "state": state}
        except Exception as exc:
            _log.warning("core toolset install failed: %s", exc)
            integrity = _integrity_failure(exc)
            failed = {"ok": False, "error": str(exc),
                      **({"integrity_error": True} if integrity else {})}
            if not ytdlp_result.get("ok"):
                ytdlp_result = dict(failed)
            else:
                ffmpeg_result = dict(failed)
            state = probe()
            _emit(progress, "core",
                  f"Core tools unchanged; install failed: {exc}",
                  status="error")
            return {"ok": False, "ytdlp": ytdlp_result,
                    "ffmpeg": ffmpeg_result, "state": state,
                    "error": str(exc),
                    **({"integrity_error": True} if integrity else {})}
        finally:
            if stage is not None:
                shutil.rmtree(stage, ignore_errors=True)


# ── whisper stack installer ────────────────────────────────────────────────
def _run_streaming(cmd: list[str], progress: Progress | None, phase: str,
                   label: str, timeout: int = 2400, *,
                   cancel_event: threading.Event | None = None,
                   task_id: str | None = None) -> tuple[int, str]:
    """Run a subprocess, streaming stdout lines to progress. Returns
    (returncode, tail_of_output)."""
    si, cf = _no_window()
    _emit(progress, phase, f"{label}…", None)
    tail: list[str] = []
    job_id = str(task_id or f"dependency-install-{uuid.uuid4().hex}")
    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1, startupinfo=si, creationflags=cf)
    except Exception as e:
        return 1, str(e)

    def _handle_line(line: str) -> None:
        line = line.rstrip()
        if not line:
            return
        tail.append(line)
        del tail[:-40]  # keep enough context for pip failure diagnostics
        # Surface meaningful pip lines without spamming every byte.
        low = line.lower()
        if any(k in low for k in ("downloading", "installing", "collecting",
                                  "building", "successfully", "error",
                                  "warning")):
            status = ("error" if "error" in low else
                      "warning" if "warning" in low else "running")
            _emit(progress, phase, f"{label}: {line[:120]}", status=status)

    try:
        result = supervise_streaming_process(
            proc,
            on_stdout_line=_handle_line,
            cancel_event=cancel_event,
            timeout=timeout,
            owner="dependency-install",
            task_id=job_id,
            role=str(phase or "install"),
        )
    except Exception as e:
        return 1, str(e)
    if result.timed_out:
        return 1, "timed out"
    if result.cancelled:
        return 1, "cancelled"
    return result.returncode or 0, "\n".join(tail)


def _worker_lock_path(*, cuda: bool) -> Path:
    """Return the checked-in (or PyInstaller-bundled) worker artifact lock."""
    filename = "worker-cuda.lock" if cuda else "worker-cpu.lock"
    candidates: list[Path] = []
    frozen_root = getattr(sys, "_MEIPASS", "")
    if frozen_root:
        candidates.append(Path(frozen_root) / "requirements" / filename)
    candidates.append(Path(__file__).resolve().parents[1] / "requirements" / filename)
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(
        f"Verified transcription dependency lock is missing: {filename}"
    )


def _fetch_py311_sha256(installer_url: str) -> str | None:
    """Fetch the SHA-256 of the Python installer from its Sigstore bundle.

    Python.org publishes a .sigstore JSON alongside each Windows installer.
    Tries the Sigstore bundle v0.2/v0.3 format (messageSignature.messageDigest,
    base64-encoded digest) and falls back to the older canonicalizedBody format.
    Returns a lowercase 64-char hex string, or None if parsing fails.
    """
    try:
        import base64 as _b64
        import json as _json
        sig_text = _fetch_text(installer_url + ".sigstore", timeout=15)
        bundle = _json.loads(sig_text)
        # Sigstore bundle v0.2+: top-level messageSignature.messageDigest.digest
        ms = bundle.get("messageSignature") or {}
        md = ms.get("messageDigest") or {}
        if md.get("algorithm", "").startswith("SHA2_256") and md.get("digest"):
            return _b64.b64decode(md["digest"]).hex().lower()
        # Older Sigstore format: body field is base64(json) with spec.data.hash.value
        body_b64 = bundle.get("payload") or bundle.get("body") or ""
        if body_b64:
            body = _json.loads(_b64.b64decode(body_b64 + "==")
                               .decode("utf-8", errors="replace"))
            val = ((body.get("spec") or {})
                   .get("data", {}).get("hash", {}).get("value") or "")
            if len(val) == 64 and all(c in "0123456789abcdef" for c in val.lower()):
                return val.lower()
    except Exception as e:
        _log.debug("py311 sigstore parse failed: %s", e)
    return None


def _install_python311_unlocked(progress: Progress | None = None) -> dict:
    """Ensure a Python 3.11 interpreter exists. If one is already found,
    reuse it; otherwise download + silently install the official per-user
    build to the location find_python311() checks first.
    Returns {ok, path}."""
    existing = _find_python311()
    if existing:
        _emit(progress, "python", f"Python 3.11 found: {existing}", 100, "ok")
        return {"ok": True, "path": existing, "skipped": True}

    target = Path(os.path.expandvars(
        r"%LOCALAPPDATA%\Programs\Python\Python311"))
    installer = managed_bin_dir() / f"python-{_PY311_VERSION}-amd64.exe"
    try:
        _download(_PY311_URL, installer, progress, "python",
                  f"Python {_PY311_VERSION}")
        # Verify against the Sigstore bundle published alongside each release.
        _emit(progress, "python", "Verifying Python installer integrity…", None)
        expected = _fetch_py311_sha256(_PY311_URL)
        if not expected:
            raise RuntimeError(
                "Python installer checksum unavailable or malformed")
        _verify_sha256(installer, expected)  # deletes + raises on mismatch
        _emit(progress, "python",
              "Installing Python 3.11 (per-user, no admin)…", None)
        si, cf = _no_window()
        # Per-user, quiet, don't touch PATH or the py launcher, include pip.
        r = subprocess.run(
            [str(installer), "/quiet",
             "InstallAllUsers=0", "PrependPath=0", "Include_launcher=0",
             "Include_test=0", "Include_doc=0", "Include_pip=1",
             "AssociateFiles=0", "Shortcuts=0",
             f'TargetDir={target}'],
            capture_output=True, text=True, timeout=900,
            startupinfo=si, creationflags=cf)
        try:
            os.remove(installer)
        except OSError:
            pass
        py = _find_python311()
        if not py:
            # The official installer returns 0 even when it relaunches
            # elevated/cancelled; re-derive directly from the target dir.
            cand = target / "python.exe"
            py = str(cand) if cand.is_file() else None
        if py:
            _emit(progress, "python", "Python 3.11 installed.", 100, "ok")
            return {"ok": True, "path": py}
        _emit(progress, "python",
              f"Python 3.11 installer finished (code {r.returncode}) but "
              "interpreter not found.", status="error")
        return {"ok": False, "error": f"installer exit {r.returncode}; "
                                      "python.exe not found after install"}
    except RuntimeError as e:
        _log.warning("python 3.11 integrity check failed: %s", e)
        try:
            if installer.exists():
                os.remove(installer)
        except OSError:
            pass
        _emit(progress, "python", f"Python 3.11 integrity check failed: {e}",
              status="error")
        return {"ok": False, "error": str(e), "integrity_error": True}
    except Exception as e:
        _log.warning("python 3.11 install failed: %s", e)
        try:
            if installer.exists():
                os.remove(installer)
        except OSError:
            pass
        _emit(progress, "python", f"Python 3.11 install failed: {e}",
              status="error")
        return {"ok": False, "error": str(e)}


def install_python311(progress: Progress | None = None) -> dict:
    """Serialized public wrapper for the Python runtime installer."""
    with _INSTALL_LOCK:
        return _install_python311_unlocked(progress)


def _install_whisper_stack_unlocked(progress: Progress | None = None) -> dict:
    """Full transcription-stack setup: Python 3.11 + pip packages.

    Installs faster-whisper + transformers, plus torch (CUDA build if an
    NVIDIA GPU is detected, else CPU). Verifies the imports at the end.
    """
    py = install_python311(progress)
    if not py.get("ok"):
        return {"ok": False, "stage": "python311", "error": py.get("error")}
    python = py["path"]

    gpu = detect_gpu()
    _emit(progress, "whisper",
          f"GPU: {gpu['name']}" if gpu["ok"]
          else "No NVIDIA GPU detected - installing CPU build.", None)

    # One resolver transaction installs the known-good CPU or CUDA stack.
    # Every direct and transitive Windows artifact has an exact version and
    # SHA-256 in the bundled lock.  This replaces the old broad ranges and
    # unbounded pip upgrade, which could produce a different worker each day.
    try:
        lock = _worker_lock_path(cuda=bool(gpu["ok"]))
    except OSError as exc:
        _emit(progress, "whisper", str(exc), status="error")
        return {"ok": False, "stage": "lock", "error": str(exc),
                "integrity_error": True}
    label = ("Installing verified CUDA transcription stack"
             if gpu["ok"] else
             "Installing verified CPU transcription stack")
    rc, tail = _run_streaming(
        [python, "-m", "pip", "install", "--disable-pip-version-check",
         "--require-hashes", "--only-binary=:all:", "-r", str(lock)],
        progress, "whisper", label, timeout=5400)
    if rc != 0:
        _emit(progress, "whisper",
              f"Verified package install failed: {tail[-160:]}",
              status="error")
        return {"ok": False, "stage": "packages", "error": tail[-400:]}

    rc, tail = _run_streaming(
        [python, "-m", "pip", "check"], progress, "whisper",
        "Checking transcription package compatibility", timeout=300)
    if rc != 0:
        _emit(progress, "whisper", f"Package check failed: {tail[-160:]}",
              status="error")
        return {"ok": False, "stage": "pip-check", "error": tail[-400:]}

    # Verify the actual worker imports after pip's dependency check.
    _emit(progress, "whisper", "Verifying transcription stack…", None)
    ok = _whisper_ready(python)
    if ok:
        _emit(progress, "whisper", "Transcription stack ready.", 100, "ok")
        return {"ok": True, "python311": python, "gpu": gpu}
    _emit(progress, "whisper",
          "Packages installed but import verification failed.", status="error")
    return {"ok": False, "stage": "verify",
            "error": "faster_whisper/torch import failed after install",
            "python311": python}


def install_whisper_stack(progress: Progress | None = None) -> dict:
    """Serialize all Python and pip mutations against core installs."""
    with _INSTALL_LOCK:
        return _install_whisper_stack_unlocked(progress)


__all__ = [
    "managed_bin_dir", "ensure_bin_on_path", "probe", "detect_gpu",
    "install_ytdlp", "install_ffmpeg", "install_core",
    "install_python311", "install_whisper_stack",
]
