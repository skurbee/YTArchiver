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
import os
import shutil
import subprocess
import urllib.request
import zipfile
from collections.abc import Callable
from pathlib import Path
from urllib.parse import urlparse

from .log import get_logger
from .ytarchiver_config import APP_DATA_DIR

_log = get_logger(__name__)

Progress = Callable[[dict], None]

# ── download sources ────────────────────────────────────────────────────
_YTDLP_URL = "https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp.exe"
_FFMPEG_ZIP_URL = "https://www.gyan.dev/ffmpeg/builds/ffmpeg-release-essentials.zip"
# Pinned Python 3.11 (last 3.11 with a Windows installer at time of writing).
_PY311_VERSION = "3.11.9"
_PY311_URL = f"https://www.python.org/ftp/python/{_PY311_VERSION}/python-{_PY311_VERSION}-amd64.exe"
# torch CUDA wheel index (cu121 covers modern NVIDIA drivers); CPU uses PyPI.
_TORCH_CUDA_INDEX = "https://download.pytorch.org/whl/cu121"


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
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    actual = h.hexdigest().lower()
    if actual != expected_hex.lower():
        try:
            path.unlink()
        except OSError:
            pass
        raise RuntimeError(
            f"integrity check failed for {path.name}: "
            f"expected {expected_hex[:16]}…, got {actual[:16]}…"
        )


# ── small helpers ─────────────────────────────────────────────────────────
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
                          f"{got // (1024*1024)}/{total // (1024*1024)} MB",
                          got * 100.0 / total)
                else:
                    _emit(progress, phase,
                          f"Downloading {label}… {got // (1024*1024)} MB")
    os.replace(tmp, dest)


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
           "profile": "", "detail": ""}
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
                    yt_n = con.execute(
                        "SELECT COUNT(*) FROM moz_cookies "
                        "WHERE host LIKE '%youtube.com%'").fetchone()[0]
                    ph = ",".join("?" * len(AUTH))
                    auth_n = con.execute(
                        "SELECT COUNT(*) FROM moz_cookies WHERE "
                        "(host LIKE '%youtube.com%' OR host LIKE '%google.com%') "
                        f"AND name IN ({ph})", AUTH).fetchone()[0]
                finally:
                    con.close()
                if yt_n > 0:
                    res["has_yt_cookies"] = True
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
    """True if the given Python 3.11 can import faster_whisper + torch."""
    if not py311 or not os.path.isfile(py311):
        return False
    try:
        si, cf = _no_window()
        r = subprocess.run(
            [py311, "-c", "import faster_whisper, torch"],
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
                    "detail": "faster-whisper + torch import OK" if whisper_ok
                              else ("Python 3.11 found - packages not verified"
                                    if py311 else "Python 3.11 not found")},
        "gpu": gpu,
        # YouTube auth: Firefox cookies (Chromium not supported on Windows).
        "cookies": cookies,
        # Convenience: are the must-haves for downloading present?
        "core_ok": bool(ytdlp and ffmpeg and ffprobe),
    }


# ── core installers (yt-dlp + ffmpeg) ──────────────────────────────────────
def install_ytdlp(progress: Progress | None = None, force: bool = False) -> dict:
    """Download yt-dlp.exe into the managed bin dir."""
    ensure_bin_on_path()
    if not force and _which("yt-dlp"):
        _emit(progress, "ytdlp", "yt-dlp already present.", 100, "ok")
        return {"ok": True, "skipped": True, "path": _which("yt-dlp")}
    dest = managed_bin_dir() / "yt-dlp.exe"
    try:
        _download(_YTDLP_URL, dest, progress, "ytdlp", "yt-dlp")
        # Verify against the SHA2-256SUMS file published alongside each release.
        _emit(progress, "ytdlp", "Verifying yt-dlp integrity…", None)
        try:
            sums_url = _YTDLP_URL.replace("/yt-dlp.exe", "/SHA2-256SUMS")
            sums_text = _fetch_text(sums_url)
            expected = None
            for line in sums_text.splitlines():
                parts = line.split()
                if len(parts) == 2 and parts[1].lower() == "yt-dlp.exe":
                    expected = parts[0]
                    break
            if expected:
                _verify_sha256(dest, expected)  # deletes + raises on mismatch
            else:
                _log.warning("SHA2-256SUMS has no yt-dlp.exe entry; skipping hash check")
        except RuntimeError:
            raise  # integrity mismatch — propagate to outer handler
        except Exception as e:
            _log.warning("yt-dlp hash check unavailable (%s); continuing", e)
        _emit(progress, "ytdlp", "yt-dlp installed.", 100, "ok")
        return {"ok": True, "path": str(dest)}
    except RuntimeError as e:
        _emit(progress, "ytdlp", f"yt-dlp integrity check failed: {e}", status="error")
        return {"ok": False, "error": str(e), "integrity_error": True}
    except Exception as e:
        _log.warning("yt-dlp install failed: %s", e)
        _emit(progress, "ytdlp", f"yt-dlp download failed: {e}", status="error")
        return {"ok": False, "error": str(e)}


def install_ffmpeg(progress: Progress | None = None, force: bool = False) -> dict:
    """Download the gyan.dev essentials zip; extract ffmpeg.exe + ffprobe.exe
    into the managed bin dir."""
    ensure_bin_on_path()
    if not force and _which("ffmpeg") and _which("ffprobe"):
        _emit(progress, "ffmpeg", "ffmpeg already present.", 100, "ok")
        return {"ok": True, "skipped": True}
    bin_dir = managed_bin_dir()
    zip_path = bin_dir / "_ffmpeg_dl.zip"
    try:
        _download(_FFMPEG_ZIP_URL, zip_path, progress, "ffmpeg", "ffmpeg")
        # Verify against gyan.dev's published .sha256 sidecar before extraction.
        _emit(progress, "ffmpeg", "Verifying ffmpeg integrity…", None)
        try:
            sha_text = _fetch_text(_FFMPEG_ZIP_URL + ".sha256")
            token = sha_text.split()[0] if sha_text.strip() else ""
            if len(token) == 64 and all(c in "0123456789abcdef" for c in token.lower()):
                _verify_sha256(zip_path, token)  # deletes + raises on mismatch
            else:
                _log.warning("ffmpeg .sha256 has unexpected format; skipping hash check")
        except RuntimeError:
            raise  # integrity mismatch
        except Exception as e:
            _log.warning("ffmpeg hash check unavailable (%s); continuing", e)
        _emit(progress, "ffmpeg", "Extracting ffmpeg…", None)
        wanted = {"ffmpeg.exe", "ffprobe.exe"}
        found: set[str] = set()
        with zipfile.ZipFile(zip_path, "r") as zf:
            for zn in zf.namelist():
                base = os.path.basename(zn)
                if base in wanted:
                    with zf.open(zn) as src, open(bin_dir / base, "wb") as dst:
                        shutil.copyfileobj(src, dst)
                    found.add(base)
        try:
            os.remove(zip_path)
        except OSError:
            pass
        if not {"ffmpeg.exe", "ffprobe.exe"} <= found:
            missing = ", ".join(sorted({"ffmpeg.exe", "ffprobe.exe"} - found))
            raise RuntimeError(f"zip missing {missing}")
        _emit(progress, "ffmpeg", "ffmpeg + ffprobe installed.", 100, "ok")
        return {"ok": True, "path": str(bin_dir / "ffmpeg.exe")}
    except RuntimeError as e:
        _log.warning("ffmpeg install failed: %s", e)
        try:
            if zip_path.exists():
                os.remove(zip_path)
        except OSError:
            pass
        is_integrity = "integrity check failed" in str(e)
        _emit(progress, "ffmpeg",
              f"ffmpeg {'integrity check' if is_integrity else 'install'} failed: {e}",
              status="error")
        return {"ok": False, "error": str(e), **({"integrity_error": True} if is_integrity else {})}
    except Exception as e:
        _log.warning("ffmpeg install failed: %s", e)
        try:
            if zip_path.exists():
                os.remove(zip_path)
        except OSError:
            pass
        _emit(progress, "ffmpeg", f"ffmpeg install failed: {e}", status="error")
        return {"ok": False, "error": str(e)}


def install_core(progress: Progress | None = None, force: bool = False) -> dict:
    """Install both core binaries. Returns a combined result + fresh probe."""
    y = install_ytdlp(progress, force=force)
    f = install_ffmpeg(progress, force=force)
    ensure_bin_on_path()
    state = probe()
    ok = bool(y.get("ok") and f.get("ok"))
    _emit(progress, "core",
          "Core tools ready." if state.get("core_ok") else "Some core tools missing.",
          100, "ok" if state.get("core_ok") else "error")
    return {"ok": ok, "ytdlp": y, "ffmpeg": f, "state": state}


# ── whisper stack installer ────────────────────────────────────────────────
def _run_streaming(cmd: list[str], progress: Progress | None, phase: str,
                   label: str, timeout: int = 2400) -> tuple[int, str]:
    """Run a subprocess, streaming stdout lines to progress. Returns
    (returncode, tail_of_output)."""
    si, cf = _no_window()
    _emit(progress, phase, f"{label}…", None)
    tail: list[str] = []
    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1, startupinfo=si, creationflags=cf)
    except Exception as e:
        return 1, str(e)
    try:
        assert proc.stdout is not None
        for line in proc.stdout:
            line = line.rstrip()
            if not line:
                continue
            tail.append(line)
            del tail[:-40]  # keep enough context for pip failure diagnostics
            # Surface meaningful pip lines without spamming every byte.
            low = line.lower()
            if any(k in low for k in ("downloading", "installing", "collecting",
                                      "building", "successfully", "error", "warning")):
                status = ("error" if "error" in low else
                          "warning" if "warning" in low else "running")
                _emit(progress, phase, f"{label}: {line[:120]}",
                      status=status)
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        try:
            proc.kill()
        except Exception:
            pass
        return 1, "timed out"
    except Exception as e:
        return 1, str(e)
    return proc.returncode or 0, "\n".join(tail)


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


def install_python311(progress: Progress | None = None) -> dict:
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
        try:
            expected = _fetch_py311_sha256(_PY311_URL)
            if expected:
                _verify_sha256(installer, expected)  # deletes + raises on mismatch
            else:
                _log.warning("python 3.11 sha256 unavailable from sigstore; skipping hash check")
        except RuntimeError:
            raise  # integrity mismatch — propagate to outer handler
        except Exception as e:
            _log.warning("python 3.11 hash check unavailable (%s); continuing", e)
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


def install_whisper_stack(progress: Progress | None = None) -> dict:
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

    # 1) upgrade pip (best-effort)
    rc_pip, tail_pip = _run_streaming(
        [python, "-m", "pip", "install", "--upgrade", "pip"],
        progress, "whisper", "Upgrading pip", timeout=300)
    if rc_pip != 0:
        _log.warning("pip upgrade failed during whisper setup: %s", tail_pip)
        _emit(progress, "whisper",
              "pip upgrade failed; continuing with existing pip",
              status="warning")

    # 2) torch (CUDA or CPU) — version-pinned to avoid silent ABI breaks
    if gpu["ok"]:
        torch_cmd = [python, "-m", "pip", "install", "torch>=2.1.0,<3.0",
                     "--index-url", _TORCH_CUDA_INDEX]
        torch_label = "Installing torch (CUDA)"
    else:
        torch_cmd = [python, "-m", "pip", "install", "torch>=2.1.0,<3.0"]
        torch_label = "Installing torch (CPU)"
    rc, tail = _run_streaming(torch_cmd, progress, "whisper", torch_label,
                              timeout=3600)
    if rc != 0:
        _emit(progress, "whisper", f"torch install failed: {tail[-160:]}",
              status="error")
        return {"ok": False, "stage": "torch", "error": tail[-400:]}

    # 3) faster-whisper + transformers — pinned to avoid silent compatibility breaks
    rc, tail = _run_streaming(
        [python, "-m", "pip", "install",
         "faster-whisper>=1.0.0,<2.0", "transformers>=4.36.0,<5.0"],
        progress, "whisper", "Installing faster-whisper + transformers",
        timeout=1800)
    if rc != 0:
        _emit(progress, "whisper",
              f"faster-whisper install failed: {tail[-160:]}", status="error")
        return {"ok": False, "stage": "faster-whisper", "error": tail[-400:]}

    # 4) verify
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


__all__ = [
    "managed_bin_dir", "ensure_bin_on_path", "probe", "detect_gpu",
    "install_ytdlp", "install_ffmpeg", "install_core",
    "install_python311", "install_whisper_stack",
]
