"""Metadata components remain usable without loading sync orchestration."""

from __future__ import annotations

import inspect
import io
import json
import os
import subprocess
import sys
from pathlib import Path
from unittest import mock


def test_metadata_import_does_not_load_sync_orchestrator(tmp_path):
    profile = tmp_path / "import-profile"
    local = tmp_path / "import-local"
    profile.mkdir()
    local.mkdir()
    env = dict(os.environ, APPDATA=str(profile), LOCALAPPDATA=str(local),
               PYTHONDONTWRITEBYTECODE="1")
    code = (
        "import json, sys; import backend.metadata; "
        "print(json.dumps([name for name in sys.modules "
        "if name == 'backend.sync' or name.startswith('backend.sync.')]))"
    )
    result = subprocess.run(
        [sys.executable, "-c", code], env=env,
        cwd=Path(__file__).resolve().parents[1], capture_output=True,
        text=True, timeout=20,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout.strip().splitlines()[-1]) == []


def test_legacy_exports_preserve_real_helper_signatures():
    from backend.metadata import _refresh_proxies, catalog, core, durations

    for name, owner in (
        ("_flat_playlist_bulk_stats", catalog),
        ("_resolve_ids_by_title", catalog),
        ("_probe_file_duration", durations),
        ("_probe_durations_bulk", durations),
    ):
        implementation = getattr(owner, name)
        assert getattr(core, name) is implementation
        assert getattr(_refresh_proxies, name) is implementation
        assert all(p.kind not in (p.VAR_POSITIONAL, p.VAR_KEYWORD)
                   for p in inspect.signature(implementation).parameters.values())


def test_catalog_title_matching_keeps_ambiguous_candidates_unbound(tmp_path):
    from backend.metadata import catalog

    paths = [str(tmp_path / name) for name in ("Unique.mp4", "Repeated.mp4")]
    process = mock.Mock(returncode=0, pid=None)
    process.poll.return_value = 0
    process.stdout = io.StringIO(
        "abcdefghijk\tUnique\nlmnopqrstuv\tRepeated\nwxyzABCDEFG\tRepeated\n")
    process.stderr = None
    with mock.patch.object(catalog.youtube_traffic, "acquire", return_value={"ok": True}), \
            mock.patch.object(catalog, "_find_cookie_source", return_value=[]), \
            mock.patch.object(catalog, "popen_ytdlp", return_value=process), \
            mock.patch("backend.youtube_session.handle_youtube_failure_text", return_value=""):
        result = catalog._resolve_ids_by_title(
            "yt-dlp", "https://example.invalid/channel", paths, mock.Mock())
    assert result == {paths[0]: "abcdefghijk"}


def test_shared_folder_and_format_helpers_preserve_legacy_exports():
    from backend import ytdlp_options
    from backend.sync import ytdlp_proc

    assert ytdlp_proc.sanitize_folder is ytdlp_options.sanitize_folder
    assert ytdlp_proc._find_cookie_source is ytdlp_options._find_cookie_source
    assert ytdlp_proc.sanitize_folder("CON") == "_CON"
    assert ytdlp_proc.sanitize_folder("A/B .") == "A_B"
    assert "height<=720" in ytdlp_proc.build_format_string("720")
