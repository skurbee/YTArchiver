from __future__ import annotations

import atexit
import os
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from unittest import mock

import pytest

_TEST_APPDATA = tempfile.TemporaryDirectory(
    prefix="ytarchiver-patch4-process-tests-")
atexit.register(_TEST_APPDATA.cleanup)
os.environ["APPDATA"] = _TEST_APPDATA.name
Path(_TEST_APPDATA.name, "YTArchiver").mkdir(parents=True, exist_ok=True)

from backend import process_runner


class _CooperativeProcess:
    pid = None

    def __init__(self):
        self.alive = True
        self.returncode = None
        self.terminate_calls = 0
        self.kill_calls = 0

    def poll(self):
        return None if self.alive else self.returncode

    def terminate(self):
        self.terminate_calls += 1
        self.alive = False
        self.returncode = -15

    def kill(self):
        self.kill_calls += 1
        self.alive = False
        self.returncode = -9

    def wait(self, timeout=None):
        if self.alive:
            raise subprocess.TimeoutExpired("fake", timeout)
        return self.returncode


class _StubbornProcess(_CooperativeProcess):
    """Ignores terminate and consumes its wait timeout until killed."""

    def terminate(self):
        self.terminate_calls += 1

    def wait(self, timeout=None):
        if not self.alive:
            return self.returncode
        time.sleep(max(0.0, float(timeout or 0.0)))
        if self.alive:
            raise subprocess.TimeoutExpired("stubborn", timeout)
        return self.returncode


def test_registry_snapshot_and_owner_stop_preserve_unrelated_processes():
    registry = process_runner.ProcessRegistry()
    sync = _CooperativeProcess()
    manual = _CooperativeProcess()
    gpu = _CooperativeProcess()
    registry.register(
        sync, owner="sync", task_id="sync-1", role="download")
    registry.register(
        manual, owner="manual-download", task_id="manual-1",
        role="download")
    registry.register(
        gpu, owner="gpu", task_id="gpu-1", role="whisper")

    snapshot = registry.snapshot(owner="sync")
    assert len(snapshot) == 1
    assert snapshot[0].proc is sync
    assert snapshot[0].task_id == "sync-1"
    assert snapshot[0].role == "download"

    assert registry.terminate_owner("sync", timeout=0.2) == 1
    assert sync.terminate_calls == 1
    assert manual.terminate_calls == 0
    assert gpu.terminate_calls == 0
    assert registry.alive_count(owner="sync") == 0
    assert registry.alive_count(owner="manual-download") == 1
    assert registry.alive_count(owner="gpu") == 1

    registry.kill_all(timeout=0.2)


def test_registry_terminate_job_uses_stable_task_id_not_owner_wildcard():
    registry = process_runner.ProcessRegistry()
    first = _CooperativeProcess()
    second = _CooperativeProcess()
    registry.register(first, owner="sync", task_id="channel-a")
    registry.register(second, owner="sync", task_id="channel-b")

    assert registry.terminate_job(
        "channel-a", owner="sync", timeout=0.2) == 1
    assert first.terminate_calls == 1
    assert second.terminate_calls == 0
    assert [row.task_id for row in registry.snapshot(owner="sync")] == [
        "channel-b"]

    registry.kill_all(timeout=0.2)


def test_registry_re_registration_updates_metadata_without_duplicates():
    registry = process_runner.ProcessRegistry()
    proc = _CooperativeProcess()

    registry.register(proc)
    registry.register(proc, owner="sync", task_id="stable", role="catalog")

    snapshot = registry.snapshot()
    assert len(snapshot) == 1
    assert snapshot[0].owner == "sync"
    assert snapshot[0].task_id == "stable"
    assert snapshot[0].role == "catalog"
    registry.kill_all(timeout=0.2)


def test_kill_all_uses_one_global_deadline_for_many_stubborn_processes():
    registry = process_runner.ProcessRegistry()
    procs = [_StubbornProcess() for _ in range(3)]
    for index, proc in enumerate(procs):
        registry.register(proc, owner="test", task_id=str(index))

    started = time.monotonic()
    assert registry.kill_all(timeout=0.25) == 3
    elapsed = time.monotonic() - started

    # Sequential per-process waits would take at least 0.75 seconds.
    assert elapsed < 0.5
    assert all(proc.terminate_calls == 1 for proc in procs)
    assert all(proc.kill_calls == 1 for proc in procs)


def _pid_running(pid: int) -> bool:
    try:
        import psutil
        proc = psutil.Process(pid)
        return bool(proc.is_running() and proc.status() != psutil.STATUS_ZOMBIE)
    except Exception:
        return False


def _kill_exact_pid(pid: int) -> None:
    try:
        import psutil
        proc = psutil.Process(pid)
        proc.kill()
        proc.wait(timeout=2)
    except Exception:
        pass


def test_owner_stop_kills_exact_registered_tree_but_not_sibling(tmp_path):
    pytest.importorskip("psutil")
    child_pid_file = tmp_path / "owned-child.pid"
    child_code = "import time; time.sleep(60)"
    root_code = (
        "import pathlib, subprocess, sys, time; "
        "child=subprocess.Popen([sys.executable, '-c', sys.argv[2]]); "
        "pathlib.Path(sys.argv[1]).write_text(str(child.pid), encoding='ascii'); "
        "time.sleep(60)"
    )
    creationflags = process_runner.subprocess_creationflags()
    root = subprocess.Popen(
        [sys.executable, "-c", root_code, str(child_pid_file), child_code],
        creationflags=creationflags,
    )
    sibling = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)"],
        creationflags=creationflags,
    )
    child_pid = 0
    registry = process_runner.ProcessRegistry()
    try:
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and not child_pid_file.exists():
            time.sleep(0.02)
        assert child_pid_file.exists()
        child_pid = int(child_pid_file.read_text(encoding="ascii"))
        assert _pid_running(child_pid)
        assert sibling.poll() is None

        registry.register(
            root, owner="sync", task_id="tree-1", role="download")
        assert registry.terminate_owner("sync", timeout=3.0) == 1

        assert root.poll() is not None
        child_deadline = time.monotonic() + 2.0
        while time.monotonic() < child_deadline and _pid_running(child_pid):
            time.sleep(0.02)
        assert not _pid_running(child_pid)
        assert sibling.poll() is None
    finally:
        registry.kill_all(timeout=0.5)
        if root.poll() is None:
            root.kill()
            root.wait(timeout=2)
        if sibling.poll() is None:
            sibling.kill()
            sibling.wait(timeout=2)
        if child_pid:
            _kill_exact_pid(child_pid)


def test_streaming_timeout_stops_a_silent_child(monkeypatch):
    monkeypatch.setattr(
        process_runner.youtube_traffic, "acquire",
        lambda *_args, **_kwargs: {"ok": True},
    )
    registry = process_runner.ProcessRegistry()
    runner = process_runner.YtDlpRunner(registry=registry)

    started = time.monotonic()
    result = runner.run_streaming(
        [sys.executable, "-c", "import time; time.sleep(60)"],
        timeout=0.2,
        owner="sync",
        task_id="silent-timeout",
    )
    elapsed = time.monotonic() - started

    assert result.timed_out
    assert not result.cancelled
    assert result.returncode != 0
    assert elapsed < 3.0
    assert registry.alive_count() == 0


def test_streaming_cancel_stops_a_silent_child_without_waiting_for_output(
        monkeypatch):
    monkeypatch.setattr(
        process_runner.youtube_traffic, "acquire",
        lambda *_args, **_kwargs: {"ok": True},
    )
    registry = process_runner.ProcessRegistry()
    runner = process_runner.YtDlpRunner(registry=registry)
    cancel = threading.Event()
    timer = threading.Timer(0.15, cancel.set)
    timer.start()
    try:
        started = time.monotonic()
        result = runner.run_streaming(
            [sys.executable, "-c", "import time; time.sleep(60)"],
            timeout=10.0,
            cancel_event=cancel,
            owner="sync",
            task_id="silent-cancel",
        )
        elapsed = time.monotonic() - started
    finally:
        timer.cancel()
        timer.join(timeout=1.0)

    assert result.cancelled
    assert not result.timed_out
    assert result.returncode != 0
    assert elapsed < 3.0
    assert registry.alive_count() == 0


def test_streaming_bounded_reader_drains_heavy_output(monkeypatch):
    monkeypatch.setattr(
        process_runner.youtube_traffic, "acquire",
        lambda *_args, **_kwargs: {"ok": True},
    )
    lines: list[str] = []
    runner = process_runner.YtDlpRunner(
        registry=process_runner.ProcessRegistry())
    code = (
        "import sys; "
        "[print(f'line-{i}') for i in range(1500)]; "
        "[print(f'err-{i}', file=sys.stderr) for i in range(250)]"
    )

    result = runner.run_streaming(
        [sys.executable, "-c", code],
        timeout=10.0,
        on_stdout_line=lines.append,
        owner="test",
        task_id="heavy-output",
    )

    assert result.returncode == 0
    assert not result.cancelled
    assert not result.timed_out
    assert len(lines) == 1500
    assert len(result.stderr_tail) == 200
    assert result.stderr_tail[-1] == "err-249"


def test_popen_ytdlp_forwards_owner_metadata_to_registry():
    proc = mock.Mock()
    registry = mock.Mock()

    with mock.patch.object(process_runner.subprocess, "Popen",
                           return_value=proc):
        result = process_runner.popen_ytdlp(
            ["yt-dlp", "url"], registry=registry,
            owner="sync", task_id="sync-9", role="download")

    assert result is proc
    registry.register.assert_called_once_with(
        proc, owner="sync", task_id="sync-9", role="download")


def test_process_owner_scope_labels_legacy_sync_launches():
    proc = mock.Mock()
    registry = mock.Mock()

    with mock.patch.object(process_runner.subprocess, "Popen",
                           return_value=proc), \
            process_runner.process_owner_scope("sync", "sync-legacy-1"):
        process_runner.popen_ytdlp(
            ["yt-dlp", "url"], registry=registry, role="catalog")

    registry.register.assert_called_once_with(
        proc, owner="sync", task_id="sync-legacy-1", role="catalog")


def test_managed_task_supplies_owner_context_to_legacy_child_registration():
    from backend.services.job_supervisor import JobSupervisor
    from backend.services.managed_work import start_managed_task

    api = mock.Mock()
    api._job_supervisor = JobSupervisor()
    registry = process_runner.ProcessRegistry()
    proc = _CooperativeProcess()

    worker = start_managed_task(
        api,
        owner="metadata",
        label="Refresh metadata",
        task_id="metadata-task-7",
        target=lambda: registry.register(proc, role="catalog"),
        name="managed-owner-context",
    )
    worker.join(1.0)

    snapshot = registry.snapshot()
    assert len(snapshot) == 1
    assert snapshot[0].owner == "metadata"
    assert snapshot[0].task_id == "metadata-task-7"
    assert snapshot[0].role == "catalog"
    registry.kill_all(timeout=0.2)


def test_sync_force_stop_leaves_manual_gpu_and_updater_processes_alive(
        monkeypatch):
    from backend.api_mixins import sync_mixin

    registry = process_runner.ProcessRegistry()
    sync = _CooperativeProcess()
    manual = _CooperativeProcess()
    gpu = _CooperativeProcess()
    updater = _CooperativeProcess()
    registry.register(sync, owner="sync", task_id="sync-job")
    registry.register(
        manual, owner="manual-download", task_id="manual-job")
    registry.register(gpu, owner="gpu", task_id="gpu-job")
    registry.register(
        updater, owner="ytdlp-updater", task_id="updater-job")
    monkeypatch.setattr(sync_mixin, "PROCESS_REGISTRY", registry)

    api = sync_mixin.SyncMixin()
    api.sync_clear_queue = mock.Mock(
        return_value={"ok": True, "removed": 4})
    api._log_stream = mock.Mock()
    api._on_queue_changed = mock.Mock()
    try:
        result = api.sync_force_stop()

        assert result == {"ok": True, "removed": 4, "killed": 1}
        assert sync.poll() is not None
        assert manual.poll() is None
        assert gpu.poll() is None
        assert updater.poll() is None
        assert {record.owner for record in registry.snapshot()} == {
            "manual-download", "gpu", "ytdlp-updater"}
    finally:
        registry.kill_all(timeout=0.2)


def test_dependency_installer_assigns_unique_owned_job_metadata(monkeypatch):
    from backend import deps_installer

    proc = mock.Mock()
    proc.stdout = None
    proc.stderr = None
    supervisor = mock.Mock(
        return_value=process_runner.StreamingRunResult(0, []))
    monkeypatch.setattr(deps_installer.subprocess, "Popen", mock.Mock(
        return_value=proc))
    monkeypatch.setattr(
        deps_installer, "supervise_streaming_process", supervisor)

    rc, tail = deps_installer._run_streaming(
        ["python", "-m", "pip"], None, "whisper", "Installing", timeout=7)

    assert (rc, tail) == (0, "")
    kwargs = supervisor.call_args.kwargs
    assert kwargs["owner"] == "dependency-install"
    assert kwargs["role"] == "whisper"
    assert kwargs["timeout"] == 7
    assert kwargs["task_id"].startswith("dependency-install-")


def test_archive_manual_download_uses_supervisor_owner_and_timeout(
        monkeypatch, tmp_path):
    from backend.api_mixins import archive_mixin

    class ImmediateThread:
        def __init__(self, target, **_kwargs):
            self.target = target

        def start(self):
            self.target()

    api = archive_mixin.ArchiveMixin()
    api._log_stream = mock.Mock()
    api._window = None
    api._push_url_history = mock.Mock()
    api._push_recent_refresh = mock.Mock()
    proc = mock.Mock()
    supervisor = mock.Mock(
        return_value=process_runner.StreamingRunResult(1, []))
    popen = mock.Mock(return_value=proc)
    monkeypatch.setattr(archive_mixin.threading, "Thread", ImmediateThread)
    monkeypatch.setattr(
        archive_mixin.sync_backend, "find_yt_dlp", lambda: "yt-dlp")
    monkeypatch.setattr(
        archive_mixin.sync_backend, "build_format_string", lambda _r: "best")
    monkeypatch.setattr(
        archive_mixin.sync_backend, "_find_cookie_source", list)
    monkeypatch.setattr(
        archive_mixin, "load_config",
        lambda: {"video_out_dir": str(tmp_path)})
    monkeypatch.setattr(
        archive_mixin, "_probe_output_folder_writable", lambda _p: None)
    monkeypatch.setattr(
        archive_mixin.youtube_traffic, "is_youtube_url", lambda _u: False)
    monkeypatch.setattr(archive_mixin, "popen_ytdlp", popen)
    monkeypatch.setattr(
        archive_mixin, "supervise_streaming_process", supervisor)

    result = api.archive_single_video("https://example.com/video")

    assert result["ok"] and result["started"]
    assert result["task_id"].startswith("manual-download-")
    launch_kwargs = popen.call_args.kwargs
    assert launch_kwargs["owner"] == "manual-download"
    assert launch_kwargs["task_id"] == result["task_id"]
    assert launch_kwargs["role"] == "download"
    supervise_kwargs = supervisor.call_args.kwargs
    assert supervise_kwargs["owner"] == "manual-download"
    assert supervise_kwargs["task_id"] == result["task_id"]
    assert supervise_kwargs["role"] == "download"
    assert supervise_kwargs["timeout"] == 900
    assert isinstance(supervise_kwargs["cancel_event"], threading.Event)
    assert not api.archive_single_is_running()


def test_archive_manual_download_cancel_sets_exact_job_event():
    from backend.api_mixins.archive_mixin import ArchiveMixin

    api = ArchiveMixin()
    api._ensure_archive_single_tracking()
    first = threading.Event()
    second = threading.Event()
    with api._archive_single_lock:
        api._archive_single_cancel_events.update({
            "manual-download-one": first,
            "manual-download-two": second,
        })

    result = api.archive_single_cancel("manual-download-one")

    assert result == {"ok": True, "cancelled": 1}
    assert first.is_set()
    assert not second.is_set()


def test_updater_streaming_child_has_owner_job_timeout_and_cancel(monkeypatch):
    from backend.api_mixins.settings_mixin import SettingsMixin

    class Stub(SettingsMixin):
        def _settings_log_stream(self):
            return self.stream

        def _push_ytdlp_update_status(self, *_args, **_kwargs):
            return None

    class FinishedProcess:
        stdout = None
        stderr = None
        returncode = 0

        def wait(self, timeout=None):
            return self.returncode

    api = Stub()
    api.stream = mock.Mock()
    api._ensure_ytdlp_update_runtime()
    api._ytdlp_update_running = True
    cancel = threading.Event()
    payload = {
        "yt": "yt-dlp",
        "channel": "stable",
        "automatic": False,
        "record_check": False,
        "task_id": "ytdlp-update-test",
        "cancel_event": cancel,
    }
    supervisor = mock.Mock(
        return_value=process_runner.StreamingRunResult(0, []))
    monkeypatch.setattr(subprocess, "Popen", mock.Mock(
        return_value=FinishedProcess()))
    monkeypatch.setattr(
        process_runner, "supervise_streaming_process", supervisor)

    api._run_ytdlp_update(payload)

    kwargs = supervisor.call_args.kwargs
    assert kwargs["owner"] == "ytdlp-updater"
    assert kwargs["task_id"] == "ytdlp-update-test"
    assert kwargs["role"] == "self-update"
    assert kwargs["timeout"] == 900
    assert kwargs["cancel_event"] is cancel


def test_ffprobe_post_kill_communicate_is_bounded(monkeypatch):
    class _WedgedProbe(_StubbornProcess):
        stdout = None
        stderr = None

        def __init__(self):
            super().__init__()
            self.communicate_timeouts = []

        def communicate(self, timeout=None):
            self.communicate_timeouts.append(timeout)
            raise subprocess.TimeoutExpired("ffprobe", timeout, output="partial")

    proc = _WedgedProbe()
    monkeypatch.setattr(process_runner.subprocess, "Popen", lambda *_a, **_k: proc)
    runner = process_runner.FfmpegRunner(
        registry=process_runner.ProcessRegistry())

    rc, out, err = runner.probe_capture(
        ["ffprobe", "file.mp4"], timeout=0.05,
        owner="metadata", task_id="probe-1")

    assert rc == -1
    assert out == "partial"
    assert err == "timeout"
    assert proc.communicate_timeouts == [0.05, 1.0]
    assert proc.kill_calls == 1
