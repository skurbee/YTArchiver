"""Result lifetime is shared by all four real asynchronous API adapters."""
from __future__ import annotations

import threading
from types import SimpleNamespace

import pytest

from backend.api_mixins import channel_mixin, media_ops_mixin, subs_mixin
from backend.services.operation_results import OperationLimitError, OperationResults


class Api(channel_mixin.ChannelMixin, media_ops_mixin.MediaOpsMixin, subs_mixin.SubsMixin):
    pass


@pytest.fixture
def api_fixture(monkeypatch, tmp_path):
    from backend import drift_scan, redownload, sync
    channel = {"name": "Example", "folder": "Example"}
    folder = tmp_path / "Example"
    folder.mkdir()
    (folder / "video.mp4").write_bytes(b"synthetic fixture")
    api = Api()
    api._config = {"output_dir": str(tmp_path)}
    api._log_stream = SimpleNamespace(emit_text=lambda *_a: None, flush=lambda: None)
    api._operation_results = OperationResults()
    queued = []
    released = []
    monkeypatch.setattr(subs_mixin.subs_backend, "get_channel", lambda _identity: channel)
    monkeypatch.setattr(sync, "find_yt_dlp", lambda: "fake-ytdlp")
    monkeypatch.setattr(sync, "_find_cookie_source", list)
    monkeypatch.setattr(subs_mixin.youtube_traffic, "acquire",
                        lambda _kind, *, cancel_event=None: {"ok": True})
    monkeypatch.setattr(subs_mixin, "run_ytdlp", lambda *_a, **_k: SimpleNamespace(stdout="Example\n"))
    monkeypatch.setattr(redownload, "_ffprobe_media_info", lambda _path: (1280, 720, ""))
    monkeypatch.setattr(drift_scan, "scan_channel", lambda *_a: {"ok": True, "issues": ["sample"]})
    monkeypatch.setattr(drift_scan, "apply_channel", lambda *_a, **_k: {"ok": True, "actions": {}})
    monkeypatch.setattr(media_ops_mixin, "try_global_archive_lease", lambda **_k: SimpleNamespace(
        ok=True, lease=SimpleNamespace(release=lambda: released.append(True))))
    for module in (channel_mixin, media_ops_mixin, subs_mixin):
        monkeypatch.setattr(module, "start_managed_task", lambda _api, **kw: queued.append(kw["target"]))
    return api, queued, released


def submit(api, family):
    if family == "preview":
        return api.subs_preview_folder_name("https://www.youtube.com/@Example")
    if family == "resolution":
        return api.chan_scan_resolution_mismatch("Example", "720")
    if family == "drift-scan":
        return api.drift_scan_channel({"name": "Example"})
    return api.drift_apply_channel({"name": "Example"})


def poll(api, family, token):
    method = {"preview": api.subs_preview_folder_poll,
              "resolution": api.chan_scan_resolution_mismatch_poll,
              "drift-scan": api.drift_scan_channel_poll,
              "drift-apply": api.drift_apply_channel_poll}[family]
    return method(token)


@pytest.mark.parametrize("family", ["preview", "resolution", "drift-scan", "drift-apply"])
def test_each_api_retains_completed_response_for_repeat_poll_and_expires_it(api_fixture, family):
    api, queued, released = api_fixture
    now = [0.0]
    api._operation_results = OperationResults(ttl=10, clock=lambda: now[0])
    response = submit(api, family)
    assert response["ok"]
    token = response["token"]
    assert poll(api, family, token)["pending"]
    now[0] = 100  # Slow pending work is not aged out as if completed.
    assert poll(api, family, token)["pending"]
    queued.pop()()
    first = poll(api, family, token)
    assert first["ok"] and not first.get("pending"), first
    assert poll(api, family, token) == first
    first["injected"] = True
    assert "injected" not in poll(api, family, token)
    if family.startswith("drift"):
        assert released == [True]
    now[0] = 111
    assert poll(api, family, token) == {"ok": False, "error": "unknown token"}


@pytest.mark.parametrize("family", ["preview", "resolution", "drift-scan", "drift-apply"])
def test_each_api_discards_failed_launches(api_fixture, monkeypatch, family):
    api, _queued, released = api_fixture

    def fail(*_args, **_kwargs):
        raise RuntimeError("could not start")

    for module in (channel_mixin, media_ops_mixin, subs_mixin):
        monkeypatch.setattr(module, "start_managed_task", fail)
    result = submit(api, family)
    assert not result["ok"]
    assert "could not start" in result["error"]
    assert api._operation_results._records == {}
    if family.startswith("drift"):
        assert released == [True]


def test_registry_bounds_completed_records_and_admits_no_unowned_pending_work():
    now = [0.0]
    registry = OperationResults(max_completed=2, max_pending=1, clock=lambda: now[0])
    registry.begin("scan", "pending")
    with pytest.raises(OperationLimitError):
        registry.begin("scan", "rejected")
    for number in range(3):
        token = "pending" if number == 0 else str(number)
        if number:
            registry.begin("scan", token)
        registry.complete(token, {"ok": False, "error": "worker failed"})
        now[0] += 1
    assert len(registry._records) == 2
    assert registry.poll("scan", "pending")["error"] == "unknown token"
    assert registry.poll("scan", "2")["error"] == "worker failed"
    assert registry.poll("other", "2")["error"] == "unknown token"


def test_active_cancellation_waits_for_worker_acknowledgement():
    registry = OperationResults()
    cancel, entered, release = (threading.Event() for _ in range(3))
    registry.begin("scan", "running")

    def work():
        entered.set()
        assert release.wait(2)
        return {"ok": False, "cancelled": True, "processed": 2}

    worker = threading.Thread(target=lambda: registry.run("running", work))
    worker.start()
    assert entered.wait(1)
    try:
        cancel.set()
        assert registry.poll("scan", "running")["pending"]
    finally:
        release.set()
        worker.join(2)
    assert registry.poll("scan", "running")["processed"] == 2
    registry.begin("scan", "skipped")
    assert registry.poll("scan", "skipped")["pending"]
    registry.cancelled_before_start("skipped")
    assert registry.poll("scan", "skipped")["cancelled"]


def test_exception_result_is_retained_with_completion_time():
    registry = OperationResults()
    registry.begin("scan", "failed")

    def fail():
        raise OSError("unreadable")

    registry.run("failed", fail)
    assert registry.poll("scan", "failed") == {"ok": False, "error": "unreadable"}
    assert registry.poll("scan", "failed") == {"ok": False, "error": "unreadable"}
    assert registry._records["failed"].completed_at is not None


@pytest.mark.parametrize("family", ["drift-scan", "drift-apply"])
def test_cancelled_registered_scan_releases_lease_before_worker_enters(api_fixture, monkeypatch, family):
    from backend.services.job_supervisor import JobSupervisor
    from backend.services.managed_work import start_managed_task

    api, _queued, released = api_fixture
    api._job_supervisor = JobSupervisor()
    monkeypatch.setattr(media_ops_mixin, "start_managed_task", start_managed_task)
    held, release = threading.Event(), threading.Event()
    original_start = threading.Thread.start

    def hold_start(thread):
        if thread.name == ("drift-scan-channel" if family == "drift-scan" else "drift-apply-channel"):
            held.set()
            assert release.wait(2)
        return original_start(thread)

    monkeypatch.setattr(threading.Thread, "start", hold_start)
    response = {}
    launcher = threading.Thread(target=lambda: response.update(submit(api, family)))
    launcher.start()
    try:
        assert held.wait(1)
        api._job_supervisor.close_admission("restore")
        api._job_supervisor.prepare_all()
    finally:
        release.set()
        launcher.join(2)
    assert not api._job_supervisor.join_until(1)
    assert released == [True]
    assert poll(api, family, response["token"])["cancelled"]
