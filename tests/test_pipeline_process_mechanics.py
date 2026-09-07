"""Subprocess mechanics with synthetic children and disposable filesystem state."""
from __future__ import annotations

import io
import subprocess
import threading
import time
from types import SimpleNamespace

import pytest

from backend import process_runner
from backend.process_runner import ProcessOutputReader, ProcessRegistry, supervise_streaming_process


class _Pipe:
    def __init__(self, release, *, fail=False, prefix=""):
        self.release = release
        self.fail = fail
        self.prefix = prefix
        self.close_calls = 0

    def readline(self):
        if self.prefix:
            line, self.prefix = self.prefix, ""
            return line
        if self.fail:
            raise OSError("synthetic pipe read failure")
        self.release.wait(5)
        return ""

    def close(self):
        self.close_calls += 1
        assert self.release.is_set(), "must not close a pipe held by its reader"


class _Child:
    pid = None

    def __init__(self, *, output=None, exited=False):
        self.release = threading.Event()
        self.returncode = 0 if exited else None
        self.stdout = output if output is not None else _Pipe(self.release)
        self.stderr = None
        self.stops = 0

    def poll(self):
        return self.returncode

    def exit(self, code=0):
        self.returncode = code
        self.release.set()

    def terminate(self):
        self.stops += 1
        self.exit(-15)

    def kill(self):
        self.stops += 1
        self.exit(-9)

    def wait(self, timeout=None):
        if self.returncode is None:
            raise subprocess.TimeoutExpired("synthetic child", timeout)
        return self.returncode


def test_pause_excludes_deadline_and_preserves_callback_order():
    pause = threading.Event()
    pause.set()
    child = _Child(output=io.StringIO("first\nsecond\n"))
    received = []
    transitions = []
    resume = threading.Timer(0.3, pause.clear)
    resume.start()
    def consume(line):
        assert not pause.is_set()
        received.append(line)
        if line == "second":
            child.exit()
    try:
        result = supervise_streaming_process(
            child, registry=ProcessRegistry(), on_stdout_line=consume,
            timeout=0.15, idle_timeout=0.15, pause_event=pause,
            on_pause_change=transitions.append,
        )
    finally:
        resume.cancel()
        resume.join()
    assert received == ["first", "second"]
    assert transitions == [True, False]
    assert result.output_complete
    assert not result.timed_out
    assert child.stops == 0


def test_cancel_while_paused_does_not_deliver_buffered_callbacks():
    pause = threading.Event()
    pause.set()
    cancel = threading.Event()
    child = _Child(output=io.StringIO("buffered\n"))
    received = []
    transitions = []
    timer = threading.Timer(0.15, cancel.set)
    timer.start()
    try:
        result = supervise_streaming_process(
            child, registry=ProcessRegistry(), on_stdout_line=received.append,
            cancel_event=cancel, pause_event=pause, on_pause_change=transitions.append,
        )
    finally:
        timer.cancel()
        timer.join()
    assert result.cancelled and not result.output_complete
    assert transitions == [True, False]
    assert received == []
    assert child.stops > 0


def test_pipe_error_is_not_reported_as_complete_output():
    release = threading.Event()
    release.set()
    child = _Child(output=_Pipe(release, fail=True, prefix="partial\n"), exited=True)
    received = []
    result = supervise_streaming_process(child, registry=ProcessRegistry(), on_stdout_line=received.append)
    assert result.returncode == 0
    assert received == ["partial"]
    assert not result.output_complete


def test_failed_consumer_does_not_report_successful_complete_output():
    child = _Child(output=io.StringIO("valid\n"), exited=True)
    def consume(line):
        raise ValueError("synthetic parser failure")
    result = supervise_streaming_process(
        child, registry=ProcessRegistry(), on_stdout_line=consume)
    assert result.returncode == 0
    assert not result.output_complete


def test_slow_post_exit_backlog_is_drained_without_losing_completeness():
    expected = [str(value) for value in range(600)]
    child = _Child(output=io.StringIO("\n".join(expected) + "\n"), exited=True)
    received = []
    def consume(line):
        time.sleep(0.002)
        received.append(line)
    result = supervise_streaming_process(
        child, registry=ProcessRegistry(), on_stdout_line=consume)
    assert received == expected
    assert result.output_complete
    assert not result.timed_out


def test_explicit_deadline_still_bounds_post_exit_backlog():
    child = _Child(output=io.StringIO("item\n" * 600), exited=True)
    received = []
    def consume(line):
        time.sleep(0.005)
        received.append(line)
    result = supervise_streaming_process(
        child, registry=ProcessRegistry(), on_stdout_line=consume, timeout=0.1)
    assert len(received) < 600
    assert result.timed_out
    assert not result.output_complete


def test_held_pipe_has_bounded_drain_and_is_not_closed_while_reader_blocks():
    child = _Child(exited=True)
    started = time.monotonic()
    try:
        result = supervise_streaming_process(child, registry=ProcessRegistry())
        assert time.monotonic() - started < 3
        assert not result.output_complete
        assert child.stdout.close_calls == 0
    finally:
        child.release.set()


def test_finalizer_does_not_close_a_still_blocked_output_reader():
    child = _Child(exited=True)
    reader = ProcessOutputReader(child).start()
    try:
        reader.close(timeout=0.01)
        result = process_runner.finish_owned_process(
            child, registry=ProcessRegistry(), output_reader=reader)
        assert result == 0
        assert child.stdout.close_calls == 0
    finally:
        child.release.set()


def test_binary_reader_preserves_original_line_bytes():
    child = _Child(output=io.BytesIO(b"title\x96legacy\r\n"), exited=True)
    reader = ProcessOutputReader(child).start()
    try:
        assert reader.read(timeout=1) == ("stdout", b"title\x96legacy\r\n")
    finally:
        reader.close()


@pytest.mark.parametrize("broken", [False, True])
def test_sync_adapter_preserves_bytes_and_reports_incomplete_pipe(broken):
    from backend.sync.ytdlp_session import iter_download_output
    release = threading.Event()
    release.set()
    data = b"title\x96legacy\r\n"
    output = _Pipe(release, fail=True, prefix=data) if broken else io.BytesIO(data)
    child = _Child(output=output, exited=True)
    watchdog = SimpleNamespace(stop_event=threading.Event(), output_reader=None,
                               output_complete=False)
    assert list(iter_download_output(child, watchdog)) == [data]
    assert watchdog.output_complete == (not broken)


@pytest.mark.parametrize("broken", [False, True, "nonzero"])
def test_redownload_catalog_requires_complete_output(monkeypatch, broken):
    from backend import redownload, youtube_session
    release = threading.Event()
    release.set()
    output = (_Pipe(release, fail=True, prefix="aaaaaaaaaaa|||Partial\n") if broken is True else
              io.StringIO("aaaaaaaaaaa|||Same\nbbbbbbbbbbb|||Same\nccccccccccc|||Unique\n"))
    child = _Child(output=output, exited=True)
    if broken == "nonzero":
        child.exit(1)
    monkeypatch.setattr(redownload, "popen_ytdlp", lambda *args, **kwargs: child)
    monkeypatch.setattr(redownload, "find_yt_dlp", lambda: "fake-tool")
    monkeypatch.setattr(redownload, "_find_cookie_source", list)
    monkeypatch.setattr(redownload.youtube_traffic, "acquire", lambda *args, **kwargs: {"ok": True})
    monkeypatch.setattr(youtube_session, "handle_youtube_failure_text", lambda *args, **kwargs: "")
    stream = SimpleNamespace(emit=lambda *args: None, emit_error=lambda *args: None)
    result = redownload._fetch_yt_catalog("https://example.invalid/channel", threading.Event(), None, stream)
    assert result == ({} if broken else {"Same": "", "Unique": "ccccccccccc"})


@pytest.mark.parametrize("cancelled", [False, True])
def test_compression_incomplete_or_cancelled_output_preserves_original(monkeypatch, tmp_path, cancelled):
    from backend import compress
    source = tmp_path / "source.mp4"
    source.write_bytes(b"original media")
    cancel = threading.Event()
    child = _Child()
    monkeypatch.setattr(compress, "find_ffmpeg", lambda: "fake-ffmpeg")
    monkeypatch.setattr(compress, "get_video_duration", lambda *args: 60.0)
    monkeypatch.setattr(compress, "get_video_codec", lambda *args: "h264")
    def launch(cmd, **kwargs):
        from pathlib import Path
        Path(cmd[-1]).write_bytes(b"incomplete media")
        if cancelled:
            cancel.set()
        else:
            child.exit(0)
            child.stderr = _Pipe(child.release, fail=True)
            child.stdout = None
        return child
    monkeypatch.setattr(compress.subprocess, "Popen", launch)
    stream = SimpleNamespace(emit=lambda *args: None, emit_error=lambda *args: None,
                             emit_text=lambda *args: None, emit_dim=lambda *args: None)
    result = compress.compress_video(str(source), stream, cancel_event=cancel)
    assert not result["ok"]
    assert result["reason"] == ("cancelled" if cancelled else "ffmpeg_error")
    assert source.read_bytes() == b"original media"
    assert list(tmp_path.iterdir()) == [source]
