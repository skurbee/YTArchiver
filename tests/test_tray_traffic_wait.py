"""Request-limit waiting is visible without claiming the downloader is working."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from PIL import Image, ImageDraw, ImageFont

from backend import taskbar_overlay, tray

AMBER = (245, 180, 55, 255)
ERROR_RED = (205, 35, 45, 255)
COUNT_RED = (220, 40, 40, 255)


@pytest.fixture
def controller(monkeypatch):
    ctrl = tray.TrayController()
    ctrl._Image = Image
    ctrl._ImageDraw = ImageDraw
    monkeypatch.setattr(ImageDraw, "ImageFont", ImageFont, raising=False)
    ctrl._base_img = Image.new("RGBA", (256, 256), (0, 0, 0, 255))
    ctrl._icon = SimpleNamespace(icon=ctrl._base_img, title="")
    ctrl._started = True
    ctrl._taskbar_hwnd = 12345
    events = []

    class FakeTaskbar:
        available = True

        def __init__(self, hwnd):
            assert hwnd == 12345

        def set_pil_image(self, image, description=""):
            events.append(("set", image.copy(), description))
            return True

        def clear(self):
            events.append(("clear",))
            return True

        def close(self):
            events.append(("close",))

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            self.close()

    monkeypatch.setattr(taskbar_overlay, "WindowsTaskbarOverlay", FakeTaskbar)
    return ctrl, events


def has_color(image, color):
    return any(image.getpixel((x, y)) == color
               for y in range(image.height) for x in range(image.width))


def amber_components(image):
    remaining = {(x, y) for y in range(image.height) for x in range(image.width)
                 if image.getpixel((x, y)) == AMBER}
    components = []
    while remaining:
        component = {remaining.pop()}
        pending = list(component)
        while pending:
            x, y = pending.pop()
            for neighbor in ((x - 1, y), (x + 1, y), (x, y - 1), (x, y + 1)):
                if neighbor in remaining:
                    remaining.remove(neighbor)
                    component.add(neighbor)
                    pending.append(neighbor)
        components.append(component)
    return components


@pytest.mark.parametrize("sync,gpu,waiting,expected", [
    (True, False, True, None),
    (False, False, True, None),
    (True, True, True, "red"),
    (False, True, True, "red"),
    (True, False, False, "blue"),
    (False, False, False, None),
])
def test_wait_suppresses_sync_animation_but_actual_gpu_activity_wins(
        sync, gpu, waiting, expected):
    assert tray.activity_spin_color(
        sync_working=sync, gpu_working=gpu, traffic_waiting=waiting) == expected


def test_wait_without_badges_updates_both_native_surfaces(controller):
    ctrl, events = controller
    ctrl.set_traffic_waiting(True)
    assert ctrl._traffic_waiting
    assert ctrl._spin_thread is None
    assert ctrl._icon.icon.size == (32, 32)
    assert has_color(ctrl._icon.icon, AMBER)
    rendered = [event for event in events if event[0] == "set"]
    assert rendered
    assert has_color(rendered[-1][1], AMBER)
    assert "wait" in rendered[-1][2].lower()
    assert not any(event[0] == "clear" for event in events)


@pytest.mark.parametrize("badge,errors", [(0, 0), (7, 0), (0, 2), (12, 2)])
def test_paused_frame_is_static_and_preserves_alert_visibility(controller, badge, errors):
    ctrl, _events = controller
    ctrl._badge_count = badge
    ctrl._error_count = errors
    ctrl._traffic_waiting = True
    first = ctrl._compose_taskbar_overlay(0)
    second = ctrl._compose_taskbar_overlay(1)
    assert first.size == (32, 32)
    assert first.tobytes() == second.tobytes()
    assert has_color(first, AMBER)
    bars = amber_components(first)
    assert len(bars) == 2
    for bar in bars:
        width = max(x for x, _y in bar) - min(x for x, _y in bar) + 1
        height = max(y for _x, y in bar) - min(y for _x, y in bar) + 1
        assert height >= 10 and height >= width * 2
    if errors:
        assert has_color(first, ERROR_RED)
        assert not has_color(first, COUNT_RED)
    elif badge:
        assert has_color(first, COUNT_RED)


def test_clearing_errors_during_wait_reveals_preserved_download_count(controller):
    ctrl, _events = controller
    ctrl.set_badge(7)
    ctrl.set_error(2)
    ctrl.set_traffic_waiting(True)
    assert has_color(ctrl._icon.icon, ERROR_RED)
    ctrl.set_error(0)
    assert ctrl._traffic_waiting
    assert ctrl._badge_count == 7
    assert has_color(ctrl._icon.icon, AMBER)
    assert has_color(ctrl._icon.icon, COUNT_RED)


def test_clearing_wait_restores_idle_and_clears_taskbar_overlay(controller):
    ctrl, events = controller
    ctrl.set_traffic_waiting(True)
    events.clear()
    ctrl.set_traffic_waiting(False)
    assert not ctrl._traffic_waiting
    assert not has_color(ctrl._icon.icon, AMBER)
    assert any(event[0] == "clear" for event in events)


def test_waiting_before_window_creation_applies_when_hwnd_arrives(controller):
    ctrl, events = controller
    ctrl._taskbar_hwnd = 0
    ctrl.set_traffic_waiting(True)
    assert events == []
    ctrl.set_window_handle(12345)
    assert has_color([event for event in events if event[0] == "set"][-1][1], AMBER)


@pytest.mark.parametrize("color", ["blue", "red"])
def test_starting_real_work_clears_wait_state(controller, monkeypatch, color):
    ctrl, _events = controller

    class DeferredThread:
        def __init__(self, **_kwargs):
            self.started = False

        def start(self):
            self.started = True

    monkeypatch.setattr(tray.threading, "Thread", DeferredThread)
    ctrl.set_traffic_waiting(True)
    ctrl.start_spin(color)
    assert not ctrl._traffic_waiting
    assert ctrl._spin_thread.started
    assert not has_color(ctrl._compose_taskbar_overlay(0), AMBER)


def test_stale_loop_closes_its_com_object_without_clearing_current_overlay(controller):
    ctrl, events = controller
    ctrl._spin_epoch = 2
    ctrl._traffic_waiting = True
    ctrl._spin_stop.clear()
    ctrl._spin_loop(epoch=1)
    assert events == [("close",)]


def test_frame_composed_before_stop_cannot_replace_or_clear_new_wait_icon(
        controller, monkeypatch):
    ctrl, events = controller
    ctrl._spin_epoch = 1
    ctrl._spin_stop.clear()
    stale = Image.new("RGBA", (32, 32), (0, 0, 255, 255))
    current = Image.new("RGBA", (32, 32), AMBER)

    def supersede_during_composition(_frame):
        ctrl._spin_epoch = 2
        ctrl._spin_stop.set()
        ctrl._traffic_waiting = True
        ctrl._icon.icon = current
        return stale

    monkeypatch.setattr(ctrl, "_compose_tray_spin_frame", supersede_during_composition)
    ctrl._spin_loop(epoch=1)
    assert ctrl._icon.icon is current
    assert events == [("close",)]


def test_stopping_invalidates_old_epoch_before_join_times_out(controller):
    ctrl, _events = controller
    ctrl._spin_epoch = 7
    observed = []

    class SlowOldThread:
        def join(self, timeout):
            observed.append((ctrl._spin_epoch, ctrl._spin_stop.is_set(), timeout))

    ctrl._spin_thread = SlowOldThread()
    ctrl.stop_spin()
    assert observed == [(8, True, 0.5)]
    assert ctrl._spin_thread is None
