"""Regression coverage for the backup status exposed to the UI."""

from __future__ import annotations

from backend import auto_backup
from backend.api_mixins.settings_mixin import SettingsMixin


def test_settings_load_keeps_manual_and_automatic_backup_times_separate():
    api = SettingsMixin()
    api._config = {
        "last_backup_ts": 1_700_000_123.25,
        "last_auto_backup_ts": 1_600_000_456.75,
    }

    loaded = api.settings_load()

    assert loaded["last_backup_ts"] == 1_700_000_123.25
    assert loaded["last_auto_backup_ts"] == 1_600_000_456.75
    assert loaded["last_auto_backup_ts"] != loaded["last_backup_ts"]


def test_archive_about_text_describes_app_state_backup_and_current_restore_route():
    text = auto_backup._about_text()
    one_line = " ".join(text.split())

    assert "Health tab > Backups > Restore" in one_line
    assert "Backup and Migration" not in text

    backup_block = text.split(
        f"{auto_backup.BACKUP_PREFIX}*.zip", 1
    )[1].split("To restore:", 1)[0]
    backup_block_lower = backup_block.lower()

    # The ZIP remembers which video IDs the app has handled. It does not
    # contain the archived video files themselves.
    assert "downloaded-video-id list" in backup_block_lower
    assert "downloaded videos" not in backup_block_lower
    assert "archived videos" not in backup_block_lower
    assert ".mp4" not in backup_block_lower
