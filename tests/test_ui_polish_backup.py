"""Backup identity fields are derived from a manifest, never guessed from file dates."""
import json
import time
import zipfile

from backend import auto_backup, ytarchiver_config
from backend.api_mixins import backup_mixin


def test_new_manifest_records_creation_time_and_preview_content_flags(tmp_path, monkeypatch):
    config = tmp_path / "ytarchiver_config.json"
    config.write_text('{"channels":[]}', encoding="utf-8")
    database = tmp_path / "transcription_index.db"
    monkeypatch.setattr(auto_backup, "APP_DATA_DIR", tmp_path)
    monkeypatch.setattr(auto_backup, "TRANSCRIPTION_DB", database)
    monkeypatch.setattr(auto_backup, "backup_file_entries", lambda: [(config.name, config)])
    monkeypatch.setattr(ytarchiver_config, "TRANSCRIPTION_DB", database)
    archive = tmp_path / "chosen-backup.zip"
    before = time.time()
    auto_backup.build_backup_zip(str(archive))
    after = time.time()
    with zipfile.ZipFile(archive) as zipped:
        assert zipped.testzip() is None
        manifest = json.loads(zipped.read(auto_backup.BACKUP_MANIFEST_NAME))
        assert before <= manifest["created_at"] <= after
        assert auto_backup.BOOKMARK_BACKUP_NAME in zipped.namelist()

    class Window:
        def create_file_dialog(self, *args, **kwargs):
            return (str(archive),)

    class Api(backup_mixin.BackupMixin):
        _window = Window()
        _fmt_bytes_short = staticmethod(lambda value: str(value))

    preview = Api().import_full_backup_preview()
    assert preview["ok"], preview
    assert preview["zip_name"] == "chosen-backup.zip"
    assert preview["created_at"] == manifest["created_at"]
    assert preview["bookmarks_included"] and not preview["index_included"]
    assert preview["zip_modified_at"] == archive.stat().st_mtime


def test_legacy_zip_date_is_separate_from_unknown_backup_creation_time(tmp_path):
    archive = tmp_path / "legacy.zip"
    with zipfile.ZipFile(archive, "w") as zipped:
        zipped.writestr("ytarchiver_config.json", '{"channels":[]}')

    class Window:
        def create_file_dialog(self, *args, **kwargs):
            return (str(archive),)

    class Api(backup_mixin.BackupMixin):
        _window = Window()
        _fmt_bytes_short = staticmethod(lambda value: str(value))

    preview = Api().import_full_backup_preview()
    assert preview["ok"]
    assert preview["created_at"] is None
    assert preview["zip_modified_at"] > 0
    assert not preview["bookmarks_included"] and not preview["index_included"]
