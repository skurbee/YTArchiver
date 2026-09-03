"""Durable recovery journal for channel folder/config transactions.

A channel rename or removal changes two resources: the archive filesystem and
``config.json``.  The journal records the intended config patch before the
folder changes and records the moved-folder checkpoint before config is saved.
Startup recovery then reconciles the two resources while holding the same
channel aliases used by live jobs.
"""

from __future__ import annotations

import copy
import json
import logging
import os
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any

from backend.services.channel_leases import (
    LeaseOwner,
    channel_aliases,
    channel_leases,
    global_archive_aliases,
)
from backend.ytarchiver_config import APP_DATA_DIR, load_config

_log = logging.getLogger(__name__)
JOURNAL_VERSION = 3
CHANNEL_TRANSACTION_FILE = APP_DATA_DIR / "channel_folder_transaction.json"
CHANNEL_TRANSACTION_ALIAS = "channel-transaction:*"


class ChannelTransactionConflict(RuntimeError):
    """The edited config fields changed after the operation was prepared."""


class ChannelTransactionJournalError(RuntimeError):
    """The recovery journal exists but cannot be interpreted safely."""


def _fsync_parent(path: Path) -> None:
    """Best-effort directory sync after replacing or removing the journal."""
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        descriptor = os.open(str(path.parent), flags)
    except OSError:
        # Windows normally refuses directory handles through os.open.  The
        # journal file itself was still flushed before the atomic replace.
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)


def write_channel_transaction(record: dict[str, Any]) -> bool:
    """Atomically persist a complete journal checkpoint."""
    value = copy.deepcopy(record)
    value.setdefault("version", JOURNAL_VERSION)
    value.setdefault("tx_id", uuid.uuid4().hex)
    value["updated_at"] = time.time()
    temporary_path = ""
    descriptor = -1
    try:
        CHANNEL_TRANSACTION_FILE.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_path = tempfile.mkstemp(
            prefix=f".{CHANNEL_TRANSACTION_FILE.name}.",
            suffix=".tmp",
            dir=str(CHANNEL_TRANSACTION_FILE.parent),
        )
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            descriptor = -1
            json.dump(value, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, CHANNEL_TRANSACTION_FILE)
        temporary_path = ""
        _fsync_parent(CHANNEL_TRANSACTION_FILE)
        return True
    except (OSError, TypeError, ValueError) as exc:
        _log.warning("channel transaction journal save failed: %s", exc)
        return False
    finally:
        if descriptor >= 0:
            try:
                os.close(descriptor)
            except OSError:
                pass
        if temporary_path:
            try:
                os.unlink(temporary_path)
            except OSError:
                pass


def load_channel_transaction(*, strict: bool = False) -> dict[str, Any] | None:
    """Load the journal, optionally failing closed when it is malformed."""
    try:
        value = json.loads(CHANNEL_TRANSACTION_FILE.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as exc:
        message = f"Channel transaction journal could not be read: {exc}"
        _log.warning(message)
        if strict:
            raise ChannelTransactionJournalError(message) from exc
        return None
    if not isinstance(value, dict):
        message = "Channel transaction journal root is not an object"
        _log.warning(message)
        if strict:
            raise ChannelTransactionJournalError(message)
        return None
    return value


def clear_channel_transaction() -> bool:
    """Remove the journal durably after both resources agree."""
    try:
        CHANNEL_TRANSACTION_FILE.unlink(missing_ok=True)
        _fsync_parent(CHANNEL_TRANSACTION_FILE)
        return True
    except OSError as exc:
        _log.warning("channel transaction journal clear failed: %s", exc)
        return False


def channel_transaction_aliases() -> frozenset[str]:
    """Serialize users of the single process-wide transaction journal."""
    return frozenset({CHANNEL_TRANSACTION_ALIAS})


def channel_locator(channel: dict[str, Any] | None) -> dict[str, Any]:
    """Return stable identity plus rename fallbacks for one config record."""
    source = channel or {}
    locator: dict[str, Any] = {}
    for key in (
        "channel_id",
        "id",
        "stable_key",
        "url",
        "name",
        "folder",
        "folder_override",
    ):
        if key in source:
            locator[key] = copy.deepcopy(source[key])
    return locator


def build_channel_config_patch(
    old_channel: dict[str, Any],
    new_channel: dict[str, Any],
) -> dict[str, dict[str, dict[str, Any]]]:
    """Describe only edited fields, including explicit key removals."""
    patch: dict[str, dict[str, dict[str, Any]]] = {}
    for key in sorted(set(old_channel) | set(new_channel)):
        old_present = key in old_channel
        new_present = key in new_channel
        old_value = old_channel.get(key)
        new_value = new_channel.get(key)
        if old_present == new_present and old_value == new_value:
            continue
        patch[key] = {
            "old": {
                "present": old_present,
                "value": copy.deepcopy(old_value),
            },
            "new": {
                "present": new_present,
                "value": copy.deepcopy(new_value),
            },
        }
    return patch


def _field_matches(
    channel: dict[str, Any],
    endpoint: dict[str, Any],
    key: str,
) -> bool:
    present = bool(endpoint.get("present"))
    if present != (key in channel):
        return False
    return not present or channel.get(key) == endpoint.get("value")


def channel_matches_patch(
    channel: dict[str, Any],
    patch: dict[str, Any],
    side: str,
) -> bool:
    """Return whether every edited field is at one patch endpoint."""
    if side not in {"old", "new"} or not patch:
        return False
    for key, endpoints in patch.items():
        if not isinstance(endpoints, dict):
            return False
        endpoint = endpoints.get(side)
        if not isinstance(endpoint, dict) or not _field_matches(channel, endpoint, key):
            return False
    return True


def apply_channel_config_patch(
    channel: dict[str, Any],
    patch: dict[str, Any],
) -> dict[str, Any]:
    """Apply an optimistic field patch without overwriting unrelated changes."""
    if not patch:
        return dict(channel)
    if channel_matches_patch(channel, patch, "new"):
        return dict(channel)
    if not channel_matches_patch(channel, patch, "old"):
        changed_fields = ", ".join(sorted(patch))
        raise ChannelTransactionConflict(
            "Channel changed while the edit was open "
            f"({changed_fields}). Reload and retry."
        )
    updated = dict(channel)
    for key, endpoints in patch.items():
        endpoint = endpoints["new"]
        if endpoint.get("present"):
            updated[key] = copy.deepcopy(endpoint.get("value"))
        else:
            updated.pop(key, None)
    return updated


def make_rename_transaction(
    *,
    identity: dict[str, Any],
    old_channel: dict[str, Any],
    new_channel: dict[str, Any],
    old_path: str,
    new_path: str,
) -> dict[str, Any]:
    """Build the prepared checkpoint for an archive-folder rename."""
    return {
        "version": JOURNAL_VERSION,
        "tx_id": uuid.uuid4().hex,
        "operation": "rename",
        "state": "prepared",
        "recovery_required": False,
        "identity": channel_locator(identity or old_channel),
        "old_identity": channel_locator(old_channel),
        "new_identity": channel_locator(new_channel),
        "config_patch": build_channel_config_patch(old_channel, new_channel),
        "old_path": os.path.abspath(old_path),
        "new_path": os.path.abspath(new_path),
    }


def make_remove_transaction(
    *,
    identity: dict[str, Any],
    old_channel: dict[str, Any],
    old_path: str,
    archive_root: str,
) -> dict[str, Any]:
    """Build the prepared checkpoint for an archive-folder removal."""
    from backend.services.file_ops import channel_trash_destination

    transaction_id = uuid.uuid4().hex
    trash_path = channel_trash_destination(
        old_path,
        archive_root,
        transaction_id,
    )
    return {
        "version": JOURNAL_VERSION,
        "tx_id": transaction_id,
        "operation": "remove",
        "state": "prepared",
        "recovery_required": False,
        "identity": channel_locator(identity or old_channel),
        "old_identity": channel_locator(old_channel),
        "old_path": os.path.abspath(old_path),
        "archive_root": os.path.abspath(archive_root),
        # This exact destination is durable before the move begins.  Do not
        # replace it with a path returned after the move: a process loss in
        # that gap is precisely what this journal must recover.
        "trashed_folder_path": os.path.abspath(trash_path),
    }


def checkpoint_channel_transaction(
    record: dict[str, Any],
    state: str,
    **updates: Any,
) -> bool:
    """Persist a state transition while retaining the transaction id."""
    record.update(copy.deepcopy(updates))
    record["state"] = state
    return write_channel_transaction(record)


def mark_channel_recovery_required(
    record: dict[str, Any],
    *,
    phase: str,
    error: object,
) -> bool:
    """Leave a truthful marker when automatic rollback cannot finish."""
    record.update(
        {
            "state": "recovery_required",
            "recovery_required": True,
            "failure_phase": str(phase),
            "error": str(error),
        }
    )
    return write_channel_transaction(record)


def _text(value: object) -> str:
    return str(value or "").strip().casefold()


def _stable_aliases(locator: dict[str, Any]) -> frozenset[str]:
    return channel_aliases(locator)


def _fallback_locator_match(channel: dict[str, Any], locator: dict[str, Any]) -> bool:
    """Match mutable fields only when the journal has no stable identity."""
    wanted = [key for key in ("name", "folder") if _text(locator.get(key))]
    return bool(wanted) and all(
        _text(channel.get(key)) == _text(locator.get(key)) for key in wanted
    )


def _candidate_channels(
    channels: list[dict[str, Any]],
    *locators: dict[str, Any],
) -> list[dict[str, Any]]:
    wanted_aliases: set[str] = set()
    for locator in locators:
        wanted_aliases.update(_stable_aliases(locator))
    if wanted_aliases:
        return [
            channel
            for channel in channels
            if not wanted_aliases.isdisjoint(_stable_aliases(channel))
        ]
    return [
        channel
        for channel in channels
        if any(_fallback_locator_match(channel, locator) for locator in locators)
    ]


def _rename_config_state(
    channels: list[dict[str, Any]],
    record: dict[str, Any],
) -> str:
    old_identity = record.get("old_identity")
    new_identity = record.get("new_identity")
    patch = record.get("config_patch")
    if not isinstance(old_identity, dict) or not isinstance(new_identity, dict):
        return "ambiguous"
    if not isinstance(patch, dict) or not patch:
        return "ambiguous"
    candidates = _candidate_channels(channels, old_identity, new_identity)
    if len(candidates) != 1:
        return "ambiguous"
    old_match = channel_matches_patch(candidates[0], patch, "old")
    new_match = channel_matches_patch(candidates[0], patch, "new")
    if old_match == new_match:
        return "ambiguous"
    return "old" if old_match else "new"


def _remove_config_state(
    channels: list[dict[str, Any]],
    record: dict[str, Any],
) -> str:
    old_identity = record.get("old_identity")
    identity = record.get("identity")
    if not isinstance(old_identity, dict):
        old_identity = identity if isinstance(identity, dict) else {}
    candidates = _candidate_channels(channels, old_identity)
    if not candidates:
        return "removed"
    if len(candidates) == 1:
        return "present"
    return "ambiguous"


def _record_aliases(record: dict[str, Any]) -> frozenset[str]:
    aliases: set[str] = {CHANNEL_TRANSACTION_ALIAS}
    for key in ("identity", "old_identity", "new_identity"):
        locator = record.get(key)
        if isinstance(locator, dict):
            aliases.update(channel_aliases(locator))
    paths = [
        str(record.get(key) or "")
        for key in ("old_path", "new_path", "trashed_folder_path")
    ]
    paths = [path for path in paths if path]
    if paths:
        aliases.update(channel_aliases(paths=paths))
    return frozenset(aliases) or global_archive_aliases()


def _clear_or_fail(action: str, record: dict[str, Any]) -> dict[str, Any]:
    if clear_channel_transaction():
        return {"ok": True, "recovered": True, "action": action}
    return {
        "ok": False,
        "recovered": True,
        "recovery_required": True,
        "error": "Recovery completed, but its journal could not be cleared.",
        "record": record,
    }


def _recovery_failure(
    record: dict[str, Any],
    error: object,
    *,
    phase: str,
) -> dict[str, Any]:
    journal_saved = mark_channel_recovery_required(
        record,
        phase=phase,
        error=error,
    )
    return {
        "ok": False,
        "recovered": False,
        "recovery_required": True,
        "error": str(error),
        "journal_saved": journal_saved,
        "record": record,
    }


def _recover_rename(
    record: dict[str, Any],
    channels: list[dict[str, Any]],
) -> dict[str, Any]:
    old_path = str(record.get("old_path") or "")
    new_path = str(record.get("new_path") or "")
    if not old_path or not new_path or old_path == new_path:
        return _recovery_failure(
            record,
            "Rename journal has invalid folder paths.",
            phase="validate",
        )

    config_state = _rename_config_state(channels, record)
    old_exists = os.path.isdir(old_path)
    new_exists = os.path.isdir(new_path)
    if config_state == "ambiguous" or old_exists == new_exists:
        return _recovery_failure(
            record,
            "Rename recovery cannot unambiguously match config and folder state.",
            phase="reconcile",
        )
    if config_state == "old" and old_exists:
        return _clear_or_fail("rename-rolled-back", record)
    if config_state == "new" and new_exists:
        return _clear_or_fail("rename-kept", record)

    source, destination = (
        (new_path, old_path) if config_state == "old" else (old_path, new_path)
    )
    action = "rename-rolled-back" if config_state == "old" else "rename-finished"
    try:
        os.rename(source, destination)
    except OSError as exc:
        return _recovery_failure(record, exc, phase="folder_reconcile")
    return _clear_or_fail(action, record)


def _recover_remove(
    record: dict[str, Any],
    channels: list[dict[str, Any]],
) -> dict[str, Any]:
    old_path = str(record.get("old_path") or "")
    trash_path = str(record.get("trashed_folder_path") or "")
    archive_root = str(record.get("archive_root") or "")
    transaction_id = str(record.get("tx_id") or "")
    try:
        journal_version = int(record.get("version") or 0)
    except (TypeError, ValueError):
        journal_version = 0
    if not old_path:
        return _recovery_failure(
            record,
            "Removal journal has no original folder path.",
            phase="validate",
        )
    if journal_version >= 3:
        if not trash_path or not archive_root or not transaction_id:
            return _recovery_failure(
                record,
                "Removal journal is missing its reserved trash destination.",
                phase="validate",
            )
        try:
            from backend.services.file_ops import channel_trash_destination

            expected_trash_path = channel_trash_destination(
                old_path,
                archive_root,
                transaction_id,
            )
            if (os.path.normcase(os.path.realpath(trash_path))
                    != os.path.normcase(os.path.realpath(expected_trash_path))):
                raise ValueError(
                    "Removal journal trash path does not match its transaction.")
        except (OSError, ValueError) as exc:
            return _recovery_failure(record, exc, phase="validate")

    config_state = _remove_config_state(channels, record)
    old_exists = os.path.isdir(old_path)
    trash_exists = bool(trash_path) and os.path.isdir(trash_path)
    if config_state == "ambiguous":
        return _recovery_failure(
            record,
            "Removal recovery matched more than one config channel.",
            phase="reconcile",
        )
    if config_state == "removed":
        if old_exists:
            return _recovery_failure(
                record,
                "Subscription is removed but its original folder still exists.",
                phase="reconcile",
            )
        return _clear_or_fail("remove-kept", record)

    if old_exists and not trash_exists:
        return _clear_or_fail("remove-rolled-back", record)
    if old_exists or not trash_exists:
        return _recovery_failure(
            record,
            "Removal recovery cannot locate exactly one quarantined folder.",
            phase="reconcile",
        )

    from backend.services.file_ops import restore_trash_entry

    restored = restore_trash_entry(
        trash_path,
        expected_transaction_id=(
            transaction_id if journal_version >= 3 else ""
        ),
    )
    if not restored.get("ok"):
        return _recovery_failure(
            record,
            restored.get("error") or "Trash restore failed.",
            phase="folder_reconcile",
        )
    return _clear_or_fail("remove-rolled-back", record)


def recover_channel_transaction() -> dict[str, Any]:
    """Reconcile one interrupted folder/config operation before jobs start."""
    try:
        record = load_channel_transaction(strict=True)
    except ChannelTransactionJournalError as exc:
        return {
            "ok": False,
            "recovered": False,
            "recovery_required": True,
            "error": str(exc),
        }
    if record is None:
        return {"ok": True, "recovered": False}

    operation = str(record.get("operation") or "")
    transaction_id = str(record.get("tx_id") or "unknown")
    owner = LeaseOwner(
        owner="channel-transaction-recovery",
        job_id=transaction_id,
        label="Channel folder recovery",
        kind="recovery",
    )
    acquired = channel_leases.try_acquire(_record_aliases(record), owner)
    if not acquired.ok or acquired.lease is None:
        return {
            "ok": False,
            "recovered": False,
            "recovery_required": True,
            "error": acquired.explanation,
            "record": record,
        }

    with acquired.lease:
        try:
            loaded = load_config()
            raw_channels = loaded.get("channels") or []
            channels = [channel for channel in raw_channels if isinstance(channel, dict)]
        except Exception as exc:
            return _recovery_failure(record, exc, phase="config_load")
        if operation == "rename":
            return _recover_rename(record, channels)
        if operation == "remove":
            return _recover_remove(record, channels)
        return _recovery_failure(
            record,
            f"Unknown channel transaction operation: {operation!r}",
            phase="validate",
        )
