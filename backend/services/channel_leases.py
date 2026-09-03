"""Process-wide leases for channel and archive mutations.

The archiver has several jobs that can refer to the same channel in different
ways: by its YouTube identity, by its current folder, or by both its old and
new folder during a move.  A lease therefore owns an *alias set*, not one lock
name.  Acquisition is protected by one condition variable so acquiring many
aliases is atomic; a failed request never keeps a partial set of aliases.

Lock order for code that uses this service:

1. supervisor admission / job registration;
2. the global archive lease, or one atomic set of channel aliases;
3. a config transaction;
4. queue, journal, or index state;
5. filesystem and child-process work.

Never keep one channel lease while waiting for a second channel lease.  Build
both alias sets first and use :meth:`ChannelLeaseManager.acquire_many`.
Keeping this order prevents a reorganization, sync, and shutdown from each
holding one resource while waiting for another.
"""

from __future__ import annotations

import math
import os
import threading
import time
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import urlsplit

GLOBAL_ARCHIVE_ALIAS = "archive:*"
_CHANNEL_TAB_NAMES = frozenset({"featured", "shorts", "streams", "videos"})
_YOUTUBE_HOSTS = frozenset(
    {
        "m.youtube.com",
        "music.youtube.com",
        "www.youtube.com",
        "youtube.com",
        "www.youtube-nocookie.com",
        "youtube-nocookie.com",
    }
)


class CancellationSignal(Protocol):
    """Small protocol shared by ``threading.Event`` and compatible tokens."""

    def is_set(self) -> bool:
        """Return whether cancellation was requested."""


def normalize_channel_url(value: object) -> str:
    """Return a stable, scheme-free channel URL for use in an alias.

    Query strings, fragments, a trailing slash, and YouTube's content-tab
    suffixes do not change channel identity.  YouTube handles are
    case-insensitive, while channel IDs are deliberately left case-sensitive.
    """
    raw = str(value or "").strip()
    if not raw:
        return ""
    if raw.startswith("@"):
        raw = f"youtube.com/{raw}"
    if "://" not in raw:
        raw = f"https://{raw.lstrip('/')}"

    try:
        parsed = urlsplit(raw)
        host = (parsed.hostname or "").casefold().rstrip(".")
    except ValueError:
        return ""
    if not host:
        return ""
    if host in _YOUTUBE_HOSTS:
        host = "youtube.com"

    segments = [part for part in parsed.path.replace("\\", "/").split("/") if part]
    while segments and segments[-1].casefold() in _CHANNEL_TAB_NAMES:
        segments.pop()
    if segments and segments[0].startswith("@"):
        segments[0] = segments[0].casefold()
    elif segments and segments[0].casefold() in {"c", "channel", "user"}:
        segments[0] = segments[0].casefold()

    path = "/".join(segments)
    return f"{host}/{path}" if path else host


def canonical_path(value: os.PathLike[str] | str) -> str:
    """Return one canonical spelling for a filesystem path alias."""
    raw = os.fspath(value).strip()
    if not raw:
        raise ValueError("channel path cannot be empty")
    resolved = os.path.realpath(os.path.abspath(os.path.expanduser(raw)))
    # Windows APIs may return the extended path prefix for one spelling but
    # not another.  It is not part of the resource's identity.
    if resolved.startswith("\\\\?\\UNC\\"):
        resolved = f"\\\\{resolved[8:]}"
    elif resolved.startswith("\\\\?\\"):
        resolved = resolved[4:]
    return os.path.normcase(os.path.normpath(resolved))


def canonical_alias(value: object) -> str:
    """Canonicalize a public lease alias and reject empty aliases."""
    alias = str(value or "").strip()
    if not alias:
        raise ValueError("lease aliases cannot be empty")
    if alias.casefold() == GLOBAL_ARCHIVE_ALIAS:
        return GLOBAL_ARCHIVE_ALIAS

    prefix, separator, payload = alias.partition(":")
    if not separator:
        return alias
    prefix = prefix.casefold()
    payload = payload.strip()
    if not payload:
        raise ValueError(f"lease alias {prefix!r} has an empty value")
    if prefix == "channel-path":
        payload = canonical_path(payload)
    elif prefix == "channel-url":
        payload = normalize_channel_url(payload)
        if not payload:
            raise ValueError("channel URL alias is invalid")
    return f"{prefix}:{payload}"


def path_alias(value: os.PathLike[str] | str) -> str:
    """Return the canonical alias for a channel folder."""
    return f"channel-path:{canonical_path(value)}"


def _path_values(
    paths: Iterable[os.PathLike[str] | str] | os.PathLike[str] | str,
) -> Iterable[os.PathLike[str] | str]:
    if isinstance(paths, (str, os.PathLike)):
        return (paths,)
    return paths


def channel_aliases(
    channel: Mapping[str, Any] | None = None,
    *,
    channel_id: object | None = None,
    url: object | None = None,
    stable_key: object | None = None,
    paths: Iterable[os.PathLike[str] | str] | os.PathLike[str] | str = (),
) -> frozenset[str]:
    """Build aliases for every stable identity and known path of a channel.

    ``name`` and ``folder`` are intentionally ignored because users may edit
    them.  Pass resolved old/current/new folders through ``paths`` so a move
    conflicts with work that still knows an earlier path.
    """
    source = channel or {}
    if channel_id is None:
        channel_id = source.get("channel_id") or source.get("id")
    if url is None:
        url = source.get("url") or source.get("channel_url")
    if stable_key is None:
        stable_key = source.get("stable_key")

    aliases: set[str] = set()
    if channel_id is not None and str(channel_id).strip():
        aliases.add(f"channel-id:{str(channel_id).strip()}")
    normalized_url = normalize_channel_url(url)
    if normalized_url:
        aliases.add(f"channel-url:{normalized_url}")
    if stable_key is not None and str(stable_key).strip():
        aliases.add(f"channel-key:{str(stable_key).strip()}")
    for channel_path in _path_values(paths):
        if os.fspath(channel_path).strip():
            aliases.add(path_alias(channel_path))
    return frozenset(aliases)


def global_archive_aliases() -> frozenset[str]:
    """Return the alias set that excludes every other archive mutation."""
    return frozenset({GLOBAL_ARCHIVE_ALIAS})


@dataclass(frozen=True, slots=True)
class LeaseOwner:
    """Identity and diagnostic metadata for one logical job."""

    owner: str
    job_id: str
    label: str = ""
    task_id: str = ""
    kind: str = ""

    def __post_init__(self) -> None:
        for field_name in ("owner", "job_id", "label", "task_id", "kind"):
            value = str(getattr(self, field_name) or "").strip()
            object.__setattr__(self, field_name, value)
        if not self.owner:
            raise ValueError("lease owner cannot be empty")
        if not self.job_id:
            raise ValueError("lease job_id cannot be empty")

    @property
    def key(self) -> tuple[str, str]:
        """Key used for reentrancy; labels never change job identity."""
        return self.owner, self.job_id

    def describe(self) -> str:
        """Return a compact explanation suitable for a busy response."""
        description = self.label or self.kind or self.owner
        details = [f"job {self.job_id}"]
        if self.task_id:
            details.append(f"task {self.task_id}")
        return f"{description} ({', '.join(details)})"


@dataclass(frozen=True, slots=True)
class LeaseSnapshot:
    """Immutable diagnostics for one currently active logical job."""

    owner: str
    job_id: str
    label: str
    task_id: str
    kind: str
    aliases: tuple[str, ...]
    depth: int
    acquired_at: float
    held_seconds: float

    def as_dict(self) -> dict[str, Any]:
        """Return JSON-ready diagnostic data."""
        return {
            "owner": self.owner,
            "job_id": self.job_id,
            "label": self.label,
            "task_id": self.task_id,
            "kind": self.kind,
            "aliases": list(self.aliases),
            "depth": self.depth,
            "acquired_at": self.acquired_at,
            "held_seconds": self.held_seconds,
        }


@dataclass(slots=True)
class _OwnerState:
    owner: LeaseOwner
    alias_counts: Counter[str]
    depth: int
    acquired_at: float
    acquired_monotonic: float


class ChannelLease:
    """A reentrant acquisition token; release is safe to call more than once."""

    def __init__(
        self,
        manager: ChannelLeaseManager,
        owner: LeaseOwner,
        aliases: frozenset[str],
        acquired_at: float,
    ) -> None:
        self._manager = manager
        self._owner = owner
        self._aliases = aliases
        self._acquired_at = acquired_at
        self._released = False
        self._release_lock = threading.Lock()

    @property
    def owner(self) -> LeaseOwner:
        return self._owner

    @property
    def aliases(self) -> frozenset[str]:
        return self._aliases

    @property
    def acquired_at(self) -> float:
        return self._acquired_at

    @property
    def released(self) -> bool:
        with self._release_lock:
            return self._released

    def release(self) -> bool:
        """Release this token once and return whether a release occurred."""
        with self._release_lock:
            if self._released:
                return False
            self._manager._release(self._owner.key, self._aliases)
            self._released = True
            return True

    def __enter__(self) -> ChannelLease:
        return self

    def __exit__(self, _exc_type, _exc, _traceback) -> None:
        self.release()


@dataclass(frozen=True, slots=True)
class LeaseAcquireResult:
    """Result of a non-throwing lease admission attempt."""

    ok: bool
    status: str
    lease: ChannelLease | None
    blockers: tuple[LeaseSnapshot, ...]
    explanation: str

    def __bool__(self) -> bool:
        return self.ok


class ChannelLeaseManager:
    """Atomically coordinate mutations that may share channel aliases."""

    def __init__(self) -> None:
        self._condition = threading.Condition(threading.RLock())
        self._owners: dict[tuple[str, str], _OwnerState] = {}

    @staticmethod
    def _aliases(values: Iterable[object] | object) -> frozenset[str]:
        if isinstance(values, (str, os.PathLike)):
            values = (values,)
        aliases = frozenset(canonical_alias(value) for value in values)
        if not aliases:
            raise ValueError("at least one lease alias is required")
        return aliases

    @classmethod
    def _many_aliases(
        cls,
        alias_sets: Iterable[Iterable[object] | object],
    ) -> frozenset[str]:
        flattened: set[str] = set()
        for values in alias_sets:
            flattened.update(cls._aliases(values))
        if not flattened:
            raise ValueError("at least one lease alias set is required")
        return frozenset(flattened)

    @staticmethod
    def _requester_key(
        requester: LeaseOwner | tuple[str, str] | None,
    ) -> tuple[str, str] | None:
        if requester is None:
            return None
        if isinstance(requester, LeaseOwner):
            return requester.key
        if len(requester) != 2:
            raise ValueError("requester key must contain owner and job_id")
        return str(requester[0]), str(requester[1])

    def _blocking_states_locked(
        self,
        aliases: frozenset[str],
        requester_key: tuple[str, str] | None,
    ) -> list[_OwnerState]:
        wants_global = GLOBAL_ARCHIVE_ALIAS in aliases
        blockers: list[_OwnerState] = []
        for key, state in self._owners.items():
            if key == requester_key:
                continue
            held = state.alias_counts.keys()
            if wants_global or GLOBAL_ARCHIVE_ALIAS in held or not aliases.isdisjoint(held):
                blockers.append(state)
        blockers.sort(key=lambda state: state.owner.key)
        return blockers

    @staticmethod
    def _snapshot_state(state: _OwnerState, now: float) -> LeaseSnapshot:
        owner = state.owner
        return LeaseSnapshot(
            owner=owner.owner,
            job_id=owner.job_id,
            label=owner.label,
            task_id=owner.task_id,
            kind=owner.kind,
            aliases=tuple(sorted(state.alias_counts)),
            depth=state.depth,
            acquired_at=state.acquired_at,
            held_seconds=max(0.0, now - state.acquired_monotonic),
        )

    def _blocker_snapshots_locked(
        self,
        aliases: frozenset[str],
        requester_key: tuple[str, str] | None,
    ) -> tuple[LeaseSnapshot, ...]:
        now = time.monotonic()
        return tuple(
            self._snapshot_state(state, now)
            for state in self._blocking_states_locked(aliases, requester_key)
        )

    @staticmethod
    def _busy_text(blockers: tuple[LeaseSnapshot, ...]) -> str:
        if not blockers:
            return "No active lease blocks this operation."
        descriptions = []
        for blocker in blockers:
            # This explanation is returned directly to several UI dialogs.
            # Keep opaque orchestration IDs in the structured blocker snapshots
            # for diagnostics, not in the user-facing sentence.
            descriptions.append(blocker.label or blocker.kind or "another task")
        return (
            "Archive work is busy with " + "; ".join(descriptions)
            + ". Try again after the active work finishes."
        )

    def _grant_locked(
        self,
        aliases: frozenset[str],
        owner: LeaseOwner,
    ) -> LeaseAcquireResult:
        state = self._owners.get(owner.key)
        if state is None:
            state = _OwnerState(
                owner=owner,
                alias_counts=Counter(),
                depth=0,
                acquired_at=time.time(),
                acquired_monotonic=time.monotonic(),
            )
            self._owners[owner.key] = state
        state.alias_counts.update(aliases)
        state.depth += 1
        lease = ChannelLease(self, state.owner, aliases, state.acquired_at)
        return LeaseAcquireResult(
            ok=True,
            status="acquired",
            lease=lease,
            blockers=(),
            explanation="Lease acquired.",
        )

    def try_acquire(
        self,
        aliases: Iterable[object] | object,
        owner: LeaseOwner,
        *,
        cancel_event: CancellationSignal | None = None,
    ) -> LeaseAcquireResult:
        """Try once without waiting; return ``busy`` when aliases conflict."""
        requested = self._aliases(aliases)
        with self._condition:
            if cancel_event is not None and cancel_event.is_set():
                return LeaseAcquireResult(
                    False, "cancelled", None, (), "Lease request was cancelled."
                )
            blockers = self._blocker_snapshots_locked(requested, owner.key)
            if blockers:
                return LeaseAcquireResult(False, "busy", None, blockers, self._busy_text(blockers))
            return self._grant_locked(requested, owner)

    def acquire(
        self,
        aliases: Iterable[object] | object,
        owner: LeaseOwner,
        *,
        timeout: float,
        cancel_event: CancellationSignal | None = None,
        poll_interval: float = 0.05,
    ) -> LeaseAcquireResult:
        """Wait for a finite duration and remain responsive to cancellation."""
        requested = self._aliases(aliases)
        timeout = float(timeout)
        poll_interval = float(poll_interval)
        if not math.isfinite(timeout) or timeout < 0:
            raise ValueError("lease timeout must be a finite non-negative number")
        if not math.isfinite(poll_interval) or poll_interval <= 0:
            raise ValueError("lease poll_interval must be a finite positive number")
        deadline = time.monotonic() + timeout

        with self._condition:
            while True:
                if cancel_event is not None and cancel_event.is_set():
                    blockers = self._blocker_snapshots_locked(requested, owner.key)
                    return LeaseAcquireResult(
                        False,
                        "cancelled",
                        None,
                        blockers,
                        "Lease request was cancelled.",
                    )
                blockers = self._blocker_snapshots_locked(requested, owner.key)
                if not blockers:
                    return self._grant_locked(requested, owner)
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return LeaseAcquireResult(
                        False,
                        "timeout",
                        None,
                        blockers,
                        "Timed out waiting for a channel lease. " + self._busy_text(blockers),
                    )
                self._condition.wait(min(poll_interval, remaining))

    def try_acquire_many(
        self,
        alias_sets: Iterable[Iterable[object] | object],
        owner: LeaseOwner,
        *,
        cancel_event: CancellationSignal | None = None,
    ) -> LeaseAcquireResult:
        """Atomically try to acquire the union of several alias sets."""
        return self.try_acquire(self._many_aliases(alias_sets), owner, cancel_event=cancel_event)

    def acquire_many(
        self,
        alias_sets: Iterable[Iterable[object] | object],
        owner: LeaseOwner,
        *,
        timeout: float,
        cancel_event: CancellationSignal | None = None,
        poll_interval: float = 0.05,
    ) -> LeaseAcquireResult:
        """Atomically wait for the union of several alias sets."""
        return self.acquire(
            self._many_aliases(alias_sets),
            owner,
            timeout=timeout,
            cancel_event=cancel_event,
            poll_interval=poll_interval,
        )

    def blockers_for(
        self,
        aliases: Iterable[object] | object,
        requester: LeaseOwner | tuple[str, str] | None = None,
    ) -> tuple[LeaseSnapshot, ...]:
        """Return immutable details for jobs blocking the requested aliases."""
        requested = self._aliases(aliases)
        with self._condition:
            return self._blocker_snapshots_locked(requested, self._requester_key(requester))

    def busy_explanation(
        self,
        aliases: Iterable[object] | object,
        requester: LeaseOwner | tuple[str, str] | None = None,
    ) -> str:
        """Explain which active job, if any, blocks these aliases."""
        return self._busy_text(self.blockers_for(aliases, requester))

    def active_snapshot(self) -> tuple[LeaseSnapshot, ...]:
        """Return deterministic diagnostics for every currently held job."""
        with self._condition:
            now = time.monotonic()
            return tuple(
                self._snapshot_state(self._owners[key], now) for key in sorted(self._owners)
            )

    def _release(
        self,
        owner_key: tuple[str, str],
        aliases: frozenset[str],
    ) -> None:
        with self._condition:
            state = self._owners.get(owner_key)
            if state is None:
                raise RuntimeError("lease owner disappeared before release")
            for alias in aliases:
                count = state.alias_counts.get(alias, 0)
                if count <= 0:
                    raise RuntimeError(f"lease alias {alias!r} was not held")
                if count == 1:
                    del state.alias_counts[alias]
                else:
                    state.alias_counts[alias] = count - 1
            state.depth -= 1
            if state.depth == 0:
                if state.alias_counts:
                    raise RuntimeError("lease reference counts did not balance")
                del self._owners[owner_key]
            elif state.depth < 0:
                raise RuntimeError("lease depth became negative")
            self._condition.notify_all()


# One process-wide instance is the integration point for application jobs.
channel_leases = ChannelLeaseManager()
