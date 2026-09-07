"""Completed-video transcription shares only its originating download lease."""

import threading

import pytest

from backend.services.channel_leases import (
    ChannelLeaseManager,
    LeaseOwner,
    global_archive_aliases,
)

CHANNEL = frozenset({"channel-key:test-channel"})
OTHER_CHANNEL = frozenset({"channel-key:other-channel"})
ADMISSION_METHODS = ("try_acquire", "acquire", "try_acquire_many", "acquire_many")


def _download(job_id="download-1", kind="download"):
    return LeaseOwner("sync", job_id, kind=kind, label="Downloading channel")


def _processing(job_id="processing-1", parent_job_id="download-1", kind="transcribe"):
    return LeaseOwner(
        "processing", job_id, kind=kind, parent_job_id=parent_job_id,
        label="Transcribing completed video",
    )


def _admit(manager, owner, *, method="try_acquire", aliases=CHANNEL, cancel_event=None):
    kwargs = {"cancel_event": cancel_event}
    if method in {"acquire", "acquire_many"}:
        kwargs["timeout"] = 0
    if method.endswith("_many"):
        aliases = [aliases]
    return getattr(manager, method)(aliases, owner, **kwargs)


def _hold(manager, owner, aliases=CHANNEL):
    result = manager.try_acquire(aliases, owner)
    assert result.ok and result.lease is not None, result
    return result.lease


@pytest.mark.parametrize("raw, normalized", [(None, ""), ("", ""), (" parent-1 ", "parent-1")])
def test_parent_identity_uses_normal_owner_normalization(raw, normalized):
    owner = _processing(parent_job_id=raw)
    assert owner.parent_job_id == normalized
    assert owner.key == ("processing", "processing-1")
    assert LeaseOwner("processing", "recovered", kind="transcribe").parent_job_id == ""


@pytest.mark.parametrize("method", ADMISSION_METHODS)
@pytest.mark.parametrize("processing_first", [False, True])
def test_exact_parent_pair_can_acquire_in_either_order(method, processing_first):
    manager = ChannelLeaseManager()
    first, second = (_processing(), _download()) if processing_first else (
        _download(), _processing()
    )
    with _hold(manager, first):
        assert manager.blockers_for(CHANNEL, requester=second) == ()
        result = _admit(manager, second, method=method)
        assert result.ok and result.lease is not None, result
        with result.lease:
            assert len(manager.active_snapshot()) == 2
            assert manager.blockers_for(CHANNEL, requester=first) == ()
    assert manager.active_snapshot() == ()


@pytest.mark.parametrize("parent_job_id", ["", "different-download"])
@pytest.mark.parametrize("processing_first", [False, True])
def test_missing_or_different_parent_stays_exclusive(parent_job_id, processing_first):
    manager = ChannelLeaseManager()
    processing = _processing(parent_job_id=parent_job_id)
    first, second = (processing, _download()) if processing_first else (_download(), processing)
    with _hold(manager, first):
        assert len(manager.blockers_for(CHANNEL, requester=second)) == 1
        for method in ADMISSION_METHODS:
            result = _admit(manager, second, method=method)
            assert not result.ok and result.lease is None
            assert len(result.blockers) == 1
            assert len(manager.active_snapshot()) == 1
    assert manager.active_snapshot() == ()


@pytest.mark.parametrize("kind", ["compress", "retranscribe", "", "punctuation"])
def test_other_processing_operations_never_share_download(kind):
    manager = ChannelLeaseManager()
    with _hold(manager, _download()):
        requester = _processing(kind=kind)
        assert manager.try_acquire(CHANNEL, requester).status == "busy"
        assert manager.blockers_for(CHANNEL, requester=requester)


@pytest.mark.parametrize("kind", ["redownload", "reorganize", "transcribe", "compress", ""])
def test_other_sync_operations_never_share_processing(kind):
    manager = ChannelLeaseManager()
    with _hold(manager, _download(kind=kind)):
        assert manager.try_acquire(CHANNEL, _processing()).status == "busy"


def test_manual_transcription_and_second_processing_job_stay_exclusive():
    manager = ChannelLeaseManager()
    with _hold(manager, _download()), _hold(manager, _processing()):
        requesters = (
            _processing(job_id="another-video"),
            _processing(job_id="manual-retranscribe", parent_job_id=""),
            LeaseOwner("manual", "manual-1", kind="transcribe", parent_job_id="download-1"),
        )
        for requester in requesters:
            blocked = manager.try_acquire(CHANNEL, requester)
            assert blocked.status == "busy"
            assert "processing-1" in {blocker.job_id for blocker in blocked.blockers}
        assert len(manager.active_snapshot()) == 2


@pytest.mark.parametrize("processing_first", [False, True])
@pytest.mark.parametrize("holder_global", [False, True])
def test_global_archive_requests_remain_exclusive_for_matching_pair(
    processing_first, holder_global,
):
    manager = ChannelLeaseManager()
    first, second = (_processing(), _download()) if processing_first else (
        _download(), _processing()
    )
    held_aliases = global_archive_aliases() if holder_global else CHANNEL
    requested_aliases = CHANNEL if holder_global else global_archive_aliases()
    with _hold(manager, first, held_aliases):
        assert manager.blockers_for(requested_aliases, requester=second)
        assert manager.try_acquire(requested_aliases, second).status == "busy"


def test_global_upgrade_cannot_bypass_cooperating_owner():
    manager = ChannelLeaseManager()
    with _hold(manager, _download()), _hold(manager, _processing()):
        for requester in (_download(), _processing()):
            blocked = manager.try_acquire(global_archive_aliases(), requester)
            assert blocked.status == "busy"
            assert len(blocked.blockers) == 1


def test_tuple_requester_only_shares_using_registered_owner_metadata():
    manager = ChannelLeaseManager()
    processing = _processing()
    with _hold(manager, _download()):
        assert manager.blockers_for(CHANNEL, requester=processing) == ()
        assert manager.blockers_for(CHANNEL, requester=processing.key)
        assert manager.blockers_for(CHANNEL)
        with _hold(manager, processing):
            assert manager.blockers_for(CHANNEL, requester=processing.key) == ()
            assert manager.blockers_for(CHANNEL, requester=_download().key) == ()
            assert len(manager.blockers_for(CHANNEL)) == 2
        assert manager.blockers_for(CHANNEL, requester=processing.key)


def test_existing_owner_cannot_gain_parent_permission_by_reusing_its_key():
    manager = ChannelLeaseManager()
    original = _processing(parent_job_id="")
    forged_parent = _processing()
    with _hold(manager, _download()), _hold(manager, original, OTHER_CHANNEL):
        for requester in (forged_parent, original.key):
            assert manager.blockers_for(CHANNEL, requester=requester)
        assert manager.try_acquire(CHANNEL, forged_parent).status == "busy"
        assert len(manager.active_snapshot()) == 2


def test_registered_pair_reentrancy_preserves_original_owner_and_depth():
    manager = ChannelLeaseManager()
    original = _processing()
    with _hold(manager, _download()), _hold(manager, original):
        changed_metadata = _processing(parent_job_id="different", kind="compress")
        with _hold(manager, changed_metadata) as nested:
            assert nested.owner is original
            assert nested.owner.parent_job_id == "download-1"
            processing_state = next(
                state for state in manager.active_snapshot() if state.owner == "processing"
            )
            assert processing_state.depth == 2
            assert manager.blockers_for(CHANNEL, requester=changed_metadata) == ()
        assert next(
            state.depth for state in manager.active_snapshot() if state.owner == "processing"
        ) == 1
    assert manager.active_snapshot() == ()


@pytest.mark.parametrize("release_processing_first", [False, True])
def test_reorganization_waits_until_both_cooperating_tokens_release(release_processing_first):
    manager = ChannelLeaseManager()
    download_lease = _hold(manager, _download())
    processing_lease = _hold(manager, _processing())
    reorganization = LeaseOwner("reorganize", "move-1", kind="reorganize")
    first, second = (processing_lease, download_lease) if release_processing_first else (
        download_lease, processing_lease
    )
    try:
        blocked = manager.try_acquire(CHANNEL, reorganization)
        assert blocked.status == "busy"
        assert len(blocked.blockers) == 2
        assert first.release()
        assert not first.release()
        blocked = manager.acquire(CHANNEL, reorganization, timeout=0)
        assert blocked.status == "timeout"
        assert len(blocked.blockers) == 1
        assert second.release()
        with _hold(manager, reorganization):
            assert len(manager.active_snapshot()) == 1
    finally:
        download_lease.release()
        processing_lease.release()
    assert manager.active_snapshot() == ()


def test_lingering_processing_blocks_next_download_after_parent_finishes():
    manager = ChannelLeaseManager()
    with _hold(manager, _processing()):
        with _hold(manager, _download()):
            pass
        assert manager.try_acquire(CHANNEL, _download("download-2")).status == "busy"
        assert manager.blockers_for(CHANNEL, requester=_download("download-2"))
    with _hold(manager, _download("download-2")):
        assert len(manager.active_snapshot()) == 1


@pytest.mark.parametrize("method", ADMISSION_METHODS)
def test_cancelled_child_cannot_join_or_leak_a_lease(method):
    manager = ChannelLeaseManager()
    cancel = threading.Event()
    cancel.set()
    with _hold(manager, _download()):
        result = _admit(manager, _processing(), method=method, cancel_event=cancel)
        assert result.status == "cancelled"
        assert result.lease is None
        assert len(manager.active_snapshot()) == 1
    assert manager.active_snapshot() == ()


def test_blocked_child_wait_can_be_cancelled_then_all_leases_release():
    manager = ChannelLeaseManager()
    cancel = threading.Event()
    attempted = threading.Event()
    results = []

    class Cancellation:
        def is_set(self):
            attempted.set()
            return cancel.is_set()

    def wait_for_lease():
        results.append(manager.acquire(
            CHANNEL, _processing(parent_job_id="unrelated"), timeout=1,
            cancel_event=Cancellation(), poll_interval=0.01,
        ))

    with _hold(manager, _download()):
        worker = threading.Thread(target=wait_for_lease, daemon=True)
        worker.start()
        try:
            assert attempted.wait(0.5)
            cancel.set()
            worker.join(0.5)
            assert not worker.is_alive()
            assert len(results) == 1
            assert results[0].status == "cancelled"
            assert results[0].lease is None
            assert len(manager.active_snapshot()) == 1
        finally:
            cancel.set()
            worker.join(1)
    assert manager.active_snapshot() == ()


@pytest.mark.parametrize("method", ["try_acquire_many", "acquire_many"])
def test_many_channel_request_cannot_partially_join_its_parent(method):
    manager = ChannelLeaseManager()
    with _hold(manager, _download()), _hold(manager, _download("other"), OTHER_CHANNEL):
        kwargs = {"timeout": 0} if method == "acquire_many" else {}
        result = getattr(manager, method)([CHANNEL, OTHER_CHANNEL], _processing(), **kwargs)
        assert not result.ok and result.lease is None
        assert [blocker.job_id for blocker in result.blockers] == ["other"]
        assert all(state.owner == "sync" for state in manager.active_snapshot())
    assert manager.active_snapshot() == ()
