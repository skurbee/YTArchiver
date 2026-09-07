"""Choose runnable Processing work without parking behind a busy channel."""

from backend.queues import make_task_id
from backend.services.channel_leases import LeaseOwner, channel_leases


def processing_owner(job):
    if not job.get("task_id"):
        job["task_id"] = make_task_id("gpu")
    kind = str(job.get("kind") or "transcribe")
    # This capability is captured from the originating Sync at download
    # completion. It is runtime-only: restart recovery gets exclusive access.
    parent = (str(job.get("_download_sync_job_id") or "")
              if kind == "transcribe" and job.get("from_download")
              and not job.get("retranscribe") else "")
    return LeaseOwner(
        "processing", str(job["task_id"]), label="GPU processing",
        task_id=str(job["task_id"]), kind=kind, parent_job_id=parent)


def download_parent_job_id(aliases):
    """Capture only the normal download already protecting this channel."""
    matches = [row.job_id for row in channel_leases.blockers_for(aliases)
               if row.owner == "sync" and row.kind == "download"]
    return matches[0] if len(matches) == 1 else ""


def reserve_ready_job(jobs, aliases_for_job, on_waiting):
    """Keep pending order, skipping blocked channels until one can run.

    Admission happens before queue/journal locks. The caller holds the returned
    lease through execution and releases it even if promotion fails.
    """
    for job in jobs:
        cancel = job.get("cancel")
        if cancel is not None and cancel.is_set():
            # Cancellation cleanup must not wait for an unrelated channel job.
            return job, None
        result = channel_leases.try_acquire(
            aliases_for_job(job), processing_owner(job), cancel_event=cancel)
        if result.ok:
            job.pop("_channel_wait_signature", None)
            return job, result.lease
        if result.status == "cancelled":
            return job, None
        signature = tuple((row.owner, row.job_id) for row in result.blockers)
        if job.get("_channel_wait_signature") != signature:
            job["_channel_wait_signature"] = signature
            on_waiting(job, result.blockers)
    return None, None
