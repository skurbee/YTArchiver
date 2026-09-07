# JS-callable API adapters

`main.Api` exposes the methods in 24 feature mixins to
`window.pywebview.api`. These classes preserve the existing bridge method names
while behavior moves into explicit services. A feature file is an adapter
boundary; adding a mixin does not by itself separate state ownership.

## Dependency ownership

New behavior belongs in a named service with constructor dependencies. Assemble
it through `AppServices` before the window admits calls. Keep the container a
dependency holder. For concrete examples:

- `ApplicationInformation` owns information projection and URL-history updates;
  `InfoMixin` delegates those endpoints through `services.information`.
- `StartupSequence` receives `StartupDependencies`; disk scanning and stage
  ordering are normal importable components with separate contracts. It does
  not receive the whole `Api` object.
- The catalog session owns read/transaction admission. Queue coordinators own
  exact-ID lifecycle commands. Call these owners instead of reassembling their
  locks and persistence steps in bridge methods.

Legacy feature adapters still use attributes initialized by `Api`:

| Attribute | Owner and purpose |
|---|---|
| `services` | Composed repositories, runtime dependencies and feature services |
| `_config` | Current UI snapshot; fresh reads go through the config repository |
| `_window` | Native window, absent before attachment; prefer the event bus |
| `_log_stream` | Application logging dependency, also in `services` |
| `_queues`, `_transcribe` | Existing aliases of composed worker dependencies |
| `_sync_thread`, `_sync_cancel`, `_sync_pause`, `_sync_skip` | Sync-lane lifecycle |
| `_redwnl_pending`, `_redwnl_lock` | Legacy runtime companion state; use lane commands |
| `_job_supervisor` | Admission, cancellation and shutdown ownership |
| `_autorun`, `_trash_retention`, `_auto_backup` | Long-lived schedulers |
| `_tray` | Optional native tray integration |

Do not create new cross-feature attributes on first use. Declare dependencies
where the component is assembled. Compatibility support for isolated legacy
adapters is not a template for new application code.

## Imports and results

Use explicit imports. `_shared.py` contains only bridge errors, dialog-result
normalization, common resolutions and the logger; it is not a union of backend
and standard-library imports. Do not import `main` from a backend module.

Use typed internal outcomes and stable error codes. Adapt them to the existing
bridge response at the endpoint. Distinguish failure or unavailability from a
successful empty result, and keep user-facing text separate from control codes.

## Background work

Bridge calls can overlap. Keep foreground queries bounded and use the existing
owners for long work. New background tasks must register before their thread
starts so shutdown and restoration can close admission and wait for the writer.

```python
import threading

from backend.services.job_supervisor import WorkAdmissionClosed
from backend.services.managed_work import start_managed_task

from ._shared import _api_err


def start_feature(self, payload):
    cancel = threading.Event()
    # Resolve and validate dependencies and input before admission.
    worker = self.services.feature  # A service assembled by the application.
    try:
        start_managed_task(
            self,
            owner="feature",
            label="Feature operation",
            cancel=cancel,
            target=lambda: worker.run(payload, cancel=cancel),
            name="feature-operation",
        )
    except WorkAdmissionClosed as exc:
        return _api_err("WORK_ADMISSION_CLOSED", str(exc), retryable=True)
    return {"ok": True, "queued": True}
```

`feature` is an illustrative new service, not an existing AppServices field.
For an existing workflow, use its coordinator rather than adding a second task
owner. Native window creation and single-instance handling belong exclusively
to the explicit desktop launch function.

## Verification

Use `scripts/check.ps1` for the full gate. Python test files require separate
processes. The root collection guard rejects directory/multi-file selection and
establishes a disposable profile before test imports. Test services through
their actual interfaces with supplied repositories and callbacks; avoid parsing
production source to extract a nested function for execution.

When changing an endpoint, update the browser fixture registry and verify its
response contract, delayed completion, and error handling. Edit HTML templates
or partials and regenerate `web/index.html` when markup or script order changes.
