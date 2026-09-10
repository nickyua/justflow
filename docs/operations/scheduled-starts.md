# One-off scheduled starts

A scheduled start asks Justflow to run a workflow once at a future time. Create it through the
tenant-scoped API with a workflow name, input, business request ID, timezone-aware `start_at`, and
workload class (`interactive`, `standard`, or `batch`). Your host controls the allowed workload
classes and input limits. Use a schedule trigger for recurring jobs and a batch workflow to
process a customer population.

Create, reschedule, and cancel require `x-idempotency-key`. Mutations also require the current
version. Retrying the same request recovers its result; a changed body or stale version returns a conflict.
Scope, task queue, Temporal IDs, and numeric priority are host-owned.

Persist the original request, idempotency key and stable authenticated principal before sending.
After an ambiguous response, retry that exact request, even if its original due time has passed.
Recovery checks retained acceptance before applying new-request time or active-trigger validation.
Do not recompute an appointment offset or replace the key when recovering an accepted operation.
Likewise, a successful reschedule or cancel can be recovered after later state transitions; its
response describes the recovered operation and is not necessarily the latest projection.

The [customer walkthrough](customer-workflows.md) includes an independent HTTP client and an
explicit two-hour reminder policy, with daylight-saving and already-due handling.

## Pending quota admission

Every create passes through a quota controller supplied by the host before its Temporal schedule is
created. In the local runtime profile, Justflow automatically installs a best-effort controller.
It serializes creates only within one gateway process and counts pending starts through Temporal
Visibility. Visibility is eventually consistent, and separate controller instances do not share
locks, so this local guard is not an exact cross-process quota.

Production hosts must provide a `ScheduledStartQuotaController` that coordinates quota decisions
across all gateway processes using shared state. When that controller is absent, creation fails with
`quota_unavailable` before a Temporal schedule is created. The built-in local controller is a
development convenience and must not be used as a production admission authority.

## Resolve at fire time

Acceptance validates input against the then-active workflow but does not pin that definition. At due
time Justflow arbitrates the lifecycle version, resolves the currently active definition and a
compatible worker, validates protected input again, and pins those immutable identities for the run.
If the new definition no longer accepts the input, dispatch fails instead of changing the input
to make it pass validation.

Operations projections omit the protected input. They reveal definition, artifact, workflow, and
run identity only after dispatch. Retain every definition, configuration/component revision,
worker artifact, and codec key needed by pending starts and their resulting runs.

## Concurrency and recovery

Due dispatch, reschedule, and cancel compete through one opaque version-scoped Temporal arbiter, so
one outcome owns each version. A request that exceeds the bounded API wait can return `in_progress`;
retry the same command and idempotency key. Internal due/arbiter workflows are infrastructure and
must not be terminated. Recover a stalled chain through Temporal reset using recorded input and the
documented operator procedure.

Cancellation before dispatch cancels the scheduled start. Once a workflow has started, use the
workflow cancellation or termination operation instead; canceling the schedule cannot undo an
already-started run. Cancellation is cooperative, while termination is immediate and can skip
workflow cleanup/audit behavior.

Terminal projections expire according to configured retention. Cleanup must not remove state while
an operation, replay, audit, or investigation still depends on it.
