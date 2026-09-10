# Configuration lifecycle

## Runtime scope and authority

Every operation is bound to a typed tenant/application/environment scope. Authentication grants the
scope for API callers; trusted host registration grants it for webhook sources, broker subscriptions,
schedules, CloudEvent mappings, and host adapters. Payloads cannot select scope. External identities
use a one-way scope digest rather than raw tenant labels.

The default `local/justflow/development` scope is for compatible local use. A configured runtime
scope is not a tenant registry and must never be filled from request data.

The runtime accepts an absent scope digest only for local-scope schedules, workflow targets, and
broker envelopes. Every non-local runtime requires an exact scope digest. This compatibility policy
remains necessary while unscoped local schedules or queued messages can still be observed; it can be
removed only after those schedules are deleted or migrated and every broker redelivery window has
closed.

## Configuration sources

Select exactly one typed source:

- `files` for reviewed local/application configuration;
- `sqlite` for a durable single-host managed store;
- `aws` for S3 revision bodies plus DynamoDB metadata/activation records;
- an explicit host-supplied `ConfigurationSource` implementation.

If the store is unavailable or a revision cannot be read, the operation fails instead of using
an empty configuration.

## Draft to activation

A managed configuration change follows these steps:

1. Create/update a draft using optimistic concurrency.
2. Validate declarations, component references, bindings, policy, and limits.
3. Publish an immutable content-addressed revision.
4. Plan an activation against the expected current revision.
5. Roll compatible workers, confirm their live capacity, and confirm Temporal route registration.
6. Reconcile schedule triggers.
7. Atomically advance the active revision and record the outcome.

Saving keeps your work in a draft. Publishing records a revision; activation makes it available
to new runs. If the expected version is stale, the server returns a conflict so you can review the
newer changes. Rollback activates a retained revision using the same readiness and schedule checks.

Tenant authors select exact typed components from an immutable platform catalog constrained by a
host policy. Catalog revision, component references, resolved service/resource bindings, definition,
and worker artifact are retained as execution provenance. Tenant configuration cannot name Python
classes, endpoints, providers, credentials, or task queues.

## Capabilities and quotas

The capabilities endpoint reports what the authenticated principal and configured providers can do;
clients must fail closed when a capability is absent or API compatibility is unsupported. Hosts set
bounded draft, revision, activation, schedule, and scheduled-start quotas. Outside the local runtime
profile, an unavailable authoritative scheduled-start quota controller returns an explicit error
rather than estimating acceptance. The automatic local controller is process-local,
visibility-based, and best-effort; see [scheduled starts](scheduled-starts.md#pending-quota-admission).

## Backup and recovery

Back up immutable revision bodies, configuration metadata, activation records, component catalogs,
and definition catalogs together with documented restoration order. For S3/DynamoDB, enable
versioning/PITR and test recovery into an isolated scope. After restore, verify content digests and
active pointers before starting gateways/workers; then reconcile schedules. Never rewrite an
immutable body to repair a pointer.

See [security](security.md), [scheduled starts](scheduled-starts.md), and
[troubleshooting](troubleshooting.md).
