# Deployment and durability

## Process roles

Run independently scalable gateway and worker roles in production.

- The **gateway** owns authenticated ingress, webhook/broker consumers, scoped operations, and
  configuration APIs.
- The **worker** polls business and internal task queues and executes workflows/activities.
- An **activation controller**, supplied by your application, deploys compatible workers, checks
  readiness, reconciles schedules, and selects the active configuration revision.

`justflow serve` co-locates gateway and worker for local evaluation. `worker-server` adds probes to
a worker process. Use graceful shutdown at least as long as the configured drain window.

## Health and metrics

`/livez` proves the process loop is alive. `/readyz` is a public aggregate with no component detail.
Authenticated `/healthz` reports component status, and `/metrics` exposes aggregate
Prometheus text. A liveness failure can restart a stuck process; readiness removes an instance from
new traffic while a required dependency is unavailable.

Correlate gateway request identity with workflow/run ID, definition digest, worker artifact/image
digest, audit record, and metrics. Never place business data in correlation IDs or labels.

## Temporal connections

Choose one explicit profile:

- **Local:** loopback or explicitly named isolated host, `runtime.profile=local`, and
  `connection.mode=local_plaintext`. This is development only.
- **Secured self-hosted:** TLS server identity, optional mTLS, durable persistence/visibility,
  authentication, multi-AZ capacity, backup, upgrade, monitoring, and disaster recovery owned by
  the operator.
- **Temporal Cloud:** TLS endpoint, namespace, and workload-injected API key or mTLS identity.

TLS protects the network connection. A payload codec protects payloads persisted in Temporal
history. Neither replaces the other.

## Definitions and worker compatibility

Publish definitions to an immutable catalog before accepting starts. Multi-instance deployments
need a durable shared catalog; the S3 backend uses immutable conditional writes and alias
compare-and-swap. Enable bucket versioning, encryption enforcement, backup/recovery, and deletion
denial for ordinary worker/publisher roles.

Build production workers with explicit deployment name, build ID, package version, source revision,
and immutable `sha256:` artifact/image digest. Keep definitions, histories, worker builds, component
catalog revisions, configuration revisions, and payload keys while open executions or supported
replays depend on them. Retire a definition only after its aliases are inactive and dependency
checks prove it unused.

## Containers

Build your production application image from the Justflow base image, adding your actions,
providers, and configuration. Pin both the base and final image by digest. Run as the
image's non-root user, use read-only filesystems where possible, mount only bounded writable state,
inject secrets at runtime, and verify SBOM/signature/provenance for the exact digest.

The repository Compose file is for local testing. It has no high availability and uses a temporary
Temporal development server. See [run locally](local-deployment.md) for the Compose instructions
and [AWS deployment](../aws/deployment.md) for the ECS/Fargate plan.
The [AWS data-services guide](../aws/data-services.md) covers
DynamoDB, Redis, PostgreSQL, ECS secret injection, and private network flows.

## Capacity and limits

Defaults include bounded trigger, activity, workflow, audit, cache, signal, queue, collection,
fan-out, loop, history, and continuation sizes. The generated
[settings reference](../reference/generated/settings.md) is authoritative for values. These are
correctness boundaries, not throughput promises; load-test your application, provider endpoints,
Temporal namespace, and storage before production.
