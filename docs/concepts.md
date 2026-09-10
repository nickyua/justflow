# Concepts and guarantees

## The model

- An **action** is application code that performs one operation.
- A **service** maps a stable name to a transport and retry/deadline policy.
- A **resource** is a named, capability-checked dependency such as a cache, archive, object store,
  database, or secret reader.
- A **workflow** describes a reusable process, such as checking one customer or processing a
  batch. Pass customer references as input and load data in actions instead of storing customer
  lists in workflow configuration.
- A **trigger** is an authorized way to start a workflow: API, recurring schedule, webhook,
  CloudEvent mapping, broker, or host adapter.
- A **definition** is a published workflow identified by a content digest. It includes the
  compatibility and environment information needed to run it and cannot be edited after publication.
- A **worker** executes workflows and actions from a Temporal task queue. Its deployment name and
  build ID identify the code it runs.
- The **host application** is your application that configures Justflow, including its
  authentication, integrations, and deployment settings.

Temporal records workflow progress, timers, signals, retries, and child workflows. Justflow
validates configuration, selects workflow definitions and compatible workers, checks triggers and
runtime limits, and provides tenant-scoped APIs. Your application configures external services;
your deployment is responsible for networking, credentials, storage, capacity, and backups.

Gateway health reports `worker_registration` when Temporal has recorded the pinned deployment route
for the workflow task queue. Registration is durable routing evidence, not a live-capacity signal;
starts can queue while every compatible worker is offline. A co-located or worker-only process also
requires the separate `worker` component, which reflects its own running worker lifecycle. External
deployments must monitor worker replicas and queue latency in addition to gateway readiness.

## Choosing how to start a workflow

Choose the mechanism that matches the job:

| Need | Use |
| --- | --- |
| Reuse orchestration for one entity reference | A workflow definition |
| Start it immediately | An authorized declared trigger |
| Start it once in the future | A scheduled start |
| Run it on a reusable cadence | A `schedule` trigger in `triggers.yaml` |
| Enumerate a population and fan out | An explicit bounded batch-parent workflow |

A scheduled start resolves the active definition when it fires; a recurring schedule trigger is
configuration reconciled to Temporal. Neither is a place to store a tenant population.

## Local and managed authoring

Local file configuration reads `resources.yaml`, `services.yaml`, `triggers.yaml`, and
`workflows/*.yaml`. Local-source console editing is opt-in, writes an optimistic-concurrency draft,
and requires process restart before future starts use it.

Managed configuration follows a sequence: edit a draft, validate it, publish a revision, prepare
and activate it, then reconcile schedules. Rollback activates a retained revision. Tenant documents
select approved typed components and bindings from a durable platform catalog; they cannot supply
Python classes, endpoints, credentials, or provider implementations.

## API and console

Use the [HTTP/OpenAPI contract](reference/http-api.md) for production clients. The beta admin
console is a separate optional distribution, uses same-origin scoped APIs, and never connects
directly to Temporal, cloud storage, or provider infrastructure. The upstream Temporal Web UI is
for infrastructure operators inspecting Temporal; it is not a tenant authorization boundary.

## Durability does not remove dependencies

An open run still depends on its Temporal history, definition, worker build, component contracts,
payload-codec keys, and external resources. Keep those dependencies until retention policy proves
that no supported run or replay needs them. See [deployment](operations/deployment.md) and
[security](operations/security.md).
