# Changelog

Notable changes in public releases of Justflow.

## [Unreleased]

## [0.1.0] — 2026-09-10

Initial public beta release of `justflow` and the optional `justflow-admin`
package. Supports CPython 3.11–3.14.

### Workflow execution

- Define durable workflows in YAML and execute them on Temporal, with typed
  Python actions implementing application behavior.
- Compose sequential steps, result-based and conditional branches, bounded
  parallel iteration, child workflows, durable sleeps and event waits,
  polling loops, and failure paths.
- Validate definitions, expressions, references, and input/output contracts;
  configure per-step caching and explicit execution limits.
- Publish immutable workflow definitions, retain execution provenance, and
  check compatibility against representative recorded Temporal histories.
- Record workflow outcomes through configurable audit capture and archival,
  with redaction and host-provided payload encryption support.

### Triggers and scheduling

- Declare authorized API, schedule, webhook, CloudEvent, broker, and
  application-host triggers.
- Run recurring cron, calendar, or interval schedules with timezone,
  overlap, catch-up, pause/resume, and reconciliation controls.
- Create, inspect, reschedule, and cancel one-off scheduled starts through
  versioned APIs with idempotent operations.
- Build bounded customer batches and independent per-customer workflows.
  A global batch operates within its trusted tenant/application/environment
  scope; it does not bypass tenant authorization.
- Use the customer-workflow example to schedule reminders relative to an
  appointment. Application code computes the due time, such as two elapsed
  hours before the appointment, and submits a one-off scheduled start.

### APIs and administration

- Expose authenticated, tenant-scoped control and operations APIs for
  application automation and external dashboards.
- Provide versioned OpenAPI 3.1 contracts and JSON authoring schemas with
  offline export commands.
- Manage configuration drafts, publication, activation, and rollback through
  APIs composed with the consuming application's host bindings.
- Install the optional beta administration console over the same public APIs.
- Compose authentication, resources, providers, catalogs, and Temporal
  connections through typed Python interfaces.

### Integrations

- Execute actions directly in Python, through approved HTTPS endpoints,
  through TLS gRPC, or through broker-backed queues using SQS.
- Access S3 objects and archives, DynamoDB, Secrets Manager, SSM, PostgreSQL,
  Redis, and static configuration through registered resource providers.
- Configure transport deadlines, message and result limits, retry behavior,
  dead-letter handling, and receiving-service idempotency contracts.
- Supply custom transports, brokers, resources, and ingress mappings through
  explicit host registration.

### Tooling and documentation

- Validate definitions, run local workflows, start workers, manage triggers,
  and export contracts from the CLI.
- Render workflow diagrams as Mermaid or standalone interactive HTML.
- Follow runnable examples for local execution, host application composition,
  object ingestion, onboarding, scheduled reporting, and customer workflows.
- Use deployment and operations guides for local development and self-hosted
  environments, including an ECS/Fargate plan for self-hosted Temporal.
- Install optional capabilities through the `aws`, `grpc`, `postgres`,
  `redis`, `control`, `admin`, and `all` extras.

### Beta scope and limitations

- Workflow definitions, publishers, and provider registration must be trusted.
  Tenant-scoped APIs do not sandbox arbitrary Python action code.
- Production deployments require application-owned infrastructure,
  authentication, credentials, provider bindings, and operational acceptance
  tests. Live AWS qualification of the ECS/Fargate deployment remains pending.
- Production one-off scheduled starts require a shared authoritative quota
  binding from the host. Without it, creation is rejected with
  `quota_unavailable`. Recurring schedules do not use that quota controller.
- Managed configuration activation requires the consuming host's deployment
  and routing bindings.
- Queue delivery is at least once. Exactly-once external side effects are not
  guaranteed; receiving services must implement durable idempotency where
  required.
- The built-in gRPC transport uses JSON request/response bytes over unary
  RPCs. Generated Protobuf clients and streaming RPCs need a custom transport.
- Lambda is available for evaluation; its operational contract is outside
  the supported beta integration profile.
- Static independent fork/join branches, visual workflow editing, AI workflow
  building, and filesystem hot reload are not included.
- Runtime limits define correctness bounds, not throughput, availability,
  capacity, or regulatory certification claims.

See the [getting-started guide](docs/getting-started.md),
[customer workflow examples](docs/operations/customer-workflows.md),
[integration contracts](docs/integrations.md),
[limitations](docs/limitations.md), and
[AWS acceptance runbook](docs/aws/self-hosted-temporal.md).
