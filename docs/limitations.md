# Limitations and compatibility

## Supported baseline

- CPython 3.11, 3.12, 3.13, and 3.14.
- Temporal Python SDK 1.x within the package constraint; local development tooling uses the
  repository-pinned Temporal CLI.
- Authoring schema and OpenAPI version 0.1.0.
- Trusted workflow/service definitions, trusted trigger publishers, and explicit host provider
  registration.
- Direct, HTTPS, TLS gRPC, SQS-backed queue, resource, and host integration boundaries described in
  this documentation.

Optional extras are independent compatibility boundaries. Pin and scan your resolved application
environment. An integration is supported only when its external service, authentication, network,
retention, and retry requirements are also met.

## Not in 0.1

- Static independent fork/join branches (bounded dynamic `for_each` has an implicit join).
- OWS import or automatic translation from another workflow format.
- Arbitrary external Temporal activity registration/discovery.
- Filesystem watch/hot reload.
- Markdown/HTML injection into rendered diagrams.
- Visual workflow editing or AI workflow building.
- Treating the Temporal Web UI as tenant authorization.
- Exactly-once external side effects.
- A production-ready single-node Temporal topology.
- Regulatory certification or automatic downstream application security certification.

Lambda exists for evaluation, but its operational contract is not yet stable. The beta admin
console can change within the documented API compatibility envelope; production integrations should
use OpenAPI.

## Resource envelope

Runtime payload, collection, fan-out, loop, history, cache, audit, message, query, and API limits are
typed and fail closed. See [settings](reference/generated/settings.md). They are defaults and hard
bounds for correctness, not latency, throughput, availability, or capacity claims. Larger workloads
need explicit configuration plus representative testing.

## Replay and retention

Justflow cannot replay without retained Temporal history, compatible worker code, exact definitions,
component/provider contracts, and readable codec keys. It cannot guarantee an external provider's
idempotency, settlement, durability, or disaster recovery. Those guarantees must be stated and
tested by the provider and deployment owner.
