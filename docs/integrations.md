# Integrations

Every action call uses one typed request/result contract, but transport security, retry, settlement,
and idempotency remain provider-specific. A service declaration selects a host-registered transport;
workflow YAML cannot import code or invent endpoints.

## Direct Python

`direct` invokes a trusted `BaseAction` in the worker process. It is fast and receives only declared
resources, but it is not a hostile-code sandbox: action code has the worker process's privileges.
Retries can repeat side effects, so actions must use request identity for idempotency.

Declared `TransportError` and `ResourceOperationError` retryability is preserved across direct
actions. An unexpected application exception receives a safe error message, without its raw body.

```python
from justflow import BaseAction


class InventoryActions(BaseAction):
    async def reserve(self, order_ref: str) -> dict[str, object]:
        return {"order_ref": order_ref, "reserved": True}
```

## HTTPS

The HTTPS transport uses a fixed validated base URL, bounded request/response bodies, connection and
dispatch deadlines, and an origin allowlist. Production endpoints require HTTPS. Authentication is
supplied by the host boundary; definitions must not contain tokens. Treat a timeout as unknown
external outcome and make the receiver idempotent before enabling retries.

Responses are streamed up to `max_response_bytes` (512 KiB by default), under an overall request
deadline, and closed on failure or cancellation. The transport requests identity encoding and
rejects compressed responses; configure the upstream to honor that requirement.

## TLS gRPC

Install `justflow[grpc]`. A service selects a named TLS profile from typed runtime settings; server
name, trust roots, and optional client identity are host-owned. Message sizes and deadlines are
bounded. TLS authenticates and protects the connection, not payloads retained by either system.

The built-in adapter supports JSON request/response bytes over unary RPCs. It is not a generated
Protobuf client and does not implement streaming RPCs. Use a custom transport for those contracts.
Concurrent connection attempts share one channel, and failed or canceled readiness closes it.

## Lambda

Install `justflow[aws]`. Lambda invocation uses workload identity and a configured function target,
never static AWS keys. Retry and timeout can outlive the caller's certainty about execution, so the
function must deduplicate durable side effects. Lambda is available for evaluation in 0.1; its
operational contract is not yet stable.

## Queue and broker calls

The `queue` transport publishes through an explicitly registered broker such as SQS. Logical
destinations resolve in typed runtime configuration; queue URLs, receipt handles, credentials, and
polling policy do not enter service YAML. Trigger and response consumers require dead-letter
destinations.

Delivery is at-least-once. Activity retries reuse the same message identity. `idempotency: durable`
requires the receiving service to persist identity-to-result and replay the stored response;
`idempotency: none` permits repeated side effects. Acknowledgement happens only after the matching
Temporal start or signal succeeds. Malformed, permanently unrouteable, and exhausted messages go to
the configured dead-letter destination.

## Resources

Built-ins cover memory and S3 archives, S3 objects, DynamoDB, Secrets Manager, SSM, PostgreSQL,
Redis, and static configuration. Resource capabilities are checked before startup. Direct actions
receive only their declared resource names, and initialization/cleanup follows dependency order.

AWS resources use workload identity. PostgreSQL/Redis can resolve credentials from a named runtime
secret or a secret-reader resource. Secret values remain outside declarations, definition identity,
diagnostics, and logs.

S3 object and catalog reads enforce byte limits while consuming the stream and close the body
even on rejection. Catalog metadata has a separate 64 MiB document ceiling.
PostgreSQL reads use a transaction-owned cursor, fetch one row ahead, and reject results exceeding
`max_rows` or `max_result_bytes` before retaining an unbounded collection. The command deadline
includes pool acquisition and consumption. Each row is decoded by the database driver before its
byte check; select bounded columns and paginate large datasets rather than reading arbitrary large
fields. Initialization failure closes the failing resource and previously initialized resources.

## Payload protection

`payload_protection.mode: codec` requires a host-provided `PayloadProtectionBinding` backed by an
`AuthenticatedPayloadCipher`. The cipher is an AEAD boundary: encryption and decryption receive
canonical associated data that binds the format version, key identity, and payload kind. A provider
must reject modified ciphertext, associated data, or authentication tags; successful plaintext
parsing is not an integrity check.

Temporal payloads and encrypted audit archives use format version 2. `active_key_id` selects the
write key and every key in `readable_key_ids` remains available for reads. Retain the worker build
and keys needed by every supported open history and archive. Codec mode fails closed when metadata,
version, key availability, authentication, or plaintext decoding is invalid.

## Webhooks, CloudEvents, and host registration

A webhook source must verify authenticity before mapping a bounded payload. A CloudEvent mapper
validates event type/source and preserves provider event identity for deterministic start
deduplication. A host adapter can expose application-native ingress but must assign scope from
trusted registration, not request data.

`RuntimeApplication` is the composition seam for registries, authentication, catalog,
configuration, payload protection, Temporal credentials, webhook sources, and CloudEvent mappings.
See the maintained
[host application](https://github.com/nickyua/justflow/tree/main/examples/host_application)
and [custom provider guide](custom-providers.md).

## Unsupported integration shortcuts

OWS import, arbitrary external Temporal activities, filesystem watch mode, Markdown injection,
visual editing, and AI workflow building are not 0.1 capabilities. Generate declarations in your
own tooling, then validate them against the public schemas and `justflow validate` boundary.
