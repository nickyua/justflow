# Troubleshooting

Start with the error code, request ID, workflow ID, and run ID. These let you follow a request
through logs and the operations API without sharing application data. For deployment or
compatibility problems, also record the scope, definition digest, worker build/image digest,
configuration revision, and audit record ID.

## Request did not start

1. Check authentication, authorization, scope, and the capabilities endpoint.
2. Confirm the declared trigger exists and is not paused.
3. Validate input and business/idempotency identity against OpenAPI and the workflow contract.
4. Check the definition catalog, `worker_registration`, live worker capacity, and queue latency.
5. Inspect `justflow_api_requests_total`, component health, and Temporal connectivity.

A timeout may mean Temporal accepted the operation after the client stopped waiting. Retry with
the same request and idempotency key. A new business request ID could start a duplicate workflow.

## Run is waiting or not progressing

Describe the run through the scoped operations API. Distinguish a durable signal wait, timer,
activity retry, child workflow, continuation, paused recurring trigger, and scheduled-start arbiter.
Then inspect the exact Temporal run as an infrastructure operator. Do not terminate internal Justflow
workflows; use reset/recovery procedures that preserve recorded input and identities.

## Failure, cancellation, and termination

The error code identifies a configuration, provider, input, compatibility, resource, transport,
or Temporal availability problem. Provider messages have size limits. Cancellation is cooperative
and may run cleanup; termination is immediate and may skip
workflow cleanup/audit. A failed workflow does not imply an external side effect was rolled back.

## Continuation and replay

Continue-as-new changes the Temporal run ID but preserves workflow identity, pinned definition,
deployment/build routing, and required state. Follow the continuation chain returned by operations.

Replay needs the retained history, exact definition semantics, compatible code path, component
contracts, and payload keys. A replay failure is a compatibility signal; do not suppress it or edit
an immutable definition. Apply Temporal patching for deterministic code changes and retain old
workers for the support window.

## Cache integrity

Cache hits are validated as strict JSON and against the same output contract as live results.
Malformed, non-finite, schema-invalid, or identity-mismatched entries fail. Investigate provider
integrity, namespace/version, TTL, and encryption; do not coerce or silently discard corruption.

## Broker delivery and dead letters

Queue delivery is at-least-once. Confirm message identity, destination mapping, visibility timeout,
consumer health, redelivery count, response correlation, and Temporal acceptance before settlement.
Malformed/permanent/exhausted messages move to the configured dead-letter destination. Redrive only
after fixing the cause and preserving the original identity; otherwise side effects can duplicate.

## Configuration and schedule drift

Compare active revision, expected draft version, worker registration, live worker capacity,
schedule plan digest, and actual owned Temporal schedules. A stale cursor, activation conflict,
ownership change, or plan conflict requires refreshing the state before retrying.
Restore immutable bodies before active pointers and reconcile schedules after recovery.

See the generated [error taxonomy](../reference/generated/errors.md) and
[settings reference](../reference/generated/settings.md).
