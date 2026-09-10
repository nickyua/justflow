# Workflow authoring

The versioned [workflow schema](../reference/generated/schemas.md) defines the allowed fields.
The
[prime_stats](https://github.com/nickyua/justflow/blob/main/examples/prime_stats/configs/workflows/prime_stats.yaml)
and
[product_onboarding](https://github.com/nickyua/justflow/tree/main/examples/product_onboarding/configs/workflows)
definitions are executable examples.

## Steps, sequence, and result

`steps` names reusable operations. Each step has one `target`: `kind: service`
requires `service` and `action`, while `kind: workflow` requires one child `workflow`. `flow` names
each invocation and its transition. `result` publishes one resolved reference as workflow output.

```yaml
workflow: lookup_order
result: lookup.order
steps:
  get_order:
    target:
      kind: service
      service: orders
      action: get
flow:
  - name: lookup
    op: get_order
    input:
      order_ref: order_ref
    output: order
    then: done
  - name: done
    terminal: true
```

References resolve from trigger globals, workflow parameters, and earlier named outputs. Validation
rejects unknown references, cycles, unreachable states, and illegal transition combinations.

## Branching and failure handling

`on_result` evaluates in order and ends with one default target. `condition` can skip an operation
to its `then` target. `on_failure` handles an operation failure; workflow-level `on_error` routes
otherwise unhandled failures.

```yaml
- name: decide
  op: classify
  output: classification
  on_result:
    - when: output.accepted == true
      then: accepted
    - default: rejected
```

Failures are recorded with stable error codes. The transport or provider determines whether an
error can be retried; your workflow defines where to go if the operation fails.

## Fan-out and fork/join

`for_each` invokes one operation per item. `parallel: true` requires `max_concurrency`; the runtime
also enforces global item, allocation-chunk, and invocation limits. `on_iteration_fail` is `stop`,
`skip`, or `collect`.

```yaml
- name: inspect_items
  op: inspect
  input: load.items
  for_each: input
  as: item
  parallel: true
  max_concurrency: 8
  on_iteration_fail: collect
  output: inspections
  then: summarize
```

This is dynamic bounded fan-out with an implicit join. Static independent fork/join branches are
not a 0.1.0 feature.

## Polling loops and durable waits

An `until` operation retries with an optional durable interval and a bounded maximum. Exhaustion can
route to a named state.

```yaml
- name: poll
  op: read_status
  until: output.ready == true
  max_iterations: 20
  interval_sec: 30
  on_exhausted: timed_out
  output: status
  then: ready
```

`wait_for` durably waits for a declared signal/event and must have a relative or absolute timeout.
The product-onboarding example demonstrates the timeout branch.

```yaml
- name: approval
  wait_for:
    signal: approved
    timeout_sec: 3600
    on_timeout: approval_expired
  output: decision
  then: continue
```

### Absolute wait deadlines

`timeout_until` points to a workflow parameter or earlier step output that resolves when the wait
begins. The resolved value must be either numeric epoch seconds or an ISO-8601 timestamp with `Z` or
an explicit UTC offset. The runtime normalizes offset timestamps to UTC before calculating the
timer. A timestamp without an offset, such as `2026-09-01T12:00:00`, is invalid because it does not
identify one instant.

```yaml
- name: wait_for_confirmation
  wait_for:
    signal: confirmed
    timeout_until: schedule.confirmation_deadline
    on_timeout: request_manual_confirmation
  then: confirmed
```

Store and emit deadline timestamps in UTC with `Z`. If the source value represents local wall-clock
time, the producing action must resolve its IANA timezone and daylight-saving policy before
returning it. When both `timeout_sec` and `timeout_until` are present, the wait uses whichever
expires first. A deadline that has already passed takes the timeout path immediately.

Use `sleep_sec` for a durable delay that produces no output.

## Child workflows

A step with a workflow target starts a Temporal child using the exact definition selected for that
child. Its trigger globals contain the declared step parameters, the resolved flow input under
`input` when present, and the `as` alias during iteration. Parent workflow parameters are available
for interpolation but are not implicitly copied. The child's workflow input schema validates this
whole globals object, including these envelope fields. A child used both through an API and with
`as: customer` can require a `customer` field and accept the additional `input` field supplied by
the parent. Declare `customer: "${customer}"` in the child's workflow params so its flow can refer
to it. Iteration aliases cannot use reserved engine roots such as `input`. The child owns its
input/output contracts and internal cache.

```yaml
steps:
  provision:
    target:
      kind: workflow
      workflow: provision_workspace
    params:
      workspace_ref: "${workspace_ref}"
```

## Contracts and cache integrity

Workflow and action `input_schema`/`output_schema` accept an inline Draft 2020-12 JSON Schema or a
dotted Pydantic model path. `$ref` and `$dynamicRef` resolve only within the declared resource;
local pointers, anchors, dynamic anchors and nested `$id` resources are supported. An HTTPS `$id`
is an identifier, not a download. External references are rejected and the runtime resolver cannot
retrieve schemas from the network. Values use strict JSON: duplicate keys,
non-finite numbers, malformed cache entries, and schema-invalid values fail without coercion.

```yaml
steps:
  lookup:
    target:
      kind: service
      service: inventory
      action: lookup
    input_schema: application.contracts.LookupRequest
    output_schema: application.contracts.LookupResult
    cache:
      resource: workflow_cache
      key: inventory/${item_ref}
      ttl_sec: 300
```

Caching is for idempotent operations and is unavailable on child, `for_each`, or `until` steps.
The cache provider must protect sensitive values and enforce TTL.

## Audit capture

`on_complete` selects an archive resource and finite retention policy. Capture is metadata-only by
default. `redacted` requires explicit JSON Pointer paths and a byte bound; `approved-full` additionally
requires encrypted archival. Ordinary failure and completion attempt archival, while Temporal
service termination, timeout, and cancellation do not currently invoke it.

```yaml
on_complete:
  resource: audit_store
  path: audit/orders/${request_id}.json
  retention_policy: audit-30d
  capture:
    mode: redacted
    paths:
      - /params/contact_ref
    max_payload_bytes: 65536
```

See [security](../operations/security.md) for Temporal payload codecs, archive encryption, and
retention responsibilities.
