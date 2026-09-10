# Upgrading to 0.1

## Class declarations to typed providers

Resource declarations can name a trusted application-local class directly. For a reusable
integration, register a provider so its configuration, capabilities, and version can be validated:

1. Define a strict Pydantic provider config model.
2. Give the provider a stable name, contract version, capabilities, and typed factory.
3. Register it in host code and supply the registry to runtime/schema generation.
4. Change YAML from `class` to `provider` plus provider-owned `config`.
5. Keep credentials and client objects in host bindings/workload identity, outside config.
6. Validate, publish a new definition, and retain old provider code for open-run replay support.

Do not mass-rewrite immutable definitions. Existing runs remain bound to their selected definition
and worker build.

## Local catalog to durable catalog

The local catalog's durability is the filesystem it occupies. Before independently replacing or
scaling workers:

1. Create a versioned, encrypted S3 bucket/prefix with least-privilege roles and backup policy.
2. Configure the typed S3 catalog settings without static credentials.
3. Run the migration dry-run and review additions/conflicts.
4. Apply the migration; identical retry is safe.
5. Verify immutable manifests, aliases, and environment snapshots by digest.
6. Roll gateways/workers by immutable image digest while retaining the local copy for recovery.

<!-- tested: tests/definitions/test_catalog_migration.py -->
```console
justflow definitions migrate --config-dir configs --dry-run
justflow definitions migrate --config-dir configs
```

## `schedules.yaml` to `triggers.yaml`

Move schedule entries beneath `triggers`, set `kind: schedule` and `workflow`, then validate and use
`justflow triggers plan/apply`. The old `justflow schedules` surface is not part of 0.1. Preserve
operator pause state and make an explicit decision for retained unscoped schedules.

## Compatibility discipline

### Queue response matching in 0.1.0

The version 1 broker envelope stays unchanged. Receivers must echo `message_id` as `in_reply_to`
and preserve `step_invocation_id`, workflow/run IDs, step name and action. Correlation IDs remain
tracing metadata and can differ from business request IDs, including in child workflows.

| Worker | Relay | Compatibility |
| --- | --- | --- |
| Previous | Previous | Legacy matching; independent correlation IDs can strand a wait |
| 0.1.0 | Previous | Legacy activity signal keys remain accepted |
| 0.1.0 | 0.1.0 | Invocation matching, including old histories already waiting |
| Previous | 0.1.0 | Unsupported; upgrade workers first |

Upgrade all workers capable of receiving retained queue responses before upgrading relays.
The `justflow-queue-invocation-response-v1` Temporal patch preserves legacy replay decisions.
The new worker also accepts the canonical invocation key for old waiting histories. Rolling back
only workers while new relays are active is unsafe; preserve the compatible pair and retained
histories during rollback.

Relay deduplication is bounded and process-local. An in-flight duplicate or exhausted pending
capacity is retried, never acknowledged as completed. Only successful Temporal delivery permits
completion/acknowledgement. Completed entries can expire or be evicted; another replica or a
restart can deliver again. Receivers and workflows must tolerate at-least-once delivery.

### Explicit production scope

The production profile rejects the local compatibility scope. Set all three runtime scope fields
and bind principals to that trusted scope. Offline CLI commands use the local profile unless
explicitly configured otherwise; starting production processes requires production settings.
The example's demo authentication factories now reject production use.

### Publication retries

Retain the original request body, idempotency key and stable actor identity after an ambiguous
publication or activation response. An applied operation is recovered before current draft or
active-pointer checks. A changed body, actor or activation plan is a new/conflicting operation.
Do not delete operation receipts or their immutable revisions during the promised retry window.

Review the changelog, regenerate schemas/OpenAPI, validate all configuration, replay retained
histories, and exercise downstream image/security gates before deployment. Definition changes get a
new digest; worker implementation changes get a new build ID; deterministic workflow-code changes
use Temporal patching. Keep old definitions, workers, catalogs, revisions, and codec keys for the
published support window.
