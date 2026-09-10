# Error taxonomy

HTTP errors use `{"error": {"code": string, "message": string}}`. Treat the 
bounded `code` as machine-readable and the message as operator-facing context. Validation 
diagnostics instead carry source, location, severity, category, and message.

## Public HTTP codes

- `activation_conflict`
- `activation_not_found`
- `activation_too_large`
- `activation_unavailable`
- `catalog_unavailable`
- `cloud_event_unavailable`
- `collection_limit`
- `configuration_conflict`
- `configuration_error`
- `configuration_not_found`
- `configuration_too_large`
- `configuration_unavailable`
- `confirmation_required`
- `conflict`
- `definition_unavailable`
- `dependent_triggers`
- `forbidden`
- `incompatible_worker`
- `input_rejected`
- `invalid_identity`
- `invalid_operation`
- `invalid_payload`
- `invalid_query`
- `invalid_request`
- `invalid_workflow_fragment`
- `not_found`
- `not_managed`
- `plan_conflict`
- `priority_unsupported`
- `provider_unavailable`
- `quota_exceeded`
- `quota_unavailable`
- `self_trigger_loop`
- `source_rejected`
- `stale_cursor`
- `temporal_unavailable`
- `trigger_paused`
- `trigger_unavailable`
- `unauthenticated`
- `unavailable`
- `unknown_schedule`
- `unknown_source`
- `unknown_workflow`
- `verification_failed`

## Typed runtime codes

### Workflow start

`invalid_request`, `unknown_workflow`, `definition_unavailable`, `incompatible_worker`, `input_rejected`, `configuration_error`, `trigger_paused`, `trigger_unavailable`, `temporal_unavailable`

### Workflow control

`invalid_request`, `filter_unavailable`, `not_found`, `temporal_unavailable`

### Operations query

`invalid_query`, `not_found`, `stale_cursor`, `unavailable`

### Schedule operation

`unknown_schedule`, `not_managed`, `invalid_operation`, `confirmation_required`, `temporal_unavailable`

### Schedule reconciliation

`catalog_unavailable`, `collection_limit`, `confirmation_required`, `plan_conflict`, `temporal_unavailable`

### Schedule apply

`ownership_changed`, `temporal_unavailable`

### Scheduled start

`invalid_request`, `trigger_unavailable`, `trigger_paused`, `input_rejected`, `not_found`, `conflict`, `quota_exceeded`, `quota_unavailable`, `priority_unsupported`, `temporal_unavailable`

### Webhook

`unknown_source`, `verification_failed`, `invalid_payload`, `invalid_identity`, `provider_unavailable`

### CloudEvent

`unknown_mapping`, `invalid_payload`, `invalid_identity`, `source_rejected`, `self_trigger_loop`, `mapping_unavailable`

## Validation diagnostics

Severities: `error`, `warning`.

Categories: `declaration`, `import`, `limit`, `lint`, `reference`, `semantic`, `provider`.
