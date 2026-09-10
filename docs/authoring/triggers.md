# Trigger authoring

A workflow can start only through a declared trigger that the caller is authorized to use.
The [trigger schema](../reference/generated/schemas.md) defines six kinds.

| Kind | Host-owned binding | Purpose |
| --- | --- | --- |
| `api` | authenticated API permission | Immediate API start |
| `schedule` | reconciled Temporal Schedule | Recurring cadence |
| `webhook` | registered verifier/source | Signed webhook start |
| `event` | registered CloudEvent mapper | Cloud/provider event start |
| `broker` | registered broker subscription | Queue/topic start |
| `host` | registered adapter | Application-owned ingress |

```yaml
triggers:
  orders_api:
    kind: api
    workflow: process_order
  orders_webhook:
    kind: webhook
    workflow: process_order
    source: commerce
  object_created:
    kind: event
    workflow: ingest_object
    mapping: s3_object_created
```

Source, mapping, broker, and adapter names resolve only from trusted host registration. Payloads
cannot select a scope or add authority. `paused: true` blocks new starts for every kind.
Engine-owned maintenance, activation, scheduled-start, and dispatch workflows are internal-only:
they are not declared tenant targets, are omitted from public workflow lists, and cannot be started
through an external trigger.

## Recurring schedules

A schedule trigger sets the cadence, timezone, overlap behavior, catch-up limits, optional backfill,
input, and an optional fixed definition digest. Cron, calendar, and interval specs are supported; interval
schedules use UTC.

```yaml
triggers:
  daily_reporting:
    kind: schedule
    workflow: scheduled_reporting
    input:
      report_date: "2026-08-10"
    spec:
      kind: cron
      expressions: ["0 9 * * *"]
    timezone: Europe/Zurich
    overlap_policy: buffer_one
    catch_up_window_seconds: 3600
    paused: true
    backfill:
      enabled: true
      max_window_seconds: 604800
      max_actions: 7
```

Plan before apply. Deployment automation may use `--non-interactive`; interactive operation uses
the freshly computed plan digest. Run-now requires an idempotency key.

<!-- tested: tests/runtime/test_schedule_operations.py -->
```console
justflow triggers --config-dir configs plan
justflow triggers --config-dir configs apply --confirm PLAN_DIGEST
justflow triggers --config-dir configs trigger-now daily_reporting --idempotency-key RUN_IDENTITY
```

Pause state is preserved as operator state across reconciliation. Deletion requires the current
desired identity. Schedule reconciliation never adopts unrelated Temporal schedules.

## Migrating `schedules.yaml`

The 0.1 contract uses `triggers.yaml`; `schedules.yaml` and `justflow schedules` are obsolete.
Move each declaration under `triggers`, add `kind: schedule` and its `workflow`, validate, then use
`justflow triggers plan`. The plan reports retained unscoped state and requires an explicit migrate
or retain decision before mutation.

## One-off scheduled starts

Scheduled starts are API-created future invocations, not YAML and not recurring configuration.
They have independent versioned reschedule/cancel semantics and resolve the active definition at
fire time. See [scheduled-start operations](../operations/scheduled-starts.md).
