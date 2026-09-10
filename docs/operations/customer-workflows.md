# Customer batches, individual runs and reminders

The examples in `examples/scheduled_reporting/customer_configs` show two ways to run customer
work. `customer_batch` fetches one page and starts an independent
`customer_check` child for each reference, with at most four children running concurrently.
`customer_check` can also start directly through its API trigger. A batch is global within the
host's trusted tenant/application/environment scope; it does not bypass tenant authorization.
Customer identifiers are opaque workflow input, never tenant credentials or scope selectors.

The sample directory contains two synthetic customers. A page contains at most 25 customers and
returns `next_cursor`; an empty page completes with no child starts. For a larger population,
persist the cursor in the consuming application and start further bounded pages with distinct,
stable business request IDs. Do not place the whole customer population in a trigger or history.
The batch collects child failures as `_error` outcomes, preserving successful siblings. Inspect
those outcomes: a completed batch does not imply every customer succeeded.

## Run the declarations locally

From a source checkout with the development dependencies installed:

<!-- tested: tests/integration/test_customer_workflows_example.py -->
```console
.venv/bin/pip install --editable examples/scheduled_reporting
PYTHONPATH=examples/scheduled_reporting/src .venv/bin/python -m justflow validate \
  --config-dir examples/scheduled_reporting/customer_configs
PYTHONPATH=examples/scheduled_reporting/src .venv/bin/python -m justflow run customer_batch \
  --config-dir examples/scheduled_reporting/customer_configs --param cursor=0 --param page_size=25
PYTHONPATH=examples/scheduled_reporting/src .venv/bin/python -m justflow run customer_check \
  --config-dir examples/scheduled_reporting/customer_configs \
  --param 'customer={"customer_ref":"customer_alpha"}'
```

The single-customer input is `{"customer":{"customer_ref":"customer_alpha"}}`. A parent
iteration also supplies an `input` envelope, so the child contract explicitly permits it. The
child only uses the declared `customer` parameter. See [child input mapping](../authoring/workflows.md#child-workflows).

## Daily 7 am versus one-off reminders

`daily_customer_batch` declares cron `0 7 * * *`, timezone `Europe/Zurich`, overlap policy
`buffer_one`, and a one-hour catch-up window. It starts paused. Select the actual business timezone
before publication; the schedule is a local wall-clock time, not a fixed UTC hour. Applying a paused
declaration cannot start a run. Publish definitions and use the documented
[trigger plan/apply and resume commands](../authoring/triggers.md) with this configuration directory.
Workers must contain the installed example package and matching definitions.

A daily recurring schedule does not use the one-off scheduled-start quota controller. An
appointment reminder is different: compute an explicit UTC due time and submit a scheduled-start
request. The example's `reminder_start_at` subtracts two **elapsed** hours after UTC normalization,
including daylight-saving transitions. Supply an unambiguous timezone-aware appointment timestamp
and an injected current time. An already-due new reminder raises `AppointmentTooLateError`; the
application must explicitly decide whether to skip it or start immediately. The helper never
silently changes that policy. Ambiguous local wall times require the host to select an offset/fold
before calling it; reject nonexistent local times when collecting appointments.

## Build a dashboard using only HTTP

`scheduled_reporting.customer_client.CustomerWorkflowClient` borrows an `httpx.AsyncClient` with
the gateway origin, authentication and finite timeout. It does not import admin UI code. It uses
public Pydantic API models, bounded response streaming, safe error codes and independent
idempotency keys. Use the [authentication guide](authentication.md) to configure a production
credential; the example's local demo headers are not accepted by the production factory.

```python
from datetime import datetime

import httpx
from scheduled_reporting.customer_client import CustomerWorkflowClient, reminder_start_at

from justflow.runtime import ScheduledStartCreateRequest
from justflow.runtime.api_models import StartApiRequest


async def check_customer(http: httpx.AsyncClient, customer_ref: str, request_id: str):
    client = CustomerWorkflowClient(http)
    return await client.start(StartApiRequest(
        workflow_name="customer_check",
        business_request_id=request_id,
        input={"customer": {"customer_ref": customer_ref}},
    ))


def new_reminder_request(
    customer_ref: str, request_id: str, appointment_at: datetime, *, now: datetime
) -> ScheduledStartCreateRequest:
    return ScheduledStartCreateRequest(
        workflow_name="customer_check",
        business_request_id=request_id,
        input={"customer": {"customer_ref": customer_ref}},
        start_at=reminder_start_at(appointment_at, now=now),
        workload_class="standard",
    )
```

Before first submission, persist the request and a create idempotency key in application storage.
Call `client.schedule(request, key=create_key)`. Retain `scheduled_start_id` and `version` from the
accepted response. If the response is lost, submit the exact persisted request and key again;
do not call `new_reminder_request` again after its due time.

To move an appointment, compute its new due time and persist a
`ScheduledStartRescheduleRequest(expected_version=version, start_at=new_due)` with a new operation
key, then call `client.reschedule(identity, request, key=reschedule_key)`. To cancel, persist a
`ScheduledStartCancelRequest(expected_version=version)` and call
`client.cancel(identity, request, key=cancel_key)`. Retry ambiguous mutations with their original
body and key. Handle `conflict` by fetching the current scheduled-start projection; never silently
overwrite another actor's update. An `in_progress` response means arbitration continues: retry the
same command. Cancellation cannot undo an already started customer workflow.

Read `/v1/operations/capabilities` first and expose only allowed controls. A production gateway
without a shared authoritative quota binding rejects new one-off starts with `quota_unavailable`.
Daily recurring batches can still be selected for the first deployment. The
[scheduled-start lifecycle](scheduled-starts.md) specifies retention and dispatch-time resolution.

## What is tested

Local Temporal integration tests execute the real batch declarations, individual children and
empty population. Separate tests deliver queue responses out of order to independent child runs
with a shared trace correlation, including one failed child. The standalone client test drives
create/reschedule/cancel and accepted-operation recovery through the actual ASGI API and Temporal
arbiter. Fixed-time unit cases cover the two-hour offset and daylight-saving transitions.

The first AWS project adds SQS, HTTPS, S3 and PostgreSQL providers to its own actions and resource
bindings. gRPC remains conditional. These examples do not claim live AWS qualification; the
[Fargate acceptance runbook](../aws/self-hosted-temporal.md) records those deployment checks.
