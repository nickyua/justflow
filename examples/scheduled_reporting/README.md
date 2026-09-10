# Scheduled reporting example

For a daily 7 am customer batch, individual customer workflows, and appointment
reminders, see the [customer walkthrough](../../docs/operations/customer-workflows.md).
It includes a standalone API client. The customer examples live in `customer_configs`;
the reporting example below uses `configs`.

This example declares a paused daily schedule in `Europe/Zurich`, builds one
report for an explicit input date, and records a local fake delivery. The date
is supplied as workflow input, so tests and manual runs never read the wall
clock. Engine schedule tests cover skipped and repeated daylight-saving wall
times with fixed calendar inputs.

The local profile injects a read-only `static` configuration resource. It
contains no secret values and is granted only to the report-building action.

From the repository root:

```console
.venv/bin/pip install --editable examples/scheduled_reporting
PYTHONPATH=examples/scheduled_reporting/src .venv/bin/python -m justflow validate \
  --config-dir examples/scheduled_reporting/configs
PYTHONPATH=examples/scheduled_reporting/src .venv/bin/python -m justflow run \
  scheduled_reporting \
  --config-dir examples/scheduled_reporting/configs \
  --param report_date=2026-08-10
JUSTFLOW_RUNTIME__PROFILE=local PYTHONPATH=examples/scheduled_reporting/src .venv/bin/python -m justflow definitions publish \
  --config-dir examples/scheduled_reporting/configs
```

With a local Temporal server running, inspect and apply the desired trigger:

```console
PYTHONPATH=examples/scheduled_reporting/src .venv/bin/python -m justflow triggers \
  --config-dir examples/scheduled_reporting/configs \
  --temporal-address 127.0.0.1:7233 \
  --runtime-profile local \
  plan
PYTHONPATH=examples/scheduled_reporting/src .venv/bin/python -m justflow triggers \
  --config-dir examples/scheduled_reporting/configs \
  --temporal-address 127.0.0.1:7233 \
  --runtime-profile local \
  apply --confirm PLAN_DIGEST_FROM_PREVIOUS_COMMAND
```

The schedule starts paused, so applying it won't run the workflow. Use the
same command prefix to resume, pause, run it now, or backfill missed runs:

```console
PYTHONPATH=examples/scheduled_reporting/src .venv/bin/python -m justflow triggers \
  --config-dir examples/scheduled_reporting/configs \
  --temporal-address 127.0.0.1:7233 --runtime-profile local \
  resume daily_reporting
PYTHONPATH=examples/scheduled_reporting/src .venv/bin/python -m justflow triggers \
  --config-dir examples/scheduled_reporting/configs \
  --temporal-address 127.0.0.1:7233 --runtime-profile local \
  pause daily_reporting
PYTHONPATH=examples/scheduled_reporting/src .venv/bin/python -m justflow triggers \
  --config-dir examples/scheduled_reporting/configs \
  --temporal-address 127.0.0.1:7233 --runtime-profile local \
  trigger-now daily_reporting --idempotency-key manual-demo-1
PYTHONPATH=examples/scheduled_reporting/src .venv/bin/python -m justflow triggers \
  --config-dir examples/scheduled_reporting/configs \
  --temporal-address 127.0.0.1:7233 --runtime-profile local \
  backfill daily_reporting \
  --start 2026-08-03T09:00:00+02:00 --end 2026-08-10T09:00:00+02:00
```

`trigger-now` retries with the same idempotency key resolve to the same request
identity. The configured backfill window and action count bound every backfill.
The active schedule retains its pinned definition and runtime target until an
explicitly confirmed apply changes it.

For AWS, keep the local configuration resource unchanged and add a separate
host-owned `aws_secrets_manager` resource for sensitive delivery credentials.
Its YAML contains only immutable secret aliases and optional version selectors;
the AWS SDK obtains credentials from workload identity. An action granted that
resource reads through `SecretReader` and must never return or log the value.
Do not replace the local example with a credential-bearing configuration file.
