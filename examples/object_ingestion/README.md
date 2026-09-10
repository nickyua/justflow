# Object ingestion example

This example starts a workflow when EventBridge reports that an S3 object was
created. It accepts only JSON objects under
`s3://example-ingestion/incoming/` and declares `archive/` as a write
destination, preventing a workflow write from matching its own source filter.

The sample event uses synthetic data. The workflow runs Python actions locally
and stores audit metadata in memory. Validation and tests do not contact AWS.

From the repository root:

```console
.venv/bin/pip install --editable examples/object_ingestion
PYTHONPATH=examples/object_ingestion/src .venv/bin/python -m justflow validate \
  --config-dir examples/object_ingestion/configs
JUSTFLOW_RUNTIME__PROFILE=local PYTHONPATH=examples/object_ingestion/src .venv/bin/python -m justflow definitions publish \
  --config-dir examples/object_ingestion/configs
```

With a local Temporal server on `127.0.0.1:7233`, start the application:

```console
PYTHONPATH=examples/object_ingestion/src \
.venv/bin/uvicorn object_ingestion.asgi:app \
  --host 127.0.0.1 \
  --port 8323
```

Submit the fixture through the same public route used by an external event
adapter:

```console
curl --fail --header 'content-type: application/json' \
  --data @examples/object_ingestion/fixtures/s3-object-created.json \
  http://127.0.0.1:8323/events/s3_object_created
```

Redelivering the same object version and sequencer derives the same business
request identity, so Temporal accepts only one workflow execution. The event's
outer delivery ID may change without changing that object identity. Unmatched,
malformed, oversized, or self-generated archive events are rejected before a
workflow starts and can be settled to the configured dead-letter destination
by broker ingress.

A real deployment uses the same mapper with EventBridge/SQS ingress and a
host-owned S3 object provider. Bucket names, IAM permissions, endpoints, and
workload credentials remain runtime bindings; they are not tenant-authored
workflow parameters or browser-visible configuration.
