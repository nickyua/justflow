# Product onboarding example

This example accepts an onboarding request through the API, waits for approval
with a timeout, and starts a child workflow to provision a workspace. A local
fake service simulates provisioning, including a failure that triggers compensation.

The example contains no customer data, credentials, vendor SDKs, or external
network calls. Its metadata-only audit record is retained in memory for the
local process lifetime.

From the repository root:

```console
.venv/bin/pip install --editable examples/product_onboarding
PYTHONPATH=examples/product_onboarding/src .venv/bin/python -m justflow validate \
  --config-dir examples/product_onboarding/configs
JUSTFLOW_RUNTIME__PROFILE=local PYTHONPATH=examples/product_onboarding/src .venv/bin/python -m justflow definitions publish \
  --config-dir examples/product_onboarding/configs
PYTHONPATH=examples/product_onboarding/src .venv/bin/python -m justflow graph \
  product_onboarding \
  --config-dir examples/product_onboarding/configs \
  --output /tmp/product-onboarding.html
```

With a local Temporal development server listening on `127.0.0.1:7233`, start
the API and worker:

```console
PYTHONPATH=examples/product_onboarding/src \
JUSTFLOW_RUNTIME__PROFILE=local \
JUSTFLOW_TEMPORAL__CONNECTION__MODE=local_plaintext \
JUSTFLOW_DEPLOYMENT__BUILD_ID=product-onboarding-local \
JUSTFLOW_DEPLOYMENT__ARTIFACT_DIGEST=local-development \
JUSTFLOW_DEPLOYMENT__PACKAGE_VERSION=0.1.0-dev \
.venv/bin/python -m justflow serve \
  --config-dir examples/product_onboarding/configs \
  --temporal-address 127.0.0.1:7233 \
  --host 127.0.0.1 \
  --port 8322
```

Start one request. Repeating the same `business_request_id` returns the same
workflow identity instead of starting another execution:

```console
curl --fail --header 'content-type: application/json' \
  --data '{"workflow_name":"product_onboarding","business_request_id":"onboarding-demo-1","input":{"customer_ref":"customer-demo","plan":"starter","simulate_provisioning_failure":false}}' \
  http://127.0.0.1:8322/v1/workflows
```

Set `WORKFLOW_ID` to the response's `workflow_id`, then send the approval:

```console
WORKFLOW_ID=jf1.workflow.example
curl --fail --header 'content-type: application/json' \
  --data '{"payload":{"decision":"approved"}}' \
  "http://127.0.0.1:8322/v1/workflows/${WORKFLOW_ID}/events/onboarding_approved"
```

Set `simulate_provisioning_failure` to `true` with a new business request ID to
exercise compensation. Real CRM, catalog, and payment integrations replace the
local direct service with an approved HTTPS or queue transport; their
credentials and endpoints remain host-owned runtime configuration.
