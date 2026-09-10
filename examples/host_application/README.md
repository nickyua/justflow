# Host application example

This example shows how to use Justflow inside your application. It registers
providers, adds a typed `host_tenant_settings` resource, and supplies authentication.
The resource validates its configuration, exposes the `config` capability, and
handles startup and cleanup asynchronously. Unavailable access returns a Justflow
resource error.

The configuration is deliberately local-only: it binds the control server and
Temporal connection to loopback, uses plaintext only with the explicit local
runtime profile, and contains no credentials or external service addresses.
Importing the application validates its setup without making a network connection.
The ASGI server starts runtime components when the application starts.

Install and validate the example from the repository root:

```console
.venv/bin/pip install --editable examples/host_application
JUSTFLOW_RUNTIME__PROFILE=local .venv/bin/host-application-config validate \
  --source-dir examples/host_application/src/host_application/configs
```

The combined local process is convenient for development:

```console
PYTHONPATH=examples/host_application/src \
.venv/bin/uvicorn host_application.asgi:app --host 127.0.0.1 --port 8080
```

You can also run the worker and gateway separately with the same configuration:

```console
JUSTFLOW_RUNTIME__PROFILE=local PYTHONPATH=examples/host_application/src \
.venv/bin/uvicorn host_application.worker:create_worker_application \
  --factory --host 127.0.0.1 --port 8081
JUSTFLOW_RUNTIME__PROFILE=local PYTHONPATH=examples/host_application/src \
.venv/bin/uvicorn host_application.gateway:create_gateway_application \
  --factory --host 127.0.0.1 --port 8080
```

The default application reads bundled Git-managed files. To exercise the
writable single-host profile, publish and activate the same immutable bundle in
SQLite when composing the application:

```python
from pathlib import Path

from host_application import create_application

application = create_application(configuration_database=Path(".justflow/configuration.sqlite3"))
app = application.create_combined_app()
```

Initialization is idempotent. It creates the first active pointer but refuses to
replace an existing pointer with different content. The
`publish_local_configuration` and `activate_local_configuration` functions keep
publication and compare-and-swap activation separate for later revisions; a
successful activation retry returns the already-active revision. SQLite is for
one local host, not a horizontally scaled production deployment.

`RuntimeScope` is supplied by the host, never a request payload. The example
tests initialize two tenant/application/environment scopes in one SQLite file
and prove that a revision from one scope cannot be read through the other. In a
managed deployment, use separate scope-bound runtime compositions backed by the
S3/DynamoDB configuration stores and host authentication-derived scope.

The local authentication example accepts `x-host-subject: local-operator` to
demonstrate the interface. Use the production example below when testing real credentials.

## Production authentication and AWS

`host_application.production:create_gateway_application` uses typed credential
grants, expiry and rotation, trusted scope, HTTPS browser protection and the
optional admin panel. The matching worker factory is
`host_application.production:create_worker_application`. Both require an explicit
production profile and tenant/application/environment scope. The local factories
reject the production profile.

Read the [authentication guide](../../docs/operations/authentication.md) for the
two `HOST_APPLICATION_` settings listed in `.env.example`, credential issuance,
browser login, CSRF protection and revocation limitations. Supply credentials
from a secret manager; never put tokens or credential hashes into YAML or an image.

The [ECS/Fargate runbook](../../docs/aws/self-hosted-temporal.md) renders the two
production commands and settings. It lists the activation controller, initial
configuration, readiness checks, and routing updates your application still needs.
Selecting an AWS configuration store alone does not enable managed editing or
activation. First-deployment acceptance is still pending.

## Container runtime

This application is also the downstream Docker reference; there is no separate
container-only implementation. The base image contains Justflow, while the
application image adds this package and its immutable configuration. Node.js is
used only to prebuild optional administration assets and is absent at runtime.

From the repository root, build both images, validate Compose, and start
Temporal, configuration initialization, definition publication, the worker,
and the gateway:

```console
./scripts/run_local_stack.sh
```

The one-shot configuration and catalog services remain separate stages. Worker
and gateway then run as separate non-root, read-only containers from the same
application image. Rebuilding an image does not change a running process;
restart the services to select the new artifact and configuration.

```console
curl --fail http://127.0.0.1:8080/livez
curl --fail http://127.0.0.1:8080/readyz
curl --fail --header 'x-host-subject: local-operator' \
  http://127.0.0.1:8080/metrics
curl --fail --header 'content-type: application/json' \
  --header 'x-host-subject: local-operator' \
  --data '{"workflow_name":"hello","business_request_id":"compose-demo-1","input":{"subject_ref":"subject-demo"}}' \
  http://127.0.0.1:8080/v1/workflows
```

The script derives source revision, build date, and source URL from Git. A
production deployment must resolve the final application image digest and pass
that immutable value as `JUSTFLOW_DEPLOYMENT__ARTIFACT_DIGEST=sha256:...`.
Temporal UI is available at `http://127.0.0.1:8233` for raw history and
low-level Temporal debugging. The optional administration UI is not enabled by
this Compose application.

Stop without deleting state:

```console
docker compose down
```

`docker compose down --volumes` also deletes local SQLite and catalog state; use
it only when that reset is intended.
