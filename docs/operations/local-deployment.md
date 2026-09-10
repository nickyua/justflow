# Run Justflow locally

Use `justflow run` to try one workflow, keep the runtime running to use the
admin panel and operations APIs, or use Docker Compose to run separate workers
and a gateway. All repository examples
bind application ports to loopback and use a Temporal development server.

## Install the repository checkout

Python 3.11–3.14 is supported. The administration console is a separate
distribution, so a source checkout that enables the panel must install both the
engine and `packages/justflow-admin`.

<!-- tested: tests/test_documentation.py -->
```console
git clone https://github.com/nickyua/justflow.git
cd justflow
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]" -e packages/justflow-admin -e examples/prime_stats
```

An installed release can instead use `pip install "justflow[control,admin]"`.
The `admin` extra installs the matching console distribution.

## Run one workflow and exit

`justflow run` downloads and starts a temporary Temporal development server,
executes one workflow, and stops it. It does not preserve histories between
runs.

<!-- tested: tests/integration/test_direct_workflow.py -->
```console
.venv/bin/python -m justflow validate --config-dir examples/prime_stats/configs
.venv/bin/python -m justflow run prime_stats --config-dir examples/prime_stats/configs --param n=20 --param seed=42
```

Use this while editing actions or workflow files when you don't need to keep
the runtime running or inspect runs in the browser.

## Run the persistent development system

The persistent system has two long-lived server processes: a Temporal
development server and `justflow serve`. The latter combines worker, gateway,
operations APIs, and the admin panel's static files. The panel runs in the browser
and does not need a third server process.

Start Temporal in one terminal. Install the
[Temporal CLI](https://docs.temporal.io/cli) first if the `temporal` command is
not available.

<!-- tested: tests/integration/test_prime_stats.py -->
```console
temporal server start-dev --port 7233 --ui-port 8233
```

Publish the immutable definitions, then start the combined runtime in a second
terminal. Repeat publication after changing workflow declarations. The
`serve-demo` target supplies the local settings and expects
Temporal at `127.0.0.1:7233`.

<!-- tested: tests/integration/test_prime_stats.py -->
```console
JUSTFLOW_RUNTIME__PROFILE=local PYTHONPATH=examples/prime_stats/src .venv/bin/python -m justflow definitions publish --config-dir examples/prime_stats/configs
make serve-demo
```

Open the administration console at <http://127.0.0.1:8321/admin> and Temporal
UI at <http://127.0.0.1:8233>. Verify the runtime and start a workflow:

<!-- tested: tests/runtime/test_control_api.py -->
```console
curl --fail http://127.0.0.1:8321/livez
curl --fail http://127.0.0.1:8321/readyz
curl --fail --header 'content-type: application/json' --data '{"workflow_name":"prime_stats","business_request_id":"local-demo-1","input":{"n":20,"seed":42}}' http://127.0.0.1:8321/v1/workflows
```

Stop each foreground process with `Ctrl-C`. Local authoring saves workflows
under `configs/workflows/` and schedule declarations in `triggers.yaml`. A
successful save publishes definitions, but the running process retains its
startup snapshot; restart `make serve-demo` before starting against the new
revision.

Python integration hosts can keep the async
`justflow.engine.local_temporal.start_local_environment()` context open instead
of installing the Temporal CLI. The first call may download the development
server binary.

## Run separate services with Docker Compose

The repository Compose application uses one immutable host-application image
for separate worker and gateway services. One-shot configuration and catalog
initializers complete first, then both runtime services connect to the same
Temporal development server and persisted local volume.

Docker with the Compose plugin must be running. From the repository root:

<!-- tested: .github/workflows/ci.yml -->
```console
./scripts/run_local_stack.sh
```

The script builds the Justflow base and host-application images from the
current Git revision, validates `compose.yaml`, starts the stack, and waits for
health. It does not contact AWS. The gateway is at <http://127.0.0.1:8080> and
Temporal UI is at <http://127.0.0.1:8233>.

<!-- tested: tests/integration/test_host_application_example.py -->
```console
curl --fail http://127.0.0.1:8080/livez
curl --fail http://127.0.0.1:8080/readyz
curl --fail --header 'x-host-subject: local-operator' http://127.0.0.1:8080/metrics
curl --fail --header 'content-type: application/json' --header 'x-host-subject: local-operator' --data '{"workflow_name":"hello","business_request_id":"compose-demo-1","input":{"subject_ref":"subject-demo"}}' http://127.0.0.1:8080/v1/workflows
```

Stop the containers while retaining the local catalog and configuration state:

<!-- tested: .github/workflows/ci.yml -->
```console
docker compose down
```

Temporal development history is temporary in this setup. `docker compose
down --volumes` also deletes the retained catalog and configuration state; use
it only when an intentional clean reset is required.

## Limits of the local setup

- The Temporal development server uses plaintext, runs on one node, and does
  not provide production durability.
- The example host authentication accepts `x-host-subject: local-operator`
  only to demonstrate authentication. It is not production
  authentication.
- SQLite configuration and the local definition catalog are for one durable
  host volume, not independently replaceable replicas.
- Loopback exposure does not supply TLS, high availability, backup, workload
  identity, or disaster recovery.

For a production deployment, use separate gateway and worker roles, secured
durable Temporal, a shared durable catalog, host authentication and scope
resolution, injected secrets, and a final application image pinned by digest.
Continue with [deployment and durability](deployment.md) or the
[AWS deployment guide](../aws/deployment.md).
