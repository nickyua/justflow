# Getting started

## Install

Create a Python 3.11–3.14 environment. The core package includes direct Python and HTTPS calls.
Install extras for the other integrations you need.

<!-- tested: tests/test_documentation.py -->
```console
python3 -m venv .venv
.venv/bin/pip install justflow
.venv/bin/pip install "justflow[control,admin]"
```

Available extras are `aws`, `grpc`, `postgres`, `redis`, `control`, `admin`, and `all`. Use a
lockfile to pin your application's dependencies in production. You will also need to configure
credentials and permissions for any external services you use.

## Run an example

The repository's
[prime_stats example](https://github.com/nickyua/justflow/tree/main/examples/prime_stats)
generates numbers from a fixed seed, checks them for primality in parallel, summarizes the
results, and writes a local audit record.

<!-- tested: tests/integration/test_direct_workflow.py -->
```console
git clone https://github.com/nickyua/justflow.git
cd justflow
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]" -e examples/prime_stats
.venv/bin/python -m justflow validate --config-dir examples/prime_stats/configs
.venv/bin/python -m justflow run prime_stats --config-dir examples/prime_stats/configs --param n=20 --param seed=42
```

`justflow run` starts a temporary Temporal development server, executes one workflow, and stops
the server. Use a persistent Temporal service when you need to keep workflows running.

## Validate and visualize

Export the installed schemas into an editor workspace, validate all declarations together, then
render a self-contained HTML graph.

<!-- tested: tests/test_schemas.py -->
```console
.venv/bin/python -m justflow schema export --output .justflow/schemas
.venv/bin/python -m justflow validate --config-dir examples/prime_stats/configs --format json
.venv/bin/python -m justflow graph prime_stats --config-dir examples/prime_stats/configs --output prime_stats.html
```

The schema checks fields and provider configuration. `validate` also checks references,
cycles, importability, workflow semantics, resource capabilities, and runtime limits. A graph
contains definition metadata, never runtime payloads.

## Publish definitions

Production starts resolve an active workflow name to an immutable definition digest. Publish
after validation and before starting workers:

<!-- tested: tests/definitions/test_definition_catalog.py -->
```console
.venv/bin/python -m justflow validate --config-dir configs
JUSTFLOW_RUNTIME__PROFILE=local .venv/bin/python -m justflow definitions publish --config-dir configs
```

The local catalog stores definitions on disk. It works for development or a single host with
persistent storage. Workers running on separate hosts need shared storage, such as the S3 backend in
[deployment](operations/deployment.md).

## First container run

The repository Compose stack is an isolated local demonstration with a Temporal development
server, one-shot configuration/catalog initialization, worker, and API. It exposes only loopback
ports.

<!-- tested: .github/workflows/ci.yml -->
```console
docker compose up --detach --wait --wait-timeout 120
curl --fail http://127.0.0.1:8080/livez
curl --fail http://127.0.0.1:8080/readyz
docker compose down --volumes
```

For production, split gateway and worker roles, provide authentication, use secured durable
Temporal, and pin the application image by digest. See [run locally](operations/local-deployment.md)
for the persistent panel and full Compose procedures. Continue with [concepts](concepts.md),
[deployment](operations/deployment.md), or the [AWS deployment guide](aws/deployment.md).
