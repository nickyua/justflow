# Justflow

**Durable YAML workflows executed on [Temporal](https://temporal.io).**

[![CI](https://github.com/nickyua/justflow/actions/workflows/ci.yml/badge.svg)](https://github.com/nickyua/justflow/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.11--3.14-blue)
![Typed](https://img.shields.io/badge/typing-py.typed-informational)

Describe your workflow in YAML and implement its actions in Python or call an existing service.
Temporal keeps track of progress, retries, and long-running waits. Justflow validates the workflow,
manages its definitions, and provides APIs to start and inspect runs.

## What you can build

- Multi-step processes with branches, parallel work, child workflows, and failure handling.
- Scheduled jobs, event-driven workflows, and appointment reminders.
- Customer batches and individual customer workflows within a tenant's scope.
- Workflows that call HTTPS, gRPC, or SQS-backed services and use resources such as S3 and PostgreSQL.
- Your own dashboard using the public API, or an installation with the optional admin panel.

See the [changelog](CHANGELOG.md) for the full feature list and beta limitations.

## Quickstart

The [`prime_stats`](examples/prime_stats) example runs a complete workflow on your machine.

<!-- tested: tests/integration/test_direct_workflow.py -->
```console
git clone https://github.com/nickyua/justflow.git
cd justflow
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]" -e examples/prime_stats
.venv/bin/python -m justflow validate --config-dir examples/prime_stats/configs
.venv/bin/python -m justflow run prime_stats --config-dir examples/prime_stats/configs --param n=20 --param seed=42
```

This command starts a temporary Temporal development server and stops it after the workflow
finishes. Use a persistent Temporal service for production.

## Install

<!-- tested: tests/test_documentation.py -->
```console
pip install justflow
pip install "justflow[control,admin]"
```

Core includes direct and HTTPS transports. Optional extras are `aws`, `grpc`, `postgres`, `redis`,
`control`, `admin`, and `all`. Justflow supports Python 3.11–3.14.

## Public interfaces

- Export versioned JSON Schemas for your editor with
  `justflow schema export --output .justflow/schemas`.
- Export the OpenAPI 3.1 document for dashboard and automation clients with
  `justflow api schema export --output .justflow/api`.
- The optional admin panel is a separate package that uses these same tenant-scoped APIs.
- Your application supplies authentication, integrations, storage, and Temporal credentials through
  typed Python interfaces.

## Documentation

- [Getting started](docs/getting-started.md)
- [Concepts and guarantees](docs/concepts.md)
- [Workflow and trigger authoring](docs/authoring/index.md)
- [Integrations and custom providers](docs/integrations.md)
- [Deployment, security, configuration, and troubleshooting](docs/operations/deployment.md)
- [Local runtime and Docker Compose](docs/operations/local-deployment.md)
- [AWS deployment](docs/aws/deployment.md)
- [AWS data services and ECS connectivity](docs/aws/data-services.md)
- [CLI, settings, schemas, OpenAPI, errors, and Python API](docs/reference/generated/cli.md)
- [Upgrade guide](docs/upgrading.md), [limitations](docs/limitations.md), and
  [support](docs/support.md)

## Development

<!-- tested: .github/workflows/ci.yml -->
```console
.venv/bin/pip install -e packages/justflow-admin
npm --prefix ui/admin ci
make check
```

This runs frontend checks, formatting, typing, unit/integration/replay/sandbox tests,
documentation checks, and package installation tests. See [CONTRIBUTING.md](CONTRIBUTING.md).

## Status and license

0.1 is beta software. Read the [support policy](SUPPORT.md), [security policy](SECURITY.md),
[changelog](CHANGELOG.md), and [limitations](docs/limitations.md) before production adoption.

Licensed under the [Apache License 2.0](LICENSE); attribution details are in [NOTICE](NOTICE).
