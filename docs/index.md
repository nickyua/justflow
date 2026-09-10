# Justflow

Justflow runs workflows written in YAML on Temporal. Write actions in Python or connect existing
services, then describe their order, branches, parallel work, and waits in a workflow file.
Temporal records progress so a workflow can resume after a worker restarts.

Use the public [HTTP API](reference/http-api.md) to build a dashboard or automate workflow
operations. The optional admin panel uses the same tenant-scoped APIs.

## Start here

- [Getting started](getting-started.md) installs the project and runs the
  `prime_stats` example.
- [Concepts](concepts.md) explains what Justflow, Temporal, and your application each do.
- [Authoring](authoring/index.md) covers workflow, service, resource, and trigger files.
- [Operations](operations/deployment.md) covers production roles, health, security, durability,
  and troubleshooting.
- [Run locally](operations/local-deployment.md) covers the one-shot runner, persistent development
  runtime with the admin panel, and Docker Compose.
- [AWS deployment](aws/deployment.md) provides an ECS/Fargate deployment plan and configuration
  templates. Live AWS acceptance checks remain part of the first deployment.
- [AWS data services](aws/data-services.md) covers DynamoDB, Redis, PostgreSQL, private networking,
  ECS secret injection, migrations, backup, and failover.

## What you get

- Published workflow definitions that stay unchanged for the runs using them.
- Durable execution with configurable limits on retries, waits, loops, parallel work, and history.
- Direct Python, HTTPS, TLS gRPC, and queue calls, plus database and storage providers.
- API and event triggers, recurring schedules, and one-off scheduled starts.
- Tenant-scoped workflow, configuration, and operations APIs, with health checks and metrics.
- Editor schemas, OpenAPI export, workflow validation, and diagrams.

See [integrations](integrations.md) for each provider's requirements and the evaluation-only
Lambda transport.

Justflow supports Python 3.11 through 3.14. It is an Apache-2.0 project; see
[support and release status](support.md) before adopting the 0.1 series.
