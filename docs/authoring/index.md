# Authoring

Justflow configuration uses four kinds of YAML files. Export the schemas for your installed version
and configure your editor to use the matching schema for each file.

| File | Purpose | Schema |
| --- | --- | --- |
| `resources.yaml` | Databases, caches, archives, and other named dependencies | [resources](../reference/generated/schemas.md) |
| `services.yaml` | Transport, deadline, retry, and service defaults | [services](../reference/generated/schemas.md) |
| `triggers.yaml` | Authorized API, schedule, webhook, event, broker, and host starts | [triggers](../reference/generated/schemas.md) |
| `workflows/*.yaml` | Steps, flow, contracts, result, and audit policy | [workflow](../reference/generated/schemas.md) |

Managed authoring additionally uses the configuration envelope, tenant configuration, tenant
policy, runtime-scope, and platform-component-catalog schemas listed in the same reference.

<!-- tested: tests/test_schemas.py -->
```console
justflow schema export --output .justflow/schemas
justflow validate --config-dir configs --format json
```

## Services and resources

A service selects one registered transport and sets its timeouts and retry limits. Before enabling
retries, make sure repeating a request won't duplicate an external side effect.

```yaml
services:
  inventory:
    transport: https
    transport_config:
      base_url: https://inventory.internal
    connect_timeout_sec: 5
    dispatch_timeout_sec: 10
    response_timeout_sec: 30
    retries: 2
```

A resource declares exactly one `provider` or trusted application-local `class`. Provider config is
strictly validated. Credentials belong in typed runtime bindings or workload identity, not YAML.

```yaml
resources:
  audit_store:
    provider: memory_archive
    config:
      retention_policies:
        audit-30d: 2592000
```

The built-in provider schemas are generated into `resources.schema.json` and
`services.schema.json`. Host-registered providers extend schema generation when their explicit
registries are supplied.

## Workflow and trigger guides

- [Workflows](workflows.md) covers branching, fan-out, loops, waits, child workflows, failure,
  caching, contracts, and audit.
- [Triggers](triggers.md) covers every declared trigger kind, schedule reconciliation, event starts,
  and the difference between recurring schedules and scheduled starts.
- [Integrations](../integrations.md) covers transports, retries, and security requirements.
- [Custom providers](../custom-providers.md) shows how to add an integration to your application.

Keep credentials and customer records out of configuration. Pass references in workflow input
and load the data through services or resources with the required permissions.
