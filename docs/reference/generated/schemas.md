# Schemas

Authoring schemas are versioned as `0.1.0` and ship in the wheel. 
Export them without network access with:

<!-- tested: tests/test_schemas.py -->
```console
justflow schema export --output .justflow/schemas
```

| Declaration | Schema identifier | Installed resource |
| --- | --- | --- |
| `configuration-envelope.schema.json` | `https://justflow.dev/schemas/0.1.0/configuration-envelope.schema.json` | `justflow.schemas.bundled/configuration-envelope.schema.json` |
| `configuration.schema.json` | `https://justflow.dev/schemas/0.1.0/configuration.schema.json` | `justflow.schemas.bundled/configuration.schema.json` |
| `platform-component-catalog.schema.json` | `https://justflow.dev/schemas/0.1.0/platform-component-catalog.schema.json` | `justflow.schemas.bundled/platform-component-catalog.schema.json` |
| `resources.schema.json` | `https://justflow.dev/schemas/0.1.0/resources.schema.json` | `justflow.schemas.bundled/resources.schema.json` |
| `runtime-scope.schema.json` | `https://justflow.dev/schemas/0.1.0/runtime-scope.schema.json` | `justflow.schemas.bundled/runtime-scope.schema.json` |
| `services.schema.json` | `https://justflow.dev/schemas/0.1.0/services.schema.json` | `justflow.schemas.bundled/services.schema.json` |
| `tenant-authoring-policy.schema.json` | `https://justflow.dev/schemas/0.1.0/tenant-authoring-policy.schema.json` | `justflow.schemas.bundled/tenant-authoring-policy.schema.json` |
| `tenant-configuration.schema.json` | `https://justflow.dev/schemas/0.1.0/tenant-configuration.schema.json` | `justflow.schemas.bundled/tenant-configuration.schema.json` |
| `triggers.schema.json` | `https://justflow.dev/schemas/0.1.0/triggers.schema.json` | `justflow.schemas.bundled/triggers.schema.json` |
| `workflow.schema.json` | `https://justflow.dev/schemas/0.1.0/workflow.schema.json` | `justflow.schemas.bundled/workflow.schema.json` |

## HTTP API

OpenAPI `0.1.0` ships as `justflow.openapi.bundled/justflow-openapi-0.1.0.json`. Export it with:

<!-- tested: tests/test_openapi.py -->
```console
justflow api schema export --output .justflow/api
```

Configure an editor to use the exported file that matches the declaration being 
edited. The export is conflict-safe: it refuses to overwrite different content.
