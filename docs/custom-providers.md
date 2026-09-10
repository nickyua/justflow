# Custom providers

Write a custom provider when your application needs a transport, broker, or resource that is not
built in. The
[host/provider example](https://github.com/nickyua/justflow/tree/main/examples/host_application)
shows how to register one in application code.

## Provider contract

A provider has:

- a stable registered name and contract version;
- a strict Pydantic configuration model with unknown fields rejected;
- a factory that validates host bindings and returns the runtime object;
- declared capabilities, safe error messages, and asynchronous startup and cleanup;
- deterministic definition identity derived only from non-secret configuration.

Transport objects implement async `send()` and `close()`. Broker adapters own publish/consume and
settlement behavior. Resource objects expose only declared capabilities. Convert dependency
exceptions into the appropriate Justflow errors, keeping credentials and application data out
of messages returned to callers.

## Registration

Register providers in trusted host code, then pass the registry into `RuntimeApplication` or the
lower-level engine boundary. There is no entry-point discovery and declarations cannot import
provider code.

```python
from justflow.runtime import RuntimeApplication
from justflow.transports import builtin_transport_registry

transports = builtin_transport_registry()
transports.register(application_transport_provider)
runtime = RuntimeApplication(settings, transport_registry=transports)
```

The provider's strict config model participates in generated authoring schemas when schema
generation receives the same registry. Keep host-only bindings—credential objects, clients,
network identities, and mutable deployment state—outside that schema.

## Security and retry review

Before making a provider available, document:

- authentication and authorization source;
- TLS and endpoint validation;
- request/response size and time bounds;
- idempotency identity and what a timeout means;
- retryable versus permanent errors;
- queue settlement, redelivery, and dead-letter behavior where applicable;
- secret loading/rotation and log redaction;
- cleanup, health/readiness, metrics, and capacity limits.

Unit tests use fakes with no network. Put live-provider tests behind an explicit integration marker
and caller-owned credentials. The example host demonstrates configuration, resource, gateway, and
worker composition without external network access during import or build.
