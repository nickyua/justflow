# HTTP API and OpenAPI

See [host authentication](../operations/authentication.md) for credentials, trusted scope,
permissions and the capability/mode map, and the [customer client walkthrough](../operations/customer-workflows.md)
for an example independent of the bundled admin panel.

Justflow ships one versioned OpenAPI 3.1 document generated from the same Pydantic contracts used by
the runtime. It covers public health detail, capabilities, workflow control, triggers, scheduled
starts, operations, and local/managed configuration routes.

<!-- tested: tests/test_openapi.py -->
```console
justflow api schema export --output .justflow/api
```

The export runs without a network connection. It creates `justflow-openapi-0.1.0.json` and refuses to
overwrite different content. The file is also available as the installed package resource
`justflow.openapi.bundled/justflow-openapi-0.1.0.json`.

## Contract rules

- `operationId` values are stable and unique within the versioned contract.
- Every operation declares public, host, or webhook security requirements.
- Request and response models reject unknown fields where the runtime does.
- Error bodies use the documented [error codes](generated/errors.md).
- List operations have page limits and opaque cursors. A cursor applies only to its scope and query.
- Operations that support safe retries document the required `x-idempotency-key` header.
- Protected host bindings, infrastructure identities, credentials, raw tenant scope choices, and
  business payload examples are intentionally absent.

Clients should send the documented media type, reject unknown incompatible API versions, treat
opaque identities/cursors as indivisible strings, and branch on stable error `code` rather than
message text. Re-read capabilities after authorization or deployment changes.

The optional admin client is contract-tested against fixtures that also validate against this
document. External dashboards and automation should generate or maintain their own client against
the exported OpenAPI document.

The Python functions for generating and exporting the document are listed under
[`justflow.openapi`](generated/python-api.md#justflowopenapi).
