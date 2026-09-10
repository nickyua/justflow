# Authentication and external dashboards

The administration panel and external clients use the same HTTP APIs. A host supplies an
`AuthenticationProvider` to `RuntimeApplication`; installing the package does not choose an
identity provider. Production requires an explicit `RuntimeScope` and authentication binding.

## Host contract

`authenticate(AuthenticationRequest)` verifies credentials and returns an `AuthenticatedPrincipal`
with a stable, opaque `principal_id`, trusted `scope_grants`, and one `effective_scope`.
`authorize(principal, AuthorizationRequest)` checks the action, scope and optional resource digest.
See the generated [action enum](../reference/generated/authorization-actions.md).

Do not derive grants from a workflow input, customer ID, query parameter or caller-supplied tenant
header. Customer/global execution has the same authorization scope. A population workflow gains no
extra authority; its host-owned database query must constrain the tenant and bound its page size.
For multiple granted scopes, the host selects an effective scope only after checking membership.

Raise `AuthenticationError` for rejected credentials (generic HTTP 401), return `False` for a denied
operation (403), and let unexpected identity-service failures become the API's generic 503. Never
log credentials. Remote identity lookups must use asynchronous I/O or bounded offloading. Capability
checks have no resource identity; each concrete request still gets an authorization check.

## Production credential example

The [host example](https://github.com/nickyua/justflow/tree/main/examples/host_application) includes
`host_application.production:create_gateway_application` and `create_worker_application`.
The gateway sets up token authentication and browser request checks. Dashboard credentials are
loaded only by the gateway. The `host_application.gateway` and `worker` factories are local
examples and reject production settings.

For a small group of trusted users, this example accepts randomly generated tokens over HTTPS.
Browsers use HTTP Basic with a credential ID and token; standalone clients use `Authorization:
Bearer <token>`. Generate tokens in your secret-management workflow using at least 32 random bytes.
Use generated tokens rather than passwords chosen by users. The host loads only SHA-256 token
digests and compares them in constant time. Basic requires transport encryption;
its encoding does not protect credentials. See [RFC 7617](https://www.rfc-editor.org/rfc/rfc7617).

Load host settings once at startup:

| Environment variable | Value |
| --- | --- |
| `HOST_APPLICATION_PUBLIC_ORIGIN` | Exact HTTPS origin, such as `https://dashboard.example.com`, without trailing slash |
| `HOST_APPLICATION_CREDENTIALS` | Secret-injected JSON array of `CredentialGrant` objects |
| `JUSTFLOW_RUNTIME__PROFILE` | `production` |
| `JUSTFLOW_RUNTIME__SCOPE` | Explicit host-owned tenant/application/environment JSON |
| `JUSTFLOW_OPERATIONS__ADMIN_PANEL_ENABLED` | `true` when shipping the panel |

Each credential record contains `credential_id`, stable `principal_id`, `token_sha256`, aware UTC
`expires_at`, `scope`, and an explicit `actions` array. Credential IDs and token digests must be
unique. Rotating credentials for the same principal must preserve scope and actions. All grants
must match this host's runtime scope. Store the JSON in Secrets Manager and inject it into gateway
tasks; never commit live tokens, hashes or credential records. The host's typed `HostSettings` and
`CredentialGrant` are the schema; startup rejects invalid records.

For browser login, open the origin root `/` first. The HTTP 401 Basic challenge establishes the
origin-wide protection space; successful authentication redirects to `/admin`. The browser sends
the credential on same-origin asset and API requests. The UI contains no token, token field, local
storage credential or AWS credential. Every unsafe Basic-authenticated request must carry the exact
configured `Origin`; foreign, duplicate and missing origins are denied. No cross-origin access is
enabled. This implements the origin checks described by
[OWASP](https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html).

Terminate HTTPS at the ALB, allow gateway ingress only from its security group, and configure
Uvicorn `FORWARDED_ALLOW_IPS` to the ALB subnet CIDRs. Do not use `*` on a reachable gateway. The
browser boundary rejects non-HTTPS application requests; only aggregate `/livez` and `/readyz`
remain available over the private health-check connection. Authentication headers must be excluded
from proxy/application logs. Use a dedicated dashboard origin with no untrusted applications.

Rotation is a rolling deployment: install overlapping old/new credential records, replace all
gateway tasks, distribute the new token, then remove the old record and replace every task again.
Keep the principal ID stable for idempotent-operation ownership. Revocation becomes effective when
every task has reloaded the new configuration, or at credential expiry, whichever occurs first.
Browser Basic credentials can remain cached until the browser session is closed; the example does
not promise a logout button, immediate distributed revocation, MFA or SSO. Hosts needing those
features should replace this adapter with their identity provider while retaining scope/action
checks and browser request protection.

The example's credential flow is exercised through the real ASGI control API in
`tests/applications/test_host_authentication.py`, including denied actions, foreign scopes, expiry,
rotation and browser origin enforcement. AWS TLS, proxy configuration and actual browser behavior
must also pass the first-deployment acceptance checks.

## Configuration modes and capability discovery

Start every dashboard session with `GET /v1/operations/capabilities`. Read `api_compatibility`,
`scope`, `configuration_mode`, and individual permission booleans; refresh after deployment or
identity changes. A UI control should be enabled only when its capability is present and true.

| Route family / behavior | `local_source` | `managed` | `unavailable` |
| --- | --- | --- | --- |
| `/v1/configuration/draft`, YAML, fragments, schedules, validation | Supported with grants | Supported with grants | Absent |
| Apply the draft to local source files | Supported; development profile only | Absent | Absent |
| Publish immutable revision, plan activation, activate/rollback | Absent | Requires publication/controller bindings | Absent |
| `/v1/operations/*` reads | Per configured source/service | Per configured source/service | Independent of authoring; dimensions may report unavailable |
| `/v1/workflows/*`, `/v1/triggers/*` controls | Per capability | Per capability | Independent of authoring |
| `/v1/scheduled-starts/*` | Per capability | Production requires an authoritative quota binding | Independent of authoring, fail-closed when unbound |

The production credential example supplies authentication and starts the runtime. Your application
still needs to provide a distributed activation controller, worker deployment and readiness checks,
routing updates, and shared quotas for future starts. Managed authoring remains unavailable until
its required bindings are installed; AWS storage settings alone do not enable editing.
The remaining setup is listed in the
[Fargate runbook](../aws/self-hosted-temporal.md).

## Standalone client

Export the [OpenAPI contract](../reference/http-api.md) and implement against its request/response
schemas. No browser module imports are needed. For example, with a credential obtained through
the host's secret distribution process:

<!-- opt-in: cloud procedure -->
```console
curl --fail --header "Authorization: Bearer ${OPERATOR_TOKEN}" \
  "https://${APPLICATION_DOMAIN}/v1/operations/capabilities"
curl --fail --header "Authorization: Bearer ${OPERATOR_TOKEN}" \
  "https://${APPLICATION_DOMAIN}/v1/operations/workflows"
curl --fail --header "Authorization: Bearer ${OPERATOR_TOKEN}" \
  --header 'Content-Type: application/json' --data-binary @start-request.json \
  "https://${APPLICATION_DOMAIN}/v1/workflows"
```

Use opaque identifiers returned by the API. Match error `code`, not message text. For draft YAML,
read the draft's version and request `/v1/configuration/draft/export?expected_version=<version>`;
retry the read on conflict. Save with the matching optimistic-concurrency token and preserve local
edits made while a save is in flight. For durable mutation APIs, retain the original request and
idempotency key so a lost-response retry can recover the original result.
