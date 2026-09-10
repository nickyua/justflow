# Security and data protection

## Authentication and authorization

Production administrative and control routes require a host-supplied authentication provider.
The server checks permissions for each action and resource after authentication has established
the caller's scope. Capabilities help clients show the right controls, but every change still
requires an authorization check. Use the local header adapter and plaintext mode only for development.

The [authentication guide](authentication.md) covers the Python interfaces, permissions, browser
login, and example credential provider.

Keep gateways behind authenticated ingress, workers and Temporal endpoints in private networking,
and provider credentials in workload identity or a secret manager. The optional admin console uses
same-origin API calls and must never receive infrastructure credentials.

## TLS and Temporal payloads

Use TLS with explicit server identity for Temporal, HTTPS, gRPC, and external stores. mTLS/API keys
authenticate a workload where required. TLS protects bytes in transit; it does not encrypt payloads
persisted in Temporal history.

Sensitive Temporal payloads require the host payload-codec binding and key lifecycle. Codec mode
records only key identifiers in non-secret settings. Keep old decryption keys while histories,
replays, resets, or scheduled invocations may need them. Decryption fails if a required key is missing.

## Data boundaries

| Boundary | Policy |
| --- | --- |
| Definitions/configuration | No secrets, credentials, tenant populations, or payload samples |
| Temporal history | Business payloads; codec required for sensitive use |
| Logs/metrics | Opaque IDs, stable codes, bounded outcomes; no bodies, tokens, PII, or cache keys |
| Audit archive | Metadata-only default; explicit redaction or approved encrypted capture |
| Cache | Strict JSON output; encrypted access-controlled backend and finite TTL for sensitive use |
| Queue/broker | At-least-once payloads; encryption, least privilege, bounded retention, DLQ policy |
| Memo/search | Digests and closed identifiers only; never raw tenant labels or business data |
| Renderer/admin | Definition and scoped projection only; no runtime payloads or cloud credentials |

## Scheduled-start admission authority

Production deployments must provide a `ScheduledStartQuotaController` whose shared backing state
and coordination are authoritative across gateway processes. If that boundary is absent,
scheduled-start creation fails closed before creating a Temporal schedule. The controller installed
automatically by the local runtime profile uses a process-local lock and eventually consistent
Temporal Visibility; it is a development guardrail, not an exact production quota.

## Retention and deletion

Every archive declaration names a finite provider-enforced retention policy. S3 archives tag objects
for matching lifecycle rules; in-memory archives expire in process. Configure and test Temporal
history retention, Visibility retention, broker/DLQ retention, cache TTL, configuration/catalog
version retention, scheduled-start projections, and backups as one data lifecycle.

Deletion must not break open runs, replay support, audit/legal holds, active aliases, rollback, or
pending scheduled starts. Definition retirement is an operator-approved dependency check, not a
filesystem cleanup job.

## Catalog and configuration integrity

Use immutable object creation, digest verification, versioned buckets, compare-and-swap pointers,
DynamoDB conditional writes/PITR, and least-privilege roles. Back up and restore bodies before
pointers. Corrupt or missing immutable state fails closed.

## Browser and Temporal UI boundaries

Temporal Web UI is for infrastructure operators and can expose raw workflow history. It does not
enforce Justflow's tenant permissions. Tenant operators use the Justflow operations API
and optional admin console. Put both UIs behind their appropriate identity and network controls.

## Reporting vulnerabilities

Follow the private reporting process in the repository [security policy](../support.md#security).
Do not include real credentials, tenant data, or exploitable production endpoints in a public issue.
