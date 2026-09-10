# First deployment: self-hosted Temporal on ECS/Fargate

This plan covers the first AWS deployment of the 0.1.0 beta: private Temporal services on
ECS/Fargate, with RDS PostgreSQL for persistence and SQL visibility. The example application uses
daily schedules, SQS, HTTP, S3, and PostgreSQL, with optional gRPC.
**The deployment has not yet been tested on AWS. Complete the acceptance checks below before
running production workloads.**

## Version and environment record

Start with these candidate versions and digests, resolved on 2026-09-09. They still need to be
tested together in your environment. Mirror the verified images into ECR and record their digests.

| Component | Candidate / required record |
| --- | --- |
| Justflow | `0.1.0`; exact application image digest and worker build ID |
| Temporal server | `temporalio/server:1.31.2@sha256:b5ecdb8282bededae2a10c36e8d862e27d0bc2d247fc73c5416025997ab4a1da` |
| Schema tools | `temporalio/admin-tools:1.31.2@sha256:dbc5fcd6ee8f0f4d808bf765af9a87dea9d8a283abfdcfbd2fc148496ba66107` |
| Temporal SQL schemas | Candidate PostgreSQL core `1.19`, visibility `1.14`; verify bundled migrations before applying |
| PostgreSQL | Initial major `16`; pin the account-supported RDS minor and parameter group before provisioning |
| Python SDK / platform | Record installed `temporalio` version, Python version and image CPU architecture |
| Environment | Account, region, VPC/subnets, namespace, trusted scope, DNS/certificate identities and operator |

The [1.31 release notes](https://github.com/temporalio/temporal/releases/tag/v1.31.0) identify the SQL
schema versions, stable Worker Deployment APIs and priority/fairness behavior. The selected
[1.31.2 patch](https://github.com/temporalio/temporal/releases/tag/v1.31.2) includes an authorization
security fix. Recheck advisories before deployment. Do not select an older vulnerable build simply
to match a local development image.

## Private compute and discovery

Plan separate ECS services for frontend, internal frontend, history, matching and Temporal's
internal worker. These are distinct from Justflow workers. Start with fixed replica counts across
two availability zones; two replicas per role are the planning baseline, subject to an explicit
capacity/availability review before provisioning. Select CPU/memory and database connection budgets
from a bounded representative test. Do not autoscale from a metric that has no publisher.

Use `awsvpc`, no public IPs, private task ENIs and a stable private frontend DNS name. An internal
NLB may pass TCP 7233 through to frontend TLS; do not accidentally terminate client identity at a
different layer. Register IP targets and test health checking, long polling, deregistration and
task replacement. Keep internal frontend access limited to Temporal servers; it is not an
alternative endpoint for application clients.

Set `BIND_ON_IP` to the task's private address and `TEMPORAL_BROADCAST_ADDRESS` to that same routable
address, obtained at startup from validated Fargate metadata. Reject absent/ambiguous addresses;
do not advertise loopback, a shared load-balancer address or an old task IP. Metadata v4 supplies
the task's network information. See the
[AWS response contract](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-metadata-endpoint-v4-fargate-response.html).
Membership discovery uses Temporal's persistence-backed membership; DNS/NLB discovery is the
frontend client boundary. Test a replacement task joining after its predecessor disappears.

The pinned [server configuration](https://github.com/temporalio/temporal/blob/v1.31.2/config/docker.yaml)
uses RPC ports 7233 (frontend), 7234 (history), 7235 (matching), 7236 (internal frontend) and 7239
(internal worker), with corresponding membership ports 6933–6936 and 6939. Restrict RPC and
membership to the intended Temporal security groups. Membership uses TCP; see the
[pinned membership implementation](https://github.com/temporalio/temporal/blob/v1.31.2/common/membership/ringpop/factory.go).
Do not publish HTTP/debug/metrics listeners through the public ALB.

The consuming deployment must supply the Temporal ECS task definitions, metadata bootstrap and
secret-volume installation before its first apply. The application fixtures in this repository
do not create those services. Review those artifacts together with the security-group rules.

## Durable SQL and explicit schema jobs

Use separate logical databases and credentials for Temporal persistence, Temporal visibility and
application PostgreSQL. Temporal servers need both of their stores; Justflow workers need only
application SQL access. RDS must require TLS with hostname/CA verification, encryption, backups,
PITR and deletion protection. Keep the database private and configure Multi-AZ recovery.
PostgreSQL supports Temporal's current SQL visibility model; follow the
[PostgreSQL setup guide](https://docs.temporal.io/self-hosted-guide/visibility/postgresql).

Before the first server starts, run the pinned `temporal-sql-tool` as a one-shot ECS administration
task. Create the two databases under the separately authorized database administrator, run
`setup-schema -v 0.0` only for new empty stores, then apply the bundled
`schema/postgresql/v12/temporal/versioned` and `schema/postgresql/v12/visibility/versioned` migrations
with `update-schema`. Record tool version, schema versions and exit codes. Read the selected tool's
help for connection/TLS flags; inject credentials as secrets, never command-line literals. Existing
databases use only the supported ordered upgrade path, not repeated bootstrap commands.

Grant runtime users the DML rights required by their schemas, without schema-owner credentials.
Sum pool limits across every server role, replica and rolling-update surge, plus application pools,
migration/admin reserve and RDS overhead. Set `numHistoryShards` explicitly before bootstrap and
record the decision: changing a live cluster's shard count requires a migration/rebuild strategy.
See Temporal's [production checklist](https://docs.temporal.io/self-hosted-guide/production-checklist).

## TLS and authorization

Use verified frontend TLS and mTLS between Temporal services, including the internal frontend.
Install CA/certificate/key material in a private task volume before the server starts, owned by the
runtime user with restrictive permissions. Secret-writing bootstrap containers must finish
successfully before the server container starts. Only CA material may be built into an application
image; private keys and tokens must be injected. Mount the Justflow trust file at the rendered
`TEMPORAL_ROOT_CA_PATH` and verify its identity before starting gateway/worker tasks.

For this baseline, configure Temporal's `authorizer: default` and `claimMapper: default`, using
the dedicated trusted JWT key source and `permissions` claim. The default JWT mapper verifies the
signature and maps namespace/action grants; the default authorizer denies missing grants. Its
built-in mapper must not be assumed to enforce an arbitrary application's issuer/audience policy.
Use a dedicated Temporal token issuer/key set, required expiry and namespace grants; if your issuer
needs additional claim checks, implement and test that claim mapper before deployment. See the
[pinned JWT mapper](https://github.com/temporalio/temporal/blob/v1.31.2/common/authorization/default_jwt_claim_mapper.go)
and [authorizer](https://github.com/temporalio/temporal/blob/v1.31.2/common/authorization/default_authorizer.go).

Issue separate credentials to gateway, Justflow worker, controller and operator. Start with only
the selected namespace's required read/write permissions for application callers; controller and
schema/namespace administration have separate authority. Validate every RPC actually used by
worker registration and routing, and refine the role policy from those results. No application
credential receives unrestricted system administration. Verify missing, expired, foreign-namespace
and insufficient-action credentials are denied. A successful public health check does not prove
authentication: the default authorizer intentionally permits basic health RPCs.

With a protected external frontend, enable the internal frontend for Temporal's own workers and
configure its internode mTLS trust. Do not resolve internal authentication failures by switching
to a no-op claim mapper. Temporal authentication is independent of the Justflow
[dashboard credential adapter](../operations/authentication.md).

Refresh JWTs before expiry and roll every process holding the startup credential, with overlap
longer than the controlled rollout. If short token lifetimes require refresh without task
replacement, supply a refreshing Temporal client binding and qualify it before launch. Record
certificate renewal, key overlap, revoked-key cache lifetime and an emergency revocation procedure.

## Namespace and feature settings

Create the project namespace under the operator role with explicit history retention. Register
these keyword search attributes before indexed operations are enabled:
`JustflowScopeDigest`, `JustflowLogicalWorkflow`, `JustflowDefinitionDigest`,
`JustflowTriggerSource`, and `JustflowWorkerArtifact`. Verify keyword capacity and actual list
queries on SQL visibility. Keep customer payloads and credentials out of search attributes.

Version the dynamic configuration file. Preserve `matching.newUseMatcher: true` for priority
handling. Enable `matching.enableFairness` only for the selected namespace/task queues when its
behavior is being qualified. Enable `system.enableRingpopTLS: true` on all cluster members from
initial startup when using encrypted membership; mixing incompatible membership settings is not a
rotation procedure. Do not set `system.disableStreamingAuthorizer: true`. These keys are defined
in the [pinned configuration source](https://github.com/temporalio/temporal/blob/v1.31.2/common/dynamicconfig/constants.go).

Test actual Worker Deployment registration/current-version routing, schedules, query/signal/cancel,
priority under a bounded backlog and fairness if enabled. SDK acceptance of a field is not evidence
that a server honors its semantics. Retain the exact dynamic configuration with the evidence record.

## Application rendering and deployment checkpoints

The application renderer accepts reviewed **non-secret** placeholder values as a JSON object of
strings. It derives `SCOPE_DIGEST` from the three scope IDs, requires a full image digest, merges
common/role settings into each ECS task, and validates library settings without inheriting your
shell's runtime configuration. It creates a new output directory and refuses to overwrite one.
Secret values remain in Secrets Manager; only secret ARNs are rendering inputs.

<!-- tested: tests/test_aws_rendering.py -->
```console
.venv/bin/python scripts/render_aws_reference.py --values deployment-values.json --output rendered
```

Missing values are listed before any output is written. The default excludes Redis startup secrets
and application DynamoDB write grants. `--include-redis` and `--include-application-dynamodb` opt in
to those boundaries. The renderer outputs the application task definitions, role policies and
runtime contract. It does not provision infrastructure, install TLS files, create the first active
configuration, or replace the consuming project's IaC review.

Before applying the environment, the deployment owner must finish and test these host bindings:

1. Bootstrap a reviewed immutable configuration/definition revision and its initial active pointer.
2. Compose managed publication, query and activation services if editable managed APIs are enabled.
3. Supply the activation execution context, API request/result path, durable lease/checkpoints,
   ECS worker rollout, compatible readiness observations, schedule reconciliation and retirement.
4. Refresh prepared routing on every gateway replica after activation. A controller changing its
   own in-memory index is insufficient. Prove lost-response retry and rollback against retained state.
5. For exact-time/appointment-offset production starts, supply authoritative shared admission with
   stable reservation identity, ambiguous-create recovery and idempotent release/reconciliation.
   Leave those capabilities fail-closed until this is implemented. Daily 7 am recurring schedules
   use ordinary Temporal Schedules and do not require this future-start quota binding.

The credential example does not include this AWS controller. Implement the application-specific
bindings before deployment. If a required library interface is missing, release a new library
version rather than patching an installed wheel.

## Acceptance and evidence

Copy [the evidence template](reference/first-deployment-evidence.json) into the deployment record.
Use `passed`, `failed` or `not_run` for every check, with a timestamp and a link to redacted evidence.
Retain image digests, schema/configuration hashes, task revisions and the previous working revision.
Required checks are:

| Check | Evidence required before normal project workloads |
| --- | --- |
| Identity | Actual browser root login and API requests; invalid/action/foreign-scope denials; Temporal namespace denials |
| Configuration | First publication, next revision, stale edit, activation, lost-response retry, gateway-replica refresh and rollback |
| Daily batch | Explicit IANA timezone and DST policy; paused schedule inspected before enabling; one scoped bounded batch with independent customer child workflows |
| SQS | Different business/correlation/invocation IDs; duplicate during in-flight signal; failed first delivery retried without premature acknowledgement |
| HTTP/S3/PostgreSQL | Valid requests, bounded oversized responses, cancellation and connection/body cleanup; real TLS and IAM denials |
| gRPC if enabled | Actual service/method/JSON protocol agreement, TLS identity, concurrent calls and canceled connection setup |
| Responsiveness | Slow storage does not block unrelated readiness/control requests |
| Temporal durability | Replace a Justflow worker and a Temporal task during work; resume from durable state, continue-as-new and replay |
| Recovery | Restore databases/catalogs to an isolated target, measure recovery, recover retained artifacts/codec keys |
| Future starts if enabled | Two-client quota race, duplicate admission, unknown create outcome, restart/reconciliation, reschedule/cancel and timely release |

Start only with synthetic opaque references. Apply real infrastructure through reviewed account-owned
tooling, then use the application deployment commands in [AWS deployment](deployment.md). Keep
failed checks open until they pass in AWS; process health and local tests cover only part of the deployment.

## Rollback and ownership

Pause affected triggers, preserve failure history and fix the smallest failing layer. Roll back
the application using its previous verified task/image revision and compatible configuration.
Retain workers required by open histories. A Temporal image downgrade does not undo SQL migrations;
follow the selected release's upgrade policy or restore into a separate compatible target. Test
restoration before promising recovery objectives.

After 0.1.0 publication, library fixes use 0.1.1 or the next normal version. Environment/host fixes
get a new immutable application/deployment revision. Never overwrite a released wheel or image.
Record who owns backups, on-call response, token renewal and deferred checks before admitting
ordinary workloads.
