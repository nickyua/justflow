# Downstream security CI

Justflow's release checks cover the library and its published artifacts. Your application's CI
also needs to check its code, configuration, dependencies, providers, and final container image.

Include these steps in your application CI:

1. install from the application's lockfile and audit the installed dependencies;
2. run type checking, unit/integration tests, `justflow validate`, and schema/OpenAPI drift checks;
3. scan the complete repository history and workspace for committed secrets;
4. build the final application image once by immutable base digest;
5. inspect runtime user, provenance, filesystem, and image history;
6. scan the exact final digest for vulnerabilities, secrets, and misconfiguration;
7. generate an SBOM, sign/attest the digest, and deploy that same digest.

The tested
[GitHub Actions example](https://github.com/nickyua/justflow/blob/main/docs/examples/downstream-security.yml)
shows this sequence in GitHub Actions. Replace its paths and policy with your application values,
and keep action revisions and scanner images pinned.

## What to scan

- Python/npm lockfiles after resolution, including optional integration extras.
- `resources.yaml`, `services.yaml`, `triggers.yaml`, workflow files, managed configuration exports,
  component catalogs, IAM/infra files, and provider code.
- Git history, build context, image layers/history, and the running image filesystem.
- Base and final application image digests independently.

Use an exception file only for reviewed findings with owner, reason, and finite expiry. The policy
must reject malformed, expired, duplicate, unused, or unbounded exceptions. Never bypass a gate to
make a release green.

## Runtime checks

Exercise `/livez`, `/readyz`, authenticated `/healthz` and `/metrics`, one idempotent test start, and
graceful role shutdown in an isolated environment. Live cloud mutation belongs in an explicit opt-in
job with caller-owned credentials and disposable resources, never the default pull-request suite.
