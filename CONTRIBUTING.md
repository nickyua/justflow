# Contributing

This guide covers local setup, checks to run before merging a PR, and the
conventions used in the codebase.

## Development setup

```bash
git clone https://github.com/nickyua/justflow.git
cd justflow
python3.14 -m venv .venv
.venv/bin/pip install --constraint constraints/ci.txt \
  --editable ".[dev]" --editable packages/justflow-admin \
  --editable examples/prime_stats build
npm --prefix ui/admin ci
npm --prefix ui/admin run build
```

The last step builds the administration panel bundle into
`packages/justflow-admin/src/justflow_admin/assets/dist/`. The bundle is gitignored build
output (shipped in the wheel via hatch `artifacts`), and the backend test
suite and `justflow serve` read it from disk — rebuild it after changing
`ui/admin` sources (`make admin-check` does this too).

## Checks before merging

Run the full check suite before merging a PR:

```bash
make check
```

You can also run the checks separately:

| Command                   | What it runs |
| ------------------------- | ------------ |
| `make admin-check`        | Admin panel types, lint, unit coverage, and production build with Node 24 |
| `make lint`               | Ruff and mypy |
| `make format-check`       | Pinned Ruff formatter check |
| `make test-unit`          | Unit suite without Temporal |
| `make coverage`           | Unit suite with branch coverage and the minimum coverage requirement |
| `make test-replay`        | Committed Temporal history replay against the compatible worker code |
| `make test-sandbox`       | Checks that workflow code runs in Temporal's sandbox |
| `make test-integration`   | Integration suite against the pinned local Temporal server |
| `make build`              | Main and example sdist/wheel builds |
| `make verify-distribution` | Exact artifact-name, metadata, and contents inspection |
| `make smoke-wheel`        | Isolated installation and import of the built wheel |

CI (`.github/workflows/ci.yml`) runs the same checks on every PR. Unit and
integration tests cover Python 3.11 through 3.14; package checks use
Python 3.14.

Keep existing replay histories when changing workflow behavior: they verify
that old runs still work. Add recordings for new behavior, review the history
index, and run `make test-replay`. Use Temporal versioning when a change would
alter decisions recorded by an existing workflow.

## Project layout

```
src/justflow/          the engine library
ui/admin/              separately built TypeScript administration panel
examples/prime_stats/ runnable application with its own action package and configs
tests/                unit tests (+ tests/integration for the Temporal-backed suite)
```

## Conventions

- Add types and run mypy. Resolve type errors through correct types and
  narrowing rather than suppressions.
- Give meaningful values names: use constants for fixed values and settings
  for values that vary by deployment.
- Raise on programmer errors and corrupt state. Handle expected external
  failures explicitly, and preserve enough information to diagnose them.
- Keep unit tests independent of the network, wall clock, and test order.
  Inject time and seed randomness. Use frozen dataclass cases for test matrices.
- Translate dependency errors into the appropriate Justflow exception;
  transport failures use `TransportError` subclasses.
- Keep each PR focused on one change and explain why it is needed. Open a
  draft PR when you create a branch, then add implementation commits to it.

## Reporting issues

Follow [SUPPORT.md](SUPPORT.md) when reporting a bug. A minimal workflow and
the error code are useful; remove credentials and customer data before
sharing configuration, logs, or audit output. Report vulnerabilities through
[SECURITY.md](SECURITY.md).
