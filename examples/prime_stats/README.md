# Prime statistics example

This example generates seeded random numbers, checks them for primality with
bounded parallel fan-out, and summarizes the results.

The application is self-contained:

```text
prime_stats/
├── configs/
│   ├── resources.yaml
│   ├── services.yaml
│   ├── triggers.yaml
│   └── workflows/
│       └── prime_stats.yaml
├── src/
│   └── prime_stats/
│       └── actions/
├── pyproject.toml
└── README.md
```

From the repository root:

```bash
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]" -e examples/prime_stats
.venv/bin/python -m justflow validate \
  --config-dir examples/prime_stats/configs
.venv/bin/python -m justflow run prime_stats \
  --config-dir examples/prime_stats/configs \
  --param n=20 \
  --param seed=42
.venv/bin/python -m justflow graph prime_stats \
  --config-dir examples/prime_stats/configs \
  --mermaid-only
.venv/bin/python -m justflow graph prime_stats \
  --config-dir examples/prime_stats/configs \
  --output /tmp/prime-stats.html
```

The `pyproject.toml` installs only this example's action package. It is a
separate application dependency and is not included in the engine
distribution.

The Mermaid command prints diagram source you can keep alongside your workflow. The HTML command
writes an interactive graph that can be opened locally. Schedule declaration
and operations are demonstrated separately by `examples/scheduled_reporting`.

## Run the full system with the admin panel

This starts a Temporal dev server, the Justflow runtime, and the operations
panel against this example. The panel is a separate distribution; install it
from the repository checkout before enabling it:

```bash
.venv/bin/pip install -e packages/justflow-admin
```

From the repository root:

1. Start a local Temporal dev server (via the [Temporal CLI](https://docs.temporal.io/cli),
   or an async process that keeps
   `justflow.engine.local_temporal.start_local_environment()` open):

   ```bash
   temporal server start-dev --port 7233 --ui-port 8233
   ```

2. Publish the example definitions (once, and again after changing workflow
   declarations — the runtime refuses stale catalogs at startup):

   ```bash
   JUSTFLOW_RUNTIME__PROFILE=local PYTHONPATH=examples/prime_stats/src .venv/bin/python -m justflow definitions publish \
     --config-dir examples/prime_stats/configs
   ```

3. Serve the runtime with the panel and local authoring enabled — or run
   `make serve-demo`, which is exactly this command:

   ```bash
   PYTHONPATH=examples/prime_stats/src \
   JUSTFLOW_RUNTIME__PROFILE=local \
   JUSTFLOW_DEPLOYMENT__BUILD_ID=local-dev \
   JUSTFLOW_DEPLOYMENT__ARTIFACT_DIGEST=local-development \
   JUSTFLOW_DEPLOYMENT__PACKAGE_VERSION=0.1.0-dev \
   JUSTFLOW_TEMPORAL__CONNECTION__MODE=local_plaintext \
   JUSTFLOW_OPERATIONS__ADMIN_PANEL_ENABLED=true \
   JUSTFLOW_OPERATIONS__LOCAL_SOURCE_AUTHORING_ENABLED=true \
   .venv/bin/python -m justflow serve --config-dir examples/prime_stats/configs \
     --temporal-address 127.0.0.1:7233 --host 127.0.0.1 --port 8321
   ```

   Why each setting is required:

   - `JUSTFLOW_RUNTIME__PROFILE=local` — local-source authoring is only
     permitted under the local profile.
   - `JUSTFLOW_TEMPORAL__CONNECTION__MODE=local_plaintext` — the default
     connection expects TLS; the dev server is plaintext.
   - `JUSTFLOW_DEPLOYMENT__BUILD_ID` supplies the required worker build
     identity, and `local-development` is the recognized local artifact digest.
     This runbook pins `PACKAGE_VERSION`; installed package metadata can supply
     it when available.
   - The two `JUSTFLOW_OPERATIONS__*` flags opt in to the panel and local-source
     editing APIs; both default to off (the panel is an optional client of the
     same authenticated APIs).

4. Open the panel at <http://127.0.0.1:8321/admin> (Temporal UI:
   <http://localhost:8233>; health: `/livez`, `/readyz`). Start a demo run:

   ```bash
   curl -s -X POST http://127.0.0.1:8321/v1/workflows \
     -H 'content-type: application/json' \
     -d '{"workflow_name": "prime_stats", "business_request_id": "demo-1", "input": {"n": 20, "seed": 42}}'
   ```

Local authoring edits these config files directly (workflow saves land in
`configs/workflows/`, trigger saves in `triggers.yaml`) and publishes
definitions automatically; saved changes take effect after a restart.

For separate worker and gateway containers, see `examples/host_application`.
This example focuses on installing Justflow, running a workflow, and inspecting
its result and diagram.
