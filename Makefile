PY ?= .venv/bin/python
COV_MIN ?= 85
EXPECTED_VERSION ?= 0.1.0
PRIME_STATS_DIR := examples/prime_stats
PRODUCT_ONBOARDING_DIR := examples/product_onboarding
SCHEDULED_REPORTING_DIR := examples/scheduled_reporting
OBJECT_INGESTION_DIR := examples/object_ingestion
HOST_APPLICATION_DIR := examples/host_application
ADMIN_DIR := ui/admin
ADMIN_PACKAGE_DIR := packages/justflow-admin
QUALITY_PATHS := src tests examples scripts

.PHONY: check admin-check admin-build docs-generate docs-generate-check docs-build docs-check \
	security-policy container-static lint format-check test \
	test-unit test-integration test-replay test-sandbox coverage build verify-distribution \
	smoke-wheel release-readiness-check release-readiness release-dry-run demo serve-demo

check: admin-check docs-check security-policy lint format-check coverage test-replay test-sandbox \
	test-integration smoke-wheel release-readiness-check

admin-check:
	npm --prefix $(ADMIN_DIR) run check

admin-build:
	npm --prefix $(ADMIN_DIR) run build

docs-generate:
	$(PY) scripts/generate_docs_reference.py

docs-generate-check:
	$(PY) scripts/generate_docs_reference.py --check

docs-build:
	$(PY) -m mkdocs build --strict

docs-check: docs-generate-check
	$(PY) scripts/verify_docs.py
	$(PY) scripts/verify_aws_docs.py
	$(PY) -m mkdocs build --strict

release-readiness-check:
	$(PY) -m scripts.verify_release_readiness

release-readiness: docs-check security-policy release-readiness-check

release-dry-run: release-readiness container-static smoke-wheel

security-policy:
	$(PY) scripts/validate_vulnerability_exceptions.py

container-static: security-policy
	docker compose config --quiet

lint:
	$(PY) -m mypy
	$(PY) -m ruff check $(QUALITY_PATHS)

format-check:
	$(PY) -m ruff format --check $(QUALITY_PATHS)

test: test-unit test-integration

test-unit:
	$(PY) -m pytest tests --ignore=tests/integration -q

test-integration:
	$(PY) -m pytest tests/integration -q

test-replay:
	$(PY) -m pytest tests/definitions/test_replay_compatibility.py -q

test-sandbox:
	$(PY) -m pytest tests/integration/test_workflow_sandbox.py -q

coverage:
	$(PY) -m pytest tests --ignore=tests/integration -q \
		--cov=src/justflow --cov-branch --cov-fail-under=$(COV_MIN) \
		--cov-report=term-missing:skip-covered

build: admin-build
	$(PY) scripts/prepare_build_directories.py
	$(PY) -m build --outdir dist
	$(PY) -m build $(ADMIN_PACKAGE_DIR) --outdir dist
	$(PY) -m build $(PRIME_STATS_DIR) --outdir example-dist
	$(PY) -m build $(PRODUCT_ONBOARDING_DIR) --outdir example-dist
	$(PY) -m build $(SCHEDULED_REPORTING_DIR) --outdir example-dist
	$(PY) -m build $(OBJECT_INGESTION_DIR) --outdir example-dist
	$(PY) -m build $(HOST_APPLICATION_DIR) --outdir example-dist

verify-distribution: build
	$(PY) scripts/verify_distribution.py dist $(EXPECTED_VERSION)

smoke-wheel: verify-distribution
	$(PY) scripts/smoke_test_wheel.py dist

demo:
	PYTHONPATH=$(PRIME_STATS_DIR)/src $(PY) -m justflow run prime_stats \
		--config-dir $(PRIME_STATS_DIR)/configs --param n=20 --param seed=42

# Serve the runtime with the admin panel against a local Temporal dev server
# (expects one on 127.0.0.1:7233; see examples/prime_stats/README.md).
serve-demo:
	PYTHONPATH=$(PRIME_STATS_DIR)/src \
	JUSTFLOW_RUNTIME__PROFILE=local \
	JUSTFLOW_DEPLOYMENT__BUILD_ID=local-dev \
	JUSTFLOW_DEPLOYMENT__ARTIFACT_DIGEST=local-development \
	JUSTFLOW_DEPLOYMENT__PACKAGE_VERSION=0.1.0-dev \
	JUSTFLOW_TEMPORAL__CONNECTION__MODE=local_plaintext \
	JUSTFLOW_OPERATIONS__ADMIN_PANEL_ENABLED=true \
	JUSTFLOW_OPERATIONS__LOCAL_SOURCE_AUTHORING_ENABLED=true \
	$(PY) -m justflow serve --config-dir $(PRIME_STATS_DIR)/configs \
		--temporal-address 127.0.0.1:7233 --host 127.0.0.1 --port 8321
