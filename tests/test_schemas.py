"""Generated authoring schema, packaging, and export contracts."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from justflow.config.loader import ConfigLoader
from justflow.resources.base import (
    ResourceCapability,
    ResourceFactoryContext,
    StrictResourceConfig,
)
from justflow.resources.builtins import builtin_resource_registry
from justflow.resources.registry import ResourceProvider
from justflow.schemas import (
    AUTHORING_SCHEMA_VERSION,
    SCHEMA_FILE_NAMES,
    SchemaExportConflictError,
    build_authoring_schemas,
    export_authoring_schemas,
    load_bundled_schemas,
)
from justflow.transports.base import Completed, StrictTransportConfig, TransportRequest
from justflow.transports.builtins import builtin_transport_registry
from justflow.transports.registry import (
    TransportFactoryContext,
    TransportProvider,
)
from tests.conftest import PRIME_STATS_CONFIG_DIR

SCOPE_DIGEST = "a" * 64


class CustomTransportConfig(StrictTransportConfig):
    endpoint: str


class CustomTransport:
    async def send(self, request: TransportRequest) -> Completed:
        return Completed(data={"action": request.action})

    async def close(self) -> None:
        return None


def _custom_transport_factory(
    config: CustomTransportConfig,
    context: TransportFactoryContext,
) -> CustomTransport:
    assert config.endpoint
    assert context.dispatch_timeout_sec > 0
    return CustomTransport()


class CustomResourceConfig(StrictResourceConfig):
    namespace: str


class CustomResource:
    async def initialize(self) -> None:
        return None

    async def close(self) -> None:
        return None

    def get(self, key: str, default: object = None) -> object:
        return default


def _custom_resource_factory(
    config: CustomResourceConfig,
    _context: ResourceFactoryContext,
) -> CustomResource:
    assert config.namespace
    return CustomResource()


def test_bundled_schemas_match_runtime_generation() -> None:
    assert load_bundled_schemas() == build_authoring_schemas()


@pytest.mark.parametrize("file_name", SCHEMA_FILE_NAMES)
def test_bundled_schema_is_valid_and_versioned(file_name: str) -> None:
    schema = load_bundled_schemas()[file_name]

    Draft202012Validator.check_schema(schema)
    assert schema["$id"] == (f"https://justflow.dev/schemas/{AUTHORING_SCHEMA_VERSION}/{file_name}")
    assert "$schema" not in schema["properties"]


def test_maintained_example_validates_against_every_file_schema() -> None:
    schemas = load_bundled_schemas()
    resources = ConfigLoader._load_yaml(PRIME_STATS_CONFIG_DIR / "resources.yaml")
    services = ConfigLoader._load_yaml(PRIME_STATS_CONFIG_DIR / "services.yaml")
    workflow_path = PRIME_STATS_CONFIG_DIR / "workflows" / "prime_stats.yaml"
    workflow = ConfigLoader._load_yaml(workflow_path)

    declarations = {
        "configuration-envelope.schema.json": {
            "format_version": 1,
            "scope_digest": SCOPE_DIGEST,
            "bundle": {
                "resources": resources,
                "services": services,
                "triggers": {"triggers": {}},
                "workflows": {workflow["workflow"]: workflow},
            },
        },
        "configuration.schema.json": {
            "resources": resources["resources"],
            "services": services["services"],
            "triggers": {},
            "workflows": [workflow],
        },
        "platform-component-catalog.schema.json": {
            "revision_id": "f" * 64,
            "steps": {},
            "triggers": {},
        },
        "resources.schema.json": resources,
        "runtime-scope.schema.json": {
            "tenant": "tenant-a",
            "application": "orders",
            "environment": "production",
        },
        "triggers.schema.json": {"triggers": {}},
        "services.schema.json": services,
        "tenant-authoring-policy.schema.json": {
            "scope_digest": SCOPE_DIGEST,
            "component_catalog_revision": "f" * 64,
            "resource_bindings": {},
            "service_bindings": {},
            "secret_aliases": [],
            "temporal": {
                "mode": "shared",
                "namespace_identity": "b" * 64,
                "task_queue_identity": "c" * 64,
                "worker_deployment_identity": "d" * 64,
                "storage_prefix_identity": "e" * 64,
                "runtime_credential_aliases": [],
            },
        },
        "tenant-configuration.schema.json": {
            "component_catalog_revision": "f" * 64,
            "workflows": {
                "example": {
                    "workflow": "example",
                    "steps": {},
                    "flow": [{"name": "done", "terminal": True}],
                }
            },
            "triggers": {},
        },
        "workflow.schema.json": workflow,
    }

    for file_name, declaration in declarations.items():
        errors = sorted(
            Draft202012Validator(schemas[file_name]).iter_errors(declaration),
            key=lambda error: list(error.absolute_path),
        )
        assert not errors, [error.message for error in errors]


def test_host_composed_schema_includes_only_explicit_custom_provider() -> None:
    generic = build_authoring_schemas()["services.schema.json"]
    registry = builtin_transport_registry()
    registry.register(
        TransportProvider(
            name="custom",
            contract_version="1",
            config_model=CustomTransportConfig,
            factory=_custom_transport_factory,
        )
    )
    composed = build_authoring_schemas(registry)["services.schema.json"]
    declaration = {
        "services": {
            "example": {
                "transport": "custom",
                "transport_config": {"endpoint": "local"},
                "dispatch_timeout_sec": 10,
                "retries": 0,
            }
        }
    }

    assert list(Draft202012Validator(generic).iter_errors(declaration))
    assert not list(Draft202012Validator(composed).iter_errors(declaration))


def test_host_composed_schema_includes_explicit_custom_resource_provider() -> None:
    generic = build_authoring_schemas()["resources.schema.json"]
    registry = builtin_resource_registry()
    registry.register(
        ResourceProvider(
            name="custom_resource",
            contract_version="1",
            config_model=CustomResourceConfig,
            capabilities=frozenset({ResourceCapability.CONFIG}),
            factory=_custom_resource_factory,
        )
    )
    composed = build_authoring_schemas(resource_registry=registry)["resources.schema.json"]
    declaration = {
        "resources": {
            "example": {
                "provider": "custom_resource",
                "config": {"namespace": "local"},
            }
        }
    }

    assert list(Draft202012Validator(generic).iter_errors(declaration))
    assert not list(Draft202012Validator(composed).iter_errors(declaration))


def test_resource_schema_accepts_application_class_and_secret_dependency() -> None:
    schema = build_authoring_schemas()["resources.schema.json"]
    declaration = {
        "resources": {
            "application_resource": {
                "class": "application.resources.CustomResource",
                "config": {"mode": "synthetic"},
            },
            "platform_secrets": {
                "provider": "aws_secrets_manager",
                "config": {
                    "secrets": {"database_credentials": {"secret_id": "configured-secret-id"}}
                },
            },
            "database": {
                "provider": "postgresql",
                "config": {
                    "connection": {
                        "endpoint": {
                            "host": "database.local",
                            "database": "application",
                        },
                        "credentials": {
                            "secret_resource": "platform_secrets",
                            "secret_alias": "database_credentials",
                        },
                    }
                },
            },
        }
    }

    assert not list(Draft202012Validator(schema).iter_errors(declaration))


def test_resource_schema_rejects_provider_and_class_together() -> None:
    schema = build_authoring_schemas()["resources.schema.json"]
    declaration = {
        "resources": {
            "ambiguous": {
                "provider": "static",
                "class": "application.resources.CustomResource",
            }
        }
    }

    assert list(Draft202012Validator(schema).iter_errors(declaration))


@pytest.mark.parametrize(
    ("file_name", "declaration"),
    [
        (
            "resources.schema.json",
            {"resources": {"invalid/name": {"class": "package.Resource"}}},
        ),
        (
            "workflow.schema.json",
            {
                "workflow": "example",
                "steps": {},
                "flow": [{"name": "done", "terminal": True}],
                "result": "invalid..reference",
            },
        ),
    ],
    ids=["identifier", "reference"],
)
def test_generated_schemas_enforce_shared_grammar(
    file_name: str,
    declaration: dict[str, object],
) -> None:
    schema = build_authoring_schemas()[file_name]

    assert list(Draft202012Validator(schema).iter_errors(declaration))


def test_schema_export_is_offline_idempotent_and_conflict_safe(tmp_path: Path) -> None:
    destination = tmp_path / "schemas"

    first = export_authoring_schemas(destination)
    second = export_authoring_schemas(destination)

    assert first == second
    assert tuple(path.name for path in first) == SCHEMA_FILE_NAMES
    exported = {path.name: json.loads(path.read_text(encoding="utf-8")) for path in first}
    assert exported == load_bundled_schemas()

    conflict = destination / SCHEMA_FILE_NAMES[0]
    conflict.write_text("{}\n", encoding="utf-8")
    untouched = destination / SCHEMA_FILE_NAMES[1]
    previous = untouched.read_bytes()

    with pytest.raises(SchemaExportConflictError, match="overwrite different files"):
        export_authoring_schemas(destination)

    assert conflict.read_text(encoding="utf-8") == "{}\n"
    assert untouched.read_bytes() == previous
