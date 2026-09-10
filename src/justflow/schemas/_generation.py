"""Schema generation and offline export."""

from __future__ import annotations

import json
from copy import deepcopy
from importlib.resources import files
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from justflow.config.models import ResourceConfig, ServiceConfig, WorkflowConfig
from justflow.config.triggers import TriggersConfig
from justflow.configuration.models import (
    ConfigurationEnvelope,
    PlatformComponentCatalog,
    TenantConfiguration,
)
from justflow.configuration.policy import TenantAuthoringPolicy
from justflow.resources.builtins import builtin_resource_registry
from justflow.resources.registry import ResourceRegistry
from justflow.scope import RuntimeScope
from justflow.transports.builtins import builtin_transport_registry
from justflow.transports.registry import TransportRegistry

AUTHORING_SCHEMA_VERSION = "0.1.0"
SCHEMA_DIALECT = "https://json-schema.org/draft/2020-12/schema"
SCHEMA_BASE_URI = f"https://justflow.dev/schemas/{AUTHORING_SCHEMA_VERSION}"
RESOURCE_SCHEMA_FILE = "resources.schema.json"
SERVICE_SCHEMA_FILE = "services.schema.json"
TRIGGER_SCHEMA_FILE = "triggers.schema.json"
WORKFLOW_SCHEMA_FILE = "workflow.schema.json"
CONFIGURATION_SCHEMA_FILE = "configuration.schema.json"
CONFIGURATION_ENVELOPE_SCHEMA_FILE = "configuration-envelope.schema.json"
RUNTIME_SCOPE_SCHEMA_FILE = "runtime-scope.schema.json"
TENANT_POLICY_SCHEMA_FILE = "tenant-authoring-policy.schema.json"
PLATFORM_COMPONENT_CATALOG_SCHEMA_FILE = "platform-component-catalog.schema.json"
TENANT_CONFIGURATION_SCHEMA_FILE = "tenant-configuration.schema.json"
SCHEMA_FILE_NAMES = (
    CONFIGURATION_ENVELOPE_SCHEMA_FILE,
    CONFIGURATION_SCHEMA_FILE,
    PLATFORM_COMPONENT_CATALOG_SCHEMA_FILE,
    RESOURCE_SCHEMA_FILE,
    RUNTIME_SCOPE_SCHEMA_FILE,
    SERVICE_SCHEMA_FILE,
    TENANT_POLICY_SCHEMA_FILE,
    TENANT_CONFIGURATION_SCHEMA_FILE,
    TRIGGER_SCHEMA_FILE,
    WORKFLOW_SCHEMA_FILE,
)
BUNDLED_SCHEMA_PACKAGE = "justflow.schemas.bundled"


class SchemaExportConflictError(Exception):
    """A schema export would overwrite different repository content."""


def build_authoring_schemas(
    transport_registry: TransportRegistry | None = None,
    resource_registry: ResourceRegistry | None = None,
) -> dict[str, dict[str, Any]]:
    resolved_transport_registry = (
        builtin_transport_registry() if transport_registry is None else transport_registry
    )
    resolved_resource_registry = (
        builtin_resource_registry() if resource_registry is None else resource_registry
    )
    resources = _resources_schema(resolved_resource_registry)
    services = _services_schema(resolved_transport_registry)
    workflow = _model_schema(
        WorkflowConfig,
        file_name=WORKFLOW_SCHEMA_FILE,
        title="Justflow workflow YAML",
    )
    _close_identifier_mapping(workflow["properties"]["params"])
    _close_identifier_mapping(workflow["properties"]["steps"])
    triggers = _model_schema(
        TriggersConfig,
        file_name=TRIGGER_SCHEMA_FILE,
        title="Justflow triggers.yaml",
    )
    _close_identifier_mapping(triggers["properties"]["triggers"])
    configuration = _configuration_schema(resources, triggers, services, workflow)
    return {
        CONFIGURATION_ENVELOPE_SCHEMA_FILE: _model_schema(
            ConfigurationEnvelope,
            file_name=CONFIGURATION_ENVELOPE_SCHEMA_FILE,
            title="Justflow versioned configuration envelope",
        ),
        CONFIGURATION_SCHEMA_FILE: configuration,
        PLATFORM_COMPONENT_CATALOG_SCHEMA_FILE: _model_schema(
            PlatformComponentCatalog,
            file_name=PLATFORM_COMPONENT_CATALOG_SCHEMA_FILE,
            title="Justflow platform component catalog",
        ),
        RESOURCE_SCHEMA_FILE: resources,
        RUNTIME_SCOPE_SCHEMA_FILE: _model_schema(
            RuntimeScope,
            file_name=RUNTIME_SCOPE_SCHEMA_FILE,
            title="Justflow runtime scope",
        ),
        SERVICE_SCHEMA_FILE: services,
        TENANT_POLICY_SCHEMA_FILE: _model_schema(
            TenantAuthoringPolicy,
            file_name=TENANT_POLICY_SCHEMA_FILE,
            title="Justflow tenant authoring policy",
        ),
        TENANT_CONFIGURATION_SCHEMA_FILE: _model_schema(
            TenantConfiguration,
            file_name=TENANT_CONFIGURATION_SCHEMA_FILE,
            title="Justflow tenant configuration",
        ),
        TRIGGER_SCHEMA_FILE: triggers,
        WORKFLOW_SCHEMA_FILE: workflow,
    }


def load_bundled_schemas() -> dict[str, dict[str, Any]]:
    root = files(BUNDLED_SCHEMA_PACKAGE)
    return {
        file_name: json.loads(root.joinpath(file_name).read_text(encoding="utf-8"))
        for file_name in SCHEMA_FILE_NAMES
    }


def export_authoring_schemas(
    destination: str | Path,
    *,
    transport_registry: TransportRegistry | None = None,
    resource_registry: ResourceRegistry | None = None,
) -> tuple[Path, ...]:
    target_directory = Path(destination)
    schemas = (
        load_bundled_schemas()
        if transport_registry is None and resource_registry is None
        else build_authoring_schemas(transport_registry, resource_registry)
    )
    rendered = {file_name: _schema_bytes(schema) for file_name, schema in schemas.items()}
    conflicts = [
        target_directory / file_name
        for file_name, content in rendered.items()
        if (target_directory / file_name).exists()
        and (target_directory / file_name).read_bytes() != content
    ]
    if conflicts:
        raise SchemaExportConflictError(
            "Schema export would overwrite different files: "
            + ", ".join(str(path) for path in sorted(conflicts))
        )

    target_directory.mkdir(parents=True, exist_ok=True)
    exported: list[Path] = []
    for file_name in SCHEMA_FILE_NAMES:
        target = target_directory / file_name
        if not target.exists():
            try:
                with target.open("xb") as stream:
                    stream.write(rendered[file_name])
            except OSError as exc:
                raise SchemaExportConflictError(f"Cannot export schema '{target}': {exc}") from exc
        exported.append(target)
    return tuple(exported)


def _model_schema(
    model: type[BaseModel],
    *,
    file_name: str,
    title: str,
) -> dict[str, Any]:
    schema = model.model_json_schema(by_alias=True, mode="validation")
    schema["$id"] = _schema_id(file_name)
    schema["$schema"] = SCHEMA_DIALECT
    schema["title"] = title
    return _ordered_schema(schema)


def _resources_schema(registry: ResourceRegistry) -> dict[str, Any]:
    base = ResourceConfig.model_json_schema(by_alias=True, mode="validation")
    definitions = deepcopy(base.pop("$defs", {}))
    variants: list[dict[str, Any]] = []
    for provider_name, provider in sorted(registry.providers.items()):
        definition_name = _resource_provider_definition_name(provider_name)
        provider_schema = provider.config_model.model_json_schema(
            by_alias=True,
            mode="validation",
        )
        definitions[definition_name] = _rebase_local_references(
            provider_schema,
            prefix=f"#/$defs/{definition_name}/$defs/",
        )
        variant = deepcopy(base)
        properties = variant.setdefault("properties", {})
        properties.pop("class", None)
        properties["provider"] = {"const": provider_name, "type": "string"}
        properties["config"] = {"$ref": f"#/$defs/{definition_name}"}
        required = set(variant.get("required", []))
        if provider_schema.get("required"):
            required.add("config")
        variant["required"] = sorted(required)
        variants.append(variant)

    class_variant = deepcopy(base)
    class_properties = class_variant.setdefault("properties", {})
    class_properties.pop("provider", None)
    class_options = class_properties["class"].get("anyOf", [])
    class_properties["class"] = deepcopy(
        next(option for option in class_options if option.get("type") == "string")
    )
    class_variant["required"] = ["class"]
    variants.append(class_variant)

    schema: dict[str, Any] = {
        "$defs": definitions,
        "$id": _schema_id(RESOURCE_SCHEMA_FILE),
        "$schema": SCHEMA_DIALECT,
        "additionalProperties": False,
        "properties": {
            "resources": {
                "additionalProperties": {"oneOf": variants},
                "propertyNames": _identifier_schema(),
                "title": "Resources",
                "type": "object",
            }
        },
        "required": ["resources"],
        "title": "Justflow resources.yaml",
        "type": "object",
    }
    return _ordered_schema(schema)


def _services_schema(registry: TransportRegistry) -> dict[str, Any]:
    base = ServiceConfig.model_json_schema(by_alias=True, mode="validation")
    definitions = deepcopy(base.pop("$defs", {}))
    variants: list[dict[str, Any]] = []
    for provider_name, provider in sorted(registry.providers.items()):
        definition_name = _provider_definition_name(provider_name)
        provider_schema = provider.config_model.model_json_schema(
            by_alias=True,
            mode="validation",
        )
        definitions[definition_name] = _rebase_local_references(
            provider_schema,
            prefix=f"#/$defs/{definition_name}/$defs/",
        )
        variant = deepcopy(base)
        properties = variant.setdefault("properties", {})
        properties["transport"] = {"const": provider_name, "type": "string"}
        properties["transport_config"] = {"$ref": f"#/$defs/{definition_name}"}
        required = set(variant.get("required", []))
        if provider_schema.get("required"):
            required.add("transport_config")
        variant["required"] = sorted(required)
        variants.append(variant)

    schema: dict[str, Any] = {
        "$defs": definitions,
        "$id": _schema_id(SERVICE_SCHEMA_FILE),
        "$schema": SCHEMA_DIALECT,
        "additionalProperties": False,
        "properties": {
            "services": {
                "additionalProperties": {"oneOf": variants},
                "propertyNames": _identifier_schema(),
                "title": "Services",
                "type": "object",
            }
        },
        "required": ["services"],
        "title": "Justflow services.yaml",
        "type": "object",
    }
    return _ordered_schema(schema)


def _configuration_schema(
    resources: dict[str, Any],
    triggers: dict[str, Any],
    services: dict[str, Any],
    workflow: dict[str, Any],
) -> dict[str, Any]:
    schema = {
        "$id": _schema_id(CONFIGURATION_SCHEMA_FILE),
        "$schema": SCHEMA_DIALECT,
        "additionalProperties": False,
        "properties": {
            "resources": deepcopy(resources["properties"]["resources"]),
            "services": deepcopy(services["properties"]["services"]),
            "triggers": deepcopy(triggers["properties"]["triggers"]),
            "workflows": {
                "items": deepcopy(workflow),
                "minItems": 1,
                "type": "array",
            },
        },
        "required": ["resources", "services", "triggers", "workflows"],
        "title": "Justflow configuration envelope",
        "type": "object",
    }
    configuration_definitions = {
        **deepcopy(resources.get("$defs", {})),
        **deepcopy(services.get("$defs", {})),
        **deepcopy(triggers.get("$defs", {})),
    }
    if configuration_definitions:
        schema["$defs"] = configuration_definitions
    return _ordered_schema(schema)


def _rebase_local_references(value: Any, *, prefix: str) -> Any:
    if isinstance(value, dict):
        return {
            key: (
                f"{prefix}{item.removeprefix('#/$defs/')}"
                if key == "$ref" and isinstance(item, str) and item.startswith("#/$defs/")
                else _rebase_local_references(item, prefix=prefix)
            )
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_rebase_local_references(item, prefix=prefix) for item in value]
    return value


def _identifier_schema() -> dict[str, Any]:
    workflow_schema = WorkflowConfig.model_json_schema(mode="validation")
    return deepcopy(workflow_schema["properties"]["workflow"])


def _close_identifier_mapping(schema: dict[str, Any]) -> None:
    schema["additionalProperties"] = False
    schema["propertyNames"] = _identifier_schema()


def _provider_definition_name(provider_name: str) -> str:
    encoded = "".join(character if character.isalnum() else "_" for character in provider_name)
    return f"transport_{encoded}_config"


def _resource_provider_definition_name(provider_name: str) -> str:
    encoded = "".join(character if character.isalnum() else "_" for character in provider_name)
    return f"resource_{encoded}_config"


def _schema_id(file_name: str) -> str:
    return f"{SCHEMA_BASE_URI}/{file_name}"


def _schema_bytes(schema: dict[str, Any]) -> bytes:
    return (
        json.dumps(
            schema,
            allow_nan=False,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _ordered_schema(schema: dict[str, Any]) -> dict[str, Any]:
    return json.loads(_schema_bytes(schema))
