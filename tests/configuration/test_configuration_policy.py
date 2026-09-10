from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TypeAlias

import pytest
from pydantic import ValidationError

from justflow.config.models import (
    FlowStep,
    ResourceConfig,
    ResourcesConfig,
    ServiceConfig,
    ServiceOperationTarget,
    ServicesConfig,
)
from justflow.config.runtime_limits import DEFAULT_RUNTIME_LIMITS
from justflow.config.triggers import TriggerKind
from justflow.configuration import (
    ComponentReference,
    ComponentReplayContract,
    ComponentRuntimeIdentity,
    ConfigurationError,
    ConfigurationScopeError,
    FilePlatformComponentCatalogSource,
    PlatformComponentCatalog,
    PlatformStepComponent,
    PlatformTriggerComponent,
    ResourceAuthoringGrant,
    RevisionRecord,
    ServiceAuthoringGrant,
    TemporalIsolationMode,
    TemporalIsolationPolicy,
    TenantAuthoringPolicy,
    TenantComponentOperation,
    TenantConfiguration,
    TenantTriggerDeclaration,
    TenantWorkflowConfig,
    TriggerBindingGrant,
    validate_tenant_authoring,
)
from justflow.configuration.models import configuration_revision_identity
from justflow.provenance import provenance_digest
from justflow.resources.base import ResourceCapability
from justflow.resources.builtins import builtin_resource_registry
from justflow.scope import RuntimeScope
from justflow.transports.builtins import builtin_transport_registry

SCOPE = RuntimeScope.create(
    tenant="tenant-a",
    application="orders",
    environment="production",
)
OTHER_SCOPE = RuntimeScope.create(
    tenant="tenant-b",
    application="orders",
    environment="production",
)
STEP_REFERENCE = ComponentReference(name="message.send", version="1.0.0")
TRIGGER_REFERENCE = ComponentReference(name="request.received", version="1.0.0")
UNKNOWN_STEP_REFERENCE = ComponentReference(name="message.delete", version="1.0.0")
OTHER_STEP_REFERENCE = ComponentReference(name="other.send", version="1.0.0")
STEP_RUNTIME = ComponentRuntimeIdentity(
    implementation="http.action",
    version="1.0.0",
    artifact_digest=provenance_digest({"implementation": "http.action@1.0.0"}),
)
TRIGGER_RUNTIME = ComponentRuntimeIdentity(
    implementation="api.trigger",
    version="1.0.0",
    artifact_digest=provenance_digest({"implementation": "api.trigger@1.0.0"}),
)
STEP_REPLAY = ComponentReplayContract(
    deterministic=True,
    retirement_identity=provenance_digest({"retirement": "message.send@1.0.0"}),
)
TRIGGER_REPLAY = ComponentReplayContract(
    deterministic=True,
    retirement_identity=provenance_digest({"retirement": "request.received@1.0.0"}),
)
PLATFORM_RESOURCES = ResourcesConfig(
    resources={"cache": ResourceConfig(provider="memory_cache", config={})}
)
PLATFORM_SERVICES = ServicesConfig(
    services={
        "http_service": ServiceConfig.model_validate(
            {
                "transport": "http",
                "transport_config": {"base_url": "https://service.invalid"},
                "connect_timeout_sec": 5,
                "dispatch_timeout_sec": 10,
                "retries": 0,
            }
        )
    }
)
COMPONENT_CATALOG = PlatformComponentCatalog.create(
    steps={
        STEP_REFERENCE.identity: PlatformStepComponent(
            reference=STEP_REFERENCE,
            runtime_implementation=STEP_RUNTIME,
            replay=STEP_REPLAY,
            transport="http",
            action="POST:/messages",
            parameter_schema={
                "type": "object",
                "properties": {
                    "message": {"type": "string"},
                    "credential_alias": {"type": "string"},
                },
                "required": ["message"],
                "additionalProperties": False,
            },
            input_schema={"type": "object"},
            output_schema={"type": "object"},
            resource_slots={"cache": ResourceCapability.CACHE},
            approved_alias_parameters=frozenset({"credential_alias"}),
        )
    },
    triggers={
        TRIGGER_REFERENCE.identity: PlatformTriggerComponent(
            reference=TRIGGER_REFERENCE,
            runtime_implementation=TRIGGER_RUNTIME,
            replay=TRIGGER_REPLAY,
            kind=TriggerKind.API,
            parameter_schema={
                "type": "object",
                "properties": {"route": {"type": "string"}},
                "required": ["route"],
                "additionalProperties": False,
            },
            output_schema={"type": "object"},
        )
    },
)


def policy(*, scope: RuntimeScope = SCOPE) -> TenantAuthoringPolicy:
    return TenantAuthoringPolicy(
        scope_digest=scope.digest,
        component_catalog_revision=COMPONENT_CATALOG.revision_id,
        step_components=frozenset({STEP_REFERENCE}),
        trigger_components=frozenset({TRIGGER_REFERENCE}),
        resource_bindings={
            "cache": ResourceAuthoringGrant(
                provider="memory_cache",
                capabilities=frozenset({ResourceCapability.CACHE}),
            )
        },
        service_bindings={"http_service": ServiceAuthoringGrant(transport="http")},
        trigger_bindings={"tenant_api": TriggerBindingGrant(kind=TriggerKind.API)},
        secret_aliases=frozenset({"orders-api"}),
        temporal=TemporalIsolationPolicy.for_scope(
            scope,
            mode=TemporalIsolationMode.SHARED,
            namespace="shared",
            task_queue="orders",
            worker_deployment="orders",
            storage_prefix="orders",
            runtime_credential_aliases=frozenset({"orders-api"}),
        ),
    )


def tenant_configuration(
    *,
    component: ComponentReference = STEP_REFERENCE,
    service_binding: str = "http_service",
    resource_bindings: dict[str, str] | None = None,
    parameters: dict[str, object] | None = None,
    trigger_binding: str = "tenant_api",
) -> TenantConfiguration:
    workflow = TenantWorkflowConfig(
        workflow="example",
        steps={
            "send": TenantComponentOperation(
                component=component,
                service_binding=service_binding,
                resource_bindings=(
                    {"cache": "cache"} if resource_bindings is None else resource_bindings
                ),
                parameters=(
                    {"message": "hello", "credential_alias": "orders-api"}
                    if parameters is None
                    else parameters
                ),
            )
        },
        flow=[
            FlowStep.model_validate({"name": "send", "op": "send", "then": "done"}),
            FlowStep.model_validate({"name": "done", "terminal": True}),
        ],
    )
    return TenantConfiguration(
        component_catalog_revision=COMPONENT_CATALOG.revision_id,
        workflows={"example": workflow},
        triggers={
            "api": TenantTriggerDeclaration(
                component=TRIGGER_REFERENCE,
                kind=TriggerKind.API,
                workflow="example",
                binding_alias=trigger_binding,
                parameters={"route": "example"},
            )
        },
    )


def tenant_revision(
    configuration: TenantConfiguration,
    *,
    scope: RuntimeScope = SCOPE,
) -> RevisionRecord:
    revision_id = configuration_revision_identity(scope.digest, configuration, None)
    return RevisionRecord(
        scope_digest=scope.digest,
        revision_id=revision_id,
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        bundle=configuration,
    )


@dataclass(frozen=True, kw_only=True)
class Returns:
    action: str


@dataclass(frozen=True, kw_only=True)
class Raises:
    exc: type[Exception]
    match: str


AuthoringOutcome: TypeAlias = Returns | Raises


@dataclass(frozen=True, kw_only=True)
class AuthoringCase:
    id: str
    configuration: TenantConfiguration
    scope: RuntimeScope
    outcome: AuthoringOutcome


AUTHORING_CASES = [
    AuthoringCase(
        id="approved-exact-component",
        configuration=tenant_configuration(),
        scope=SCOPE,
        outcome=Returns(action="POST:/messages"),
    ),
    AuthoringCase(
        id="cross-scope",
        configuration=tenant_configuration(),
        scope=OTHER_SCOPE,
        outcome=Raises(exc=ConfigurationScopeError, match="runtime scope"),
    ),
    AuthoringCase(
        id="unknown-component",
        configuration=tenant_configuration(component=UNKNOWN_STEP_REFERENCE),
        scope=SCOPE,
        outcome=Raises(exc=ConfigurationError, match="component is not approved"),
    ),
    AuthoringCase(
        id="unapproved-service-binding",
        configuration=tenant_configuration(service_binding="private_http"),
        scope=SCOPE,
        outcome=Raises(exc=ConfigurationError, match="service binding is not approved"),
    ),
    AuthoringCase(
        id="missing-resource-slot",
        configuration=tenant_configuration(resource_bindings={}),
        scope=SCOPE,
        outcome=Raises(exc=ConfigurationError, match="resource slots"),
    ),
    AuthoringCase(
        id="invalid-component-parameters",
        configuration=tenant_configuration(parameters={"message": 7}),
        scope=SCOPE,
        outcome=Raises(exc=ConfigurationError, match="parameters do not satisfy"),
    ),
    AuthoringCase(
        id="unapproved-credential-alias",
        configuration=tenant_configuration(
            parameters={"message": "hello", "credential_alias": "unknown"}
        ),
        scope=SCOPE,
        outcome=Raises(exc=ConfigurationError, match="credential alias is not approved"),
    ),
    AuthoringCase(
        id="unapproved-trigger-binding",
        configuration=tenant_configuration(trigger_binding="private_api"),
        scope=SCOPE,
        outcome=Raises(exc=ConfigurationError, match="Trigger binding is not approved"),
    ),
]


@pytest.mark.parametrize("case", AUTHORING_CASES, ids=lambda case: case.id)
def test_tenant_authoring_contract(case: AuthoringCase, tmp_path) -> None:
    if isinstance(case.outcome, Raises):
        with pytest.raises(case.outcome.exc, match=case.outcome.match):
            validate_tenant_authoring(
                tenant_revision(case.configuration, scope=case.scope),
                scope=case.scope,
                policy=policy(),
                component_catalog=COMPONENT_CATALOG,
                platform_resources=PLATFORM_RESOURCES,
                platform_services=PLATFORM_SERVICES,
                transport_registry=builtin_transport_registry(),
                resource_registry=builtin_resource_registry(),
                limits=DEFAULT_RUNTIME_LIMITS,
                config_dir=tmp_path,
            )
        return

    validated = validate_tenant_authoring(
        tenant_revision(case.configuration, scope=case.scope),
        scope=case.scope,
        policy=policy(),
        component_catalog=COMPONENT_CATALOG,
        platform_resources=PLATFORM_RESOURCES,
        platform_services=PLATFORM_SERVICES,
        transport_registry=builtin_transport_registry(),
        resource_registry=builtin_resource_registry(),
        limits=DEFAULT_RUNTIME_LIMITS,
        config_dir=tmp_path,
    )

    target = validated.bundle.workflows["example"].steps["send"].target
    assert isinstance(target, ServiceOperationTarget)
    assert target.action == case.outcome.action


def test_resolution_pins_catalog_components_and_narrows_capabilities(tmp_path) -> None:
    source_revision = tenant_revision(tenant_configuration())
    validated = validate_tenant_authoring(
        source_revision,
        scope=SCOPE,
        policy=policy(),
        component_catalog=COMPONENT_CATALOG,
        platform_resources=PLATFORM_RESOURCES,
        platform_services=PLATFORM_SERVICES,
        transport_registry=builtin_transport_registry(),
        resource_registry=builtin_resource_registry(),
        limits=DEFAULT_RUNTIME_LIMITS,
        config_dir=tmp_path,
    )

    assert validated.component_catalog_revision == COMPONENT_CATALOG.revision_id
    assert validated.component_references["example"] == {"send": STEP_REFERENCE}
    assert validated.triggers["api"].component == TRIGGER_REFERENCE
    assert validated.resources["cache"].capabilities == frozenset({ResourceCapability.CACHE})
    assert validated.bundle.tenant_resolution is not None
    assert (
        validated.bundle.tenant_resolution.tenant_configuration_revision_id
        == source_revision.revision_id
    )
    assert validated.bundle.tenant_resolution.component_identities == (
        STEP_REFERENCE.identity,
        TRIGGER_REFERENCE.identity,
    )


def test_resolution_loads_exact_platform_catalog_revision(tmp_path) -> None:
    catalog_directory = tmp_path / "components"
    catalog_directory.mkdir()
    (catalog_directory / f"{COMPONENT_CATALOG.revision_id}.json").write_bytes(
        COMPONENT_CATALOG.canonical_bytes()
    )

    validated = validate_tenant_authoring(
        tenant_revision(tenant_configuration()),
        scope=SCOPE,
        policy=policy(),
        component_catalog_source=FilePlatformComponentCatalogSource(catalog_directory),
        platform_resources=PLATFORM_RESOURCES,
        platform_services=PLATFORM_SERVICES,
        transport_registry=builtin_transport_registry(),
        resource_registry=builtin_resource_registry(),
        limits=DEFAULT_RUNTIME_LIMITS,
        config_dir=tmp_path,
    )

    assert validated.component_catalog_revision == COMPONENT_CATALOG.revision_id


@pytest.mark.parametrize("field", ["resources", "services"])
def test_tenant_document_rejects_platform_owned_declarations(field: str) -> None:
    value = tenant_configuration().model_dump(mode="python")
    value[field] = {}

    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        TenantConfiguration.model_validate(value)


@pytest.mark.parametrize("field", ["action", "transport", "class"])
def test_tenant_component_selection_rejects_executable_declarations(field: str) -> None:
    value = tenant_configuration().workflows["example"].steps["send"].model_dump(mode="python")
    value[field] = "tenant.implementation"

    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        TenantComponentOperation.model_validate(value)


def test_component_catalog_revision_pins_runtime_and_replay_contracts() -> None:
    changed_runtime = STEP_RUNTIME.model_copy(
        update={
            "artifact_digest": provenance_digest({"implementation": "http.action@1.0.0-rebuilt"})
        }
    )
    changed_step = COMPONENT_CATALOG.steps[STEP_REFERENCE.identity].model_copy(
        update={"runtime_implementation": changed_runtime}
    )

    changed_catalog = PlatformComponentCatalog.create(
        steps={STEP_REFERENCE.identity: changed_step},
        triggers=dict(COMPONENT_CATALOG.triggers),
    )

    assert changed_catalog.revision_id != COMPONENT_CATALOG.revision_id


def test_component_replay_compatibility_cannot_cross_component_names() -> None:
    replay = ComponentReplayContract(
        deterministic=True,
        compatible_versions=(OTHER_STEP_REFERENCE,),
        retirement_identity=STEP_REPLAY.retirement_identity,
    )

    with pytest.raises(ValidationError, match="same component name"):
        PlatformStepComponent(
            **COMPONENT_CATALOG.steps[STEP_REFERENCE.identity].model_dump(
                mode="python",
                exclude={"replay"},
            ),
            replay=replay,
        )


def test_authoring_policy_cannot_elevate_provider_capabilities(tmp_path) -> None:
    unsafe_policy = policy().model_copy(
        update={
            "resource_bindings": {
                "cache": ResourceAuthoringGrant(
                    provider="memory_cache",
                    capabilities=frozenset({ResourceCapability.CACHE, ResourceCapability.DATABASE}),
                )
            }
        }
    )

    with pytest.raises(ConfigurationError, match="does not implement"):
        validate_tenant_authoring(
            tenant_revision(tenant_configuration()),
            scope=SCOPE,
            policy=unsafe_policy,
            component_catalog=COMPONENT_CATALOG,
            platform_resources=PLATFORM_RESOURCES,
            platform_services=PLATFORM_SERVICES,
            transport_registry=builtin_transport_registry(),
            resource_registry=builtin_resource_registry(),
            limits=DEFAULT_RUNTIME_LIMITS,
            config_dir=tmp_path,
        )


def test_temporal_isolation_identities_are_safe_and_scope_specific() -> None:
    first = policy().temporal
    second = policy(scope=OTHER_SCOPE).temporal

    assert first.namespace_identity != second.namespace_identity
    assert first.task_queue_identity != second.task_queue_identity
    assert "tenant-a" not in repr(first)
