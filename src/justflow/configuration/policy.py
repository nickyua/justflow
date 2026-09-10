"""Tenant authoring, capability, secret-alias, and runtime-isolation policy."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from enum import Enum
from pathlib import Path
from types import MappingProxyType
from typing import Self

from pydantic import Field, model_validator

from justflow.config.diagnostics import ValidationDiagnostic
from justflow.config.models import (
    ChildWorkflowTarget,
    EvaluatorCondition,
    ResourcesConfig,
    ServiceOperationTarget,
    ServicesConfig,
    StepDefinition,
    WorkflowConfig,
)
from justflow.config.runtime_limits import RuntimeLimits
from justflow.config.triggers import (
    ApiTriggerDeclaration,
    BrokerTriggerDeclaration,
    EventTriggerDeclaration,
    HostTriggerDeclaration,
    ScheduleTriggerDeclaration,
    TriggerDeclaration,
    TriggerKind,
    TriggersConfig,
    WebhookTriggerDeclaration,
)
from justflow.config.validator import ConfigValidationError, ConfigValidator
from justflow.configuration.errors import ConfigurationError, ConfigurationScopeError
from justflow.configuration.models import (
    ComponentCatalogRevision,
    ComponentReference,
    ConfigurationBundle,
    PlatformComponentCatalog,
    PlatformStepComponent,
    RevisionRecord,
    StrictConfigurationModel,
    TenantComponentOperation,
    TenantConfiguration,
    TenantConfigurationResolution,
    TenantTriggerDeclaration,
    TenantWorkflowConfig,
    TenantWorkflowOperation,
    configuration_revision_identity,
)
from justflow.configuration.ports import PlatformComponentCatalogSource
from justflow.engine.contracts import (
    ContractViolation,
    SchemaDeclarationError,
    validate_payload,
)
from justflow.provenance import provenance_digest
from justflow.resources.base import ResourceCapability
from justflow.resources.registry import ResolvedResource, ResourceRegistry
from justflow.scope import SCOPE_DIGEST_LENGTH, RuntimeScope, safe_identity_digest
from justflow.transports.registry import ResolvedService, TransportRegistry

MAX_POLICY_BINDINGS = 1_000
MAX_POLICY_FIELD_NAMES = 100
MAX_SECRET_ALIASES = 1_000
MAX_POLICY_NAME_LENGTH = 128


class TemporalIsolationMode(str, Enum):
    SHARED = "shared"
    DEDICATED = "dedicated"


class ResourceAuthoringGrant(StrictConfigurationModel):
    provider: str = Field(min_length=1, max_length=MAX_POLICY_NAME_LENGTH)
    capabilities: frozenset[ResourceCapability] = Field(
        min_length=1,
        max_length=MAX_POLICY_FIELD_NAMES,
    )


class ServiceAuthoringGrant(StrictConfigurationModel):
    transport: str = Field(min_length=1, max_length=MAX_POLICY_NAME_LENGTH)


class TriggerBindingGrant(StrictConfigurationModel):
    kind: TriggerKind


class TemporalIsolationPolicy(StrictConfigurationModel):
    mode: TemporalIsolationMode
    namespace_identity: str = Field(
        min_length=SCOPE_DIGEST_LENGTH,
        max_length=SCOPE_DIGEST_LENGTH,
        pattern=rf"^[0-9a-f]{{{SCOPE_DIGEST_LENGTH}}}$",
    )
    task_queue_identity: str = Field(
        min_length=SCOPE_DIGEST_LENGTH,
        max_length=SCOPE_DIGEST_LENGTH,
        pattern=rf"^[0-9a-f]{{{SCOPE_DIGEST_LENGTH}}}$",
    )
    worker_deployment_identity: str = Field(
        min_length=SCOPE_DIGEST_LENGTH,
        max_length=SCOPE_DIGEST_LENGTH,
        pattern=rf"^[0-9a-f]{{{SCOPE_DIGEST_LENGTH}}}$",
    )
    storage_prefix_identity: str = Field(
        min_length=SCOPE_DIGEST_LENGTH,
        max_length=SCOPE_DIGEST_LENGTH,
        pattern=rf"^[0-9a-f]{{{SCOPE_DIGEST_LENGTH}}}$",
    )
    runtime_credential_aliases: frozenset[str] = Field(
        default_factory=frozenset,
        max_length=MAX_SECRET_ALIASES,
    )

    @classmethod
    def for_scope(
        cls,
        scope: RuntimeScope,
        *,
        mode: TemporalIsolationMode,
        namespace: str,
        task_queue: str,
        worker_deployment: str,
        storage_prefix: str,
        runtime_credential_aliases: frozenset[str] = frozenset(),
    ) -> Self:
        identities = (namespace, task_queue, worker_deployment, storage_prefix)
        if any(not value or len(value) > MAX_POLICY_NAME_LENGTH for value in identities):
            raise ValueError("Temporal isolation identity is invalid")
        return cls(
            mode=mode,
            namespace_identity=safe_identity_digest(
                "namespace",
                f"{scope.digest}:{namespace}",
            ),
            task_queue_identity=safe_identity_digest(
                "task-queue",
                f"{scope.digest}:{task_queue}",
            ),
            worker_deployment_identity=safe_identity_digest(
                "worker-deployment",
                f"{scope.digest}:{worker_deployment}",
            ),
            storage_prefix_identity=safe_identity_digest(
                "storage-prefix",
                f"{scope.digest}:{storage_prefix}",
            ),
            runtime_credential_aliases=runtime_credential_aliases,
        )

    @model_validator(mode="after")
    def validate_credential_aliases(self) -> Self:
        _validate_field_names(self.runtime_credential_aliases)
        return self


class TenantAuthoringPolicy(StrictConfigurationModel):
    scope_digest: str = Field(
        min_length=SCOPE_DIGEST_LENGTH,
        max_length=SCOPE_DIGEST_LENGTH,
        pattern=rf"^[0-9a-f]{{{SCOPE_DIGEST_LENGTH}}}$",
    )
    component_catalog_revision: ComponentCatalogRevision
    step_components: frozenset[ComponentReference] = Field(
        default_factory=frozenset,
        max_length=MAX_POLICY_BINDINGS,
    )
    trigger_components: frozenset[ComponentReference] = Field(
        default_factory=frozenset,
        max_length=MAX_POLICY_BINDINGS,
    )
    resource_bindings: dict[str, ResourceAuthoringGrant] = Field(
        default_factory=dict,
        max_length=MAX_POLICY_BINDINGS,
    )
    service_bindings: dict[str, ServiceAuthoringGrant] = Field(
        default_factory=dict,
        max_length=MAX_POLICY_BINDINGS,
    )
    trigger_bindings: dict[str, TriggerBindingGrant] = Field(
        default_factory=dict,
        max_length=MAX_POLICY_BINDINGS,
    )
    secret_aliases: frozenset[str] = Field(
        default_factory=frozenset,
        max_length=MAX_SECRET_ALIASES,
    )
    temporal: TemporalIsolationPolicy

    @model_validator(mode="after")
    def validate_aliases(self) -> Self:
        _validate_field_names(self.secret_aliases)
        return self


@dataclass(frozen=True, kw_only=True)
class ResolvedTenantTrigger:
    component: ComponentReference
    kind: TriggerKind
    workflow: str
    binding_alias: str
    resource_bindings: Mapping[str, str]
    parameters: Mapping[str, object]
    output_schema: Mapping[str, object]


@dataclass(frozen=True, kw_only=True)
class TenantValidatedConfiguration:
    bundle: ConfigurationBundle
    component_catalog_revision: ComponentCatalogRevision
    component_references: Mapping[str, Mapping[str, ComponentReference]]
    triggers: Mapping[str, ResolvedTenantTrigger]
    resources: Mapping[str, ResolvedResource]
    services: Mapping[str, ResolvedService]
    diagnostics: tuple[ValidationDiagnostic, ...]


def validate_tenant_authoring(
    revision: RevisionRecord,
    *,
    scope: RuntimeScope,
    policy: TenantAuthoringPolicy,
    component_catalog: PlatformComponentCatalog | None = None,
    component_catalog_source: PlatformComponentCatalogSource | None = None,
    platform_resources: ResourcesConfig,
    platform_services: ServicesConfig,
    transport_registry: TransportRegistry,
    resource_registry: ResourceRegistry,
    limits: RuntimeLimits,
    config_dir: str | Path,
) -> TenantValidatedConfiguration:
    if (component_catalog is None) == (component_catalog_source is None):
        raise ValueError("Provide one platform component catalog or catalog source")
    if policy.scope_digest != scope.digest:
        raise ConfigurationScopeError("Authoring policy does not belong to the runtime scope")
    if revision.scope_digest != scope.digest:
        raise ConfigurationScopeError("Tenant configuration revision belongs to another scope")
    if (
        configuration_revision_identity(
            revision.scope_digest,
            revision.bundle,
            revision.parent_revision_id,
        )
        != revision.revision_id
    ):
        raise ConfigurationError("Tenant configuration revision identity is invalid")
    configuration = revision.bundle
    if not isinstance(configuration, TenantConfiguration):
        raise ConfigurationError("Tenant authoring requires a tenant configuration revision")
    if component_catalog_source is not None:
        component_catalog = component_catalog_source.read(policy.component_catalog_revision)
    if component_catalog is None:
        raise RuntimeError("Validated component catalog selection is unavailable")
    if (
        configuration.component_catalog_revision != policy.component_catalog_revision
        or component_catalog.revision_id != policy.component_catalog_revision
    ):
        raise ConfigurationError("Tenant configuration uses an unapproved component catalog")
    resources = _approved_resources(platform_resources, policy)
    services = _approved_services(platform_services, policy)
    workflows: dict[str, WorkflowConfig] = {}
    component_references: dict[str, Mapping[str, ComponentReference]] = {}
    for name, workflow in configuration.workflows.items():
        resolved_workflow, references = _resolve_workflow(
            workflow,
            policy=policy,
            component_catalog=component_catalog,
        )
        workflows[name] = resolved_workflow
        component_references[name] = MappingProxyType(references)
    triggers = {
        name: _resolve_trigger(
            declaration,
            policy=policy,
            component_catalog=component_catalog,
        )
        for name, declaration in configuration.triggers.items()
    }
    bundle = ConfigurationBundle(
        resources=resources,
        services=services,
        workflows=workflows,
        triggers=TriggersConfig(
            triggers={
                name: _runtime_trigger(declaration, triggers[name])
                for name, declaration in configuration.triggers.items()
            }
        ),
    )
    validator = ConfigValidator(
        bundle.resources,
        bundle.services,
        bundle.workflows,
        transport_registry=transport_registry,
        resource_registry=resource_registry,
        limits=limits,
        config_dir=config_dir,
        workflow_sources={},
        triggers=bundle.triggers,
    )
    validation = validator.validate()
    try:
        validation.raise_if_invalid()
    except ConfigValidationError as exc:
        raise ConfigurationError("Tenant-authored configuration is invalid") from exc
    narrowed_resources: dict[str, ResolvedResource] = {}
    for name, resource in validator.resolved_resources.items():
        granted = policy.resource_bindings[name].capabilities
        if not granted.issubset(resource.capabilities):
            raise ConfigurationError(
                "Authoring policy grants a capability the provider does not implement"
            )
        narrowed_resources[name] = replace(resource, capabilities=granted)
    component_identities = tuple(
        sorted(
            {
                reference.identity
                for references in component_references.values()
                for reference in references.values()
            }
            | {trigger.component.identity for trigger in triggers.values()}
        )
    )
    resolution = TenantConfigurationResolution(
        tenant_configuration_revision_id=revision.revision_id,
        component_catalog_revision=component_catalog.revision_id,
        component_identities=component_identities,
        resolution_digest=provenance_digest(
            {
                "component_catalog_revision": component_catalog.revision_id,
                "component_identities": component_identities,
                "policy": policy.model_dump(mode="json"),
                "scope_digest": scope.digest,
                "tenant_configuration_revision_id": str(revision.revision_id),
            }
        ),
    )
    bundle = bundle.model_copy(update={"tenant_resolution": resolution})
    return TenantValidatedConfiguration(
        bundle=bundle,
        component_catalog_revision=component_catalog.revision_id,
        component_references=MappingProxyType(component_references),
        triggers=MappingProxyType(triggers),
        resources=MappingProxyType(narrowed_resources),
        services=MappingProxyType(dict(validator.resolved_services)),
        diagnostics=tuple(validation.warnings),
    )


def _approved_resources(
    resources: ResourcesConfig,
    policy: TenantAuthoringPolicy,
) -> ResourcesConfig:
    approved = {}
    for name, grant in policy.resource_bindings.items():
        declaration = resources.resources.get(name)
        if declaration is None:
            raise ConfigurationError("Approved resource binding is not configured by the platform")
        if declaration.provider != grant.provider or declaration.class_path is not None:
            raise ConfigurationError(
                "Approved resource provider does not match platform configuration"
            )
        approved[name] = declaration
    return ResourcesConfig(resources=approved)


def _approved_services(
    services: ServicesConfig,
    policy: TenantAuthoringPolicy,
) -> ServicesConfig:
    approved = {}
    for name, grant in policy.service_bindings.items():
        declaration = services.services.get(name)
        if declaration is None:
            raise ConfigurationError("Approved service binding is not configured by the platform")
        if declaration.transport != grant.transport:
            raise ConfigurationError(
                "Approved service transport does not match platform configuration"
            )
        approved[name] = declaration
    return ServicesConfig(services=approved)


def _resolve_workflow(
    workflow: TenantWorkflowConfig,
    *,
    policy: TenantAuthoringPolicy,
    component_catalog: PlatformComponentCatalog,
) -> tuple[WorkflowConfig, dict[str, ComponentReference]]:
    for flow_step in workflow.flow:
        for branch in flow_step.on_result or ():
            if isinstance(branch.when, EvaluatorCondition):
                raise ConfigurationError(
                    "Tenant-authored conditions cannot import Python evaluators"
                )
    if workflow.on_complete is not None:
        grant = policy.resource_bindings.get(workflow.on_complete.resource)
        if grant is None or ResourceCapability.ARCHIVE not in grant.capabilities:
            raise ConfigurationError("Workflow archive binding is not approved for this scope")
    steps: dict[str, StepDefinition] = {}
    references: dict[str, ComponentReference] = {}
    for name, operation in workflow.steps.items():
        if isinstance(operation, TenantWorkflowOperation):
            steps[name] = StepDefinition(target=ChildWorkflowTarget(workflow=operation.workflow))
            continue
        component = _step_component(operation, policy, component_catalog)
        _validate_component_parameters(
            operation.parameters,
            component.parameter_schema,
            approved_alias_parameters=component.approved_alias_parameters,
            approved_aliases=policy.secret_aliases,
            boundary=component.reference.identity,
        )
        if set(operation.resource_bindings) != set(component.resource_slots):
            raise ConfigurationError("Component resource slots do not match its declaration")
        for slot, capability in component.resource_slots.items():
            resource_name = operation.resource_bindings[slot]
            grant = policy.resource_bindings.get(resource_name)
            if grant is None or capability not in grant.capabilities:
                raise ConfigurationError("Component resource capability is not approved")
        if operation.cache is not None:
            cache_grant = policy.resource_bindings.get(operation.cache.resource)
            if (
                not component.allow_cache
                or cache_grant is None
                or ResourceCapability.CACHE not in cache_grant.capabilities
            ):
                raise ConfigurationError("Component cache binding is not approved")
        service_grant = policy.service_bindings.get(operation.service_binding)
        if service_grant is None or service_grant.transport != component.transport:
            raise ConfigurationError("Component service binding is not approved")
        steps[name] = StepDefinition(
            target=ServiceOperationTarget(
                service=operation.service_binding,
                action=component.action,
            ),
            params=operation.parameters,
            required_resources=sorted(set(operation.resource_bindings.values())),
            cache=operation.cache,
            input_schema=component.input_schema,
            output_schema=component.output_schema,
        )
        references[name] = component.reference
    try:
        resolved = WorkflowConfig.model_validate(
            {
                "workflow": workflow.workflow,
                "description": workflow.description,
                "on_complete": workflow.on_complete,
                "on_error": workflow.on_error,
                "params": workflow.params,
                "input_schema": workflow.input_schema,
                "output_schema": workflow.output_schema,
                "result": workflow.result,
                "steps": steps,
                "flow": workflow.flow,
            }
        )
    except ValueError as exc:
        raise ConfigurationError("Tenant-authored workflow is invalid") from exc
    return resolved, references


def _step_component(
    operation: TenantComponentOperation,
    policy: TenantAuthoringPolicy,
    component_catalog: PlatformComponentCatalog,
) -> PlatformStepComponent:
    if operation.component not in policy.step_components:
        raise ConfigurationError("Step component is not approved for this scope")
    component = component_catalog.steps.get(operation.component.identity)
    if component is None or component.reference != operation.component:
        raise ConfigurationError("Step component does not exist in the approved catalog")
    return component


def _resolve_trigger(
    declaration: TenantTriggerDeclaration,
    *,
    policy: TenantAuthoringPolicy,
    component_catalog: PlatformComponentCatalog,
) -> ResolvedTenantTrigger:
    if declaration.component not in policy.trigger_components:
        raise ConfigurationError("Trigger component is not approved for this scope")
    component = component_catalog.triggers.get(declaration.component.identity)
    if component is None or component.reference != declaration.component:
        raise ConfigurationError("Trigger component does not exist in the approved catalog")
    if declaration.kind is not component.kind:
        raise ConfigurationError("Trigger kind does not match its approved component")
    binding = policy.trigger_bindings.get(declaration.binding_alias)
    if binding is None or binding.kind is not component.kind:
        raise ConfigurationError("Trigger binding is not approved for this scope")
    if set(declaration.resource_bindings) != set(component.resource_slots):
        raise ConfigurationError("Trigger component resource slots do not match its declaration")
    for slot, capability in component.resource_slots.items():
        resource_name = declaration.resource_bindings[slot]
        grant = policy.resource_bindings.get(resource_name)
        if grant is None or capability not in grant.capabilities:
            raise ConfigurationError("Trigger component resource capability is not approved")
    _validate_component_parameters(
        declaration.parameters,
        component.parameter_schema,
        approved_alias_parameters=component.approved_alias_parameters,
        approved_aliases=policy.secret_aliases,
        boundary=component.reference.identity,
    )
    return ResolvedTenantTrigger(
        component=component.reference,
        kind=component.kind,
        workflow=declaration.workflow,
        binding_alias=declaration.binding_alias,
        resource_bindings=MappingProxyType(dict(declaration.resource_bindings)),
        parameters=MappingProxyType(dict(declaration.parameters)),
        output_schema=MappingProxyType(dict(component.output_schema)),
    )


def _runtime_trigger(
    declaration: TenantTriggerDeclaration,
    resolved: ResolvedTenantTrigger,
) -> TriggerDeclaration:
    common = {
        "kind": resolved.kind,
        "workflow": declaration.workflow,
        "paused": declaration.paused,
    }
    if resolved.kind is TriggerKind.API:
        return ApiTriggerDeclaration.model_validate(common)
    if resolved.kind is TriggerKind.WEBHOOK:
        return WebhookTriggerDeclaration.model_validate(
            {**common, "source": resolved.binding_alias}
        )
    if resolved.kind is TriggerKind.EVENT:
        return EventTriggerDeclaration.model_validate({**common, "mapping": resolved.binding_alias})
    if resolved.kind is TriggerKind.BROKER:
        return BrokerTriggerDeclaration.model_validate({**common, "broker": resolved.binding_alias})
    if resolved.kind is TriggerKind.HOST:
        return HostTriggerDeclaration.model_validate({**common, "adapter": resolved.binding_alias})
    reserved = {"kind", "paused", "workflow"}.intersection(resolved.parameters)
    if reserved:
        raise ConfigurationError("Schedule trigger parameters contain reserved fields")
    try:
        return ScheduleTriggerDeclaration.model_validate({**resolved.parameters, **common})
    except ValueError as exc:
        raise ConfigurationError("Schedule trigger parameters are invalid") from exc


def _validate_component_parameters(
    parameters: Mapping[str, object],
    schema: dict[str, object],
    *,
    approved_alias_parameters: frozenset[str],
    approved_aliases: frozenset[str],
    boundary: str,
) -> None:
    for name in approved_alias_parameters.intersection(parameters):
        value = parameters[name]
        if not isinstance(value, str) or value not in approved_aliases:
            raise ConfigurationError("Component credential alias is not approved for this scope")
    try:
        validate_payload(
            schema,
            dict(parameters),
            direction="parameters",
            step_name=boundary,
        )
    except (ContractViolation, SchemaDeclarationError) as exc:
        raise ConfigurationError("Component parameters do not satisfy their contract") from exc


def _validate_field_names(values: frozenset[str]) -> None:
    if any(not value or len(value) > MAX_POLICY_NAME_LENGTH for value in values):
        raise ValueError("Policy field or alias name is invalid")
