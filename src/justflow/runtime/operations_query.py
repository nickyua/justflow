"""Typed, scope-preserving operational reads across runtime state sources."""

from __future__ import annotations

import json
from collections.abc import Mapping
from enum import Enum
from typing import Any, Protocol, TypeVar, cast

import yaml
from pydantic import Field

from justflow.config.authored_yaml import authored_dump, render_authored_yaml
from justflow.config.grammar import RESERVED_DATA_ROOTS, placeholder_names
from justflow.config.models import (
    EvaluatorCondition,
    ResourcesConfig,
    ServiceOperationTarget,
    ServicesConfig,
    WorkflowConfig,
)
from justflow.config.settings import MAX_CONTROL_LIST_LIMIT, OperationsMetricsLinkSettings
from justflow.config.triggers import TriggerKind, TriggersConfig
from justflow.configuration.activation import ActivationPage
from justflow.configuration.activation_errors import ActivationError
from justflow.configuration.activation_store import ActivationStore
from justflow.configuration.errors import ConfigurationError
from justflow.configuration.models import (
    ActivePointer,
    PlatformComponentCatalog,
    PlatformStepComponent,
    PlatformTriggerComponent,
    RevisionPage,
)
from justflow.configuration.policy import TenantAuthoringPolicy
from justflow.configuration.ports import ConfigurationStore, PlatformComponentCatalogSource
from justflow.configuration.publication import TenantAuthoringPolicySource
from justflow.definitions.catalog import CatalogError, DefinitionCatalogStore
from justflow.definitions.manifest import DefinitionManifest
from justflow.provenance import provenance_digest
from justflow.resources.base import ResourceCapability
from justflow.resources.registry import ResourceRegistry
from justflow.runtime.blocking_io import run_blocking
from justflow.runtime.health import HealthRegistry, HealthReport
from justflow.runtime.operations import (
    StrictControlModel,
    WorkflowControlService,
    WorkflowDescription,
    WorkflowListQuery,
    WorkflowListResult,
)
from justflow.runtime.schedule_operations import ManagedScheduleDescription, ScheduleOperationError
from justflow.schemas import load_bundled_schemas
from justflow.schemas._generation import TENANT_CONFIGURATION_SCHEMA_FILE
from justflow.scope import RuntimeScope, decode_scope_cursor, encode_scope_cursor
from justflow.visualization.graph import build_graph

MAX_OPERATIONS_REASON_LENGTH = 256
MAX_SCHEMA_DOCUMENT_BYTES = 1_048_576
MAX_WORKFLOW_GRAPH_NODES = 500
MAX_WORKFLOW_GRAPH_EDGES = 1_000
MAX_WORKFLOW_GRAPH_ID_LENGTH = 256
MAX_WORKFLOW_GRAPH_LABEL_LENGTH = 128
MAX_WORKFLOW_DESCRIPTION_LENGTH = 4_096
MAX_OPERATIONS_TRIGGER_ENTRIES = 1_000
PageItem = TypeVar("PageItem")


class OperationsAvailability(str, Enum):
    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"


class OperationsDimension(StrictControlModel):
    availability: OperationsAvailability
    reason: str | None = Field(default=None, max_length=MAX_OPERATIONS_REASON_LENGTH)


class DefinitionSummary(StrictControlModel):
    logical_workflow: str
    definition_digest: str
    active: bool
    required_engine_workflow_abi: str


class WorkflowRegistration(StrictControlModel):
    logical_workflow: str
    active_definition_digest: str
    retained_definition_count: int = Field(ge=1)
    required_engine_workflow_abi: str


class WorkflowServiceDependency(StrictControlModel):
    service: str
    actions: tuple[str, ...]


MAX_NODE_METADATA_KEYS = 16
MAX_NODE_METADATA_VALUE_LENGTH = 200


def bounded_node_metadata(metadata: Mapping[str, object]) -> dict[str, str]:
    """Project declaration-level node annotations as bounded display strings."""
    projected: dict[str, str] = {}
    for key in sorted(metadata)[:MAX_NODE_METADATA_KEYS]:
        value = metadata[key]
        rendered = value if isinstance(value, str) else json.dumps(value, sort_keys=True)
        projected[str(key)] = rendered[:MAX_NODE_METADATA_VALUE_LENGTH]
    return projected


class WorkflowGraphNode(StrictControlModel):
    metadata: dict[str, str] = Field(default_factory=dict)
    node_id: str = Field(min_length=1, max_length=MAX_WORKFLOW_GRAPH_ID_LENGTH)
    label: str = Field(min_length=1, max_length=MAX_WORKFLOW_GRAPH_LABEL_LENGTH)
    kind: str = Field(min_length=1, max_length=MAX_WORKFLOW_GRAPH_LABEL_LENGTH)
    group: str | None = Field(default=None, max_length=MAX_WORKFLOW_GRAPH_LABEL_LENGTH)


class WorkflowGraphEdge(StrictControlModel):
    source: str = Field(min_length=1, max_length=MAX_WORKFLOW_GRAPH_ID_LENGTH)
    target: str = Field(min_length=1, max_length=MAX_WORKFLOW_GRAPH_ID_LENGTH)
    label: str | None = Field(default=None, max_length=MAX_WORKFLOW_GRAPH_LABEL_LENGTH)
    dashed: bool = False


class WorkflowStepBinding(StrictControlModel):
    """One `steps:` entry — the operation and the components it binds to."""

    operation: str
    service: str | None = None
    action: str | None = None
    subworkflow: str | None = None
    resources: tuple[str, ...] = Field(default_factory=tuple)


class WorkflowArchivalBinding(StrictControlModel):
    resource: str
    retention_policy: str


class WorkflowDetail(WorkflowRegistration):
    description: str = Field(max_length=MAX_WORKFLOW_DESCRIPTION_LENGTH)
    graph_nodes: tuple[WorkflowGraphNode, ...] = Field(max_length=MAX_WORKFLOW_GRAPH_NODES)
    service_dependencies: tuple[WorkflowServiceDependency, ...] = Field(default_factory=tuple)
    resource_dependencies: tuple[str, ...] = Field(default_factory=tuple)
    has_input_contract: bool = False
    has_output_contract: bool = False
    input_schema: dict[str, Any] | None = None
    referenced_globals: tuple[str, ...] = Field(default_factory=tuple)
    step_bindings: tuple[WorkflowStepBinding, ...] = Field(default_factory=tuple)
    archival: WorkflowArchivalBinding | None = None
    graph_edges: tuple[WorkflowGraphEdge, ...] = Field(max_length=MAX_WORKFLOW_GRAPH_EDGES)
    definitions: tuple[DefinitionSummary, ...] = Field(max_length=MAX_CONTROL_LIST_LIMIT)


MAX_AUTHORING_REFERENCE_ENTRIES = 500
MAX_DEFINITION_DOCUMENT_BYTES = 262_144


class AuthoringReferenceAction(StrictControlModel):
    """An action observed in current workflow declarations, with any declared
    step-level contracts. Authoritative schemas arrive with the host-published
    component catalog; nothing is invented here."""

    name: str
    workflows: tuple[str, ...]
    input_schema: dict[str, Any] | None = None
    output_schema: dict[str, Any] | None = None
    input_contract_identity: str | None = None
    output_contract_identity: str | None = None
    contract_conflict: bool = False


class AuthoringReferenceService(StrictControlModel):
    name: str
    description: str | None = Field(default=None, max_length=MAX_WORKFLOW_DESCRIPTION_LENGTH)
    actions: tuple[AuthoringReferenceAction, ...] = Field(default_factory=tuple)


class AuthoringReferenceResource(StrictControlModel):
    name: str
    description: str | None = Field(default=None, max_length=MAX_WORKFLOW_DESCRIPTION_LENGTH)
    capabilities: tuple[str, ...]


class AuthoringReferenceAuthority(str, Enum):
    IMMUTABLE_CATALOG = "immutable_catalog"
    OBSERVED_HOST = "observed_host"


class AuthoringReferenceComponentKind(str, Enum):
    STEP = "step"
    TRIGGER = "trigger"


class AuthoringReferenceResourceSlot(StrictControlModel):
    name: str
    capability: str


class AuthoringReferenceComponent(StrictControlModel):
    kind: AuthoringReferenceComponentKind
    name: str
    version: str
    description: str | None = Field(default=None, max_length=MAX_WORKFLOW_DESCRIPTION_LENGTH)
    action: str | None = None
    capabilities: tuple[str, ...] = Field(default_factory=tuple)
    parameter_schema: dict[str, Any] | None = None
    parameter_contract_identity: str | None = None
    input_schema: dict[str, Any] | None = None
    input_contract_identity: str | None = None
    output_schema: dict[str, Any] | None = None
    output_contract_identity: str | None = None
    resource_slots: tuple[AuthoringReferenceResourceSlot, ...] = Field(default_factory=tuple)


class AuthoringReferenceTriggerBinding(StrictControlModel):
    name: str
    kind: str


class AuthoringReference(StrictControlModel):
    """Least-privilege authoring catalog: names and capabilities, never configuration."""

    dimension: OperationsDimension
    authority: AuthoringReferenceAuthority
    catalog_revision: str | None = None
    components: tuple[AuthoringReferenceComponent, ...] = Field(
        default_factory=tuple,
        max_length=MAX_AUTHORING_REFERENCE_ENTRIES,
    )
    services: tuple[AuthoringReferenceService, ...] = Field(
        max_length=MAX_AUTHORING_REFERENCE_ENTRIES
    )
    resources: tuple[AuthoringReferenceResource, ...] = Field(
        max_length=MAX_AUTHORING_REFERENCE_ENTRIES
    )
    trigger_bindings: tuple[AuthoringReferenceTriggerBinding, ...] = Field(
        default_factory=tuple,
        max_length=MAX_AUTHORING_REFERENCE_ENTRIES,
    )
    truncated: bool = False


class AuthoringReferenceSource(Protocol):
    def read(self, scope: RuntimeScope) -> AuthoringReference: ...


def _observed_service_actions(
    workflows: Mapping[str, WorkflowConfig],
) -> dict[str, tuple[AuthoringReferenceAction, ...]]:
    """Per service: actions referenced by current declarations, with declared
    step-level contracts when exactly one distinct contract exists (conflicts
    are disclosed, never resolved)."""
    observed: dict[tuple[str, str], dict[str, object]] = {}
    for workflow_name, workflow in sorted(workflows.items()):
        for step in workflow.steps.values():
            if not isinstance(step.target, ServiceOperationTarget):
                continue
            entry = observed.setdefault(
                (step.target.service, step.target.action),
                {"workflows": set(), "inputs": [], "outputs": []},
            )
            cast(set[str], entry["workflows"]).add(workflow_name)
            if step.input_schema is not None:
                cast(list[object], entry["inputs"]).append(step.input_schema)
            if step.output_schema is not None:
                cast(list[object], entry["outputs"]).append(step.output_schema)
    actions: dict[str, list[AuthoringReferenceAction]] = {}
    for (service, action), entry in sorted(observed.items()):
        inputs = _distinct_schemas(cast(list[object], entry["inputs"]))
        outputs = _distinct_schemas(cast(list[object], entry["outputs"]))
        actions.setdefault(service, []).append(
            AuthoringReferenceAction(
                name=action,
                workflows=tuple(sorted(cast(set[str], entry["workflows"]))),
                input_schema=inputs[0] if len(inputs) == 1 else None,
                output_schema=outputs[0] if len(outputs) == 1 else None,
                input_contract_identity=(_schema_identity(inputs[0]) if len(inputs) == 1 else None),
                output_contract_identity=(
                    _schema_identity(outputs[0]) if len(outputs) == 1 else None
                ),
                contract_conflict=len(inputs) > 1 or len(outputs) > 1,
            )
        )
    return {service: tuple(entries) for service, entries in actions.items()}


def _distinct_schemas(schemas: list[object]) -> list[dict[str, Any]]:
    distinct: list[dict[str, Any]] = []
    seen: set[str] = set()
    for schema in schemas:
        if not isinstance(schema, dict):
            continue
        key = json.dumps(schema, sort_keys=True)
        if key not in seen:
            seen.add(key)
            distinct.append(schema)
    return distinct


def build_authoring_reference(
    *,
    services: ServicesConfig,
    resources: ResourcesConfig,
    resource_registry: ResourceRegistry,
    workflows: Mapping[str, WorkflowConfig] | None = None,
) -> AuthoringReference:
    service_names = sorted(services.services)
    service_actions = _observed_service_actions(workflows or {})
    resource_names = sorted(resources.resources)
    providers = resource_registry.providers
    resource_entries = []
    for name in resource_names[:MAX_AUTHORING_REFERENCE_ENTRIES]:
        declaration = resources.resources[name]
        capabilities: tuple[str, ...] = ()
        if declaration.provider is not None:
            provider = providers.get(declaration.provider)
            if provider is not None:
                capabilities = tuple(
                    sorted(capability.value for capability in provider.capabilities)
                )
        resource_entries.append(AuthoringReferenceResource(name=name, capabilities=capabilities))
    return AuthoringReference(
        dimension=_available(),
        authority=AuthoringReferenceAuthority.OBSERVED_HOST,
        services=tuple(
            AuthoringReferenceService(name=name, actions=service_actions.get(name, ()))
            for name in service_names[:MAX_AUTHORING_REFERENCE_ENTRIES]
        ),
        resources=tuple(resource_entries),
        truncated=len(service_names) > MAX_AUTHORING_REFERENCE_ENTRIES
        or len(resource_names) > MAX_AUTHORING_REFERENCE_ENTRIES,
    )


class StaticAuthoringReferenceSource:
    def __init__(self, scope: RuntimeScope, reference: AuthoringReference) -> None:
        self._scope = scope
        self._reference = reference

    def read(self, scope: RuntimeScope) -> AuthoringReference:
        if scope != self._scope:
            return _unavailable_authoring_reference(
                "Authoring reference is unavailable for the runtime scope"
            )
        return self._reference


class UnavailableAuthoringReferenceSource:
    def __init__(self, reason: str) -> None:
        self._reason = reason

    def read(self, scope: RuntimeScope) -> AuthoringReference:
        del scope
        return _unavailable_authoring_reference(self._reason)


class CatalogAuthoringReferenceSource:
    def __init__(
        self,
        *,
        component_catalog_source: PlatformComponentCatalogSource,
        policy_source: TenantAuthoringPolicySource,
    ) -> None:
        self._component_catalog_source = component_catalog_source
        self._policy_source = policy_source

    def read(self, scope: RuntimeScope) -> AuthoringReference:
        try:
            policy = self._policy_source.read(scope)
            if policy.scope_digest != scope.digest:
                raise ConfigurationError("Authoring policy scope does not match")
            catalog = self._component_catalog_source.read(policy.component_catalog_revision)
            if catalog.revision_id != policy.component_catalog_revision:
                raise ConfigurationError("Component catalog revision does not match")
            return build_catalog_authoring_reference(policy=policy, catalog=catalog)
        except ConfigurationError:
            return _unavailable_authoring_reference(
                "Managed component catalog or authoring policy is unavailable"
            )


def build_catalog_authoring_reference(
    *,
    policy: TenantAuthoringPolicy,
    catalog: PlatformComponentCatalog,
) -> AuthoringReference:
    if catalog.revision_id != policy.component_catalog_revision:
        raise ConfigurationError("Component catalog revision does not match policy")
    step_components = tuple(
        _project_step_component(_required_step_component(catalog, reference.identity))
        for reference in sorted(policy.step_components, key=lambda item: item.identity)
    )
    trigger_components = tuple(
        _project_trigger_component(_required_trigger_component(catalog, reference.identity))
        for reference in sorted(policy.trigger_components, key=lambda item: item.identity)
    )
    service_names = sorted(policy.service_bindings)
    resource_names = sorted(policy.resource_bindings)
    trigger_binding_names = sorted(policy.trigger_bindings)
    truncated = any(
        len(entries) > MAX_AUTHORING_REFERENCE_ENTRIES
        for entries in (
            step_components + trigger_components,
            service_names,
            resource_names,
            trigger_binding_names,
        )
    )
    return AuthoringReference(
        dimension=_available(),
        authority=AuthoringReferenceAuthority.IMMUTABLE_CATALOG,
        catalog_revision=catalog.revision_id,
        components=(step_components + trigger_components)[:MAX_AUTHORING_REFERENCE_ENTRIES],
        services=tuple(
            AuthoringReferenceService(name=name)
            for name in service_names[:MAX_AUTHORING_REFERENCE_ENTRIES]
        ),
        resources=tuple(
            AuthoringReferenceResource(
                name=name,
                capabilities=tuple(
                    sorted(
                        capability.value
                        for capability in policy.resource_bindings[name].capabilities
                    )
                ),
            )
            for name in resource_names[:MAX_AUTHORING_REFERENCE_ENTRIES]
        ),
        trigger_bindings=tuple(
            AuthoringReferenceTriggerBinding(
                name=name,
                kind=policy.trigger_bindings[name].kind.value,
            )
            for name in trigger_binding_names[:MAX_AUTHORING_REFERENCE_ENTRIES]
        ),
        truncated=truncated,
    )


def _project_step_component(
    component: PlatformStepComponent,
) -> AuthoringReferenceComponent:
    return AuthoringReferenceComponent(
        kind=AuthoringReferenceComponentKind.STEP,
        name=component.reference.name,
        version=component.reference.version,
        description=component.description,
        action=component.action,
        parameter_schema=component.parameter_schema,
        parameter_contract_identity=_schema_identity(component.parameter_schema),
        input_schema=component.input_schema,
        input_contract_identity=_schema_identity(component.input_schema),
        output_schema=component.output_schema,
        output_contract_identity=_schema_identity(component.output_schema),
        resource_slots=_resource_slots(component.resource_slots),
    )


def _project_trigger_component(
    component: PlatformTriggerComponent,
) -> AuthoringReferenceComponent:
    return AuthoringReferenceComponent(
        kind=AuthoringReferenceComponentKind.TRIGGER,
        name=component.reference.name,
        version=component.reference.version,
        description=component.description,
        capabilities=(component.kind.value,),
        parameter_schema=component.parameter_schema,
        parameter_contract_identity=_schema_identity(component.parameter_schema),
        output_schema=component.output_schema,
        output_contract_identity=_schema_identity(component.output_schema),
        resource_slots=_resource_slots(component.resource_slots),
    )


def _resource_slots(
    resource_slots: Mapping[str, ResourceCapability],
) -> tuple[AuthoringReferenceResourceSlot, ...]:
    return tuple(
        AuthoringReferenceResourceSlot(name=name, capability=capability.value)
        for name, capability in sorted(resource_slots.items())
    )


def _required_step_component(
    catalog: PlatformComponentCatalog,
    identity: str,
) -> PlatformStepComponent:
    component = catalog.steps.get(identity)
    if component is None:
        raise ConfigurationError("Approved step component is missing from the catalog")
    return component


def _required_trigger_component(
    catalog: PlatformComponentCatalog,
    identity: str,
) -> PlatformTriggerComponent:
    component = catalog.triggers.get(identity)
    if component is None:
        raise ConfigurationError("Approved trigger component is missing from the catalog")
    return component


def _schema_identity(schema: Mapping[str, object]) -> str:
    return provenance_digest({"schema": schema})


def _unavailable_authoring_reference(reason: str) -> AuthoringReference:
    return AuthoringReference(
        dimension=_unavailable(reason),
        authority=AuthoringReferenceAuthority.IMMUTABLE_CATALOG,
        components=(),
        services=(),
        resources=(),
    )


class WorkflowDefinitionDocument(StrictControlModel):
    logical_workflow: str
    definition_digest: str
    document: str


class DefinitionPage(StrictControlModel):
    dimension: OperationsDimension
    definitions: tuple[DefinitionSummary, ...]
    next_cursor: str | None = Field(default=None, repr=False)


class WorkflowRegistrationPage(StrictControlModel):
    dimension: OperationsDimension
    workflows: tuple[WorkflowRegistration, ...]
    next_cursor: str | None = Field(default=None, repr=False)


class ConfigurationOperationsView(StrictControlModel):
    dimension: OperationsDimension
    active: ActivePointer | None = None
    revisions: RevisionPage


class ActivationOperationsView(StrictControlModel):
    dimension: OperationsDimension
    activations: ActivationPage


class TriggerOperationalState(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"


class TriggerSummary(StrictControlModel):
    name: str
    kind: TriggerKind
    workflow_name: str
    state: TriggerOperationalState
    schedule: ManagedScheduleDescription | None = None


class TriggerOperationsView(StrictControlModel):
    dimension: OperationsDimension
    triggers: tuple[TriggerSummary, ...] = Field(max_length=MAX_OPERATIONS_TRIGGER_ENTRIES)


class MetricsLink(StrictControlModel):
    label: str
    url: str


class OperationsOverview(StrictControlModel):
    health: HealthReport
    executions: OperationsDimension
    definitions: OperationsDimension
    configuration: OperationsDimension
    activations: OperationsDimension
    triggers: OperationsDimension
    metrics_links: tuple[MetricsLink, ...]


class ConfigurationSchema(StrictControlModel):
    media_type: str = "application/schema+json"
    schema_document: dict[str, object]


class DefinitionCatalogSource(Protocol):
    def read(self, scope: RuntimeScope) -> DefinitionCatalogStore: ...


class ScheduleOperationsSource(Protocol):
    async def list(self, scope: RuntimeScope) -> tuple[ManagedScheduleDescription, ...]: ...


class BoundScheduleOperationsSource(Protocol):
    async def list(self) -> tuple[ManagedScheduleDescription, ...]: ...


class ScopedScheduleOperationsSource:
    def __init__(
        self,
        operators: Mapping[RuntimeScope, BoundScheduleOperationsSource],
    ) -> None:
        indexed: dict[str, tuple[RuntimeScope, BoundScheduleOperationsSource]] = {}
        for scope, operator in operators.items():
            if scope.digest in indexed:
                raise ValueError("Schedule operations source has a duplicate runtime scope")
            indexed[scope.digest] = (scope, operator)
        self._operators = indexed

    async def list(self, scope: RuntimeScope) -> tuple[ManagedScheduleDescription, ...]:
        entry = self._operators.get(scope.digest)
        if entry is None or entry[0] != scope:
            raise OperationsQueryError(
                OperationsQueryErrorCode.UNAVAILABLE,
                "Managed schedules are unavailable for the runtime scope",
            )
        return await entry[1].list()


class DeclaredTriggerSource(Protocol):
    def read(self, scope: RuntimeScope) -> TriggersConfig: ...


class ScopedDeclaredTriggerSource:
    def __init__(self, declarations: Mapping[RuntimeScope, TriggersConfig]) -> None:
        indexed: dict[str, tuple[RuntimeScope, TriggersConfig]] = {}
        for scope, triggers in declarations.items():
            if scope.digest in indexed:
                raise ValueError("Declared trigger source has a duplicate runtime scope")
            indexed[scope.digest] = (scope, triggers)
        self._declarations = indexed

    def read(self, scope: RuntimeScope) -> TriggersConfig:
        entry = self._declarations.get(scope.digest)
        if entry is None or entry[0] != scope:
            raise OperationsQueryError(
                OperationsQueryErrorCode.UNAVAILABLE,
                "Declared triggers are unavailable for the runtime scope",
            )
        return entry[1]


class OperationsQueryErrorCode(str, Enum):
    INVALID_QUERY = "invalid_query"
    NOT_FOUND = "not_found"
    STALE_CURSOR = "stale_cursor"
    UNAVAILABLE = "unavailable"


class OperationsQueryError(Exception):
    def __init__(self, code: OperationsQueryErrorCode, message: str) -> None:
        self.code = code
        super().__init__(message)


class OperationsQueryService:
    def __init__(
        self,
        *,
        executions: WorkflowControlService,
        health: HealthRegistry,
        definition_catalogs: DefinitionCatalogSource | None = None,
        configuration_store: ConfigurationStore | None = None,
        activation_store: ActivationStore | None = None,
        triggers: DeclaredTriggerSource | None = None,
        schedules: ScheduleOperationsSource | None = None,
        authoring_reference_source: AuthoringReferenceSource | None = None,
        metrics_links: tuple[OperationsMetricsLinkSettings, ...] = (),
    ) -> None:
        self._executions = executions
        self._health = health
        self._definition_catalogs = definition_catalogs
        self._configuration_store = configuration_store
        self._activation_store = activation_store
        self._triggers = triggers
        self._schedules = schedules
        self._authoring_reference_source = authoring_reference_source
        self._metrics_links = tuple(
            MetricsLink(label=link.label, url=link.url) for link in metrics_links
        )

    def overview(self) -> OperationsOverview:
        return OperationsOverview(
            health=self._health.report(),
            executions=_available(),
            definitions=_configured(self._definition_catalogs, "Definition catalog"),
            configuration=_configured(self._configuration_store, "Configuration history"),
            activations=_configured(self._activation_store, "Activation history"),
            triggers=_configured(self._triggers, "Declared triggers"),
            metrics_links=self._metrics_links,
        )

    async def list_runs(
        self,
        scope: RuntimeScope,
        *,
        limit: int,
        cursor: str | None,
        query: WorkflowListQuery,
    ) -> WorkflowListResult:
        _validate_limit(limit)
        return await self._executions.list(
            scope=scope,
            limit=limit,
            page_token=cursor,
            query=query,
        )

    async def describe_run(
        self,
        scope: RuntimeScope,
        workflow_id: str,
        *,
        run_id: str | None,
    ) -> WorkflowDescription:
        return await self._executions.describe(workflow_id, run_id=run_id, scope=scope)

    def list_definitions(
        self,
        scope: RuntimeScope,
        *,
        limit: int,
        cursor: str | None,
    ) -> DefinitionPage:
        _validate_limit(limit)
        source = self._definition_catalogs
        if source is None:
            return DefinitionPage(
                dimension=_unavailable("Definition catalog is not configured"),
                definitions=(),
            )
        try:
            state = source.read(scope).inspect()
        except (CatalogError, ActivationError) as exc:
            raise OperationsQueryError(
                OperationsQueryErrorCode.UNAVAILABLE,
                "Definition catalog is unavailable",
            ) from exc
        definitions = tuple(
            DefinitionSummary(
                logical_workflow=logical_name,
                definition_digest=digest,
                active=state.catalog.aliases.get(logical_name) == digest,
                required_engine_workflow_abi=manifest.required_engine_workflow_abi,
            )
            for (logical_name, digest), manifest in sorted(state.catalog.manifests.items())
        )
        page, next_cursor = _page(
            scope,
            definitions,
            kind="definitions",
            snapshot=state.alias_version,
            limit=limit,
            cursor=cursor,
        )
        return DefinitionPage(
            dimension=_available(),
            definitions=page,
            next_cursor=next_cursor,
        )

    def list_workflows(
        self,
        scope: RuntimeScope,
        *,
        limit: int,
        cursor: str | None,
    ) -> WorkflowRegistrationPage:
        _validate_limit(limit)
        source = self._definition_catalogs
        if source is None:
            return WorkflowRegistrationPage(
                dimension=_unavailable("Definition catalog is not configured"),
                workflows=(),
            )
        try:
            state = source.read(scope).inspect()
        except (CatalogError, ActivationError) as exc:
            raise OperationsQueryError(
                OperationsQueryErrorCode.UNAVAILABLE,
                "Definition catalog is unavailable",
            ) from exc
        retained_counts: dict[str, int] = {}
        for logical_name, _ in state.catalog.manifests:
            retained_counts[logical_name] = retained_counts.get(logical_name, 0) + 1
        workflows = tuple(
            WorkflowRegistration(
                logical_workflow=logical_name,
                active_definition_digest=digest,
                retained_definition_count=retained_counts[logical_name],
                required_engine_workflow_abi=state.catalog.get(
                    logical_name,
                    digest,
                ).required_engine_workflow_abi,
            )
            for logical_name, digest in sorted(state.catalog.aliases.items())
        )
        page, next_cursor = _page(
            scope,
            workflows,
            kind="workflows",
            snapshot=state.alias_version,
            limit=limit,
            cursor=cursor,
        )
        return WorkflowRegistrationPage(
            dimension=_available(),
            workflows=page,
            next_cursor=next_cursor,
        )

    def workflow_detail(self, scope: RuntimeScope, logical_workflow: str) -> WorkflowDetail:
        source = self._definition_catalogs
        if source is None:
            raise OperationsQueryError(
                OperationsQueryErrorCode.UNAVAILABLE,
                "Definition catalog is not configured",
            )
        try:
            state = source.read(scope).inspect()
            digest = state.catalog.aliases.get(logical_workflow)
            if digest is None:
                raise OperationsQueryError(
                    OperationsQueryErrorCode.NOT_FOUND,
                    "Workflow registration was not found",
                )
            manifest = state.catalog.get(logical_workflow, digest)
            workflow = WorkflowConfig.model_validate(manifest.workflow)
            children = _workflow_children(state.catalog.manifests, manifest)
            graph = build_graph(workflow, subworkflows=children)
        except OperationsQueryError:
            raise
        except (CatalogError, ActivationError, ValueError) as exc:
            raise OperationsQueryError(
                OperationsQueryErrorCode.UNAVAILABLE,
                "Workflow registration is unavailable",
            ) from exc
        if (
            len(graph.nodes) > MAX_WORKFLOW_GRAPH_NODES
            or len(graph.edges) > MAX_WORKFLOW_GRAPH_EDGES
        ):
            raise OperationsQueryError(
                OperationsQueryErrorCode.UNAVAILABLE,
                "Workflow graph exceeds its operational response bound",
            )
        definitions = tuple(
            DefinitionSummary(
                logical_workflow=name,
                definition_digest=definition_digest,
                active=state.catalog.aliases.get(name) == definition_digest,
                required_engine_workflow_abi=definition.required_engine_workflow_abi,
            )
            for (name, definition_digest), definition in sorted(state.catalog.manifests.items())
            if name == logical_workflow
        )
        service_dependencies, resource_dependencies = _workflow_dependencies(workflow)
        return WorkflowDetail(
            logical_workflow=logical_workflow,
            active_definition_digest=digest,
            retained_definition_count=len(definitions),
            required_engine_workflow_abi=manifest.required_engine_workflow_abi,
            description=workflow.description,
            service_dependencies=service_dependencies,
            resource_dependencies=resource_dependencies,
            has_input_contract=workflow.input_schema is not None,
            has_output_contract=workflow.output_schema is not None,
            referenced_globals=tuple(
                sorted(placeholder_names(manifest.workflow) - RESERVED_DATA_ROOTS)
            ),
            step_bindings=_step_bindings(workflow),
            archival=_archival_binding(workflow),
            input_schema=(
                workflow.input_schema if isinstance(workflow.input_schema, dict) else None
            ),
            graph_nodes=tuple(
                WorkflowGraphNode(
                    node_id=node.id,
                    label=graph_node_label(node.id, node.node_type),
                    kind=node.node_type,
                    group=node.group,
                    metadata=bounded_node_metadata(node.metadata),
                )
                for node in graph.nodes
            ),
            graph_edges=tuple(
                WorkflowGraphEdge(
                    source=edge.source,
                    target=edge.target,
                    label=graph_edge_label(edge.label),
                    dashed=edge.style == "dashed",
                )
                for edge in graph.edges
            ),
            definitions=definitions[:MAX_CONTROL_LIST_LIMIT],
        )

    def authoring_reference(self, scope: RuntimeScope) -> AuthoringReference:
        source = self._authoring_reference_source
        if source is None:
            return _unavailable_authoring_reference("Authoring reference is not configured")
        return source.read(scope)

    def workflow_definition(
        self,
        scope: RuntimeScope,
        logical_workflow: str,
    ) -> WorkflowDefinitionDocument:
        source = self._definition_catalogs
        if source is None:
            raise OperationsQueryError(
                OperationsQueryErrorCode.UNAVAILABLE,
                "Definition catalog is not configured",
            )
        try:
            state = source.read(scope).inspect()
            digest = state.catalog.aliases.get(logical_workflow)
            if digest is None:
                raise OperationsQueryError(
                    OperationsQueryErrorCode.NOT_FOUND,
                    "Workflow registration was not found",
                )
            manifest = state.catalog.get(logical_workflow, digest)
            document = render_authored_yaml(
                authored_dump(WorkflowConfig.model_validate(manifest.workflow))
            )
        except OperationsQueryError:
            raise
        except (CatalogError, ActivationError, ValueError, yaml.YAMLError) as exc:
            raise OperationsQueryError(
                OperationsQueryErrorCode.UNAVAILABLE,
                "Workflow definition is unavailable",
            ) from exc
        if len(document.encode("utf-8")) > MAX_DEFINITION_DOCUMENT_BYTES:
            raise OperationsQueryError(
                OperationsQueryErrorCode.UNAVAILABLE,
                "Workflow definition exceeds its operational response bound",
            )
        return WorkflowDefinitionDocument(
            logical_workflow=logical_workflow,
            definition_digest=digest,
            document=document,
        )

    def configuration(
        self,
        scope: RuntimeScope,
        *,
        limit: int,
        cursor: str | None,
    ) -> ConfigurationOperationsView:
        _validate_limit(limit)
        store = self._configuration_store
        if store is None:
            return ConfigurationOperationsView(
                dimension=_unavailable("Configuration history is not configured"),
                revisions=RevisionPage(revisions=()),
            )
        try:
            return ConfigurationOperationsView(
                dimension=_available(),
                active=store.read_active(scope),
                revisions=store.list_revisions(scope, limit=limit, cursor=cursor),
            )
        except ConfigurationError as exc:
            raise OperationsQueryError(
                OperationsQueryErrorCode.UNAVAILABLE,
                "Configuration history is unavailable",
            ) from exc

    def activations(
        self,
        scope: RuntimeScope,
        *,
        limit: int,
        cursor: str | None,
    ) -> ActivationOperationsView:
        _validate_limit(limit)
        store = self._activation_store
        if store is None:
            return ActivationOperationsView(
                dimension=_unavailable("Activation history is not configured"),
                activations=ActivationPage(activations=()),
            )
        try:
            return ActivationOperationsView(
                dimension=_available(),
                activations=store.list_activations(scope, limit=limit, cursor=cursor),
            )
        except ActivationError as exc:
            raise OperationsQueryError(
                OperationsQueryErrorCode.UNAVAILABLE,
                "Activation history is unavailable",
            ) from exc

    async def list_triggers(self, scope: RuntimeScope) -> TriggerOperationsView:
        source = self._triggers
        if source is None:
            return TriggerOperationsView(
                dimension=_unavailable("Declared triggers are not configured"),
                triggers=(),
            )
        declarations = await run_blocking(source.read, scope)
        if len(declarations.triggers) > MAX_OPERATIONS_TRIGGER_ENTRIES:
            raise OperationsQueryError(
                OperationsQueryErrorCode.UNAVAILABLE,
                "Declared trigger inventory exceeds its response bound",
            )
        schedule_declarations = declarations.schedules
        observed_schedules: tuple[ManagedScheduleDescription, ...] = ()
        if schedule_declarations:
            schedules = self._schedules
            if schedules is None:
                return TriggerOperationsView(
                    dimension=_unavailable("Schedule-trigger state is not configured"),
                    triggers=(),
                )
            try:
                observed_schedules = await schedules.list(scope)
            except ScheduleOperationError as exc:
                raise OperationsQueryError(
                    OperationsQueryErrorCode.UNAVAILABLE,
                    "Schedule-trigger state is unavailable",
                ) from exc
        schedules_by_name = {schedule.schedule_name: schedule for schedule in observed_schedules}
        inventory = tuple(
            TriggerSummary(
                name=name,
                kind=declaration.kind,
                workflow_name=declaration.workflow,
                state=(
                    TriggerOperationalState.ACTIVE
                    if not declaration.paused
                    and (
                        declaration.kind is not TriggerKind.SCHEDULE
                        or (
                            (schedule := schedules_by_name.get(name)) is not None
                            and not schedule.paused
                        )
                    )
                    else TriggerOperationalState.INACTIVE
                ),
                schedule=schedules_by_name.get(name),
            )
            for name, declaration in sorted(declarations.triggers.items())
        )
        return TriggerOperationsView(
            dimension=_available(),
            triggers=inventory,
        )

    @staticmethod
    def configuration_schema() -> ConfigurationSchema:
        schema = load_bundled_schemas()[TENANT_CONFIGURATION_SCHEMA_FILE]
        if len(json.dumps(schema, separators=(",", ":")).encode("utf-8")) > (
            MAX_SCHEMA_DOCUMENT_BYTES
        ):
            raise OperationsQueryError(
                OperationsQueryErrorCode.UNAVAILABLE,
                "Configuration schema exceeds its response bound",
            )
        return ConfigurationSchema(schema_document=schema)


def _configured(value: object | None, label: str) -> OperationsDimension:
    return _available() if value is not None else _unavailable(f"{label} is not configured")


def _available() -> OperationsDimension:
    return OperationsDimension(availability=OperationsAvailability.AVAILABLE)


def _unavailable(reason: str) -> OperationsDimension:
    return OperationsDimension(
        availability=OperationsAvailability.UNAVAILABLE,
        reason=reason,
    )


def _validate_limit(limit: int) -> None:
    if not 1 <= limit <= MAX_CONTROL_LIST_LIMIT:
        raise OperationsQueryError(
            OperationsQueryErrorCode.INVALID_QUERY,
            f"Operations query limit must be within {MAX_CONTROL_LIST_LIMIT}",
        )


def _workflow_children(
    manifests: Mapping[tuple[str, str], DefinitionManifest],
    root: DefinitionManifest,
) -> dict[str, WorkflowConfig]:
    children: dict[str, WorkflowConfig] = {}
    pending = list(root.children)
    while pending:
        child = pending.pop()
        manifest = manifests.get((child.workflow, child.definition_digest))
        if manifest is None:
            raise ValueError("Child workflow definition is unavailable")
        if child.workflow in children:
            continue
        children[child.workflow] = WorkflowConfig.model_validate(manifest.workflow)
        pending.extend(manifest.children)
        if len(children) > MAX_CONTROL_LIST_LIMIT:
            raise ValueError("Workflow child graph exceeds its operational response bound")
    return children


def _step_bindings(workflow: WorkflowConfig) -> tuple[WorkflowStepBinding, ...]:
    bindings = []
    for operation, step in sorted(workflow.steps.items()):
        resources = set(step.required_resources)
        if step.cache is not None:
            resources.add(step.cache.resource)
        service = None
        action = None
        subworkflow = None
        if isinstance(step.target, ServiceOperationTarget):
            service = step.target.service
            action = step.target.action
        else:
            subworkflow = step.target.workflow
        bindings.append(
            WorkflowStepBinding(
                operation=operation,
                service=service,
                action=action,
                subworkflow=subworkflow,
                resources=tuple(sorted(resources)),
            )
        )
    return tuple(bindings)


def _archival_binding(workflow: WorkflowConfig) -> WorkflowArchivalBinding | None:
    if workflow.on_complete is None:
        return None
    return WorkflowArchivalBinding(
        resource=workflow.on_complete.resource,
        retention_policy=workflow.on_complete.retention_policy,
    )


def _workflow_dependencies(
    workflow: WorkflowConfig,
) -> tuple[tuple[WorkflowServiceDependency, ...], tuple[str, ...]]:
    services: dict[str, set[str]] = {}
    resources: set[str] = set()
    for step in workflow.steps.values():
        if isinstance(step.target, ServiceOperationTarget):
            actions = services.setdefault(step.target.service, set())
            actions.add(step.target.action)
        resources.update(step.required_resources)
        if step.cache is not None:
            resources.add(step.cache.resource)
    for flow_step in workflow.flow:
        for branch in flow_step.on_result or []:
            if isinstance(branch.when, EvaluatorCondition):
                resources.update(branch.when.resources)
    return (
        tuple(
            WorkflowServiceDependency(service=service, actions=tuple(sorted(actions)))
            for service, actions in sorted(services.items())
        ),
        tuple(sorted(resources)),
    )


def graph_node_label(node_id: str, node_type: str) -> str:
    if node_type == "decision" and node_id.endswith("_check"):
        return node_id.removesuffix("_check")
    return node_id


def graph_edge_label(label: str | None) -> str | None:
    if label is None:
        return None
    safe_labels = {
        "yes",
        "skip",
        "on failure",
        "timeout",
        "exhausted",
        "calls",
        "per item",
        "default",
    }
    return label if label in safe_labels else "condition"


def _page(
    scope: RuntimeScope,
    values: tuple[PageItem, ...],
    *,
    kind: str,
    snapshot: str | None,
    limit: int,
    cursor: str | None,
) -> tuple[tuple[PageItem, ...], str | None]:
    offset = _page_offset(scope, kind=kind, snapshot=snapshot, cursor=cursor)
    selected = values[offset : offset + limit]
    next_offset = offset + len(selected)
    next_cursor = (
        encode_scope_cursor(
            scope,
            {"kind": kind, "offset": next_offset, "snapshot": snapshot},
        )
        if next_offset < len(values)
        else None
    )
    return selected, next_cursor


def _page_offset(
    scope: RuntimeScope,
    *,
    kind: str,
    snapshot: str | None,
    cursor: str | None,
) -> int:
    if cursor is None:
        return 0
    try:
        position = decode_scope_cursor(scope, cursor)
    except ValueError as exc:
        raise OperationsQueryError(
            OperationsQueryErrorCode.INVALID_QUERY,
            "Operations continuation cursor is invalid",
        ) from exc
    if (
        not isinstance(position, dict)
        or set(position) != {"kind", "offset", "snapshot"}
        or position["kind"] != kind
        or not isinstance(position["offset"], int)
        or position["offset"] < 0
    ):
        raise OperationsQueryError(
            OperationsQueryErrorCode.INVALID_QUERY,
            "Operations continuation cursor is invalid",
        )
    if position["snapshot"] != snapshot:
        raise OperationsQueryError(
            OperationsQueryErrorCode.STALE_CURSOR,
            "Operations data changed after the continuation cursor was issued",
        )
    return position["offset"]
