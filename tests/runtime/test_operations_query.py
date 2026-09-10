"""Tests for the scope-preserving operational read facade."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from http import HTTPStatus
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock, MagicMock, call

import pytest

from justflow.config.models import FlowStep, ResourcesConfig, ServicesConfig, WorkflowConfig
from justflow.config.runtime_limits import RuntimeLimits
from justflow.config.settings import (
    ControlSettings,
    OperationsMetricsLinkSettings,
    ScheduledStartSettings,
    ScheduledStartWorkloadClass,
)
from justflow.config.triggers import TriggerKind, TriggersConfig
from justflow.configuration.activation import ActivationPage
from justflow.configuration.models import (
    ComponentReference,
    ComponentReplayContract,
    ComponentRuntimeIdentity,
    PlatformComponentCatalog,
    PlatformStepComponent,
    PlatformTriggerComponent,
    RevisionPage,
)
from justflow.configuration.policy import (
    ResourceAuthoringGrant,
    ServiceAuthoringGrant,
    TemporalIsolationMode,
    TemporalIsolationPolicy,
    TenantAuthoringPolicy,
    TriggerBindingGrant,
)
from justflow.configuration.publication import StaticTenantAuthoringPolicySource
from justflow.definitions.catalog import (
    CatalogError,
    CatalogState,
    DefinitionCatalog,
    DefinitionCatalogStore,
)
from justflow.definitions.manifest import DefinitionManifest, build_definition_manifests
from justflow.resources.base import ResourceCapability
from justflow.resources.builtins import builtin_resource_registry
from justflow.runtime.health import HealthRegistry
from justflow.runtime.operations import (
    ControlErrorCode,
    ControlOperationError,
    WorkflowControlService,
    WorkflowExecutionState,
    WorkflowListQuery,
    WorkflowListResult,
)
from justflow.runtime.operations_api import (
    OperationsApi,
    OperationsApiRequestError,
    OperationsRouteKind,
)
from justflow.runtime.operations_query import (
    AuthoringReferenceAuthority,
    AuthoringReferenceSource,
    CatalogAuthoringReferenceSource,
    OperationsAvailability,
    OperationsQueryError,
    OperationsQueryErrorCode,
    OperationsQueryService,
    ScopedDeclaredTriggerSource,
    ScopedScheduleOperationsSource,
    _archival_binding,
    _step_bindings,
    _workflow_dependencies,
    build_authoring_reference,
)
from justflow.runtime.schedule_operations import ScheduleOperationError, ScheduleOperationErrorCode
from justflow.runtime.scheduled_start_service import ScheduledStartService
from justflow.runtime.scheduled_starts import (
    ScheduledStartDescription,
    ScheduledStartPage,
    ScheduledStartState,
)
from justflow.scope import (
    RuntimeScope,
    ScopeBindingKind,
    TrustedScopeBinding,
    encode_scope_cursor,
)

SCOPE_A = RuntimeScope.create(
    tenant="tenant-a",
    application="orders",
    environment="production",
)
SCOPE_B = RuntimeScope.create(
    tenant="tenant-b",
    application="orders",
    environment="production",
)
COMPONENT_ARTIFACT_DIGEST = f"sha256:{'c' * 64}"
COMPONENT_RETIREMENT_IDENTITY = f"sha256:{'d' * 64}"


class ExactComponentCatalogSource:
    def __init__(self, catalog: PlatformComponentCatalog) -> None:
        self.catalog = catalog
        self.revisions: list[str] = []

    def read(self, revision_id: str) -> PlatformComponentCatalog:
        self.revisions.append(revision_id)
        return self.catalog


def component_catalog_and_policy() -> tuple[PlatformComponentCatalog, TenantAuthoringPolicy]:
    step_reference = ComponentReference(name="notifications", version="1.2.0")
    unapproved_reference = ComponentReference(name="unapproved", version="1.0.0")
    trigger_reference = ComponentReference(name="scheduled", version="2.0.0")
    runtime = ComponentRuntimeIdentity(
        implementation="private-runtime",
        version="9.0.0",
        artifact_digest=COMPONENT_ARTIFACT_DIGEST,
    )
    replay = ComponentReplayContract(
        deterministic=True,
        retirement_identity=COMPONENT_RETIREMENT_IDENTITY,
    )
    catalog = PlatformComponentCatalog.create(
        steps={
            step_reference.identity: PlatformStepComponent(
                reference=step_reference,
                description="Send an approved notification.",
                runtime_implementation=runtime,
                replay=replay,
                transport="private-https-transport",
                action="send",
                parameter_schema={"type": "object"},
                input_schema={"type": "object"},
                output_schema={"type": "object"},
                resource_slots={"cache": ResourceCapability.CACHE},
            ),
            unapproved_reference.identity: PlatformStepComponent(
                reference=unapproved_reference,
                description="Must not be visible to this scope.",
                runtime_implementation=runtime,
                replay=replay,
                transport="private-transport",
                action="hidden",
                parameter_schema={},
                input_schema={},
                output_schema={},
            ),
        },
        triggers={
            trigger_reference.identity: PlatformTriggerComponent(
                reference=trigger_reference,
                description="Start from an approved schedule.",
                runtime_implementation=runtime,
                replay=replay,
                kind=TriggerKind.SCHEDULE,
                parameter_schema={"type": "object"},
                output_schema={"type": "object"},
            )
        },
    )
    policy = TenantAuthoringPolicy(
        scope_digest=SCOPE_A.digest,
        component_catalog_revision=catalog.revision_id,
        step_components=frozenset({step_reference}),
        trigger_components=frozenset({trigger_reference}),
        resource_bindings={
            "workflow_cache": ResourceAuthoringGrant(
                provider="private-provider",
                capabilities=frozenset({ResourceCapability.CACHE}),
            )
        },
        service_bindings={
            "notifications": ServiceAuthoringGrant(transport="private-https-transport")
        },
        trigger_bindings={"hourly": TriggerBindingGrant(kind=TriggerKind.SCHEDULE)},
        temporal=TemporalIsolationPolicy.for_scope(
            SCOPE_A,
            mode=TemporalIsolationMode.SHARED,
            namespace="private-namespace",
            task_queue="private-task-queue",
            worker_deployment="private-worker-deployment",
            storage_prefix="private-storage-prefix",
        ),
    )
    return catalog, policy


class CatalogSource:
    def __init__(self) -> None:
        manifests = tuple(
            cast(
                DefinitionManifest,
                SimpleNamespace(
                    logical_name=name,
                    definition_digest=digest,
                    required_engine_workflow_abi="justflow.workflow.v1",
                ),
            )
            for name, digest in (("alpha", "a" * 64), ("beta", "b" * 64))
        )
        self.state = CatalogState(
            catalog=DefinitionCatalog(
                manifests,
                {manifest.logical_name: manifest.definition_digest for manifest in manifests},
            ),
            alias_version="catalog-v1",
        )

    def read(self, scope: RuntimeScope) -> DefinitionCatalogStore:
        del scope
        return cast(DefinitionCatalogStore, self)

    def inspect(self) -> CatalogState:
        return self.state


class FailingCatalogSource(CatalogSource):
    def inspect(self) -> CatalogState:
        raise CatalogError("synthetic-storage-secret")


class WorkflowCatalogSource(CatalogSource):
    def __init__(self) -> None:
        workflow = WorkflowConfig(
            workflow="example",
            description="Bounded workflow detail",
            steps={},
            flow=[FlowStep(name="done", terminal=True)],
        )
        manifest = build_definition_manifests(
            {workflow.workflow: workflow},
            {},
            RuntimeLimits(),
        )[workflow.workflow]
        self.state = CatalogState(
            catalog=DefinitionCatalog(
                (manifest,),
                {manifest.logical_name: manifest.definition_digest},
            ),
            alias_version="catalog-v1",
        )


def query_service(
    *,
    controls: WorkflowControlService | None = None,
    catalogs: CatalogSource | None = None,
    configuration_store: object | None = None,
    activation_store: object | None = None,
    triggers: object | None = None,
    schedules: object | None = None,
    authoring_reference_source: AuthoringReferenceSource | None = None,
) -> OperationsQueryService:
    execution_service = controls or cast(WorkflowControlService, MagicMock())
    return OperationsQueryService(
        executions=execution_service,
        health=HealthRegistry(frozenset()),
        definition_catalogs=catalogs,
        configuration_store=configuration_store,
        activation_store=activation_store,
        triggers=triggers,
        schedules=schedules,
        authoring_reference_source=authoring_reference_source,
        metrics_links=(
            OperationsMetricsLinkSettings(
                label="Metrics",
                url="https://metrics.example.invalid/justflow",
            ),
        ),
    )


def test_overview_reports_availability_without_inventing_data() -> None:
    overview = query_service().overview()

    assert overview.executions.availability is OperationsAvailability.AVAILABLE
    assert overview.definitions.availability is OperationsAvailability.UNAVAILABLE
    assert overview.configuration.availability is OperationsAvailability.UNAVAILABLE
    assert overview.activations.availability is OperationsAvailability.UNAVAILABLE
    assert overview.triggers.availability is OperationsAvailability.UNAVAILABLE
    assert overview.metrics_links[0].url == "https://metrics.example.invalid/justflow"


async def test_execution_queries_pass_the_trusted_scope_to_the_provider() -> None:
    controls = MagicMock(spec=WorkflowControlService)
    controls.list = AsyncMock(return_value=WorkflowListResult(workflows=()))
    controls.describe = AsyncMock()
    service = query_service(controls=controls)
    filters = WorkflowListQuery(state="failed", scope="current")

    await service.list_runs(SCOPE_A, limit=7, cursor="cursor", query=filters)
    await service.describe_run(SCOPE_A, "workflow-1", run_id="run-1")

    controls.list.assert_awaited_once_with(
        scope=SCOPE_A,
        limit=7,
        page_token="cursor",
        query=filters,
    )
    controls.describe.assert_awaited_once_with(
        "workflow-1",
        run_id="run-1",
        scope=SCOPE_A,
    )


def test_definition_pages_are_bounded_scope_bound_and_snapshot_bound() -> None:
    catalogs = CatalogSource()
    service = query_service(catalogs=catalogs)

    first = service.list_definitions(SCOPE_A, limit=1, cursor=None)
    assert [item.logical_workflow for item in first.definitions] == ["alpha"]
    assert first.next_cursor is not None

    second = service.list_definitions(SCOPE_A, limit=1, cursor=first.next_cursor)
    assert [item.logical_workflow for item in second.definitions] == ["beta"]
    workflows = service.list_workflows(SCOPE_A, limit=1, cursor=None)
    assert [item.logical_workflow for item in workflows.workflows] == ["alpha"]
    assert workflows.next_cursor is not None

    with pytest.raises(OperationsQueryError) as foreign:
        service.list_definitions(SCOPE_B, limit=1, cursor=first.next_cursor)
    assert foreign.value.code is OperationsQueryErrorCode.INVALID_QUERY

    catalogs.state = CatalogState(
        catalog=catalogs.state.catalog,
        alias_version="catalog-v2",
    )
    with pytest.raises(OperationsQueryError) as stale:
        service.list_definitions(SCOPE_A, limit=1, cursor=first.next_cursor)
    assert stale.value.code is OperationsQueryErrorCode.STALE_CURSOR


def test_catalog_errors_are_layered_without_provider_details() -> None:
    with pytest.raises(OperationsQueryError) as raised:
        query_service(catalogs=FailingCatalogSource()).list_workflows(
            SCOPE_A,
            limit=10,
            cursor=None,
        )

    assert raised.value.code is OperationsQueryErrorCode.UNAVAILABLE
    assert "synthetic-storage-secret" not in str(raised.value)


def test_workflow_detail_uses_only_the_active_immutable_manifest() -> None:
    detail = query_service(catalogs=WorkflowCatalogSource()).workflow_detail(SCOPE_A, "example")

    assert detail.logical_workflow == "example"
    assert detail.description == "Bounded workflow detail"
    assert [(node.node_id, node.kind) for node in detail.graph_nodes] == [("done", "terminal")]
    assert detail.graph_edges == ()
    assert detail.retained_definition_count == 1
    assert detail.definitions[0].active is True

    with pytest.raises(OperationsQueryError) as missing:
        query_service(catalogs=WorkflowCatalogSource()).workflow_detail(SCOPE_A, "missing")
    assert missing.value.code is OperationsQueryErrorCode.NOT_FOUND


def test_workflow_definition_projects_only_the_declaration_document() -> None:
    service = query_service(catalogs=WorkflowCatalogSource())

    definition = service.workflow_definition(SCOPE_A, "example")

    assert definition.logical_workflow == "example"
    assert len(definition.definition_digest) == 64
    assert "workflow: example" in definition.document
    assert "description: Bounded workflow detail" in definition.document
    assert "transport" not in definition.document
    assert "config" not in definition.document

    with pytest.raises(OperationsQueryError) as missing:
        service.workflow_definition(SCOPE_A, "missing")
    assert missing.value.code is OperationsQueryErrorCode.NOT_FOUND

    with pytest.raises(OperationsQueryError) as unavailable:
        query_service().workflow_definition(SCOPE_A, "example")
    assert unavailable.value.code is OperationsQueryErrorCode.UNAVAILABLE


def test_authoring_reference_projects_names_and_capabilities_only() -> None:
    reference = build_authoring_reference(
        services=ServicesConfig.model_validate(
            {
                "services": {
                    "notifications": {
                        "transport": "https",
                        "dispatch_timeout_sec": 5,
                        "retries": 0,
                    },
                    "billing": {"transport": "https", "dispatch_timeout_sec": 5, "retries": 0},
                }
            }
        ),
        resources=ResourcesConfig.model_validate(
            {
                "resources": {
                    "workflow_cache": {"provider": "memory_cache", "config": {}},
                    "custom": {"class": "tests.workflow_fixtures.resources.Custom", "config": {}},
                }
            }
        ),
        resource_registry=builtin_resource_registry(),
    )

    assert [service.name for service in reference.services] == ["billing", "notifications"]
    assert [resource.name for resource in reference.resources] == ["custom", "workflow_cache"]
    cache = reference.resources[1]
    assert cache.capabilities
    assert all(isinstance(capability, str) for capability in cache.capabilities)
    assert reference.resources[0].capabilities == ()
    assert reference.authority is AuthoringReferenceAuthority.OBSERVED_HOST
    assert reference.dimension.availability is OperationsAvailability.AVAILABLE
    assert reference.truncated is False
    dumped = reference.model_dump_json()
    assert "transport" not in dumped
    assert "https" not in dumped
    assert "class" not in dumped

    unavailable = query_service().authoring_reference(SCOPE_A)
    assert unavailable.dimension.availability is OperationsAvailability.UNAVAILABLE


def test_managed_authoring_reference_projects_only_exact_policy_approved_contracts() -> None:
    catalog, policy = component_catalog_and_policy()
    catalog_source = ExactComponentCatalogSource(catalog)
    source = CatalogAuthoringReferenceSource(
        component_catalog_source=catalog_source,
        policy_source=StaticTenantAuthoringPolicySource({SCOPE_A.digest: policy}),
    )

    reference = source.read(SCOPE_A)

    assert reference.dimension.availability is OperationsAvailability.AVAILABLE
    assert reference.authority is AuthoringReferenceAuthority.IMMUTABLE_CATALOG
    assert reference.catalog_revision == catalog.revision_id
    assert catalog_source.revisions == [catalog.revision_id]
    assert [(component.name, component.version) for component in reference.components] == [
        ("notifications", "1.2.0"),
        ("scheduled", "2.0.0"),
    ]
    step = reference.components[0]
    assert step.description == "Send an approved notification."
    assert step.action == "send"
    assert step.parameter_schema == {"type": "object"}
    assert step.parameter_contract_identity is not None
    assert [(slot.name, slot.capability) for slot in step.resource_slots] == [("cache", "cache")]
    assert [(resource.name, resource.capabilities) for resource in reference.resources] == [
        ("workflow_cache", ("cache",))
    ]
    assert [(binding.name, binding.kind) for binding in reference.trigger_bindings] == [
        ("hourly", "schedule")
    ]
    dumped = reference.model_dump_json()
    for forbidden in (
        "runtime_implementation",
        "private-runtime",
        "private-provider",
        "private-https-transport",
        "private-namespace",
        "private-task-queue",
        "artifact_digest",
        "retirement_identity",
        "scope_digest",
        "unapproved",
        "Must not be visible",
    ):
        assert forbidden not in dumped


def test_managed_authoring_reference_is_scope_isolated_and_explicitly_unavailable() -> None:
    catalog, policy = component_catalog_and_policy()
    source = CatalogAuthoringReferenceSource(
        component_catalog_source=ExactComponentCatalogSource(catalog),
        policy_source=StaticTenantAuthoringPolicySource({SCOPE_A.digest: policy}),
    )

    reference = source.read(SCOPE_B)

    assert reference.dimension.availability is OperationsAvailability.UNAVAILABLE
    assert reference.dimension.reason == (
        "Managed component catalog or authoring policy is unavailable"
    )
    assert reference.components == ()
    assert reference.services == ()
    assert reference.resources == ()


def test_workflow_dependencies_project_services_resources_and_contracts() -> None:
    workflow = WorkflowConfig.model_validate(
        {
            "workflow": "orders",
            "input_schema": {"type": "object"},
            "steps": {
                "charge": {
                    "service": "billing",
                    "action": "charge",
                    "required_resources": ["ledger"],
                },
                "notify": {
                    "service": "notifications",
                    "action": "send",
                    "cache": {"resource": "workflow_cache", "key": "notify"},
                },
            },
            "flow": [
                {
                    "name": "charge",
                    "op": "charge",
                    "on_result": [
                        {
                            "when": {"evaluator": "checks.approve", "resources": ["approvals"]},
                            "then": "notify",
                        },
                        {"default": "done"},
                    ],
                },
                {"name": "notify", "op": "notify", "then": "done"},
                {"name": "done", "terminal": True},
            ],
        }
    )

    services, resources = _workflow_dependencies(workflow)

    assert [(entry.service, entry.actions) for entry in services] == [
        ("billing", ("charge",)),
        ("notifications", ("send",)),
    ]
    assert resources == ("approvals", "ledger", "workflow_cache")


def test_step_bindings_and_archival_project_per_operation_facts() -> None:
    workflow = WorkflowConfig.model_validate(
        {
            "workflow": "orders",
            "on_complete": {
                "resource": "audit_store",
                "path": "audit/${request_id}.json",
                "retention_policy": "local-demo",
            },
            "steps": {
                "charge": {
                    "service": "billing",
                    "action": "charge",
                    "required_resources": ["ledger"],
                    "cache": {"resource": "workflow_cache", "key": "charge"},
                },
            },
            "flow": [
                {"name": "charge", "op": "charge", "then": "done"},
                {"name": "done", "terminal": True},
            ],
        }
    )

    bindings = _step_bindings(workflow)
    archival = _archival_binding(workflow)

    assert [
        (entry.operation, entry.service, entry.action, entry.resources) for entry in bindings
    ] == [("charge", "billing", "charge", ("ledger", "workflow_cache"))]
    assert archival is not None
    assert (archival.resource, archival.retention_policy) == ("audit_store", "local-demo")


def test_archival_binding_is_absent_without_an_on_complete_declaration() -> None:
    workflow = WorkflowConfig.model_validate(
        {"workflow": "orders", "steps": {}, "flow": [{"name": "done", "terminal": True}]}
    )

    assert _archival_binding(workflow) is None


def test_configuration_and_activation_reads_keep_provider_scope_and_bounds() -> None:
    configuration_store = MagicMock()
    configuration_store.read_active.return_value = None
    configuration_store.list_revisions.return_value = RevisionPage(revisions=())
    activation_store = MagicMock()
    activation_store.list_activations.return_value = ActivationPage(activations=())
    service = query_service(
        configuration_store=configuration_store,
        activation_store=activation_store,
    )

    configuration = service.configuration(SCOPE_A, limit=5, cursor="configuration-cursor")
    activations = service.activations(SCOPE_A, limit=6, cursor="activation-cursor")

    assert configuration.dimension.availability is OperationsAvailability.AVAILABLE
    assert activations.dimension.availability is OperationsAvailability.AVAILABLE
    configuration_store.read_active.assert_called_once_with(SCOPE_A)
    configuration_store.list_revisions.assert_called_once_with(
        SCOPE_A,
        limit=5,
        cursor="configuration-cursor",
    )
    activation_store.list_activations.assert_called_once_with(
        SCOPE_A,
        limit=6,
        cursor="activation-cursor",
    )


async def test_schedule_operations_resolve_only_the_exact_trusted_scope() -> None:
    operator = MagicMock()
    operator.list = AsyncMock(return_value=())
    source = ScopedScheduleOperationsSource({SCOPE_A: operator})

    assert await source.list(SCOPE_A) == ()
    operator.list.assert_awaited_once_with()

    with pytest.raises(OperationsQueryError) as foreign:
        await source.list(SCOPE_B)
    assert foreign.value.code is OperationsQueryErrorCode.UNAVAILABLE


async def test_trigger_view_reports_missing_and_failed_providers_explicitly() -> None:
    missing = await query_service().list_triggers(SCOPE_A)
    assert missing.dimension.availability is OperationsAvailability.UNAVAILABLE

    declarations = TriggersConfig.model_validate(
        {
            "triggers": {
                "daily": {
                    "kind": "schedule",
                    "workflow": "example",
                    "spec": {"kind": "cron", "expressions": ["0 9 * * *"]},
                }
            }
        }
    )
    operator = MagicMock()
    operator.list = AsyncMock(
        side_effect=ScheduleOperationError(
            ScheduleOperationErrorCode.TEMPORAL_UNAVAILABLE,
            "synthetic-provider-secret",
        )
    )
    source = ScopedScheduleOperationsSource({SCOPE_A: operator})
    with pytest.raises(OperationsQueryError) as failed:
        await query_service(
            triggers=ScopedDeclaredTriggerSource({SCOPE_A: declarations}),
            schedules=source,
        ).list_triggers(SCOPE_A)
    assert failed.value.code is OperationsQueryErrorCode.UNAVAILABLE
    assert "synthetic-provider-secret" not in str(failed.value)


async def test_trigger_inventory_includes_every_declared_kind_with_binary_state() -> None:
    declarations = TriggersConfig.model_validate(
        {
            "triggers": {
                "api": {"kind": "api", "workflow": "example"},
                "daily": {
                    "kind": "schedule",
                    "workflow": "example",
                    "spec": {"kind": "cron", "expressions": ["0 9 * * *"]},
                },
                "webhook": {
                    "kind": "webhook",
                    "workflow": "example",
                    "source": "orders",
                    "paused": True,
                },
                "event": {
                    "kind": "event",
                    "workflow": "example",
                    "mapping": "order_events",
                },
                "broker": {
                    "kind": "broker",
                    "workflow": "example",
                    "broker": "commands",
                },
                "host": {
                    "kind": "host",
                    "workflow": "example",
                    "adapter": "console",
                },
            }
        }
    )
    operator = MagicMock()
    operator.list = AsyncMock(return_value=())

    view = await query_service(
        triggers=ScopedDeclaredTriggerSource({SCOPE_A: declarations}),
        schedules=ScopedScheduleOperationsSource({SCOPE_A: operator}),
    ).list_triggers(SCOPE_A)

    assert [
        (trigger.name, trigger.kind.value, trigger.state.value) for trigger in view.triggers
    ] == [
        ("api", "api", "active"),
        ("broker", "broker", "active"),
        ("daily", "schedule", "inactive"),
        ("event", "event", "active"),
        ("host", "host", "active"),
        ("webhook", "webhook", "inactive"),
    ]


@pytest.mark.parametrize(
    "cursor",
    [
        pytest.param("invalid", id="invalid-encoding"),
        pytest.param(
            encode_scope_cursor(
                SCOPE_A,
                {"kind": "workflows", "offset": 1, "snapshot": "catalog-v1"},
            ),
            id="wrong-kind",
        ),
        pytest.param(
            encode_scope_cursor(
                SCOPE_A,
                {"kind": "definitions", "offset": -1, "snapshot": "catalog-v1"},
            ),
            id="negative-offset",
        ),
    ],
)
def test_definition_pages_reject_invalid_continuation_cursors(cursor: str) -> None:
    with pytest.raises(OperationsQueryError) as raised:
        query_service(catalogs=CatalogSource()).list_definitions(
            SCOPE_A,
            limit=1,
            cursor=cursor,
        )
    assert raised.value.code is OperationsQueryErrorCode.INVALID_QUERY


def test_operations_queries_reject_out_of_range_page_sizes() -> None:
    with pytest.raises(OperationsQueryError) as raised:
        query_service(catalogs=CatalogSource()).list_definitions(
            SCOPE_A,
            limit=0,
            cursor=None,
        )
    assert raised.value.code is OperationsQueryErrorCode.INVALID_QUERY


@dataclass(frozen=True, kw_only=True)
class RouteCase:
    id: str
    segments: tuple[str, ...]
    kind: OperationsRouteKind


ROUTE_CASES = [
    RouteCase(id="overview", segments=("v1", "operations"), kind=OperationsRouteKind.OVERVIEW),
    RouteCase(
        id="workflows",
        segments=("v1", "operations", "workflows"),
        kind=OperationsRouteKind.WORKFLOWS,
    ),
    RouteCase(
        id="workflow-detail",
        segments=("v1", "operations", "workflows", "example"),
        kind=OperationsRouteKind.WORKFLOW_DETAIL,
    ),
    RouteCase(
        id="definitions",
        segments=("v1", "operations", "definitions"),
        kind=OperationsRouteKind.DEFINITIONS,
    ),
    RouteCase(id="runs", segments=("v1", "operations", "runs"), kind=OperationsRouteKind.RUNS),
    RouteCase(
        id="run-detail",
        segments=("v1", "operations", "runs", "workflow-1"),
        kind=OperationsRouteKind.RUN_DETAIL,
    ),
    RouteCase(
        id="triggers",
        segments=("v1", "operations", "triggers"),
        kind=OperationsRouteKind.TRIGGERS,
    ),
    RouteCase(
        id="configuration",
        segments=("v1", "operations", "configuration"),
        kind=OperationsRouteKind.CONFIGURATION,
    ),
    RouteCase(
        id="activations",
        segments=("v1", "operations", "activations"),
        kind=OperationsRouteKind.ACTIVATIONS,
    ),
    RouteCase(
        id="configuration-schema",
        segments=("v1", "operations", "configuration-schema"),
        kind=OperationsRouteKind.CONFIGURATION_SCHEMA,
    ),
    RouteCase(
        id="scheduled-starts",
        segments=("v1", "operations", "scheduled-starts"),
        kind=OperationsRouteKind.SCHEDULED_STARTS,
    ),
    RouteCase(
        id="scheduled-start-detail",
        segments=("v1", "operations", "scheduled-starts", "opaque-id"),
        kind=OperationsRouteKind.SCHEDULED_START_DETAIL,
    ),
]


@pytest.mark.parametrize("case", ROUTE_CASES, ids=lambda case: case.id)
def test_operations_api_has_a_closed_read_only_route_set(case: RouteCase) -> None:
    api = OperationsApi(settings=ControlSettings(), query_service=query_service())

    route = api.resolve_route("GET", case.segments)

    assert route.kind is case.kind


@dataclass(frozen=True, kw_only=True)
class RouteRejectionCase:
    id: str
    method: str
    segments: tuple[str, ...]
    expected_status: HTTPStatus
    expected_code: str


ROUTE_REJECTION_CASES = [
    RouteRejectionCase(
        id="segment-bound",
        method="GET",
        segments=("v1", "operations", "runs", "workflow", "run", "extra"),
        expected_status=HTTPStatus.REQUEST_URI_TOO_LONG,
        expected_code="path_too_large",
    ),
    RouteRejectionCase(
        id="read-only",
        method="POST",
        segments=("v1", "operations"),
        expected_status=HTTPStatus.NOT_FOUND,
        expected_code="not_found",
    ),
    RouteRejectionCase(
        id="closed-route-set",
        method="GET",
        segments=("v1", "operations", "unknown"),
        expected_status=HTTPStatus.NOT_FOUND,
        expected_code="not_found",
    ),
    RouteRejectionCase(
        id="legacy-schedule-route",
        method="GET",
        segments=("v1", "operations", "schedules"),
        expected_status=HTTPStatus.NOT_FOUND,
        expected_code="not_found",
    ),
]


@pytest.mark.parametrize("case", ROUTE_REJECTION_CASES, ids=lambda case: case.id)
def test_operations_api_rejects_unbounded_or_unknown_routes(case: RouteRejectionCase) -> None:
    api = OperationsApi(settings=ControlSettings(), query_service=query_service())

    with pytest.raises(OperationsApiRequestError) as raised:
        api.resolve_route(case.method, case.segments)

    assert raised.value.status is case.expected_status
    assert raised.value.code == case.expected_code


@dataclass(frozen=True, kw_only=True)
class DispatchCase:
    id: str
    segments: tuple[str, ...]
    query: tuple[tuple[str, tuple[str, ...]], ...]
    service_method: str
    expected_args: tuple[object, ...]
    expected_kwargs: tuple[tuple[str, object], ...]
    asynchronous: bool = False


DISPATCH_CASES = [
    DispatchCase(
        id="overview",
        segments=("v1", "operations"),
        query=(),
        service_method="overview",
        expected_args=(),
        expected_kwargs=(),
    ),
    DispatchCase(
        id="workflows",
        segments=("v1", "operations", "workflows"),
        query=(("limit", ("3",)),),
        service_method="list_workflows",
        expected_args=(SCOPE_A,),
        expected_kwargs=(("limit", 3), ("cursor", None)),
    ),
    DispatchCase(
        id="workflow-detail",
        segments=("v1", "operations", "workflows", "example"),
        query=(),
        service_method="workflow_detail",
        expected_args=(SCOPE_A, "example"),
        expected_kwargs=(),
    ),
    DispatchCase(
        id="definitions",
        segments=("v1", "operations", "definitions"),
        query=(("limit", ("3",)),),
        service_method="list_definitions",
        expected_args=(SCOPE_A,),
        expected_kwargs=(("limit", 3), ("cursor", None)),
    ),
    DispatchCase(
        id="runs",
        segments=("v1", "operations", "runs"),
        query=(("limit", ("3",)), ("state", ("failed",)), ("scope", ("current",))),
        service_method="list_runs",
        expected_args=(SCOPE_A,),
        expected_kwargs=(
            ("limit", 3),
            ("cursor", None),
            (
                "query",
                WorkflowListQuery(
                    state=WorkflowExecutionState.FAILED,
                    scope="current",
                ),
            ),
        ),
        asynchronous=True,
    ),
    DispatchCase(
        id="run-detail",
        segments=("v1", "operations", "runs", "workflow-1"),
        query=(("run_id", ("run-1",)),),
        service_method="describe_run",
        expected_args=(SCOPE_A, "workflow-1"),
        expected_kwargs=(("run_id", "run-1"),),
        asynchronous=True,
    ),
    DispatchCase(
        id="triggers",
        segments=("v1", "operations", "triggers"),
        query=(),
        service_method="list_triggers",
        expected_args=(SCOPE_A,),
        expected_kwargs=(),
        asynchronous=True,
    ),
    DispatchCase(
        id="configuration",
        segments=("v1", "operations", "configuration"),
        query=(("limit", ("3",)),),
        service_method="configuration",
        expected_args=(SCOPE_A,),
        expected_kwargs=(("limit", 3), ("cursor", None)),
    ),
    DispatchCase(
        id="activations",
        segments=("v1", "operations", "activations"),
        query=(("limit", ("3",)),),
        service_method="activations",
        expected_args=(SCOPE_A,),
        expected_kwargs=(("limit", 3), ("cursor", None)),
    ),
    DispatchCase(
        id="configuration-schema",
        segments=("v1", "operations", "configuration-schema"),
        query=(),
        service_method="configuration_schema",
        expected_args=(),
        expected_kwargs=(),
    ),
]


@pytest.mark.parametrize("case", DISPATCH_CASES, ids=lambda case: case.id)
async def test_operations_api_dispatches_only_the_resolved_scoped_read(
    case: DispatchCase,
) -> None:
    result = MagicMock()
    result.model_dump.return_value = {"case": case.id}
    queries = MagicMock(spec=OperationsQueryService)
    operation = (
        AsyncMock(return_value=result) if case.asynchronous else MagicMock(return_value=result)
    )
    setattr(queries, case.service_method, operation)
    api = OperationsApi(settings=ControlSettings(), query_service=queries)
    route = api.resolve_route("GET", case.segments)
    binding = TrustedScopeBinding.create(
        kind=ScopeBindingKind.API,
        scope=SCOPE_A,
        binding_id="principal",
    )

    response = await api.dispatch(
        route,
        query={name: list(values) for name, values in case.query},
        scope_binding=binding,
    )

    assert response.payload == {"case": case.id}
    expected_kwargs = dict(case.expected_kwargs)
    if case.asynchronous:
        operation.assert_awaited_once_with(*case.expected_args, **expected_kwargs)
    else:
        operation.assert_called_once_with(*case.expected_args, **expected_kwargs)


async def test_operations_api_validates_filters_and_translates_unavailable_indexes() -> None:
    controls = MagicMock(spec=WorkflowControlService)
    controls.list = AsyncMock(
        side_effect=ControlOperationError(
            ControlErrorCode.FILTER_UNAVAILABLE,
            "The requested filter requires host-enabled Temporal Visibility indexes",
            retryable=False,
        )
    )
    api = OperationsApi(
        settings=ControlSettings(),
        query_service=query_service(controls=controls),
    )
    route = api.resolve_route("GET", ("v1", "operations", "runs"))
    binding = TrustedScopeBinding.create(
        kind=ScopeBindingKind.API,
        scope=SCOPE_A,
        binding_id="principal",
    )

    with pytest.raises(OperationsApiRequestError) as unavailable:
        await api.dispatch(
            route,
            query={"definition_digest": ["a" * 64], "limit": ["5"]},
            scope_binding=binding,
        )
    assert unavailable.value.status == 409
    assert unavailable.value.code == "filter_unavailable"

    with pytest.raises(OperationsApiRequestError) as invalid:
        await api.dispatch(
            route,
            query={"unbounded": ["true"]},
            scope_binding=binding,
        )
    assert invalid.value.status == 400


async def test_operations_api_lists_safe_scheduled_start_projections() -> None:
    scheduled_start = ScheduledStartDescription(
        scheduled_start_id="opaque-scheduled-start",
        workflow_name="reporting",
        trigger_name="reporting_api",
        start_at=datetime(2026, 8, 12, 8, 0, tzinfo=UTC),
        workload_class=ScheduledStartWorkloadClass.STANDARD,
        state=ScheduledStartState.SCHEDULED,
        version=1,
        accepted_at=datetime(2026, 8, 11, 8, 0, tzinfo=UTC),
        updated_at=datetime(2026, 8, 11, 8, 0, tzinfo=UTC),
    )
    service = MagicMock(spec=ScheduledStartService)
    first_page = ScheduledStartPage(
        scheduled_starts=(scheduled_start,),
        next_cursor="opaque-cursor",
    )
    final_page = ScheduledStartPage(scheduled_starts=())
    service.list = AsyncMock(side_effect=(first_page, final_page))
    api = OperationsApi(
        settings=ControlSettings(),
        query_service=query_service(),
        scheduled_start_settings=ScheduledStartSettings(),
        scheduled_starts=service,
    )
    route = api.resolve_route("GET", ("v1", "operations", "scheduled-starts"))
    binding = TrustedScopeBinding.create(
        kind=ScopeBindingKind.API,
        scope=SCOPE_A,
        binding_id="principal",
    )

    response = await api.dispatch(
        route,
        query={"limit": ["7"], "state": ["scheduled", "failed"]},
        scope_binding=binding,
    )

    assert response.payload == first_page.model_dump(mode="json")
    assert isinstance(response.payload, dict)
    cursor = response.payload["next_cursor"]
    assert isinstance(cursor, str)

    continued = await api.dispatch(
        route,
        query={
            "cursor": [cursor],
            "limit": ["7"],
            "state": ["scheduled", "failed"],
        },
        scope_binding=binding,
    )

    assert continued.payload == final_page.model_dump(mode="json")
    service.list.assert_has_awaits(
        [
            call(
                scope=SCOPE_A,
                limit=7,
                cursor=None,
                states=frozenset({ScheduledStartState.SCHEDULED, ScheduledStartState.FAILED}),
            ),
            call(
                scope=SCOPE_A,
                limit=7,
                cursor=cursor,
                states=frozenset({ScheduledStartState.SCHEDULED, ScheduledStartState.FAILED}),
            ),
        ]
    )


async def test_operations_api_describes_scheduled_start_in_authorized_scope() -> None:
    result = MagicMock()
    result.model_dump.return_value = {"scheduled_start_id": "opaque-id"}
    service = MagicMock(spec=ScheduledStartService)
    service.describe = AsyncMock(return_value=result)
    api = OperationsApi(
        settings=ControlSettings(),
        query_service=query_service(),
        scheduled_starts=service,
    )
    route = api.resolve_route(
        "GET",
        ("v1", "operations", "scheduled-starts", "opaque-id"),
    )
    binding = TrustedScopeBinding.create(
        kind=ScopeBindingKind.API,
        scope=SCOPE_A,
        binding_id="principal",
    )

    response = await api.dispatch(route, query={}, scope_binding=binding)

    assert response.payload == {"scheduled_start_id": "opaque-id"}
    service.describe.assert_awaited_once_with("opaque-id", scope=SCOPE_A)


@dataclass(frozen=True, kw_only=True)
class QueryErrorCase:
    id: str
    code: OperationsQueryErrorCode
    expected_status: HTTPStatus


QUERY_ERROR_CASES = [
    QueryErrorCase(
        id="invalid-query",
        code=OperationsQueryErrorCode.INVALID_QUERY,
        expected_status=HTTPStatus.BAD_REQUEST,
    ),
    QueryErrorCase(
        id="stale-cursor",
        code=OperationsQueryErrorCode.STALE_CURSOR,
        expected_status=HTTPStatus.CONFLICT,
    ),
    QueryErrorCase(
        id="provider-unavailable",
        code=OperationsQueryErrorCode.UNAVAILABLE,
        expected_status=HTTPStatus.SERVICE_UNAVAILABLE,
    ),
    QueryErrorCase(
        id="not-found",
        code=OperationsQueryErrorCode.NOT_FOUND,
        expected_status=HTTPStatus.NOT_FOUND,
    ),
]


@pytest.mark.parametrize("case", QUERY_ERROR_CASES, ids=lambda case: case.id)
async def test_operations_api_translates_layered_query_errors(case: QueryErrorCase) -> None:
    queries = MagicMock(spec=OperationsQueryService)
    queries.overview.side_effect = OperationsQueryError(case.code, "Safe operations error")
    api = OperationsApi(settings=ControlSettings(), query_service=queries)
    route = api.resolve_route("GET", ("v1", "operations"))
    binding = TrustedScopeBinding.create(
        kind=ScopeBindingKind.API,
        scope=SCOPE_A,
        binding_id="principal",
    )

    with pytest.raises(OperationsApiRequestError) as raised:
        await api.dispatch(route, query={}, scope_binding=binding)

    assert raised.value.status is case.expected_status
    assert raised.value.code == case.code.value


def test_configuration_schema_is_the_bundled_bounded_contract() -> None:
    schema = query_service().configuration_schema()

    assert schema.media_type == "application/schema+json"
    assert schema.schema_document["title"] == "Justflow tenant configuration"
