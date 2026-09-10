"""Generate and export the public OpenAPI document from runtime contracts."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from importlib.resources import files
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from justflow.configuration.activation import (
    ActivationPage,
    ActivationPlan,
    ActivationRecord,
    PublicationRecord,
    WorkerReadinessRegistration,
)
from justflow.configuration.lifecycle import (
    ConfigurationDiscardRecord,
    ConfigurationDiscardResult,
    ConfigurationRelationships,
    LocalConfigurationApplyResult,
)
from justflow.configuration.local_authoring import LocalAuthoringDraft
from justflow.configuration.models import DraftRecord, RevisionPage, RevisionRecord
from justflow.configuration.publication import (
    ConfigurationDiff,
    ConfigurationValidationReport,
)
from justflow.runtime.api_compatibility import PUBLIC_API_COMPATIBILITY_VERSION
from justflow.runtime.api_models import (
    ActivationReadinessApiResponse,
    ApiErrorResponse,
    CapabilitiesApiResponse,
    ProbeApiResponse,
    ScheduledStartCreateApiResponse,
    SignalEventApiRequest,
    StartApiRequest,
    StartWorkflowApiResponse,
    StatusApiResponse,
    TriggerApplyApiResponse,
    TriggerDeleteApiRequest,
    TriggersFragmentApiResponse,
    WorkflowFragmentApiResponse,
)
from justflow.runtime.configuration_api import (
    ActivateConfigurationRequest,
    ApplyConfigurationRequest,
    CreateDraftRequest,
    DiscardConfigurationRequest,
    PlanActivationRequest,
    PublishConfigurationRequest,
    RollbackConfigurationRequest,
    UpdateDraftRequest,
)
from justflow.runtime.health import HealthReport
from justflow.runtime.local_authoring_api import (
    LocalAuthoringRequest,
    UpdateLocalAuthoringRequest,
    WorkflowFragmentPreviewInvalid,
    WorkflowFragmentPreviewReady,
)
from justflow.runtime.operations import WorkflowDescription, WorkflowListResult
from justflow.runtime.operations_query import (
    ActivationOperationsView,
    AuthoringReference,
    ConfigurationOperationsView,
    ConfigurationSchema,
    DefinitionPage,
    OperationsOverview,
    TriggerOperationsView,
    WorkflowDefinitionDocument,
    WorkflowDetail,
    WorkflowRegistrationPage,
)
from justflow.runtime.scheduled_starts import (
    ScheduledStartCancelRequest,
    ScheduledStartCreateRequest,
    ScheduledStartDescription,
    ScheduledStartMutationResult,
    ScheduledStartPage,
    ScheduledStartRescheduleRequest,
)

OPENAPI_VERSION = "0.1.0"
OPENAPI_SPECIFICATION_VERSION = "3.1.0"
OPENAPI_FILE_NAME = f"justflow-openapi-{OPENAPI_VERSION}.json"
OPENAPI_BUNDLED_PACKAGE = "justflow.openapi.bundled"
OPENAPI_JSON_SCHEMA_DIALECT = "https://json-schema.org/draft/2020-12/schema"
MAX_CURSOR_LENGTH = 16_384
MAX_IDENTITY_LENGTH = 1_000
MAX_QUERY_IDENTITY_LENGTH = 128
MAX_CONFIGURATION_YAML_BYTES = 1_048_576
MAX_CLOUD_EVENT_PROPERTIES = 64


class OpenApiExportConflictError(Exception):
    """An OpenAPI export would overwrite different repository content."""


class SecurityKind(str, Enum):
    PUBLIC = "public"
    HOST = "host"
    WEBHOOK = "webhook"


@dataclass(frozen=True, kw_only=True)
class ApiParameter:
    name: str
    location: str
    required: bool
    description: str
    schema: Mapping[str, object]


@dataclass(frozen=True, kw_only=True)
class ApiResponseContract:
    status: int
    description: str
    models: tuple[type[BaseModel], ...] = ()
    media_type: str = "application/json"
    schema: Mapping[str, object] | None = None


@dataclass(frozen=True, kw_only=True)
class PublicApiRoute:
    method: str
    path: str
    operation_id: str
    tag: str
    summary: str
    security: SecurityKind
    responses: tuple[ApiResponseContract, ...]
    request_models: tuple[type[BaseModel], ...] = ()
    request_media_type: str = "application/json"
    request_schema: Mapping[str, object] | None = None
    parameters: tuple[ApiParameter, ...] = ()
    error_codes: tuple[str, ...] = ()
    idempotent: bool = False


class _SchemaRegistry:
    def __init__(self) -> None:
        self.schemas: dict[str, dict[str, Any]] = {}

    def reference(self, model: type[BaseModel]) -> dict[str, str]:
        name = model.__name__
        generated = model.model_json_schema(
            by_alias=True,
            mode="validation",
            ref_template="#/components/schemas/{model}",
        )
        definitions = generated.pop("$defs", {})
        self._add(name, generated)
        for definition_name, definition in definitions.items():
            self._add(definition_name, definition)
        return {"$ref": f"#/components/schemas/{name}"}

    def _add(self, name: str, schema: dict[str, Any]) -> None:
        existing = self.schemas.get(name)
        if existing is not None and existing != schema:
            raise ValueError(f"OpenAPI schema component '{name}' has conflicting definitions")
        self.schemas[name] = schema


def build_openapi_document() -> dict[str, Any]:
    registry = _SchemaRegistry()
    paths: dict[str, dict[str, object]] = {}
    operation_ids: set[str] = set()
    for route in public_api_routes():
        if route.operation_id in operation_ids:
            raise ValueError(f"Duplicate OpenAPI operation ID '{route.operation_id}'")
        operation_ids.add(route.operation_id)
        methods = paths.setdefault(route.path, {})
        if route.method.lower() in methods:
            raise ValueError(f"Duplicate OpenAPI route '{route.method} {route.path}'")
        methods[route.method.lower()] = _operation_document(route, registry)
    document: dict[str, Any] = {
        "openapi": OPENAPI_SPECIFICATION_VERSION,
        "jsonSchemaDialect": OPENAPI_JSON_SCHEMA_DIALECT,
        "info": {
            "title": "Justflow public API",
            "version": OPENAPI_VERSION,
            "description": (
                "Versioned automation contract for workflow control, operations, "
                "configuration, declared triggers, and one-off scheduled starts."
            ),
            "license": {
                "name": "Apache-2.0",
                "identifier": "Apache-2.0",
            },
        },
        "servers": [{"url": "/", "description": "Current Justflow deployment"}],
        "tags": [
            {"name": name, "description": description}
            for name, description in _tag_descriptions().items()
        ],
        "paths": paths,
        "components": {
            "schemas": registry.schemas,
            "securitySchemes": {
                "HostAuthentication": {
                    "type": "http",
                    "scheme": "bearer",
                    "description": (
                        "Credential interpreted by the host-supplied authentication facade. "
                        "The concrete credential format is deployment-specific."
                    ),
                },
                "WebhookSignature": {
                    "type": "apiKey",
                    "in": "header",
                    "name": "X-Webhook-Signature",
                    "description": (
                        "Representative signature header. The registered webhook adapter "
                        "defines and verifies its actual bounded header contract."
                    ),
                },
            },
        },
        "x-justflow-api-compatibility": PUBLIC_API_COMPATIBILITY_VERSION,
    }
    return _canonical_document(document)


def load_bundled_openapi_document() -> dict[str, Any]:
    resource = files(OPENAPI_BUNDLED_PACKAGE).joinpath(OPENAPI_FILE_NAME)
    return json.loads(resource.read_text(encoding="utf-8"))


def render_openapi_document(document: Mapping[str, Any] | None = None) -> bytes:
    return _document_bytes(build_openapi_document() if document is None else document)


def export_openapi_document(destination: str | Path) -> Path:
    target = Path(destination)
    if target.exists() and target.is_dir() or not target.suffix:
        target = target / OPENAPI_FILE_NAME
    rendered = render_openapi_document(load_bundled_openapi_document())
    if target.exists():
        if target.read_bytes() != rendered:
            raise OpenApiExportConflictError(
                f"OpenAPI export would overwrite different file '{target}'"
            )
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        with target.open("xb") as stream:
            stream.write(rendered)
    except OSError as exc:
        raise OpenApiExportConflictError(
            f"Cannot export OpenAPI document '{target}': {exc}"
        ) from exc
    return target


def public_api_routes() -> tuple[PublicApiRoute, ...]:
    ok = lambda model: ApiResponseContract(
        status=200, description="Successful response", models=(model,)
    )
    accepted = lambda model: ApiResponseContract(
        status=202,
        description="Request accepted or already accepted",
        models=(model,),
    )
    created = lambda *models: ApiResponseContract(
        status=201,
        description="Resource created",
        models=models,
    )
    yaml_response = ApiResponseContract(
        status=200,
        description="Bounded YAML representation",
        media_type="application/yaml",
        schema={"type": "string", "maxLength": MAX_CONFIGURATION_YAML_BYTES},
    )
    host_errors = ("unauthenticated", "forbidden")
    control_errors = host_errors + (
        "invalid_request",
        "not_found",
        "temporal_unavailable",
    )
    routes = [
        PublicApiRoute(
            method="GET",
            path="/livez",
            operation_id="getLiveness",
            tag="health",
            summary="Check process liveness",
            security=SecurityKind.PUBLIC,
            responses=(ok(ProbeApiResponse),),
        ),
        PublicApiRoute(
            method="GET",
            path="/readyz",
            operation_id="getReadiness",
            tag="health",
            summary="Check bounded runtime readiness",
            security=SecurityKind.PUBLIC,
            responses=(
                ok(ProbeApiResponse),
                ApiResponseContract(
                    status=503,
                    description="A required component is unavailable",
                    models=(ProbeApiResponse,),
                ),
            ),
        ),
        PublicApiRoute(
            method="GET",
            path="/healthz",
            operation_id="getDetailedHealth",
            tag="health",
            summary="Read authenticated component health",
            security=SecurityKind.HOST,
            responses=(ok(HealthReport),),
            error_codes=host_errors,
        ),
        PublicApiRoute(
            method="GET",
            path="/metrics",
            operation_id="getMetrics",
            tag="health",
            summary="Read Prometheus metrics",
            security=SecurityKind.HOST,
            responses=(
                ApiResponseContract(
                    status=200,
                    description="Prometheus text exposition",
                    media_type="text/plain",
                    schema={"type": "string"},
                ),
            ),
            error_codes=host_errors,
        ),
        PublicApiRoute(
            method="POST",
            path="/webhooks/{source_name}",
            operation_id="receiveWebhook",
            tag="ingress",
            summary="Receive a registered signed webhook",
            security=SecurityKind.WEBHOOK,
            request_media_type="application/octet-stream",
            request_schema={"type": "string", "format": "binary"},
            responses=(accepted(StartWorkflowApiResponse), ok(StartWorkflowApiResponse)),
            parameters=(_path_parameter("source_name", "Registered webhook source name"),),
            error_codes=(
                "unknown_source",
                "verification_failed",
                "invalid_payload",
                "invalid_identity",
                "provider_unavailable",
                "temporal_unavailable",
            ),
            idempotent=True,
        ),
        PublicApiRoute(
            method="POST",
            path="/events/{mapping_name}",
            operation_id="receiveCloudEvent",
            tag="ingress",
            summary="Receive a registered cloud-event mapping",
            security=SecurityKind.HOST,
            request_schema={
                "type": "object",
                "maxProperties": MAX_CLOUD_EVENT_PROPERTIES,
                "additionalProperties": True,
            },
            responses=(accepted(StartWorkflowApiResponse), ok(StartWorkflowApiResponse)),
            parameters=(_path_parameter("mapping_name", "Registered cloud-event mapping"),),
            error_codes=control_errors
            + (
                "invalid_payload",
                "invalid_identity",
                "source_rejected",
                "self_trigger_loop",
                "cloud_event_unavailable",
            ),
            idempotent=True,
        ),
        PublicApiRoute(
            method="POST",
            path="/v1/workflows",
            operation_id="startWorkflow",
            tag="workflows",
            summary="Start an active workflow through an API trigger",
            security=SecurityKind.HOST,
            request_models=(StartApiRequest,),
            responses=(accepted(StartWorkflowApiResponse), ok(StartWorkflowApiResponse)),
            error_codes=control_errors
            + (
                "unknown_workflow",
                "definition_unavailable",
                "incompatible_worker",
                "input_rejected",
                "configuration_error",
                "trigger_paused",
                "trigger_unavailable",
            ),
            idempotent=True,
        ),
        PublicApiRoute(
            method="GET",
            path="/v1/workflows",
            operation_id="listWorkflowExecutions",
            tag="workflows",
            summary="List scoped workflow executions",
            security=SecurityKind.HOST,
            responses=(ok(WorkflowListResult),),
            parameters=(_limit_parameter(), _page_token_parameter()),
            error_codes=control_errors,
        ),
        PublicApiRoute(
            method="GET",
            path="/v1/workflows/{workflow_id}",
            operation_id="describeWorkflowExecution",
            tag="workflows",
            summary="Describe one scoped workflow execution",
            security=SecurityKind.HOST,
            responses=(ok(WorkflowDescription),),
            parameters=(
                _path_parameter("workflow_id", "Opaque scoped workflow identity"),
                _run_id_parameter(),
            ),
            error_codes=control_errors,
        ),
        PublicApiRoute(
            method="POST",
            path="/v1/workflows/{workflow_id}/events/{event_name}",
            operation_id="signalWorkflowEvent",
            tag="workflows",
            summary="Deliver a bounded event to a workflow execution",
            security=SecurityKind.HOST,
            request_models=(SignalEventApiRequest,),
            responses=(accepted(StatusApiResponse),),
            parameters=(
                _path_parameter("workflow_id", "Opaque scoped workflow identity"),
                _path_parameter("event_name", "Declared workflow event name"),
                _run_id_parameter(),
            ),
            error_codes=control_errors,
            idempotent=False,
        ),
    ]
    for operation in ("cancel", "terminate"):
        routes.append(
            PublicApiRoute(
                method="POST",
                path=f"/v1/workflows/{{workflow_id}}/{operation}",
                operation_id=f"{operation}WorkflowExecution",
                tag="workflows",
                summary=f"{operation.title()} a workflow execution",
                security=SecurityKind.HOST,
                responses=(accepted(StatusApiResponse),),
                parameters=(
                    _path_parameter("workflow_id", "Opaque scoped workflow identity"),
                    _run_id_parameter(),
                ),
                error_codes=control_errors,
                idempotent=True,
            )
        )
    routes.extend(_trigger_routes(ok, accepted, host_errors))
    routes.extend(_scheduled_start_routes(ok, accepted, host_errors))
    routes.extend(_operations_routes(ok, host_errors))
    routes.extend(_configuration_routes(ok, accepted, created, yaml_response, host_errors))
    return tuple(routes)


def _trigger_routes(
    ok: Any,
    accepted: Any,
    host_errors: tuple[str, ...],
) -> list[PublicApiRoute]:
    routes = [
        PublicApiRoute(
            method="POST",
            path="/v1/triggers/apply",
            operation_id="applyDeclaredTriggers",
            tag="triggers",
            summary="Reconcile schedule-kind triggers",
            security=SecurityKind.HOST,
            responses=(ok(TriggerApplyApiResponse),),
            error_codes=host_errors
            + (
                "catalog_unavailable",
                "collection_limit",
                "confirmation_required",
                "plan_conflict",
                "temporal_unavailable",
            ),
            idempotent=True,
        )
    ]
    for operation in ("pause", "resume"):
        routes.append(
            PublicApiRoute(
                method="POST",
                path=f"/v1/triggers/{{trigger_name}}/{operation}",
                operation_id=f"{operation}DeclaredTrigger",
                tag="triggers",
                summary=f"{operation.title()} a schedule-kind trigger",
                security=SecurityKind.HOST,
                responses=(ok(StatusApiResponse),),
                parameters=(_path_parameter("trigger_name", "Declared trigger name"),),
                error_codes=host_errors + _trigger_error_codes(),
                idempotent=True,
            )
        )
    routes.extend(
        (
            PublicApiRoute(
                method="POST",
                path="/v1/triggers/{trigger_name}/run",
                operation_id="runDeclaredTriggerNow",
                tag="triggers",
                summary="Run a schedule-kind trigger now",
                security=SecurityKind.HOST,
                responses=(accepted(StatusApiResponse), ok(StatusApiResponse)),
                parameters=(
                    _path_parameter("trigger_name", "Declared trigger name"),
                    _idempotency_parameter(),
                ),
                error_codes=host_errors + _trigger_error_codes(),
                idempotent=True,
            ),
            PublicApiRoute(
                method="POST",
                path="/v1/triggers/{trigger_name}/delete",
                operation_id="deleteDeclaredTrigger",
                tag="triggers",
                summary="Delete one managed schedule-kind trigger",
                security=SecurityKind.HOST,
                request_models=(TriggerDeleteApiRequest,),
                responses=(ok(StatusApiResponse),),
                parameters=(_path_parameter("trigger_name", "Declared trigger name"),),
                error_codes=host_errors + _trigger_error_codes(),
                idempotent=True,
            ),
        )
    )
    return routes


def _scheduled_start_routes(
    ok: Any,
    accepted: Any,
    host_errors: tuple[str, ...],
) -> list[PublicApiRoute]:
    errors = host_errors + (
        "invalid_request",
        "trigger_unavailable",
        "trigger_paused",
        "input_rejected",
        "not_found",
        "conflict",
        "quota_exceeded",
        "quota_unavailable",
        "priority_unsupported",
        "temporal_unavailable",
    )
    return [
        PublicApiRoute(
            method="POST",
            path="/v1/scheduled-starts",
            operation_id="createScheduledStart",
            tag="scheduled-starts",
            summary="Schedule one future workflow invocation",
            security=SecurityKind.HOST,
            request_models=(ScheduledStartCreateRequest,),
            responses=(
                accepted(ScheduledStartCreateApiResponse),
                ok(ScheduledStartCreateApiResponse),
            ),
            parameters=(_idempotency_parameter(),),
            error_codes=errors,
            idempotent=True,
        ),
        PublicApiRoute(
            method="POST",
            path="/v1/scheduled-starts/{scheduled_start_id}/reschedule",
            operation_id="rescheduleScheduledStart",
            tag="scheduled-starts",
            summary="Move a still-pending scheduled start",
            security=SecurityKind.HOST,
            request_models=(ScheduledStartRescheduleRequest,),
            responses=(accepted(ScheduledStartMutationResult), ok(ScheduledStartMutationResult)),
            parameters=(
                _path_parameter("scheduled_start_id", "Opaque scoped scheduled-start identity"),
                _idempotency_parameter(),
            ),
            error_codes=errors,
            idempotent=True,
        ),
        PublicApiRoute(
            method="POST",
            path="/v1/scheduled-starts/{scheduled_start_id}/cancel",
            operation_id="cancelScheduledStart",
            tag="scheduled-starts",
            summary="Cancel a still-pending scheduled start",
            security=SecurityKind.HOST,
            request_models=(ScheduledStartCancelRequest,),
            responses=(accepted(ScheduledStartMutationResult), ok(ScheduledStartMutationResult)),
            parameters=(
                _path_parameter("scheduled_start_id", "Opaque scoped scheduled-start identity"),
                _idempotency_parameter(),
            ),
            error_codes=errors,
            idempotent=True,
        ),
    ]


def _operations_routes(ok: Any, host_errors: tuple[str, ...]) -> list[PublicApiRoute]:
    definitions: tuple[tuple[str, str, str, type[BaseModel], tuple[ApiParameter, ...]], ...] = (
        ("", "getOperationsOverview", "Read the operations overview", OperationsOverview, ()),
        (
            "/workflows",
            "listRegisteredWorkflows",
            "List registered workflows",
            WorkflowRegistrationPage,
            (_limit_parameter(), _cursor_parameter()),
        ),
        (
            "/definitions",
            "listDefinitions",
            "List retained definitions",
            DefinitionPage,
            (_limit_parameter(), _cursor_parameter()),
        ),
        (
            "/runs",
            "listOperationsRuns",
            "List workflow runs with bounded filters",
            WorkflowListResult,
            _run_filter_parameters(),
        ),
        ("/triggers", "listDeclaredTriggers", "List declared triggers", TriggerOperationsView, ()),
        (
            "/configuration",
            "getConfigurationOperations",
            "Read active and retained configuration",
            ConfigurationOperationsView,
            (_limit_parameter(), _cursor_parameter()),
        ),
        (
            "/activations",
            "listActivationOperations",
            "List configuration activations",
            ActivationOperationsView,
            (_limit_parameter(), _cursor_parameter()),
        ),
        (
            "/configuration-schema",
            "getEditableConfigurationSchema",
            "Read the safe editable configuration schema",
            ConfigurationSchema,
            (),
        ),
        (
            "/authoring-reference",
            "getAuthoringReference",
            "Read the sanitized authoring reference",
            AuthoringReference,
            (),
        ),
        (
            "/scheduled-starts",
            "listScheduledStarts",
            "List one-off scheduled starts",
            ScheduledStartPage,
            (_limit_parameter(), _cursor_parameter(), _state_parameter()),
        ),
    )
    routes = [
        PublicApiRoute(
            method="GET",
            path="/v1/operations/capabilities",
            operation_id="getCapabilities",
            tag="operations",
            summary="Read API compatibility and authorized capabilities",
            security=SecurityKind.HOST,
            responses=(ok(CapabilitiesApiResponse),),
            error_codes=host_errors,
        )
    ]
    routes.extend(
        PublicApiRoute(
            method="GET",
            path=f"/v1/operations{suffix}",
            operation_id=operation_id,
            tag="operations",
            summary=summary,
            security=SecurityKind.HOST,
            responses=(ok(model),),
            parameters=parameters,
            error_codes=host_errors + ("invalid_query", "not_found", "stale_cursor", "unavailable"),
        )
        for suffix, operation_id, summary, model, parameters in definitions
    )
    routes.extend(
        (
            PublicApiRoute(
                method="GET",
                path="/v1/operations/workflows/{workflow_name}",
                operation_id="getRegisteredWorkflow",
                tag="operations",
                summary="Read one registered workflow",
                security=SecurityKind.HOST,
                responses=(ok(WorkflowDetail),),
                parameters=(_path_parameter("workflow_name", "Logical workflow name"),),
                error_codes=host_errors + ("not_found", "unavailable"),
            ),
            PublicApiRoute(
                method="GET",
                path="/v1/operations/workflows/{workflow_name}/definition",
                operation_id="getWorkflowDefinition",
                tag="operations",
                summary="Read one safe workflow definition projection",
                security=SecurityKind.HOST,
                responses=(ok(WorkflowDefinitionDocument),),
                parameters=(_path_parameter("workflow_name", "Logical workflow name"),),
                error_codes=host_errors + ("not_found", "unavailable"),
            ),
            PublicApiRoute(
                method="GET",
                path="/v1/operations/runs/{workflow_id}",
                operation_id="getOperationsRun",
                tag="operations",
                summary="Read one workflow run",
                security=SecurityKind.HOST,
                responses=(ok(WorkflowDescription),),
                parameters=(
                    _path_parameter("workflow_id", "Opaque scoped workflow identity"),
                    _run_id_parameter(),
                ),
                error_codes=host_errors + ("not_found", "unavailable"),
            ),
            PublicApiRoute(
                method="GET",
                path="/v1/operations/scheduled-starts/{scheduled_start_id}",
                operation_id="getScheduledStart",
                tag="operations",
                summary="Read one safe scheduled-start projection",
                security=SecurityKind.HOST,
                responses=(ok(ScheduledStartDescription),),
                parameters=(
                    _path_parameter("scheduled_start_id", "Opaque scoped scheduled-start identity"),
                ),
                error_codes=host_errors + ("not_found", "temporal_unavailable"),
            ),
        )
    )
    return routes


def _configuration_routes(
    ok: Any,
    accepted: Any,
    created: Any,
    yaml_response: ApiResponseContract,
    host_errors: tuple[str, ...],
) -> list[PublicApiRoute]:
    errors = host_errors + (
        "invalid_request",
        "configuration_not_found",
        "configuration_conflict",
        "configuration_too_large",
        "configuration_unavailable",
        "activation_not_found",
        "activation_conflict",
        "activation_too_large",
        "activation_unavailable",
    )
    idem_and_correlation = (_idempotency_parameter(), _correlation_parameter())
    routes = [
        PublicApiRoute(
            method="POST",
            path="/v1/configuration/draft",
            operation_id="createConfigurationDraft",
            tag="configuration",
            summary="Create the scoped working configuration",
            security=SecurityKind.HOST,
            request_models=(CreateDraftRequest, LocalAuthoringRequest),
            responses=(created(DraftRecord, LocalAuthoringDraft),),
            error_codes=errors,
        ),
        PublicApiRoute(
            method="GET",
            path="/v1/configuration/draft",
            operation_id="getConfigurationDraft",
            tag="configuration",
            summary="Read the scoped working configuration",
            security=SecurityKind.HOST,
            responses=(
                ApiResponseContract(
                    status=200,
                    description="Current working configuration",
                    models=(DraftRecord, LocalAuthoringDraft),
                ),
            ),
            error_codes=errors,
        ),
        PublicApiRoute(
            method="PUT",
            path="/v1/configuration/draft",
            operation_id="updateConfigurationDraft",
            tag="configuration",
            summary="Replace the working configuration with optimistic concurrency",
            security=SecurityKind.HOST,
            request_models=(UpdateDraftRequest, UpdateLocalAuthoringRequest),
            responses=(
                ApiResponseContract(
                    status=200,
                    description="Updated working configuration",
                    models=(DraftRecord, LocalAuthoringDraft),
                ),
            ),
            error_codes=errors,
            idempotent=True,
        ),
        PublicApiRoute(
            method="POST",
            path="/v1/configuration/draft/import",
            operation_id="importConfigurationDraft",
            tag="configuration",
            summary="Import bounded YAML into the working configuration",
            security=SecurityKind.HOST,
            request_media_type="application/yaml",
            request_schema={"type": "string", "maxLength": MAX_CONFIGURATION_YAML_BYTES},
            responses=(
                ApiResponseContract(
                    status=200,
                    description="Imported working configuration",
                    models=(DraftRecord, LocalAuthoringDraft),
                ),
            ),
            parameters=(_expected_version_parameter(required=False),),
            error_codes=errors,
            idempotent=True,
        ),
        PublicApiRoute(
            method="GET",
            path="/v1/configuration/draft/export",
            operation_id="exportConfigurationDraft",
            tag="configuration",
            summary="Export the working configuration as bounded YAML",
            security=SecurityKind.HOST,
            responses=(yaml_response,),
            parameters=(_expected_version_parameter(required=False),),
            error_codes=errors,
        ),
        PublicApiRoute(
            method="POST",
            path="/v1/configuration/draft/validate",
            operation_id="validateConfigurationDraft",
            tag="configuration",
            summary="Validate the working configuration",
            security=SecurityKind.HOST,
            responses=(ok(ConfigurationValidationReport),),
            error_codes=errors,
            idempotent=True,
        ),
        PublicApiRoute(
            method="GET",
            path="/v1/configuration/relationships",
            operation_id="getConfigurationRelationships",
            tag="configuration",
            summary="Compare working and active declarations",
            security=SecurityKind.HOST,
            responses=(ok(ConfigurationRelationships),),
            error_codes=errors,
        ),
        PublicApiRoute(
            method="POST",
            path="/v1/configuration/apply",
            operation_id="applyLocalConfiguration",
            tag="configuration",
            summary="Apply a local-source working configuration",
            security=SecurityKind.HOST,
            request_models=(ApplyConfigurationRequest,),
            responses=(ok(LocalConfigurationApplyResult),),
            parameters=idem_and_correlation,
            error_codes=errors,
            idempotent=True,
        ),
        PublicApiRoute(
            method="POST",
            path="/v1/configuration/draft/discard",
            operation_id="discardConfigurationDraft",
            tag="configuration",
            summary="Reset the working configuration to its active state",
            security=SecurityKind.HOST,
            request_models=(DiscardConfigurationRequest,),
            responses=(ok(ConfigurationDiscardResult),),
            parameters=idem_and_correlation,
            error_codes=errors,
            idempotent=True,
        ),
        PublicApiRoute(
            method="GET",
            path="/v1/configuration/discards/{discard_id}",
            operation_id="getConfigurationDiscard",
            tag="configuration",
            summary="Read a discard operation",
            security=SecurityKind.HOST,
            responses=(ok(ConfigurationDiscardRecord),),
            parameters=(_path_parameter("discard_id", "Opaque discard identity"),),
            error_codes=errors,
        ),
    ]
    routes.extend(_local_fragment_routes(ok, errors))
    routes.extend(_managed_configuration_routes(ok, accepted, errors, idem_and_correlation))
    return routes


def _local_fragment_routes(ok: Any, errors: tuple[str, ...]) -> list[PublicApiRoute]:
    yaml_request = {"type": "string", "maxLength": MAX_CONFIGURATION_YAML_BYTES}
    routes = [
        PublicApiRoute(
            method="GET",
            path="/v1/configuration/draft/triggers",
            operation_id="getTriggerDraftFragment",
            tag="configuration",
            summary="Read the local trigger fragment",
            security=SecurityKind.HOST,
            responses=(ok(TriggersFragmentApiResponse),),
            error_codes=errors,
        ),
        PublicApiRoute(
            method="PUT",
            path="/v1/configuration/draft/triggers",
            operation_id="updateTriggerDraftFragment",
            tag="configuration",
            summary="Update the local trigger fragment",
            security=SecurityKind.HOST,
            request_media_type="application/yaml",
            request_schema=yaml_request,
            responses=(ok(LocalAuthoringDraft),),
            parameters=(_expected_version_parameter(required=True),),
            error_codes=errors,
            idempotent=True,
        ),
        PublicApiRoute(
            method="POST",
            path="/v1/configuration/draft/workflows/{workflow_name}/preview",
            operation_id="previewWorkflowDraftFragment",
            tag="configuration",
            summary="Validate and graph one workflow fragment without saving",
            security=SecurityKind.HOST,
            request_media_type="application/yaml",
            request_schema=yaml_request,
            responses=(
                ApiResponseContract(
                    status=200,
                    description="Fragment-only preview",
                    models=(WorkflowFragmentPreviewReady, WorkflowFragmentPreviewInvalid),
                ),
            ),
            parameters=(_path_parameter("workflow_name", "Logical workflow name"),),
            error_codes=errors,
            idempotent=True,
        ),
    ]
    for method, operation_id, summary, response_model in (
        (
            "GET",
            "getWorkflowDraftFragment",
            "Read one local workflow fragment",
            WorkflowFragmentApiResponse,
        ),
        (
            "PUT",
            "updateWorkflowDraftFragment",
            "Update one local workflow fragment",
            LocalAuthoringDraft,
        ),
        (
            "DELETE",
            "deleteWorkflowDraftFragment",
            "Delete one local workflow fragment",
            LocalAuthoringDraft,
        ),
    ):
        routes.append(
            PublicApiRoute(
                method=method,
                path="/v1/configuration/draft/workflows/{workflow_name}",
                operation_id=operation_id,
                tag="configuration",
                summary=summary,
                security=SecurityKind.HOST,
                request_media_type="application/yaml",
                request_schema=yaml_request if method == "PUT" else None,
                responses=(ok(response_model),),
                parameters=(
                    _path_parameter("workflow_name", "Logical workflow name"),
                    *(
                        (_expected_version_parameter(required=True),)
                        if method in {"PUT", "DELETE"}
                        else ()
                    ),
                ),
                error_codes=errors + ("dependent_triggers", "invalid_workflow_fragment"),
                idempotent=method != "GET",
            )
        )
    return routes


def _managed_configuration_routes(
    ok: Any,
    accepted: Any,
    errors: tuple[str, ...],
    idem_and_correlation: tuple[ApiParameter, ...],
) -> list[PublicApiRoute]:
    routes = [
        PublicApiRoute(
            method="GET",
            path="/v1/configuration/revisions",
            operation_id="listConfigurationRevisions",
            tag="configuration",
            summary="List immutable configuration revisions",
            security=SecurityKind.HOST,
            responses=(ok(RevisionPage),),
            parameters=(_limit_parameter(), _cursor_parameter()),
            error_codes=errors,
        ),
        PublicApiRoute(
            method="GET",
            path="/v1/configuration/revisions/compare",
            operation_id="compareConfigurationRevisions",
            tag="configuration",
            summary="Compare two immutable configuration revisions",
            security=SecurityKind.HOST,
            responses=(ok(ConfigurationDiff),),
            parameters=(
                _query_parameter("source_revision_id", "Source revision identity", required=True),
                _query_parameter("target_revision_id", "Target revision identity", required=True),
            ),
            error_codes=errors,
        ),
        PublicApiRoute(
            method="GET",
            path="/v1/configuration/revisions/{revision_id}",
            operation_id="getConfigurationRevision",
            tag="configuration",
            summary="Read one immutable configuration revision",
            security=SecurityKind.HOST,
            responses=(ok(RevisionRecord),),
            parameters=(_path_parameter("revision_id", "Configuration revision identity"),),
            error_codes=errors,
        ),
        PublicApiRoute(
            method="POST",
            path="/v1/configuration/publications",
            operation_id="publishConfiguration",
            tag="configuration",
            summary="Publish a valid immutable configuration revision",
            security=SecurityKind.HOST,
            request_models=(PublishConfigurationRequest,),
            responses=(accepted(PublicationRecord),),
            parameters=idem_and_correlation,
            error_codes=errors,
            idempotent=True,
        ),
        PublicApiRoute(
            method="GET",
            path="/v1/configuration/publications/{publication_id}",
            operation_id="getConfigurationPublication",
            tag="configuration",
            summary="Read a publication operation",
            security=SecurityKind.HOST,
            responses=(ok(PublicationRecord),),
            parameters=(_path_parameter("publication_id", "Opaque publication identity"),),
            error_codes=errors,
        ),
        PublicApiRoute(
            method="POST",
            path="/v1/configuration/activations/plan",
            operation_id="planConfigurationActivation",
            tag="configuration",
            summary="Plan activation of an immutable revision",
            security=SecurityKind.HOST,
            request_models=(PlanActivationRequest,),
            responses=(ok(ActivationPlan),),
            error_codes=errors,
            idempotent=True,
        ),
        PublicApiRoute(
            method="POST",
            path="/v1/configuration/activations",
            operation_id="activateConfiguration",
            tag="configuration",
            summary="Start a confirmed configuration activation",
            security=SecurityKind.HOST,
            request_models=(ActivateConfigurationRequest,),
            responses=(accepted(ActivationRecord),),
            parameters=idem_and_correlation,
            error_codes=errors,
            idempotent=True,
        ),
        PublicApiRoute(
            method="GET",
            path="/v1/configuration/activations",
            operation_id="listConfigurationActivations",
            tag="configuration",
            summary="List configuration activations",
            security=SecurityKind.HOST,
            responses=(ok(ActivationPage),),
            parameters=(_limit_parameter(), _cursor_parameter()),
            error_codes=errors,
        ),
        PublicApiRoute(
            method="GET",
            path="/v1/configuration/activations/{activation_id}",
            operation_id="getConfigurationActivation",
            tag="configuration",
            summary="Read one configuration activation",
            security=SecurityKind.HOST,
            responses=(ok(ActivationRecord),),
            parameters=(_path_parameter("activation_id", "Opaque activation identity"),),
            error_codes=errors,
        ),
        PublicApiRoute(
            method="GET",
            path="/v1/configuration/activations/{activation_id}/readiness",
            operation_id="getConfigurationActivationReadiness",
            tag="configuration",
            summary="Read bounded activation readiness",
            security=SecurityKind.HOST,
            responses=(ok(ActivationReadinessApiResponse),),
            parameters=(_path_parameter("activation_id", "Opaque activation identity"),),
            error_codes=errors,
        ),
        PublicApiRoute(
            method="POST",
            path="/v1/configuration/activations/{activation_id}/readiness",
            operation_id="registerConfigurationActivationReadiness",
            tag="configuration",
            summary="Register compatible worker readiness",
            security=SecurityKind.HOST,
            request_models=(WorkerReadinessRegistration,),
            responses=(accepted(ActivationRecord),),
            parameters=(_path_parameter("activation_id", "Opaque activation identity"),),
            error_codes=errors,
            idempotent=True,
        ),
        PublicApiRoute(
            method="POST",
            path="/v1/configuration/activations/{activation_id}/rollback",
            operation_id="rollbackConfigurationActivation",
            tag="configuration",
            summary="Activate a retained revision as an audited rollback",
            security=SecurityKind.HOST,
            request_models=(RollbackConfigurationRequest,),
            responses=(accepted(ActivationRecord),),
            parameters=(
                _path_parameter("activation_id", "Opaque activation identity"),
                *idem_and_correlation,
            ),
            error_codes=errors,
            idempotent=True,
        ),
    ]
    return routes


def _operation_document(route: PublicApiRoute, registry: _SchemaRegistry) -> dict[str, object]:
    responses: dict[str, object] = {
        str(response.status): _response_document(response, registry) for response in route.responses
    }
    operation: dict[str, object] = {
        "operationId": route.operation_id,
        "summary": route.summary,
        "tags": [route.tag],
        "responses": responses,
        "x-justflow-idempotent": route.idempotent,
    }
    if route.security is SecurityKind.PUBLIC:
        operation["security"] = []
    elif route.security is SecurityKind.WEBHOOK:
        operation["security"] = [{"WebhookSignature": []}]
    else:
        operation["security"] = [{"HostAuthentication": []}]
    parameters = list(route.parameters)
    path_parameter_names = {
        part[1:-1] for part in route.path.split("/") if part.startswith("{") and part.endswith("}")
    }
    declared_path_names = {
        parameter.name for parameter in parameters if parameter.location == "path"
    }
    if path_parameter_names != declared_path_names:
        raise ValueError(f"OpenAPI path parameters disagree for '{route.path}'")
    if parameters:
        operation["parameters"] = [_parameter_document(parameter) for parameter in parameters]
    if route.request_models or route.request_schema is not None:
        request_schema = (
            _model_union_schema(route.request_models, registry)
            if route.request_models
            else dict(route.request_schema or {})
        )
        operation["requestBody"] = {
            "required": True,
            "content": {route.request_media_type: {"schema": request_schema}},
        }
    if route.error_codes:
        operation["x-justflow-error-codes"] = sorted(set(route.error_codes))
        error_response = {
            "description": "Bounded public error",
            "content": {"application/json": {"schema": registry.reference(ApiErrorResponse)}},
        }
        success_statuses = {response.status for response in route.responses}
        for status in _error_statuses(route.error_codes):
            if status not in success_statuses:
                responses[str(status)] = error_response
    return operation


def _response_document(
    response: ApiResponseContract,
    registry: _SchemaRegistry,
) -> dict[str, object]:
    schema = (
        _model_union_schema(response.models, registry)
        if response.models
        else dict(response.schema or {})
    )
    return {
        "description": response.description,
        "content": {response.media_type: {"schema": schema}},
    }


def _model_union_schema(
    models: tuple[type[BaseModel], ...],
    registry: _SchemaRegistry,
) -> dict[str, object]:
    references = [registry.reference(model) for model in models]
    if len(references) == 1:
        return dict(references[0])
    return {"oneOf": references}


def _parameter_document(parameter: ApiParameter) -> dict[str, object]:
    return {
        "name": parameter.name,
        "in": parameter.location,
        "required": parameter.required,
        "description": parameter.description,
        "schema": dict(parameter.schema),
    }


def _path_parameter(name: str, description: str) -> ApiParameter:
    return ApiParameter(
        name=name,
        location="path",
        required=True,
        description=description,
        schema={"type": "string", "minLength": 1, "maxLength": MAX_IDENTITY_LENGTH},
    )


def _query_parameter(name: str, description: str, *, required: bool = False) -> ApiParameter:
    return ApiParameter(
        name=name,
        location="query",
        required=required,
        description=description,
        schema={"type": "string", "minLength": 1, "maxLength": MAX_QUERY_IDENTITY_LENGTH},
    )


def _limit_parameter() -> ApiParameter:
    return ApiParameter(
        name="limit",
        location="query",
        required=False,
        description="Bounded page size",
        schema={"type": "integer", "minimum": 1, "maximum": 100},
    )


def _cursor_parameter() -> ApiParameter:
    return ApiParameter(
        name="cursor",
        location="query",
        required=False,
        description="Opaque scope-bound continuation cursor",
        schema={"type": "string", "minLength": 1, "maxLength": MAX_CURSOR_LENGTH},
    )


def _page_token_parameter() -> ApiParameter:
    return ApiParameter(
        name="page_token",
        location="query",
        required=False,
        description="Opaque Temporal continuation token",
        schema={"type": "string", "minLength": 1, "maxLength": MAX_CURSOR_LENGTH},
    )


def _run_id_parameter() -> ApiParameter:
    return _query_parameter("run_id", "Optional exact Temporal run identity")


def _state_parameter() -> ApiParameter:
    return ApiParameter(
        name="state",
        location="query",
        required=False,
        description="Closed scheduled-start lifecycle state; repeat to select several states",
        schema={
            "type": "array",
            "items": {
                "type": "string",
                "enum": ["scheduled", "dispatching", "started", "canceled", "failed"],
            },
            "maxItems": 5,
        },
    )


def _expected_version_parameter(*, required: bool) -> ApiParameter:
    return ApiParameter(
        name="expected_version",
        location="query",
        required=required,
        description="Optimistic-concurrency version",
        schema={"type": "integer", "minimum": 1},
    )


def _idempotency_parameter() -> ApiParameter:
    return ApiParameter(
        name="X-Idempotency-Key",
        location="header",
        required=True,
        description="Caller-stable identity for safe retries",
        schema={"type": "string", "minLength": 1, "maxLength": 256},
    )


def _correlation_parameter() -> ApiParameter:
    return ApiParameter(
        name="X-Correlation-Id",
        location="header",
        required=False,
        description="Safe caller correlation identity",
        schema={"type": "string", "minLength": 1, "maxLength": 256},
    )


def _run_filter_parameters() -> tuple[ApiParameter, ...]:
    return (
        _limit_parameter(),
        _cursor_parameter(),
        _query_parameter("workflow", "Logical workflow name"),
        _query_parameter("state", "Closed workflow execution state"),
        _query_parameter("started_after", "Inclusive timezone-aware start boundary"),
        _query_parameter("started_before", "Inclusive timezone-aware end boundary"),
        _query_parameter("definition_digest", "Exact immutable definition digest"),
        _query_parameter("trigger_source", "Closed trigger-source kind"),
        _query_parameter("worker_artifact", "Exact worker artifact digest"),
        ApiParameter(
            name="scope",
            location="query",
            required=False,
            description="Explicit current-scope filter",
            schema={"type": "string", "const": "current"},
        ),
    )


def _trigger_error_codes() -> tuple[str, ...]:
    return (
        "invalid_request",
        "unknown_schedule",
        "not_managed",
        "invalid_operation",
        "confirmation_required",
        "temporal_unavailable",
    )


def _error_statuses(error_codes: tuple[str, ...]) -> tuple[int, ...]:
    statuses = {400, 401, 403, 404, 409, 413, 422, 429, 503}
    if not any(code in error_codes for code in ("quota_exceeded",)):
        statuses.remove(429)
    if not any(code.endswith("too_large") or code == "request_too_large" for code in error_codes):
        statuses.remove(413)
    return tuple(sorted(statuses))


def _tag_descriptions() -> dict[str, str]:
    return {
        "health": "Bounded process probes, authenticated health, and metrics.",
        "ingress": "Host-registered webhook and cloud-event ingress.",
        "workflows": "Immediate execution control and observation.",
        "triggers": "Declared trigger reconciliation and schedule-kind controls.",
        "scheduled-starts": "Durable one-off future workflow invocations.",
        "operations": "Scoped operational projections without a second state store.",
        "configuration": "Local or managed configuration authoring and activation.",
    }


def _canonical_document(document: Mapping[str, Any]) -> dict[str, Any]:
    return json.loads(_document_bytes(document))


def _document_bytes(document: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(
            document,
            allow_nan=False,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
