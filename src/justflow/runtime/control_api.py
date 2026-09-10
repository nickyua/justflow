"""Bounded optional ASGI control and webhook application."""

from __future__ import annotations

import ipaddress
import json
import logging
from collections.abc import Awaitable, Mapping, MutableMapping, Sequence
from dataclasses import dataclass
from http import HTTPStatus
from typing import Any, Literal, Protocol, TypedDict
from urllib.parse import parse_qs

from pydantic import ValidationError

from justflow.config.settings import ControlSettings
from justflow.provenance import RuntimeProfile
from justflow.runtime.admin_panel import AdminPanel
from justflow.runtime.api_compatibility import PUBLIC_API_COMPATIBILITY
from justflow.runtime.api_models import (
    ApiCompatibilityResponse,
    ApiErrorDetail,
    ApiErrorResponse,
    CapabilitiesApiResponse,
    ProbeApiResponse,
    ScheduledStartCreateApiResponse,
    SignalApiRequest,
    SignalEventApiRequest,
    StartApiRequest,
    StartWorkflowApiResponse,
    StatusApiResponse,
    TriggerApplyApiResponse,
    TriggerApplyItemApiResponse,
    TriggerDeleteApiRequest,
    TriggerPathApiRequest,
)
from justflow.runtime.auth import (
    AuthenticatedPrincipal,
    AuthenticationError,
    AuthenticationProvider,
    AuthenticationRequest,
    AuthorizationAction,
    AuthorizationRequest,
    authorization_resource_digest,
)
from justflow.runtime.cloud_events import (
    CloudEventErrorCode,
    CloudEventIngress,
    CloudEventMappingError,
)
from justflow.runtime.configuration_api import (
    ConfigurationApiBinding,
    ConfigurationApiRequestError,
)
from justflow.runtime.health import HealthRegistry
from justflow.runtime.metrics import ApiMetricOutcome, MetricsRegistry
from justflow.runtime.operations import (
    ControlErrorCode,
    ControlOperationError,
    WorkflowControlService,
)
from justflow.runtime.operations_api import OperationsApi, OperationsApiRequestError
from justflow.runtime.schedule_operations import (
    ScheduleApplier,
    ScheduleControlService,
    ScheduleOperationError,
    ScheduleOperationErrorCode,
    TriggerRunNowStatus,
)
from justflow.runtime.schedule_reconciler import (
    ScheduleReconciliationError,
    ScheduleReconciliationErrorCode,
)
from justflow.runtime.scheduled_start_service import ScheduledStartService
from justflow.runtime.scheduled_starts import (
    ScheduledStartCancelRequest,
    ScheduledStartCreateRequest,
    ScheduledStartError,
    ScheduledStartErrorCode,
    ScheduledStartMutationStatus,
    ScheduledStartRescheduleRequest,
)
from justflow.runtime.starter import (
    ControlApiSourceIdentity,
    StartErrorCode,
    StartStatus,
    StartWorkflowRequest,
    WorkflowStarter,
    WorkflowStartError,
)
from justflow.runtime.webhooks import (
    SignedWebhookRequest,
    WebhookError,
    WebhookErrorCode,
    WebhookIngress,
)
from justflow.scope import (
    LOCAL_RUNTIME_SCOPE,
    RuntimeScope,
    ScopeBindingKind,
    ScopeResolutionError,
    TrustedScopeBinding,
    require_scope_grant,
    safe_identity_digest,
)

JSON_CONTENT_TYPE = b"application/json; charset=utf-8"
PROMETHEUS_CONTENT_TYPE = b"text/plain; version=0.0.4; charset=utf-8"
MAX_ROUTE_SEGMENTS = 8
MAX_ROUTE_SEGMENT_LENGTH = 256
MAX_IDEMPOTENCY_KEY_LENGTH = 256
IDEMPOTENCY_HEADER = b"x-idempotency-key"
CLOUD_EVENT_PUBLIC_ERRORS = {
    CloudEventErrorCode.INVALID_PAYLOAD: "Cloud-event payload is invalid",
    CloudEventErrorCode.INVALID_IDENTITY: "Cloud-event source identity is invalid",
    CloudEventErrorCode.SOURCE_REJECTED: "Cloud event does not match its registered mapping",
    CloudEventErrorCode.SELF_TRIGGER_LOOP: "Cloud event was rejected by loop prevention",
    CloudEventErrorCode.MAPPING_UNAVAILABLE: "Cloud-event mapping is unavailable",
    CloudEventErrorCode.UNKNOWN_MAPPING: "Cloud-event mapping is not available",
}

logger = logging.getLogger(__name__)


class AsgiScope(TypedDict, total=False):
    scheme: str
    type: str
    method: str
    path: str
    query_string: bytes
    headers: Sequence[tuple[bytes, bytes]]


class AsgiMessage(TypedDict, total=False):
    type: str
    body: bytes
    more_body: bool
    status: int
    headers: Sequence[tuple[bytes, bytes]]
    message: str


class AsgiReceive(Protocol):
    def __call__(self) -> Awaitable[AsgiMessage]: ...


class AsgiSend(Protocol):
    def __call__(self, message: AsgiMessage) -> Awaitable[None]: ...


@dataclass(frozen=True)
class ApiResponse:
    status: HTTPStatus
    payload: object | None = None
    content_type: bytes = JSON_CONTENT_TYPE
    raw_body: bytes | None = None
    headers: tuple[tuple[bytes, bytes], ...] = ()


@dataclass(frozen=True, kw_only=True)
class AuthenticationContext:
    principal: AuthenticatedPrincipal | None
    scope_binding: TrustedScopeBinding


class RequestRejectedError(Exception):
    def __init__(self, status: HTTPStatus, code: str, message: str) -> None:
        self.status = status
        self.code = code
        super().__init__(message)


class ControlApi:
    """ASGI application exposing bounded workflow runtime controls."""

    def __init__(
        self,
        *,
        settings: ControlSettings,
        runtime_profile: RuntimeProfile,
        starter: WorkflowStarter,
        controls: WorkflowControlService,
        health: HealthRegistry,
        metrics: MetricsRegistry,
        authentication: AuthenticationProvider | None = None,
        webhooks: WebhookIngress | None = None,
        cloud_events: CloudEventIngress | None = None,
        configuration_api: ConfigurationApiBinding | None = None,
        operations_api: OperationsApi | None = None,
        schedule_controls: ScheduleControlService | None = None,
        schedule_applier: ScheduleApplier | None = None,
        scheduled_starts: ScheduledStartService | None = None,
        admin_panel: AdminPanel | None = None,
        runtime_scope: RuntimeScope = LOCAL_RUNTIME_SCOPE,
    ) -> None:
        if runtime_profile is not RuntimeProfile.LOCAL and authentication is None:
            raise ValueError(
                "Control API authentication is required outside the local runtime profile"
            )
        if authentication is None and not _is_loopback_host(settings.host):
            raise ValueError(
                "Unauthenticated control API access requires an explicit loopback host"
            )
        self._settings = settings
        self._starter = starter
        self._controls = controls
        self._health = health
        self._metrics = metrics
        self._authentication = authentication
        self._webhooks = webhooks
        self._cloud_events = cloud_events
        self._configuration_api = configuration_api
        self._operations_api = operations_api
        self._schedule_controls = schedule_controls
        self._schedule_applier = schedule_applier
        self._scheduled_starts = scheduled_starts
        self._admin_panel = admin_panel
        self._local_scope = runtime_scope

    async def __call__(
        self,
        scope: AsgiScope,
        receive: AsgiReceive,
        send: AsgiSend,
    ) -> None:
        if scope.get("type") == "lifespan":
            await self._lifespan(receive, send)
            return
        if scope.get("type") != "http":
            await self._send(send, ApiResponse(HTTPStatus.NOT_FOUND))
            return
        try:
            response = await self._dispatch(scope, receive)
        except RequestRejectedError as exc:
            response = _error_response(exc.status, exc.code, str(exc))
        await self._send(send, response)

    async def _dispatch(self, scope: AsgiScope, receive: AsgiReceive) -> ApiResponse:
        method = scope.get("method", "")
        path = scope.get("path", "")
        headers = self._validated_headers(scope.get("headers", ()))
        query = self._parse_query(scope.get("query_string", b""))
        segments = _path_segments(path)

        if method == "GET" and segments == ("livez",):
            return ApiResponse(
                HTTPStatus.OK,
                ProbeApiResponse(status="live").model_dump(mode="json"),
            )
        if method == "GET" and segments == ("readyz",):
            report = self._health.report()
            status = HTTPStatus.OK if report.ready else HTTPStatus.SERVICE_UNAVAILABLE
            return ApiResponse(
                status,
                ProbeApiResponse(status="ready" if report.ready else "unavailable").model_dump(
                    mode="json"
                ),
            )
        if method == "GET" and segments == ("healthz",):
            await self._authorize(method, path, headers, AuthorizationAction.HEALTH, None)
            self._metrics.record_api("health", ApiMetricOutcome.SUCCESS)
            return ApiResponse(HTTPStatus.OK, self._health.report().model_dump(mode="json"))
        if method == "GET" and segments == ("metrics",):
            await self._authorize(method, path, headers, AuthorizationAction.METRICS, None)
            self._metrics.record_api("metrics", ApiMetricOutcome.SUCCESS)
            return ApiResponse(
                HTTPStatus.OK,
                content_type=PROMETHEUS_CONTENT_TYPE,
                raw_body=self._metrics.render_prometheus(),
            )
        if method == "POST" and len(segments) == 2 and segments[0] == "webhooks":
            return await self._webhook(segments[1], headers, receive)
        if method == "POST" and len(segments) == 2 and segments[0] == "events":
            return await self._cloud_event(
                method,
                path,
                segments[1],
                headers,
                receive,
            )
        if segments and segments[0] == "admin":
            return await self._admin_route(method, path, segments, query, headers)
        if not segments or segments[0] != "v1":
            raise RequestRejectedError(HTTPStatus.NOT_FOUND, "not_found", "Route not found")
        if len(segments) >= 2 and segments[:2] == ("v1", "configuration"):
            return await self._configuration_route(
                method,
                path,
                segments,
                query,
                headers,
                receive,
            )
        if len(segments) >= 2 and segments[:2] == ("v1", "operations"):
            if segments == ("v1", "operations", "capabilities"):
                return await self._capabilities(method, path, query, headers)
            return await self._operations_route(
                method,
                path,
                segments,
                query,
                headers,
            )
        return await self._control_route(method, path, segments, query, headers, receive)

    async def _admin_route(
        self,
        method: str,
        path: str,
        segments: tuple[str, ...],
        query: Mapping[str, list[str]],
        headers: tuple[tuple[bytes, bytes], ...],
    ) -> ApiResponse:
        panel = self._admin_panel
        # Query strings are client-side routing state (e.g. ?tab=definition on a
        # bookmarked SPA URL); the panel never reads them server-side.
        del query
        if panel is None or method != "GET" or not panel.has_route(segments):
            raise RequestRejectedError(HTTPStatus.NOT_FOUND, "not_found", "Route not found")
        await self._authorize(
            method,
            path,
            headers,
            AuthorizationAction.ADMIN_PANEL_VIEW,
            None,
        )
        asset = panel.resolve(segments)
        if asset is None:
            raise RuntimeError("Administration panel route disappeared after authorization")
        self._metrics.record_api("admin_panel_view", ApiMetricOutcome.SUCCESS)
        return ApiResponse(
            HTTPStatus.OK,
            content_type=asset.content_type,
            raw_body=asset.body,
            headers=asset.headers,
        )

    async def _operations_route(
        self,
        method: str,
        path: str,
        segments: tuple[str, ...],
        query: Mapping[str, list[str]],
        headers: tuple[tuple[bytes, bytes], ...],
    ) -> ApiResponse:
        api = self._operations_api
        if api is None:
            raise RequestRejectedError(
                HTTPStatus.NOT_FOUND,
                "not_found",
                "Operations API is not available",
            )
        try:
            route = api.resolve_route(method, segments)
        except OperationsApiRequestError as exc:
            raise RequestRejectedError(exc.status, exc.code, str(exc)) from exc
        scope_binding = await self._authorize(
            method,
            path,
            headers,
            route.action,
            route.resource,
        )
        try:
            response = await api.dispatch(
                route,
                query=query,
                scope_binding=scope_binding,
            )
        except OperationsApiRequestError as exc:
            outcome = (
                ApiMetricOutcome.SERVER_ERROR
                if exc.status >= HTTPStatus.INTERNAL_SERVER_ERROR
                else ApiMetricOutcome.CLIENT_ERROR
            )
            self._metrics.record_api(route.kind.value, outcome)
            raise RequestRejectedError(exc.status, exc.code, str(exc)) from exc
        self._metrics.record_api(route.kind.value, ApiMetricOutcome.SUCCESS)
        return ApiResponse(status=response.status, payload=response.payload)

    async def _capabilities(
        self,
        method: str,
        path: str,
        query: Mapping[str, list[str]],
        headers: tuple[tuple[bytes, bytes], ...],
    ) -> ApiResponse:
        if method != "GET" or query:
            raise RequestRejectedError(HTTPStatus.NOT_FOUND, "not_found", "Route not found")
        context = await self._authentication_context(
            method,
            path,
            headers,
            AuthorizationAction.OPERATIONS_VIEW,
        )
        await self._require_authorized(
            context,
            AuthorizationAction.OPERATIONS_VIEW,
            None,
        )
        configuration_actions = (
            AuthorizationAction.CONFIGURATION_VIEW,
            AuthorizationAction.CONFIGURATION_EDIT,
            AuthorizationAction.CONFIGURATION_VALIDATE,
            AuthorizationAction.CONFIGURATION_APPLY,
            AuthorizationAction.CONFIGURATION_DISCARD,
            AuthorizationAction.CONFIGURATION_PUBLISH,
            AuthorizationAction.CONFIGURATION_ACTIVATE,
            AuthorizationAction.CONFIGURATION_ROLLBACK,
        )
        capabilities: dict[str, object] = {
            "api_compatibility": ApiCompatibilityResponse.from_contract(PUBLIC_API_COMPATIBILITY),
            "operations_view": True,
            "scope": context.scope_binding.scope.model_dump_public(),
            "configuration_mode": (
                self._configuration_api.authoring_mode.value
                if self._configuration_api is not None
                else "unavailable"
            ),
        }
        for action in configuration_actions:
            capabilities[action.value] = (
                self._configuration_api is not None
                and action in self._configuration_api.supported_actions
                and await self._authorization_allowed(
                    context,
                    action,
                    None,
                )
            )
        workflow_actions = {
            "workflow_start": AuthorizationAction.START,
            "workflow_signal": AuthorizationAction.SIGNAL,
            "workflow_cancel": AuthorizationAction.CANCEL,
            "workflow_terminate": AuthorizationAction.TERMINATE,
        }
        for capability, action in workflow_actions.items():
            capabilities[capability] = await self._authorization_allowed(
                context,
                action,
                None,
            )
        trigger_actions = {
            "trigger_pause": AuthorizationAction.TRIGGER_PAUSE,
            "trigger_resume": AuthorizationAction.TRIGGER_RESUME,
            "trigger_run": AuthorizationAction.TRIGGER_RUN,
            "trigger_delete": AuthorizationAction.TRIGGER_DELETE,
        }
        for capability, action in trigger_actions.items():
            capabilities[capability] = (
                self._schedule_controls is not None
                and await self._authorization_allowed(
                    context,
                    action,
                    None,
                )
            )
        scheduled_start_actions = {
            "scheduled_start_create": AuthorizationAction.SCHEDULED_START_CREATE,
            "scheduled_start_view": AuthorizationAction.SCHEDULED_START_VIEW,
            "scheduled_start_reschedule": AuthorizationAction.SCHEDULED_START_RESCHEDULE,
            "scheduled_start_cancel": AuthorizationAction.SCHEDULED_START_CANCEL,
        }
        for capability, action in scheduled_start_actions.items():
            capabilities[capability] = self._scheduled_starts is not None and (
                await self._authorization_allowed(context, action, None)
            )
        capabilities["trigger_apply"] = self._schedule_applier is not None and (
            await self._authorization_allowed(
                context,
                AuthorizationAction.TRIGGER_APPLY,
                None,
            )
        )
        self._metrics.record_api("operations_capabilities", ApiMetricOutcome.SUCCESS)
        return ApiResponse(
            HTTPStatus.OK,
            CapabilitiesApiResponse.model_validate(capabilities).model_dump(mode="json"),
        )

    async def _configuration_route(
        self,
        method: str,
        path: str,
        segments: tuple[str, ...],
        query: Mapping[str, list[str]],
        headers: tuple[tuple[bytes, bytes], ...],
        receive: AsgiReceive,
    ) -> ApiResponse:
        api = self._configuration_api
        if api is None:
            raise RequestRejectedError(
                HTTPStatus.NOT_FOUND,
                "not_found",
                "Configuration API is not available",
            )
        try:
            route = api.resolve_route(method, segments)
        except ConfigurationApiRequestError as exc:
            raise RequestRejectedError(exc.status, exc.code, str(exc)) from exc
        scope_binding = await self._authorize(
            method,
            path,
            headers,
            route.action,
            route.resource,
        )
        try:
            response = await api.dispatch(
                route,
                query=query,
                headers=headers,
                read_body=lambda: self._read_body(receive),
                scope_binding=scope_binding,
            )
        except ConfigurationApiRequestError as exc:
            outcome = (
                ApiMetricOutcome.SERVER_ERROR
                if exc.status >= HTTPStatus.INTERNAL_SERVER_ERROR
                else ApiMetricOutcome.CLIENT_ERROR
            )
            self._metrics.record_api(route.kind.value, outcome)
            raise RequestRejectedError(exc.status, exc.code, str(exc)) from exc
        self._metrics.record_api(route.kind.value, ApiMetricOutcome.SUCCESS)
        return ApiResponse(
            status=response.status,
            payload=response.payload,
            content_type=response.content_type or JSON_CONTENT_TYPE,
            raw_body=response.raw_body,
        )

    async def _control_route(
        self,
        method: str,
        path: str,
        segments: tuple[str, ...],
        query: Mapping[str, list[str]],
        headers: tuple[tuple[bytes, bytes], ...],
        receive: AsgiReceive,
    ) -> ApiResponse:
        if segments == ("v1", "scheduled-starts") and method == "POST":
            scope_binding = await self._authorize(
                method,
                path,
                headers,
                AuthorizationAction.SCHEDULED_START_CREATE,
                None,
            )
            return await self._create_scheduled_start(
                query,
                headers,
                receive,
                scope_binding,
            )
        if (
            len(segments) == 4
            and segments[:2] == ("v1", "scheduled-starts")
            and segments[3] in {"reschedule", "cancel"}
            and method == "POST"
        ):
            scheduled_start_id = segments[2]
            action = (
                AuthorizationAction.SCHEDULED_START_RESCHEDULE
                if segments[3] == "reschedule"
                else AuthorizationAction.SCHEDULED_START_CANCEL
            )
            scope_binding = await self._authorize(
                method,
                path,
                headers,
                action,
                scheduled_start_id,
            )
            mutation: Literal["reschedule", "cancel"] = (
                "reschedule" if segments[3] == "reschedule" else "cancel"
            )
            return await self._mutate_scheduled_start(
                scheduled_start_id,
                mutation,
                query,
                headers,
                receive,
                scope_binding,
            )
        if segments == ("v1", "triggers", "apply") and method == "POST":
            scope_binding = await self._authorize(
                method,
                path,
                headers,
                AuthorizationAction.TRIGGER_APPLY,
                None,
            )
            return await self._trigger_apply(scope_binding.scope)
        if (
            len(segments) == 4
            and segments[:2] == ("v1", "triggers")
            and segments[3] in {"pause", "resume", "run", "delete"}
            and method == "POST"
        ):
            trigger_name = segments[2]
            trigger_action = {
                "pause": AuthorizationAction.TRIGGER_PAUSE,
                "resume": AuthorizationAction.TRIGGER_RESUME,
                "run": AuthorizationAction.TRIGGER_RUN,
                "delete": AuthorizationAction.TRIGGER_DELETE,
            }[segments[3]]
            scope_binding = await self._authorize(
                method,
                path,
                headers,
                trigger_action,
                trigger_name,
            )
            trigger_operation: Literal["pause", "resume", "run", "delete"] = (
                "pause"
                if segments[3] == "pause"
                else "resume"
                if segments[3] == "resume"
                else "run"
                if segments[3] == "run"
                else "delete"
            )
            return await self._trigger_control(
                trigger_name,
                trigger_operation,
                query,
                headers,
                receive,
                scope_binding.scope,
            )
        if segments == ("v1", "workflows") and method == "POST":
            scope_binding = await self._authorize(
                method,
                path,
                headers,
                AuthorizationAction.START,
                None,
            )
            return await self._start(receive, scope_binding)
        if segments == ("v1", "workflows") and method == "GET":
            scope_binding = await self._authorize(
                method,
                path,
                headers,
                AuthorizationAction.LIST,
                None,
            )
            return await self._list(query, scope_binding.scope)
        if len(segments) == 3 and segments[:2] == ("v1", "workflows") and method == "GET":
            workflow_id = segments[2]
            scope_binding = await self._authorize(
                method,
                path,
                headers,
                AuthorizationAction.DESCRIBE,
                workflow_id,
            )
            return await self._describe(workflow_id, query, scope_binding.scope)
        if (
            len(segments) == 5
            and segments[:2] == ("v1", "workflows")
            and segments[3] == "events"
            and method == "POST"
        ):
            workflow_id = segments[2]
            scope_binding = await self._authorize(
                method,
                path,
                headers,
                AuthorizationAction.SIGNAL,
                workflow_id,
            )
            return await self._signal(
                workflow_id,
                segments[4],
                query,
                receive,
                scope_binding.scope,
            )
        if (
            len(segments) == 4
            and segments[:2] == ("v1", "workflows")
            and segments[3] in {"cancel", "terminate"}
            and method == "POST"
        ):
            workflow_id = segments[2]
            action = (
                AuthorizationAction.CANCEL
                if segments[3] == "cancel"
                else AuthorizationAction.TERMINATE
            )
            scope_binding = await self._authorize(
                method,
                path,
                headers,
                action,
                workflow_id,
            )
            operation: Literal["cancel", "terminate"] = (
                "cancel" if segments[3] == "cancel" else "terminate"
            )
            return await self._stop_execution(
                workflow_id,
                operation,
                query,
                scope_binding.scope,
            )
        raise RequestRejectedError(HTTPStatus.NOT_FOUND, "not_found", "Route not found")

    async def _create_scheduled_start(
        self,
        query: Mapping[str, list[str]],
        headers: tuple[tuple[bytes, bytes], ...],
        receive: AsgiReceive,
        scope_binding: TrustedScopeBinding,
    ) -> ApiResponse:
        operation = AuthorizationAction.SCHEDULED_START_CREATE.value
        service = self._require_scheduled_starts()
        try:
            _only_query_fields(query, set())
            request = ScheduledStartCreateRequest.model_validate(await self._read_json(receive))
            idempotency_key = _required_header(headers, IDEMPOTENCY_HEADER)
            result = await service.create(
                request,
                idempotency_key=idempotency_key,
                scope_binding=scope_binding,
            )
        except (ValidationError, ValueError) as exc:
            self._record_client_error(operation)
            raise RequestRejectedError(
                HTTPStatus.UNPROCESSABLE_ENTITY,
                "invalid_request",
                "Scheduled-start request is invalid",
            ) from exc
        except ScheduledStartError as exc:
            raise self._translate_scheduled_start_error(operation, exc) from exc
        self._metrics.record_api(operation, ApiMetricOutcome.SUCCESS)
        status = (
            HTTPStatus.ACCEPTED
            if result.status is ScheduledStartMutationStatus.ACCEPTED
            else HTTPStatus.OK
        )
        response = ScheduledStartCreateApiResponse(
            **result.model_dump(mode="python"),
            status_url=(
                f"/v1/operations/scheduled-starts/{result.scheduled_start.scheduled_start_id}"
            ),
        )
        return ApiResponse(status, response.model_dump(mode="json"))

    async def _mutate_scheduled_start(
        self,
        scheduled_start_id: str,
        mutation: Literal["reschedule", "cancel"],
        query: Mapping[str, list[str]],
        headers: tuple[tuple[bytes, bytes], ...],
        receive: AsgiReceive,
        scope_binding: TrustedScopeBinding,
    ) -> ApiResponse:
        operation = (
            AuthorizationAction.SCHEDULED_START_RESCHEDULE.value
            if mutation == "reschedule"
            else AuthorizationAction.SCHEDULED_START_CANCEL.value
        )
        service = self._require_scheduled_starts()
        try:
            _only_query_fields(query, set())
            idempotency_key = _required_header(headers, IDEMPOTENCY_HEADER)
            body = await self._read_json(receive)
            if mutation == "reschedule":
                result = await service.reschedule(
                    scheduled_start_id,
                    ScheduledStartRescheduleRequest.model_validate(body),
                    idempotency_key=idempotency_key,
                    scope=scope_binding.scope,
                )
            else:
                result = await service.cancel(
                    scheduled_start_id,
                    ScheduledStartCancelRequest.model_validate(body),
                    idempotency_key=idempotency_key,
                    scope=scope_binding.scope,
                )
        except (ValidationError, ValueError) as exc:
            self._record_client_error(operation)
            raise RequestRejectedError(
                HTTPStatus.UNPROCESSABLE_ENTITY,
                "invalid_request",
                "Scheduled-start mutation is invalid",
            ) from exc
        except ScheduledStartError as exc:
            raise self._translate_scheduled_start_error(operation, exc) from exc
        self._metrics.record_api(operation, ApiMetricOutcome.SUCCESS)
        status = (
            HTTPStatus.ACCEPTED
            if result.status is ScheduledStartMutationStatus.IN_PROGRESS
            else HTTPStatus.OK
        )
        return ApiResponse(status, result.model_dump(mode="json"))

    def _require_scheduled_starts(self) -> ScheduledStartService:
        if self._scheduled_starts is None:
            raise RequestRejectedError(
                HTTPStatus.NOT_FOUND,
                "not_found",
                "Scheduled starts are not available",
            )
        return self._scheduled_starts

    async def _start(
        self,
        receive: AsgiReceive,
        scope_binding: TrustedScopeBinding,
    ) -> ApiResponse:
        operation = "start"
        try:
            body = StartApiRequest.model_validate(await self._read_json(receive))
            result = await self._starter.start(
                StartWorkflowRequest(
                    workflow_name=body.workflow_name,
                    business_request_id=body.business_request_id,
                    input=body.input,
                    source=ControlApiSourceIdentity(request_id=body.business_request_id),
                    definition_digest=body.definition_digest,
                    correlation_id=body.correlation_id,
                    trace_id=body.trace_id,
                ),
                scope_binding=scope_binding,
            )
        except ValidationError as exc:
            self._record_client_error(operation)
            raise RequestRejectedError(
                HTTPStatus.UNPROCESSABLE_ENTITY,
                "invalid_request",
                "Workflow start request is invalid",
            ) from exc
        except WorkflowStartError as exc:
            raise self._translate_start_error(operation, exc) from exc
        self._metrics.record_api(operation, ApiMetricOutcome.SUCCESS)
        status = HTTPStatus.ACCEPTED if result.status is StartStatus.STARTED else HTTPStatus.OK
        response = StartWorkflowApiResponse(
            **result.model_dump(mode="python"),
            status_url=f"/v1/workflows/{result.workflow_id}",
        )
        return ApiResponse(status, response.model_dump(mode="json"))

    async def _list(
        self,
        query: Mapping[str, list[str]],
        scope: RuntimeScope,
    ) -> ApiResponse:
        operation = "list"
        try:
            _only_query_fields(query, {"limit", "page_token"})
            limit_text = _single_query_value(query, "limit")
            limit = self._settings.default_list_limit if limit_text is None else int(limit_text)
            result = await self._controls.list(
                limit=limit,
                page_token=_single_query_value(query, "page_token"),
                scope=scope,
            )
        except ValueError as exc:
            self._record_client_error(operation)
            raise RequestRejectedError(
                HTTPStatus.BAD_REQUEST,
                "invalid_request",
                "Workflow list query is invalid",
            ) from exc
        except ControlOperationError as exc:
            raise self._translate_control_error(operation, exc) from exc
        for workflow in result.workflows:
            self._metrics.record_execution_observation(workflow.status)
        self._metrics.record_api(operation, ApiMetricOutcome.SUCCESS)
        return ApiResponse(HTTPStatus.OK, result.model_dump(mode="json"))

    async def _describe(
        self,
        workflow_id: str,
        query: Mapping[str, list[str]],
        scope: RuntimeScope,
    ) -> ApiResponse:
        operation = "describe"
        try:
            _only_query_fields(query, {"run_id"})
            result = await self._controls.describe(
                workflow_id,
                run_id=_single_query_value(query, "run_id"),
                scope=scope,
            )
        except ValueError as exc:
            self._record_client_error(operation)
            raise RequestRejectedError(
                HTTPStatus.BAD_REQUEST,
                "invalid_request",
                "Workflow description query is invalid",
            ) from exc
        except ControlOperationError as exc:
            raise self._translate_control_error(operation, exc) from exc
        self._metrics.record_execution_observation(result.status)
        self._metrics.record_api(operation, ApiMetricOutcome.SUCCESS)
        return ApiResponse(HTTPStatus.OK, result.model_dump(mode="json"))

    async def _signal(
        self,
        workflow_id: str,
        signal_name: str,
        query: Mapping[str, list[str]],
        receive: AsgiReceive,
        scope: RuntimeScope,
    ) -> ApiResponse:
        operation = "signal"
        try:
            _only_query_fields(query, {"run_id"})
            body_payload = await self._read_json(receive)
            if not isinstance(body_payload, dict):
                raise TypeError("Event request must be an object")
            event = SignalEventApiRequest.model_validate(body_payload)
            body = SignalApiRequest(event_name=signal_name, payload=event.payload)
            await self._controls.signal_event(
                workflow_id,
                body.event_name,
                body.payload,
                run_id=_single_query_value(query, "run_id"),
                scope=scope,
            )
        except (TypeError, ValidationError, ValueError) as exc:
            self._record_client_error(operation)
            raise RequestRejectedError(
                HTTPStatus.UNPROCESSABLE_ENTITY,
                "invalid_request",
                "Workflow event request is invalid",
            ) from exc
        except ControlOperationError as exc:
            raise self._translate_control_error(operation, exc) from exc
        self._metrics.record_api(operation, ApiMetricOutcome.SUCCESS)
        return ApiResponse(
            HTTPStatus.ACCEPTED,
            StatusApiResponse(status="accepted").model_dump(mode="json"),
        )

    async def _stop_execution(
        self,
        workflow_id: str,
        operation: Literal["cancel", "terminate"],
        query: Mapping[str, list[str]],
        scope: RuntimeScope,
    ) -> ApiResponse:
        try:
            _only_query_fields(query, {"run_id"})
            run_id = _single_query_value(query, "run_id")
            if operation == "cancel":
                await self._controls.cancel(workflow_id, run_id=run_id, scope=scope)
            else:
                await self._controls.terminate(workflow_id, run_id=run_id, scope=scope)
        except ValueError as exc:
            self._record_client_error(operation)
            raise RequestRejectedError(
                HTTPStatus.BAD_REQUEST,
                "invalid_request",
                "Workflow operation query is invalid",
            ) from exc
        except ControlOperationError as exc:
            raise self._translate_control_error(operation, exc) from exc
        self._metrics.record_api(operation, ApiMetricOutcome.SUCCESS)
        return ApiResponse(
            HTTPStatus.ACCEPTED,
            StatusApiResponse(status="accepted").model_dump(mode="json"),
        )

    async def _trigger_apply(self, scope: RuntimeScope) -> ApiResponse:
        applier = self._schedule_applier
        if applier is None:
            raise RequestRejectedError(
                HTTPStatus.NOT_FOUND,
                "not_found",
                "Trigger apply is not available",
            )
        if scope != self._local_scope:
            raise RequestRejectedError(
                HTTPStatus.FORBIDDEN,
                "forbidden",
                "Trigger apply is bound to another runtime scope",
            )
        try:
            result = await applier.apply()
        except ScheduleReconciliationError as exc:
            status = {
                ScheduleReconciliationErrorCode.CATALOG_UNAVAILABLE: HTTPStatus.SERVICE_UNAVAILABLE,
                ScheduleReconciliationErrorCode.COLLECTION_LIMIT: HTTPStatus.SERVICE_UNAVAILABLE,
                ScheduleReconciliationErrorCode.CONFIRMATION_REQUIRED: HTTPStatus.CONFLICT,
                ScheduleReconciliationErrorCode.PLAN_CONFLICT: HTTPStatus.CONFLICT,
                ScheduleReconciliationErrorCode.TEMPORAL_UNAVAILABLE: (
                    HTTPStatus.SERVICE_UNAVAILABLE
                ),
            }[exc.code]
            self._metrics.record_api("trigger_apply", ApiMetricOutcome.SERVER_ERROR)
            raise RequestRejectedError(status, exc.code.value, str(exc)) from exc
        self._metrics.record_api("trigger_apply", ApiMetricOutcome.SUCCESS)
        return ApiResponse(
            status=HTTPStatus.OK,
            payload=TriggerApplyApiResponse(
                plan_digest=result.plan_digest,
                successful=result.successful,
                items=tuple(
                    TriggerApplyItemApiResponse(
                        schedule_id=item.schedule_id,
                        trigger_name=item.schedule_name,
                        change=item.change,
                        status=item.status,
                        error_code=item.error_code,
                    )
                    for item in result.items
                ),
            ).model_dump(mode="json"),
        )

    async def _trigger_control(
        self,
        trigger_name: str,
        operation: Literal["pause", "resume", "run", "delete"],
        query: Mapping[str, list[str]],
        headers: tuple[tuple[bytes, bytes], ...],
        receive: AsgiReceive,
        scope: RuntimeScope,
    ) -> ApiResponse:
        controls = self._schedule_controls
        if controls is None:
            raise RequestRejectedError(
                HTTPStatus.NOT_FOUND,
                "not_found",
                "Schedule-trigger controls are not available",
            )
        metric_operation = f"trigger_{operation}"
        claim_identity: str | None = None
        run_status: TriggerRunNowStatus | None = None
        try:
            _only_query_fields(query, set())
            trigger = TriggerPathApiRequest(trigger_name=trigger_name)
            if operation == "pause":
                if await self._read_body(receive):
                    raise ValueError("Pause request body must be empty")
                await controls.pause(trigger.trigger_name, scope=scope)
            elif operation == "resume":
                if await self._read_body(receive):
                    raise ValueError("Resume request body must be empty")
                await controls.resume(trigger.trigger_name, scope=scope)
            elif operation == "run":
                if await self._read_body(receive):
                    raise ValueError("Run-now request body must be empty")
                idempotency_key = _required_header(headers, IDEMPOTENCY_HEADER)
                if len(idempotency_key) > MAX_IDEMPOTENCY_KEY_LENGTH:
                    raise ValueError("Idempotency key exceeds its bound")
                claim_identity = safe_identity_digest(
                    "trigger-run-now",
                    f"{scope.digest}:{trigger.trigger_name}:{idempotency_key}",
                )
                run_status = await controls.trigger_now(
                    trigger.trigger_name,
                    request_identity_digest=claim_identity,
                    scope=scope,
                )
            else:
                request = TriggerDeleteApiRequest.model_validate(await self._read_json(receive))
                await controls.delete(
                    trigger.trigger_name,
                    confirmation=request.confirmation,
                    scope=scope,
                )
        except (ValidationError, ValueError) as exc:
            self._record_client_error(metric_operation)
            raise RequestRejectedError(
                HTTPStatus.UNPROCESSABLE_ENTITY,
                "invalid_request",
                "Schedule-trigger operation request is invalid",
            ) from exc
        except ScheduleOperationError as exc:
            status, message = {
                ScheduleOperationErrorCode.UNKNOWN_SCHEDULE: (
                    HTTPStatus.NOT_FOUND,
                    "Schedule trigger was not found",
                ),
                ScheduleOperationErrorCode.NOT_MANAGED: (
                    HTTPStatus.NOT_FOUND,
                    "Schedule trigger was not found",
                ),
                ScheduleOperationErrorCode.INVALID_OPERATION: (
                    HTTPStatus.CONFLICT,
                    "Schedule-trigger operation is not valid in its current state",
                ),
                ScheduleOperationErrorCode.CONFIRMATION_REQUIRED: (
                    HTTPStatus.CONFLICT,
                    "Schedule-trigger deletion confirmation is stale",
                ),
                ScheduleOperationErrorCode.TEMPORAL_UNAVAILABLE: (
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    "Schedule-trigger operation is temporarily unavailable",
                ),
            }[exc.code]
            outcome = (
                ApiMetricOutcome.SERVER_ERROR
                if status >= HTTPStatus.INTERNAL_SERVER_ERROR
                else ApiMetricOutcome.CLIENT_ERROR
            )
            self._metrics.record_api(metric_operation, outcome)
            raise RequestRejectedError(status, exc.code.value, message) from exc
        self._metrics.record_api(metric_operation, ApiMetricOutcome.SUCCESS)
        if operation == "run":
            if claim_identity is None or run_status is None:
                raise RuntimeError("Accepted run-now request has no idempotency identity")
            logger.info(
                "Schedule-trigger run-now request completed",
                extra={
                    "trigger_name": trigger_name,
                    "scope_digest": scope.digest,
                    "idempotency_digest": claim_identity,
                    "trigger_run_status": run_status.value,
                },
            )
            status = (
                HTTPStatus.ACCEPTED if run_status is TriggerRunNowStatus.ACCEPTED else HTTPStatus.OK
            )
            return ApiResponse(
                status,
                StatusApiResponse(status=run_status.value).model_dump(mode="json"),
            )
        return ApiResponse(
            HTTPStatus.OK,
            StatusApiResponse(status=f"{operation}d").model_dump(mode="json"),
        )

    async def _webhook(
        self,
        source_name: str,
        headers: tuple[tuple[bytes, bytes], ...],
        receive: AsgiReceive,
    ) -> ApiResponse:
        if self._webhooks is None:
            raise RequestRejectedError(
                HTTPStatus.NOT_FOUND,
                "not_found",
                "Webhook source is not available",
            )
        body = await self._read_body(receive)
        try:
            result = await self._webhooks.receive(
                source_name,
                SignedWebhookRequest(headers=headers, body=body),
            )
        except WebhookError as exc:
            status = {
                WebhookErrorCode.UNKNOWN_SOURCE: HTTPStatus.NOT_FOUND,
                WebhookErrorCode.VERIFICATION_FAILED: HTTPStatus.UNAUTHORIZED,
                WebhookErrorCode.INVALID_PAYLOAD: HTTPStatus.BAD_REQUEST,
                WebhookErrorCode.INVALID_IDENTITY: HTTPStatus.BAD_REQUEST,
                WebhookErrorCode.PROVIDER_UNAVAILABLE: HTTPStatus.SERVICE_UNAVAILABLE,
            }[exc.code]
            raise RequestRejectedError(status, exc.code.value, str(exc)) from exc
        except WorkflowStartError as exc:
            raise self._translate_start_error("start", exc) from exc
        status = HTTPStatus.ACCEPTED if result.status is StartStatus.STARTED else HTTPStatus.OK
        response = StartWorkflowApiResponse(
            **result.model_dump(mode="python"),
            status_url=f"/v1/workflows/{result.workflow_id}",
        )
        return ApiResponse(status, response.model_dump(mode="json"))

    async def _cloud_event(
        self,
        method: str,
        path: str,
        mapping_name: str,
        headers: tuple[tuple[bytes, bytes], ...],
        receive: AsgiReceive,
    ) -> ApiResponse:
        ingress = self._cloud_events
        if ingress is None:
            raise RequestRejectedError(
                HTTPStatus.NOT_FOUND,
                "not_found",
                "Cloud-event ingress is not available",
            )
        context = await self._authentication_context(
            method,
            path,
            headers,
            AuthorizationAction.START,
        )
        try:
            bound_scope = ingress.scope_for(mapping_name)
        except CloudEventMappingError as exc:
            raise RequestRejectedError(
                HTTPStatus.NOT_FOUND,
                "not_found",
                "Cloud-event mapping is not available",
            ) from exc
        if context.scope_binding.scope != bound_scope:
            self._metrics.record_api(
                AuthorizationAction.START.value,
                ApiMetricOutcome.AUTH_ERROR,
            )
            raise RequestRejectedError(
                HTTPStatus.FORBIDDEN,
                "forbidden",
                "The authenticated principal is not authorized",
            )
        await self._require_authorized(
            context,
            AuthorizationAction.START,
            f"cloud-event:{mapping_name}",
        )
        event = await self._read_json(receive)
        try:
            result = await ingress.receive(mapping_name, event)
        except CloudEventMappingError as exc:
            status = (
                HTTPStatus.SERVICE_UNAVAILABLE if exc.retryable else HTTPStatus.UNPROCESSABLE_ENTITY
            )
            code = (
                "cloud_event_unavailable"
                if exc.code is CloudEventErrorCode.MAPPING_UNAVAILABLE
                else exc.code.value
            )
            outcome = (
                ApiMetricOutcome.SERVER_ERROR if exc.retryable else ApiMetricOutcome.CLIENT_ERROR
            )
            self._metrics.record_api(AuthorizationAction.START.value, outcome)
            raise RequestRejectedError(
                status,
                code,
                CLOUD_EVENT_PUBLIC_ERRORS[exc.code],
            ) from exc
        except WorkflowStartError as exc:
            raise self._translate_start_error(AuthorizationAction.START.value, exc) from exc
        self._metrics.record_api(AuthorizationAction.START.value, ApiMetricOutcome.SUCCESS)
        status = HTTPStatus.ACCEPTED if result.status is StartStatus.STARTED else HTTPStatus.OK
        response = StartWorkflowApiResponse(
            **result.model_dump(mode="python"),
            status_url=f"/v1/workflows/{result.workflow_id}",
        )
        return ApiResponse(status, response.model_dump(mode="json"))

    async def _authorize(
        self,
        method: str,
        path: str,
        headers: tuple[tuple[bytes, bytes], ...],
        action: AuthorizationAction,
        resource: str | None,
    ) -> TrustedScopeBinding:
        context = await self._authentication_context(method, path, headers, action)
        await self._require_authorized(context, action, resource)
        return context.scope_binding

    async def _authentication_context(
        self,
        method: str,
        path: str,
        headers: tuple[tuple[bytes, bytes], ...],
        action: AuthorizationAction,
    ) -> AuthenticationContext:
        provider = self._authentication
        if provider is None:
            return AuthenticationContext(
                principal=None,
                scope_binding=TrustedScopeBinding.create(
                    kind=ScopeBindingKind.API,
                    scope=self._local_scope,
                    binding_id="local-control",
                ),
            )
        try:
            principal = await provider.authenticate(
                AuthenticationRequest(method=method, path=path, headers=headers)
            )
            scope_binding = principal.scope_binding
            require_scope_grant(
                principal.scope_grants,
                scope_binding.scope,
            )
        except AuthenticationError as exc:
            self._metrics.record_api(action.value, ApiMetricOutcome.AUTH_ERROR)
            raise RequestRejectedError(
                HTTPStatus.UNAUTHORIZED,
                "unauthenticated",
                "Authentication is required",
            ) from exc
        except ScopeResolutionError as exc:
            self._metrics.record_api(action.value, ApiMetricOutcome.AUTH_ERROR)
            raise RequestRejectedError(
                HTTPStatus.FORBIDDEN,
                "forbidden",
                "The authenticated principal is not authorized",
            ) from exc
        except Exception as exc:
            self._metrics.record_api(action.value, ApiMetricOutcome.SERVER_ERROR)
            raise RequestRejectedError(
                HTTPStatus.SERVICE_UNAVAILABLE,
                "authentication_unavailable",
                "Authentication is unavailable",
            ) from exc
        return AuthenticationContext(principal=principal, scope_binding=scope_binding)

    async def _require_authorized(
        self,
        context: AuthenticationContext,
        action: AuthorizationAction,
        resource: str | None,
    ) -> None:
        if await self._authorization_allowed(context, action, resource):
            return
        self._metrics.record_api(action.value, ApiMetricOutcome.AUTH_ERROR)
        raise RequestRejectedError(
            HTTPStatus.FORBIDDEN,
            "forbidden",
            "The authenticated principal is not authorized",
        )

    async def _authorization_allowed(
        self,
        context: AuthenticationContext,
        action: AuthorizationAction,
        resource: str | None,
    ) -> bool:
        provider = self._authentication
        principal = context.principal
        if provider is None or principal is None:
            return True
        effective_scope = context.scope_binding.scope
        try:
            return await provider.authorize(
                principal,
                AuthorizationRequest(
                    action=action,
                    scope=effective_scope,
                    resource_identity_digest=authorization_resource_digest(
                        effective_scope,
                        resource,
                    ),
                ),
            )
        except Exception as exc:
            self._metrics.record_api(action.value, ApiMetricOutcome.SERVER_ERROR)
            raise RequestRejectedError(
                HTTPStatus.SERVICE_UNAVAILABLE,
                "authentication_unavailable",
                "Authentication is unavailable",
            ) from exc

    async def _read_json(self, receive: AsgiReceive) -> object:
        body = await self._read_body(receive)
        try:
            return json.loads(
                body,
                object_pairs_hook=_reject_duplicate_keys,
                parse_constant=_reject_non_finite,
            )
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            raise RequestRejectedError(
                HTTPStatus.BAD_REQUEST,
                "invalid_json",
                "Request body must be strict JSON",
            ) from exc

    async def _read_body(self, receive: AsgiReceive) -> bytes:
        chunks: list[bytes] = []
        size = 0
        while True:
            message = await receive()
            if message.get("type") == "http.disconnect":
                raise RequestRejectedError(
                    HTTPStatus.BAD_REQUEST,
                    "client_disconnected",
                    "Client disconnected before the request completed",
                )
            if message.get("type") != "http.request":
                raise RequestRejectedError(
                    HTTPStatus.BAD_REQUEST,
                    "invalid_request",
                    "Request body could not be read",
                )
            chunk = message.get("body", b"")
            size += len(chunk)
            if size > self._settings.max_request_body_bytes:
                raise RequestRejectedError(
                    HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                    "request_too_large",
                    "Request body exceeds its configured byte limit",
                )
            chunks.append(chunk)
            if not message.get("more_body", False):
                return b"".join(chunks)

    def _validated_headers(
        self,
        headers: Sequence[tuple[bytes, bytes]],
    ) -> tuple[tuple[bytes, bytes], ...]:
        if len(headers) > self._settings.max_header_count:
            raise RequestRejectedError(
                HTTPStatus.REQUEST_HEADER_FIELDS_TOO_LARGE,
                "headers_too_large",
                "Request headers exceed their configured count limit",
            )
        total = sum(len(name) + len(value) for name, value in headers)
        if total > self._settings.max_header_bytes:
            raise RequestRejectedError(
                HTTPStatus.REQUEST_HEADER_FIELDS_TOO_LARGE,
                "headers_too_large",
                "Request headers exceed their configured byte limit",
            )
        return tuple(headers)

    def _parse_query(self, query: bytes) -> Mapping[str, list[str]]:
        if len(query) > self._settings.max_query_bytes:
            raise RequestRejectedError(
                HTTPStatus.REQUEST_URI_TOO_LONG,
                "query_too_large",
                "Request query exceeds its configured byte limit",
            )
        try:
            return parse_qs(
                query.decode("ascii"),
                keep_blank_values=True,
                strict_parsing=True,
                max_num_fields=self._settings.max_header_count,
            )
        except (UnicodeDecodeError, ValueError) as exc:
            raise RequestRejectedError(
                HTTPStatus.BAD_REQUEST,
                "invalid_query",
                "Request query is invalid",
            ) from exc

    async def _send(self, send: AsgiSend, response: ApiResponse) -> None:
        if response.raw_body is not None:
            # Raw bodies come from internal producers (admin panel assets, metrics
            # rendering) that enforce their own byte bounds; the JSON payload limit
            # does not apply to them.
            body = response.raw_body
        elif response.payload is None:
            body = b""
        else:
            body = json.dumps(
                response.payload,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
        if response.raw_body is None and len(body) > self._settings.max_response_body_bytes:
            response = _error_response(
                HTTPStatus.SERVICE_UNAVAILABLE,
                "response_too_large",
                "Response exceeds its configured byte limit",
            )
            body = json.dumps(response.payload, separators=(",", ":")).encode("utf-8")
        await send(
            AsgiMessage(
                type="http.response.start",
                status=int(response.status),
                headers=((b"content-type", response.content_type), *response.headers),
            )
        )
        await send(AsgiMessage(type="http.response.body", body=body, more_body=False))

    async def _lifespan(self, receive: AsgiReceive, send: AsgiSend) -> None:
        while True:
            message = await receive()
            message_type = message.get("type")
            if message_type == "lifespan.startup":
                await send(AsgiMessage(type="lifespan.startup.complete"))
            elif message_type == "lifespan.shutdown":
                await send(AsgiMessage(type="lifespan.shutdown.complete"))
                return

    def _translate_start_error(
        self,
        operation: str,
        exc: WorkflowStartError,
    ) -> RequestRejectedError:
        status = {
            StartErrorCode.INVALID_REQUEST: HTTPStatus.BAD_REQUEST,
            StartErrorCode.UNKNOWN_WORKFLOW: HTTPStatus.NOT_FOUND,
            StartErrorCode.DEFINITION_UNAVAILABLE: HTTPStatus.CONFLICT,
            StartErrorCode.INCOMPATIBLE_WORKER: HTTPStatus.CONFLICT,
            StartErrorCode.INPUT_REJECTED: HTTPStatus.UNPROCESSABLE_ENTITY,
            StartErrorCode.CONFIGURATION_ERROR: HTTPStatus.SERVICE_UNAVAILABLE,
            StartErrorCode.TRIGGER_PAUSED: HTTPStatus.CONFLICT,
            StartErrorCode.TRIGGER_UNAVAILABLE: HTTPStatus.NOT_FOUND,
            StartErrorCode.TEMPORAL_UNAVAILABLE: HTTPStatus.SERVICE_UNAVAILABLE,
        }[exc.code]
        outcome = ApiMetricOutcome.SERVER_ERROR if exc.retryable else ApiMetricOutcome.CLIENT_ERROR
        self._metrics.record_api(operation, outcome)
        return RequestRejectedError(status, exc.code.value, str(exc))

    def _translate_control_error(
        self,
        operation: str,
        exc: ControlOperationError,
    ) -> RequestRejectedError:
        status = {
            ControlErrorCode.INVALID_REQUEST: HTTPStatus.BAD_REQUEST,
            ControlErrorCode.FILTER_UNAVAILABLE: HTTPStatus.CONFLICT,
            ControlErrorCode.NOT_FOUND: HTTPStatus.NOT_FOUND,
            ControlErrorCode.TEMPORAL_UNAVAILABLE: HTTPStatus.SERVICE_UNAVAILABLE,
        }[exc.code]
        outcome = ApiMetricOutcome.SERVER_ERROR if exc.retryable else ApiMetricOutcome.CLIENT_ERROR
        self._metrics.record_api(operation, outcome)
        return RequestRejectedError(status, exc.code.value, str(exc))

    def _translate_scheduled_start_error(
        self,
        operation: str,
        exc: ScheduledStartError,
    ) -> RequestRejectedError:
        status = {
            ScheduledStartErrorCode.INVALID_REQUEST: HTTPStatus.BAD_REQUEST,
            ScheduledStartErrorCode.TRIGGER_UNAVAILABLE: HTTPStatus.NOT_FOUND,
            ScheduledStartErrorCode.TRIGGER_PAUSED: HTTPStatus.CONFLICT,
            ScheduledStartErrorCode.INPUT_REJECTED: HTTPStatus.UNPROCESSABLE_ENTITY,
            ScheduledStartErrorCode.NOT_FOUND: HTTPStatus.NOT_FOUND,
            ScheduledStartErrorCode.CONFLICT: HTTPStatus.CONFLICT,
            ScheduledStartErrorCode.QUOTA_EXCEEDED: HTTPStatus.TOO_MANY_REQUESTS,
            ScheduledStartErrorCode.QUOTA_UNAVAILABLE: HTTPStatus.SERVICE_UNAVAILABLE,
            ScheduledStartErrorCode.PRIORITY_UNSUPPORTED: HTTPStatus.SERVICE_UNAVAILABLE,
            ScheduledStartErrorCode.TEMPORAL_UNAVAILABLE: HTTPStatus.SERVICE_UNAVAILABLE,
        }[exc.code]
        outcome = ApiMetricOutcome.SERVER_ERROR if exc.retryable else ApiMetricOutcome.CLIENT_ERROR
        self._metrics.record_api(operation, outcome)
        return RequestRejectedError(status, exc.code.value, str(exc))

    def _record_client_error(self, operation: str) -> None:
        self._metrics.record_api(operation, ApiMetricOutcome.CLIENT_ERROR)


def _path_segments(path: str) -> tuple[str, ...]:
    if not path.startswith("/"):
        raise RequestRejectedError(HTTPStatus.BAD_REQUEST, "invalid_path", "Path is invalid")
    segments = tuple(segment for segment in path.split("/") if segment)
    if len(segments) > MAX_ROUTE_SEGMENTS or any(
        len(segment) > MAX_ROUTE_SEGMENT_LENGTH for segment in segments
    ):
        raise RequestRejectedError(
            HTTPStatus.REQUEST_URI_TOO_LONG,
            "path_too_large",
            "Path exceeds its configured bounds",
        )
    return segments


def _required_header(
    headers: tuple[tuple[bytes, bytes], ...],
    name: bytes,
) -> str:
    values = [value for key, value in headers if key.lower() == name]
    if len(values) != 1:
        raise ValueError("Required request header must occur exactly once")
    try:
        value = values[0].decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("Required request header is not valid UTF-8") from exc
    if not value:
        raise ValueError("Required request header must not be empty")
    return value


def _is_loopback_host(host: str) -> bool:
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _single_query_value(query: Mapping[str, list[str]], name: str) -> str | None:
    values = query.get(name)
    if values is None:
        return None
    if len(values) != 1 or not values[0]:
        raise ValueError("Query field must have exactly one non-empty value")
    return values[0]


def _only_query_fields(query: Mapping[str, list[str]], allowed: set[str]) -> None:
    if set(query) - allowed:
        raise ValueError("Unknown query field")


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> MutableMapping[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON object key")
        result[key] = value
    return result


def _reject_non_finite(value: str) -> Any:
    raise ValueError(f"Non-finite JSON number '{value}'")


def _error_response(status: HTTPStatus, code: str, message: str) -> ApiResponse:
    payload = ApiErrorResponse(error=ApiErrorDetail(code=code, message=message))
    return ApiResponse(status, payload.model_dump(mode="json"))
