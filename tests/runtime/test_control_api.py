"""In-process ASGI tests for the bounded control API."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from justflow_admin import BetaAdminPanel

from justflow.config.settings import (
    MIN_CONTROL_RESPONSE_BYTES,
    ControlSettings,
    ScheduledStartWorkloadClass,
)
from justflow.configuration.models import DraftRecord, TenantConfiguration
from justflow.configuration.publication import ConfigurationPublicationService
from justflow.provenance import RuntimeProfile, WorkerArtifactIdentity
from justflow.runtime.admin_panel import AdminPanel
from justflow.runtime.api_compatibility import PUBLIC_API_COMPATIBILITY
from justflow.runtime.auth import (
    AuthenticatedPrincipal,
    AuthenticationError,
    AuthenticationRequest,
    AuthorizationAction,
    AuthorizationRequest,
)
from justflow.runtime.cloud_events import (
    CloudEventIngress,
    CloudEventMappingRegistry,
    EventBridgeEventMapper,
)
from justflow.runtime.configuration_activation import ConfigurationActivationController
from justflow.runtime.configuration_api import ConfigurationApi
from justflow.runtime.control_api import AsgiMessage, ControlApi
from justflow.runtime.health import HealthComponent, HealthRegistry
from justflow.runtime.metrics import ApiMetricOutcome, MetricsRegistry
from justflow.runtime.operations import (
    ControlErrorCode,
    ControlOperationError,
    WorkflowControlService,
    WorkflowDescription,
    WorkflowExecutionState,
    WorkflowListQuery,
    WorkflowListResult,
)
from justflow.runtime.operations_api import OperationsApi
from justflow.runtime.operations_query import OperationsQueryService
from justflow.runtime.schedule_operations import (
    ManagedScheduleDescription,
    ScheduleApplier,
    ScheduleControlService,
    ScheduleOperationError,
    ScheduleOperationErrorCode,
    TriggerRunNowStatus,
)
from justflow.runtime.schedule_reconciler import (
    ScheduleApplyItem,
    ScheduleApplyResult,
    ScheduleApplyStatus,
)
from justflow.runtime.scheduled_start_service import ScheduledStartService
from justflow.runtime.scheduled_starts import (
    ScheduledStartDescription,
    ScheduledStartError,
    ScheduledStartErrorCode,
    ScheduledStartMutationResult,
    ScheduledStartMutationStatus,
    ScheduledStartState,
    make_scheduled_start_id,
)
from justflow.runtime.schedules import ScheduleChangeKind
from justflow.runtime.starter import StartStatus, StartWorkflowResult, WorkflowStarter
from justflow.scope import LOCAL_RUNTIME_SCOPE, RuntimeScope, TrustedScopeBinding

DEFINITION_DIGEST = "a" * 64
SOURCE_DIGEST = "b" * 64
ENVIRONMENT_SNAPSHOT_DIGEST = "e" * 64
SCOPE_A = RuntimeScope.create(
    tenant="tenant-a",
    application="shared-application",
    environment="production",
)
SCOPE_B = RuntimeScope.create(
    tenant="tenant-b",
    application="shared-application",
    environment="production",
)
CLOUD_EVENT_MAPPING = "order-events"
CLOUD_EVENT_PAYLOAD = {
    "version": "0",
    "id": "event-1",
    "detail-type": "Order Created",
    "source": "com.example.orders",
    "account": "123456789012",
    "time": "2026-08-06T08:30:00Z",
    "region": "eu-central-1",
    "resources": [],
    "detail": {"order_id": "order-1"},
}
SCHEDULED_START_ID = make_scheduled_start_id(
    LOCAL_RUNTIME_SCOPE,
    "example",
    "request-later-1",
)
SCHEDULED_START_AT = datetime(2026, 8, 12, 8, 0, tzinfo=UTC)
INITIAL_SCHEDULED_START_VERSION = 1
PRIVATE_CLIENT_MUTATION_VALUE = "private-client-mutation-value"
PRIVATE_ARBITER_RESULT_VALUE = "private-arbiter-result-value"
INVALID_ARBITER_RESULT_MESSAGE = "Temporal returned an invalid scheduled-start arbitration result"


class AuthBehavior(str, Enum):
    ALLOW = "allow"
    REJECT = "reject"
    DENY = "deny"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True, kw_only=True)
class AuthorizationCase:
    id: str
    behavior: AuthBehavior
    expected_status: int
    expected_code: str | None


@dataclass(frozen=True, kw_only=True)
class ConfigurationAuthorizationCase:
    id: str
    method: str
    path: str
    expected_action: AuthorizationAction


@dataclass(frozen=True, kw_only=True)
class ScheduleControlCase:
    id: str
    operation: str
    expected_action: AuthorizationAction
    expected_status: int
    expected_response_status: str
    confirmation: str | None = None
    idempotency_key: str | None = None


@dataclass(frozen=True, kw_only=True)
class ScheduleControlErrorCase:
    id: str
    error_code: ScheduleOperationErrorCode
    expected_status: int
    expected_public_code: str
    expected_message: str


AUTHORIZATION_CASES = [
    AuthorizationCase(
        id="authorized",
        behavior=AuthBehavior.ALLOW,
        expected_status=200,
        expected_code=None,
    ),
    AuthorizationCase(
        id="unauthenticated",
        behavior=AuthBehavior.REJECT,
        expected_status=401,
        expected_code="unauthenticated",
    ),
    AuthorizationCase(
        id="forbidden",
        behavior=AuthBehavior.DENY,
        expected_status=403,
        expected_code="forbidden",
    ),
    AuthorizationCase(
        id="provider-unavailable",
        behavior=AuthBehavior.UNAVAILABLE,
        expected_status=503,
        expected_code="authentication_unavailable",
    ),
]

CONFIGURATION_AUTHORIZATION_CASES = [
    ConfigurationAuthorizationCase(
        id="view",
        method="GET",
        path="/v1/configuration/draft",
        expected_action=AuthorizationAction.CONFIGURATION_VIEW,
    ),
    ConfigurationAuthorizationCase(
        id="edit",
        method="POST",
        path="/v1/configuration/draft",
        expected_action=AuthorizationAction.CONFIGURATION_EDIT,
    ),
    ConfigurationAuthorizationCase(
        id="validate",
        method="POST",
        path="/v1/configuration/draft/validate",
        expected_action=AuthorizationAction.CONFIGURATION_VALIDATE,
    ),
    ConfigurationAuthorizationCase(
        id="publish",
        method="POST",
        path="/v1/configuration/publications",
        expected_action=AuthorizationAction.CONFIGURATION_PUBLISH,
    ),
    ConfigurationAuthorizationCase(
        id="activate",
        method="POST",
        path="/v1/configuration/activations/plan",
        expected_action=AuthorizationAction.CONFIGURATION_ACTIVATE,
    ),
    ConfigurationAuthorizationCase(
        id="rollback",
        method="POST",
        path=f"/v1/configuration/activations/{'a' * 64}/rollback",
        expected_action=AuthorizationAction.CONFIGURATION_ROLLBACK,
    ),
]

SCHEDULE_CONTROL_CASES = [
    ScheduleControlCase(
        id="pause",
        operation="pause",
        expected_action=AuthorizationAction.TRIGGER_PAUSE,
        expected_status=200,
        expected_response_status="paused",
    ),
    ScheduleControlCase(
        id="resume",
        operation="resume",
        expected_action=AuthorizationAction.TRIGGER_RESUME,
        expected_status=200,
        expected_response_status="resumed",
    ),
    ScheduleControlCase(
        id="run",
        operation="run",
        expected_action=AuthorizationAction.TRIGGER_RUN,
        expected_status=202,
        expected_response_status="accepted",
        idempotency_key="request-1",
    ),
    ScheduleControlCase(
        id="delete",
        operation="delete",
        expected_action=AuthorizationAction.TRIGGER_DELETE,
        expected_status=200,
        expected_response_status="deleted",
        confirmation="sha256:managed-schedule",
    ),
]

SCHEDULE_CONTROL_ERROR_CASES = [
    ScheduleControlErrorCase(
        id="unknown",
        error_code=ScheduleOperationErrorCode.UNKNOWN_SCHEDULE,
        expected_status=404,
        expected_public_code="unknown_schedule",
        expected_message="Schedule trigger was not found",
    ),
    ScheduleControlErrorCase(
        id="wrong-scope",
        error_code=ScheduleOperationErrorCode.NOT_MANAGED,
        expected_status=404,
        expected_public_code="not_managed",
        expected_message="Schedule trigger was not found",
    ),
    ScheduleControlErrorCase(
        id="invalid-state",
        error_code=ScheduleOperationErrorCode.INVALID_OPERATION,
        expected_status=409,
        expected_public_code="invalid_operation",
        expected_message="Schedule-trigger operation is not valid in its current state",
    ),
    ScheduleControlErrorCase(
        id="stale-confirmation",
        error_code=ScheduleOperationErrorCode.CONFIRMATION_REQUIRED,
        expected_status=409,
        expected_public_code="confirmation_required",
        expected_message="Schedule-trigger deletion confirmation is stale",
    ),
    ScheduleControlErrorCase(
        id="temporal-unavailable",
        error_code=ScheduleOperationErrorCode.TEMPORAL_UNAVAILABLE,
        expected_status=503,
        expected_public_code="temporal_unavailable",
        expected_message="Schedule-trigger operation is temporarily unavailable",
    ),
]


class FakeStarter:
    def __init__(self, status: StartStatus = StartStatus.STARTED) -> None:
        self.status = status
        self.requests: list[object] = []
        self.scope_bindings: list[TrustedScopeBinding] = []

    async def start(
        self,
        request: object,
        *,
        scope_binding: TrustedScopeBinding,
    ) -> StartWorkflowResult:
        self.requests.append(request)
        self.scope_bindings.append(scope_binding)
        return StartWorkflowResult(
            workflow_id="example-request-1",
            run_id="run-1",
            workflow_name="example",
            definition_digest=DEFINITION_DIGEST,
            environment_snapshot_digest=ENVIRONMENT_SNAPSHOT_DIGEST,
            trigger_name="example_api",
            artifact_identity=WorkerArtifactIdentity(
                deployment_name="deployment",
                build_id="build",
                artifact_digest=f"sha256:{'c' * 64}",
                package_version="1.0.0",
            ),
            source_identity_digest=SOURCE_DIGEST,
            scope_digest=scope_binding.scope.digest,
            status=self.status,
        )


class FakeControls:
    def __init__(self) -> None:
        self.signals: list[tuple[str, str, object, str | None]] = []
        self.cancellations: list[tuple[str, str | None]] = []
        self.terminations: list[tuple[str, str | None]] = []
        self.describe_error: ControlOperationError | None = None
        self.scopes: list[RuntimeScope] = []
        self.queries: list[WorkflowListQuery | None] = []

    async def list(
        self,
        *,
        limit: int,
        page_token: str | None,
        scope: RuntimeScope,
        query: WorkflowListQuery | None = None,
    ) -> WorkflowListResult:
        self.scopes.append(scope)
        self.queries.append(query)
        return WorkflowListResult(workflows=(), next_page_token=page_token)

    async def describe(
        self,
        workflow_id: str,
        *,
        run_id: str | None = None,
        scope: RuntimeScope,
    ) -> WorkflowDescription:
        self.scopes.append(scope)
        if self.describe_error is not None:
            raise self.describe_error
        return WorkflowDescription(
            workflow_id=workflow_id,
            run_id=run_id or "run-1",
            workflow_type="example@digest",
            status="RUNNING",
            task_queue="queue",
            start_time="2026-01-01T00:00:00+00:00",
            close_time=None,
            logical_workflow="example",
            definition_digest=DEFINITION_DIGEST,
        )

    async def signal_event(
        self,
        workflow_id: str,
        event_name: str,
        payload: object,
        *,
        run_id: str | None = None,
        scope: RuntimeScope,
    ) -> None:
        self.scopes.append(scope)
        self.signals.append((workflow_id, event_name, payload, run_id))

    async def cancel(
        self,
        workflow_id: str,
        *,
        run_id: str | None = None,
        scope: RuntimeScope,
    ) -> None:
        self.scopes.append(scope)
        self.cancellations.append((workflow_id, run_id))

    async def terminate(
        self,
        workflow_id: str,
        *,
        run_id: str | None = None,
        scope: RuntimeScope,
    ) -> None:
        self.scopes.append(scope)
        self.terminations.append((workflow_id, run_id))


class FakeScheduleControls:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, RuntimeScope, str | None]] = []
        self.error: ScheduleOperationError | None = None
        self.run_identities: set[str] = set()

    async def pause(
        self,
        schedule_name: str,
        *,
        scope: RuntimeScope,
    ) -> ManagedScheduleDescription:
        self._record("pause", schedule_name, scope)
        return cast(ManagedScheduleDescription, object())

    async def resume(
        self,
        schedule_name: str,
        *,
        scope: RuntimeScope,
    ) -> ManagedScheduleDescription:
        self._record("resume", schedule_name, scope)
        return cast(ManagedScheduleDescription, object())

    async def trigger_now(
        self,
        schedule_name: str,
        *,
        request_identity_digest: str,
        scope: RuntimeScope,
    ) -> TriggerRunNowStatus:
        self._record("run", schedule_name, scope)
        if request_identity_digest in self.run_identities:
            return TriggerRunNowStatus.ALREADY_ACCEPTED
        self.run_identities.add(request_identity_digest)
        return TriggerRunNowStatus.ACCEPTED

    async def delete(
        self,
        schedule_name: str,
        *,
        confirmation: str,
        scope: RuntimeScope,
    ) -> None:
        self._record("delete", schedule_name, scope, confirmation)

    def _record(
        self,
        operation: str,
        schedule_name: str,
        scope: RuntimeScope,
        confirmation: str | None = None,
    ) -> None:
        if self.error is not None:
            raise self.error
        self.calls.append((operation, schedule_name, scope, confirmation))


class FakeAuthentication:
    def __init__(
        self,
        behavior: AuthBehavior,
        *,
        scope: RuntimeScope = LOCAL_RUNTIME_SCOPE,
    ) -> None:
        self.behavior = behavior
        self.scope = scope
        self.actions: list[AuthorizationAction] = []
        self.authorization_requests: list[AuthorizationRequest] = []

    async def authenticate(
        self,
        request: AuthenticationRequest,
    ) -> AuthenticatedPrincipal:
        if self.behavior is AuthBehavior.REJECT:
            raise AuthenticationError("rejected")
        if self.behavior is AuthBehavior.UNAVAILABLE:
            raise RuntimeError("private provider detail")
        return AuthenticatedPrincipal(
            principal_id="principal",
            scope_grants=frozenset({self.scope}),
            effective_scope=self.scope,
        )

    async def authorize(
        self,
        principal: AuthenticatedPrincipal,
        request: AuthorizationRequest,
    ) -> bool:
        self.actions.append(request.action)
        self.authorization_requests.append(request)
        return self.behavior is AuthBehavior.ALLOW


class FakeConfigurationPublication:
    def __init__(self) -> None:
        self.drafts: dict[str, DraftRecord] = {}

    def create_draft(
        self,
        scope: RuntimeScope,
        configuration: TenantConfiguration,
    ) -> DraftRecord:
        draft = DraftRecord(scope_digest=scope.digest, version=1, bundle=configuration)
        self.drafts[scope.digest] = draft
        return draft

    def read_draft(self, scope: RuntimeScope) -> DraftRecord:
        return self.drafts[scope.digest]


def _app(
    *,
    starter: FakeStarter | None = None,
    controls: FakeControls | None = None,
    authentication: FakeAuthentication | None = None,
    profile: RuntimeProfile = RuntimeProfile.LOCAL,
    control_settings: ControlSettings | None = None,
    configuration_api: ConfigurationApi | None = None,
    operations_api: OperationsApi | None = None,
    schedule_controls: FakeScheduleControls | None = None,
    schedule_applier: ScheduleApplier | None = None,
    scheduled_starts: ScheduledStartService | None = None,
    admin_panel: AdminPanel | None = None,
    cloud_events: CloudEventIngress | None = None,
    runtime_scope: RuntimeScope = LOCAL_RUNTIME_SCOPE,
    metrics: MetricsRegistry | None = None,
) -> ControlApi:
    health = HealthRegistry(frozenset({HealthComponent.CATALOG}))
    health.mark_ready(HealthComponent.CATALOG)
    return ControlApi(
        settings=control_settings or ControlSettings(),
        runtime_profile=profile,
        starter=cast(WorkflowStarter, starter or FakeStarter()),
        controls=cast(WorkflowControlService, controls or FakeControls()),
        health=health,
        metrics=metrics if metrics is not None else MetricsRegistry(),
        authentication=authentication,
        cloud_events=cloud_events,
        configuration_api=configuration_api,
        operations_api=operations_api,
        schedule_controls=cast(ScheduleControlService, schedule_controls),
        schedule_applier=schedule_applier,
        scheduled_starts=scheduled_starts,
        admin_panel=admin_panel,
        runtime_scope=runtime_scope,
    )


def _cloud_event_ingress(
    starter: FakeStarter,
    *,
    scope: RuntimeScope,
) -> CloudEventIngress:
    registry = CloudEventMappingRegistry()
    registry.register(
        CLOUD_EVENT_MAPPING,
        EventBridgeEventMapper(
            mapping_name=CLOUD_EVENT_MAPPING,
            workflow_name="example",
            source="com.example.orders",
            detail_type="Order Created",
        ),
        scope=scope,
    )
    return CloudEventIngress(registry, cast(WorkflowStarter, starter))


async def _request(
    app: ControlApi,
    method: str,
    path: str,
    *,
    payload: object | None = None,
    raw_body: bytes | None = None,
    query: bytes = b"",
    headers: tuple[tuple[bytes, bytes], ...] = (),
) -> tuple[int, bytes]:
    body = (
        raw_body
        if raw_body is not None
        else (
            json.dumps(payload, separators=(",", ":")).encode("utf-8")
            if payload is not None
            else b""
        )
    )
    incoming = [AsgiMessage(type="http.request", body=body, more_body=False)]
    outgoing: list[AsgiMessage] = []

    async def receive() -> AsgiMessage:
        return incoming.pop(0)

    async def send(message: AsgiMessage) -> None:
        outgoing.append(message)

    await app(
        {
            "type": "http",
            "method": method,
            "path": path,
            "query_string": query,
            "headers": headers,
        },
        receive,
        send,
    )
    return outgoing[0]["status"], outgoing[1].get("body", b"")


@pytest.mark.parametrize(
    ("start_status", "expected_status"),
    [
        pytest.param(StartStatus.STARTED, 202, id="accepted"),
        pytest.param(StartStatus.DUPLICATE, 200, id="duplicate"),
    ],
)
async def test_start_returns_public_execution_identity(
    start_status: StartStatus,
    expected_status: int,
) -> None:
    starter = FakeStarter(start_status)

    status, body = await _request(
        _app(starter=starter),
        "POST",
        "/v1/workflows",
        payload={
            "workflow_name": "example",
            "business_request_id": "request-1",
            "input": {"value": 1},
        },
    )
    response = json.loads(body)

    assert status == expected_status
    assert response["workflow_id"] == "example-request-1"
    assert response["run_id"] == "run-1"
    assert response["definition_digest"] == DEFINITION_DIGEST
    assert response["artifact_identity"]["build_id"] == "build"
    assert response["status_url"] == "/v1/workflows/example-request-1"


def _scheduled_start_result(
    status: ScheduledStartMutationStatus,
    *,
    state: ScheduledStartState = ScheduledStartState.SCHEDULED,
    version: int = 1,
    expected_version: int | None = None,
) -> ScheduledStartMutationResult:
    return ScheduledStartMutationResult(
        status=status,
        expected_version=expected_version,
        scheduled_start=ScheduledStartDescription(
            scheduled_start_id=SCHEDULED_START_ID,
            workflow_name="example",
            trigger_name="example_api",
            start_at=SCHEDULED_START_AT,
            workload_class=ScheduledStartWorkloadClass.STANDARD,
            state=state,
            version=version,
            accepted_at=SCHEDULED_START_AT - timedelta(hours=1),
            updated_at=SCHEDULED_START_AT - timedelta(hours=1),
        ),
    )


@pytest.mark.parametrize(
    ("mutation_status", "expected_status"),
    [
        pytest.param(ScheduledStartMutationStatus.ACCEPTED, 202, id="accepted"),
        pytest.param(ScheduledStartMutationStatus.DUPLICATE, 200, id="duplicate"),
    ],
)
async def test_scheduled_start_create_has_distinct_authorized_route(
    mutation_status: ScheduledStartMutationStatus,
    expected_status: int,
) -> None:
    service = MagicMock(spec=ScheduledStartService)
    service.create = AsyncMock(return_value=_scheduled_start_result(mutation_status))
    authentication = FakeAuthentication(AuthBehavior.ALLOW)

    status, body = await _request(
        _app(
            scheduled_starts=service,
            authentication=authentication,
            profile=RuntimeProfile.PRODUCTION,
        ),
        "POST",
        "/v1/scheduled-starts",
        payload={
            "workflow_name": "example",
            "business_request_id": "request-later-1",
            "input": {"private": "not-returned"},
            "start_at": SCHEDULED_START_AT.isoformat(),
            "workload_class": "standard",
        },
        headers=((b"x-idempotency-key", b"create-key"),),
    )

    response = json.loads(body)
    assert status == expected_status
    assert response["scheduled_start"]["scheduled_start_id"] == SCHEDULED_START_ID
    assert response["status_url"] == (f"/v1/operations/scheduled-starts/{SCHEDULED_START_ID}")
    assert b"not-returned" not in body
    assert authentication.actions == [AuthorizationAction.SCHEDULED_START_CREATE]
    request = service.create.await_args.args[0]
    assert request.input == {"private": "not-returned"}
    assert service.create.await_args.kwargs["idempotency_key"] == "create-key"


@pytest.mark.parametrize(
    ("mutation", "expected_action", "expected_status"),
    [
        pytest.param(
            "reschedule",
            AuthorizationAction.SCHEDULED_START_RESCHEDULE,
            ScheduledStartMutationStatus.RESCHEDULED,
            id="reschedule",
        ),
        pytest.param(
            "cancel",
            AuthorizationAction.SCHEDULED_START_CANCEL,
            ScheduledStartMutationStatus.CANCELED,
            id="cancel",
        ),
    ],
)
async def test_scheduled_start_mutations_have_independent_authorization(
    mutation: str,
    expected_action: AuthorizationAction,
    expected_status: ScheduledStartMutationStatus,
) -> None:
    service = MagicMock(spec=ScheduledStartService)
    getattr(service, mutation).return_value = _scheduled_start_result(
        expected_status,
        state=(
            ScheduledStartState.CANCELED if mutation == "cancel" else ScheduledStartState.SCHEDULED
        ),
        version=2,
    )
    authentication = FakeAuthentication(AuthBehavior.ALLOW)
    payload = (
        {"start_at": (SCHEDULED_START_AT + timedelta(hours=1)).isoformat(), "expected_version": 1}
        if mutation == "reschedule"
        else {"expected_version": 1}
    )

    status, body = await _request(
        _app(
            scheduled_starts=service,
            authentication=authentication,
            profile=RuntimeProfile.PRODUCTION,
        ),
        "POST",
        f"/v1/scheduled-starts/{SCHEDULED_START_ID}/{mutation}",
        payload=payload,
        headers=((b"x-idempotency-key", b"mutation-key"),),
    )

    assert status == 200
    assert json.loads(body)["status"] == expected_status.value
    assert authentication.actions == [expected_action]
    getattr(service, mutation).assert_awaited_once()


async def test_scheduled_start_mutation_reports_owned_transition_as_accepted() -> None:
    service = MagicMock(spec=ScheduledStartService)
    service.cancel = AsyncMock(
        return_value=_scheduled_start_result(
            ScheduledStartMutationStatus.IN_PROGRESS,
            expected_version=INITIAL_SCHEDULED_START_VERSION,
        )
    )

    status, body = await _request(
        _app(
            scheduled_starts=service,
            authentication=FakeAuthentication(AuthBehavior.ALLOW),
            profile=RuntimeProfile.PRODUCTION,
        ),
        "POST",
        f"/v1/scheduled-starts/{SCHEDULED_START_ID}/cancel",
        payload={"expected_version": INITIAL_SCHEDULED_START_VERSION},
        headers=((b"x-idempotency-key", b"mutation-key"),),
    )

    assert status == 202
    assert json.loads(body) == {
        "status": ScheduledStartMutationStatus.IN_PROGRESS.value,
        "scheduled_start": _scheduled_start_result(
            ScheduledStartMutationStatus.IN_PROGRESS
        ).scheduled_start.model_dump(mode="json"),
        "expected_version": INITIAL_SCHEDULED_START_VERSION,
    }


def internal_arbiter_result_error() -> ScheduledStartError:
    error = ScheduledStartError(
        ScheduledStartErrorCode.TEMPORAL_UNAVAILABLE,
        INVALID_ARBITER_RESULT_MESSAGE,
        retryable=True,
    )
    error.__cause__ = ValueError(PRIVATE_ARBITER_RESULT_VALUE)
    return error


@pytest.mark.parametrize(
    (
        "payload",
        "service_error",
        "expected_status",
        "expected_code",
        "expected_message",
        "expected_outcome",
        "private_value",
    ),
    [
        pytest.param(
            {"expected_version": PRIVATE_CLIENT_MUTATION_VALUE},
            None,
            422,
            "invalid_request",
            "Scheduled-start mutation is invalid",
            ApiMetricOutcome.CLIENT_ERROR,
            PRIVATE_CLIENT_MUTATION_VALUE,
            id="malformed-client-request",
        ),
        pytest.param(
            {"expected_version": INITIAL_SCHEDULED_START_VERSION},
            internal_arbiter_result_error(),
            503,
            "temporal_unavailable",
            INVALID_ARBITER_RESULT_MESSAGE,
            ApiMetricOutcome.SERVER_ERROR,
            PRIVATE_ARBITER_RESULT_VALUE,
            id="malformed-internal-arbiter-result",
        ),
    ],
)
async def test_scheduled_start_mutation_preserves_the_error_boundary(
    payload: dict[str, object],
    service_error: ScheduledStartError | None,
    expected_status: int,
    expected_code: str,
    expected_message: str,
    expected_outcome: ApiMetricOutcome,
    private_value: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    metrics = MetricsRegistry()
    service = MagicMock(spec=ScheduledStartService)
    service.cancel = AsyncMock(side_effect=service_error)

    status, body = await _request(
        _app(
            scheduled_starts=service,
            authentication=FakeAuthentication(AuthBehavior.ALLOW),
            profile=RuntimeProfile.PRODUCTION,
            metrics=metrics,
        ),
        "POST",
        f"/v1/scheduled-starts/{SCHEDULED_START_ID}/cancel",
        payload=payload,
        headers=((b"x-idempotency-key", b"mutation-key"),),
    )

    assert status == expected_status
    assert json.loads(body)["error"] == {
        "code": expected_code,
        "message": expected_message,
    }
    assert private_value.encode("utf-8") not in body
    assert private_value not in caplog.text
    operation = AuthorizationAction.SCHEDULED_START_CANCEL.value
    metric = (
        "justflow_control_api_requests_total"
        f'{{operation="{operation}",outcome="{expected_outcome.value}"}} 1'
    )
    assert metric in metrics.render_prometheus().decode("utf-8")
    if service_error is None:
        service.cancel.assert_not_awaited()
    else:
        service.cancel.assert_awaited_once()


async def test_authenticated_effective_scope_overrides_the_single_scope_adapter() -> None:
    starter = FakeStarter()
    controls = FakeControls()
    authentication = FakeAuthentication(AuthBehavior.ALLOW, scope=SCOPE_A)
    app = _app(
        starter=starter,
        controls=controls,
        authentication=authentication,
        profile=RuntimeProfile.PRODUCTION,
        runtime_scope=SCOPE_B,
    )

    start_status, _ = await _request(
        app,
        "POST",
        "/v1/workflows",
        payload={
            "workflow_name": "example",
            "business_request_id": "request-1",
        },
    )
    list_status, _ = await _request(app, "GET", "/v1/workflows")

    assert start_status == 202
    assert list_status == 200
    assert starter.scope_bindings[0].scope == SCOPE_A
    assert controls.scopes == [SCOPE_A]
    assert [request.scope for request in authentication.authorization_requests] == [
        SCOPE_A,
        SCOPE_A,
    ]


@pytest.mark.parametrize(
    ("start_status", "expected_status"),
    [
        pytest.param(StartStatus.STARTED, 202, id="accepted"),
        pytest.param(StartStatus.DUPLICATE, 200, id="duplicate"),
    ],
)
async def test_cloud_event_http_ingress_authenticates_and_uses_registered_scope(
    start_status: StartStatus,
    expected_status: int,
) -> None:
    starter = FakeStarter(start_status)
    authentication = FakeAuthentication(AuthBehavior.ALLOW, scope=SCOPE_A)
    app = _app(
        starter=starter,
        authentication=authentication,
        profile=RuntimeProfile.PRODUCTION,
        cloud_events=_cloud_event_ingress(starter, scope=SCOPE_A),
    )

    status, body = await _request(
        app,
        "POST",
        f"/events/{CLOUD_EVENT_MAPPING}",
        payload=CLOUD_EVENT_PAYLOAD,
        headers=((b"authorization", b"opaque-value"),),
    )

    response = json.loads(body)
    assert status == expected_status
    assert response["status"] == start_status.value
    assert response["status_url"] == "/v1/workflows/example-request-1"
    assert authentication.actions == [AuthorizationAction.START]
    assert authentication.authorization_requests[0].scope == SCOPE_A
    assert starter.scope_bindings[0].scope == SCOPE_A
    assert starter.scope_bindings[0].kind.value == "cloud_event"


async def test_cloud_event_http_ingress_rejects_cross_scope_before_reading_body() -> None:
    starter = FakeStarter()
    app = _app(
        starter=starter,
        authentication=FakeAuthentication(AuthBehavior.ALLOW, scope=SCOPE_B),
        profile=RuntimeProfile.PRODUCTION,
        cloud_events=_cloud_event_ingress(starter, scope=SCOPE_A),
    )

    status, body = await _request(
        app,
        "POST",
        f"/events/{CLOUD_EVENT_MAPPING}",
        raw_body=b"not-read-before-scope-rejection",
    )

    assert status == 403
    assert json.loads(body)["error"]["code"] == "forbidden"
    assert starter.requests == []


async def test_cloud_event_http_ingress_returns_safe_mapping_error() -> None:
    starter = FakeStarter()
    authentication = FakeAuthentication(AuthBehavior.ALLOW, scope=SCOPE_A)
    app = _app(
        starter=starter,
        authentication=authentication,
        profile=RuntimeProfile.PRODUCTION,
        cloud_events=_cloud_event_ingress(starter, scope=SCOPE_A),
    )
    invalid = {**CLOUD_EVENT_PAYLOAD, "private-secret": "must-not-leak"}

    status, body = await _request(
        app,
        "POST",
        f"/events/{CLOUD_EVENT_MAPPING}",
        payload=invalid,
    )

    response = json.loads(body)
    assert status == 422
    assert response["error"]["code"] == "invalid_payload"
    assert b"must-not-leak" not in body
    assert starter.requests == []


async def test_start_requires_business_id_and_bounded_body() -> None:
    starter = FakeStarter()
    app = _app(
        starter=starter,
        control_settings=ControlSettings(max_request_body_bytes=32),
    )

    missing_status, missing_body = await _request(
        app,
        "POST",
        "/v1/workflows",
        payload={"workflow_name": "example"},
    )
    large_status, large_body = await _request(
        app,
        "POST",
        "/v1/workflows",
        raw_body=b"x" * 33,
    )

    assert missing_status == 422
    assert json.loads(missing_body)["error"]["code"] == "invalid_request"
    assert large_status == 413
    assert json.loads(large_body)["error"]["code"] == "request_too_large"
    assert starter.requests == []


@pytest.mark.parametrize("case", AUTHORIZATION_CASES, ids=lambda case: case.id)
async def test_administrative_health_authorization(case: AuthorizationCase) -> None:
    authentication = FakeAuthentication(case.behavior)

    status, body = await _request(
        _app(authentication=authentication),
        "GET",
        "/healthz",
        headers=((b"authorization", b"opaque-value"),),
    )

    assert status == case.expected_status
    if case.expected_code is None:
        assert json.loads(body)["ready"] is True
        assert authentication.actions == [AuthorizationAction.HEALTH]
    else:
        assert json.loads(body)["error"]["code"] == case.expected_code


async def test_response_byte_limit_governs_json_payloads_not_prebounded_assets() -> None:
    """Panel assets enforce their own byte bounds at resolve time; the JSON
    response limit must not reject them (the vendored mermaid bundle exceeds it)."""
    authentication = FakeAuthentication(AuthBehavior.ALLOW, scope=SCOPE_A)
    tiny = ControlSettings(max_response_body_bytes=MIN_CONTROL_RESPONSE_BYTES)
    app = _app(
        authentication=authentication,
        profile=RuntimeProfile.PRODUCTION,
        control_settings=tiny,
        admin_panel=BetaAdminPanel(),
    )

    asset_status, asset_body = await _request(app, "GET", "/admin/assets/mermaid.js")
    json_status, json_body = await _request(app, "GET", "/v1/operations/capabilities")

    assert asset_status == 200
    assert len(asset_body) > tiny.max_response_body_bytes
    assert json_status == 503
    assert json.loads(json_body)["error"]["code"] == "response_too_large"


async def test_administration_panel_documents_ignore_client_routing_queries() -> None:
    """?tab=… on a bookmarked SPA URL is client-side state; refreshing must not 404."""
    authentication = FakeAuthentication(AuthBehavior.ALLOW, scope=SCOPE_A)
    app = _app(
        authentication=authentication,
        profile=RuntimeProfile.PRODUCTION,
        admin_panel=BetaAdminPanel(),
    )

    status, body = await _request(app, "GET", "/admin/workflows/example", query=b"tab=definition")

    assert status == 200
    assert b'id="view-workflow"' in body


async def test_administration_panel_is_feature_gated_and_separately_authorized() -> None:
    missing_status, _ = await _request(_app(), "GET", "/admin")
    authentication = FakeAuthentication(AuthBehavior.DENY, scope=SCOPE_A)
    panel = MagicMock(spec=AdminPanel)
    panel.has_route.return_value = True
    denied_status, denied_body = await _request(
        _app(
            authentication=authentication,
            profile=RuntimeProfile.PRODUCTION,
            admin_panel=panel,
        ),
        "GET",
        "/admin",
    )

    assert missing_status == 404
    assert denied_status == 403
    assert json.loads(denied_body)["error"]["code"] == "forbidden"
    assert authentication.actions == [AuthorizationAction.ADMIN_PANEL_VIEW]
    panel.resolve.assert_not_called()


@pytest.mark.parametrize(
    "configuration_available",
    [
        pytest.param(False, id="read-only-host"),
        pytest.param(True, id="configuration-host"),
    ],
)
async def test_operations_capabilities_require_both_permission_and_api_availability(
    configuration_available: bool,
) -> None:
    authentication = FakeAuthentication(AuthBehavior.ALLOW, scope=SCOPE_A)
    configuration_api = object.__new__(ConfigurationApi) if configuration_available else None

    status, body = await _request(
        _app(
            authentication=authentication,
            profile=RuntimeProfile.PRODUCTION,
            configuration_api=configuration_api,
        ),
        "GET",
        "/v1/operations/capabilities",
    )
    capabilities = json.loads(body)

    assert status == 200
    assert capabilities["operations_view"] is True
    assert capabilities["api_compatibility"] == PUBLIC_API_COMPATIBILITY.public_dict()
    assert capabilities["scope"] == {
        "tenant": "tenant-a",
        "application": "shared-application",
        "environment": "production",
    }
    assert capabilities["configuration_mode"] == (
        "managed" if configuration_available else "unavailable"
    )
    assert capabilities["workflow_start"] is True
    assert capabilities["workflow_signal"] is True
    assert capabilities["workflow_cancel"] is True
    assert capabilities["workflow_terminate"] is True
    assert capabilities["trigger_pause"] is False
    assert capabilities["trigger_resume"] is False
    assert capabilities["trigger_run"] is False
    assert capabilities["trigger_delete"] is False
    assert capabilities["configuration_edit"] is configuration_available
    assert capabilities["configuration_apply"] is False
    assert capabilities["configuration_discard"] is configuration_available
    assert capabilities["configuration_publish"] is configuration_available
    assert capabilities["configuration_activate"] is configuration_available


async def test_trigger_apply_reconciles_declared_state_and_is_availability_gated() -> None:
    authentication = FakeAuthentication(AuthBehavior.ALLOW, scope=SCOPE_A)
    applier = MagicMock(spec=ScheduleApplier)
    applier.apply = AsyncMock(
        return_value=ScheduleApplyResult(
            plan_digest="digest-1",
            items=(
                ScheduleApplyItem(
                    schedule_id="jf-sched-1",
                    schedule_name="daily",
                    change=ScheduleChangeKind.CREATE,
                    status=ScheduleApplyStatus.APPLIED,
                ),
            ),
        )
    )

    status, body = await _request(
        _app(
            authentication=authentication,
            profile=RuntimeProfile.PRODUCTION,
            schedule_applier=applier,
            runtime_scope=SCOPE_A,
        ),
        "POST",
        "/v1/triggers/apply",
    )
    missing_status, _ = await _request(
        _app(authentication=FakeAuthentication(AuthBehavior.ALLOW, scope=SCOPE_A)),
        "POST",
        "/v1/triggers/apply",
    )

    payload = json.loads(body)
    assert status == 200
    assert payload["successful"] is True
    assert payload["items"] == [
        {
            "schedule_id": "jf-sched-1",
            "trigger_name": "daily",
            "change": "create",
            "status": "applied",
            "error_code": None,
        }
    ]
    applier.apply.assert_awaited_once()
    assert missing_status == 404


async def test_trigger_capabilities_require_controls_and_permission() -> None:
    authentication = FakeAuthentication(AuthBehavior.ALLOW, scope=SCOPE_A)

    status, body = await _request(
        _app(
            authentication=authentication,
            profile=RuntimeProfile.PRODUCTION,
            schedule_controls=FakeScheduleControls(),
        ),
        "GET",
        "/v1/operations/capabilities",
    )
    capabilities = json.loads(body)

    assert status == 200
    assert capabilities["trigger_pause"] is True
    assert capabilities["trigger_resume"] is True
    assert capabilities["trigger_run"] is True
    assert capabilities["trigger_delete"] is True


async def test_operations_api_uses_authenticated_scope_and_digested_resources() -> None:
    controls = FakeControls()
    authentication = FakeAuthentication(AuthBehavior.ALLOW, scope=SCOPE_A)
    operations_api = OperationsApi(
        settings=ControlSettings(),
        query_service=OperationsQueryService(
            executions=cast(WorkflowControlService, controls),
            health=HealthRegistry(frozenset()),
        ),
    )
    app = _app(
        controls=controls,
        authentication=authentication,
        profile=RuntimeProfile.PRODUCTION,
        operations_api=operations_api,
    )

    list_status, _ = await _request(
        app,
        "GET",
        "/v1/operations/runs",
        query=b"state=failed&scope=current&limit=5",
    )
    detail_status, _ = await _request(
        app,
        "GET",
        "/v1/operations/runs/workflow-1",
        query=b"run_id=run-1",
    )

    assert list_status == 200
    assert detail_status == 200
    assert controls.scopes == [SCOPE_A, SCOPE_A]
    assert controls.queries[0] == WorkflowListQuery(
        state=WorkflowExecutionState.FAILED,
        scope="current",
    )
    assert authentication.actions == [
        AuthorizationAction.OPERATIONS_VIEW,
        AuthorizationAction.OPERATIONS_VIEW,
    ]
    detail_request = authentication.authorization_requests[-1]
    assert detail_request.resource_identity_digest is not None
    assert detail_request.resource_identity_digest != "workflow-1"


@pytest.mark.parametrize(
    "case",
    CONFIGURATION_AUTHORIZATION_CASES,
    ids=lambda case: case.id,
)
async def test_configuration_permissions_fail_closed(
    case: ConfigurationAuthorizationCase,
) -> None:
    authentication = FakeAuthentication(AuthBehavior.DENY, scope=SCOPE_A)
    api = object.__new__(ConfigurationApi)

    status, body = await _request(
        _app(
            authentication=authentication,
            profile=RuntimeProfile.PRODUCTION,
            configuration_api=api,
        ),
        case.method,
        case.path,
        raw_body=b"not-read-before-authorization",
    )

    assert status == 403
    assert json.loads(body)["error"]["code"] == "forbidden"
    assert authentication.actions == [case.expected_action]


async def test_configuration_draft_routes_use_authenticated_scope() -> None:
    publication = FakeConfigurationPublication()
    authentication = FakeAuthentication(AuthBehavior.ALLOW, scope=SCOPE_A)
    api = ConfigurationApi(
        settings=ControlSettings(),
        publication=cast(ConfigurationPublicationService, publication),
        activation=cast(ConfigurationActivationController, object()),
    )
    configuration = TenantConfiguration(
        component_catalog_revision="a" * 64,
        workflows={},
        triggers={},
    )
    app = _app(
        authentication=authentication,
        profile=RuntimeProfile.PRODUCTION,
        configuration_api=api,
    )

    create_status, _ = await _request(
        app,
        "POST",
        "/v1/configuration/draft",
        payload={"configuration": configuration.model_dump(mode="json")},
    )
    read_status, read_body = await _request(
        app,
        "GET",
        "/v1/configuration/draft",
    )

    assert create_status == 201
    assert read_status == 200
    assert json.loads(read_body)["scope_digest"] == SCOPE_A.digest
    assert authentication.actions == [
        AuthorizationAction.CONFIGURATION_EDIT,
        AuthorizationAction.CONFIGURATION_VIEW,
    ]
    assert set(publication.drafts) == {SCOPE_A.digest}


def test_capability_policy_administration_is_a_separate_permission() -> None:
    configuration_permissions = {
        AuthorizationAction.CONFIGURATION_VIEW,
        AuthorizationAction.CONFIGURATION_EDIT,
        AuthorizationAction.CONFIGURATION_VALIDATE,
        AuthorizationAction.CONFIGURATION_PUBLISH,
        AuthorizationAction.CONFIGURATION_ACTIVATE,
        AuthorizationAction.CONFIGURATION_ROLLBACK,
    }

    assert AuthorizationAction.CAPABILITY_POLICY_ADMINISTER not in configuration_permissions


def test_control_api_fails_closed_outside_local_mode() -> None:
    with pytest.raises(ValueError, match="authentication is required"):
        _app(profile=RuntimeProfile.PRODUCTION)


def test_unauthenticated_local_control_api_requires_loopback_binding() -> None:
    with pytest.raises(ValueError, match="loopback host"):
        _app(control_settings=ControlSettings(host="0.0.0.0"))


async def test_liveness_and_readiness_do_not_enumerate_workflows() -> None:
    controls = FakeControls()
    app = _app(controls=controls)

    live_status, live_body = await _request(app, "GET", "/livez")
    ready_status, ready_body = await _request(app, "GET", "/readyz")

    assert live_status == 200
    assert json.loads(live_body) == {"status": "live"}
    assert ready_status == 200
    assert json.loads(ready_body) == {"status": "ready"}


async def test_signal_cancel_and_terminate_are_bounded_controls() -> None:
    controls = FakeControls()
    app = _app(controls=controls)

    signal_status, _ = await _request(
        app,
        "POST",
        "/v1/workflows/workflow-1/events/approved",
        query=b"run_id=run-1",
        payload={"payload": {"approved": True}},
    )
    cancel_status, _ = await _request(
        app,
        "POST",
        "/v1/workflows/workflow-1/cancel",
    )
    terminate_status, _ = await _request(
        app,
        "POST",
        "/v1/workflows/workflow-1/terminate",
        query=b"run_id=run-2",
    )

    assert signal_status == 202
    assert controls.signals == [("workflow-1", "approved", {"approved": True}, "run-1")]
    assert cancel_status == 202
    assert controls.cancellations == [("workflow-1", None)]
    assert terminate_status == 202
    assert controls.terminations == [("workflow-1", "run-2")]


@pytest.mark.parametrize("case", SCHEDULE_CONTROL_CASES, ids=lambda case: case.id)
async def test_schedule_controls_are_authorized_and_scope_bound(
    case: ScheduleControlCase,
) -> None:
    controls = FakeScheduleControls()
    authentication = FakeAuthentication(AuthBehavior.ALLOW, scope=SCOPE_A)
    payload = {"confirmation": case.confirmation} if case.confirmation is not None else None
    headers = (
        ((b"x-idempotency-key", case.idempotency_key.encode("utf-8")),)
        if case.idempotency_key is not None
        else ()
    )

    status, body = await _request(
        _app(
            authentication=authentication,
            profile=RuntimeProfile.PRODUCTION,
            schedule_controls=controls,
        ),
        "POST",
        f"/v1/triggers/daily/{case.operation}",
        payload=payload,
        headers=headers,
    )

    assert status == case.expected_status
    assert json.loads(body) == {"status": case.expected_response_status}
    assert controls.calls == [(case.operation, "daily", SCOPE_A, case.confirmation)]
    assert authentication.actions == [case.expected_action]
    assert authentication.authorization_requests[0].resource_identity_digest is not None


@pytest.mark.parametrize(
    "case",
    SCHEDULE_CONTROL_ERROR_CASES,
    ids=lambda case: case.id,
)
async def test_schedule_control_errors_are_layered(case: ScheduleControlErrorCase) -> None:
    controls = FakeScheduleControls()
    controls.error = ScheduleOperationError(case.error_code, "private provider detail")

    status, body = await _request(
        _app(schedule_controls=controls),
        "POST",
        "/v1/triggers/daily/pause",
    )
    response = json.loads(body)

    assert status == case.expected_status
    assert response["error"] == {
        "code": case.expected_public_code,
        "message": case.expected_message,
    }
    assert "private provider detail" not in response["error"]["message"]


async def test_trigger_run_now_is_concurrency_safe_and_idempotent() -> None:
    controls = FakeScheduleControls()
    app = _app(schedule_controls=controls)
    headers = ((b"x-idempotency-key", b"request-1"),)

    responses = await asyncio.gather(
        _request(app, "POST", "/v1/triggers/daily/run", headers=headers),
        _request(app, "POST", "/v1/triggers/daily/run", headers=headers),
    )

    assert sorted(status for status, _ in responses) == [200, 202]
    assert {json.loads(body)["status"] for _, body in responses} == {
        "accepted",
        "already_accepted",
    }
    assert controls.calls == [
        ("run", "daily", LOCAL_RUNTIME_SCOPE, None),
        ("run", "daily", LOCAL_RUNTIME_SCOPE, None),
    ]
    assert len(controls.run_identities) == 1


async def test_trigger_run_now_requires_an_empty_body_and_idempotency_key() -> None:
    controls = FakeScheduleControls()
    app = _app(schedule_controls=controls)

    missing_key_status, _ = await _request(app, "POST", "/v1/triggers/daily/run")
    body_status, _ = await _request(
        app,
        "POST",
        "/v1/triggers/daily/run",
        raw_body=b"{}",
        headers=((b"x-idempotency-key", b"request-1"),),
    )

    assert missing_key_status == 422
    assert body_status == 422
    assert controls.calls == []


async def test_legacy_schedule_and_http_backfill_routes_are_removed() -> None:
    app = _app(schedule_controls=FakeScheduleControls())

    legacy_status, _ = await _request(app, "POST", "/v1/schedules/daily/pause")
    backfill_status, _ = await _request(app, "POST", "/v1/triggers/daily/backfill")

    assert legacy_status == 404
    assert backfill_status == 404


async def test_control_exceptions_are_translated_without_library_details() -> None:
    controls = FakeControls()
    controls.describe_error = ControlOperationError(
        ControlErrorCode.TEMPORAL_UNAVAILABLE,
        "Cannot describe the workflow execution",
        retryable=True,
    )

    status, body = await _request(
        _app(controls=controls),
        "GET",
        "/v1/workflows/workflow-1",
    )

    response = json.loads(body)
    assert status == 503
    assert response["error"] == {
        "code": "temporal_unavailable",
        "message": "Cannot describe the workflow execution",
    }
    assert "RPC" not in body.decode("utf-8")
