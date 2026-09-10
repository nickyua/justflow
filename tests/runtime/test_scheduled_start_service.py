"""Temporal-backed one-off scheduled-start service tests."""

from __future__ import annotations

import asyncio
import base64
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from types import SimpleNamespace
from typing import TypeAlias
from unittest.mock import AsyncMock, MagicMock

import pytest
from temporalio.client import (
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleAlreadyRunningError,
    ScheduleUpdate,
    WorkflowExecutionStatus,
)
from temporalio.common import Priority, WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError, RPCStatusCode

from justflow.config.models import FlowStep, WorkflowConfig
from justflow.config.runtime_limits import DEFAULT_RUNTIME_LIMITS
from justflow.config.settings import (
    ScheduledStartSettings,
    ScheduledStartWorkloadClass,
    ScheduledStartWorkloadPolicySettings,
)
from justflow.config.triggers import TriggerKind
from justflow.definitions.manifest import ENGINE_WORKFLOW_ABI, build_definition_manifests
from justflow.definitions.routing import WorkerDeployment, WorkflowStartTarget
from justflow.provenance import LOCAL_ARTIFACT_DIGEST, WorkerArtifactIdentity
from justflow.runtime.metrics import MetricsRegistry
from justflow.runtime.scheduled_start_service import (
    BestEffortLocalScheduledStartQuotaController,
    ScheduledStartService,
)
from justflow.runtime.scheduled_starts import (
    MEMO_SCHEDULED_START_ARBITER_COMMAND,
    SCHEDULED_START_ARBITER_WORKFLOW_TYPE,
    SCHEDULED_START_DUE_WORKFLOW_TYPE,
    PendingScheduledStartRecord,
    ScheduledStartArbiterApplied,
    ScheduledStartArbiterCommand,
    ScheduledStartArbiterInitialInput,
    ScheduledStartArbiterPreparedDue,
    ScheduledStartArbiterPreparedProjection,
    ScheduledStartArbiterStale,
    ScheduledStartAttemptAmbiguous,
    ScheduledStartAttemptOutcome,
    ScheduledStartAttemptRejected,
    ScheduledStartAttemptRequest,
    ScheduledStartCancelCommand,
    ScheduledStartCancelRequest,
    ScheduledStartCreateRequest,
    ScheduledStartDueCommand,
    ScheduledStartDueInput,
    ScheduledStartDueSkipped,
    ScheduledStartError,
    ScheduledStartErrorCode,
    ScheduledStartFailureCode,
    ScheduledStartMutationStatus,
    ScheduledStartProjectionRecord,
    ScheduledStartRescheduleCommand,
    ScheduledStartRescheduleRequest,
    ScheduledStartResolvedTarget,
    ScheduledStartRun,
    ScheduledStartState,
    cancel_scheduled_start,
    claim_scheduled_start,
    complete_scheduled_start,
    describe_scheduled_start,
    make_scheduled_start_arbiter_id,
    make_scheduled_start_due_id,
    make_scheduled_start_schedule_id,
    reschedule_scheduled_start,
)
from justflow.runtime.starter import (
    MatchedTrigger,
    PreparedWorkflowStart,
    StartErrorCode,
    StartRequestCertainty,
    StartStatus,
    StartWorkflowResult,
    WorkflowStarter,
    WorkflowStartError,
)
from justflow.scope import (
    LOCAL_RUNTIME_SCOPE,
    RuntimeScope,
    ScopeBindingKind,
    TrustedScopeBinding,
    encode_scope_cursor,
)

WORKFLOW_NAME = "reporting"
TRIGGER_NAME = "reporting_api"
TASK_QUEUE = "scheduled-start-dispatch"
BUSINESS_REQUEST_ID = "private-appointment-42"
IDEMPOTENCY_KEY = "create-request-42"
NOW = datetime(2026, 8, 11, 8, 0, tzinfo=UTC)
START_AT = NOW + timedelta(hours=2)
RESCHEDULED_AT = NOW + timedelta(hours=3)
RESCHEDULE_DIGEST = "b" * 64
CANCEL_DIGEST = "c" * 64
INITIAL_VERSION = 1
WORKFLOW_CONFIG = WorkflowConfig(
    workflow=WORKFLOW_NAME,
    steps={},
    flow=[FlowStep.model_validate({"name": "done", "terminal": True})],
)
MANIFEST = build_definition_manifests(
    {WORKFLOW_NAME: WORKFLOW_CONFIG},
    {},
    DEFAULT_RUNTIME_LIMITS,
)[WORKFLOW_NAME]
ARTIFACT = WorkerArtifactIdentity(
    deployment_name="local",
    build_id="local",
    artifact_digest=LOCAL_ARTIFACT_DIGEST,
    package_version="0.1.0",
)
TARGET = WorkflowStartTarget(
    manifest=MANIFEST,
    deployment=WorkerDeployment(
        artifact_identity=ARTIFACT,
        compatible_engine_workflow_abis=frozenset({ENGINE_WORKFLOW_ABI}),
    ),
    environment_snapshot_digest="e" * 64,
)
SCOPE_BINDING = TrustedScopeBinding.create(
    kind=ScopeBindingKind.API,
    scope=LOCAL_RUNTIME_SCOPE,
    binding_id="test-principal",
)
OTHER_RUNTIME_SCOPE = RuntimeScope.create(
    tenant="other",
    application="justflow",
    environment="development",
)
RUN = ScheduledStartRun(
    workflow_id="workflow-id",
    run_id="run-id",
    definition_digest=MANIFEST.definition_digest,
    artifact_identity=ARTIFACT,
    environment_snapshot_digest=TARGET.environment_snapshot_digest,
)
ArbiterResultFactory = Callable[[dict[str, object]], dict[str, object] | BaseException | None]
PRIVATE_ARBITER_RESULT_VALUE = "private-arbiter-result-value"
INVALID_ARBITER_RESULT_MESSAGE = "Temporal returned an invalid scheduled-start arbitration result"
ARBITER_RPC_TIMEOUT_SECONDS = 7
MUTATION_WAIT_SECONDS = 3
ARBITER_RPC_TIMEOUT = timedelta(seconds=ARBITER_RPC_TIMEOUT_SECONDS)
PRIVATE_RPC_ERROR = "private Temporal RPC error"


@dataclass(frozen=True, kw_only=True)
class ArbiterResultReturns:
    value: ScheduledStartMutationStatus


@dataclass(frozen=True, kw_only=True)
class ArbiterResultRaises:
    exc: type[ScheduledStartError]
    match: str
    code: ScheduledStartErrorCode
    retryable: bool


ArbiterResultOutcome: TypeAlias = ArbiterResultReturns | ArbiterResultRaises


@dataclass(frozen=True, kw_only=True)
class ArbiterResultBoundaryCase:
    id: str
    result_factory: ArbiterResultFactory
    outcome: ArbiterResultOutcome


class ArbiterCallPath(str, Enum):
    DUPLICATE_IDENTITY = "duplicate_identity"
    MUTATION_RESULT = "mutation_result"
    AUTHORITATIVE_RUNNING = "authoritative_running"
    AUTHORITATIVE_COMPLETED = "authoritative_completed"


@dataclass(frozen=True, kw_only=True)
class ArbiterCallPolicyCase:
    id: str
    path: ArbiterCallPath
    expected_describe_timeouts: tuple[timedelta, ...]
    expected_result_timeouts: tuple[timedelta, ...]


class ArbiterFailurePath(str, Enum):
    DUPLICATE_IDENTITY = "duplicate_identity"
    AUTHORITATIVE_DESCRIPTION = "authoritative_description"
    AUTHORITATIVE_RESULT = "authoritative_result"


@dataclass(frozen=True, kw_only=True)
class ArbiterFailureCase:
    id: str
    path: ArbiterFailurePath
    expected_message: str


class FakeScheduleAsyncIterator:
    def __init__(
        self,
        values: list[object],
        *,
        page_size: int,
        next_page_token: bytes | None,
    ) -> None:
        self._values = values
        self._page_size = page_size
        self._next_page_start = _decode_page_start(next_page_token)
        self.current_page: tuple[object, ...] | None = None
        self.current_page_index = 0
        self.next_page_token = next_page_token

    async def fetch_next_page(self, *, page_size: int | None = None) -> None:
        selected_page_size = page_size or self._page_size
        page_end = min(self._next_page_start + selected_page_size, len(self._values))
        self.current_page = tuple(self._values[self._next_page_start : page_end])
        self.current_page_index = 0
        self._next_page_start = page_end
        self.next_page_token = (
            _encode_page_start(page_end) if page_end < len(self._values) else None
        )

    def __aiter__(self) -> FakeScheduleAsyncIterator:
        return self

    async def __anext__(self) -> object:
        while True:
            if self.current_page is None:
                await self.fetch_next_page()
                continue
            if self.current_page_index < len(self.current_page):
                value = self.current_page[self.current_page_index]
                self.current_page_index += 1
                return value
            if self.next_page_token is None:
                raise StopAsyncIteration
            await self.fetch_next_page()


def _encode_page_start(page_start: int) -> bytes:
    return str(page_start).encode("ascii")


def _decode_page_start(page_token: bytes | None) -> int:
    return int(page_token.decode("ascii")) if page_token is not None else 0


class FakeDescription:
    def __init__(self, schedule_id: str, schedule: Schedule, memo: dict[str, str]) -> None:
        self.id = schedule_id
        self.schedule = schedule
        self._memo = memo
        self.data_converter = MagicMock()

    async def memo_value(self, key: str, default: object, *, type_hint: type) -> object:
        del type_hint
        return self._memo.get(key, default)


class FakeScheduleHandle:
    def __init__(self, store: dict[str, FakeDescription], schedule_id: str) -> None:
        self._store = store
        self._schedule_id = schedule_id
        self.update_count = 0

    async def describe(self, **_: object) -> FakeDescription:
        try:
            return self._store[self._schedule_id]
        except KeyError as exc:
            raise RPCError("missing", RPCStatusCode.NOT_FOUND, b"") from exc

    async def update(self, updater, **_: object) -> None:
        current = self._store[self._schedule_id]
        result = await updater(SimpleNamespace(description=current))
        if isinstance(result, ScheduleUpdate):
            self.update_count += 1
            self._store[self._schedule_id] = FakeDescription(
                self._schedule_id,
                result.schedule,
                current._memo,
            )


class FakeWorkflowDescription:
    def __init__(
        self,
        memo: dict[str, str],
        *,
        status: WorkflowExecutionStatus,
    ) -> None:
        self._memo = memo
        self.status = status
        self.start_time = NOW

    async def memo_value(self, key: str, default: object, *, type_hint: type) -> object:
        del type_hint
        return self._memo.get(key, default)


class FakeWorkflowHandle:
    def __init__(
        self,
        memo: dict[str, str],
        result: dict[str, object] | None,
        *,
        status: WorkflowExecutionStatus | None = None,
        describe_error: BaseException | None = None,
        result_error: BaseException | None = None,
    ) -> None:
        self._memo = memo
        self._result = result
        self._status = status
        self._describe_error = describe_error
        self._result_error = result_error
        self.describe_rpc_timeouts: list[timedelta | None] = []
        self.result_rpc_timeouts: list[timedelta | None] = []

    async def describe(
        self,
        *,
        rpc_timeout: timedelta | None = None,
    ) -> FakeWorkflowDescription:
        self.describe_rpc_timeouts.append(rpc_timeout)
        if self._describe_error is not None:
            raise self._describe_error
        return FakeWorkflowDescription(
            self._memo,
            status=self._status
            or (
                WorkflowExecutionStatus.RUNNING
                if self._result is None
                else WorkflowExecutionStatus.COMPLETED
            ),
        )

    async def result(
        self,
        *,
        rpc_timeout: timedelta | None = None,
    ) -> dict[str, object]:
        self.result_rpc_timeouts.append(rpc_timeout)
        if self._result_error is not None:
            raise self._result_error
        if self._result is None:
            await asyncio.Future()
        assert self._result is not None
        return self._result


class MissingWorkflowHandle:
    async def describe(
        self,
        *,
        rpc_timeout: timedelta | None = None,
    ) -> FakeWorkflowDescription:
        del rpc_timeout
        raise RPCError("missing", RPCStatusCode.NOT_FOUND, b"")


def temporal_client(
    *,
    arbiter_result_factory: ArbiterResultFactory | None = None,
    create_schedule_error: Exception | None = None,
) -> tuple[
    MagicMock,
    dict[str, FakeDescription],
    dict[str, FakeWorkflowHandle],
]:
    client = MagicMock()
    schedules: dict[str, FakeDescription] = {}
    workflows: dict[str, FakeWorkflowHandle] = {}

    async def create_schedule(
        schedule_id: str,
        schedule: Schedule,
        *,
        memo: dict[str, str],
        **_: object,
    ) -> FakeScheduleHandle:
        if create_schedule_error is not None:
            raise create_schedule_error
        if schedule_id in schedules:
            raise ScheduleAlreadyRunningError()
        schedules[schedule_id] = FakeDescription(schedule_id, schedule, memo)
        return FakeScheduleHandle(schedules, schedule_id)

    async def list_schedules(**kwargs: object) -> FakeScheduleAsyncIterator:
        page_size = kwargs["page_size"]
        next_page_token = kwargs["next_page_token"]
        assert isinstance(page_size, int)
        assert isinstance(next_page_token, bytes | None)
        return FakeScheduleAsyncIterator(
            [SimpleNamespace(id=schedule_id) for schedule_id in sorted(schedules)],
            page_size=page_size,
            next_page_token=next_page_token,
        )

    async def start_workflow(
        workflow: str,
        argument: dict[str, object],
        *,
        id: str,
        memo: dict[str, str],
        **_: object,
    ) -> FakeWorkflowHandle:
        if id in workflows:
            raise WorkflowAlreadyStartedError(id, workflow)
        result = arbiter_result_factory(argument) if arbiter_result_factory else None
        handle = FakeWorkflowHandle(
            memo,
            result if not isinstance(result, BaseException) else None,
            result_error=result if isinstance(result, BaseException) else None,
        )
        workflows[id] = handle
        return handle

    client.create_schedule = AsyncMock(side_effect=create_schedule)
    client.list_schedules = AsyncMock(side_effect=list_schedules)
    client.start_workflow = AsyncMock(side_effect=start_workflow)
    client.get_schedule_handle.side_effect = lambda schedule_id: FakeScheduleHandle(
        schedules,
        schedule_id,
    )
    client.get_workflow_handle.side_effect = lambda workflow_id, **_: workflows.get(
        workflow_id,
        MissingWorkflowHandle(),
    )
    return client, schedules, workflows


def workflow_starter() -> MagicMock:
    starter = MagicMock(spec=WorkflowStarter)
    starter.find_existing = AsyncMock(return_value=None)
    starter.prepare = AsyncMock(
        return_value=PreparedWorkflowStart(
            target=TARGET,
            trigger=MatchedTrigger(name=TRIGGER_NAME, kind=TriggerKind.API),
            normalized_input={"reference": "opaque-reference"},
        )
    )
    return starter


def create_request(
    *,
    start_at: datetime = START_AT,
    workload_class: ScheduledStartWorkloadClass = ScheduledStartWorkloadClass.STANDARD,
    business_request_id: str = BUSINESS_REQUEST_ID,
) -> ScheduledStartCreateRequest:
    return ScheduledStartCreateRequest(
        workflow_name=WORKFLOW_NAME,
        input={"reference": "opaque-reference"},
        business_request_id=business_request_id,
        start_at=start_at,
        workload_class=workload_class,
    )


def service(
    client: MagicMock,
    *,
    settings: ScheduledStartSettings | None = None,
    quota: bool = True,
    starter: MagicMock | None = None,
    metrics: MetricsRegistry | None = None,
    decision_recorder: MagicMock | None = None,
    clock: Callable[[], datetime] = lambda: NOW,
) -> ScheduledStartService:
    return ScheduledStartService(
        client,
        starter or workflow_starter(),
        settings or ScheduledStartSettings(),
        task_queue=TASK_QUEUE,
        quota_controller=BestEffortLocalScheduledStartQuotaController() if quota else None,
        metrics=metrics,
        decision_recorder=decision_recorder,
        clock=clock,
    )


def action_record(schedules: dict[str, FakeDescription]) -> PendingScheduledStartRecord:
    return pending_record(next(iter(schedules.values())))


def pending_record(description: FakeDescription) -> PendingScheduledStartRecord:
    action = description.schedule.action
    assert isinstance(action, ScheduleActionStartWorkflow)
    due = ScheduledStartDueInput.model_validate(action.args[0])
    return PendingScheduledStartRecord.model_validate(due.record)


def applied_mutation_result(
    raw_initial: dict[str, object],
) -> dict[str, object] | None:
    initial = ScheduledStartArbiterInitialInput.model_validate(raw_initial)
    command = initial.command
    projection: ScheduledStartProjectionRecord
    if isinstance(command, ScheduledStartRescheduleCommand):
        projection, _ = reschedule_scheduled_start(
            initial.record,
            ScheduledStartRescheduleRequest(
                start_at=command.start_at,
                expected_version=command.expected_version,
            ),
            request_digest=command.request_digest,
            updated_at=NOW,
        )
    elif isinstance(command, ScheduledStartCancelCommand):
        projection, _ = cancel_scheduled_start(
            initial.record,
            ScheduledStartCancelRequest(expected_version=command.expected_version),
            request_digest=command.request_digest,
            updated_at=NOW,
        )
    else:
        return None
    return ScheduledStartArbiterApplied(
        consumed_version=initial.record.version,
        winner=command.kind,
        request_digest=command.request_digest,
        scheduled_start=describe_scheduled_start(projection),
    ).model_dump(mode="json")


def stale_mutation_result(raw_initial: dict[str, object]) -> dict[str, object]:
    initial = ScheduledStartArbiterInitialInput.model_validate(raw_initial)
    return ScheduledStartArbiterStale(
        consumed_version=initial.record.version,
        current=describe_scheduled_start(initial.record),
    ).model_dump(mode="json")


def started_mutation_result(raw_initial: dict[str, object]) -> dict[str, object]:
    initial = ScheduledStartArbiterInitialInput.model_validate(raw_initial)
    claimed = claim_scheduled_start(
        initial.record,
        expected_version=initial.record.version,
        claimed_at=NOW,
    )
    assert claimed is not None
    started = complete_scheduled_start(claimed, RUN, completed_at=NOW)
    return ScheduledStartArbiterApplied(
        consumed_version=initial.record.version,
        winner=initial.command.kind,
        request_digest=(
            initial.command.request_digest
            if not isinstance(initial.command, ScheduledStartDueCommand)
            else None
        ),
        scheduled_start=describe_scheduled_start(started),
    ).model_dump(mode="json")


def missing_arbiter_result_discriminator(_: dict[str, object]) -> dict[str, object]:
    return {"private": PRIVATE_ARBITER_RESULT_VALUE}


def unknown_arbiter_result_discriminator(_: dict[str, object]) -> dict[str, object]:
    return {"status": PRIVATE_ARBITER_RESULT_VALUE}


def invalid_nested_arbiter_result(_: dict[str, object]) -> dict[str, object]:
    return {
        "status": "applied",
        "consumed_version": INITIAL_VERSION,
        "winner": "cancel",
        "request_digest": CANCEL_DIGEST,
        "scheduled_start": {"scheduled_start_id": PRIVATE_ARBITER_RESULT_VALUE},
    }


ARBITER_RESULT_BOUNDARY_CASES = [
    ArbiterResultBoundaryCase(
        id="valid-applied",
        result_factory=applied_mutation_result,
        outcome=ArbiterResultReturns(value=ScheduledStartMutationStatus.CANCELED),
    ),
    ArbiterResultBoundaryCase(
        id="missing-discriminator",
        result_factory=missing_arbiter_result_discriminator,
        outcome=ArbiterResultRaises(
            exc=ScheduledStartError,
            match=INVALID_ARBITER_RESULT_MESSAGE,
            code=ScheduledStartErrorCode.TEMPORAL_UNAVAILABLE,
            retryable=True,
        ),
    ),
    ArbiterResultBoundaryCase(
        id="unknown-discriminator",
        result_factory=unknown_arbiter_result_discriminator,
        outcome=ArbiterResultRaises(
            exc=ScheduledStartError,
            match=INVALID_ARBITER_RESULT_MESSAGE,
            code=ScheduledStartErrorCode.TEMPORAL_UNAVAILABLE,
            retryable=True,
        ),
    ),
    ArbiterResultBoundaryCase(
        id="invalid-nested-scheduled-start",
        result_factory=invalid_nested_arbiter_result,
        outcome=ArbiterResultRaises(
            exc=ScheduledStartError,
            match=INVALID_ARBITER_RESULT_MESSAGE,
            code=ScheduledStartErrorCode.TEMPORAL_UNAVAILABLE,
            retryable=True,
        ),
    ),
]

ARBITER_CALL_POLICY_CASES = [
    ArbiterCallPolicyCase(
        id="duplicate-mutation-identity",
        path=ArbiterCallPath.DUPLICATE_IDENTITY,
        expected_describe_timeouts=(ARBITER_RPC_TIMEOUT,),
        expected_result_timeouts=(),
    ),
    ArbiterCallPolicyCase(
        id="mutation-result",
        path=ArbiterCallPath.MUTATION_RESULT,
        expected_describe_timeouts=(),
        expected_result_timeouts=(ARBITER_RPC_TIMEOUT,),
    ),
    ArbiterCallPolicyCase(
        id="authoritative-running-description",
        path=ArbiterCallPath.AUTHORITATIVE_RUNNING,
        expected_describe_timeouts=(ARBITER_RPC_TIMEOUT,),
        expected_result_timeouts=(),
    ),
    ArbiterCallPolicyCase(
        id="authoritative-completed-result",
        path=ArbiterCallPath.AUTHORITATIVE_COMPLETED,
        expected_describe_timeouts=(ARBITER_RPC_TIMEOUT,),
        expected_result_timeouts=(ARBITER_RPC_TIMEOUT,),
    ),
]

ARBITER_FAILURE_CASES = [
    ArbiterFailureCase(
        id="duplicate-identity-describe",
        path=ArbiterFailurePath.DUPLICATE_IDENTITY,
        expected_message="Temporal could not identify the scheduled-start winner",
    ),
    ArbiterFailureCase(
        id="authoritative-describe",
        path=ArbiterFailurePath.AUTHORITATIVE_DESCRIPTION,
        expected_message="Temporal could not read scheduled-start arbitration state",
    ),
    ArbiterFailureCase(
        id="authoritative-result",
        path=ArbiterFailurePath.AUTHORITATIVE_RESULT,
        expected_message="Temporal could not read the scheduled-start arbitration result",
    ),
]


async def test_create_uses_one_limited_native_due_schedule_with_closed_priority() -> None:
    client, schedules, _ = temporal_client()

    result = await service(client).create(
        create_request(workload_class=ScheduledStartWorkloadClass.INTERACTIVE),
        idempotency_key=IDEMPOTENCY_KEY,
        scope_binding=SCOPE_BINDING,
    )

    assert result.status is ScheduledStartMutationStatus.ACCEPTED
    assert result.scheduled_start.state is ScheduledStartState.SCHEDULED
    schedule = next(iter(schedules.values())).schedule
    assert schedule.state.limited_actions is True
    assert schedule.state.remaining_actions == 1
    assert isinstance(schedule.action, ScheduleActionStartWorkflow)
    assert schedule.action.workflow == SCHEDULED_START_DUE_WORKFLOW_TYPE
    assert schedule.action.retry_policy is not None
    assert schedule.action.priority.priority_key == 1
    assert schedule.action.priority.fairness_key == LOCAL_RUNTIME_SCOPE.digest


@pytest.mark.parametrize(
    ("error", "expected_code"),
    [
        pytest.param(
            RPCError("unsupported", RPCStatusCode.INVALID_ARGUMENT, b""),
            ScheduledStartErrorCode.PRIORITY_UNSUPPORTED,
            id="priority-unsupported",
        ),
        pytest.param(
            RuntimeError("transport failed"),
            ScheduledStartErrorCode.TEMPORAL_UNAVAILABLE,
            id="transport-failure",
        ),
    ],
)
async def test_create_classifies_temporal_schedule_failures(
    error: Exception,
    expected_code: ScheduledStartErrorCode,
) -> None:
    client, schedules, _ = temporal_client(create_schedule_error=error)

    with pytest.raises(ScheduledStartError) as raised:
        await service(client).create(
            create_request(),
            idempotency_key=IDEMPOTENCY_KEY,
            scope_binding=SCOPE_BINDING,
        )

    assert raised.value.code is expected_code
    assert schedules == {}


async def test_exact_and_conflicting_duplicate_create_are_stable() -> None:
    client, schedules, _ = temporal_client()
    facade = service(client)
    first = await facade.create(
        create_request(),
        idempotency_key=IDEMPOTENCY_KEY,
        scope_binding=SCOPE_BINDING,
    )
    duplicate = await facade.create(
        create_request(),
        idempotency_key=IDEMPOTENCY_KEY,
        scope_binding=SCOPE_BINDING,
    )

    with pytest.raises(ScheduledStartError) as raised:
        await facade.create(
            create_request(start_at=RESCHEDULED_AT),
            idempotency_key="different-key",
            scope_binding=SCOPE_BINDING,
        )

    assert duplicate.status is ScheduledStartMutationStatus.DUPLICATE
    assert duplicate.scheduled_start == first.scheduled_start
    assert raised.value.code is ScheduledStartErrorCode.CONFLICT
    assert len(schedules) == 1


async def test_accepted_create_retry_recovers_after_due_time_and_trigger_removal() -> None:
    client, schedules, _ = temporal_client()
    now = NOW
    starter = workflow_starter()
    facade = service(client, clock=lambda: now, starter=starter)
    request = create_request()
    first = await facade.create(
        request, idempotency_key=IDEMPOTENCY_KEY, scope_binding=SCOPE_BINDING
    )
    now = request.start_at + timedelta(hours=1)
    starter.prepare.side_effect = RuntimeError("trigger removed")
    repeated = await facade.create(
        request, idempotency_key=IDEMPOTENCY_KEY, scope_binding=SCOPE_BINDING
    )
    assert repeated.status is ScheduledStartMutationStatus.DUPLICATE
    assert repeated.scheduled_start == first.scheduled_start
    assert len(schedules) == 1


async def test_accepted_reschedule_retry_recovers_after_due_time() -> None:
    client, _, _ = temporal_client(arbiter_result_factory=applied_mutation_result)
    now = NOW
    facade = service(client, clock=lambda: now)
    first = await facade.create(
        create_request(), idempotency_key=IDEMPOTENCY_KEY, scope_binding=SCOPE_BINDING
    )
    identity = first.scheduled_start.scheduled_start_id
    request = ScheduledStartRescheduleRequest(start_at=RESCHEDULED_AT, expected_version=1)
    accepted = await facade.reschedule(
        identity, request, idempotency_key="reschedule", scope=LOCAL_RUNTIME_SCOPE
    )
    now = RESCHEDULED_AT + timedelta(hours=1)
    repeated = await facade.reschedule(
        identity, request, idempotency_key="reschedule", scope=LOCAL_RUNTIME_SCOPE
    )
    assert repeated.status is ScheduledStartMutationStatus.DUPLICATE
    assert repeated.scheduled_start == accepted.scheduled_start


async def test_public_reschedule_claims_an_arbiter_without_writing_the_schedule() -> None:
    client, schedules, _ = temporal_client(arbiter_result_factory=applied_mutation_result)
    facade = service(client)
    created = await facade.create(
        create_request(),
        idempotency_key=IDEMPOTENCY_KEY,
        scope_binding=SCOPE_BINDING,
    )
    before = next(iter(schedules.values())).schedule

    result = await facade.reschedule(
        created.scheduled_start.scheduled_start_id,
        ScheduledStartRescheduleRequest(start_at=RESCHEDULED_AT, expected_version=1),
        idempotency_key="reschedule-key",
        scope=LOCAL_RUNTIME_SCOPE,
    )

    assert result.status is ScheduledStartMutationStatus.RESCHEDULED
    assert next(iter(schedules.values())).schedule is before
    start_call = client.start_workflow.await_args
    assert start_call.args[0] == SCHEDULED_START_ARBITER_WORKFLOW_TYPE
    assert start_call.kwargs["id_reuse_policy"] is WorkflowIDReusePolicy.REJECT_DUPLICATE
    assert start_call.kwargs["id_conflict_policy"] is WorkflowIDConflictPolicy.FAIL
    assert "retry_policy" not in start_call.kwargs
    assert "execution_timeout" not in start_call.kwargs
    assert "run_timeout" not in start_call.kwargs

    duplicate = await facade.reschedule(
        created.scheduled_start.scheduled_start_id,
        ScheduledStartRescheduleRequest(start_at=RESCHEDULED_AT, expected_version=1),
        idempotency_key="reschedule-key",
        scope=LOCAL_RUNTIME_SCOPE,
    )

    assert duplicate.status is ScheduledStartMutationStatus.DUPLICATE
    assert duplicate.scheduled_start == result.scheduled_start


async def test_public_mutation_returns_in_progress_when_the_arbiter_result_is_unavailable() -> None:
    client, _, _ = temporal_client(
        arbiter_result_factory=lambda _: RuntimeError("result unavailable")
    )
    facade = service(client)
    created = await facade.create(
        create_request(),
        idempotency_key=IDEMPOTENCY_KEY,
        scope_binding=SCOPE_BINDING,
    )

    result = await facade.cancel(
        created.scheduled_start.scheduled_start_id,
        ScheduledStartCancelRequest(expected_version=1),
        idempotency_key="cancel-key",
        scope=LOCAL_RUNTIME_SCOPE,
    )

    assert result.status is ScheduledStartMutationStatus.IN_PROGRESS
    assert result.expected_version == 1


async def test_public_mutation_overall_timeout_returns_in_progress_without_provider_warning(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    observed_timeouts: list[float | None] = []

    async def expire_wait(
        awaitable: Awaitable[object],
        *,
        timeout: float | None,
    ) -> object:
        observed_timeouts.append(timeout)
        task = asyncio.ensure_future(awaitable)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        raise TimeoutError

    monkeypatch.setattr(asyncio, "wait_for", expire_wait)
    client, _, _ = temporal_client(arbiter_result_factory=applied_mutation_result)
    facade = service(
        client,
        settings=ScheduledStartSettings(
            dispatch_timeout_seconds=ARBITER_RPC_TIMEOUT_SECONDS,
            mutation_wait_seconds=MUTATION_WAIT_SECONDS,
        ),
    )
    created = await facade.create(
        create_request(),
        idempotency_key=IDEMPOTENCY_KEY,
        scope_binding=SCOPE_BINDING,
    )

    result = await facade.cancel(
        created.scheduled_start.scheduled_start_id,
        ScheduledStartCancelRequest(expected_version=INITIAL_VERSION),
        idempotency_key="cancel-key",
        scope=LOCAL_RUNTIME_SCOPE,
    )

    assert result.status is ScheduledStartMutationStatus.IN_PROGRESS
    assert observed_timeouts == [MUTATION_WAIT_SECONDS]
    assert "result is not yet available" not in caplog.text


async def test_public_mutation_preserves_result_wait_cancellation() -> None:
    client, _, _ = temporal_client(arbiter_result_factory=lambda _: asyncio.CancelledError())
    facade = service(client)
    created = await facade.create(
        create_request(),
        idempotency_key=IDEMPOTENCY_KEY,
        scope_binding=SCOPE_BINDING,
    )

    with pytest.raises(asyncio.CancelledError):
        await facade.cancel(
            created.scheduled_start.scheduled_start_id,
            ScheduledStartCancelRequest(expected_version=INITIAL_VERSION),
            idempotency_key="cancel-key",
            scope=LOCAL_RUNTIME_SCOPE,
        )


@pytest.mark.parametrize(
    "case",
    ARBITER_CALL_POLICY_CASES,
    ids=lambda case: case.id,
)
async def test_arbiter_reads_apply_the_configured_rpc_timeout(
    case: ArbiterCallPolicyCase,
) -> None:
    result_factory = (
        applied_mutation_result if case.path is ArbiterCallPath.MUTATION_RESULT else None
    )
    client, schedules, workflows = temporal_client(arbiter_result_factory=result_factory)
    facade = service(
        client,
        settings=ScheduledStartSettings(
            dispatch_timeout_seconds=ARBITER_RPC_TIMEOUT_SECONDS,
            mutation_wait_seconds=MUTATION_WAIT_SECONDS,
        ),
    )
    created = await facade.create(
        create_request(),
        idempotency_key=IDEMPOTENCY_KEY,
        scope_binding=SCOPE_BINDING,
    )
    record = action_record(schedules)
    arbiter_id = make_scheduled_start_arbiter_id(
        LOCAL_RUNTIME_SCOPE,
        record.scheduled_start_id,
        record.version,
    )

    if case.path is ArbiterCallPath.MUTATION_RESULT:
        await facade.cancel(
            created.scheduled_start.scheduled_start_id,
            ScheduledStartCancelRequest(expected_version=INITIAL_VERSION),
            idempotency_key="cancel-key",
            scope=LOCAL_RUNTIME_SCOPE,
        )
    else:
        await facade.claim_due(record, scope=LOCAL_RUNTIME_SCOPE, priority=Priority())
        if case.path is ArbiterCallPath.DUPLICATE_IDENTITY:
            with pytest.raises(ScheduledStartError):
                await facade.cancel(
                    created.scheduled_start.scheduled_start_id,
                    ScheduledStartCancelRequest(expected_version=INITIAL_VERSION),
                    idempotency_key="cancel-key",
                    scope=LOCAL_RUNTIME_SCOPE,
                )
        else:
            if case.path is ArbiterCallPath.AUTHORITATIVE_COMPLETED:
                memo = workflows[arbiter_id]._memo
                raw_initial = ScheduledStartArbiterInitialInput(
                    record=record,
                    command=ScheduledStartDueCommand(nominal_time=record.start_at),
                ).model_dump(mode="json")
                workflows[arbiter_id] = FakeWorkflowHandle(
                    memo,
                    started_mutation_result(raw_initial),
                )
            await facade.describe(
                created.scheduled_start.scheduled_start_id,
                scope=LOCAL_RUNTIME_SCOPE,
            )

    handle = workflows[arbiter_id]
    assert tuple(handle.describe_rpc_timeouts) == case.expected_describe_timeouts
    assert tuple(handle.result_rpc_timeouts) == case.expected_result_timeouts


@pytest.mark.parametrize(
    "case",
    ARBITER_FAILURE_CASES,
    ids=lambda case: case.id,
)
async def test_arbiter_rpc_failures_use_the_service_error_boundary(
    case: ArbiterFailureCase,
    caplog: pytest.LogCaptureFixture,
) -> None:
    client, schedules, workflows = temporal_client()
    facade = service(client)
    created = await facade.create(
        create_request(),
        idempotency_key=IDEMPOTENCY_KEY,
        scope_binding=SCOPE_BINDING,
    )
    record = action_record(schedules)
    await facade.claim_due(record, scope=LOCAL_RUNTIME_SCOPE, priority=Priority())
    arbiter_id = make_scheduled_start_arbiter_id(
        LOCAL_RUNTIME_SCOPE,
        record.scheduled_start_id,
        record.version,
    )
    memo = workflows[arbiter_id]._memo
    rpc_error = RPCError(PRIVATE_RPC_ERROR, RPCStatusCode.DEADLINE_EXCEEDED, b"")
    if case.path is ArbiterFailurePath.AUTHORITATIVE_RESULT:
        workflows[arbiter_id] = FakeWorkflowHandle(
            memo,
            None,
            status=WorkflowExecutionStatus.COMPLETED,
            result_error=rpc_error,
        )
    else:
        workflows[arbiter_id] = FakeWorkflowHandle(
            memo,
            None,
            describe_error=rpc_error,
        )

    with pytest.raises(ScheduledStartError, match=case.expected_message) as raised:
        if case.path is ArbiterFailurePath.DUPLICATE_IDENTITY:
            await facade.cancel(
                created.scheduled_start.scheduled_start_id,
                ScheduledStartCancelRequest(expected_version=INITIAL_VERSION),
                idempotency_key="cancel-key",
                scope=LOCAL_RUNTIME_SCOPE,
            )
        else:
            await facade.describe(
                created.scheduled_start.scheduled_start_id,
                scope=LOCAL_RUNTIME_SCOPE,
            )

    assert raised.value.code is ScheduledStartErrorCode.TEMPORAL_UNAVAILABLE
    assert raised.value.retryable is True
    assert PRIVATE_RPC_ERROR not in str(raised.value)
    assert PRIVATE_RPC_ERROR not in caplog.text


@pytest.mark.parametrize(
    "case",
    ARBITER_RESULT_BOUNDARY_CASES,
    ids=lambda case: case.id,
)
async def test_public_mutation_validates_arbiter_results_as_internal_data(
    case: ArbiterResultBoundaryCase,
    caplog: pytest.LogCaptureFixture,
) -> None:
    client, _, _ = temporal_client(arbiter_result_factory=case.result_factory)
    facade = service(client)
    created = await facade.create(
        create_request(),
        idempotency_key=IDEMPOTENCY_KEY,
        scope_binding=SCOPE_BINDING,
    )

    if isinstance(case.outcome, ArbiterResultReturns):
        result = await facade.cancel(
            created.scheduled_start.scheduled_start_id,
            ScheduledStartCancelRequest(expected_version=INITIAL_VERSION),
            idempotency_key="cancel-key",
            scope=LOCAL_RUNTIME_SCOPE,
        )

        assert result.status is case.outcome.value
        return

    with pytest.raises(case.outcome.exc, match=case.outcome.match) as raised:
        await facade.cancel(
            created.scheduled_start.scheduled_start_id,
            ScheduledStartCancelRequest(expected_version=INITIAL_VERSION),
            idempotency_key="cancel-key",
            scope=LOCAL_RUNTIME_SCOPE,
        )

    assert raised.value.code is case.outcome.code
    assert raised.value.retryable is case.outcome.retryable
    assert PRIVATE_ARBITER_RESULT_VALUE not in str(raised.value)
    assert PRIVATE_ARBITER_RESULT_VALUE not in caplog.text


@pytest.mark.parametrize(
    ("result_factory", "expected_message"),
    [
        pytest.param(
            stale_mutation_result,
            "no longer pending",
            id="stale-arbiter",
        ),
        pytest.param(
            started_mutation_result,
            "already produced a workflow run",
            id="started-reality-check",
        ),
    ],
)
async def test_public_mutation_rejects_an_authoritative_losing_result(
    result_factory: ArbiterResultFactory,
    expected_message: str,
) -> None:
    client, _, _ = temporal_client(arbiter_result_factory=result_factory)
    facade = service(client)
    created = await facade.create(
        create_request(),
        idempotency_key=IDEMPOTENCY_KEY,
        scope_binding=SCOPE_BINDING,
    )

    with pytest.raises(ScheduledStartError, match=expected_message) as raised:
        await facade.cancel(
            created.scheduled_start.scheduled_start_id,
            ScheduledStartCancelRequest(expected_version=1),
            idempotency_key="cancel-key",
            scope=LOCAL_RUNTIME_SCOPE,
        )

    assert raised.value.code is ScheduledStartErrorCode.CONFLICT


@dataclass(frozen=True, kw_only=True)
class ProjectionCase:
    id: str
    command: ScheduledStartCancelCommand | ScheduledStartRescheduleCommand
    expected_state: ScheduledStartState
    expected_version: int
    expected_remaining_actions: int


PROJECTION_CASES = [
    ProjectionCase(
        id="cancel",
        command=ScheduledStartCancelCommand(
            expected_version=INITIAL_VERSION,
            request_digest=CANCEL_DIGEST,
        ),
        expected_state=ScheduledStartState.CANCELED,
        expected_version=1,
        expected_remaining_actions=0,
    ),
    ProjectionCase(
        id="reschedule",
        command=ScheduledStartRescheduleCommand(
            expected_version=INITIAL_VERSION,
            start_at=RESCHEDULED_AT,
            request_digest=RESCHEDULE_DIGEST,
        ),
        expected_state=ScheduledStartState.SCHEDULED,
        expected_version=2,
        expected_remaining_actions=1,
    ),
]


@pytest.mark.parametrize("case", PROJECTION_CASES, ids=lambda case: case.id)
async def test_arbiter_commits_one_final_schedule_projection(case: ProjectionCase) -> None:
    client, schedules, _ = temporal_client()
    facade = service(client)
    await facade.create(
        create_request(),
        idempotency_key=IDEMPOTENCY_KEY,
        scope_binding=SCOPE_BINDING,
    )
    initial = ScheduledStartArbiterInitialInput(
        record=action_record(schedules),
        command=case.command,
    )
    preparation = await facade.prepare_arbiter(initial, scope=LOCAL_RUNTIME_SCOPE)
    assert isinstance(preparation, ScheduledStartArbiterPreparedProjection)

    applied = await facade.commit_arbiter(
        preparation.terminal,
        scope=LOCAL_RUNTIME_SCOPE,
    )

    schedule = next(iter(schedules.values())).schedule
    assert applied.scheduled_start.state is case.expected_state
    assert applied.scheduled_start.version == case.expected_version
    assert schedule.state.remaining_actions == case.expected_remaining_actions
    assert schedule.state.paused is (case.expected_remaining_actions == 0)
    assert isinstance(schedule.action, ScheduleActionStartWorkflow)
    assert schedule.action.id == make_scheduled_start_due_id(
        LOCAL_RUNTIME_SCOPE,
        applied.scheduled_start.scheduled_start_id,
        case.expected_version,
    )

    repeated = await facade.commit_arbiter(
        preparation.terminal,
        scope=LOCAL_RUNTIME_SCOPE,
    )

    assert repeated == applied


async def test_due_and_cancel_race_on_the_same_exact_arbiter_identity() -> None:
    client, schedules, workflows = temporal_client()
    facade = service(client)
    created = await facade.create(
        create_request(),
        idempotency_key=IDEMPOTENCY_KEY,
        scope_binding=SCOPE_BINDING,
    )
    record = action_record(schedules)

    claimed = await facade.claim_due(
        record,
        scope=LOCAL_RUNTIME_SCOPE,
        priority=Priority(),
    )
    with pytest.raises(ScheduledStartError) as raised:
        await facade.cancel(
            created.scheduled_start.scheduled_start_id,
            ScheduledStartCancelRequest(expected_version=1),
            idempotency_key="cancel-key",
            scope=LOCAL_RUNTIME_SCOPE,
        )

    arbiter_id = make_scheduled_start_arbiter_id(
        LOCAL_RUNTIME_SCOPE,
        record.scheduled_start_id,
        record.version,
    )
    assert arbiter_id in workflows
    assert workflows[arbiter_id]._memo[MEMO_SCHEDULED_START_ARBITER_COMMAND] == "due"
    assert raised.value.code is ScheduledStartErrorCode.CONFLICT
    assert claimed.status == "claimed"


async def test_stale_due_is_fenced_after_reschedule_even_without_arbiter_history() -> None:
    client, schedules, workflows = temporal_client()
    facade = service(client)
    await facade.create(
        create_request(),
        idempotency_key=IDEMPOTENCY_KEY,
        scope_binding=SCOPE_BINDING,
    )
    stale = action_record(schedules)
    initial = ScheduledStartArbiterInitialInput(
        record=stale,
        command=ScheduledStartRescheduleCommand(
            expected_version=1,
            start_at=RESCHEDULED_AT,
            request_digest=RESCHEDULE_DIGEST,
        ),
    )
    preparation = await facade.prepare_arbiter(initial, scope=LOCAL_RUNTIME_SCOPE)
    assert isinstance(preparation, ScheduledStartArbiterPreparedProjection)
    await facade.commit_arbiter(preparation.terminal, scope=LOCAL_RUNTIME_SCOPE)
    workflows.clear()

    result = await facade.claim_due(
        stale,
        scope=LOCAL_RUNTIME_SCOPE,
        priority=Priority(),
    )

    assert isinstance(result, ScheduledStartDueSkipped)
    assert workflows == {}
    assert result.current.version == 2


@dataclass(frozen=True, kw_only=True)
class ExistingRunRecoveryCase:
    id: str
    command: ScheduledStartArbiterCommand
    expected_request_digest: str | None


EXISTING_RUN_RECOVERY_CASES = [
    ExistingRunRecoveryCase(
        id="due",
        command=ScheduledStartDueCommand(nominal_time=START_AT),
        expected_request_digest=None,
    ),
    ExistingRunRecoveryCase(
        id="cancel-invariant-breach",
        command=ScheduledStartCancelCommand(
            expected_version=INITIAL_VERSION,
            request_digest=CANCEL_DIGEST,
        ),
        expected_request_digest=CANCEL_DIGEST,
    ),
    ExistingRunRecoveryCase(
        id="reschedule-invariant-breach",
        command=ScheduledStartRescheduleCommand(
            expected_version=INITIAL_VERSION,
            start_at=RESCHEDULED_AT,
            request_digest=RESCHEDULE_DIGEST,
        ),
        expected_request_digest=RESCHEDULE_DIGEST,
    ),
]


@pytest.mark.parametrize(
    "case",
    EXISTING_RUN_RECOVERY_CASES,
    ids=lambda case: case.id,
)
async def test_arbiter_reality_check_projects_the_existing_run_from_its_memo(
    case: ExistingRunRecoveryCase,
) -> None:
    client, schedules, _ = temporal_client()
    starter = workflow_starter()
    starter.find_existing.return_value = StartWorkflowResult(
        workflow_id="existing-workflow-id",
        run_id="existing-run-id",
        workflow_name=WORKFLOW_NAME,
        definition_digest=MANIFEST.definition_digest,
        artifact_identity=ARTIFACT,
        environment_snapshot_digest=TARGET.environment_snapshot_digest,
        trigger_name=TRIGGER_NAME,
        source_identity_digest="d" * 64,
        status=StartStatus.DUPLICATE,
    )
    facade = service(client, starter=starter)
    await facade.create(
        create_request(),
        idempotency_key=IDEMPOTENCY_KEY,
        scope_binding=SCOPE_BINDING,
    )
    record = action_record(schedules)

    preparation = await facade.prepare_arbiter(
        ScheduledStartArbiterInitialInput(record=record, command=case.command),
        scope=LOCAL_RUNTIME_SCOPE,
    )

    assert isinstance(preparation, ScheduledStartArbiterPreparedProjection)
    terminal = preparation.terminal
    assert terminal.projection.state is ScheduledStartState.STARTED
    assert terminal.projection.run is not None
    assert terminal.projection.run.workflow_id == "existing-workflow-id"
    assert terminal.projection.run.definition_digest == MANIFEST.definition_digest
    assert terminal.winner is case.command.kind
    assert terminal.request_digest == case.expected_request_digest
    starter.resolve_active_target.assert_not_called()

    await facade.commit_arbiter(terminal, scope=LOCAL_RUNTIME_SCOPE)
    committed = await facade.describe(
        record.scheduled_start_id,
        scope=LOCAL_RUNTIME_SCOPE,
    )
    assert committed.state is ScheduledStartState.STARTED
    assert committed.run is not None
    assert committed.run.workflow_id == "existing-workflow-id"


@dataclass(frozen=True, kw_only=True)
class FireTimeFailureCase:
    id: str
    start_error: StartErrorCode
    expected_failure: ScheduledStartFailureCode


FIRE_TIME_FAILURE_CASES = [
    FireTimeFailureCase(
        id="contract-drift",
        start_error=StartErrorCode.INPUT_REJECTED,
        expected_failure=ScheduledStartFailureCode.CONTRACT_DRIFT,
    ),
    FireTimeFailureCase(
        id="target-unavailable",
        start_error=StartErrorCode.UNKNOWN_WORKFLOW,
        expected_failure=ScheduledStartFailureCode.TARGET_UNAVAILABLE,
    ),
    FireTimeFailureCase(
        id="incompatible-worker",
        start_error=StartErrorCode.INCOMPATIBLE_WORKER,
        expected_failure=ScheduledStartFailureCode.INCOMPATIBLE_WORKER,
    ),
]


@pytest.mark.parametrize("case", FIRE_TIME_FAILURE_CASES, ids=lambda case: case.id)
async def test_fire_time_target_failures_prepare_a_terminal_projection(
    case: FireTimeFailureCase,
) -> None:
    client, schedules, _ = temporal_client()
    starter = workflow_starter()
    starter.resolve_active_target = AsyncMock(return_value=TARGET)
    starter.validate_resolved_input.side_effect = WorkflowStartError(
        case.start_error,
        "target unavailable",
        retryable=False,
    )
    facade = service(client, starter=starter)
    await facade.create(
        create_request(),
        idempotency_key=IDEMPOTENCY_KEY,
        scope_binding=SCOPE_BINDING,
    )
    record = action_record(schedules)
    initial = ScheduledStartArbiterInitialInput(
        record=record,
        command=ScheduledStartDueCommand(nominal_time=record.start_at),
    )

    preparation = await facade.prepare_arbiter(initial, scope=LOCAL_RUNTIME_SCOPE)

    assert isinstance(preparation, ScheduledStartArbiterPreparedProjection)
    projection = preparation.terminal.projection
    assert projection.state is ScheduledStartState.FAILED
    assert projection.failure_code is case.expected_failure
    starter.start_accepted_api.assert_not_called()


@dataclass(frozen=True, kw_only=True)
class AttemptCase:
    id: str
    error: WorkflowStartError
    expected: ScheduledStartAttemptOutcome


ATTEMPT_CASES = [
    AttemptCase(
        id="authoritative-rejection",
        error=WorkflowStartError(
            StartErrorCode.TEMPORAL_UNAVAILABLE,
            "rejected",
            retryable=True,
            request_certainty=StartRequestCertainty.AUTHORITATIVE_REJECTION,
        ),
        expected=ScheduledStartAttemptOutcome.AUTHORITATIVE_REJECTION,
    ),
    AttemptCase(
        id="ambiguous",
        error=WorkflowStartError(
            StartErrorCode.TEMPORAL_UNAVAILABLE,
            "unknown",
            retryable=True,
            request_certainty=StartRequestCertainty.AMBIGUOUS,
        ),
        expected=ScheduledStartAttemptOutcome.AMBIGUOUS,
    ),
]


@pytest.mark.parametrize("case", ATTEMPT_CASES, ids=lambda case: case.id)
async def test_tenant_start_attempt_classification(case: AttemptCase) -> None:
    client, schedules, _ = temporal_client()
    starter = workflow_starter()
    starter.resolve_active_target = AsyncMock(return_value=TARGET)
    starter.validate_resolved_input.return_value = {"reference": "opaque-reference"}
    starter.start_accepted_api = AsyncMock(side_effect=case.error)
    facade = service(client, starter=starter)
    await facade.create(
        create_request(),
        idempotency_key=IDEMPOTENCY_KEY,
        scope_binding=SCOPE_BINDING,
    )
    record = action_record(schedules)
    preparation = await facade.prepare_arbiter(
        ScheduledStartArbiterInitialInput(
            record=record,
            command=ScheduledStartDueCommand(nominal_time=record.start_at),
        ),
        scope=LOCAL_RUNTIME_SCOPE,
    )
    assert isinstance(preparation, ScheduledStartArbiterPreparedDue)

    result = await facade.attempt_dispatch(
        ScheduledStartAttemptRequest(
            record=preparation.record,
            target=preparation.target,
        ),
        scope=LOCAL_RUNTIME_SCOPE,
        priority=Priority(),
    )

    assert result.outcome is case.expected
    assert isinstance(
        result,
        ScheduledStartAttemptRejected
        if case.expected is ScheduledStartAttemptOutcome.AUTHORITATIVE_REJECTION
        else ScheduledStartAttemptAmbiguous,
    )


async def test_successful_tenant_start_uses_the_pinned_target_and_priority() -> None:
    client, schedules, _ = temporal_client()
    starter = workflow_starter()
    starter.resolve_active_target = AsyncMock(return_value=TARGET)
    starter.validate_resolved_input.return_value = {"reference": "opaque-reference"}
    starter.start_accepted_api = AsyncMock(
        return_value=StartWorkflowResult(
            workflow_id="workflow-id",
            run_id="run-id",
            workflow_name=WORKFLOW_NAME,
            definition_digest=MANIFEST.definition_digest,
            artifact_identity=ARTIFACT,
            environment_snapshot_digest=TARGET.environment_snapshot_digest,
            trigger_name=TRIGGER_NAME,
            source_identity_digest="b" * 64,
            status=StartStatus.STARTED,
        )
    )
    facade = service(client, starter=starter)
    await facade.create(
        create_request(),
        idempotency_key=IDEMPOTENCY_KEY,
        scope_binding=SCOPE_BINDING,
    )
    record = action_record(schedules)
    preparation = await facade.prepare_arbiter(
        ScheduledStartArbiterInitialInput(
            record=record,
            command=ScheduledStartDueCommand(nominal_time=record.start_at),
        ),
        scope=LOCAL_RUNTIME_SCOPE,
    )
    assert isinstance(preparation, ScheduledStartArbiterPreparedDue)
    priority = Priority(priority_key=5, fairness_key=LOCAL_RUNTIME_SCOPE.digest)

    result = await facade.attempt_dispatch(
        ScheduledStartAttemptRequest(
            record=preparation.record,
            target=ScheduledStartResolvedTarget.from_target(TARGET),
        ),
        scope=LOCAL_RUNTIME_SCOPE,
        priority=priority,
    )

    assert result.outcome is ScheduledStartAttemptOutcome.ACCEPTED
    assert starter.start_accepted_api.await_args.kwargs["priority"] == priority


async def create_scheduled_start_listing(
    facade: ScheduledStartService,
    schedules: dict[str, FakeDescription],
    states: tuple[ScheduledStartState, ...],
    *,
    case_id: str,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    scheduled_start_ids: list[str] = []
    schedule_ids: list[str] = []
    for index, state in enumerate(states):
        business_request_id = f"pagination-{case_id}-{index}"
        result = await facade.create(
            create_request(business_request_id=business_request_id),
            idempotency_key=f"create-{business_request_id}",
            scope_binding=SCOPE_BINDING,
        )
        scheduled_start_id = result.scheduled_start.scheduled_start_id
        schedule_id = make_scheduled_start_schedule_id(
            LOCAL_RUNTIME_SCOPE,
            scheduled_start_id,
        )
        scheduled_start_ids.append(scheduled_start_id)
        schedule_ids.append(schedule_id)
        if state is ScheduledStartState.SCHEDULED:
            continue
        if state is not ScheduledStartState.CANCELED:
            raise AssertionError(f"Unsupported pagination test state: {state}")
        record = pending_record(schedules[schedule_id])
        preparation = await facade.prepare_arbiter(
            ScheduledStartArbiterInitialInput(
                record=record,
                command=ScheduledStartCancelCommand(
                    expected_version=record.version,
                    request_digest=f"{index + 1:064x}",
                ),
            ),
            scope=LOCAL_RUNTIME_SCOPE,
        )
        assert isinstance(preparation, ScheduledStartArbiterPreparedProjection)
        await facade.commit_arbiter(preparation.terminal, scope=LOCAL_RUNTIME_SCOPE)
    return tuple(scheduled_start_ids), tuple(schedule_ids)


def configure_schedule_listing(client: MagicMock, schedule_ids: tuple[str, ...]) -> None:
    async def list_schedules(**kwargs: object) -> FakeScheduleAsyncIterator:
        page_size = kwargs["page_size"]
        next_page_token = kwargs["next_page_token"]
        assert isinstance(page_size, int)
        assert isinstance(next_page_token, bytes | None)
        return FakeScheduleAsyncIterator(
            [SimpleNamespace(id=schedule_id) for schedule_id in schedule_ids],
            page_size=page_size,
            next_page_token=next_page_token,
        )

    client.list_schedules = AsyncMock(side_effect=list_schedules)


@dataclass(frozen=True, kw_only=True)
class ListPaginationCase:
    id: str
    states: tuple[ScheduledStartState, ...]
    selected_states: frozenset[ScheduledStartState] | None
    request_limits: tuple[int, ...]
    expected_page_sizes: tuple[int, ...]


LIST_PAGINATION_CASES = [
    ListPaginationCase(
        id="unfiltered-boundaries",
        states=(
            ScheduledStartState.SCHEDULED,
            ScheduledStartState.CANCELED,
            ScheduledStartState.SCHEDULED,
            ScheduledStartState.CANCELED,
        ),
        selected_states=None,
        request_limits=(2, 2),
        expected_page_sizes=(2, 2),
    ),
    ListPaginationCase(
        id="filtered-mid-page-changing-limit",
        states=(
            ScheduledStartState.CANCELED,
            ScheduledStartState.CANCELED,
            ScheduledStartState.SCHEDULED,
            ScheduledStartState.SCHEDULED,
            ScheduledStartState.SCHEDULED,
            ScheduledStartState.SCHEDULED,
            ScheduledStartState.SCHEDULED,
        ),
        selected_states=frozenset({ScheduledStartState.SCHEDULED}),
        request_limits=(3, 1, 3),
        expected_page_sizes=(3, 1, 1),
    ),
    ListPaginationCase(
        id="filtered-empty-pages",
        states=(
            ScheduledStartState.CANCELED,
            ScheduledStartState.CANCELED,
            ScheduledStartState.CANCELED,
            ScheduledStartState.SCHEDULED,
            ScheduledStartState.CANCELED,
            ScheduledStartState.SCHEDULED,
        ),
        selected_states=frozenset({ScheduledStartState.SCHEDULED}),
        request_limits=(2,),
        expected_page_sizes=(2,),
    ),
]


@dataclass(frozen=True, kw_only=True)
class CursorReturns:
    expected_indices: tuple[int, ...]


@dataclass(frozen=True, kw_only=True)
class CursorRaises:
    exc: type[Exception]
    match: str
    code: ScheduledStartErrorCode


CursorOutcome: TypeAlias = CursorReturns | CursorRaises


@dataclass(frozen=True, kw_only=True)
class CursorPayloadCase:
    id: str
    position: Mapping[str, object]
    max_list_limit: int
    outcome: CursorOutcome


ENCODED_PAGE_START = base64.urlsafe_b64encode(_encode_page_start(3)).decode("ascii")
INVALID_CURSOR = CursorRaises(
    exc=ScheduledStartError,
    match="cursor is invalid",
    code=ScheduledStartErrorCode.INVALID_REQUEST,
)
CURSOR_PAYLOAD_CASES = [
    CursorPayloadCase(
        id="legacy-page-boundary",
        position={"page_token": ENCODED_PAGE_START},
        max_list_limit=2,
        outcome=CursorReturns(expected_indices=(3,)),
    ),
    CursorPayloadCase(
        id="collection-beginning",
        position={
            "page_token": None,
            "page_offset": 0,
            "page_size": 2,
            "state_filter": None,
        },
        max_list_limit=2,
        outcome=INVALID_CURSOR,
    ),
    CursorPayloadCase(
        id="missing-original-page-size",
        position={"page_token": None, "page_offset": 1, "state_filter": None},
        max_list_limit=2,
        outcome=INVALID_CURSOR,
    ),
    CursorPayloadCase(
        id="offset-above-page-size",
        position={
            "page_token": None,
            "page_offset": 3,
            "page_size": 2,
            "state_filter": None,
        },
        max_list_limit=2,
        outcome=INVALID_CURSOR,
    ),
    CursorPayloadCase(
        id="configured-page-size-bound",
        position={
            "page_token": ENCODED_PAGE_START,
            "page_offset": 0,
            "page_size": 3,
            "state_filter": None,
        },
        max_list_limit=2,
        outcome=INVALID_CURSOR,
    ),
    CursorPayloadCase(
        id="invalid-page-token",
        position={
            "page_token": "not-base64!",
            "page_offset": 0,
            "page_size": 2,
            "state_filter": None,
        },
        max_list_limit=2,
        outcome=INVALID_CURSOR,
    ),
]


@pytest.mark.parametrize("case", LIST_PAGINATION_CASES, ids=lambda case: case.id)
async def test_list_cursor_returns_each_selected_schedule_once(
    case: ListPaginationCase,
) -> None:
    client, schedules, _ = temporal_client()
    facade = service(client)
    scheduled_start_ids, schedule_ids = await create_scheduled_start_listing(
        facade,
        schedules,
        case.states,
        case_id=case.id,
    )
    configure_schedule_listing(client, schedule_ids)
    expected_ids = tuple(
        scheduled_start_id
        for scheduled_start_id, state in zip(scheduled_start_ids, case.states, strict=True)
        if case.selected_states is None or state in case.selected_states
    )
    cursor: str | None = None
    listed_ids: list[str] = []
    page_sizes: list[int] = []

    for request_index, limit in enumerate(case.request_limits):
        if request_index > 0 and cursor is None:
            break
        page = await facade.list(
            scope=LOCAL_RUNTIME_SCOPE,
            limit=limit,
            cursor=cursor,
            states=case.selected_states,
        )
        page_sizes.append(len(page.scheduled_starts))
        listed_ids.extend(item.scheduled_start_id for item in page.scheduled_starts)
        cursor = page.next_cursor

    assert tuple(page_sizes) == case.expected_page_sizes
    assert tuple(listed_ids) == expected_ids
    assert len(listed_ids) == len(set(listed_ids))
    assert cursor is None


async def test_list_rejects_an_offset_beyond_a_changed_server_page() -> None:
    states = (
        ScheduledStartState.CANCELED,
        ScheduledStartState.CANCELED,
        ScheduledStartState.SCHEDULED,
        ScheduledStartState.SCHEDULED,
        ScheduledStartState.SCHEDULED,
        ScheduledStartState.SCHEDULED,
        ScheduledStartState.SCHEDULED,
    )
    client, schedules, _ = temporal_client()
    facade = service(client)
    _, schedule_ids = await create_scheduled_start_listing(
        facade,
        schedules,
        states,
        case_id="changed-server-page",
    )
    configure_schedule_listing(client, schedule_ids)
    first_page = await facade.list(
        scope=LOCAL_RUNTIME_SCOPE,
        limit=3,
        states=frozenset({ScheduledStartState.SCHEDULED}),
    )
    assert first_page.next_cursor is not None
    configure_schedule_listing(client, schedule_ids[:4])

    with pytest.raises(ScheduledStartError, match="cursor is invalid") as raised:
        await facade.list(
            scope=LOCAL_RUNTIME_SCOPE,
            limit=1,
            cursor=first_page.next_cursor,
            states=frozenset({ScheduledStartState.SCHEDULED}),
        )

    assert raised.value.code is ScheduledStartErrorCode.INVALID_REQUEST


async def test_list_rejects_a_cursor_with_a_different_state_filter() -> None:
    states = (
        ScheduledStartState.CANCELED,
        ScheduledStartState.CANCELED,
        ScheduledStartState.SCHEDULED,
        ScheduledStartState.SCHEDULED,
        ScheduledStartState.SCHEDULED,
        ScheduledStartState.SCHEDULED,
    )
    client, schedules, _ = temporal_client()
    facade = service(client)
    _, schedule_ids = await create_scheduled_start_listing(
        facade,
        schedules,
        states,
        case_id="changed-state-filter",
    )
    configure_schedule_listing(client, schedule_ids)
    first_page = await facade.list(
        scope=LOCAL_RUNTIME_SCOPE,
        limit=3,
        states=frozenset({ScheduledStartState.SCHEDULED}),
    )
    assert first_page.next_cursor is not None

    with pytest.raises(ScheduledStartError, match="cursor is invalid") as raised:
        await facade.list(
            scope=LOCAL_RUNTIME_SCOPE,
            limit=3,
            cursor=first_page.next_cursor,
            states=frozenset({ScheduledStartState.CANCELED}),
        )

    assert raised.value.code is ScheduledStartErrorCode.INVALID_REQUEST


@pytest.mark.parametrize("case", CURSOR_PAYLOAD_CASES, ids=lambda case: case.id)
async def test_list_cursor_payload_is_bounded_and_legacy_compatible(
    case: CursorPayloadCase,
) -> None:
    client, schedules, _ = temporal_client()
    population_service = service(client)
    states = (ScheduledStartState.SCHEDULED,) * 4
    scheduled_start_ids, schedule_ids = await create_scheduled_start_listing(
        population_service,
        schedules,
        states,
        case_id=case.id,
    )
    configure_schedule_listing(client, schedule_ids)
    facade = service(
        client,
        settings=ScheduledStartSettings(
            default_list_limit=case.max_list_limit,
            max_list_limit=case.max_list_limit,
        ),
    )
    cursor = encode_scope_cursor(LOCAL_RUNTIME_SCOPE, dict(case.position))

    if isinstance(case.outcome, CursorRaises):
        with pytest.raises(case.outcome.exc, match=case.outcome.match) as raised:
            await facade.list(
                scope=LOCAL_RUNTIME_SCOPE,
                limit=case.max_list_limit,
                cursor=cursor,
            )
        assert isinstance(raised.value, ScheduledStartError)
        assert raised.value.code is case.outcome.code
        return

    page = await facade.list(
        scope=LOCAL_RUNTIME_SCOPE,
        limit=case.max_list_limit,
        cursor=cursor,
    )

    assert tuple(item.scheduled_start_id for item in page.scheduled_starts) == tuple(
        scheduled_start_ids[index] for index in case.outcome.expected_indices
    )
    assert page.next_cursor is None


@pytest.mark.parametrize(
    "cursor",
    [
        pytest.param("not-base64!", id="malformed-envelope"),
        pytest.param(
            encode_scope_cursor(
                OTHER_RUNTIME_SCOPE,
                {"page_token": ENCODED_PAGE_START},
            ),
            id="wrong-scope",
        ),
    ],
)
async def test_list_normalizes_invalid_cursor_envelopes(cursor: str) -> None:
    client, _, _ = temporal_client()

    with pytest.raises(ScheduledStartError, match="cursor is invalid") as raised:
        await service(client).list(
            scope=LOCAL_RUNTIME_SCOPE,
            limit=2,
            cursor=cursor,
        )

    assert raised.value.code is ScheduledStartErrorCode.INVALID_REQUEST


async def test_list_describes_schedules_with_bounded_concurrency() -> None:
    describe_concurrency = 2
    client, schedules, _ = temporal_client()
    population_service = service(client)
    expected_ids, schedule_ids = await create_scheduled_start_listing(
        population_service,
        schedules,
        (ScheduledStartState.SCHEDULED,) * 4,
        case_id="describe-concurrency",
    )
    configure_schedule_listing(client, schedule_ids)
    active_describes = 0
    peak_describes = 0
    describe_calls = 0

    def concurrent_handle(schedule_id: str) -> SimpleNamespace:
        async def describe(**_: object) -> FakeDescription:
            nonlocal active_describes, describe_calls, peak_describes
            active_describes += 1
            describe_calls += 1
            peak_describes = max(peak_describes, active_describes)
            await asyncio.sleep(0)
            active_describes -= 1
            return schedules[schedule_id]

        return SimpleNamespace(describe=describe)

    client.get_schedule_handle.side_effect = concurrent_handle
    facade = service(
        client,
        settings=ScheduledStartSettings(describe_concurrency=describe_concurrency),
    )

    page = await facade.list(scope=LOCAL_RUNTIME_SCOPE, limit=len(schedule_ids))

    assert tuple(item.scheduled_start_id for item in page.scheduled_starts) == expected_ids
    assert peak_describes == describe_concurrency
    assert describe_calls == len(schedule_ids)


async def test_best_effort_local_quota_rejects_non_local_scope() -> None:
    controller = BestEffortLocalScheduledStartQuotaController()

    with pytest.raises(ScheduledStartError) as raised:
        await controller.create(
            scope=OTHER_RUNTIME_SCOPE,
            limit=1,
            count_pending=AsyncMock(return_value=0),
            create_schedule=AsyncMock(return_value="created"),
        )

    assert raised.value.code is ScheduledStartErrorCode.QUOTA_UNAVAILABLE


async def test_best_effort_local_quota_serializes_creates_per_scope() -> None:
    quota_limit = 1
    pending_count = 0
    first_create_started = asyncio.Event()
    release_first_create = asyncio.Event()
    second_count_called = asyncio.Event()
    controller = BestEffortLocalScheduledStartQuotaController()

    async def count_before_first_create() -> int:
        return pending_count

    async def count_before_second_create() -> int:
        second_count_called.set()
        return pending_count

    async def create_first_schedule() -> str:
        nonlocal pending_count
        first_create_started.set()
        await release_first_create.wait()
        pending_count += 1
        return "first"

    async def create_second_schedule() -> str:
        nonlocal pending_count
        pending_count += 1
        return "second"

    first_create = asyncio.create_task(
        controller.create(
            scope=LOCAL_RUNTIME_SCOPE,
            limit=quota_limit,
            count_pending=count_before_first_create,
            create_schedule=create_first_schedule,
        )
    )
    await first_create_started.wait()
    second_create = asyncio.create_task(
        controller.create(
            scope=LOCAL_RUNTIME_SCOPE,
            limit=quota_limit,
            count_pending=count_before_second_create,
            create_schedule=create_second_schedule,
        )
    )
    await asyncio.sleep(0)

    assert second_count_called.is_set() is False

    release_first_create.set()
    assert await first_create == "first"
    with pytest.raises(ScheduledStartError) as raised:
        await second_create

    assert second_count_called.is_set() is True
    assert raised.value.code is ScheduledStartErrorCode.QUOTA_EXCEEDED
    assert pending_count == quota_limit


async def test_best_effort_local_quota_does_not_coordinate_across_instances() -> None:
    quota_limit = 1
    pending_count = 0
    first_create_started = asyncio.Event()
    release_first_create = asyncio.Event()
    first_controller = BestEffortLocalScheduledStartQuotaController()
    second_controller = BestEffortLocalScheduledStartQuotaController()

    async def count_pending() -> int:
        return pending_count

    async def create_first_schedule() -> str:
        nonlocal pending_count
        first_create_started.set()
        await release_first_create.wait()
        pending_count += 1
        return "first"

    async def create_second_schedule() -> str:
        nonlocal pending_count
        pending_count += 1
        return "second"

    first_create = asyncio.create_task(
        first_controller.create(
            scope=LOCAL_RUNTIME_SCOPE,
            limit=quota_limit,
            count_pending=count_pending,
            create_schedule=create_first_schedule,
        )
    )
    await first_create_started.wait()
    second_create = asyncio.create_task(
        second_controller.create(
            scope=LOCAL_RUNTIME_SCOPE,
            limit=quota_limit,
            count_pending=count_pending,
            create_schedule=create_second_schedule,
        )
    )
    await asyncio.sleep(0)
    second_was_admitted_while_first_was_pending = second_create.done()

    release_first_create.set()
    outcomes = await asyncio.gather(first_create, second_create, return_exceptions=True)

    assert second_was_admitted_while_first_was_pending is True
    assert outcomes == ["first", "second"]
    assert pending_count > quota_limit


async def test_pending_quota_counts_the_unconsumed_page_remainder() -> None:
    states = (
        ScheduledStartState.CANCELED,
        ScheduledStartState.CANCELED,
        ScheduledStartState.SCHEDULED,
        ScheduledStartState.SCHEDULED,
        ScheduledStartState.SCHEDULED,
        ScheduledStartState.SCHEDULED,
        ScheduledStartState.SCHEDULED,
    )
    client, schedules, _ = temporal_client()
    population_service = service(client)
    _, schedule_ids = await create_scheduled_start_listing(
        population_service,
        schedules,
        states,
        case_id="pending-quota",
    )
    configure_schedule_listing(client, schedule_ids)
    bounded_service = service(
        client,
        settings=ScheduledStartSettings(
            default_list_limit=3,
            max_pending_per_scope=5,
        ),
    )

    with pytest.raises(ScheduledStartError) as raised:
        await bounded_service.create(
            create_request(business_request_id="over-quota"),
            idempotency_key="create-over-quota",
            scope_binding=SCOPE_BINDING,
        )

    assert raised.value.code is ScheduledStartErrorCode.QUOTA_EXCEEDED
    assert len(schedules) == len(states)


async def test_list_and_describe_project_no_runtime_input() -> None:
    client, _, _ = temporal_client()
    facade = service(client)
    created = await facade.create(
        create_request(),
        idempotency_key=IDEMPOTENCY_KEY,
        scope_binding=SCOPE_BINDING,
    )

    detail = await facade.describe(
        created.scheduled_start.scheduled_start_id,
        scope=LOCAL_RUNTIME_SCOPE,
    )
    page = await facade.list(scope=LOCAL_RUNTIME_SCOPE, limit=10)

    assert page.scheduled_starts == (detail,)
    assert "opaque-reference" not in detail.model_dump_json()
    assert BUSINESS_REQUEST_ID not in detail.model_dump_json()


async def test_detail_derives_dispatching_from_the_due_arbiter() -> None:
    client, schedules, _ = temporal_client()
    facade = service(client)
    created = await facade.create(
        create_request(),
        idempotency_key=IDEMPOTENCY_KEY,
        scope_binding=SCOPE_BINDING,
    )
    await facade.claim_due(
        action_record(schedules),
        scope=LOCAL_RUNTIME_SCOPE,
        priority=Priority(),
    )

    description = await facade.describe(
        created.scheduled_start.scheduled_start_id,
        scope=LOCAL_RUNTIME_SCOPE,
    )

    assert description.state is ScheduledStartState.DISPATCHING
    assert description.dispatch_started_at == NOW


async def test_detail_reads_completed_arbiter_result_before_schedule_projection() -> None:
    client, schedules, workflows = temporal_client()
    facade = service(client)
    created = await facade.create(
        create_request(),
        idempotency_key=IDEMPOTENCY_KEY,
        scope_binding=SCOPE_BINDING,
    )
    record = action_record(schedules)
    await facade.claim_due(record, scope=LOCAL_RUNTIME_SCOPE, priority=Priority())
    arbiter_id = make_scheduled_start_arbiter_id(
        LOCAL_RUNTIME_SCOPE,
        record.scheduled_start_id,
        record.version,
    )
    memo = workflows[arbiter_id]._memo
    applied = started_mutation_result(
        ScheduledStartArbiterInitialInput(
            record=record,
            command=ScheduledStartDueCommand(nominal_time=record.start_at),
        ).model_dump(mode="json")
    )
    workflows[arbiter_id] = FakeWorkflowHandle(memo, applied)

    completed = await facade.describe(
        created.scheduled_start.scheduled_start_id,
        scope=LOCAL_RUNTIME_SCOPE,
    )

    assert completed.state is ScheduledStartState.STARTED
    assert completed.run is not None
    assert completed.run.workflow_id == RUN.workflow_id

    workflows[arbiter_id] = FakeWorkflowHandle(
        memo,
        ScheduledStartArbiterStale(
            consumed_version=record.version,
            current=describe_scheduled_start(record),
        ).model_dump(mode="json"),
    )

    stale = await facade.describe(
        created.scheduled_start.scheduled_start_id,
        scope=LOCAL_RUNTIME_SCOPE,
    )

    assert stale.state is ScheduledStartState.SCHEDULED


async def test_detail_rejects_a_malformed_completed_arbiter_result(
    caplog: pytest.LogCaptureFixture,
) -> None:
    client, schedules, workflows = temporal_client()
    facade = service(client)
    created = await facade.create(
        create_request(),
        idempotency_key=IDEMPOTENCY_KEY,
        scope_binding=SCOPE_BINDING,
    )
    record = action_record(schedules)
    await facade.claim_due(record, scope=LOCAL_RUNTIME_SCOPE, priority=Priority())
    arbiter_id = make_scheduled_start_arbiter_id(
        LOCAL_RUNTIME_SCOPE,
        record.scheduled_start_id,
        record.version,
    )
    memo = workflows[arbiter_id]._memo
    workflows[arbiter_id] = FakeWorkflowHandle(
        memo,
        invalid_nested_arbiter_result({}),
    )

    with pytest.raises(ScheduledStartError, match=INVALID_ARBITER_RESULT_MESSAGE) as raised:
        await facade.describe(
            created.scheduled_start.scheduled_start_id,
            scope=LOCAL_RUNTIME_SCOPE,
        )

    assert raised.value.code is ScheduledStartErrorCode.TEMPORAL_UNAVAILABLE
    assert raised.value.retryable is True
    assert PRIVATE_ARBITER_RESULT_VALUE not in str(raised.value)
    assert PRIVATE_ARBITER_RESULT_VALUE not in caplog.text


async def test_detail_rejects_an_invalid_or_abnormally_closed_arbiter() -> None:
    client, schedules, workflows = temporal_client()
    facade = service(client)
    created = await facade.create(
        create_request(),
        idempotency_key=IDEMPOTENCY_KEY,
        scope_binding=SCOPE_BINDING,
    )
    record = action_record(schedules)
    await facade.claim_due(record, scope=LOCAL_RUNTIME_SCOPE, priority=Priority())
    arbiter_id = make_scheduled_start_arbiter_id(
        LOCAL_RUNTIME_SCOPE,
        record.scheduled_start_id,
        record.version,
    )
    memo = workflows[arbiter_id]._memo
    workflows[arbiter_id] = FakeWorkflowHandle(
        memo,
        None,
        status=WorkflowExecutionStatus.FAILED,
    )

    with pytest.raises(ScheduledStartError, match="closed without") as closed:
        await facade.describe(
            created.scheduled_start.scheduled_start_id,
            scope=LOCAL_RUNTIME_SCOPE,
        )

    assert closed.value.code is ScheduledStartErrorCode.CONFLICT

    workflows[arbiter_id] = FakeWorkflowHandle({}, None)

    with pytest.raises(ScheduledStartError, match="not a valid owned") as invalid:
        await facade.describe(
            created.scheduled_start.scheduled_start_id,
            scope=LOCAL_RUNTIME_SCOPE,
        )

    assert invalid.value.code is ScheduledStartErrorCode.CONFLICT


@pytest.mark.parametrize(
    "limit",
    [
        pytest.param(0, id="zero"),
        pytest.param(
            ScheduledStartSettings().max_list_limit + 1,
            id="above-maximum",
        ),
    ],
)
async def test_list_rejects_an_out_of_range_limit(limit: int) -> None:
    client, _, _ = temporal_client()

    with pytest.raises(ScheduledStartError) as raised:
        await service(client).list(scope=LOCAL_RUNTIME_SCOPE, limit=limit)

    assert raised.value.code is ScheduledStartErrorCode.INVALID_REQUEST


async def test_list_classifies_temporal_transport_failure() -> None:
    client, _, _ = temporal_client()
    client.list_schedules = AsyncMock(side_effect=RuntimeError("transport failed"))

    with pytest.raises(ScheduledStartError) as raised:
        await service(client).list(scope=LOCAL_RUNTIME_SCOPE, limit=10)

    assert raised.value.code is ScheduledStartErrorCode.TEMPORAL_UNAVAILABLE


async def test_create_fails_closed_without_authoritative_quota_controller() -> None:
    client, schedules, _ = temporal_client()

    with pytest.raises(ScheduledStartError) as raised:
        await service(client, quota=False).create(
            create_request(),
            idempotency_key=IDEMPOTENCY_KEY,
            scope_binding=SCOPE_BINDING,
        )

    assert raised.value.code is ScheduledStartErrorCode.QUOTA_UNAVAILABLE
    assert schedules == {}


@dataclass(frozen=True, kw_only=True)
class CreateValidationCase:
    id: str
    create_request: ScheduledStartCreateRequest
    settings: ScheduledStartSettings
    expected_code: ScheduledStartErrorCode


CREATE_VALIDATION_CASES = [
    CreateValidationCase(
        id="past-skew",
        create_request=create_request(start_at=NOW - timedelta(seconds=6)),
        settings=ScheduledStartSettings(clock_skew_seconds=5),
        expected_code=ScheduledStartErrorCode.INVALID_REQUEST,
    ),
    CreateValidationCase(
        id="horizon",
        create_request=create_request(start_at=NOW + timedelta(seconds=61)),
        settings=ScheduledStartSettings(max_horizon_seconds=60),
        expected_code=ScheduledStartErrorCode.INVALID_REQUEST,
    ),
    CreateValidationCase(
        id="workload-grant",
        create_request=create_request(workload_class=ScheduledStartWorkloadClass.INTERACTIVE),
        settings=ScheduledStartSettings(
            workload_policy=ScheduledStartWorkloadPolicySettings(
                allowed_classes=frozenset({ScheduledStartWorkloadClass.STANDARD})
            )
        ),
        expected_code=ScheduledStartErrorCode.INVALID_REQUEST,
    ),
]


@pytest.mark.parametrize("case", CREATE_VALIDATION_CASES, ids=lambda case: case.id)
async def test_create_validation_matrix(case: CreateValidationCase) -> None:
    client, schedules, _ = temporal_client()

    with pytest.raises(ScheduledStartError) as raised:
        await service(client, settings=case.settings).create(
            case.create_request,
            idempotency_key=IDEMPOTENCY_KEY,
            scope_binding=SCOPE_BINDING,
        )

    assert raised.value.code is case.expected_code
    assert schedules == {}
