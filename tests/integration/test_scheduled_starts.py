"""Temporal-backed one-off scheduled-start lifecycle integration tests."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, TypeAlias, cast

import pytest
from temporalio import activity, workflow
from temporalio.api.common.v1 import Payload
from temporalio.client import (
    Client,
    ScheduleActionExecutionStartWorkflow,
    ScheduleActionStartWorkflow,
    ScheduleDescription,
    ScheduleHandle,
    WorkflowExecutionStatus,
)
from temporalio.common import Priority
from temporalio.worker import Worker

from justflow.config.models import FlowStep, WorkflowConfig
from justflow.config.runtime_limits import DEFAULT_RUNTIME_LIMITS
from justflow.config.settings import (
    ScheduledStartSettings,
    ScheduledStartWorkloadClass,
    ScheduleSettings,
)
from justflow.config.triggers import ApiTriggerDeclaration
from justflow.definitions.manifest import (
    ENGINE_WORKFLOW_ABI,
    SHA256_HEX_LENGTH,
    DefinitionManifest,
    build_definition_manifests,
)
from justflow.definitions.routing import (
    WorkerDeployment,
    WorkflowStartTarget,
    wait_for_worker_deployment,
    worker_deployment_config,
)
from justflow.engine.local_temporal import start_local_environment
from justflow.engine.sandbox import workflow_sandbox_runner
from justflow.provenance import WorkerArtifactIdentity
from justflow.runtime.schedule_reconciler import ScheduleReconciler
from justflow.runtime.scheduled_start_cleanup import (
    ScheduledStartCleanupActivity,
    ScheduledStartCleanupResult,
    ScheduledStartCleanupWorkflow,
    ScheduledStartMaintenance,
)
from justflow.runtime.scheduled_start_dispatch import (
    ScheduledStartArbiterWorkflow,
    ScheduledStartDispatchActivities,
    ScheduledStartDueWorkflow,
)
from justflow.runtime.scheduled_start_service import (
    BestEffortLocalScheduledStartQuotaController,
    ScheduledStartService,
)
from justflow.runtime.scheduled_starts import (
    SCHEDULED_START_ARBITER_RESULT_ADAPTER,
    SCHEDULED_START_ARBITER_WORKFLOW_TYPE,
    SCHEDULED_START_CLEANUP_ACTIVITY_TYPE,
    PendingScheduledStartRecord,
    ScheduledStartArbiterInitialInput,
    ScheduledStartArbiterPreparedProjection,
    ScheduledStartCancelCommand,
    ScheduledStartCancelRequest,
    ScheduledStartCreateRequest,
    ScheduledStartDescription,
    ScheduledStartDueClaimed,
    ScheduledStartDueInput,
    ScheduledStartDueSkipped,
    ScheduledStartError,
    ScheduledStartErrorCode,
    ScheduledStartFailureCode,
    ScheduledStartMutationResult,
    ScheduledStartMutationStatus,
    ScheduledStartRescheduleRequest,
    ScheduledStartState,
    make_scheduled_start_arbiter_id,
    make_scheduled_start_schedule_id,
)
from justflow.runtime.schedules import UnscopedScheduleDecision
from justflow.runtime.starter import (
    ControlApiSourceIdentity,
    StartWorkflowRequest,
    WorkflowStarter,
    WorkflowStartRegistration,
)
from justflow.scope import LOCAL_RUNTIME_SCOPE, RuntimeScope, ScopeBindingKind, TrustedScopeBinding

BUSINESS_TASK_QUEUE = "test-scheduled-start-business"
DISPATCH_TASK_QUEUE = "test-scheduled-start-dispatch"
WORKFLOW_NAME = "scheduled_entity"
TRIGGER_NAME = "scheduled_entity_api"
NOW = datetime(2024, 1, 1, 8, 0, tzinfo=UTC)
FIRST_DUE_TIME = datetime(2030, 1, 1, 8, 0, tzinfo=UTC)
SECOND_DUE_TIME = datetime(2030, 1, 2, 8, 0, tzinfo=UTC)
RESCHEDULED_DUE_TIME = datetime(2030, 1, 3, 8, 0, tzinfo=UTC)
ALTERNATE_RESCHEDULED_DUE_TIME = datetime(2030, 1, 4, 8, 0, tzinfo=UTC)
CLEANUP_TIME = datetime(2031, 1, 1, 8, 0, tzinfo=UTC)
TERMINAL_RETENTION_SECONDS = 1
DISPATCH_TIMEOUT_SECONDS = 5
DISPATCH_RPC_TIMEOUT = timedelta(seconds=DISPATCH_TIMEOUT_SECONDS)
DISPATCH_ATTEMPTS = 3
CLEANUP_PAGE_SIZE = 20
INITIAL_VERSION = 1
RESCHEDULED_VERSION = 2
EXPECTED_NATIVE_ACTION_COUNT = 1
EXPECTED_TERMINAL_SCHEDULE_COUNT = 4
ARBITRATION_CONTENDER_COUNT = 2
RESULT_TIMEOUT_SECONDS = 10
POLL_ATTEMPTS = 100
POLL_INTERVAL_SECONDS = 0.05
LONG_TEST_HORIZON_SECONDS = 7 * 365 * 24 * 60 * 60
PAGINATION_SCHEDULE_COUNT = 7
PAGINATION_CANCELED_COUNT = 2
PAGINATION_LIMITS = (3, 1, 3)
EXPECTED_PAGINATION_PAGE_SIZES = (3, 1, 1)
CLEANUP_PAGINATION_SCHEDULE_COUNT = 5
CLEANUP_PAGINATION_TERMINAL_COUNT = 3
CLEANUP_PAGINATION_PAGE_SIZE = 2
MAX_CLEANUP_PAGINATION_CALLS = CLEANUP_PAGINATION_SCHEDULE_COUNT + 1
CLEANUP_CONTINUATION_CURSOR = "continuation-cursor"
CONTROLLED_CLEANUP_SCANNED = 1
CONTROLLED_CLEANUP_DELETED = 0
CLEANUP_WORKFLOW_ID = "scheduled-start-cleanup-continuation"
MIN_EXPECTED_CLEANUP_PAGE_CALLS = 2
INPUT_SCHEMA = {
    "type": "object",
    "properties": {"reference": {"type": "string"}},
    "required": ["reference"],
    "additionalProperties": False,
}
DRIFTED_INPUT_SCHEMA = {
    "type": "object",
    "properties": {"replacement": {"type": "string"}},
    "required": ["replacement"],
    "additionalProperties": False,
}
ARTIFACT = WorkerArtifactIdentity(
    deployment_name="scheduled-start-integration",
    build_id="integration-1",
    artifact_digest=f"sha256:{'a' * SHA256_HEX_LENGTH}",
    package_version="0.1.0",
)
DEPLOYMENT = WorkerDeployment(
    artifact_identity=ARTIFACT,
    compatible_engine_workflow_abis=frozenset({ENGINE_WORKFLOW_ABI}),
)
SCOPE_BINDING = TrustedScopeBinding.create(
    kind=ScopeBindingKind.API,
    scope=LOCAL_RUNTIME_SCOPE,
    binding_id="scheduled-start-integration",
)


def _manifest(version: str, input_schema: dict[str, object]) -> DefinitionManifest:
    return build_definition_manifests(
        {
            WORKFLOW_NAME: WorkflowConfig(
                workflow=WORKFLOW_NAME,
                description=f"Scheduled entity {version}",
                input_schema=input_schema,
                steps={},
                flow=[FlowStep.model_validate({"name": "done", "terminal": True})],
            )
        },
        {},
        DEFAULT_RUNTIME_LIMITS,
    )[WORKFLOW_NAME]


FIRST_MANIFEST = _manifest("v1", INPUT_SCHEMA)
SECOND_MANIFEST = _manifest("v2", INPUT_SCHEMA)
DRIFTED_MANIFEST = _manifest("contract-drift", DRIFTED_INPUT_SCHEMA)
FIRST_TARGET = WorkflowStartTarget(
    manifest=FIRST_MANIFEST,
    deployment=DEPLOYMENT,
    environment_snapshot_digest="b" * SHA256_HEX_LENGTH,
)
SECOND_TARGET = WorkflowStartTarget(
    manifest=SECOND_MANIFEST,
    deployment=DEPLOYMENT,
    environment_snapshot_digest="c" * SHA256_HEX_LENGTH,
)
DRIFTED_TARGET = WorkflowStartTarget(
    manifest=DRIFTED_MANIFEST,
    deployment=DEPLOYMENT,
    environment_snapshot_digest="d" * SHA256_HEX_LENGTH,
)


@workflow.defn(name=FIRST_TARGET.workflow_type, sandboxed=False)
class FirstScheduledEntityWorkflow:
    @workflow.run
    async def run(self, trigger: dict[str, Any]) -> dict[str, str]:
        return {
            "definition_digest": str(trigger["definition_digest"]),
            "reference": str(trigger["globals"]["reference"]),
        }


@workflow.defn(name=SECOND_TARGET.workflow_type, sandboxed=False)
class SecondScheduledEntityWorkflow:
    @workflow.run
    async def run(self, trigger: dict[str, Any]) -> dict[str, str]:
        return {
            "definition_digest": str(trigger["definition_digest"]),
            "reference": str(trigger["globals"]["reference"]),
        }


class ControlledCleanupActivity:
    def __init__(self) -> None:
        self.inputs: list[dict[str, Any]] = []

    @activity.defn(name=SCHEDULED_START_CLEANUP_ACTIVITY_TYPE)
    async def cleanup(self, raw_page: dict[str, Any]) -> dict[str, Any]:
        cursor = CLEANUP_CONTINUATION_CURSOR if not self.inputs else None
        self.inputs.append(raw_page)
        return ScheduledStartCleanupResult(
            cursor=cursor,
            scanned=CONTROLLED_CLEANUP_SCANNED,
            deleted=CONTROLLED_CLEANUP_DELETED,
        ).model_dump(mode="json")


class MutableTargetResolver:
    def __init__(self, target: WorkflowStartTarget) -> None:
        self._target = target
        self._lock = asyncio.Lock()

    async def activate(self, target: WorkflowStartTarget) -> None:
        async with self._lock:
            self._target = target

    async def resolve(
        self,
        scope: RuntimeScope,
        workflow_name: str,
    ) -> WorkflowStartRegistration | None:
        if scope != LOCAL_RUNTIME_SCOPE or workflow_name != WORKFLOW_NAME:
            return None
        async with self._lock:
            target = self._target
        return WorkflowStartRegistration(
            target=target,
            triggers={TRIGGER_NAME: ApiTriggerDeclaration(workflow=WORKFLOW_NAME)},
        )


def _request(
    business_request_id: str,
    start_at: datetime,
    *,
    reference: str,
) -> ScheduledStartCreateRequest:
    return ScheduledStartCreateRequest(
        workflow_name=WORKFLOW_NAME,
        business_request_id=business_request_id,
        input={"reference": reference},
        start_at=start_at,
        workload_class=ScheduledStartWorkloadClass.STANDARD,
    )


async def _wait_for_state(
    service: ScheduledStartService,
    scheduled_start_id: str,
    state: ScheduledStartState,
) -> ScheduledStartDescription:
    for _ in range(POLL_ATTEMPTS):
        description = await service.describe(
            scheduled_start_id,
            scope=LOCAL_RUNTIME_SCOPE,
        )
        if description.state is state:
            return description
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
    raise AssertionError(f"Scheduled start did not reach {state.value}")


async def _dispatch_workflow_id(schedule_handle: ScheduleHandle) -> str:
    for _ in range(POLL_ATTEMPTS):
        description = await schedule_handle.describe()
        if description.info.recent_actions:
            action = description.info.recent_actions[-1].action
            assert isinstance(action, ScheduleActionExecutionStartWorkflow)
            return action.workflow_id
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
    raise AssertionError("Scheduled-start dispatch was not created")


async def _action_record(
    schedule_description: ScheduleDescription,
) -> PendingScheduledStartRecord:
    action = schedule_description.schedule.action
    assert isinstance(action, ScheduleActionStartWorkflow)
    raw = action.args[0]
    if isinstance(raw, Payload):
        decoded = await schedule_description.data_converter.decode([raw], [dict])
        raw = decoded[0]
    due = ScheduledStartDueInput.model_validate(raw)
    return PendingScheduledStartRecord.model_validate(due.record)


class AsyncStartBarrier:
    def __init__(self, parties: int) -> None:
        self._parties = parties
        self._arrivals = 0
        self._lock = asyncio.Lock()
        self._released = asyncio.Event()

    async def wait(self) -> None:
        async with self._lock:
            self._arrivals += 1
            if self._arrivals == self._parties:
                self._released.set()
        await asyncio.wait_for(
            self._released.wait(),
            timeout=RESULT_TIMEOUT_SECONDS,
        )


class ArbiterStartBarrierClient:
    def __init__(self, client: Client, barrier: AsyncStartBarrier) -> None:
        self._client = client
        self._barrier = barrier

    def __getattr__(self, name: str) -> Any:
        return getattr(self._client, name)

    async def start_workflow(
        self,
        workflow_type: str,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        if workflow_type == SCHEDULED_START_ARBITER_WORKFLOW_TYPE:
            await self._barrier.wait()
        return await self._client.start_workflow(workflow_type, *args, **kwargs)


@dataclass(frozen=True, kw_only=True)
class DueContender:
    pass


@dataclass(frozen=True, kw_only=True)
class CancelContender:
    idempotency_key: str


@dataclass(frozen=True, kw_only=True)
class RescheduleContender:
    idempotency_key: str
    start_at: datetime


RaceContender: TypeAlias = DueContender | CancelContender | RescheduleContender


@dataclass(frozen=True, kw_only=True)
class ArbitrationRaceCase:
    id: str
    left: RaceContender
    right: RaceContender
    expected_lifecycles: frozenset[tuple[ScheduledStartState, int]]
    expected_rescheduled_times: frozenset[datetime] = frozenset()
    exact_duplicate: bool = False


ARBITRATION_RACE_CASES = [
    ArbitrationRaceCase(
        id="due-vs-cancel",
        left=DueContender(),
        right=CancelContender(idempotency_key="cancel"),
        expected_lifecycles=frozenset(
            {
                (ScheduledStartState.STARTED, INITIAL_VERSION),
                (ScheduledStartState.CANCELED, INITIAL_VERSION),
            }
        ),
    ),
    ArbitrationRaceCase(
        id="due-vs-reschedule",
        left=DueContender(),
        right=RescheduleContender(
            idempotency_key="reschedule",
            start_at=RESCHEDULED_DUE_TIME,
        ),
        expected_lifecycles=frozenset(
            {
                (ScheduledStartState.STARTED, INITIAL_VERSION),
                (ScheduledStartState.SCHEDULED, RESCHEDULED_VERSION),
            }
        ),
        expected_rescheduled_times=frozenset({RESCHEDULED_DUE_TIME}),
    ),
    ArbitrationRaceCase(
        id="cancel-vs-reschedule",
        left=CancelContender(idempotency_key="cancel"),
        right=RescheduleContender(
            idempotency_key="reschedule",
            start_at=RESCHEDULED_DUE_TIME,
        ),
        expected_lifecycles=frozenset(
            {
                (ScheduledStartState.CANCELED, INITIAL_VERSION),
                (ScheduledStartState.SCHEDULED, RESCHEDULED_VERSION),
            }
        ),
        expected_rescheduled_times=frozenset({RESCHEDULED_DUE_TIME}),
    ),
    ArbitrationRaceCase(
        id="two-reschedules",
        left=RescheduleContender(
            idempotency_key="first-reschedule",
            start_at=RESCHEDULED_DUE_TIME,
        ),
        right=RescheduleContender(
            idempotency_key="second-reschedule",
            start_at=ALTERNATE_RESCHEDULED_DUE_TIME,
        ),
        expected_lifecycles=frozenset({(ScheduledStartState.SCHEDULED, RESCHEDULED_VERSION)}),
        expected_rescheduled_times=frozenset(
            {RESCHEDULED_DUE_TIME, ALTERNATE_RESCHEDULED_DUE_TIME}
        ),
    ),
    ArbitrationRaceCase(
        id="exact-cancel-retry",
        left=CancelContender(idempotency_key="same-cancel"),
        right=CancelContender(idempotency_key="same-cancel"),
        expected_lifecycles=frozenset({(ScheduledStartState.CANCELED, INITIAL_VERSION)}),
        exact_duplicate=True,
    ),
]


async def _run_contender(
    service: ScheduledStartService,
    contender: RaceContender,
    record: PendingScheduledStartRecord,
) -> ScheduledStartDueClaimed | ScheduledStartDueSkipped | ScheduledStartMutationResult:
    if isinstance(contender, DueContender):
        return await service.claim_due(
            record,
            scope=LOCAL_RUNTIME_SCOPE,
            priority=Priority(),
        )
    if isinstance(contender, CancelContender):
        return await service.cancel(
            record.scheduled_start_id,
            ScheduledStartCancelRequest(expected_version=record.version),
            idempotency_key=contender.idempotency_key,
            scope=LOCAL_RUNTIME_SCOPE,
        )
    return await service.reschedule(
        record.scheduled_start_id,
        ScheduledStartRescheduleRequest(
            start_at=contender.start_at,
            expected_version=record.version,
        ),
        idempotency_key=contender.idempotency_key,
        scope=LOCAL_RUNTIME_SCOPE,
    )


async def _wait_for_lifecycle(
    service: ScheduledStartService,
    scheduled_start_id: str,
    expected: frozenset[tuple[ScheduledStartState, int]],
) -> ScheduledStartDescription:
    for _ in range(POLL_ATTEMPTS):
        description = await service.describe(
            scheduled_start_id,
            scope=LOCAL_RUNTIME_SCOPE,
        )
        if (description.state, description.version) in expected:
            return description
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
    raise AssertionError("Scheduled start did not reach an expected race outcome")


async def _wait_for_scheduled_start_listing(
    service: ScheduledStartService,
    expected_ids: frozenset[str],
) -> tuple[str, ...]:
    for _ in range(POLL_ATTEMPTS):
        page = await service.list(
            scope=LOCAL_RUNTIME_SCOPE,
            limit=len(expected_ids),
        )
        listed_ids = tuple(item.scheduled_start_id for item in page.scheduled_starts)
        if frozenset(listed_ids) == expected_ids:
            return listed_ids
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
    raise AssertionError("Scheduled starts did not become visible for pagination")


async def test_filtered_list_cursor_preserves_a_real_temporal_page_remainder() -> None:
    resolver = MutableTargetResolver(FIRST_TARGET)
    settings = ScheduledStartSettings(
        dispatch_timeout_seconds=DISPATCH_TIMEOUT_SECONDS,
        dispatch_attempts=DISPATCH_ATTEMPTS,
        max_horizon_seconds=LONG_TEST_HORIZON_SECONDS,
    )
    async with await start_local_environment(identity="scheduled-start-pagination") as env:
        starter = WorkflowStarter(
            env.client,
            BUSINESS_TASK_QUEUE,
            target_resolver=resolver,
        )
        service = ScheduledStartService(
            env.client,
            starter,
            settings,
            task_queue=DISPATCH_TASK_QUEUE,
            quota_controller=BestEffortLocalScheduledStartQuotaController(),
            clock=lambda: NOW,
        )
        created_ids: list[str] = []
        for index in range(PAGINATION_SCHEDULE_COUNT):
            result = await service.create(
                _request(
                    f"pagination-{index}",
                    FIRST_DUE_TIME,
                    reference=f"pagination-{index}",
                ),
                idempotency_key=f"pagination-create-{index}",
                scope_binding=SCOPE_BINDING,
            )
            created_ids.append(result.scheduled_start.scheduled_start_id)

        listed_ids = await _wait_for_scheduled_start_listing(
            service,
            frozenset(created_ids),
        )
        for index, scheduled_start_id in enumerate(listed_ids[:PAGINATION_CANCELED_COUNT]):
            handle = env.client.get_schedule_handle(
                make_scheduled_start_schedule_id(
                    LOCAL_RUNTIME_SCOPE,
                    scheduled_start_id,
                )
            )
            record = await _action_record(await handle.describe())
            preparation = await service.prepare_arbiter(
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
            await service.commit_arbiter(
                preparation.terminal,
                scope=LOCAL_RUNTIME_SCOPE,
            )

        cursor: str | None = None
        selected_ids: list[str] = []
        page_sizes: list[int] = []
        for limit in PAGINATION_LIMITS:
            page = await service.list(
                scope=LOCAL_RUNTIME_SCOPE,
                limit=limit,
                cursor=cursor,
                states=frozenset({ScheduledStartState.SCHEDULED}),
            )
            page_sizes.append(len(page.scheduled_starts))
            selected_ids.extend(item.scheduled_start_id for item in page.scheduled_starts)
            cursor = page.next_cursor

        assert tuple(page_sizes) == EXPECTED_PAGINATION_PAGE_SIZES
        assert tuple(selected_ids) == listed_ids[PAGINATION_CANCELED_COUNT:]
    assert len(selected_ids) == len(set(selected_ids))
    assert cursor is None


async def test_cleanup_activity_processes_one_real_temporal_page_per_call() -> None:
    resolver = MutableTargetResolver(FIRST_TARGET)
    settings = ScheduledStartSettings(
        cleanup_page_size=CLEANUP_PAGINATION_PAGE_SIZE,
        dispatch_timeout_seconds=DISPATCH_TIMEOUT_SECONDS,
        dispatch_attempts=DISPATCH_ATTEMPTS,
        max_horizon_seconds=LONG_TEST_HORIZON_SECONDS,
        terminal_retention_seconds=TERMINAL_RETENTION_SECONDS,
    )
    async with await start_local_environment(identity="scheduled-start-cleanup-pagination") as env:
        starter = WorkflowStarter(
            env.client,
            BUSINESS_TASK_QUEUE,
            target_resolver=resolver,
        )
        service = ScheduledStartService(
            env.client,
            starter,
            settings,
            task_queue=DISPATCH_TASK_QUEUE,
            quota_controller=BestEffortLocalScheduledStartQuotaController(),
            clock=lambda: NOW,
        )
        created_ids: list[str] = []
        for index in range(CLEANUP_PAGINATION_SCHEDULE_COUNT):
            result = await service.create(
                _request(
                    f"cleanup-pagination-{index}",
                    FIRST_DUE_TIME,
                    reference=f"cleanup-pagination-{index}",
                ),
                idempotency_key=f"cleanup-pagination-create-{index}",
                scope_binding=SCOPE_BINDING,
            )
            created_ids.append(result.scheduled_start.scheduled_start_id)

        listed_ids = await _wait_for_scheduled_start_listing(
            service,
            frozenset(created_ids),
        )
        terminal_ids = listed_ids[:CLEANUP_PAGINATION_TERMINAL_COUNT]
        retained_ids = listed_ids[CLEANUP_PAGINATION_TERMINAL_COUNT:]
        for index, scheduled_start_id in enumerate(terminal_ids):
            handle = env.client.get_schedule_handle(
                make_scheduled_start_schedule_id(
                    LOCAL_RUNTIME_SCOPE,
                    scheduled_start_id,
                )
            )
            record = await _action_record(await handle.describe())
            preparation = await service.prepare_arbiter(
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
            await service.commit_arbiter(
                preparation.terminal,
                scope=LOCAL_RUNTIME_SCOPE,
            )

        cleanup = ScheduledStartCleanupActivity(
            env.client,
            settings,
            clock=lambda: CLEANUP_TIME,
        )
        cursor: str | None = None
        cleanup_results: list[ScheduledStartCleanupResult] = []
        for _ in range(MAX_CLEANUP_PAGINATION_CALLS):
            result = ScheduledStartCleanupResult.model_validate(
                await cleanup.cleanup({"cursor": cursor})
            )
            cleanup_results.append(result)
            cursor = result.cursor
            if cursor is None:
                break
        else:
            raise AssertionError("Scheduled-start cleanup cursor walk did not terminate")

        assert len(cleanup_results) >= MIN_EXPECTED_CLEANUP_PAGE_CALLS
        assert all(result.scanned <= CLEANUP_PAGINATION_PAGE_SIZE for result in cleanup_results)
        assert sum(result.scanned for result in cleanup_results) == len(listed_ids)
        assert sum(result.deleted for result in cleanup_results) == len(terminal_ids)
        for scheduled_start_id in terminal_ids:
            with pytest.raises(ScheduledStartError) as removed:
                await service.describe(
                    scheduled_start_id,
                    scope=LOCAL_RUNTIME_SCOPE,
                )
            assert removed.value.code is ScheduledStartErrorCode.NOT_FOUND
        for scheduled_start_id in retained_ids:
            retained = await service.describe(
                scheduled_start_id,
                scope=LOCAL_RUNTIME_SCOPE,
            )
            assert retained.state is ScheduledStartState.SCHEDULED


async def test_cleanup_workflow_follows_continue_as_new_to_completion() -> None:
    controlled_cleanup = ControlledCleanupActivity()
    async with (
        await start_local_environment(identity="scheduled-start-cleanup-workflow") as env,
        Worker(
            env.client,
            task_queue=DISPATCH_TASK_QUEUE,
            workflows=[ScheduledStartCleanupWorkflow],
            activities=[controlled_cleanup.cleanup],
            workflow_runner=workflow_sandbox_runner(),
            identity="scheduled-start-cleanup-worker",
        ),
    ):
        raw_result = await env.client.execute_workflow(
            ScheduledStartCleanupWorkflow.run,
            {},
            id=CLEANUP_WORKFLOW_ID,
            task_queue=DISPATCH_TASK_QUEUE,
            result_type=dict,
        )

    result = ScheduledStartCleanupResult.model_validate(raw_result)
    assert controlled_cleanup.inputs == [
        {"cursor": None},
        {"cursor": CLEANUP_CONTINUATION_CURSOR},
    ]
    assert result == ScheduledStartCleanupResult(
        cursor=None,
        scanned=CONTROLLED_CLEANUP_SCANNED,
        deleted=CONTROLLED_CLEANUP_DELETED,
    )


@pytest.mark.parametrize(
    "case",
    ARBITRATION_RACE_CASES,
    ids=lambda case: case.id,
)
async def test_scheduled_start_arbitration_has_one_durable_winner(
    case: ArbitrationRaceCase,
) -> None:
    resolver = MutableTargetResolver(SECOND_TARGET)
    settings = ScheduledStartSettings(
        dispatch_timeout_seconds=DISPATCH_TIMEOUT_SECONDS,
        dispatch_attempts=DISPATCH_ATTEMPTS,
        max_horizon_seconds=LONG_TEST_HORIZON_SECONDS,
    )
    async with await start_local_environment(identity=f"scheduled-start-race-{case.id}") as env:
        starter = WorkflowStarter(
            env.client,
            BUSINESS_TASK_QUEUE,
            target_resolver=resolver,
        )
        base_service = ScheduledStartService(
            env.client,
            starter,
            settings,
            task_queue=DISPATCH_TASK_QUEUE,
            quota_controller=BestEffortLocalScheduledStartQuotaController(),
            clock=lambda: NOW,
        )
        activities = ScheduledStartDispatchActivities(base_service)
        async with (
            Worker(
                env.client,
                task_queue=BUSINESS_TASK_QUEUE,
                workflows=[SecondScheduledEntityWorkflow],
                deployment_config=worker_deployment_config(DEPLOYMENT),
                identity=f"scheduled-start-race-business-{case.id}",
            ),
            Worker(
                env.client,
                task_queue=DISPATCH_TASK_QUEUE,
                workflows=[ScheduledStartArbiterWorkflow, ScheduledStartDueWorkflow],
                activities=[
                    activities.claim,
                    activities.prepare,
                    activities.attempt,
                    activities.commit,
                ],
                workflow_runner=workflow_sandbox_runner(),
                identity=f"scheduled-start-race-dispatch-{case.id}",
            ),
        ):
            await wait_for_worker_deployment(
                env.client,
                DEPLOYMENT,
                BUSINESS_TASK_QUEUE,
            )
            created = await base_service.create(
                _request(case.id, SECOND_DUE_TIME, reference=case.id),
                idempotency_key=f"create-{case.id}",
                scope_binding=SCOPE_BINDING,
            )
            schedule = env.client.get_schedule_handle(
                make_scheduled_start_schedule_id(
                    LOCAL_RUNTIME_SCOPE,
                    created.scheduled_start.scheduled_start_id,
                )
            )
            record = await _action_record(await schedule.describe())
            barrier = AsyncStartBarrier(parties=ARBITRATION_CONTENDER_COUNT)
            left_service = ScheduledStartService(
                cast(Client, ArbiterStartBarrierClient(env.client, barrier)),
                starter,
                settings,
                task_queue=DISPATCH_TASK_QUEUE,
                quota_controller=BestEffortLocalScheduledStartQuotaController(),
                clock=lambda: NOW,
            )
            right_service = ScheduledStartService(
                cast(Client, ArbiterStartBarrierClient(env.client, barrier)),
                starter,
                settings,
                task_queue=DISPATCH_TASK_QUEUE,
                quota_controller=BestEffortLocalScheduledStartQuotaController(),
                clock=lambda: NOW,
            )

            outcomes = await asyncio.gather(
                _run_contender(left_service, case.left, record),
                _run_contender(right_service, case.right, record),
                return_exceptions=True,
            )
            description = await _wait_for_lifecycle(
                base_service,
                record.scheduled_start_id,
                case.expected_lifecycles,
            )
            arbiter_handle = env.client.get_workflow_handle(
                make_scheduled_start_arbiter_id(
                    LOCAL_RUNTIME_SCOPE,
                    record.scheduled_start_id,
                    record.version,
                ),
                result_type=dict,
            )
            arbiter_result = SCHEDULED_START_ARBITER_RESULT_ADAPTER.validate_python(
                await asyncio.wait_for(
                    arbiter_handle.result(rpc_timeout=DISPATCH_RPC_TIMEOUT),
                    timeout=RESULT_TIMEOUT_SECONDS,
                )
            )

            unexpected = [
                outcome
                for outcome in outcomes
                if isinstance(outcome, BaseException)
                and not isinstance(outcome, ScheduledStartError)
            ]
            assert unexpected == []
            conflicts = [
                outcome for outcome in outcomes if isinstance(outcome, ScheduledStartError)
            ]
            assert all(conflict.code is ScheduledStartErrorCode.CONFLICT for conflict in conflicts)
            assert arbiter_result.consumed_version == record.version
            mutation_results = [
                outcome for outcome in outcomes if isinstance(outcome, ScheduledStartMutationResult)
            ]
            due_results = [
                outcome
                for outcome in outcomes
                if isinstance(outcome, ScheduledStartDueClaimed | ScheduledStartDueSkipped)
            ]
            if case.exact_duplicate:
                assert {result.status for result in mutation_results} == {
                    ScheduledStartMutationStatus.CANCELED,
                    ScheduledStartMutationStatus.DUPLICATE,
                }
                assert conflicts == []
            elif due_results:
                assert len(due_results) == 1
                if description.state is ScheduledStartState.STARTED:
                    assert isinstance(due_results[0], ScheduledStartDueClaimed)
                    assert len(conflicts) == 1
                else:
                    assert isinstance(due_results[0], ScheduledStartDueSkipped)
                    assert len(mutation_results) == 1
            else:
                assert len(mutation_results) == 1
                assert len(conflicts) == 1

            if description.state is ScheduledStartState.SCHEDULED:
                assert description.start_at in case.expected_rescheduled_times
            existing = await starter.find_existing(
                record.workflow_name,
                record.business_request_id,
                scope=LOCAL_RUNTIME_SCOPE,
            )
            if description.state is ScheduledStartState.STARTED:
                assert existing is not None
                assert description.run is not None
                assert existing.workflow_id == description.run.workflow_id
            else:
                assert existing is None


async def test_scheduled_start_lifecycle_is_temporal_durable_and_resolves_at_dispatch() -> None:
    resolver = MutableTargetResolver(FIRST_TARGET)
    settings = ScheduledStartSettings(
        terminal_retention_seconds=TERMINAL_RETENTION_SECONDS,
        cleanup_page_size=CLEANUP_PAGE_SIZE,
        dispatch_timeout_seconds=DISPATCH_TIMEOUT_SECONDS,
        dispatch_attempts=DISPATCH_ATTEMPTS,
        max_horizon_seconds=LONG_TEST_HORIZON_SECONDS,
    )

    async with await start_local_environment(identity="scheduled-start-integration-client") as env:
        starter = WorkflowStarter(
            env.client,
            BUSINESS_TASK_QUEUE,
            target_resolver=resolver,
        )
        service = ScheduledStartService(
            env.client,
            starter,
            settings,
            task_queue=DISPATCH_TASK_QUEUE,
            quota_controller=BestEffortLocalScheduledStartQuotaController(),
            clock=lambda: NOW,
        )
        activities = ScheduledStartDispatchActivities(service)
        async with Worker(
            env.client,
            task_queue=BUSINESS_TASK_QUEUE,
            workflows=[FirstScheduledEntityWorkflow, SecondScheduledEntityWorkflow],
            deployment_config=worker_deployment_config(DEPLOYMENT),
            identity="scheduled-start-business-worker",
        ):
            await wait_for_worker_deployment(env.client, DEPLOYMENT, BUSINESS_TASK_QUEUE)
            request = _request("future-entity", FIRST_DUE_TIME, reference="entity-ref")
            accepted = await service.create(
                request,
                idempotency_key="future-create",
                scope_binding=SCOPE_BINDING,
            )
            duplicate = await service.create(
                request,
                idempotency_key="future-create",
                scope_binding=SCOPE_BINDING,
            )
            with pytest.raises(ScheduledStartError) as conflict:
                await service.create(
                    request.model_copy(update={"start_at": SECOND_DUE_TIME}),
                    idempotency_key="conflicting-create",
                    scope_binding=SCOPE_BINDING,
                )

            assert accepted.status is ScheduledStartMutationStatus.ACCEPTED
            assert accepted.scheduled_start.run is None
            assert duplicate.status is ScheduledStartMutationStatus.DUPLICATE
            assert conflict.value.code is ScheduledStartErrorCode.CONFLICT

            schedule_handle = env.client.get_schedule_handle(
                make_scheduled_start_schedule_id(
                    LOCAL_RUNTIME_SCOPE,
                    accepted.scheduled_start.scheduled_start_id,
                )
            )
            native_pending = await schedule_handle.describe()
            assert tuple(native_pending.info.next_action_times) == (FIRST_DUE_TIME,)
            assert native_pending.schedule.state.limited_actions is True
            assert native_pending.schedule.state.remaining_actions == EXPECTED_NATIVE_ACTION_COUNT

            maintenance = ScheduledStartMaintenance(
                env.client,
                settings,
                task_queue=DISPATCH_TASK_QUEUE,
            )
            await maintenance.ensure_schedule()
            recurring_plan = await ScheduleReconciler(
                env.client,
                ScheduleSettings(task_queue="recurring-dispatch"),
            ).plan({}, unscoped_decision=UnscopedScheduleDecision.MIGRATE)
            assert recurring_plan.changes == ()

            await resolver.activate(SECOND_TARGET)
            await schedule_handle.trigger()
            dispatch_workflow_id = await _dispatch_workflow_id(schedule_handle)
            pending_dispatch = await env.client.get_workflow_handle(dispatch_workflow_id).describe()
            assert pending_dispatch.status is WorkflowExecutionStatus.RUNNING

            async with Worker(
                env.client,
                task_queue=DISPATCH_TASK_QUEUE,
                workflows=[ScheduledStartArbiterWorkflow, ScheduledStartDueWorkflow],
                activities=[
                    activities.claim,
                    activities.prepare,
                    activities.attempt,
                    activities.commit,
                ],
                workflow_runner=workflow_sandbox_runner(),
                identity="scheduled-start-dispatch-worker",
            ):
                await asyncio.wait_for(
                    env.client.get_workflow_handle(dispatch_workflow_id).result(),
                    timeout=RESULT_TIMEOUT_SECONDS,
                )
                started = await asyncio.wait_for(
                    _wait_for_state(
                        service,
                        accepted.scheduled_start.scheduled_start_id,
                        ScheduledStartState.STARTED,
                    ),
                    timeout=RESULT_TIMEOUT_SECONDS,
                )
                assert started.run is not None
                scheduled_result = await asyncio.wait_for(
                    env.client.get_workflow_handle(started.run.workflow_id).result(),
                    timeout=RESULT_TIMEOUT_SECONDS,
                )
                assert scheduled_result == {
                    "definition_digest": SECOND_MANIFEST.definition_digest,
                    "reference": "entity-ref",
                }
                assert started.run.definition_digest == SECOND_MANIFEST.definition_digest
                assert (
                    started.run.environment_snapshot_digest
                    == SECOND_TARGET.environment_snapshot_digest
                )

                immediate = await starter.start(
                    StartWorkflowRequest(
                        workflow_name=WORKFLOW_NAME,
                        business_request_id="immediate-entity",
                        input={"reference": "immediate-ref"},
                        source=ControlApiSourceIdentity(request_id="immediate-entity"),
                    ),
                    scope_binding=SCOPE_BINDING,
                )
                immediate_result = await asyncio.wait_for(
                    env.client.get_workflow_handle(immediate.workflow_id).result(),
                    timeout=RESULT_TIMEOUT_SECONDS,
                )
                assert immediate_result["definition_digest"] == SECOND_MANIFEST.definition_digest

                canceled_request = _request(
                    "canceled-entity",
                    SECOND_DUE_TIME,
                    reference="canceled-ref",
                )
                canceled_create = await service.create(
                    canceled_request,
                    idempotency_key="cancel-create",
                    scope_binding=SCOPE_BINDING,
                )
                reschedule_request = ScheduledStartRescheduleRequest(
                    start_at=RESCHEDULED_DUE_TIME,
                    expected_version=INITIAL_VERSION,
                )
                rescheduled = await service.reschedule(
                    canceled_create.scheduled_start.scheduled_start_id,
                    reschedule_request,
                    idempotency_key="reschedule",
                    scope=LOCAL_RUNTIME_SCOPE,
                )
                repeated_reschedule = await service.reschedule(
                    canceled_create.scheduled_start.scheduled_start_id,
                    reschedule_request,
                    idempotency_key="reschedule",
                    scope=LOCAL_RUNTIME_SCOPE,
                )
                canceled = await service.cancel(
                    canceled_create.scheduled_start.scheduled_start_id,
                    ScheduledStartCancelRequest(expected_version=RESCHEDULED_VERSION),
                    idempotency_key="cancel",
                    scope=LOCAL_RUNTIME_SCOPE,
                )
                repeated_cancel = await service.cancel(
                    canceled_create.scheduled_start.scheduled_start_id,
                    ScheduledStartCancelRequest(expected_version=RESCHEDULED_VERSION),
                    idempotency_key="cancel",
                    scope=LOCAL_RUNTIME_SCOPE,
                )
                assert rescheduled.scheduled_start.start_at == RESCHEDULED_DUE_TIME
                assert rescheduled.scheduled_start.version == RESCHEDULED_VERSION
                assert repeated_reschedule.status is ScheduledStartMutationStatus.DUPLICATE
                assert canceled.scheduled_start.state is ScheduledStartState.CANCELED
                assert repeated_cancel.status is ScheduledStartMutationStatus.DUPLICATE

                race_create = await service.create(
                    _request("race-entity", SECOND_DUE_TIME, reference="race-ref"),
                    idempotency_key="race-create",
                    scope_binding=SCOPE_BINDING,
                )
                race_schedule = env.client.get_schedule_handle(
                    make_scheduled_start_schedule_id(
                        LOCAL_RUNTIME_SCOPE,
                        race_create.scheduled_start.scheduled_start_id,
                    )
                )
                race_record = await _action_record(await race_schedule.describe())
                dispatch_result, cancel_result = await asyncio.gather(
                    service.claim_due(
                        race_record,
                        scope=LOCAL_RUNTIME_SCOPE,
                        priority=Priority(),
                    ),
                    service.cancel(
                        race_create.scheduled_start.scheduled_start_id,
                        ScheduledStartCancelRequest(expected_version=INITIAL_VERSION),
                        idempotency_key="race-cancel",
                        scope=LOCAL_RUNTIME_SCOPE,
                    ),
                    return_exceptions=True,
                )
                if isinstance(dispatch_result, ScheduledStartDueClaimed):
                    await _wait_for_state(
                        service,
                        race_create.scheduled_start.scheduled_start_id,
                        ScheduledStartState.STARTED,
                    )
                    assert isinstance(cancel_result, ScheduledStartError)
                    assert cancel_result.code is ScheduledStartErrorCode.CONFLICT
                else:
                    assert isinstance(dispatch_result, ScheduledStartDueSkipped)
                    assert isinstance(cancel_result, ScheduledStartMutationResult)
                    assert cancel_result.scheduled_start.state is ScheduledStartState.CANCELED

                drift_create = await service.create(
                    _request("drift-entity", SECOND_DUE_TIME, reference="drift-ref"),
                    idempotency_key="drift-create",
                    scope_binding=SCOPE_BINDING,
                )
                await resolver.activate(DRIFTED_TARGET)
                drift_schedule = env.client.get_schedule_handle(
                    make_scheduled_start_schedule_id(
                        LOCAL_RUNTIME_SCOPE,
                        drift_create.scheduled_start.scheduled_start_id,
                    )
                )
                await drift_schedule.trigger()
                drifted = await asyncio.wait_for(
                    _wait_for_state(
                        service,
                        drift_create.scheduled_start.scheduled_start_id,
                        ScheduledStartState.FAILED,
                    ),
                    timeout=RESULT_TIMEOUT_SECONDS,
                )
                assert drifted.failure_code is ScheduledStartFailureCode.CONTRACT_DRIFT
                assert drifted.run is None

            native_terminal = await schedule_handle.describe()
            assert native_terminal.info.num_actions == EXPECTED_NATIVE_ACTION_COUNT
            assert native_terminal.schedule.state.remaining_actions == 0
            assert native_terminal.schedule.state.paused is True

            cleanup = ScheduledStartCleanupActivity(
                env.client,
                settings,
                clock=lambda: CLEANUP_TIME,
            )
            cleanup_result = ScheduledStartCleanupResult.model_validate(await cleanup.cleanup({}))
            assert cleanup_result.deleted == EXPECTED_TERMINAL_SCHEDULE_COUNT

            with pytest.raises(ScheduledStartError) as removed:
                await service.describe(
                    accepted.scheduled_start.scheduled_start_id,
                    scope=LOCAL_RUNTIME_SCOPE,
                )
            assert removed.value.code is ScheduledStartErrorCode.NOT_FOUND
