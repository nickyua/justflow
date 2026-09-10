"""Managed schedule operator contract tests."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import TypeAlias
from unittest.mock import AsyncMock, MagicMock

import pytest
from temporalio.client import (
    ScheduleActionExecutionStartWorkflow,
    ScheduleActionResult,
)
from temporalio.exceptions import WorkflowAlreadyStartedError

from justflow.config.models import FlowStep, WorkflowConfig
from justflow.config.runtime_limits import DEFAULT_RUNTIME_LIMITS
from justflow.config.schedules import (
    BackfillPolicy,
    CalendarRange,
    CalendarScheduleSpec,
    IntervalScheduleSpec,
)
from justflow.config.settings import ScheduleSettings
from justflow.config.triggers import ScheduleTriggerDeclaration, TriggersConfig
from justflow.definitions.manifest import (
    ENGINE_WORKFLOW_ABI,
    SHA256_HEX_LENGTH,
    build_definition_manifests,
)
from justflow.definitions.routing import WorkerDeployment, WorkflowStartTarget
from justflow.provenance import WorkerArtifactIdentity
from justflow.runtime.schedule_operations import (
    ScheduleOperationError,
    ScheduleOperationErrorCode,
    ScheduleOperator,
    ScopedScheduleControlService,
    TriggerRunNowStatus,
)
from justflow.runtime.schedules import compile_schedule
from justflow.scope import LOCAL_RUNTIME_SCOPE, RuntimeScope

SCHEDULE_NAME = "daily_orders"
WORKFLOW_NAME = "record_flow"
TASK_QUEUE = "test-queue"
ENVIRONMENT_SNAPSHOT_DIGEST = "e" * SHA256_HEX_LENGTH
NOW = datetime(2026, 1, 1, tzinfo=UTC)
RUN_NOW_IDENTITY = "f" * 64
WORKFLOW_CONFIG = WorkflowConfig(
    workflow=WORKFLOW_NAME,
    steps={},
    flow=[FlowStep(name="done", terminal=True)],
)
MANIFEST = build_definition_manifests(
    {WORKFLOW_NAME: WORKFLOW_CONFIG},
    {},
    DEFAULT_RUNTIME_LIMITS,
)[WORKFLOW_NAME]
ARTIFACT = WorkerArtifactIdentity(
    deployment_name="justflow",
    build_id="test-build",
    artifact_digest=f"sha256:{'a' * SHA256_HEX_LENGTH}",
    package_version="0.1.0",
)
TARGET = WorkflowStartTarget(
    manifest=MANIFEST,
    deployment=WorkerDeployment(
        artifact_identity=ARTIFACT,
        compatible_engine_workflow_abis=frozenset({ENGINE_WORKFLOW_ABI}),
    ),
    environment_snapshot_digest=ENVIRONMENT_SNAPSHOT_DIGEST,
)
DECLARATION = ScheduleTriggerDeclaration(
    workflow=WORKFLOW_NAME,
    spec=IntervalScheduleSpec(every_seconds=60),
    backfill=BackfillPolicy(enabled=True, max_window_seconds=600, max_actions=100),
)
DESIRED = compile_schedule(
    SCHEDULE_NAME,
    DECLARATION,
    TARGET,
    task_queue=TASK_QUEUE,
)


def schedule_description() -> SimpleNamespace:
    async def memo_value(key: str, default: object, *, type_hint: type) -> object:
        del type_hint
        return DESIRED.memo.get(key, default)

    info = SimpleNamespace(
        next_action_times=[NOW + timedelta(minutes=1)],
        recent_actions=[
            ScheduleActionResult(
                scheduled_at=NOW - timedelta(minutes=1),
                started_at=NOW - timedelta(minutes=1),
                action=ScheduleActionExecutionStartWorkflow(
                    workflow_id="dispatch-workflow-id",
                    first_execution_run_id="dispatch-run-id",
                ),
            )
        ],
    )
    return SimpleNamespace(
        id=DESIRED.schedule_id,
        schedule=DESIRED.schedule,
        info=info,
        memo_value=memo_value,
        data_converter=MagicMock(),
    )


def operator(
    declaration: ScheduleTriggerDeclaration = DECLARATION,
) -> tuple[ScheduleOperator, MagicMock, MagicMock]:
    client = MagicMock()
    handle = MagicMock()
    handle.describe = AsyncMock(return_value=schedule_description())
    handle.pause = AsyncMock()
    handle.unpause = AsyncMock()
    handle.trigger = AsyncMock()
    handle.backfill = AsyncMock()
    handle.delete = AsyncMock()
    client.get_schedule_handle.return_value = handle
    client.start_workflow = AsyncMock()
    return (
        ScheduleOperator(
            client,
            TriggersConfig(triggers={SCHEDULE_NAME: declaration}),
            ScheduleSettings(),
        ),
        client,
        handle,
    )


class AsyncEntries:
    def __init__(self, values: list[object]) -> None:
        self._values = iter(values)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._values)
        except StopIteration:
            raise StopAsyncIteration from None


def listed_schedule() -> SimpleNamespace:
    async def memo_value(key: str, default: object, *, type_hint: type) -> object:
        del type_hint
        return DESIRED.memo.get(key, default)

    return SimpleNamespace(id=DESIRED.schedule_id, memo_value=memo_value)


async def test_describe_returns_pinned_target_next_run_and_recent_outcome():
    service, _, _ = operator()

    result = await service.describe(SCHEDULE_NAME)

    assert result.schedule_name == SCHEDULE_NAME
    assert result.desired_digest == DESIRED.desired_digest
    assert result.definition_digest == MANIFEST.definition_digest
    assert result.artifact_identity == ARTIFACT
    assert result.environment_snapshot_digest == ENVIRONMENT_SNAPSHOT_DIGEST
    assert result.next_run_times == (NOW + timedelta(minutes=1),)
    assert result.recent_actions[0].outcome == "accepted"


async def test_list_returns_a_bounded_managed_schedule_description():
    service, client, _ = operator()
    client.list_schedules = AsyncMock(return_value=AsyncEntries([listed_schedule()]))

    result = await service.list()

    assert len(result) == 1
    assert result[0].schedule_name == SCHEDULE_NAME


async def test_list_layers_temporal_failures_without_exposing_the_cause():
    service, client, _ = operator()
    client.list_schedules = AsyncMock(side_effect=RuntimeError("synthetic-sensitive-cause"))

    with pytest.raises(ScheduleOperationError) as raised:
        await service.list()

    assert raised.value.code is ScheduleOperationErrorCode.TEMPORAL_UNAVAILABLE
    assert "sensitive" not in str(raised.value)


@pytest.mark.parametrize(
    ("operation", "mock_name"),
    [
        pytest.param("pause", "pause", id="pause"),
        pytest.param("resume", "unpause", id="resume"),
    ],
)
async def test_non_destructive_operator_actions(
    operation: str,
    mock_name: str,
):
    service, _, handle = operator()

    await getattr(service, operation)(SCHEDULE_NAME)

    getattr(handle, mock_name).assert_awaited_once()


async def test_run_now_starts_one_temporal_dispatch_with_a_stable_identity():
    service, client, handle = operator()

    result = await service.trigger_now(
        SCHEDULE_NAME,
        request_identity_digest=RUN_NOW_IDENTITY,
    )

    assert result is TriggerRunNowStatus.ACCEPTED
    client.start_workflow.assert_awaited_once()
    kwargs = client.start_workflow.await_args.kwargs
    assert kwargs["id"].startswith("jf1.schedule-run-now.")
    assert kwargs["task_queue"] == TASK_QUEUE
    handle.trigger.assert_not_awaited()


async def test_run_now_reports_an_existing_temporal_dispatch_as_idempotent():
    service, client, _ = operator()
    client.start_workflow.side_effect = WorkflowAlreadyStartedError(
        "dispatch-id",
        "justflow.schedule-dispatch.v1",
    )

    result = await service.trigger_now(
        SCHEDULE_NAME,
        request_identity_digest=RUN_NOW_IDENTITY,
    )

    assert result is TriggerRunNowStatus.ALREADY_ACCEPTED


async def test_inactive_schedule_trigger_cannot_be_run_now():
    service, _, handle = operator(DECLARATION.model_copy(update={"paused": True}))

    with pytest.raises(ScheduleOperationError) as raised:
        await service.trigger_now(
            SCHEDULE_NAME,
            request_identity_digest=RUN_NOW_IDENTITY,
        )

    assert raised.value.code is ScheduleOperationErrorCode.INVALID_OPERATION
    handle.trigger.assert_not_awaited()


async def test_delete_requires_the_current_managed_digest():
    service, _, handle = operator()

    with pytest.raises(ScheduleOperationError) as raised:
        await service.delete(SCHEDULE_NAME, confirmation="wrong-digest")

    assert raised.value.code is ScheduleOperationErrorCode.CONFIRMATION_REQUIRED
    handle.delete.assert_not_awaited()


async def test_delete_with_explicit_identity_removes_the_managed_schedule():
    service, _, handle = operator()

    await service.delete(SCHEDULE_NAME, confirmation=DESIRED.desired_digest)

    handle.delete.assert_awaited_once()


async def test_scoped_schedule_controls_reject_an_unregistered_scope():
    service, _, handle = operator()
    controls = ScopedScheduleControlService({LOCAL_RUNTIME_SCOPE: service})
    other_scope = RuntimeScope.create(
        tenant="other-tenant",
        application="other-application",
        environment="production",
    )

    with pytest.raises(ScheduleOperationError) as raised:
        await controls.pause(SCHEDULE_NAME, scope=other_scope)

    assert raised.value.code is ScheduleOperationErrorCode.NOT_MANAGED
    handle.pause.assert_not_awaited()


@dataclass(frozen=True, kw_only=True)
class Returns:
    value: bool


@dataclass(frozen=True, kw_only=True)
class Raises:
    exc: type[ScheduleOperationError]
    match: str


Outcome: TypeAlias = Returns | Raises


@dataclass(frozen=True, kw_only=True)
class BackfillCase:
    id: str
    start_at: datetime
    end_at: datetime
    max_actions: int
    outcome: Outcome


BACKFILL_CASES = [
    BackfillCase(
        id="bounded-aware-window",
        start_at=NOW,
        end_at=NOW + timedelta(seconds=30),
        max_actions=100,
        outcome=Returns(value=True),
    ),
    BackfillCase(
        id="naive-start",
        start_at=NOW.replace(tzinfo=None),
        end_at=NOW + timedelta(seconds=30),
        max_actions=100,
        outcome=Raises(exc=ScheduleOperationError, match="timezone offsets"),
    ),
    BackfillCase(
        id="reversed-window",
        start_at=NOW,
        end_at=NOW - timedelta(seconds=1),
        max_actions=100,
        outcome=Raises(exc=ScheduleOperationError, match="after its start"),
    ),
    BackfillCase(
        id="window-limit",
        start_at=NOW,
        end_at=NOW + timedelta(seconds=601),
        max_actions=100,
        outcome=Raises(exc=ScheduleOperationError, match="time window"),
    ),
    BackfillCase(
        id="action-limit",
        start_at=NOW,
        end_at=NOW + timedelta(seconds=100),
        max_actions=1,
        outcome=Raises(exc=ScheduleOperationError, match="action bound"),
    ),
]


@pytest.mark.parametrize("case", BACKFILL_CASES, ids=lambda case: case.id)
async def test_backfill_bounds(case: BackfillCase):
    declaration = DECLARATION.model_copy(
        update={
            "backfill": BackfillPolicy(
                enabled=True,
                max_window_seconds=600,
                max_actions=case.max_actions,
            )
        }
    )
    service, _, handle = operator(declaration)

    if isinstance(case.outcome, Returns):
        await service.backfill(
            SCHEDULE_NAME,
            start_at=case.start_at,
            end_at=case.end_at,
        )
        handle.backfill.assert_awaited_once()
        return

    with pytest.raises(case.outcome.exc, match=case.outcome.match):
        await service.backfill(
            SCHEDULE_NAME,
            start_at=case.start_at,
            end_at=case.end_at,
        )
    handle.backfill.assert_not_awaited()


async def test_sparse_calendar_backfill_uses_a_daily_action_bound():
    declaration = ScheduleTriggerDeclaration(
        workflow=WORKFLOW_NAME,
        spec=CalendarScheduleSpec(),
        backfill=BackfillPolicy(
            enabled=True,
            max_window_seconds=86_400,
            max_actions=2,
        ),
    )
    service, _, handle = operator(declaration)

    await service.backfill(
        SCHEDULE_NAME,
        start_at=NOW,
        end_at=NOW + timedelta(seconds=86_400),
    )

    handle.backfill.assert_awaited_once()


async def test_dense_calendar_backfill_rejects_an_excessive_action_bound():
    declaration = ScheduleTriggerDeclaration(
        workflow=WORKFLOW_NAME,
        spec=CalendarScheduleSpec(
            second=(CalendarRange(start=0, end=59),),
            minute=(CalendarRange(start=0, end=59),),
            hour=(CalendarRange(start=0, end=23),),
        ),
        backfill=BackfillPolicy(
            enabled=True,
            max_window_seconds=86_400,
            max_actions=10_000,
        ),
    )
    service, _, handle = operator(declaration)

    with pytest.raises(ScheduleOperationError, match="action bound"):
        await service.backfill(
            SCHEDULE_NAME,
            start_at=NOW,
            end_at=NOW + timedelta(seconds=86_400),
        )

    handle.backfill.assert_not_awaited()
