"""Temporal schedule reconciliation boundary tests."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from temporalio.client import ScheduleActionStartWorkflow, ScheduleAlreadyRunningError

from justflow.config.models import FlowStep, WorkflowConfig
from justflow.config.runtime_limits import DEFAULT_RUNTIME_LIMITS
from justflow.config.schedules import IntervalScheduleSpec
from justflow.config.settings import ScheduleSettings
from justflow.config.triggers import ScheduleTriggerDeclaration
from justflow.definitions.manifest import (
    ENGINE_WORKFLOW_ABI,
    SHA256_HEX_LENGTH,
    build_definition_manifests,
)
from justflow.definitions.routing import WorkerDeployment, WorkflowStartTarget
from justflow.provenance import WorkerArtifactIdentity
from justflow.runtime.metrics import MetricsRegistry
from justflow.runtime.schedule_reconciler import (
    ScheduleApplyErrorCode,
    ScheduleApplyStatus,
    ScheduleReconciler,
    ScheduleReconciliationError,
    ScheduleReconciliationErrorCode,
    observed_schedule_from_description,
)
from justflow.runtime.scheduled_starts import (
    SCHEDULED_START_MAINTENANCE_ID_PREFIX,
    SCHEDULED_START_SCHEDULE_ID_PREFIX,
)
from justflow.runtime.schedules import (
    MEMO_SCHEDULE_DESIRED_DIGEST,
    MEMO_SCHEDULE_NAME,
    MEMO_SCHEDULE_OWNER,
    SCHEDULE_OWNER,
    DesiredSchedule,
    ScheduleChangeKind,
    compile_schedule,
    plan_schedule_reconciliation,
)

WORKFLOW_NAME = "record_flow"
TASK_QUEUE = "test-queue"
ENVIRONMENT_SNAPSHOT_DIGEST = "e" * SHA256_HEX_LENGTH
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
DEPLOYMENT = WorkerDeployment(
    artifact_identity=WorkerArtifactIdentity(
        deployment_name="justflow",
        build_id="test-build",
        artifact_digest=f"sha256:{'a' * SHA256_HEX_LENGTH}",
        package_version="0.1.0",
    ),
    compatible_engine_workflow_abis=frozenset({ENGINE_WORKFLOW_ABI}),
)
TARGET = WorkflowStartTarget(
    manifest=MANIFEST,
    deployment=DEPLOYMENT,
    environment_snapshot_digest=ENVIRONMENT_SNAPSHOT_DIGEST,
)


class AsyncEntries:
    def __init__(self, values: list[object]) -> None:
        self._iterator = iter(values)

    def __aiter__(self) -> AsyncEntries:
        return self

    async def __anext__(self) -> object:
        try:
            return next(self._iterator)
        except StopIteration:
            raise StopAsyncIteration from None


def desired_schedule(name: str, *, every_seconds: int = 60) -> DesiredSchedule:
    return compile_schedule(
        name,
        ScheduleTriggerDeclaration(
            workflow=WORKFLOW_NAME,
            spec=IntervalScheduleSpec(every_seconds=every_seconds),
        ),
        TARGET,
        task_queue=TASK_QUEUE,
    )


def described_schedule(desired: DesiredSchedule) -> SimpleNamespace:
    async def memo_value(key: str, default: object, *, type_hint: type) -> object:
        del type_hint
        return desired.memo.get(key, default)

    return SimpleNamespace(
        id=desired.schedule_id,
        schedule=desired.schedule,
        memo_value=memo_value,
        data_converter=MagicMock(),
    )


def listed_schedule(
    schedule_id: str,
    *,
    owner: str | None,
    schedule_name: str | None,
) -> SimpleNamespace:
    values = {
        MEMO_SCHEDULE_OWNER: owner,
        MEMO_SCHEDULE_NAME: schedule_name,
    }

    async def memo_value(key: str, default: object, *, type_hint: type) -> object:
        del type_hint
        return values.get(key, default)

    return SimpleNamespace(id=schedule_id, memo_value=memo_value)


def reconciler(client: MagicMock, **settings: object) -> ScheduleReconciler:
    return ScheduleReconciler(client, ScheduleSettings.model_validate(settings))


async def test_observe_bounds_collection_and_describes_only_managed_schedules():
    desired = desired_schedule("managed_schedule")
    managed = listed_schedule(
        desired.schedule_id,
        owner=SCHEDULE_OWNER,
        schedule_name=desired.schedule_name,
    )
    unmanaged = listed_schedule(
        "host.schedule",
        owner="host",
        schedule_name="host_schedule",
    )
    client = MagicMock()
    client.list_schedules = AsyncMock(return_value=AsyncEntries([managed, unmanaged]))
    handle = MagicMock()
    handle.describe = AsyncMock(return_value=described_schedule(desired))
    client.get_schedule_handle.return_value = handle

    observed = await reconciler(client, max_schedules=2).observe()

    assert observed[desired.schedule_id].desired_digest == desired.desired_digest
    assert observed[desired.schedule_id].valid_managed_identity
    assert not observed["host.schedule"].managed
    client.get_schedule_handle.assert_called_once_with(desired.schedule_id)


async def test_observe_excludes_scheduled_start_and_maintenance_namespaces() -> None:
    excluded: list[object] = [
        listed_schedule(
            f"{SCHEDULED_START_SCHEDULE_ID_PREFIX}opaque",
            owner=None,
            schedule_name=None,
        ),
        listed_schedule(
            f"{SCHEDULED_START_MAINTENANCE_ID_PREFIX}opaque",
            owner=None,
            schedule_name=None,
        ),
    ]
    client = MagicMock()
    client.list_schedules = AsyncMock(return_value=AsyncEntries(excluded))

    observed = await reconciler(client, max_schedules=1).observe()

    assert observed == {}
    client.get_schedule_handle.assert_not_called()


@pytest.mark.parametrize(
    "corrupt_value",
    [
        pytest.param(RuntimeError("private codec detail"), id="decode-error"),
        pytest.param(42, id="wrong-type"),
    ],
)
async def test_observe_surfaces_corrupt_entity_metadata_as_a_conflict(
    corrupt_value: object,
) -> None:
    desired = desired_schedule("corrupt_schedule")

    async def memo_value(key: str, default: object, *, type_hint: type) -> object:
        del type_hint
        if key == MEMO_SCHEDULE_OWNER:
            if isinstance(corrupt_value, BaseException):
                raise corrupt_value
            return corrupt_value
        return desired.memo.get(key, default)

    entry = SimpleNamespace(id=desired.schedule_id, memo_value=memo_value)
    client = MagicMock()
    client.list_schedules = AsyncMock(return_value=AsyncEntries([entry]))
    metrics = MetricsRegistry()
    observer = ScheduleReconciler(client, ScheduleSettings(), metrics=metrics)

    observed = await observer.observe()
    plan = plan_schedule_reconciliation({desired.schedule_id: desired}, observed)

    actual = observed[desired.schedule_id]
    assert actual.corrupt_metadata_keys == (MEMO_SCHEDULE_OWNER,)
    assert plan.changes[0].kind is ScheduleChangeKind.CONFLICT
    assert plan.changes[0].reason == "schedule metadata is corrupt"
    assert (
        'justflow_schedule_operations_total{operation="observe",outcome="corrupt"} 1'
        in metrics.render_prometheus().decode("utf-8")
    )
    client.get_schedule_handle.assert_not_called()


async def test_description_marks_corrupt_action_metadata() -> None:
    desired = desired_schedule("corrupt_action")
    action = desired.schedule.action
    assert isinstance(action, ScheduleActionStartWorkflow)
    corrupt_action = replace(
        action,
        memo={**dict(action.memo or {}), MEMO_SCHEDULE_DESIRED_DIGEST: 42},
    )
    description = described_schedule(desired)
    description.schedule = replace(desired.schedule, action=corrupt_action)

    observed = await observed_schedule_from_description(description)

    assert observed.corrupt_metadata_keys == (MEMO_SCHEDULE_DESIRED_DIGEST,)
    assert observed.valid_managed_identity is False


async def test_observe_rejects_collection_above_configured_bound():
    client = MagicMock()
    client.list_schedules = AsyncMock(
        return_value=AsyncEntries(
            [
                listed_schedule("one", owner=None, schedule_name=None),
                listed_schedule("two", owner=None, schedule_name=None),
            ]
        )
    )

    with pytest.raises(ScheduleReconciliationError) as raised:
        await reconciler(client, max_schedules=1).observe()

    assert raised.value.code is ScheduleReconciliationErrorCode.COLLECTION_LIMIT


async def test_apply_requires_exact_plan_confirmation_before_mutating():
    desired = desired_schedule("daily_orders")
    plan = plan_schedule_reconciliation({desired.schedule_id: desired}, {})
    client = MagicMock()
    client.create_schedule = AsyncMock()

    with pytest.raises(ScheduleReconciliationError) as raised:
        await reconciler(client).apply(plan, confirmation="wrong-plan")

    assert raised.value.code is ScheduleReconciliationErrorCode.CONFIRMATION_REQUIRED
    client.create_schedule.assert_not_awaited()


async def test_apply_is_idempotent_when_a_concurrent_create_matches_desired_state():
    desired = desired_schedule("daily_orders")
    plan = plan_schedule_reconciliation({desired.schedule_id: desired}, {})
    client = MagicMock()
    client.create_schedule = AsyncMock(side_effect=ScheduleAlreadyRunningError())
    handle = MagicMock()
    handle.describe = AsyncMock(return_value=described_schedule(desired))
    client.get_schedule_handle.return_value = handle

    result = await reconciler(client).apply(plan, confirmation=plan.plan_digest)

    assert result.successful
    assert result.items[0].status is ScheduleApplyStatus.ALREADY_APPLIED


async def test_apply_reports_precise_partial_failures_without_exposing_causes():
    first = desired_schedule("first_schedule")
    second = desired_schedule("second_schedule")
    desired = {item.schedule_id: item for item in (first, second)}
    plan = plan_schedule_reconciliation(desired, {})
    client = MagicMock()

    async def create(schedule_id: str, *_: object, **__: object) -> None:
        if schedule_id == second.schedule_id:
            raise RuntimeError("synthetic-temporal-credential")

    client.create_schedule = AsyncMock(side_effect=create)

    result = await reconciler(client).apply(plan, confirmation=plan.plan_digest)

    assert [item.status for item in result.items] == [
        ScheduleApplyStatus.APPLIED,
        ScheduleApplyStatus.FAILED,
    ]
    assert result.items[1].error_code is ScheduleApplyErrorCode.TEMPORAL_UNAVAILABLE
    assert "credential" not in repr(result)


async def test_update_rejects_a_stale_observed_state_instead_of_overwriting_it():
    desired = desired_schedule("daily_orders", every_seconds=60)
    expected = desired_schedule("daily_orders", every_seconds=120)
    concurrent = desired_schedule("daily_orders", every_seconds=180)
    observed = await observed_schedule_from_description(described_schedule(expected))
    plan = plan_schedule_reconciliation(
        {desired.schedule_id: desired},
        {observed.schedule_id: observed},
    )
    client = MagicMock()
    handle = MagicMock()

    async def update(updater, **_: object) -> None:
        await updater(SimpleNamespace(description=described_schedule(concurrent)))

    handle.update = AsyncMock(side_effect=update)
    client.get_schedule_handle.return_value = handle

    result = await reconciler(client).apply(plan, confirmation=plan.plan_digest)

    assert result.items[0].status is ScheduleApplyStatus.FAILED
    assert result.items[0].error_code is ScheduleApplyErrorCode.OWNERSHIP_CHANGED
