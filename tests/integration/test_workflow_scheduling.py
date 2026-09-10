"""Temporal-backed schedule reconciliation and occurrence integration tests."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pytest
from temporalio import workflow
from temporalio.client import WorkflowExecutionStatus
from temporalio.worker import Worker

from justflow.config.models import FlowStep, WorkflowConfig
from justflow.config.runtime_limits import DEFAULT_RUNTIME_LIMITS
from justflow.config.schedules import (
    BackfillPolicy,
    CalendarRange,
    CalendarScheduleSpec,
    IntervalScheduleSpec,
    ScheduleOverlapPolicy,
)
from justflow.config.settings import ScheduleSettings
from justflow.config.triggers import ScheduleTriggerDeclaration, TriggersConfig
from justflow.definitions.catalog import CatalogStore
from justflow.definitions.environment import (
    build_execution_environment_snapshot,
    sanitized_runtime_configuration,
)
from justflow.definitions.manifest import ENGINE_WORKFLOW_ABI, build_definition_manifests
from justflow.definitions.routing import (
    WorkerDeployment,
    WorkflowStartTarget,
    wait_for_worker_deployment,
    worker_deployment_config,
)
from justflow.engine.local_temporal import start_local_environment
from justflow.engine.sandbox import workflow_sandbox_runner
from justflow.provenance import RuntimeProfile, WorkerArtifactIdentity
from justflow.runtime.schedule_dispatch import (
    ScheduleDispatchActivity,
    ScheduleDispatchWorkflow,
)
from justflow.runtime.schedule_operations import ScheduleOperator, TriggerRunNowStatus
from justflow.runtime.schedule_reconciler import ScheduleApplyStatus, ScheduleReconciler
from justflow.runtime.schedules import (
    compile_schedule,
    make_schedule_occurrence_id,
    make_schedule_run_now_workflow_id,
)
from justflow.runtime.starter import StartStatus, WorkflowStarter
from justflow.scope import LOCAL_RUNTIME_SCOPE
from justflow.sdk.message_contract import make_workflow_id

BUSINESS_TASK_QUEUE = "test-scheduled-business"
SCHEDULE_TASK_QUEUE = "test-schedule-dispatch"
SCHEDULE_NAME = "hourly_orders"
WORKFLOW_NAME = "scheduled_business"
POLL_ATTEMPTS = 100
POLL_INTERVAL_SECONDS = 0.05
RESULT_TIMEOUT_SECONDS = 10
RUN_NOW_IDENTITY_DIGEST = "f" * 64
ARTIFACT = WorkerArtifactIdentity(
    deployment_name="justflow-schedule-test",
    build_id="integration-1",
    artifact_digest=f"sha256:{'a' * 64}",
    package_version="0.1.0",
)
DEPLOYMENT = WorkerDeployment(
    artifact_identity=ARTIFACT,
    compatible_engine_workflow_abis=frozenset({ENGINE_WORKFLOW_ABI}),
)
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
SCHEDULE_TARGET = WorkflowStartTarget(
    manifest=MANIFEST,
    deployment=DEPLOYMENT,
    environment_snapshot_digest="e" * 64,
)


@dataclass(frozen=True, kw_only=True)
class TimezoneBoundaryCase:
    id: str
    timezone: str
    spec: CalendarScheduleSpec
    expected_next_runs: tuple[datetime, ...]


TIMEZONE_BOUNDARY_CASES = [
    TimezoneBoundaryCase(
        id="international-date-line",
        timezone="Pacific/Kiritimati",
        spec=CalendarScheduleSpec(
            minute=(CalendarRange(start=30),),
            hour=(CalendarRange(start=0),),
            day_of_month=(CalendarRange(start=1),),
            month=(CalendarRange(start=1),),
            year=(CalendarRange(start=2100),),
        ),
        expected_next_runs=(datetime(2099, 12, 31, 10, 30, tzinfo=UTC),),
    ),
    TimezoneBoundaryCase(
        id="dst-missing-wall-time",
        timezone="Europe/Zurich",
        spec=CalendarScheduleSpec(
            minute=(CalendarRange(start=30),),
            hour=(CalendarRange(start=2),),
            day_of_month=(CalendarRange(start=28),),
            month=(CalendarRange(start=3),),
            year=(CalendarRange(start=2100),),
        ),
        expected_next_runs=(),
    ),
    TimezoneBoundaryCase(
        id="dst-repeated-wall-time",
        timezone="Europe/Zurich",
        spec=CalendarScheduleSpec(
            minute=(CalendarRange(start=30),),
            hour=(CalendarRange(start=2),),
            day_of_month=(CalendarRange(start=31),),
            month=(CalendarRange(start=10),),
            year=(CalendarRange(start=2100),),
        ),
        expected_next_runs=(datetime(2100, 10, 31, 1, 30, tzinfo=UTC),),
    ),
]


@workflow.defn(name=MANIFEST.logical_name + "__" + MANIFEST.definition_digest, sandboxed=False)
class ScheduledBusinessWorkflow:
    @workflow.run
    async def run(self, trigger: dict[str, Any]) -> str:
        return str(trigger["request_id"])


@pytest.mark.parametrize("case", TIMEZONE_BOUNDARY_CASES, ids=lambda case: case.id)
async def test_temporal_timezone_and_dst_boundaries(case: TimezoneBoundaryCase):
    desired = compile_schedule(
        f"timezone_{case.id.replace('-', '_')}",
        ScheduleTriggerDeclaration(
            workflow=WORKFLOW_NAME,
            spec=case.spec,
            timezone=case.timezone,
            paused=True,
        ),
        SCHEDULE_TARGET,
        task_queue=BUSINESS_TASK_QUEUE,
        dispatch_task_queue=SCHEDULE_TASK_QUEUE,
    )

    async with await start_local_environment(identity="schedule-timezone-client") as environment:
        handle = await environment.client.create_schedule(
            desired.schedule_id,
            desired.schedule,
            memo=desired.memo,
        )
        description = await handle.describe()

    assert tuple(description.info.next_action_times) == case.expected_next_runs


async def test_reconcile_dispatch_retry_and_operator_controls(tmp_path):
    catalog_store = CatalogStore(tmp_path)
    catalog = catalog_store.publish({WORKFLOW_NAME: MANIFEST})
    snapshot = build_execution_environment_snapshot(
        manifest=MANIFEST,
        artifact_identity=ARTIFACT,
        catalog_backend=catalog_store.backend_identity,
        runtime_profile=RuntimeProfile.PRODUCTION,
        configuration=sanitized_runtime_configuration(
            temporal_namespace="default",
            temporal_task_queue=BUSINESS_TASK_QUEUE,
            payload_protection_mode="plaintext",
            broker_providers={},
            runtime_limits=MANIFEST.deterministic_policy.runtime_limits,
        ),
    )
    catalog_store.store_environment_snapshot(snapshot)
    target = WorkflowStartTarget(
        manifest=MANIFEST,
        deployment=DEPLOYMENT,
        environment_snapshot_digest=snapshot.snapshot_digest,
    )
    declaration = ScheduleTriggerDeclaration(
        workflow=WORKFLOW_NAME,
        spec=IntervalScheduleSpec(every_seconds=3_600),
        overlap_policy=ScheduleOverlapPolicy.BUFFER_ONE,
        catch_up_window_seconds=60,
        paused=True,
        backfill=BackfillPolicy(enabled=True, max_window_seconds=300, max_actions=100),
    )
    desired = compile_schedule(
        SCHEDULE_NAME,
        declaration,
        target,
        task_queue=BUSINESS_TASK_QUEUE,
        dispatch_task_queue=SCHEDULE_TASK_QUEUE,
    )
    settings = ScheduleSettings(task_queue=SCHEDULE_TASK_QUEUE)

    async with await start_local_environment(identity="schedule-integration-client") as environment:
        starter = WorkflowStarter(
            environment.client,
            BUSINESS_TASK_QUEUE,
            {},
            triggers=TriggersConfig(triggers={}),
        )
        dispatch_activity = ScheduleDispatchActivity(
            catalog=catalog,
            catalog_store=catalog_store,
            starter=starter,
        )
        async with (
            Worker(
                environment.client,
                task_queue=BUSINESS_TASK_QUEUE,
                workflows=[ScheduledBusinessWorkflow],
                deployment_config=worker_deployment_config(DEPLOYMENT),
                identity="schedule-integration-business-worker",
            ),
            Worker(
                environment.client,
                task_queue=SCHEDULE_TASK_QUEUE,
                workflows=[ScheduleDispatchWorkflow],
                activities=[dispatch_activity.start_scheduled_workflow],
                workflow_runner=workflow_sandbox_runner(),
                identity="schedule-integration-dispatch-worker",
            ),
        ):
            await wait_for_worker_deployment(
                environment.client,
                DEPLOYMENT,
                BUSINESS_TASK_QUEUE,
            )
            reconciler = ScheduleReconciler(environment.client, settings)
            plan = await reconciler.plan({desired.schedule_id: desired})

            first_apply = await reconciler.apply(plan, confirmation=plan.plan_digest)
            duplicate_apply = await reconciler.apply(plan, confirmation=plan.plan_digest)
            converged = await reconciler.plan({desired.schedule_id: desired})

            assert first_apply.items[0].status is ScheduleApplyStatus.APPLIED
            assert duplicate_apply.items[0].status is ScheduleApplyStatus.ALREADY_APPLIED
            assert converged.changes == ()

            updated_declaration = declaration.model_copy(update={"catch_up_window_seconds": 120})
            updated_desired = compile_schedule(
                SCHEDULE_NAME,
                updated_declaration,
                target,
                task_queue=BUSINESS_TASK_QUEUE,
                dispatch_task_queue=SCHEDULE_TASK_QUEUE,
            )
            update_plan = await reconciler.plan({updated_desired.schedule_id: updated_desired})
            update_result = await reconciler.apply(
                update_plan,
                confirmation=update_plan.plan_digest,
            )

            assert update_result.items[0].status is ScheduleApplyStatus.APPLIED

            schedule_handle = environment.client.get_schedule_handle(desired.schedule_id)
            await schedule_handle.trigger()
            dispatch_workflow_id = await _recent_dispatch_workflow_id(schedule_handle)
            occurrence_id = make_schedule_occurrence_id(
                SCHEDULE_NAME,
                dispatch_workflow_id,
                scope_digest=LOCAL_RUNTIME_SCOPE.digest,
            )
            await asyncio.wait_for(
                environment.client.get_workflow_handle(dispatch_workflow_id).result(),
                timeout=RESULT_TIMEOUT_SECONDS,
            )
            business_workflow_id = make_workflow_id(
                WORKFLOW_NAME,
                occurrence_id,
                scope=LOCAL_RUNTIME_SCOPE,
            )
            result = await asyncio.wait_for(
                environment.client.get_workflow_handle(business_workflow_id).result(),
                timeout=RESULT_TIMEOUT_SECONDS,
            )

            assert result == occurrence_id
            retry_result = await dispatch_activity.start_scheduled_workflow(
                {
                    "plan": desired.schedule.action.args[0],
                    "occurrence_id": occurrence_id,
                }
            )
            assert retry_result["status"] == StartStatus.DUPLICATE.value

            operator = ScheduleOperator(
                environment.client,
                TriggersConfig(triggers={SCHEDULE_NAME: declaration}),
                settings,
            )
            paused = await operator.describe(SCHEDULE_NAME)
            resumed = await operator.resume(SCHEDULE_NAME)
            active_operator = ScheduleOperator(
                environment.client,
                TriggersConfig(
                    triggers={SCHEDULE_NAME: declaration.model_copy(update={"paused": False})}
                ),
                settings,
            )
            run_now = await active_operator.trigger_now(
                SCHEDULE_NAME,
                request_identity_digest=RUN_NOW_IDENTITY_DIGEST,
            )
            duplicate_run_now = await active_operator.trigger_now(
                SCHEDULE_NAME,
                request_identity_digest=RUN_NOW_IDENTITY_DIGEST,
            )
            run_now_dispatch_workflow_id = make_schedule_run_now_workflow_id(
                SCHEDULE_NAME,
                RUN_NOW_IDENTITY_DIGEST,
            )
            await asyncio.wait_for(
                environment.client.get_workflow_handle(run_now_dispatch_workflow_id).result(),
                timeout=RESULT_TIMEOUT_SECONDS,
            )
            run_now_occurrence_id = make_schedule_occurrence_id(
                SCHEDULE_NAME,
                run_now_dispatch_workflow_id,
                scope_digest=LOCAL_RUNTIME_SCOPE.digest,
            )
            run_now_business_workflow_id = make_workflow_id(
                WORKFLOW_NAME,
                run_now_occurrence_id,
                scope=LOCAL_RUNTIME_SCOPE,
            )
            run_now_result = await asyncio.wait_for(
                environment.client.get_workflow_handle(run_now_business_workflow_id).result(),
                timeout=RESULT_TIMEOUT_SECONDS,
            )
            paused_again = await operator.pause(SCHEDULE_NAME)
            temporal_description = await schedule_handle.describe()

            assert paused.paused is True
            assert resumed.paused is False
            assert run_now is TriggerRunNowStatus.ACCEPTED
            assert duplicate_run_now is TriggerRunNowStatus.ALREADY_ACCEPTED
            assert run_now_result == run_now_occurrence_id
            assert paused_again.paused is True
            assert paused.overlap_policy is ScheduleOverlapPolicy.BUFFER_ONE
            assert paused.definition_digest == MANIFEST.definition_digest
            assert paused.recent_actions[-1].workflow_id == dispatch_workflow_id
            assert temporal_description.schedule.policy.catchup_window.total_seconds() == 120
            await operator.backfill(
                SCHEDULE_NAME,
                start_at=datetime(2025, 12, 31, 23, 59, 59, tzinfo=UTC),
                end_at=datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC),
            )
            await operator.delete(SCHEDULE_NAME, confirmation=paused_again.desired_digest)


async def test_scheduled_occurrence_waits_durably_while_dispatch_worker_is_offline(tmp_path):
    schedule_name = "worker_downtime"
    catalog_store = CatalogStore(tmp_path)
    catalog = catalog_store.publish({WORKFLOW_NAME: MANIFEST})
    snapshot = build_execution_environment_snapshot(
        manifest=MANIFEST,
        artifact_identity=ARTIFACT,
        catalog_backend=catalog_store.backend_identity,
        runtime_profile=RuntimeProfile.PRODUCTION,
        configuration=sanitized_runtime_configuration(
            temporal_namespace="default",
            temporal_task_queue=BUSINESS_TASK_QUEUE,
            payload_protection_mode="plaintext",
            broker_providers={},
            runtime_limits=MANIFEST.deterministic_policy.runtime_limits,
        ),
    )
    catalog_store.store_environment_snapshot(snapshot)
    target = WorkflowStartTarget(
        manifest=MANIFEST,
        deployment=DEPLOYMENT,
        environment_snapshot_digest=snapshot.snapshot_digest,
    )
    desired = compile_schedule(
        schedule_name,
        ScheduleTriggerDeclaration(
            workflow=WORKFLOW_NAME,
            spec=IntervalScheduleSpec(every_seconds=3_600),
            paused=True,
        ),
        target,
        task_queue=BUSINESS_TASK_QUEUE,
        dispatch_task_queue=SCHEDULE_TASK_QUEUE,
    )

    async with await start_local_environment(identity="schedule-downtime-client") as environment:
        starter = WorkflowStarter(
            environment.client,
            BUSINESS_TASK_QUEUE,
            {},
            triggers=TriggersConfig(triggers={}),
        )
        dispatch_activity = ScheduleDispatchActivity(
            catalog=catalog,
            catalog_store=catalog_store,
            starter=starter,
        )
        async with Worker(
            environment.client,
            task_queue=BUSINESS_TASK_QUEUE,
            workflows=[ScheduledBusinessWorkflow],
            deployment_config=worker_deployment_config(DEPLOYMENT),
            identity="schedule-downtime-business-worker",
        ):
            await wait_for_worker_deployment(
                environment.client,
                DEPLOYMENT,
                BUSINESS_TASK_QUEUE,
            )
            reconciler = ScheduleReconciler(
                environment.client,
                ScheduleSettings(task_queue=SCHEDULE_TASK_QUEUE),
            )
            plan = await reconciler.plan({desired.schedule_id: desired})
            await reconciler.apply(plan, confirmation=plan.plan_digest)
            schedule_handle = environment.client.get_schedule_handle(desired.schedule_id)

            await schedule_handle.trigger()
            dispatch_workflow_id = await _recent_dispatch_workflow_id(schedule_handle)
            pending = await environment.client.get_workflow_handle(dispatch_workflow_id).describe()

            assert pending.status is WorkflowExecutionStatus.RUNNING

            async with Worker(
                environment.client,
                task_queue=SCHEDULE_TASK_QUEUE,
                workflows=[ScheduleDispatchWorkflow],
                activities=[dispatch_activity.start_scheduled_workflow],
                workflow_runner=workflow_sandbox_runner(),
                identity="schedule-downtime-dispatch-worker",
            ):
                await asyncio.wait_for(
                    environment.client.get_workflow_handle(dispatch_workflow_id).result(),
                    timeout=RESULT_TIMEOUT_SECONDS,
                )
                occurrence_id = make_schedule_occurrence_id(
                    schedule_name,
                    dispatch_workflow_id,
                    scope_digest=LOCAL_RUNTIME_SCOPE.digest,
                )
                business_workflow_id = make_workflow_id(
                    WORKFLOW_NAME,
                    occurrence_id,
                    scope=LOCAL_RUNTIME_SCOPE,
                )
                result = await asyncio.wait_for(
                    environment.client.get_workflow_handle(business_workflow_id).result(),
                    timeout=RESULT_TIMEOUT_SECONDS,
                )

            assert result == occurrence_id
            await schedule_handle.delete()


async def _recent_dispatch_workflow_id(schedule_handle: Any) -> str:
    for _ in range(POLL_ATTEMPTS):
        description = await schedule_handle.describe()
        if description.info.recent_actions:
            return description.info.recent_actions[-1].action.workflow_id
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
    raise AssertionError("Scheduled dispatch did not start within the polling bound")
