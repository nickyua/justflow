"""Regenerate committed Temporal histories for compatibility tests."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from functools import partial
from pathlib import Path
from typing import Any

from temporalio import activity
from temporalio.worker import Worker

from justflow.config.settings import ScheduledStartWorkloadClass
from justflow.definitions.replay import REPLAY_INDEX_FORMAT_VERSION
from justflow.definitions.routing import (
    WorkflowStartTarget,
    retry_pinned_workflow_start,
    wait_for_worker_deployment,
    worker_deployment_config,
)
from justflow.engine.local_temporal import start_local_environment
from justflow.engine.sandbox import workflow_sandbox_runner
from justflow.runtime.schedule_dispatch import (
    SCHEDULE_DISPATCH_ACTIVITY_TYPE,
    ScheduleDispatchWorkflow,
)
from justflow.runtime.scheduled_start_dispatch import (
    ScheduledStartArbiterWorkflow,
    ScheduledStartDueWorkflow,
)
from justflow.runtime.scheduled_starts import (
    SCHEDULED_START_ARBITER_WORKFLOW_TYPE,
    SCHEDULED_START_ATTEMPT_ACTIVITY_TYPE,
    SCHEDULED_START_CLAIM_ACTIVITY_TYPE,
    SCHEDULED_START_COMMIT_ACTIVITY_TYPE,
    SCHEDULED_START_DUE_WORKFLOW_TYPE,
    SCHEDULED_START_PREPARE_ACTIVITY_TYPE,
    ScheduledStartArbiterApplied,
    ScheduledStartArbiterInitialInput,
    ScheduledStartArbiterPreparedStale,
    ScheduledStartArbiterResolvedDueInput,
    ScheduledStartArbiterTerminalInput,
    ScheduledStartAttemptAccepted,
    ScheduledStartAttemptOutcome,
    ScheduledStartAttemptRequest,
    ScheduledStartCreateRequest,
    ScheduledStartDueClaimed,
    ScheduledStartDueCommand,
    ScheduledStartDueInput,
    ScheduledStartResolvedTarget,
    ScheduledStartRun,
    claim_scheduled_start,
    complete_scheduled_start,
    create_scheduled_start_record,
    describe_scheduled_start,
    make_scheduled_start_id,
)
from justflow.runtime.schedules import (
    SCHEDULE_DISPATCH_WORKFLOW_TYPE,
    ScheduleDispatchPlan,
    ScheduleTargetIdentity,
)
from justflow.scope import LOCAL_RUNTIME_SCOPE
from justflow.sdk.message_contract import WORKFLOW_EVENT_SIGNAL
from tests.replay_cases import (
    REPLAY_DEPLOYMENT,
    ReplayActivities,
    prepare_replay_definitions,
)

REPOSITORY_ROOT = Path(__file__).parent.parent
REPLAY_ROOT = REPOSITORY_ROOT / "tests" / "replay"
HISTORY_ROOT = REPLAY_ROOT / "histories"
INDEX_PATH = REPLAY_ROOT / "index.json"
TASK_QUEUE = "justflow-replay-fixtures"
SCHEDULE_TASK_QUEUE = "justflow-replay-schedule-fixtures"
CLIENT_IDENTITY = "justflow-replay-fixture-client"
WORKER_IDENTITY = "justflow-replay-fixture-worker"
WAIT_WORKFLOW = "replay_durable_wait"
BEHAVIOR_FAMILIES = {
    "replay_terminal": "terminal",
    "replay_activity": "activity",
    "replay_branching": "branching",
    "replay_parallel_fanout": "parallel-fanout",
    "replay_until_loop": "until-loop",
    "replay_durable_wait": "durable-wait",
    "replay_durable_sleep": "durable-sleep",
    "replay_failure_handler": "failure-handler",
    "replay_child_workflow": "child-workflow",
}
SCHEDULE_DISPATCH_BEHAVIOR_FAMILY = "schedule-dispatch"
SCHEDULE_DISPATCH_TARGET = "replay_terminal"
SCHEDULED_START_DUE_BEHAVIOR_FAMILY = "scheduled-start-due"
SCHEDULED_START_ARBITER_INITIAL_BEHAVIOR_FAMILY = "scheduled-start-arbiter-initial"
SCHEDULED_START_ARBITER_RESOLVED_BEHAVIOR_FAMILY = "scheduled-start-arbiter-resolved"
SCHEDULED_START_ARBITER_TERMINAL_BEHAVIOR_FAMILY = "scheduled-start-arbiter-terminal"
SCHEDULED_START_AT = datetime(2026, 8, 11, 8, 0, tzinfo=UTC)


class ReplayScheduleDispatchActivity:
    @activity.defn(name=SCHEDULE_DISPATCH_ACTIVITY_TYPE)
    async def start_scheduled_workflow(self, request: dict[str, Any]) -> dict[str, Any]:
        return {
            "occurrence_id": request["occurrence_id"],
            "status": "started",
        }


class ReplayScheduledStartActivities:
    def __init__(self, target: WorkflowStartTarget) -> None:
        self._target = target

    @activity.defn(name=SCHEDULED_START_CLAIM_ACTIVITY_TYPE)
    async def claim(self, request: dict[str, Any]) -> dict[str, Any]:
        ScheduledStartDueInput.model_validate(request)
        return ScheduledStartDueClaimed().model_dump(mode="json")

    @activity.defn(name=SCHEDULED_START_PREPARE_ACTIVITY_TYPE)
    async def prepare(self, request: dict[str, Any]) -> dict[str, Any]:
        initial = ScheduledStartArbiterInitialInput.model_validate(request)
        return ScheduledStartArbiterPreparedStale(
            current=describe_scheduled_start(initial.record)
        ).model_dump(mode="json")

    @activity.defn(name=SCHEDULED_START_ATTEMPT_ACTIVITY_TYPE)
    async def attempt(self, request: dict[str, Any]) -> dict[str, Any]:
        attempt = ScheduledStartAttemptRequest.model_validate(request)
        return ScheduledStartAttemptAccepted(
            outcome=ScheduledStartAttemptOutcome.ACCEPTED,
            run=ScheduledStartRun(
                workflow_id="replay-scheduled-tenant-run",
                run_id="replay-scheduled-tenant-run-id",
                definition_digest=attempt.target.manifest.definition_digest,
                artifact_identity=attempt.target.artifact_identity,
                environment_snapshot_digest=(attempt.target.environment_snapshot_digest),
                execution_configuration=attempt.target.execution_configuration,
            ),
        ).model_dump(mode="json")

    @activity.defn(name=SCHEDULED_START_COMMIT_ACTIVITY_TYPE)
    async def commit(self, request: dict[str, Any]) -> dict[str, Any]:
        terminal = ScheduledStartArbiterTerminalInput.model_validate(request)
        return ScheduledStartArbiterApplied(
            consumed_version=terminal.consumed_version,
            winner=terminal.winner,
            request_digest=terminal.request_digest,
            scheduled_start=describe_scheduled_start(terminal.projection),
        ).model_dump(mode="json")


async def generate() -> None:
    prepared = prepare_replay_definitions()
    activities = ReplayActivities()
    schedule_dispatch = ReplayScheduleDispatchActivity()
    scheduled_start_activities = ReplayScheduledStartActivities(
        prepared.start_targets[SCHEDULE_DISPATCH_TARGET]
    )
    fixtures: list[dict[str, Any]] = []
    HISTORY_ROOT.mkdir(parents=True, exist_ok=True)
    async with (
        await start_local_environment(identity=CLIENT_IDENTITY) as environment,
        Worker(
            environment.client,
            task_queue=TASK_QUEUE,
            workflows=list(prepared.workflow_classes.values()),
            activities=[activities.execute_step],
            workflow_runner=workflow_sandbox_runner(),
            deployment_config=worker_deployment_config(REPLAY_DEPLOYMENT),
            identity=WORKER_IDENTITY,
        ),
        Worker(
            environment.client,
            task_queue=SCHEDULE_TASK_QUEUE,
            workflows=[
                ScheduleDispatchWorkflow,
                ScheduledStartArbiterWorkflow,
                ScheduledStartDueWorkflow,
            ],
            activities=[
                schedule_dispatch.start_scheduled_workflow,
                scheduled_start_activities.claim,
                scheduled_start_activities.prepare,
                scheduled_start_activities.attempt,
                scheduled_start_activities.commit,
            ],
            workflow_runner=workflow_sandbox_runner(),
            identity=f"{WORKER_IDENTITY}-schedule",
        ),
    ):
        await wait_for_worker_deployment(
            environment.client,
            REPLAY_DEPLOYMENT,
            TASK_QUEUE,
        )
        for logical_name, behavior_family in BEHAVIOR_FAMILIES.items():
            target = prepared.start_targets[logical_name]
            workflow_id = f"replay-fixture-{behavior_family}"
            workflow_type = target.workflow_type
            start = partial(
                environment.client.start_workflow,
                workflow_type,
                {
                    "request_id": workflow_id,
                    "globals": {},
                    "definition_digest": target.manifest.definition_digest,
                    "worker_deployment": REPLAY_DEPLOYMENT.name,
                    "worker_build_id": REPLAY_DEPLOYMENT.build_id,
                    "worker_artifact": REPLAY_DEPLOYMENT.artifact_identity.model_dump(mode="json"),
                    "environment_snapshot_digest": target.environment_snapshot_digest,
                    "scope_digest": LOCAL_RUNTIME_SCOPE.digest,
                },
                id=workflow_id,
                task_queue=TASK_QUEUE,
                memo=target.memo,
                versioning_override=target.versioning_override,
            )
            handle = await retry_pinned_workflow_start(
                start,
                REPLAY_DEPLOYMENT,
                TASK_QUEUE,
            )
            if logical_name == WAIT_WORKFLOW:
                await handle.signal(
                    WORKFLOW_EVENT_SIGNAL,
                    {"signal": "approved", "data": {"approved": True}},
                )
            await handle.result()
            history = await handle.fetch_history()
            relative_path = Path("tests") / "replay" / "histories" / f"{behavior_family}.json"
            sanitized_history = _sanitize_history(json.loads(history.to_json()))
            (REPOSITORY_ROOT / relative_path).write_text(
                json.dumps(sanitized_history, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            fixtures.append(
                {
                    "id": behavior_family,
                    "behavior_family": behavior_family,
                    "workflow_id": workflow_id,
                    "workflow_type": workflow_type,
                    "definition_digest": target.manifest.definition_digest,
                    "worker_deployment": REPLAY_DEPLOYMENT.name,
                    "worker_build_id": REPLAY_DEPLOYMENT.build_id,
                    "history": str(relative_path),
                }
            )
        target = prepared.start_targets[SCHEDULE_DISPATCH_TARGET]
        workflow_id = f"replay-fixture-{SCHEDULE_DISPATCH_BEHAVIOR_FAMILY}"
        handle = await environment.client.start_workflow(
            SCHEDULE_DISPATCH_WORKFLOW_TYPE,
            ScheduleDispatchPlan(
                schedule_name="replay_schedule",
                target=ScheduleTargetIdentity.from_target(target),
                input={},
            ).model_dump(mode="json"),
            id=workflow_id,
            task_queue=SCHEDULE_TASK_QUEUE,
        )
        await handle.result()
        history = await handle.fetch_history()
        relative_path = (
            Path("tests") / "replay" / "histories" / f"{SCHEDULE_DISPATCH_BEHAVIOR_FAMILY}.json"
        )
        sanitized_history = _sanitize_history(json.loads(history.to_json()))
        (REPOSITORY_ROOT / relative_path).write_text(
            json.dumps(sanitized_history, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        fixtures.append(
            {
                "id": SCHEDULE_DISPATCH_BEHAVIOR_FAMILY,
                "behavior_family": SCHEDULE_DISPATCH_BEHAVIOR_FAMILY,
                "workflow_id": workflow_id,
                "workflow_type": SCHEDULE_DISPATCH_WORKFLOW_TYPE,
                "definition_digest": target.manifest.definition_digest,
                "worker_deployment": REPLAY_DEPLOYMENT.name,
                "worker_build_id": REPLAY_DEPLOYMENT.build_id,
                "history": str(relative_path),
            }
        )
        scheduled_request = ScheduledStartCreateRequest(
            workflow_name=SCHEDULE_DISPATCH_TARGET,
            input={},
            business_request_id="replay-scheduled-start",
            start_at=SCHEDULED_START_AT + timedelta(hours=1),
            workload_class=ScheduledStartWorkloadClass.STANDARD,
        )
        scheduled_record = create_scheduled_start_record(
            scheduled_request,
            scheduled_start_id=make_scheduled_start_id(
                LOCAL_RUNTIME_SCOPE,
                scheduled_request.workflow_name,
                scheduled_request.business_request_id,
            ),
            scope=LOCAL_RUNTIME_SCOPE,
            trigger_name="replay_api",
            request_digest="a" * 64,
            accepted_at=SCHEDULED_START_AT,
            normalized_input={},
        )
        claimed_record = claim_scheduled_start(
            scheduled_record,
            expected_version=scheduled_record.version,
            claimed_at=SCHEDULED_START_AT,
        )
        if claimed_record is None:
            raise RuntimeError("Replay scheduled start could not be claimed")
        replay_run = ScheduledStartRun(
            workflow_id="replay-scheduled-tenant-run",
            run_id="replay-scheduled-tenant-run-id",
            definition_digest=target.manifest.definition_digest,
            artifact_identity=target.deployment.artifact_identity,
            environment_snapshot_digest=target.environment_snapshot_digest,
            execution_configuration=target.execution_configuration,
        )
        terminal_record = complete_scheduled_start(
            claimed_record,
            replay_run,
            completed_at=SCHEDULED_START_AT,
        )
        scheduled_start_workflows = [
            (
                SCHEDULED_START_DUE_BEHAVIOR_FAMILY,
                SCHEDULED_START_DUE_WORKFLOW_TYPE,
                ScheduledStartDueInput(record=scheduled_record).model_dump(mode="json"),
            ),
            (
                SCHEDULED_START_ARBITER_INITIAL_BEHAVIOR_FAMILY,
                SCHEDULED_START_ARBITER_WORKFLOW_TYPE,
                ScheduledStartArbiterInitialInput(
                    record=scheduled_record,
                    command=ScheduledStartDueCommand(nominal_time=scheduled_record.start_at),
                ).model_dump(mode="json"),
            ),
            (
                SCHEDULED_START_ARBITER_RESOLVED_BEHAVIOR_FAMILY,
                SCHEDULED_START_ARBITER_WORKFLOW_TYPE,
                ScheduledStartArbiterResolvedDueInput(
                    record=claimed_record,
                    target=ScheduledStartResolvedTarget.from_target(target),
                ).model_dump(mode="json"),
            ),
            (
                SCHEDULED_START_ARBITER_TERMINAL_BEHAVIOR_FAMILY,
                SCHEDULED_START_ARBITER_WORKFLOW_TYPE,
                ScheduledStartArbiterTerminalInput(
                    projection=terminal_record,
                    consumed_version=scheduled_record.version,
                    winner=ScheduledStartDueCommand(nominal_time=scheduled_record.start_at).kind,
                ).model_dump(mode="json"),
            ),
        ]
        for behavior_family, workflow_type, workflow_input in scheduled_start_workflows:
            workflow_id = f"replay-fixture-{behavior_family}"
            handle = await environment.client.start_workflow(
                workflow_type,
                workflow_input,
                id=workflow_id,
                task_queue=SCHEDULE_TASK_QUEUE,
            )
            await handle.result()
            history = await handle.fetch_history()
            relative_path = Path("tests") / "replay" / "histories" / f"{behavior_family}.json"
            sanitized_history = _sanitize_history(json.loads(history.to_json()))
            (REPOSITORY_ROOT / relative_path).write_text(
                json.dumps(sanitized_history, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            fixtures.append(
                {
                    "id": behavior_family,
                    "behavior_family": behavior_family,
                    "workflow_id": workflow_id,
                    "workflow_type": workflow_type,
                    "definition_digest": target.manifest.definition_digest,
                    "worker_deployment": REPLAY_DEPLOYMENT.name,
                    "worker_build_id": REPLAY_DEPLOYMENT.build_id,
                    "history": str(relative_path),
                }
            )
    INDEX_PATH.write_text(
        json.dumps(
            {
                "format_version": REPLAY_INDEX_FORMAT_VERSION,
                "fixtures": fixtures,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def _sanitize_history(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: "" if key == "stackTrace" else _sanitize_history(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_sanitize_history(item) for item in value]
    return value


if __name__ == "__main__":
    asyncio.run(generate())
