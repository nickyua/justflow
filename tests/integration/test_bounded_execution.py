"""Integration coverage for bounded Temporal history continuation."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import pytest
from temporalio.client import WorkflowContinuedAsNewError
from temporalio.common import PinnedVersioningOverride
from temporalio.worker import Worker

from justflow.config.models import (
    FlowStep,
    ServiceConfig,
    StepDefinition,
    WaitForConfig,
    WorkflowConfig,
)
from justflow.config.runtime_limits import RuntimeLimits
from justflow.definitions.manifest import (
    ENGINE_WORKFLOW_ABI,
    SHA256_HEX_LENGTH,
    build_definition_manifests,
)
from justflow.definitions.routing import (
    WorkerDeployment,
    execution_memo,
    retry_pinned_workflow_start,
    wait_for_worker_deployment,
    worker_deployment_config,
)
from justflow.engine.activities import WorkflowActivities
from justflow.engine.compiler import compile_workflow
from justflow.engine.local_temporal import start_local_environment
from justflow.engine.sandbox import workflow_sandbox_runner
from justflow.provenance import WorkerArtifactIdentity
from justflow.sdk.base_action import BaseAction
from justflow.sdk.message_contract import WORKFLOW_EVENT_SIGNAL, WorkflowTrigger
from tests.conftest import configure_builtin_services

TASK_QUEUE = "test-bounded-execution"
WORKFLOW_ID = "bounded-history-workflow"
DIRECT_TIMEOUT_SEC = 10
HISTORY_EVENT_THRESHOLD = 1
MAX_CONTINUATIONS = 4
EVENT_WAIT_TIMEOUT_SEC = 10
FANOUT_ITEMS = 25
FANOUT_CHUNK_ITEMS = 10
FANOUT_PARALLELISM = 5
ENVIRONMENT_SNAPSHOT_DIGEST = "e" * SHA256_HEX_LENGTH
DEPLOYMENT = WorkerDeployment(
    artifact_identity=WorkerArtifactIdentity(
        deployment_name="justflow",
        build_id="bounded-execution-integration",
        artifact_digest=f"sha256:{'a' * 64}",
        package_version="0.1.0",
    ),
    compatible_engine_workflow_abis=frozenset({ENGINE_WORKFLOW_ABI}),
)
VERSIONING_OVERRIDE = PinnedVersioningOverride(DEPLOYMENT.temporal_version)


class CheckpointActions(BaseAction):
    async def capture(self, input: Any) -> dict[str, Any]:
        return {"step": self.context.step_name, "input": input}

    async def hold(self, input: Any) -> dict[str, bool]:
        gate = self.get_resource("gate")
        if not isinstance(gate, ActivityGate):
            raise TypeError("gate resource has an invalid type")
        gate.started.set()
        await gate.release.wait()
        return {"released": True}


@dataclass(frozen=True, kw_only=True)
class ActivityGate:
    started: asyncio.Event
    release: asyncio.Event


async def test_continue_as_new_preserves_state_and_worker_identity() -> None:
    services = configure_builtin_services(
        {
            "checkpoint": ServiceConfig(
                transport="direct",
                transport_config={"class": f"{__name__}.CheckpointActions"},
                dispatch_timeout_sec=DIRECT_TIMEOUT_SEC,
                retries=0,
            )
        }
    )
    workflow_config = WorkflowConfig(
        workflow="bounded_history",
        result="second.result",
        steps={
            "capture": StepDefinition(service="checkpoint", action="capture"),
        },
        flow=[
            FlowStep(name="first", op="capture", output="result", then="second"),
            FlowStep(
                name="second",
                op="capture",
                input="first.result",
                output="result",
                then="done",
            ),
            FlowStep(name="done", terminal=True),
        ],
    )
    limits = RuntimeLimits(history_events=HISTORY_EVENT_THRESHOLD)
    manifest = build_definition_manifests(
        {workflow_config.workflow: workflow_config},
        services.resolved,
        limits,
    )[workflow_config.workflow]
    workflow_class = compile_workflow(
        workflow_config,
        services.resolved,
        limits=limits,
        manifest=manifest,
        deployment=DEPLOYMENT,
        environment_snapshot_digest=ENVIRONMENT_SNAPSHOT_DIGEST,
    )
    activities = WorkflowActivities(services=dict(services.configured), limits=limits)
    trigger = WorkflowTrigger(
        request_id=WORKFLOW_ID,
        globals={"source": "synthetic"},
        definition_digest=manifest.definition_digest,
        worker_deployment=DEPLOYMENT.name,
        worker_build_id=DEPLOYMENT.build_id,
        worker_artifact=DEPLOYMENT.artifact_identity,
        environment_snapshot_digest=ENVIRONMENT_SNAPSHOT_DIGEST,
    ).model_dump(mode="json")

    async with (
        await start_local_environment() as env,
        Worker(
            env.client,
            task_queue=TASK_QUEUE,
            workflows=[workflow_class],
            activities=[activities.execute_step],
            workflow_runner=workflow_sandbox_runner(),
            deployment_config=worker_deployment_config(DEPLOYMENT),
        ),
    ):
        await wait_for_worker_deployment(env.client, DEPLOYMENT, TASK_QUEUE)
        handle = await retry_pinned_workflow_start(
            lambda: env.client.start_workflow(
                workflow_class.run,
                trigger,
                id=WORKFLOW_ID,
                task_queue=TASK_QUEUE,
                memo=execution_memo(manifest, DEPLOYMENT, ENVIRONMENT_SNAPSHOT_DIGEST),
                versioning_override=VERSIONING_OVERRIDE,
            ),
            DEPLOYMENT,
            TASK_QUEUE,
        )
        run_ids = [handle.run_id]
        current_handle = handle
        for _ in range(MAX_CONTINUATIONS):
            try:
                result = await current_handle.result(follow_runs=False)
            except WorkflowContinuedAsNewError as exc:
                run_ids.append(exc.new_execution_run_id)
                current_handle = env.client.get_workflow_handle(
                    WORKFLOW_ID,
                    run_id=exc.new_execution_run_id,
                )
            else:
                break
        else:
            raise AssertionError("Workflow exceeded the expected continuation bound")

        final_description = await current_handle.describe()
        final_memo = await final_description.memo()

    assert len(run_ids) > 1
    assert len(set(run_ids)) == len(run_ids)
    assert result["definition_digest"] == manifest.definition_digest
    assert result["worker_deployment"] == DEPLOYMENT.name
    assert result["worker_build_id"] == DEPLOYMENT.build_id
    assert result["worker_artifact"] == DEPLOYMENT.artifact_identity.model_dump(
        mode="json", exclude_none=True
    )
    assert result["environment_snapshot_digest"] == ENVIRONMENT_SNAPSHOT_DIGEST
    assert result["result"] == {
        "step": "second",
        "input": {"step": "first", "input": None},
    }
    assert set(result["steps"]) == {"first", "second"}
    assert final_memo["justflow.definition_digest"] == manifest.definition_digest
    assert final_memo["justflow.worker_deployment"] == DEPLOYMENT.name
    assert final_memo["justflow.worker_build_id"] == DEPLOYMENT.build_id
    assert final_memo["justflow.worker_artifact_digest"] == (
        DEPLOYMENT.artifact_identity.artifact_digest
    )
    assert final_memo["justflow.environment_snapshot_digest"] == (ENVIRONMENT_SNAPSHOT_DIGEST)


async def test_continue_as_new_carries_an_already_received_event() -> None:
    gate = ActivityGate(started=asyncio.Event(), release=asyncio.Event())
    services = configure_builtin_services(
        {
            "checkpoint": ServiceConfig(
                transport="direct",
                transport_config={"class": f"{__name__}.CheckpointActions"},
                dispatch_timeout_sec=DIRECT_TIMEOUT_SEC,
                retries=0,
            )
        },
        resources={"gate": gate},
    )
    workflow_config = WorkflowConfig(
        workflow="bounded_event",
        result="approval",
        steps={
            "hold": StepDefinition(
                service="checkpoint",
                action="hold",
                required_resources=["gate"],
            ),
        },
        flow=[
            FlowStep(name="hold", op="hold", then="approval"),
            FlowStep(
                name="approval",
                wait_for=WaitForConfig(
                    signal="approved",
                    timeout_sec=EVENT_WAIT_TIMEOUT_SEC,
                ),
                output="approval",
                then="done",
            ),
            FlowStep(name="done", terminal=True),
        ],
    )
    limits = RuntimeLimits(history_events=HISTORY_EVENT_THRESHOLD)
    workflow_class = compile_workflow(workflow_config, services.resolved, limits=limits)
    activities = WorkflowActivities(services=dict(services.configured), limits=limits)

    async with (
        await start_local_environment() as env,
        Worker(
            env.client,
            task_queue=TASK_QUEUE,
            workflows=[workflow_class],
            activities=[activities.execute_step],
            workflow_runner=workflow_sandbox_runner(),
        ),
    ):
        handle = await env.client.start_workflow(
            workflow_class.run,
            {"request_id": "bounded-event", "globals": {}},
            id="bounded-event",
            task_queue=TASK_QUEUE,
        )
        await gate.started.wait()
        await handle.signal(
            WORKFLOW_EVENT_SIGNAL,
            {"signal": "approved", "data": {"approved": True}},
        )
        gate.release.set()

        with pytest.raises(WorkflowContinuedAsNewError):
            await handle.result(follow_runs=False)
        result = await handle.result()

    assert result["result"] == {"approved": True}
    assert result["steps"]["approval"]["output"] == {"approved": True}


async def test_parallel_fanout_continues_between_chunks() -> None:
    services = configure_builtin_services(
        {
            "checkpoint": ServiceConfig(
                transport="direct",
                transport_config={"class": f"{__name__}.CheckpointActions"},
                dispatch_timeout_sec=DIRECT_TIMEOUT_SEC,
                retries=0,
            )
        }
    )
    workflow_config = WorkflowConfig(
        workflow="bounded_fanout",
        result="results",
        steps={"capture": StepDefinition(service="checkpoint", action="capture")},
        flow=[
            FlowStep(
                name="fanout",
                op="capture",
                for_each="items",
                as_var="item",
                parallel=True,
                max_concurrency=FANOUT_PARALLELISM,
                output="results",
                then="done",
            ),
            FlowStep(name="done", terminal=True),
        ],
    )
    limits = RuntimeLimits(
        fanout_items=FANOUT_ITEMS,
        fanout_chunk_items=FANOUT_CHUNK_ITEMS,
        history_events=HISTORY_EVENT_THRESHOLD,
    )
    workflow_class = compile_workflow(workflow_config, services.resolved, limits=limits)
    activities = WorkflowActivities(services=dict(services.configured), limits=limits)

    async with (
        await start_local_environment() as env,
        Worker(
            env.client,
            task_queue=TASK_QUEUE,
            workflows=[workflow_class],
            activities=[activities.execute_step],
            workflow_runner=workflow_sandbox_runner(),
        ),
    ):
        handle = await env.client.start_workflow(
            workflow_class.run,
            {
                "request_id": "bounded-fanout",
                "globals": {"items": list(range(FANOUT_ITEMS))},
            },
            id="bounded-fanout",
            task_queue=TASK_QUEUE,
        )
        with pytest.raises(WorkflowContinuedAsNewError):
            await handle.result(follow_runs=False)
        result = await handle.result()

    outputs = result["steps"]["fanout"]["output"]
    assert [output["input"] for output in outputs] == list(range(FANOUT_ITEMS))
