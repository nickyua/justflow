"""Integration test for for_each iteration with parallel execution."""

from __future__ import annotations

import pytest
from temporalio.worker import Worker

from justflow.config.models import (
    FlowStep,
    IterationFailStrategy,
    ServiceConfig,
    StepDefinition,
    WorkflowConfig,
)
from justflow.engine.activities import WorkflowActivities
from justflow.engine.compiler import compile_workflow
from justflow.engine.local_temporal import start_local_environment
from justflow.engine.sandbox import workflow_sandbox_runner
from tests.conftest import configure_builtin_services

TASK_QUEUE = "test-iteration"
DIRECT_TIMEOUT_SEC = 10
MAX_CONCURRENCY = 2


def _direct_service(class_path: str) -> ServiceConfig:
    return ServiceConfig(
        transport="direct",
        transport_config={"class": class_path},
        dispatch_timeout_sec=DIRECT_TIMEOUT_SEC,
        retries=0,
    )


@pytest.fixture
def service_config():
    return configure_builtin_services(
        {
            "fetch_record": _direct_service(
                "tests.workflow_fixtures.actions.fetch_record.FetchRecord"
            ),
            "check_item": _direct_service("tests.workflow_fixtures.actions.check_item.CheckItem"),
        }
    )


class TestIteration:
    async def test_sequential_for_each(self, service_config):
        wf_config = WorkflowConfig(
            workflow="test_seq_iteration",
            steps={
                "fetch_record": StepDefinition(service="fetch_record", action="fetch_record"),
                "check_item": StepDefinition(service="check_item", action="check_item"),
            },
            flow=[
                FlowStep(name="fetch", op="fetch_record", output="record", then="check_all"),
                FlowStep(
                    name="check_all",
                    op="check_item",
                    input="fetch.record",
                    for_each="input.items",
                    **{"as": "item"},
                    output="results",
                    then="done",
                ),
                FlowStep(name="done", terminal=True),
            ],
        )

        wf_class = compile_workflow(wf_config, service_config.resolved)
        activities = WorkflowActivities(
            services=dict(service_config.configured),
            resources={},
        )

        async with (
            await start_local_environment() as env,
            Worker(
                env.client,
                task_queue=TASK_QUEUE,
                workflows=[wf_class],
                activities=[activities.execute_step],
                workflow_runner=workflow_sandbox_runner(),
            ),
        ):
            result = await env.client.execute_workflow(
                wf_class.run,
                {"request_id": "iteration-sequential", "globals": {}},
                id="iteration-sequential",
                task_queue=TASK_QUEUE,
            )

        assert result["status"] == "completed"
        assert len(result["steps"]["check_all"]["output"]) == 3

    async def test_parallel_for_each(self, service_config):
        wf_config = WorkflowConfig(
            workflow="test_par_iteration",
            steps={
                "fetch_record": StepDefinition(service="fetch_record", action="fetch_record"),
                "check_item": StepDefinition(service="check_item", action="check_item"),
            },
            flow=[
                FlowStep(name="fetch", op="fetch_record", output="record", then="check_all"),
                FlowStep(
                    name="check_all",
                    op="check_item",
                    input="fetch.record",
                    for_each="input.items",
                    **{"as": "item"},
                    parallel=True,
                    max_concurrency=MAX_CONCURRENCY,
                    on_iteration_fail=IterationFailStrategy.COLLECT,
                    output="results",
                    then="done",
                ),
                FlowStep(name="done", terminal=True),
            ],
        )

        wf_class = compile_workflow(wf_config, service_config.resolved)
        activities = WorkflowActivities(
            services=dict(service_config.configured),
            resources={},
        )

        async with (
            await start_local_environment() as env,
            Worker(
                env.client,
                task_queue=TASK_QUEUE,
                workflows=[wf_class],
                activities=[activities.execute_step],
                workflow_runner=workflow_sandbox_runner(),
            ),
        ):
            result = await env.client.execute_workflow(
                wf_class.run,
                {"request_id": "iteration-parallel", "globals": {}},
                id="iteration-parallel",
                task_queue=TASK_QUEUE,
            )

        assert result["status"] == "completed"
        check_output = result["steps"]["check_all"]["output"]
        assert len(check_output) == 3
        assert all("_error" not in item for item in check_output)
