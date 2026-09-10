"""Integration test — a parent workflow executes a child workflow as a step."""

from __future__ import annotations

from temporalio.worker import Worker

from justflow.config.models import (
    FlowStep,
    ServiceConfig,
    StepDefinition,
    WorkflowConfig,
)
from justflow.engine.activities import WorkflowActivities
from justflow.engine.compiler import compile_workflow
from justflow.engine.local_temporal import start_local_environment
from justflow.engine.sandbox import workflow_sandbox_runner
from tests.conftest import configure_builtin_services

TASK_QUEUE = "test-subworkflows"
DIRECT_TIMEOUT_SEC = 10

SERVICES = configure_builtin_services(
    {
        "records": ServiceConfig(
            transport="direct",
            transport_config={"class": "tests.workflow_fixtures.actions.fetch_record.FetchRecord"},
            dispatch_timeout_sec=DIRECT_TIMEOUT_SEC,
            retries=0,
        )
    }
)

CHILD = WorkflowConfig(
    workflow="fetch_child",
    input_schema={"type": "object", "maxProperties": 0},
    output_schema={
        "type": "object",
        "properties": {"record_id": {"type": "string"}},
        "required": ["record_id"],
    },
    result="grab.record",
    steps={"fetch": StepDefinition(service="records", action="fetch_record")},
    flow=[
        FlowStep(name="grab", op="fetch", output="record", then="done"),
        FlowStep(name="done", terminal=True),
    ],
)

PARENT = WorkflowConfig(
    workflow="fetching_parent",
    steps={"fetch_via_child": StepDefinition(workflow="fetch_child")},
    flow=[
        FlowStep(name="delegate", op="fetch_via_child", output="record", then="done"),
        FlowStep(name="done", terminal=True),
    ],
)


async def test_parent_runs_child_and_gets_its_result():
    child_class = compile_workflow(CHILD, SERVICES.resolved)
    parent_class = compile_workflow(PARENT, SERVICES.resolved)
    activities = WorkflowActivities(services=dict(SERVICES.configured))

    async with (
        await start_local_environment() as env,
        Worker(
            env.client,
            task_queue=TASK_QUEUE,
            workflows=[parent_class, child_class],
            activities=[activities.execute_step, activities.validate_contract],
            workflow_runner=workflow_sandbox_runner(),
        ),
    ):
        result = await env.client.execute_workflow(
            parent_class.run,
            {"request_id": "parent-1", "globals": {}},
            id="parent-1",
            task_queue=TASK_QUEUE,
        )

        # The child ran as its own execution, addressable by the derived id
        child_handle = env.client.get_workflow_handle("parent-1:delegate")
        child_record = await child_handle.result()

    assert result["status"] == "completed"
    # The parent step output is the child's declared `result`, not its audit record
    assert result["steps"]["delegate"]["output"]["record_id"] == "R001"
    assert child_record["workflow"] == "fetch_child"
    assert child_record["result"]["record_id"] == "R001"
