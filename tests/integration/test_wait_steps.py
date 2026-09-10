"""Integration tests for wait_for steps: real signals, real timers."""

from __future__ import annotations

from temporalio.worker import Replayer, Worker

from justflow.config.models import (
    FlowStep,
    ServiceConfig,
    StepDefinition,
    WaitForConfig,
    WorkflowConfig,
)
from justflow.engine.activities import WorkflowActivities
from justflow.engine.compiler import compile_workflow
from justflow.engine.local_temporal import start_local_environment
from justflow.engine.sandbox import workflow_sandbox_runner
from justflow.sdk.message_contract import WORKFLOW_EVENT_SIGNAL
from tests.conftest import configure_builtin_services

TASK_QUEUE = "test-wait-steps"
SHORT_TIMEOUT_SEC = 2
SIGNAL_TIMEOUT_SEC = 60
DIRECT_TIMEOUT_SEC = 10
TEST_SIGNAL = "data_received"

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


def _wait_workflow(name: str, wait_for: WaitForConfig) -> WorkflowConfig:
    return WorkflowConfig(
        workflow=name,
        steps={"fallback_action": StepDefinition(service="records", action="fetch_record")},
        flow=[
            FlowStep(name="wait_event", wait_for=wait_for, output="event", then="done"),
            FlowStep(name="fallback", op="fallback_action", then="done"),
            FlowStep(name="done", terminal=True),
        ],
    )


class TestWaitForIntegration:
    async def test_event_resumes_the_wait(self):
        wf_config = _wait_workflow(
            "wait_happy",
            WaitForConfig(signal=TEST_SIGNAL, timeout_sec=SIGNAL_TIMEOUT_SEC),
        )
        wf_class = compile_workflow(wf_config, SERVICES.resolved)
        activities = WorkflowActivities(services=dict(SERVICES.configured))

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
            handle = await env.client.start_workflow(
                wf_class.run,
                {"request_id": "wait-1", "globals": {}},
                id="wait-1",
                task_queue=TASK_QUEUE,
            )

            await handle.signal(
                WORKFLOW_EVENT_SIGNAL,
                {"signal": TEST_SIGNAL, "data": {"values": ["a1", "a2"]}},
            )

            result = await handle.result()
            history = await handle.fetch_history()
            await Replayer(
                workflows=[wf_class],
                workflow_runner=workflow_sandbox_runner(),
            ).replay_workflow(history)

        assert result["status"] == "completed"
        assert result["steps"]["wait_event"]["status"] == "succeeded"
        assert result["steps"]["wait_event"]["output"] == {"values": ["a1", "a2"]}

    async def test_timeout_takes_the_on_timeout_branch(self):
        wf_config = _wait_workflow(
            "wait_timeout",
            WaitForConfig(
                signal=TEST_SIGNAL,
                timeout_sec=SHORT_TIMEOUT_SEC,
                on_timeout="fallback",
            ),
        )
        wf_class = compile_workflow(wf_config, SERVICES.resolved)
        activities = WorkflowActivities(services=dict(SERVICES.configured))

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
                {"request_id": "wait-2", "globals": {}},
                id="wait-2",
                task_queue=TASK_QUEUE,
            )

        assert result["status"] == "completed"
        assert result["steps"]["wait_event"]["status"] == "timed_out"
        assert result["steps"]["fallback"]["status"] == "succeeded"
        assert result["transitions"][0] == {
            "step": "wait_event",
            "matched": "timeout",
            "target": "fallback",
        }
