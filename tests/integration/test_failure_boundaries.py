"""Integration tests for durable Temporal failure details."""

from __future__ import annotations

import json
from typing import Any

import pytest
from temporalio.client import WorkflowFailureError
from temporalio.exceptions import ApplicationError
from temporalio.worker import Worker

from justflow.config.models import FlowStep, WaitForConfig, WorkflowConfig
from justflow.engine.compiler import compile_workflow
from justflow.engine.local_temporal import start_local_environment
from justflow.engine.sandbox import workflow_sandbox_runner
from justflow.sdk.message_contract import WORKFLOW_EVENT_SIGNAL

TASK_QUEUE = "test-failure-boundaries"
SIGNAL_TIMEOUT_SEC = 30
SENSITIVE_SENTINEL = "secret-pii-sentinel"


@pytest.mark.parametrize(
    "trigger,workflow_id",
    [
        pytest.param(
            {"globals": {"token": SENSITIVE_SENTINEL}},
            "missing-request-id",
            id="invalid-fields",
        ),
        pytest.param([], "non-mapping-trigger", id="non-mapping"),
    ],
)
async def test_invalid_trigger_is_recorded_in_application_error_details(
    trigger: Any,
    workflow_id: str,
):
    config = WorkflowConfig(
        workflow="invalid_trigger_boundary",
        steps={},
        flow=[FlowStep(name="done", terminal=True)],
    )
    workflow_class = compile_workflow(config, {})

    async with (
        await start_local_environment() as env,
        Worker(
            env.client,
            task_queue=TASK_QUEUE,
            workflows=[workflow_class],
            workflow_runner=workflow_sandbox_runner(),
        ),
    ):
        with pytest.raises(WorkflowFailureError) as exc_info:
            await env.client.execute_workflow(
                workflow_class.run,
                trigger,
                id=workflow_id,
                task_queue=TASK_QUEUE,
            )

    cause = exc_info.value.cause
    assert isinstance(cause, ApplicationError)
    assert cause.type == "INVALID_TRIGGER"
    assert cause.non_retryable is True
    record = cause.details[0]
    assert record["status"] == "failed"
    assert record["correlation"]["workflow"] == "invalid_trigger_boundary"
    assert record["correlation"]["run_id"]
    assert record["error"]["code"] == "INVALID_TRIGGER"
    assert record["error"]["phase"] == "trigger"
    assert SENSITIVE_SENTINEL not in str(cause)
    assert SENSITIVE_SENTINEL not in json.dumps(record)


@pytest.mark.parametrize(
    "signal_payload,workflow_id",
    [
        pytest.param({"signal": ""}, "invalid-signal-fields", id="invalid-fields"),
        pytest.param([], "non-mapping-signal", id="non-mapping"),
    ],
)
async def test_invalid_signal_is_recorded_in_application_error_details(
    signal_payload: Any,
    workflow_id: str,
):
    config = WorkflowConfig(
        workflow="invalid_signal_boundary",
        steps={},
        flow=[
            FlowStep(
                name="wait",
                wait_for=WaitForConfig(signal="ready", timeout_sec=SIGNAL_TIMEOUT_SEC),
                then="done",
            ),
            FlowStep(name="done", terminal=True),
        ],
    )
    workflow_class = compile_workflow(config, {})

    async with (
        await start_local_environment() as env,
        Worker(
            env.client,
            task_queue=TASK_QUEUE,
            workflows=[workflow_class],
            workflow_runner=workflow_sandbox_runner(),
        ),
    ):
        handle = await env.client.start_workflow(
            workflow_class.run,
            {"request_id": "invalid-signal", "globals": {}},
            id=workflow_id,
            task_queue=TASK_QUEUE,
        )
        await handle.signal(WORKFLOW_EVENT_SIGNAL, signal_payload)
        with pytest.raises(WorkflowFailureError) as exc_info:
            await handle.result()

    cause = exc_info.value.cause
    assert isinstance(cause, ApplicationError)
    assert cause.type == "INVALID_SIGNAL"
    assert cause.non_retryable is True
    record = cause.details[0]
    assert record["status"] == "failed"
    assert record["error"]["code"] == "INVALID_SIGNAL"
    assert record["error"]["phase"] == "signal"
