"""Integration tests for Temporal workflow isolation."""

from __future__ import annotations

from dataclasses import dataclass

import pytest
from temporalio import workflow
from temporalio.client import WorkflowFailureError
from temporalio.worker import Worker

from justflow.engine.local_temporal import start_local_environment
from justflow.engine.sandbox import workflow_sandbox_runner
from tests.workflow_fixtures.sandbox_probe import SandboxProbe

TASK_QUEUE = "sandbox-integration"


@dataclass(frozen=True, kw_only=True)
class Raises:
    exc: type[Exception]
    match: str


@dataclass(frozen=True, kw_only=True)
class SandboxCase:
    id: str
    operation: str
    outcome: Raises


SANDBOX_CASES = [
    SandboxCase(
        id="filesystem",
        operation="filesystem",
        outcome=Raises(exc=WorkflowFailureError, match="pathlib.Path.cwd"),
    ),
    SandboxCase(
        id="network",
        operation="network",
        outcome=Raises(exc=WorkflowFailureError, match="socket.socket"),
    ),
    SandboxCase(
        id="time",
        operation="time",
        outcome=Raises(exc=WorkflowFailureError, match="time.time"),
    ),
    SandboxCase(
        id="random",
        operation="random",
        outcome=Raises(exc=WorkflowFailureError, match="random.random"),
    ),
    SandboxCase(
        id="environment",
        operation="environment",
        outcome=Raises(exc=WorkflowFailureError, match="os.getenv"),
    ),
    SandboxCase(
        id="runtime-import",
        operation="import",
        outcome=Raises(exc=WorkflowFailureError, match="importlib.import_module"),
    ),
]


@pytest.mark.parametrize("case", SANDBOX_CASES, ids=lambda case: case.id)
async def test_nondeterministic_workflow_operations_are_rejected(case: SandboxCase) -> None:
    async with (
        await start_local_environment() as env,
        Worker(
            env.client,
            task_queue=TASK_QUEUE,
            workflows=[SandboxProbe],
            workflow_runner=workflow_sandbox_runner(),
            workflow_failure_exception_types=[workflow.NondeterminismError],
        ),
    ):
        with pytest.raises(case.outcome.exc) as exc_info:
            await env.client.execute_workflow(
                SandboxProbe.run,
                case.operation,
                id=f"sandbox-{case.id}",
                task_queue=TASK_QUEUE,
            )

    failure_chain = " ".join(
        str(error) for error in (exc_info.value, exc_info.value.__cause__) if error is not None
    )
    assert case.outcome.match in failure_chain
