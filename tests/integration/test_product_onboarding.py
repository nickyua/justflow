"""Temporal integration checks for the product-onboarding example."""

from __future__ import annotations

from pathlib import Path

from temporalio.worker import Worker

from justflow.config.loader import ConfigLoader
from justflow.engine.activities import WorkflowActivities
from justflow.engine.compiler import compile_workflow
from justflow.engine.local_temporal import start_local_environment
from justflow.engine.sandbox import workflow_sandbox_runner
from justflow.sdk.message_contract import WORKFLOW_EVENT_SIGNAL
from tests.conftest import configure_builtin_services

CONFIG_DIR = Path("examples/product_onboarding/configs")
TASK_QUEUE = "test-product-onboarding"
APPROVAL_SIGNAL = "onboarding_approved"
REQUEST = {
    "customer_ref": "customer-demo",
    "plan": "starter",
    "simulate_provisioning_failure": False,
}


async def _run_onboarding(*, simulate_failure: bool) -> tuple[dict[str, object], str]:
    loader = ConfigLoader(CONFIG_DIR)
    services = configure_builtin_services(loader.load_services().services)
    workflows = loader.load_workflows()
    parent = workflows["product_onboarding"].model_copy(update={"on_complete": None})
    child = workflows["provision_workspace"]
    parent_class = compile_workflow(parent, services.resolved)
    child_class = compile_workflow(child, services.resolved)
    activities = WorkflowActivities(services=dict(services.configured))
    request_id = "onboarding-failure" if simulate_failure else "onboarding-success"

    async with (
        await start_local_environment() as environment,
        Worker(
            environment.client,
            task_queue=TASK_QUEUE,
            workflows=[parent_class, child_class],
            activities=[activities.execute_step, activities.validate_contract],
            workflow_runner=workflow_sandbox_runner(),
        ),
    ):
        handle = await environment.client.start_workflow(
            parent_class.run,
            {
                "request_id": request_id,
                "globals": {
                    **REQUEST,
                    "simulate_provisioning_failure": simulate_failure,
                },
            },
            id=request_id,
            task_queue=TASK_QUEUE,
        )
        await handle.signal(
            WORKFLOW_EVENT_SIGNAL,
            {"signal": APPROVAL_SIGNAL, "data": {"decision": "approved"}},
        )
        result = await handle.result()

    return result, request_id


async def test_product_onboarding_runs_child_after_bounded_approval() -> None:
    result, request_id = await _run_onboarding(simulate_failure=False)

    assert result["status"] == "completed"
    assert result["reason"] is None
    assert result["steps"]["await_approval"]["status"] == "succeeded"
    assert result["steps"]["provision"]["output"] == {
        "workspace_id": "workspace-demo",
        "plan": "starter",
        "state": "provisioned",
    }
    assert request_id == "onboarding-success"


async def test_product_onboarding_compensates_child_failure() -> None:
    result, _ = await _run_onboarding(simulate_failure=True)

    assert result["status"] == "terminated"
    assert result["reason"] == "provisioning_failed"
    assert result["steps"]["provision"]["status"] == "failed"
    assert result["steps"]["compensate"]["output"] == {
        "customer_ref": "customer-demo",
        "state": "compensated",
    }
