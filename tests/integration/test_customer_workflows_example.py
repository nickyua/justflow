"""Execute the maintained one-customer and bounded population declarations on Temporal."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest
from temporalio.worker import Worker

from justflow.config.loader import ConfigLoader
from justflow.config.validator import ConfigValidator
from justflow.engine.activities import WorkflowActivities
from justflow.engine.compiler import compile_workflow
from justflow.engine.local_temporal import start_local_environment
from justflow.engine.sandbox import workflow_sandbox_runner
from justflow.resources.builtins import builtin_resource_registry
from justflow.sdk import ResourceLoader
from tests.conftest import configure_builtin_services

CONFIG_DIR = Path("examples/scheduled_reporting/customer_configs")
TASK_QUEUE = "test-customer-example"
CUSTOMER_REFS = ("customer_alpha", "customer_beta")


@dataclass(frozen=True, kw_only=True)
class Returns:
    value: dict[str, object]


@dataclass(frozen=True, kw_only=True)
class CustomerExecutionCase:
    id: str
    workflow: str
    input: dict[str, object]
    outcome: Returns


EXECUTION_CASES = [
    CustomerExecutionCase(
        id="one-customer",
        workflow="customer_check",
        input={"customer": {"customer_ref": CUSTOMER_REFS[0]}},
        outcome=Returns(value={"customer_ref": CUSTOMER_REFS[0], "state": "checked"}),
    ),
    CustomerExecutionCase(
        id="bounded-population",
        workflow="customer_batch",
        input={"cursor": 0, "page_size": 25},
        outcome=Returns(
            value={
                "outcomes": [{"customer_ref": ref, "state": "checked"} for ref in CUSTOMER_REFS],
                "next_cursor": None,
                "attempted": len(CUSTOMER_REFS),
            }
        ),
    ),
    CustomerExecutionCase(
        id="empty-population",
        workflow="customer_batch",
        input={"cursor": len(CUSTOMER_REFS), "page_size": 25},
        outcome=Returns(value={"outcomes": [], "next_cursor": None, "attempted": 0}),
    ),
]


@pytest.mark.parametrize("case", EXECUTION_CASES, ids=lambda c: c.id)
async def test_customer_workflows(case: CustomerExecutionCase) -> None:
    loader = ConfigLoader(CONFIG_DIR)
    resources, declarations, workflows = loader.load_all()
    registry = builtin_resource_registry()
    validator = ConfigValidator(
        resources,
        declarations,
        workflows,
        resource_registry=registry,
        config_dir=CONFIG_DIR,
        workflow_sources=loader.workflow_sources,
        triggers=loader.load_triggers(),
    )
    validator.validate().raise_if_invalid()
    owned = ResourceLoader(registry)
    await owned.load(dict(validator.resolved_resources))
    try:
        services = configure_builtin_services(declarations.services, resources=owned.resources)
        classes = {
            name: compile_workflow(config, services.resolved) for name, config in workflows.items()
        }
        activities = WorkflowActivities(
            services=dict(services.configured), resources=owned.resources
        )
        async with (
            await start_local_environment() as environment,
            Worker(
                environment.client,
                task_queue=TASK_QUEUE,
                workflows=list(classes.values()),
                activities=[activities.execute_step, activities.validate_contract],
                workflow_runner=workflow_sandbox_runner(),
            ),
        ):
            result = await environment.client.execute_workflow(
                classes[case.workflow].run,
                {
                    "request_id": case.id,
                    "correlation_id": "independent-batch-correlation",
                    "globals": case.input,
                },
                id=case.id,
                task_queue=TASK_QUEUE,
            )
            if case.id == "bounded-population":
                for index, ref in enumerate(CUSTOMER_REFS):
                    child = await environment.client.get_workflow_handle(
                        f"{case.id}:customers[{index}]"
                    ).result()
                    assert child["result"] == {"customer_ref": ref, "state": "checked"}
    finally:
        await owned.close()
    assert result["status"] == "completed"
    assert result["result"] == case.outcome.value
