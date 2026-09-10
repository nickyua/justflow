"""Temporal integration check for the scheduled-reporting example."""

from __future__ import annotations

from pathlib import Path

from temporalio.worker import Worker

from justflow.config.loader import ConfigLoader
from justflow.engine.activities import WorkflowActivities
from justflow.engine.compiler import compile_workflow
from justflow.engine.local_temporal import start_local_environment
from justflow.engine.sandbox import workflow_sandbox_runner
from justflow.resources.memory import StaticConfig
from tests.conftest import configure_builtin_services

CONFIG_DIR = Path("examples/scheduled_reporting/configs")
TASK_QUEUE = "test-scheduled-reporting-example"
REPORT_DATE = "2026-08-10"


async def test_scheduled_reporting_uses_explicit_occurrence_input() -> None:
    loader = ConfigLoader(CONFIG_DIR)
    resources = {"reporting_config": StaticConfig(report_title="Daily synthetic activity report")}
    services = configure_builtin_services(
        loader.load_services().services,
        resources=resources,
    )
    workflow_config = loader.load_workflows()["scheduled_reporting"].model_copy(
        update={"on_complete": None}
    )
    workflow_class = compile_workflow(workflow_config, services.resolved)
    activities = WorkflowActivities(
        services=dict(services.configured),
        resources=resources,
    )

    async with (
        await start_local_environment() as environment,
        Worker(
            environment.client,
            task_queue=TASK_QUEUE,
            workflows=[workflow_class],
            activities=[activities.execute_step, activities.validate_contract],
            workflow_runner=workflow_sandbox_runner(),
        ),
    ):
        result = await environment.client.execute_workflow(
            workflow_class.run,
            {
                "request_id": "report-2026-08-10",
                "globals": {"report_date": REPORT_DATE},
            },
            id="report-2026-08-10",
            task_queue=TASK_QUEUE,
        )

    assert result["status"] == "completed"
    assert result["result"] == {
        "report_id": "daily-2026-08-10",
        "title": "Daily synthetic activity report",
        "row_count": 10,
    }
