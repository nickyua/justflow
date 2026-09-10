"""Temporal integration check for the custom host-application example."""

from __future__ import annotations

from pathlib import Path

from host_application.resources import create_resource_registry
from temporalio.worker import Worker

from justflow.config.loader import ConfigLoader
from justflow.config.validator import ConfigValidator
from justflow.engine.activities import WorkflowActivities
from justflow.engine.compiler import compile_workflow
from justflow.engine.local_temporal import start_local_environment
from justflow.engine.sandbox import workflow_sandbox_runner
from justflow.sdk.resource_loader import ResourceLoader
from tests.conftest import configure_builtin_services

CONFIG_DIR = Path("examples/host_application/src/host_application/configs")
TASK_QUEUE = "test-host-application-example"


async def test_host_owned_provider_runs_through_the_workflow_boundary() -> None:
    config_loader = ConfigLoader(CONFIG_DIR)
    resources, service_declarations, workflows = config_loader.load_all()
    resource_registry = create_resource_registry()
    validator = ConfigValidator(
        resources,
        service_declarations,
        workflows,
        resource_registry=resource_registry,
        config_dir=CONFIG_DIR,
        workflow_sources=config_loader.workflow_sources,
        triggers=config_loader.load_triggers(),
    )
    validator.validate().raise_if_invalid()
    resource_loader = ResourceLoader(resource_registry)
    await resource_loader.load(dict(validator.resolved_resources))
    try:
        services = configure_builtin_services(
            service_declarations.services,
            resources=resource_loader.resources,
        )
        workflow_config = workflows["hello"].model_copy(update={"on_complete": None})
        workflow_class = compile_workflow(workflow_config, services.resolved)
        activities = WorkflowActivities(
            services=dict(services.configured),
            resources=resource_loader.resources,
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
                    "request_id": "host-example-request",
                    "globals": {"subject_ref": "subject-example"},
                },
                id="host-example-request",
                task_queue=TASK_QUEUE,
            )
    finally:
        await resource_loader.close()

    assert result["status"] == "completed"
    assert result["result"] == {
        "subject_ref": "subject-example",
        "message": "Welcome, subject-example",
        "state": "rendered",
    }
