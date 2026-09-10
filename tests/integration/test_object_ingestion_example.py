"""Temporal integration check for the object-ingestion example."""

from __future__ import annotations

import json
from pathlib import Path

from object_ingestion.application import MAPPING_NAME, create_event_registry
from temporalio.worker import Worker

from justflow.config.loader import ConfigLoader
from justflow.engine.activities import WorkflowActivities
from justflow.engine.compiler import compile_workflow
from justflow.engine.local_temporal import start_local_environment
from justflow.engine.sandbox import workflow_sandbox_runner
from tests.conftest import configure_builtin_services

CONFIG_DIR = Path("examples/object_ingestion/configs")
FIXTURE_PATH = Path("examples/object_ingestion/fixtures/s3-object-created.json")
TASK_QUEUE = "test-object-ingestion-example"


async def test_mapped_s3_event_runs_object_ingestion_workflow() -> None:
    loader = ConfigLoader(CONFIG_DIR)
    services = configure_builtin_services(loader.load_services().services)
    workflow_config = loader.load_workflows()["object_ingestion"].model_copy(
        update={"on_complete": None}
    )
    workflow_class = compile_workflow(workflow_config, services.resolved)
    activities = WorkflowActivities(services=dict(services.configured))
    event = json.loads(FIXTURE_PATH.read_text())
    mapper, _ = create_event_registry().resolve(MAPPING_NAME)
    identity = await mapper.event_identity(event)
    request = await mapper.to_start_request(event, identity)

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
            {"request_id": request.business_request_id, "globals": request.input},
            id=request.business_request_id,
            task_queue=TASK_QUEUE,
        )

    assert result["status"] == "completed"
    assert result["result"] == {
        "source_key": "incoming/order-100.json",
        "archive_key": "archive/order-100.json",
        "state": "archived",
    }
