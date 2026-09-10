"""Integration tests for direct-transport workflow execution."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from temporalio.worker import Replayer, Worker

from justflow.config.loader import ConfigLoader
from justflow.config.models import (
    FlowStep,
    OnResultBranch,
    ServiceConfig,
    StepDefinition,
    WorkflowConfig,
)
from justflow.engine.activities import WorkflowActivities
from justflow.engine.archival import ArchivalActivity
from justflow.engine.compiler import compile_workflow
from justflow.engine.local_temporal import start_local_environment
from justflow.engine.sandbox import workflow_sandbox_runner
from justflow.sdk.base_action import BaseAction
from tests.conftest import configure_builtin_services

TASK_QUEUE = "test-direct-workflow"
DIRECT_TIMEOUT_SEC = 10
FIXTURE_CONFIGS = Path(__file__).parents[1] / "workflow_fixtures" / "configs"


class EmptyRecord(BaseAction):
    async def fetch_record(self, input: Any) -> dict[str, Any]:
        return {"record_id": "R404", "items": []}


def _direct_service(class_path: str) -> ServiceConfig:
    return ServiceConfig(
        transport="direct",
        transport_config={"class": class_path},
        dispatch_timeout_sec=DIRECT_TIMEOUT_SEC,
        retries=0,
    )


@pytest.fixture
def direct_service_config():
    actions_package = "tests.workflow_fixtures.actions"
    return configure_builtin_services(
        {
            "fetch_record": _direct_service(f"{actions_package}.fetch_record.FetchRecord"),
            "validate_record": _direct_service(f"{actions_package}.validate_record.ValidateRecord"),
            "enrich_record": _direct_service(f"{actions_package}.enrich_record.EnrichRecord"),
            "classify_record": _direct_service(f"{actions_package}.classify_record.ClassifyRecord"),
            "format_result": _direct_service(f"{actions_package}.format_result.FormatResult"),
        }
    )


@pytest.fixture
def workflow_config():
    return ConfigLoader(FIXTURE_CONFIGS).load_workflows()["record_processing"]


@pytest.fixture
def archive_store():
    class FakeStore:
        def __init__(self):
            self.records: dict[str, str] = {}
            self.retention_policies: dict[str, str] = {}

        async def write(self, path: str, data: str, *, retention_policy: str):
            self.records[path] = data
            self.retention_policies[path] = retention_policy

    return FakeStore()


class TestDirectWorkflowEndToEnd:
    async def test_workflow_boundary_contracts(self, direct_service_config):
        wf_config = WorkflowConfig(
            workflow="test_boundary_contracts",
            input_schema={
                "type": "object",
                "properties": {"source_id": {"type": "string"}},
                "required": ["source_id"],
            },
            output_schema={
                "type": "object",
                "properties": {"record_id": {"type": "string"}},
                "required": ["record_id"],
            },
            result="fetch.record",
            steps={
                "fetch_record": StepDefinition(
                    service="fetch_record",
                    action="fetch_record",
                )
            },
            flow=[
                FlowStep(
                    name="fetch",
                    op="fetch_record",
                    output="record",
                    then="done",
                ),
                FlowStep(name="done", terminal=True),
            ],
        )
        wf_class = compile_workflow(wf_config, direct_service_config.resolved)
        activities = WorkflowActivities(
            services=dict(direct_service_config.configured),
            resources={},
        )

        async with (
            await start_local_environment() as env,
            Worker(
                env.client,
                task_queue=TASK_QUEUE,
                workflows=[wf_class],
                activities=[
                    activities.execute_step,
                    activities.validate_contract,
                ],
                workflow_runner=workflow_sandbox_runner(),
            ),
        ):
            result = await env.client.execute_workflow(
                wf_class.run,
                {
                    "request_id": "direct-contracts",
                    "globals": {"source_id": "R001"},
                },
                id="direct-contracts",
                task_queue=TASK_QUEUE,
            )
        assert result["result"]["record_id"] == "R001"

    async def test_full_workflow_happy_path(
        self, direct_service_config, workflow_config, archive_store
    ):
        wf_class = compile_workflow(workflow_config, direct_service_config.resolved)
        activities = WorkflowActivities(
            services=dict(direct_service_config.configured),
            resources={},
        )
        archival = ArchivalActivity(resources={"store": archive_store})
        request_id = "direct-happy"

        async with (
            await start_local_environment() as env,
            Worker(
                env.client,
                task_queue=TASK_QUEUE,
                workflows=[wf_class],
                activities=[activities.execute_step, archival.archive_workflow],
                workflow_runner=workflow_sandbox_runner(),
            ),
        ):
            result = await env.client.execute_workflow(
                wf_class.run,
                {"request_id": request_id, "globals": {}},
                id=request_id,
                task_queue=TASK_QUEUE,
            )
            history = await env.client.get_workflow_handle(request_id).fetch_history()
            await Replayer(
                workflows=[wf_class],
                workflow_runner=workflow_sandbox_runner(),
            ).replay_workflow(history)

        assert result["status"] == "completed"
        assert result["workflow"] == "record_processing"
        assert result["reason"] is None

        steps = result["steps"]
        assert set(steps) == {"fetch", "validate", "enrich", "classify", "format"}
        assert steps["fetch"]["output"]["record_id"] == "R001"
        assert steps["fetch"]["output"]["items"] == ["alpha", "beta", "gamma"]
        assert steps["validate"]["output"]["is_valid"] is True
        assert steps["validate"]["output"]["allowed"] == ["alpha", "beta"]
        assert steps["classify"]["output"]["category"] == "low"
        assert steps["classify"]["output"]["accepted"] is True
        assert "Accepted: True" in steps["format"]["output"]["summary"]
        archive_path = f"audit/{request_id}.json"
        assert set(archive_store.records) == {archive_path}
        assert archive_store.retention_policies == {archive_path: "test"}
        archived_record = json.loads(archive_store.records[archive_path])
        assert archived_record["capture_mode"] == "metadata-only"
        assert "params" not in archived_record
        assert "input" not in archived_record["steps"]["fetch"]
        assert "output" not in archived_record["steps"]["fetch"]

    async def test_workflow_early_termination(self, direct_service_config):
        services = configure_builtin_services(
            {
                **direct_service_config.declarations,
                "fetch_record": _direct_service(f"{__name__}.EmptyRecord"),
            }
        )

        wf_config = WorkflowConfig(
            workflow="test_rejection",
            steps={
                "fetch_record": StepDefinition(service="fetch_record", action="fetch_record"),
                "validate_record": StepDefinition(
                    service="validate_record", action="validate_record"
                ),
            },
            flow=[
                FlowStep(name="fetch", op="fetch_record", output="record", then="validate"),
                FlowStep(
                    name="validate",
                    op="validate_record",
                    input="fetch.record",
                    output="validation",
                    on_result=[
                        OnResultBranch(when="input.is_valid == false", then="rejected"),
                        OnResultBranch(default="done"),
                    ],
                ),
                FlowStep(name="rejected", terminal=True, reason="no_valid_items"),
                FlowStep(name="done", terminal=True),
            ],
        )

        wf_class = compile_workflow(wf_config, services.resolved)
        activities = WorkflowActivities(
            services=dict(services.configured),
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
                {"request_id": "direct-rejected", "globals": {}},
                id="direct-rejected",
                task_queue=TASK_QUEUE,
            )

        assert result["status"] == "terminated"
        assert result["reason"] == "no_valid_items"
        assert result["transitions"] == [
            {
                "step": "validate",
                "matched": "input.is_valid == false",
                "target": "rejected",
            }
        ]

    async def test_workflow_with_condition_skip(self, direct_service_config):
        wf_config = WorkflowConfig(
            workflow="test_condition_skip",
            steps={
                "fetch_record": StepDefinition(service="fetch_record", action="fetch_record"),
                "classify_record": StepDefinition(
                    service="classify_record", action="classify_record"
                ),
                "format_result": StepDefinition(service="format_result", action="format_result"),
            },
            flow=[
                FlowStep(name="fetch", op="fetch_record", output="data", then="maybe_classify"),
                FlowStep(
                    name="maybe_classify",
                    op="classify_record",
                    input="fetch.data",
                    condition="input.record_id == 'other'",
                    output="classification",
                    then="format",
                ),
                FlowStep(
                    name="format",
                    op="format_result",
                    input="fetch.data",
                    output="result",
                    then="done",
                ),
                FlowStep(name="done", terminal=True),
            ],
        )

        wf_class = compile_workflow(wf_config, direct_service_config.resolved)
        activities = WorkflowActivities(
            services=dict(direct_service_config.configured),
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
                {"request_id": "direct-skip", "globals": {}},
                id="direct-skip",
                task_queue=TASK_QUEUE,
            )

        assert result["status"] == "completed"
        assert result["steps"]["maybe_classify"]["status"] == "skipped"
        assert result["steps"]["format"]["status"] == "succeeded"
