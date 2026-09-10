"""Immutable pre-fix cases for the first beta's additional replay families."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from temporalio import activity

from justflow.config.models import ResourceConfig, WorkflowConfig
from justflow.config.runtime_limits import RuntimeLimits
from justflow.definitions.catalog import DefinitionCatalog
from justflow.definitions.manifest import build_definition_manifests
from justflow.definitions.routing import WorkerDeploymentRouter
from justflow.definitions.runtime import PreparedDefinitions, prepare_definitions
from justflow.engine.activities import ActivityInput, ContractValidationInput, StepActivityResult
from justflow.engine.archival import ArchiveRequest
from justflow.engine.contracts import validate_payload
from justflow.resources.builtins import builtin_resource_registry
from justflow.scope import LOCAL_RUNTIME_SCOPE
from justflow.transports.builtins import builtin_transport_registry
from tests.replay_cases import (
    REPLAY_DEPLOYMENT,
    REPLAY_ENVIRONMENT_SNAPSHOT_DIGEST,
    REPLAY_SERVICE,
    REPLAY_SERVICE_NAME,
)

BASELINE_HISTORY_EVENTS = 20
BASELINE_STEPS = 8
BASELINE_RETENTION_SECONDS = 60
BASELINE_LIMITS = RuntimeLimits(history_events=BASELINE_HISTORY_EVENTS)
BASELINE_FAMILIES = {
    "baseline_contract": "contract-validation-baseline",
    "baseline_archive": "archival-baseline",
    "baseline_continuation": "continuation-baseline",
}


class BaselineActivities:
    def __init__(self) -> None:
        self.invocations: list[str] = []
        self.archives: list[ArchiveRequest] = []

    @activity.defn(name="execute_step")
    async def execute_step(self, request: ActivityInput) -> dict[str, Any]:
        self.invocations.append(request.step_name)
        return asdict(StepActivityResult(data={"value": request.step_name}))

    @activity.defn(name="validate_contract")
    async def validate_contract(self, request: ContractValidationInput) -> None:
        validate_payload(
            request.schema,
            request.payload,
            direction=request.direction,
            step_name=request.boundary_name,
        )

    @activity.defn(name="archive_workflow")
    async def archive_workflow(self, request: ArchiveRequest) -> None:
        self.archives.append(request)


def prepare_baseline_definitions() -> PreparedDefinitions:
    operation = {"service": REPLAY_SERVICE_NAME, "action": "echo"}
    workflows = {
        "baseline_contract": WorkflowConfig.model_validate(
            {
                "workflow": "baseline_contract",
                "input_schema": {"type": "object"},
                "output_schema": {"type": "object", "required": ["value"]},
                "result": "echo.value",
                "steps": {"echo": operation},
                "flow": [
                    {"name": "echo", "op": "echo", "output": "value", "then": "done"},
                    {"name": "done", "terminal": True},
                ],
            }
        ),
        "baseline_archive": WorkflowConfig.model_validate(
            {
                "workflow": "baseline_archive",
                "on_complete": {
                    "resource": "archive",
                    "path": "baseline/record.json",
                    "retention_policy": "baseline",
                },
                "steps": {},
                "flow": [{"name": "done", "terminal": True}],
            }
        ),
        "baseline_continuation": WorkflowConfig.model_validate(
            {
                "workflow": "baseline_continuation",
                "steps": {"echo": operation},
                "result": "step0.value",
                "flow": [
                    {
                        "name": f"step{index}",
                        "op": "echo",
                        "output": "value",
                        "then": f"step{index + 1}" if index + 1 < BASELINE_STEPS else "done",
                    }
                    for index in range(BASELINE_STEPS)
                ]
                + [{"name": "done", "terminal": True}],
            }
        ),
    }
    services = builtin_transport_registry().resolve_services({REPLAY_SERVICE_NAME: REPLAY_SERVICE})
    resources = builtin_resource_registry().resolve_resources(
        {
            "archive": ResourceConfig(
                provider="memory_archive",
                config={"retention_policies": {"baseline": BASELINE_RETENTION_SECONDS}},
            )
        }
    )
    manifests = build_definition_manifests(
        workflows, services, BASELINE_LIMITS, resources=resources
    )
    return prepare_definitions(
        workflows,
        services,
        BASELINE_LIMITS,
        DefinitionCatalog.from_manifests(manifests),
        WorkerDeploymentRouter.for_deployment(REPLAY_DEPLOYMENT),
        {name: REPLAY_ENVIRONMENT_SNAPSHOT_DIGEST for name in workflows},
        runtime_scope=LOCAL_RUNTIME_SCOPE,
        resources=resources,
    )
