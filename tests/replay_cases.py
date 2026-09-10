"""Stable workflow cases used to generate and replay compatibility histories."""

from __future__ import annotations

from dataclasses import asdict
from types import MappingProxyType
from typing import Any

from temporalio import activity
from temporalio.exceptions import ApplicationError

from justflow.config.models import (
    FlowStep,
    OnResultBranch,
    ServiceConfig,
    StepDefinition,
    WaitForConfig,
    WorkflowConfig,
)
from justflow.config.runtime_limits import DEFAULT_RUNTIME_LIMITS
from justflow.definitions.catalog import DefinitionCatalog
from justflow.definitions.manifest import (
    ENGINE_WORKFLOW_ABI,
    SHA256_HEX_LENGTH,
    DefinitionManifest,
    build_definition_manifests,
    workflow_type_name,
)
from justflow.definitions.routing import WorkerDeployment, WorkerDeploymentRouter
from justflow.definitions.runtime import PreparedDefinitions, prepare_definitions
from justflow.engine.activities import ActivityInput, StepActivityResult
from justflow.engine.compiler import compile_workflow
from justflow.provenance import WorkerArtifactIdentity
from justflow.scope import LOCAL_RUNTIME_SCOPE
from justflow.transports.builtins import builtin_transport_registry

REPLAY_DEPLOYMENT = WorkerDeployment(
    artifact_identity=WorkerArtifactIdentity(
        deployment_name="justflow-replay",
        build_id="baseline-1",
        artifact_digest=f"sha256:{'a' * 64}",
        package_version="0.1.0",
    ),
    compatible_engine_workflow_abis=frozenset({ENGINE_WORKFLOW_ABI}),
)
REPLAY_SERVICE_NAME = "replay"
REPLAY_ENVIRONMENT_SNAPSHOT_DIGEST = "e" * SHA256_HEX_LENGTH
REPLAY_SERVICE = ServiceConfig(
    transport="direct",
    transport_config={"class": "tests.replay_cases.ReplayActions"},
    connect_timeout_sec=None,
    dispatch_timeout_sec=10,
    response_timeout_sec=None,
    retries=0,
)
LEGACY_RUNTIME_LIMIT_FIELDS = frozenset(
    {
        "trigger_payload_bytes",
        "workflow_output_bytes",
        "activity_input_bytes",
        "activity_output_bytes",
        "failure_record_bytes",
        "audit_record_bytes",
        "cache_entry_bytes",
        "fanout_items",
        "parallelism",
        "loop_attempts",
        "total_invocations",
        "queued_messages",
        "signal_payload_bytes",
        "collection_items",
    }
)


class ReplayActions:
    """Importable declaration target; fixture generation uses ReplayActivities."""


class ReplayActivities:
    def __init__(self) -> None:
        self._polls = 0

    @activity.defn(name="execute_step")
    async def execute_step(self, activity_input: ActivityInput) -> dict[str, Any]:
        action = activity_input.action
        if action == "fail":
            raise ApplicationError("fixture failure", type="FIXTURE_FAILURE", non_retryable=True)
        if action == "batch":
            data: Any = {"items": [1, 2, 3]}
        elif action == "item":
            data = {"item": activity_input.globals["item"]}
        elif action == "ready":
            data = {"ready": True}
        elif action == "poll":
            self._polls += 1
            data = {"ready": self._polls >= 2}
        elif action == "recover":
            data = {"recovered": True}
        else:
            data = activity_input.input
        return asdict(StepActivityResult(data=data))


def _step(**values: Any) -> StepDefinition:
    return StepDefinition.model_validate(values)


def _flow(**values: Any) -> FlowStep:
    return FlowStep.model_validate(values)


def _branch(**values: Any) -> OnResultBranch:
    return OnResultBranch.model_validate(values)


def _wait(**values: Any) -> WaitForConfig:
    return WaitForConfig.model_validate(values)


def _workflow(**values: Any) -> WorkflowConfig:
    return WorkflowConfig.model_validate(values)


def replay_workflows() -> dict[str, WorkflowConfig]:
    def operation(action: str) -> StepDefinition:
        return _step(service=REPLAY_SERVICE_NAME, action=action)

    return {
        "replay_terminal": _workflow(
            workflow="replay_terminal",
            steps={},
            flow=[_flow(name="done", terminal=True)],
        ),
        "replay_activity": _workflow(
            workflow="replay_activity",
            steps={"echo": operation("echo")},
            flow=[
                _flow(name="echo", op="echo", then="done"),
                _flow(name="done", terminal=True),
            ],
        ),
        "replay_branching": _workflow(
            workflow="replay_branching",
            steps={"ready": operation("ready")},
            flow=[
                _flow(
                    name="ready",
                    op="ready",
                    on_result=[
                        _branch(when="input.ready == true", then="done"),
                        _branch(default="rejected"),
                    ],
                ),
                _flow(name="done", terminal=True),
                _flow(name="rejected", terminal=True, reason="not_ready"),
            ],
        ),
        "replay_parallel_fanout": _workflow(
            workflow="replay_parallel_fanout",
            steps={"generate": operation("batch"), "item": operation("item")},
            flow=[
                _flow(name="generate", op="generate", output="batch", then="items"),
                _flow(
                    name="items",
                    op="item",
                    input="generate.batch",
                    for_each="input.items",
                    as_var="item",
                    parallel=True,
                    max_concurrency=2,
                    then="done",
                ),
                _flow(name="done", terminal=True),
            ],
        ),
        "replay_until_loop": _workflow(
            workflow="replay_until_loop",
            steps={"poll": operation("poll")},
            flow=[
                _flow(
                    name="poll",
                    op="poll",
                    until="input.ready == true",
                    max_iterations=3,
                    interval_sec=1,
                    on_exhausted="exhausted",
                    then="done",
                ),
                _flow(name="done", terminal=True),
                _flow(name="exhausted", terminal=True, reason="exhausted"),
            ],
        ),
        "replay_durable_wait": _workflow(
            workflow="replay_durable_wait",
            steps={},
            flow=[
                _flow(
                    name="wait",
                    wait_for=_wait(
                        signal="approved",
                        timeout_sec=30,
                        on_timeout="timed_out",
                    ),
                    then="done",
                ),
                _flow(name="done", terminal=True),
                _flow(name="timed_out", terminal=True, reason="timed_out"),
            ],
        ),
        "replay_durable_sleep": _workflow(
            workflow="replay_durable_sleep",
            steps={},
            flow=[
                _flow(name="sleep", sleep_sec=1, then="done"),
                _flow(name="done", terminal=True),
            ],
        ),
        "replay_failure_handler": _workflow(
            workflow="replay_failure_handler",
            steps={"fail": operation("fail"), "recover": operation("recover")},
            flow=[
                _flow(name="fail", op="fail", on_failure="recover", then="done"),
                _flow(name="recover", op="recover", then="done"),
                _flow(name="done", terminal=True),
            ],
        ),
        "replay_child": _workflow(
            workflow="replay_child",
            steps={},
            flow=[_flow(name="done", terminal=True)],
        ),
        "replay_child_workflow": _workflow(
            workflow="replay_child_workflow",
            steps={"child": _step(workflow="replay_child")},
            flow=[
                _flow(name="child", op="child", then="done"),
                _flow(name="done", terminal=True),
            ],
        ),
    }


def prepare_replay_definitions() -> PreparedDefinitions:
    workflows = replay_workflows()
    services = builtin_transport_registry().resolve_services({REPLAY_SERVICE_NAME: REPLAY_SERVICE})
    manifests = build_definition_manifests(workflows, services, DEFAULT_RUNTIME_LIMITS)
    prepared = prepare_definitions(
        workflows,
        services,
        DEFAULT_RUNTIME_LIMITS,
        DefinitionCatalog.from_manifests(manifests),
        WorkerDeploymentRouter.for_deployment(REPLAY_DEPLOYMENT),
        {name: REPLAY_ENVIRONMENT_SNAPSHOT_DIGEST for name in workflows},
        runtime_scope=LOCAL_RUNTIME_SCOPE,
    )
    legacy_manifests = _legacy_replay_manifests(manifests)
    workflow_classes = dict(prepared.workflow_classes)
    for logical_name, manifest in legacy_manifests.items():
        children = {child.workflow: legacy_manifests[child.workflow] for child in manifest.children}
        workflow_class = compile_workflow(
            workflows[logical_name],
            services,
            limits=DEFAULT_RUNTIME_LIMITS,
            manifest=manifest,
            deployment=REPLAY_DEPLOYMENT,
            child_manifests=children,
            environment_snapshot_digest=REPLAY_ENVIRONMENT_SNAPSHOT_DIGEST,
            child_environment_snapshot_digests={
                child_name: REPLAY_ENVIRONMENT_SNAPSHOT_DIGEST for child_name in children
            },
            runtime_scope_digest=LOCAL_RUNTIME_SCOPE.digest,
        )
        workflow_classes[workflow_type_name(logical_name, manifest.definition_digest)] = (
            workflow_class
        )
    return PreparedDefinitions(
        manifests=prepared.manifests,
        workflow_classes=MappingProxyType(workflow_classes),
        start_targets=prepared.start_targets,
    )


def _legacy_replay_manifests(
    current: dict[str, DefinitionManifest],
) -> dict[str, DefinitionManifest]:
    legacy: dict[str, DefinitionManifest] = {}

    def build(logical_name: str) -> DefinitionManifest:
        if logical_name in legacy:
            return legacy[logical_name]
        content = current[logical_name].model_dump(mode="json", exclude={"definition_digest"})
        runtime_limits = content["deterministic_policy"]["runtime_limits"]
        content["deterministic_policy"]["runtime_limits"] = {
            name: value
            for name, value in runtime_limits.items()
            if name in LEGACY_RUNTIME_LIMIT_FIELDS
        }
        content["children"] = [
            {
                "workflow": child["workflow"],
                "definition_digest": build(child["workflow"]).definition_digest,
            }
            for child in content["children"]
        ]
        manifest = DefinitionManifest.create(**content)
        legacy[logical_name] = manifest
        return manifest

    for name in sorted(current):
        build(name)
    return legacy
