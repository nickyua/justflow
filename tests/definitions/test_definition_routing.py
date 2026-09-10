"""Tests for worker compatibility routing and guarded retirement."""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import TypeAlias
from unittest.mock import AsyncMock

import pytest
from temporalio.api.enums.v1 import TaskQueueType
from temporalio.service import RPCError, RPCStatusCode

from justflow.config.models import FlowStep, WorkflowConfig
from justflow.config.runtime_limits import RuntimeLimits
from justflow.definitions.catalog import DefinitionCatalog
from justflow.definitions.manifest import (
    ENGINE_WORKFLOW_ABI,
    SHA256_HEX_LENGTH,
    build_definition_manifests,
)
from justflow.definitions.routing import (
    DeploymentRoutingError,
    OpenExecutionDependency,
    RetirementBlocker,
    WorkerDeployment,
    WorkerDeploymentRouter,
    assess_definition_retirement,
    assess_worker_retirement,
    execution_memo,
    list_open_execution_dependencies,
    retry_pinned_workflow_start,
    wait_for_worker_deployment,
)
from justflow.provenance import WorkerArtifactIdentity

TEST_ARTIFACT_DIGEST = f"sha256:{'a' * 64}"
ENVIRONMENT_SNAPSHOT_DIGEST = "e" * SHA256_HEX_LENGTH


def _manifest():
    workflow = WorkflowConfig(
        workflow="example",
        steps={},
        flow=[FlowStep(name="done", terminal=True)],
    )
    return build_definition_manifests({"example": workflow}, {}, RuntimeLimits())["example"]


def _deployment(build_id: str = "build-1") -> WorkerDeployment:
    return WorkerDeployment(
        artifact_identity=WorkerArtifactIdentity(
            deployment_name="justflow",
            build_id=build_id,
            artifact_digest=TEST_ARTIFACT_DIGEST,
            package_version="0.1.0",
        ),
        compatible_engine_workflow_abis=frozenset({ENGINE_WORKFLOW_ABI}),
    )


def test_router_selects_active_compatible_deployment() -> None:
    deployment = _deployment()
    router = WorkerDeploymentRouter.for_deployment(deployment)

    assert router.select(_manifest()) == deployment


def test_router_rejects_incompatible_engine_workflow_abi() -> None:
    workflow = WorkflowConfig(
        workflow="future",
        steps={},
        flow=[FlowStep(name="done", terminal=True)],
    )
    manifest = build_definition_manifests(
        {"future": workflow},
        {},
        RuntimeLimits(),
        required_engine_workflow_abi="justflow.workflow.v2",
    )["future"]

    with pytest.raises(DeploymentRoutingError, match="does not support"):
        WorkerDeploymentRouter.for_deployment(_deployment()).select(manifest)


async def test_wait_for_deployment_retries_pending_registration() -> None:
    ready = SimpleNamespace(
        version_task_queues=[
            SimpleNamespace(
                name="workflows",
                type=TaskQueueType.TASK_QUEUE_TYPE_WORKFLOW,
            )
        ]
    )
    describe = AsyncMock(
        side_effect=[
            RPCError("not registered", RPCStatusCode.NOT_FOUND, b""),
            SimpleNamespace(version_task_queues=[]),
            ready,
        ]
    )
    client = SimpleNamespace(
        namespace="default",
        workflow_service=SimpleNamespace(describe_worker_deployment_version=describe),
    )

    await wait_for_worker_deployment(
        client,
        _deployment(),
        "workflows",
        attempts=3,
        interval_seconds=0,
    )

    assert describe.await_count == 3


@dataclass(frozen=True, kw_only=True)
class PinnedStartReturns:
    value: object


@dataclass(frozen=True, kw_only=True)
class PinnedStartRaises:
    exc: type[Exception]
    match: str


PinnedStartOutcome: TypeAlias = PinnedStartReturns | PinnedStartRaises
PINNED_VERSION_NOT_PRESENT = (
    "Pinned version 'justflow:build-1' is not present in task queue 'workflows' of type 'Workflow'"
)
OTHER_VERSION_NOT_PRESENT = (
    "Pinned version 'justflow:build-2' is not present in task queue 'workflows' of type 'Workflow'"
)
OTHER_QUEUE_NOT_PRESENT = (
    "Pinned version 'justflow:build-1' is not present in task queue 'other' of type 'Workflow'"
)


@dataclass(frozen=True, kw_only=True)
class PinnedStartRetryCase:
    id: str
    effects: tuple[object, ...]
    attempts: int
    expected_calls: int
    outcome: PinnedStartOutcome


PINNED_START_RETRY_CASES = [
    PinnedStartRetryCase(
        id="registration-converges",
        effects=(
            RPCError(PINNED_VERSION_NOT_PRESENT, RPCStatusCode.FAILED_PRECONDITION, b""),
            "workflow-handle",
        ),
        attempts=2,
        expected_calls=2,
        outcome=PinnedStartReturns(value="workflow-handle"),
    ),
    PinnedStartRetryCase(
        id="registration-remains-unavailable",
        effects=(
            RPCError(PINNED_VERSION_NOT_PRESENT, RPCStatusCode.FAILED_PRECONDITION, b""),
            RPCError(PINNED_VERSION_NOT_PRESENT, RPCStatusCode.FAILED_PRECONDITION, b""),
        ),
        attempts=2,
        expected_calls=2,
        outcome=PinnedStartRaises(exc=RPCError, match="Pinned version"),
    ),
    PinnedStartRetryCase(
        id="unrelated-failed-precondition",
        effects=(RPCError("namespace is unavailable", RPCStatusCode.FAILED_PRECONDITION, b""),),
        attempts=2,
        expected_calls=1,
        outcome=PinnedStartRaises(exc=RPCError, match="namespace is unavailable"),
    ),
    PinnedStartRetryCase(
        id="different-pinned-version",
        effects=(RPCError(OTHER_VERSION_NOT_PRESENT, RPCStatusCode.FAILED_PRECONDITION, b""),),
        attempts=2,
        expected_calls=1,
        outcome=PinnedStartRaises(exc=RPCError, match="build-2"),
    ),
    PinnedStartRetryCase(
        id="different-task-queue",
        effects=(RPCError(OTHER_QUEUE_NOT_PRESENT, RPCStatusCode.FAILED_PRECONDITION, b""),),
        attempts=2,
        expected_calls=1,
        outcome=PinnedStartRaises(exc=RPCError, match="other"),
    ),
    PinnedStartRetryCase(
        id="unrelated-rpc-failure",
        effects=(RPCError("unavailable", RPCStatusCode.UNAVAILABLE, b""),),
        attempts=2,
        expected_calls=1,
        outcome=PinnedStartRaises(exc=RPCError, match="unavailable"),
    ),
]


@pytest.mark.parametrize("case", PINNED_START_RETRY_CASES, ids=lambda case: case.id)
async def test_pinned_workflow_start_retry(case: PinnedStartRetryCase) -> None:
    start = AsyncMock(side_effect=case.effects)

    if isinstance(case.outcome, PinnedStartReturns):
        result = await retry_pinned_workflow_start(
            start,
            _deployment(),
            "workflows",
            attempts=case.attempts,
            interval_seconds=0,
        )
        assert result == case.outcome.value
    else:
        with pytest.raises(case.outcome.exc, match=case.outcome.match):
            await retry_pinned_workflow_start(
                start,
                _deployment(),
                "workflows",
                attempts=case.attempts,
                interval_seconds=0,
            )

    assert start.await_count == case.expected_calls


async def test_pinned_workflow_start_rejects_an_empty_retry_budget() -> None:
    start = AsyncMock()

    with pytest.raises(ValueError, match="attempts must be positive"):
        await retry_pinned_workflow_start(
            start,
            _deployment(),
            "workflows",
            attempts=0,
            interval_seconds=0,
        )

    start.assert_not_awaited()


async def test_open_execution_enumeration_reads_version_metadata() -> None:
    async def executions():
        yield SimpleNamespace(id="versioned", run_id="run-1")
        yield SimpleNamespace(id="unrelated", run_id="run-2")

    descriptions = {
        "versioned": {
            "justflow.logical_workflow": "example",
            "justflow.definition_digest": "a" * 64,
            "justflow.worker_deployment": "justflow",
            "justflow.worker_build_id": "build-1",
        },
        "unrelated": {},
    }

    def handle(workflow_id: str, *, run_id: str):
        description = SimpleNamespace(memo=AsyncMock(return_value=descriptions[workflow_id]))
        return SimpleNamespace(describe=AsyncMock(return_value=description))

    client = SimpleNamespace(
        list_workflows=lambda **_kwargs: executions(),
        get_workflow_handle=handle,
    )

    dependencies = await list_open_execution_dependencies(client)

    assert dependencies == (
        OpenExecutionDependency(
            workflow_id="versioned",
            run_id="run-1",
            logical_workflow="example",
            definition_digest="a" * 64,
            worker_deployment="justflow",
            worker_build_id="build-1",
        ),
    )


def test_execution_metadata_keeps_definition_and_worker_identity_separate() -> None:
    manifest = _manifest()
    deployment = _deployment()

    memo = execution_memo(manifest, deployment, ENVIRONMENT_SNAPSHOT_DIGEST)

    assert memo == {
        "justflow.logical_workflow": "example",
        "justflow.definition_digest": manifest.definition_digest,
        "justflow.worker_deployment": "justflow",
        "justflow.worker_build_id": "build-1",
        "justflow.worker_artifact_digest": TEST_ARTIFACT_DIGEST,
        "justflow.worker_package_version": "0.1.0",
        "justflow.environment_snapshot_digest": ENVIRONMENT_SNAPSHOT_DIGEST,
    }


def test_definition_retirement_reports_every_blocker() -> None:
    manifest = _manifest()
    catalog = DefinitionCatalog.from_manifests({"example": manifest})
    open_execution = OpenExecutionDependency(
        workflow_id="workflow-id",
        run_id="run-id",
        logical_workflow="example",
        definition_digest=manifest.definition_digest,
        worker_deployment="justflow",
        worker_build_id="build-1",
    )

    assessment = assess_definition_retirement(
        catalog,
        logical_name="example",
        definition_digest=manifest.definition_digest,
        open_executions=[open_execution],
        replay_fixture_digests=frozenset(),
        scheduled_definition_digests=[manifest.definition_digest],
        approved=False,
    )

    assert assessment.blockers == frozenset(
        {
            RetirementBlocker.APPROVAL_REQUIRED,
            RetirementBlocker.ACTIVE_ALIAS,
            RetirementBlocker.OPEN_EXECUTIONS,
            RetirementBlocker.REPLAY_FIXTURE_REQUIRED,
            RetirementBlocker.ACTIVE_SCHEDULES,
        }
    )


def test_definition_retirement_is_allowed_after_all_guards_pass() -> None:
    manifest = _manifest()
    catalog = DefinitionCatalog([manifest], {})

    assessment = assess_definition_retirement(
        catalog,
        logical_name="example",
        definition_digest=manifest.definition_digest,
        open_executions=[],
        replay_fixture_digests=frozenset({manifest.definition_digest}),
        approved=True,
    )

    assert assessment.allowed is True


def test_worker_retirement_reports_active_open_and_replay_blockers() -> None:
    deployment = _deployment()
    router = WorkerDeploymentRouter.for_deployment(deployment)
    open_execution = OpenExecutionDependency(
        workflow_id="workflow-id",
        run_id="run-id",
        logical_workflow="example",
        definition_digest=_manifest().definition_digest,
        worker_deployment=deployment.name,
        worker_build_id=deployment.build_id,
    )

    assessment = assess_worker_retirement(
        router,
        deployment,
        open_executions=[open_execution],
        replay_fixture_deployments=frozenset(),
        scheduled_worker_deployments=[(deployment.name, deployment.build_id)],
        approved=False,
    )

    assert assessment.blockers == frozenset(
        {
            RetirementBlocker.APPROVAL_REQUIRED,
            RetirementBlocker.ACTIVE_DEPLOYMENT,
            RetirementBlocker.OPEN_EXECUTIONS,
            RetirementBlocker.REPLAY_FIXTURE_REQUIRED,
            RetirementBlocker.ACTIVE_SCHEDULES,
        }
    )
