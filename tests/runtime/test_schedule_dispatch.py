"""Scheduled occurrence dispatch tests."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from temporalio.exceptions import ApplicationError

from justflow.config.models import FlowStep, WorkflowConfig
from justflow.config.runtime_limits import DEFAULT_RUNTIME_LIMITS
from justflow.definitions.catalog import DefinitionCatalog, DefinitionCatalogStore
from justflow.definitions.manifest import (
    ENGINE_WORKFLOW_ABI,
    SHA256_HEX_LENGTH,
    build_definition_manifests,
)
from justflow.definitions.routing import WorkerDeployment, WorkflowStartTarget
from justflow.provenance import ExecutionConfigurationIdentity, WorkerArtifactIdentity
from justflow.runtime.schedule_dispatch import (
    SCHEDULE_TARGET_UNAVAILABLE,
    ScheduleDispatchActivity,
)
from justflow.runtime.schedules import ScheduleDispatchPlan, ScheduleTargetIdentity
from justflow.runtime.starter import (
    ScheduleSourceIdentity,
    StartStatus,
    StartWorkflowResult,
    WorkflowStarter,
)

SCHEDULE_NAME = "daily_orders"
WORKFLOW_NAME = "record_flow"
OCCURRENCE_ID = "o" * SHA256_HEX_LENGTH
ENVIRONMENT_SNAPSHOT_DIGEST = "e" * SHA256_HEX_LENGTH
SOURCE_IDENTITY_DIGEST = "b" * SHA256_HEX_LENGTH
EXECUTION_CONFIGURATION = ExecutionConfigurationIdentity(
    configuration_revision_id="c" * SHA256_HEX_LENGTH,
    resolution_digest=f"sha256:{'d' * SHA256_HEX_LENGTH}",
)
SENSITIVE_INPUT = "synthetic-private-input"
WORKFLOW_CONFIG = WorkflowConfig(
    workflow=WORKFLOW_NAME,
    steps={},
    flow=[FlowStep(name="done", terminal=True)],
)
MANIFEST = build_definition_manifests(
    {WORKFLOW_NAME: WORKFLOW_CONFIG},
    {},
    DEFAULT_RUNTIME_LIMITS,
)[WORKFLOW_NAME]
ARTIFACT = WorkerArtifactIdentity(
    deployment_name="justflow",
    build_id="test-build",
    artifact_digest=f"sha256:{'a' * SHA256_HEX_LENGTH}",
    package_version="0.1.0",
)
DEPLOYMENT = WorkerDeployment(
    artifact_identity=ARTIFACT,
    compatible_engine_workflow_abis=frozenset({ENGINE_WORKFLOW_ABI}),
)
TARGET = WorkflowStartTarget(
    manifest=MANIFEST,
    deployment=DEPLOYMENT,
    environment_snapshot_digest=ENVIRONMENT_SNAPSHOT_DIGEST,
    execution_configuration=EXECUTION_CONFIGURATION,
)
PLAN = ScheduleDispatchPlan(
    schedule_name=SCHEDULE_NAME,
    target=ScheduleTargetIdentity.from_target(TARGET),
    input={"value": SENSITIVE_INPUT},
)


def dispatch_activity(
    *,
    snapshot_definition_digest: str = MANIFEST.definition_digest,
    snapshot_artifact: WorkerArtifactIdentity = ARTIFACT,
) -> tuple[ScheduleDispatchActivity, MagicMock, MagicMock]:
    catalog_store = MagicMock(spec=DefinitionCatalogStore)
    catalog_store.load_environment_snapshot.return_value = SimpleNamespace(
        definition_digest=snapshot_definition_digest,
        worker_artifact=snapshot_artifact,
        snapshot_digest=ENVIRONMENT_SNAPSHOT_DIGEST,
        execution_configuration=EXECUTION_CONFIGURATION,
    )
    starter = MagicMock(spec=WorkflowStarter)
    starter.start_resolved = AsyncMock(
        return_value=StartWorkflowResult(
            workflow_id="workflow-id",
            run_id="run-id",
            workflow_name=WORKFLOW_NAME,
            definition_digest=MANIFEST.definition_digest,
            artifact_identity=ARTIFACT,
            environment_snapshot_digest=ENVIRONMENT_SNAPSHOT_DIGEST,
            trigger_name=SCHEDULE_NAME,
            source_identity_digest=SOURCE_IDENTITY_DIGEST,
            status=StartStatus.STARTED,
        )
    )
    handler = ScheduleDispatchActivity(
        catalog=DefinitionCatalog(
            [MANIFEST],
            {WORKFLOW_NAME: MANIFEST.definition_digest},
        ),
        catalog_store=catalog_store,
        starter=starter,
    )
    return handler, starter, catalog_store


async def test_dispatch_uses_occurrence_identity_and_pinned_workflow_starter_target():
    handler, starter, catalog_store = dispatch_activity()

    result = await handler.start_scheduled_workflow(
        {
            "plan": PLAN.model_dump(mode="json"),
            "occurrence_id": OCCURRENCE_ID,
        }
    )

    request, target = starter.start_resolved.await_args.args
    assert starter.start_resolved.await_args.kwargs["trigger_name"] == SCHEDULE_NAME
    assert request.business_request_id == OCCURRENCE_ID
    assert request.definition_digest == MANIFEST.definition_digest
    assert request.input == {"value": SENSITIVE_INPUT}
    assert request.source == ScheduleSourceIdentity(
        schedule=SCHEDULE_NAME,
        occurrence_id=OCCURRENCE_ID,
    )
    assert target.manifest == MANIFEST
    assert target.deployment.artifact_identity == ARTIFACT
    assert target.environment_snapshot_digest == ENVIRONMENT_SNAPSHOT_DIGEST
    assert target.execution_configuration == EXECUTION_CONFIGURATION
    assert result["status"] == StartStatus.STARTED.value
    catalog_store.load_environment_snapshot.assert_called_once_with(ENVIRONMENT_SNAPSHOT_DIGEST)


@pytest.mark.parametrize(
    ("snapshot_definition_digest", "snapshot_artifact"),
    [
        pytest.param("f" * SHA256_HEX_LENGTH, ARTIFACT, id="definition-mismatch"),
        pytest.param(
            MANIFEST.definition_digest,
            ARTIFACT.model_copy(update={"build_id": "other-build"}),
            id="artifact-mismatch",
        ),
    ],
)
async def test_dispatch_rejects_inconsistent_immutable_target(
    snapshot_definition_digest: str,
    snapshot_artifact: WorkerArtifactIdentity,
):
    handler, starter, _ = dispatch_activity(
        snapshot_definition_digest=snapshot_definition_digest,
        snapshot_artifact=snapshot_artifact,
    )

    with pytest.raises(ApplicationError) as raised:
        await handler.start_scheduled_workflow(
            {
                "plan": PLAN.model_dump(mode="json"),
                "occurrence_id": OCCURRENCE_ID,
            }
        )

    assert raised.value.type == SCHEDULE_TARGET_UNAVAILABLE
    assert raised.value.non_retryable is True
    assert SENSITIVE_INPUT not in str(raised.value)
    starter.start_resolved.assert_not_awaited()
