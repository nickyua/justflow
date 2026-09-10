"""Immutable schedule target preparation and host composition tests."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from temporalio.client import Client

from justflow.config.models import FlowStep, WorkflowConfig
from justflow.config.runtime_limits import DEFAULT_RUNTIME_LIMITS
from justflow.config.schedules import (
    IntervalScheduleSpec,
)
from justflow.config.settings import Settings
from justflow.config.triggers import ScheduleTriggerDeclaration, TriggersConfig
from justflow.definitions.catalog import CatalogStore
from justflow.definitions.manifest import ENGINE_WORKFLOW_ABI, build_definition_manifests
from justflow.definitions.routing import WorkerDeployment, WorkerDeploymentRouter
from justflow.provenance import WorkerArtifactIdentity
from justflow.runtime.schedule_application import ScheduleRuntime
from justflow.runtime.schedule_configuration import prepare_schedules
from justflow.runtime.schedule_reconciler import (
    ScheduleApplyStatus,
    ScheduleReconciliationError,
    ScheduleReconciliationErrorCode,
)
from justflow.runtime.schedules import (
    ScheduleConfigurationError,
    plan_schedule_reconciliation,
)
from justflow.runtime.temporal import TemporalConnectionPolicy
from tests.settings import PRODUCTION_RUNTIME

WORKFLOW_NAME = "scheduled_flow"
SCHEDULE_NAME = "daily_schedule"
TASK_QUEUE = "test-business-queue"
SCHEDULE_TASK_QUEUE = "test-schedule-queue"
ARTIFACT = WorkerArtifactIdentity(
    deployment_name="justflow",
    build_id="build-1",
    artifact_digest=f"sha256:{'a' * 64}",
    package_version="0.1.0",
)
DEPLOYMENT = WorkerDeployment(
    artifact_identity=ARTIFACT,
    compatible_engine_workflow_abis=frozenset({ENGINE_WORKFLOW_ABI}),
)


def manifest(description: str):
    workflow_config = WorkflowConfig(
        workflow=WORKFLOW_NAME,
        description=description,
        steps={},
        flow=[FlowStep(name="done", terminal=True)],
    )
    return build_definition_manifests(
        {WORKFLOW_NAME: workflow_config},
        {},
        DEFAULT_RUNTIME_LIMITS,
    )[WORKFLOW_NAME]


def settings(config_dir: str, **schedule_settings: object) -> Settings:
    return Settings.model_validate(
        {
            "runtime": PRODUCTION_RUNTIME,
            "paths": {"config_dir": config_dir},
            "temporal": {
                "address": "temporal.example:7233",
                "task_queue": TASK_QUEUE,
                "connection": {"mode": "tls", "server_name": "temporal.example"},
            },
            "schedules": {"task_queue": SCHEDULE_TASK_QUEUE, **schedule_settings},
            "deployment": {
                "name": ARTIFACT.deployment_name,
                "build_id": ARTIFACT.build_id,
                "artifact_digest": ARTIFACT.artifact_digest,
                "package_version": ARTIFACT.package_version,
            },
        }
    )


def declarations(*, definition_digest: str | None = None) -> TriggersConfig:
    return TriggersConfig(
        triggers={
            SCHEDULE_NAME: ScheduleTriggerDeclaration(
                workflow=WORKFLOW_NAME,
                definition_digest=definition_digest,
                spec=IntervalScheduleSpec(every_seconds=60),
            )
        }
    )


def test_prepare_schedules_pins_definition_deployment_and_environment_without_writing(tmp_path):
    authored_manifest = manifest("active")
    catalog_store = CatalogStore(tmp_path)
    catalog_store.publish({WORKFLOW_NAME: authored_manifest})

    prepared = prepare_schedules(
        declarations(definition_digest=authored_manifest.definition_digest),
        settings=settings(str(tmp_path)),
        catalog_store=catalog_store,
        router=WorkerDeploymentRouter.for_deployment(DEPLOYMENT),
    )

    desired = next(iter(prepared.desired.values()))
    assert desired.target.definition_digest == authored_manifest.definition_digest
    assert desired.target.artifact_identity == ARTIFACT
    assert prepared.definition_digests == frozenset({authored_manifest.definition_digest})
    assert prepared.worker_deployments == frozenset({("justflow", "build-1")})
    assert catalog_store.list_environment_snapshots() == ()


def test_prepare_schedules_rejects_retained_definition_without_active_worker_target(tmp_path):
    retained_manifest = manifest("retained")
    active_manifest = manifest("active")
    catalog_store = CatalogStore(tmp_path)
    catalog_store.publish({WORKFLOW_NAME: retained_manifest})
    catalog_store.publish({WORKFLOW_NAME: active_manifest})

    with pytest.raises(ScheduleConfigurationError, match="retained definition"):
        prepare_schedules(
            declarations(definition_digest=retained_manifest.definition_digest),
            settings=settings(str(tmp_path)),
            catalog_store=catalog_store,
            router=WorkerDeploymentRouter.for_deployment(DEPLOYMENT),
        )


def test_prepare_schedules_enforces_authored_collection_bound(tmp_path):
    catalog_store = CatalogStore(tmp_path)

    with pytest.raises(ValueError, match="configured schedule bound"):
        prepare_schedules(
            TriggersConfig(
                triggers={
                    "first": ScheduleTriggerDeclaration(
                        workflow=WORKFLOW_NAME,
                        spec=IntervalScheduleSpec(every_seconds=60),
                    ),
                    "second": ScheduleTriggerDeclaration(
                        workflow=WORKFLOW_NAME,
                        spec=IntervalScheduleSpec(every_seconds=60),
                    ),
                }
            ),
            settings=settings(str(tmp_path), max_schedules=1),
            catalog_store=catalog_store,
            router=WorkerDeploymentRouter.for_deployment(DEPLOYMENT),
        )


async def test_schedule_runtime_waits_for_target_worker_and_stores_snapshot_only_on_apply(
    tmp_path,
):
    authored_manifest = manifest("active")
    catalog_store = CatalogStore(tmp_path)
    catalog_store.publish({WORKFLOW_NAME: authored_manifest})
    (tmp_path / "triggers.yaml").write_text(
        f"""
triggers:
  {SCHEDULE_NAME}:
    kind: schedule
    workflow: {WORKFLOW_NAME}
    definition_digest: {authored_manifest.definition_digest}
    spec:
      kind: interval
      every_seconds: 60
"""
    )
    client = MagicMock(spec=Client)
    client.create_schedule = AsyncMock()
    policy = MagicMock(spec=TemporalConnectionPolicy)
    policy.connect = AsyncMock(return_value=client)

    with patch(
        "justflow.runtime.schedule_application.wait_for_worker_deployment",
        new=AsyncMock(),
    ) as wait_for_worker:
        runtime = await ScheduleRuntime.create(
            settings(str(tmp_path)),
            temporal_connection_policy=policy,
            definition_catalog_store=catalog_store,
        )

    wait_for_worker.assert_awaited_once_with(
        client,
        DEPLOYMENT,
        TASK_QUEUE,
        attempts=50,
        interval_seconds=0.1,
    )
    assert catalog_store.list_environment_snapshots() == ()
    plan = plan_schedule_reconciliation(dict(runtime.prepared.desired), {})

    result = await runtime.apply(plan, confirmation=plan.plan_digest)

    assert result.items[0].status is ScheduleApplyStatus.APPLIED
    assert len(catalog_store.list_environment_snapshots()) == 1
    client.create_schedule.assert_awaited_once()


async def test_schedule_runtime_rejects_confirmation_before_storing_snapshot(tmp_path):
    authored_manifest = manifest("active")
    catalog_store = CatalogStore(tmp_path)
    catalog_store.publish({WORKFLOW_NAME: authored_manifest})
    (tmp_path / "triggers.yaml").write_text(
        f"""
triggers:
  {SCHEDULE_NAME}:
    kind: schedule
    workflow: {WORKFLOW_NAME}
    spec:
      kind: interval
      every_seconds: 60
"""
    )
    client = MagicMock(spec=Client)
    policy = MagicMock(spec=TemporalConnectionPolicy)
    policy.connect = AsyncMock(return_value=client)

    with patch(
        "justflow.runtime.schedule_application.wait_for_worker_deployment",
        new=AsyncMock(),
    ):
        runtime = await ScheduleRuntime.create(
            settings(str(tmp_path)),
            temporal_connection_policy=policy,
            definition_catalog_store=catalog_store,
        )

    plan = plan_schedule_reconciliation(dict(runtime.prepared.desired), {})
    with pytest.raises(ScheduleReconciliationError) as raised:
        await runtime.apply(plan, confirmation="wrong-plan")

    assert raised.value.code is ScheduleReconciliationErrorCode.CONFIRMATION_REQUIRED
    assert catalog_store.list_environment_snapshots() == ()
