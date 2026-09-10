"""Resolve authored schedules to immutable workflow and environment identities."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

from justflow.config.settings import Settings
from justflow.config.triggers import TriggersConfig
from justflow.definitions.catalog import DefinitionCatalog, DefinitionCatalogStore
from justflow.definitions.environment import (
    build_execution_environment_snapshot,
    sanitized_runtime_configuration,
)
from justflow.definitions.routing import WorkerDeploymentRouter, WorkflowStartTarget
from justflow.provenance import ExecutionConfigurationIdentity, ExecutionEnvironmentSnapshot
from justflow.runtime.schedules import (
    DesiredSchedule,
    ScheduleConfigurationError,
    compile_schedule,
)


@dataclass(frozen=True, kw_only=True)
class PreparedSchedules:
    desired: Mapping[str, DesiredSchedule]
    environment_snapshots: Mapping[str, ExecutionEnvironmentSnapshot]

    @property
    def definition_digests(self) -> frozenset[str]:
        return frozenset(schedule.target.definition_digest for schedule in self.desired.values())

    @property
    def worker_deployments(self) -> frozenset[tuple[str, str]]:
        return frozenset(
            (
                schedule.target.artifact_identity.deployment_name,
                schedule.target.artifact_identity.build_id,
            )
            for schedule in self.desired.values()
        )


def prepare_schedules(
    declarations: TriggersConfig,
    *,
    settings: Settings,
    catalog_store: DefinitionCatalogStore,
    router: WorkerDeploymentRouter,
    execution_configuration: ExecutionConfigurationIdentity | None = None,
    catalog: DefinitionCatalog | None = None,
) -> PreparedSchedules:
    if len(declarations.schedules) > settings.schedules.max_schedules:
        raise ValueError("Authored schedules exceed the configured schedule bound")
    selected_catalog = catalog or catalog_store.load()
    desired: dict[str, DesiredSchedule] = {}
    snapshots: dict[str, ExecutionEnvironmentSnapshot] = {}
    for schedule_name, declaration in sorted(declarations.schedules.items()):
        active_manifest = selected_catalog.resolve(declaration.workflow)
        manifest = active_manifest
        if declaration.definition_digest is not None:
            manifest = selected_catalog.get(declaration.workflow, declaration.definition_digest)
            if manifest.definition_digest != active_manifest.definition_digest:
                raise ScheduleConfigurationError(
                    f"Schedule '{schedule_name}' targets a retained definition without "
                    "an active worker target"
                )
        deployment = router.select(manifest)
        snapshot = build_execution_environment_snapshot(
            manifest=manifest,
            artifact_identity=deployment.artifact_identity,
            catalog_backend=catalog_store.backend_identity,
            runtime_profile=settings.runtime.profile,
            configuration=sanitized_runtime_configuration(
                temporal_namespace=settings.temporal.namespace,
                temporal_task_queue=settings.temporal.task_queue,
                payload_protection_mode=settings.temporal.payload_protection.mode,
                broker_providers={
                    name: broker.provider for name, broker in settings.brokers.items()
                },
                runtime_limits=manifest.deterministic_policy.runtime_limits,
            ),
            scope=settings.runtime.scope,
            execution_configuration=execution_configuration,
        )
        target = WorkflowStartTarget(
            manifest=manifest,
            deployment=deployment,
            environment_snapshot_digest=snapshot.snapshot_digest,
            scope_digest=settings.runtime.scope.digest,
            execution_configuration=execution_configuration,
        )
        compiled = compile_schedule(
            schedule_name,
            declaration,
            target,
            task_queue=settings.temporal.task_queue,
            dispatch_task_queue=settings.schedules.task_queue,
            scope=settings.runtime.scope,
            limits=settings.limits.snapshot(),
            dispatch_timeout_seconds=settings.schedules.dispatch_timeout_seconds,
            dispatch_attempts=settings.schedules.dispatch_attempts,
        )
        desired[compiled.schedule_id] = compiled
        snapshots[snapshot.snapshot_digest] = snapshot
    return PreparedSchedules(
        desired=MappingProxyType(desired),
        environment_snapshots=MappingProxyType(snapshots),
    )
