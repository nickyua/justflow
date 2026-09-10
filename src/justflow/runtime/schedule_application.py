"""Host-composed scheduling client using one resolved Temporal connection policy."""

from __future__ import annotations

from temporalio.client import Client

from justflow.config.settings import Settings
from justflow.configuration import ConfigurationSource, configured_configuration_source
from justflow.definitions.catalog import DefinitionCatalogStore
from justflow.definitions.configuration import configured_catalog_store
from justflow.definitions.routing import WorkerDeploymentRouter, wait_for_worker_deployment
from justflow.engine.payload_protection import (
    PayloadProtectionBinding,
    configured_data_converter,
)
from justflow.engine.worker import configured_worker_deployment
from justflow.runtime.metrics import MetricsRegistry
from justflow.runtime.schedule_configuration import PreparedSchedules, prepare_schedules
from justflow.runtime.schedule_operations import ScheduleOperator
from justflow.runtime.schedule_reconciler import (
    ScheduleApplyResult,
    SchedulePlan,
    ScheduleReconciler,
    ScheduleReconciliationError,
    ScheduleReconciliationErrorCode,
)
from justflow.runtime.schedules import UnscopedScheduleDecision
from justflow.runtime.temporal import TemporalConnectionPolicy


class ScheduleRuntime:
    def __init__(
        self,
        *,
        client: Client,
        catalog_store: DefinitionCatalogStore,
        prepared: PreparedSchedules,
        reconciler: ScheduleReconciler,
        operator: ScheduleOperator,
    ) -> None:
        self.client = client
        self.catalog_store = catalog_store
        self.prepared = prepared
        self.reconciler = reconciler
        self.operator = operator

    @classmethod
    async def create(
        cls,
        settings: Settings,
        *,
        temporal_connection_policy: TemporalConnectionPolicy,
        payload_protection: PayloadProtectionBinding | None = None,
        definition_catalog_store: DefinitionCatalogStore | None = None,
        configuration_source: ConfigurationSource | None = None,
        metrics: MetricsRegistry | None = None,
    ) -> ScheduleRuntime:
        source = configuration_source or configured_configuration_source(
            settings.configuration,
            scope=settings.runtime.scope,
            config_dir=settings.paths.config_dir,
        )
        configuration_snapshot = source.read_triggers(settings.runtime.scope)
        declarations = configuration_snapshot.bundle.triggers
        catalog_store = definition_catalog_store or configured_catalog_store(
            settings.catalog,
            settings.paths.definition_catalog_dir,
            scope=settings.runtime.scope,
        )
        deployment = configured_worker_deployment(settings)
        router = WorkerDeploymentRouter.for_deployment(deployment)
        prepared = prepare_schedules(
            declarations,
            settings=settings,
            catalog_store=catalog_store,
            router=router,
            execution_configuration=configuration_snapshot.execution_identity,
        )
        data_converter = configured_data_converter(
            settings.temporal.payload_protection,
            payload_protection,
        )
        client = await temporal_connection_policy.connect(data_converter)
        await wait_for_worker_deployment(
            client,
            deployment,
            settings.temporal.task_queue,
            attempts=settings.temporal.deployment_registration.attempts,
            interval_seconds=settings.temporal.deployment_registration.interval_seconds,
        )
        return cls(
            client=client,
            catalog_store=catalog_store,
            prepared=prepared,
            reconciler=ScheduleReconciler(
                client,
                settings.schedules,
                scope=settings.runtime.scope,
                metrics=metrics,
            ),
            operator=ScheduleOperator(
                client,
                declarations,
                settings.schedules,
                scope=settings.runtime.scope,
                metrics=metrics,
            ),
        )

    async def plan(
        self,
        *,
        unscoped_decision: UnscopedScheduleDecision = (UnscopedScheduleDecision.REQUIRE_EXPLICIT),
    ) -> SchedulePlan:
        return await self.reconciler.plan(
            dict(self.prepared.desired),
            unscoped_decision=unscoped_decision,
        )

    async def apply(
        self,
        plan: SchedulePlan,
        *,
        confirmation: str,
    ) -> ScheduleApplyResult:
        self.reconciler.validate_apply(plan, confirmation=confirmation)
        try:
            for snapshot in self.prepared.environment_snapshots.values():
                self.catalog_store.store_environment_snapshot(snapshot)
        except Exception as exc:
            raise ScheduleReconciliationError(
                ScheduleReconciliationErrorCode.CATALOG_UNAVAILABLE,
                "Schedule environment snapshots could not be retained",
            ) from exc
        return await self.reconciler.apply(plan, confirmation=confirmation)
