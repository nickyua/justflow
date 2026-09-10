"""Temporal worker - hosts compiled workflows and activities."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping
from contextlib import AsyncExitStack
from dataclasses import dataclass
from typing import Any

from temporalio.client import Client
from temporalio.worker import Worker

from justflow.brokers import BrokerRegistry, ConfiguredBroker, MessagePublisher
from justflow.brokers.sqs import builtin_broker_registry
from justflow.config.models import ApprovedFullAuditCapture
from justflow.config.settings import Settings
from justflow.config.validator import ConfigValidator
from justflow.configuration import (
    ConfigurationSource,
    FileConfigurationSource,
    configured_configuration_source,
)
from justflow.definitions.catalog import DefinitionCatalogStore
from justflow.definitions.configuration import configured_catalog_store
from justflow.definitions.environment import build_execution_environment_snapshots
from justflow.definitions.manifest import ENGINE_WORKFLOW_ABI
from justflow.definitions.routing import (
    DefinitionStartTarget,
    WorkerDeployment,
    WorkerDeploymentRouter,
    wait_for_worker_deployment,
    worker_deployment_config,
)
from justflow.definitions.runtime import prepare_definitions
from justflow.engine.activities import WorkflowActivities
from justflow.engine.archival import ArchivalActivity
from justflow.engine.deduplication import BoundedDeduplicationStore
from justflow.engine.payload_protection import (
    PayloadProtectionBinding,
    PayloadProtectionError,
    configured_data_converter,
)
from justflow.engine.response_relay import ResponseRelay
from justflow.engine.sandbox import workflow_sandbox_runner
from justflow.engine.trigger_ingress import TriggerIngress
from justflow.provenance import ProvenanceError, WorkerArtifactIdentity
from justflow.resources.base import ResourceFactoryContext
from justflow.resources.builtins import builtin_resource_registry
from justflow.resources.registry import ResourceRegistry
from justflow.runtime.health import HealthComponent, HealthReason, HealthRegistry
from justflow.runtime.metrics import MetricsRegistry
from justflow.runtime.schedule_dispatch import (
    ScheduleDispatchActivity,
    ScheduleDispatchWorkflow,
)
from justflow.runtime.scheduled_start_cleanup import (
    ScheduledStartCleanupActivity,
    ScheduledStartCleanupWorkflow,
    ScheduledStartMaintenance,
)
from justflow.runtime.scheduled_start_dispatch import (
    ScheduledStartArbiterWorkflow,
    ScheduledStartDispatchActivities,
    ScheduledStartDueWorkflow,
)
from justflow.runtime.scheduled_start_service import (
    ScheduledStartService,
    ScheduledStartWorkloadPolicy,
)
from justflow.runtime.starter import WorkflowStarter, WorkflowTargetResolver
from justflow.runtime.temporal import (
    TemporalConnectionBinding,
    TemporalConnectionPolicy,
    resolve_temporal_connection,
)
from justflow.sdk.resource_loader import ResourceLoader
from justflow.transports.builtins import builtin_transport_registry
from justflow.transports.registry import ResolvedService, TransportRegistry

logger = logging.getLogger(__name__)


@dataclass(frozen=True, kw_only=True)
class EngineCleanupFailure:
    component: str
    cause: Exception


class EngineCleanupError(Exception):
    def __init__(self, failures: tuple[EngineCleanupFailure, ...]) -> None:
        self.failures = failures
        details = "; ".join(f"{failure.component}: {failure.cause}" for failure in failures)
        super().__init__(f"Engine cleanup failed: {details}")


class BackgroundServiceStoppedError(RuntimeError):
    pass


class WorkflowEngine:
    """Main engine that loads configs, compiles workflows, and runs the Temporal worker."""

    def __init__(
        self,
        settings: Settings,
        transport_registry: TransportRegistry | None = None,
        broker_registry: BrokerRegistry | None = None,
        definition_catalog_store: DefinitionCatalogStore | None = None,
        configuration_source: ConfigurationSource | None = None,
        deployment_router: WorkerDeploymentRouter | None = None,
        payload_protection: PayloadProtectionBinding | None = None,
        resource_registry: ResourceRegistry | None = None,
        resource_context: ResourceFactoryContext | None = None,
        temporal_connection_policy: TemporalConnectionPolicy | None = None,
        temporal_connection_binding: TemporalConnectionBinding | None = None,
        health_registry: HealthRegistry | None = None,
        metrics_registry: MetricsRegistry | None = None,
        workflow_target_resolver: WorkflowTargetResolver | None = None,
        scheduled_start_workload_policy: ScheduledStartWorkloadPolicy | None = None,
        enable_ingress: bool = True,
    ):
        if temporal_connection_policy is not None and temporal_connection_binding is not None:
            raise ValueError("Provide a resolved Temporal connection policy or a binding, not both")
        self.settings = settings
        self._metrics = metrics_registry or MetricsRegistry()
        self._workflow_target_resolver = workflow_target_resolver
        self._scheduled_start_workload_policy = scheduled_start_workload_policy
        required_health = {
            HealthComponent.CATALOG,
            HealthComponent.TEMPORAL,
            HealthComponent.WORKER,
            HealthComponent.PROVIDERS,
        }
        self._enable_ingress = enable_ingress
        if enable_ingress and settings.messaging.trigger is not None:
            required_health.add(HealthComponent.TRIGGER_CONSUMER)
        if enable_ingress and settings.messaging.response is not None:
            required_health.add(HealthComponent.RESPONSE_CONSUMER)
        self._health = health_registry or HealthRegistry(
            frozenset(required_health),
            metrics=self._metrics,
        )
        self._transport_registry = transport_registry or builtin_transport_registry()
        self._broker_registry = broker_registry or builtin_broker_registry()
        self._definition_catalog_store = definition_catalog_store
        self._configuration_source = configuration_source
        self._deployment_router = deployment_router
        self._payload_protection = payload_protection
        self._resource_registry = resource_registry or builtin_resource_registry()
        resolved_resource_context = resource_context or ResourceFactoryContext(
            postgres_dsns=settings.resource_connections.postgres_dsns,
            redis_urls=settings.resource_connections.redis_urls,
        )
        self._data_converter = configured_data_converter(
            settings.temporal.payload_protection,
            payload_protection,
        )
        self._temporal_connection_policy = (
            temporal_connection_policy
            or resolve_temporal_connection(
                settings.temporal,
                settings.runtime.profile,
                temporal_connection_binding,
            )
        )

        self._client: Client | None = None
        self._worker: Worker | None = None
        self._worker_task: asyncio.Task[None] | None = None
        self._schedule_worker: Worker | None = None
        self._schedule_worker_task: asyncio.Task[None] | None = None
        self._brokers: dict[str, ConfiguredBroker] = {}
        self._response_relay: ResponseRelay | None = None
        self._response_relay_task: asyncio.Task[None] | None = None
        self._trigger_ingress: TriggerIngress | None = None
        self._trigger_ingress_task: asyncio.Task[None] | None = None
        self._resource_loader = ResourceLoader(
            self._resource_registry,
            resolved_resource_context,
        )
        self._compiled_workflows: dict[str, type] = {}
        self._start_targets: dict[str, DefinitionStartTarget] = {}
        self._workflow_starter: WorkflowStarter | None = None
        self._limits = settings.limits.snapshot()
        self._lifecycle = AsyncExitStack()
        self._cleanup_failures: list[EngineCleanupFailure] = []
        self._owned_cleanups: set[str] = set()
        self._stop_lock = asyncio.Lock()
        self._stopped = False
        self._ready = asyncio.Event()
        self._own_cleanup("resources", self._resource_loader.close)

    async def start(self) -> None:
        """Load configs, validate, compile workflows, and start the worker."""
        failure: BaseException | None = None
        try:
            source = self._configuration_source or configured_configuration_source(
                self.settings.configuration,
                scope=self.settings.runtime.scope,
                config_dir=self.settings.paths.config_dir,
            )
            configuration_snapshot = source.read(self.settings.runtime.scope)
            bundle = configuration_snapshot.bundle
            resources_config = bundle.resources
            services_config = bundle.services
            workflow_configs = bundle.workflows
            if self._payload_protection is None and any(
                workflow_config.on_complete is not None
                and isinstance(
                    workflow_config.on_complete.capture,
                    ApprovedFullAuditCapture,
                )
                for workflow_config in workflow_configs.values()
            ):
                raise PayloadProtectionError(
                    "Approved-full audit capture requires payload encryption"
                )

            validator = ConfigValidator(
                resources_config,
                services_config,
                workflow_configs,
                transport_registry=self._transport_registry,
                resource_registry=self._resource_registry,
                limits=self._limits,
                config_dir=self.settings.paths.config_dir,
                workflow_sources=(
                    source.workflow_sources if isinstance(source, FileConfigurationSource) else {}
                ),
                triggers=bundle.triggers,
            )
            validator.validate().raise_if_invalid()
            resolved_resources = dict(validator.resolved_resources)
            resolved_services = dict(validator.resolved_services)
            logger.info(f"Config validation passed: {len(workflow_configs)} workflows loaded")

            catalog_store = self._definition_catalog_store or configured_catalog_store(
                self.settings.catalog,
                self.settings.paths.definition_catalog_dir,
                scope=self.settings.runtime.scope,
            )
            catalog = catalog_store.load()
            self._health.mark_ready(HealthComponent.CATALOG)
            router = self._deployment_router or WorkerDeploymentRouter.for_deployment(
                self._configured_deployment()
            )
            environment_snapshots = build_execution_environment_snapshots(
                self.settings,
                catalog,
                router,
                catalog_store.backend_identity,
                configuration_snapshot.execution_identity,
            )
            prepared = prepare_definitions(
                workflow_configs,
                resolved_services,
                self._limits,
                catalog,
                router,
                {
                    name: snapshot.snapshot_digest
                    for name, snapshot in environment_snapshots.items()
                },
                resources=resolved_resources,
                runtime_scope=self.settings.runtime.scope,
                execution_configuration=configuration_snapshot.execution_identity,
            )
            for snapshot in environment_snapshots.values():
                catalog_store.store_environment_snapshot(snapshot)
            self._compiled_workflows.update(prepared.workflow_classes)
            self._start_targets.update(prepared.start_targets)

            await self._resource_loader.load(resolved_resources)
            self._health.mark_ready(HealthComponent.PROVIDERS)
            logger.info(f"Resources loaded: {list(self._resource_loader.resources.keys())}")
            self._configure_brokers()
            reply_destination = self._queue_reply_destination(resolved_services)
            configured_services = self._transport_registry.configure_services(
                resolved_services,
                resources=self._resource_loader.resources,
                message_publishers=self._message_publishers(),
                reply_destination=reply_destination,
                security=self.settings.transport_security,
            )

            for logical_name, target in self._start_targets.items():
                logger.info(
                    "Compiled immutable workflow definition",
                    extra={
                        "workflow": logical_name,
                        "definition_digest": target.manifest.definition_digest,
                        "worker_deployment": target.deployment.name,
                        "worker_build_id": target.deployment.build_id,
                        "environment_snapshot_digest": target.environment_snapshot_digest,
                    },
                )

            activities_handler = WorkflowActivities(
                services=configured_services,
                resources=self._resource_loader.resources,
                limits=self._limits,
                metrics=self._metrics,
            )
            self._own_cleanup("activities", activities_handler.close)
            archival_handler = ArchivalActivity(
                resources=self._resource_loader.resources,
                limits=self._limits,
                payload_protection=self._payload_protection,
            )

            self._client = await self._temporal_connection_policy.connect(self._data_converter)
            self._health.mark_ready(HealthComponent.TEMPORAL)
            self._workflow_starter = WorkflowStarter(
                self._client,
                self.settings.temporal.task_queue,
                self._start_targets if self._workflow_target_resolver is None else None,
                triggers=(bundle.triggers if self._workflow_target_resolver is None else None),
                target_resolver=self._workflow_target_resolver,
                scope=self.settings.runtime.scope,
                limits=self._limits,
                metrics=self._metrics,
                pinned_start_retry=self.settings.runtime.pinned_start_retry,
            )
            schedule_dispatch = ScheduleDispatchActivity(
                catalog=catalog,
                catalog_store=catalog_store,
                starter=self._workflow_starter,
                scope=self.settings.runtime.scope,
                limits=self._limits,
            )
            scheduled_start_service = ScheduledStartService(
                self._client,
                self._workflow_starter,
                self.settings.scheduled_starts,
                task_queue=self.settings.schedules.task_queue,
                workload_policy=self._scheduled_start_workload_policy,
                metrics=self._metrics,
            )
            scheduled_start_dispatch = ScheduledStartDispatchActivities(
                scheduled_start_service,
                scope=self.settings.runtime.scope,
                limits=self._limits,
            )
            scheduled_start_cleanup = ScheduledStartCleanupActivity(
                self._client,
                self.settings.scheduled_starts,
            )
            scheduled_start_maintenance = ScheduledStartMaintenance(
                self._client,
                self.settings.scheduled_starts,
                task_queue=self.settings.schedules.task_queue,
            )

            self._worker = Worker(
                self._client,
                task_queue=self.settings.temporal.task_queue,
                workflows=list(self._compiled_workflows.values()),
                activities=[
                    activities_handler.execute_step,
                    activities_handler.evaluate_condition,
                    activities_handler.validate_contract,
                    archival_handler.archive_workflow,
                ],
                workflow_runner=workflow_sandbox_runner(),
                deployment_config=worker_deployment_config(router.active),
            )
            self._own_cleanup("temporal-worker", self._worker.shutdown)
            self._schedule_worker = Worker(
                self._client,
                task_queue=self.settings.schedules.task_queue,
                workflows=[
                    ScheduleDispatchWorkflow,
                    ScheduledStartArbiterWorkflow,
                    ScheduledStartDueWorkflow,
                    ScheduledStartCleanupWorkflow,
                ],
                activities=[
                    schedule_dispatch.start_scheduled_workflow,
                    scheduled_start_dispatch.claim,
                    scheduled_start_dispatch.prepare,
                    scheduled_start_dispatch.attempt,
                    scheduled_start_dispatch.commit,
                    scheduled_start_cleanup.cleanup,
                ],
                workflow_runner=workflow_sandbox_runner(),
            )
            self._own_cleanup("schedule-worker", self._schedule_worker.shutdown)

            self._worker_task = asyncio.create_task(
                self._worker.run(),
                name="temporal-worker",
            )
            self._schedule_worker_task = asyncio.create_task(
                self._schedule_worker.run(),
                name="schedule-worker",
            )
            await scheduled_start_maintenance.ensure_schedule()
            await wait_for_worker_deployment(
                self._client,
                router.active,
                self.settings.temporal.task_queue,
                attempts=self.settings.temporal.deployment_registration.attempts,
                interval_seconds=(self.settings.temporal.deployment_registration.interval_seconds),
            )
            self._health.mark_ready(HealthComponent.WORKER)
            if self._enable_ingress:
                self._start_response_relay()
                self._start_trigger_ingress()
                await self._wait_for_consumer_readiness()
            self._ready.set()

            logger.info(
                f"Starting Temporal worker on task queue '{self.settings.temporal.task_queue}'"
            )
            await self._supervise_services()
        except BaseException as exc:
            failure = exc
            raise
        finally:
            try:
                await self.stop()
            except Exception as cleanup_exc:
                if failure is None:
                    raise
                logger.error(
                    "Engine cleanup failed while handling another failure",
                    extra={"exception_type": type(cleanup_exc).__name__},
                )
                failure.add_note(f"Engine cleanup failed with {type(cleanup_exc).__name__}")

    def _start_response_relay(self) -> None:
        endpoint = self.settings.messaging.response
        if endpoint is None:
            logger.info("No response endpoint configured; response relay disabled")
            return
        client = self._require_client()
        broker = self._configured_broker(endpoint.broker)
        consumer = broker.consumer(
            endpoint.destination,
            dead_letter_destination=endpoint.dead_letter_destination,
        )
        self._response_relay = ResponseRelay(
            temporal_client=client,
            consumer=consumer,
            deduplication_store=BoundedDeduplicationStore(
                capacity=self.settings.messaging.deduplication_capacity,
                retention_seconds=self.settings.messaging.deduplication_retention_seconds,
            ),
            message_concurrency=self.settings.messaging.message_concurrency,
            limits=self._limits,
            metrics=self._metrics,
            scope=self.settings.runtime.scope,
            readiness_callback=lambda ready: self._set_consumer_health(
                HealthComponent.RESPONSE_CONSUMER,
                ready,
            ),
            reconnect_policy=self.settings.messaging.reconnect,
        )
        self._own_cleanup("response-relay", self._response_relay.stop)
        self._response_relay_task = asyncio.create_task(
            self._response_relay.start(), name="response-relay"
        )
        logger.info(
            "Response relay started",
            extra={"broker": endpoint.broker, "destination": endpoint.destination},
        )

    def _start_trigger_ingress(self) -> None:
        endpoint = self.settings.messaging.trigger
        if endpoint is None:
            logger.info("No trigger endpoint configured; trigger ingress disabled")
            return
        starter = self._require_workflow_starter()
        broker = self._configured_broker(endpoint.broker)
        consumer = broker.consumer(
            endpoint.destination,
            dead_letter_destination=endpoint.dead_letter_destination,
        )
        self._trigger_ingress = TriggerIngress(
            workflow_starter=starter,
            consumer=consumer,
            source_name=endpoint.broker,
            scope=self.settings.runtime.scope,
            message_concurrency=self.settings.messaging.message_concurrency,
            limits=self._limits,
            metrics=self._metrics,
            readiness_callback=lambda ready: self._set_consumer_health(
                HealthComponent.TRIGGER_CONSUMER,
                ready,
            ),
            reconnect_policy=self.settings.messaging.reconnect,
        )
        self._own_cleanup("trigger-ingress", self._trigger_ingress.stop)
        self._trigger_ingress_task = asyncio.create_task(
            self._trigger_ingress.start(), name="trigger-ingress"
        )
        logger.info(
            "Trigger ingress started",
            extra={"broker": endpoint.broker, "destination": endpoint.destination},
        )

    async def stop(self) -> None:
        """Gracefully shut down the engine."""
        async with self._stop_lock:
            if self._stopped:
                return
            self._stopped = True
            self._ready.clear()
            self._ensure_cleanup_registration()
            await self._lifecycle.aclose()

            for task in (self._trigger_ingress_task, self._response_relay_task):
                if task is None:
                    continue
                if not task.done():
                    task.cancel()
            service_tasks = [
                task
                for task in (self._trigger_ingress_task, self._response_relay_task)
                if task is not None
            ]
            if service_tasks:
                await asyncio.gather(*service_tasks, return_exceptions=True)

            for component in HealthComponent:
                self._health.mark_unavailable(component, HealthReason.STOPPED)

            logger.info("Workflow engine stopped")
            if self._cleanup_failures:
                raise EngineCleanupError(tuple(self._cleanup_failures))

    async def wait_ready(self, timeout_seconds: float) -> None:
        try:
            await asyncio.wait_for(self._ready.wait(), timeout=timeout_seconds)
        except TimeoutError:
            raise RuntimeError(
                "Workflow engine did not become ready within its startup bound"
            ) from None

    async def _wait_for_consumer_readiness(self) -> None:
        timeout = self.settings.messaging.consumer_ready_timeout_seconds
        waits = []
        if self._response_relay is not None:
            waits.append(self._response_relay.wait_ready(timeout))
        if self._trigger_ingress is not None:
            waits.append(self._trigger_ingress.wait_ready(timeout))
        if waits:
            await asyncio.gather(*waits)

    def _set_consumer_health(self, component: HealthComponent, ready: bool) -> None:
        if ready:
            self._health.mark_ready(component)
        else:
            self._health.mark_unavailable(component)

    async def _supervise_services(self) -> None:
        worker = self._worker
        if worker is None:
            raise RuntimeError("Temporal worker is not configured")
        schedule_worker = self._schedule_worker
        if schedule_worker is None:
            raise RuntimeError("Temporal schedule worker is not configured")

        worker_task = self._worker_task
        if worker_task is None:
            worker_task = asyncio.create_task(worker.run(), name="temporal-worker")
            self._worker_task = worker_task
        schedule_worker_task = self._schedule_worker_task
        if schedule_worker_task is None:
            schedule_worker_task = asyncio.create_task(
                schedule_worker.run(),
                name="schedule-worker",
            )
            self._schedule_worker_task = schedule_worker_task
        tasks = {
            task
            for task in (
                worker_task,
                schedule_worker_task,
                self._response_relay_task,
                self._trigger_ingress_task,
            )
            if task is not None
        }
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                if task.cancelled():
                    raise BackgroundServiceStoppedError(
                        f"Background service was cancelled: {task.get_name()}"
                    )
                exception = task.exception()
                if exception is not None:
                    raise exception
            if worker_task not in done:
                stopped = sorted(task.get_name() for task in done)
                raise BackgroundServiceStoppedError(
                    f"Background service stopped unexpectedly: {stopped}"
                )
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    def _require_client(self) -> Client:
        if self._client is None:
            raise RuntimeError("Temporal client is not connected")
        return self._client

    def _require_workflow_starter(self) -> WorkflowStarter:
        if self._workflow_starter is None:
            raise RuntimeError("Workflow starter is not configured")
        return self._workflow_starter

    def _configure_brokers(self) -> None:
        for name, declaration in self.settings.brokers.items():
            broker = self._broker_registry.configure(
                declaration.provider,
                declaration.config,
            )
            self._brokers[name] = broker
            self._own_cleanup(f"broker:{name}", broker.close)
        response_endpoint = self.settings.messaging.response
        if response_endpoint is None:
            return
        response_broker = self._configured_broker(response_endpoint.broker)
        retention_seconds = self.settings.messaging.deduplication_retention_seconds
        redelivery_window = response_broker.delivery_policy.max_redelivery_window_seconds
        if retention_seconds <= redelivery_window:
            raise ValueError(
                "Response deduplication retention must exceed the broker redelivery window"
            )

    def _message_publishers(self) -> dict[str, MessagePublisher]:
        return {name: broker.publisher for name, broker in self._brokers.items()}

    def _queue_reply_destination(
        self,
        services: Mapping[str, ResolvedService],
    ) -> str | None:
        queue_services = [
            service for service in services.values() if service.provider_name == "queue"
        ]
        response_endpoint = self.settings.messaging.response
        if queue_services and response_endpoint is None:
            raise ValueError(
                "A response relay must be configured when queue services are reachable"
            )
        if response_endpoint is None:
            return None
        mismatched = [
            service.name
            for service in queue_services
            if getattr(service.transport_config, "broker", None) != response_endpoint.broker
        ]
        if mismatched:
            raise ValueError(
                "Queue services must use the configured response relay broker: "
                f"{sorted(mismatched)}"
            )
        return response_endpoint.destination

    def _configured_broker(self, name: str) -> ConfiguredBroker:
        try:
            return self._brokers[name]
        except KeyError as exc:
            raise ValueError(f"Messaging broker '{name}' is not configured") from exc

    def _configured_deployment(self) -> WorkerDeployment:
        return configured_worker_deployment(self.settings)

    def _own_cleanup(
        self,
        component: str,
        cleanup: Callable[[], Awaitable[Any]],
    ) -> None:
        if component in self._owned_cleanups:
            return
        self._owned_cleanups.add(component)
        self._lifecycle.push_async_callback(
            self._run_cleanup,
            component,
            cleanup,
        )

    def _ensure_cleanup_registration(self) -> None:
        if self._worker is not None:
            self._own_cleanup("temporal-worker", self._worker.shutdown)
        if self._schedule_worker is not None:
            self._own_cleanup("schedule-worker", self._schedule_worker.shutdown)
        if self._response_relay is not None:
            self._own_cleanup("response-relay", self._response_relay.stop)
        if self._trigger_ingress is not None:
            self._own_cleanup("trigger-ingress", self._trigger_ingress.stop)

    async def _run_cleanup(
        self,
        component: str,
        cleanup: Callable[[], Awaitable[Any]],
    ) -> None:
        try:
            await cleanup()
        except Exception as exc:  # noqa: BLE001 - owned component cleanup boundary
            logger.error(
                "Engine component cleanup failed",
                extra={
                    "component": component,
                    "exception_type": type(exc).__name__,
                },
            )
            self._cleanup_failures.append(EngineCleanupFailure(component=component, cause=exc))


async def run_engine(
    settings: Settings,
    transport_registry: TransportRegistry | None = None,
    broker_registry: BrokerRegistry | None = None,
    definition_catalog_store: DefinitionCatalogStore | None = None,
    configuration_source: ConfigurationSource | None = None,
    payload_protection: PayloadProtectionBinding | None = None,
    resource_registry: ResourceRegistry | None = None,
    resource_context: ResourceFactoryContext | None = None,
    temporal_connection_policy: TemporalConnectionPolicy | None = None,
    temporal_connection_binding: TemporalConnectionBinding | None = None,
    health_registry: HealthRegistry | None = None,
    metrics_registry: MetricsRegistry | None = None,
    enable_ingress: bool = True,
) -> None:
    """Run the workflow engine (blocking)."""
    engine = WorkflowEngine(
        settings,
        transport_registry=transport_registry,
        broker_registry=broker_registry,
        definition_catalog_store=definition_catalog_store,
        configuration_source=configuration_source,
        payload_protection=payload_protection,
        resource_registry=resource_registry,
        resource_context=resource_context,
        temporal_connection_policy=temporal_connection_policy,
        temporal_connection_binding=temporal_connection_binding,
        health_registry=health_registry,
        metrics_registry=metrics_registry,
        enable_ingress=enable_ingress,
    )
    try:
        await engine.start()
    except KeyboardInterrupt:
        logger.info("Workflow engine interrupted")
    finally:
        await engine.stop()


def configured_worker_deployment(settings: Settings) -> WorkerDeployment:
    build_id = settings.deployment.build_id
    if build_id is None:
        raise ValueError(
            "Worker deployment build id is required; set JUSTFLOW_DEPLOYMENT__BUILD_ID"
        )
    artifact_digest = settings.deployment.artifact_digest
    if artifact_digest is None:
        raise ProvenanceError(
            "Worker artifact digest is required; set JUSTFLOW_DEPLOYMENT__ARTIFACT_DIGEST"
        )
    package_version = settings.deployment.package_version
    if package_version is None:
        raise ProvenanceError(
            "Worker package version is unavailable; set JUSTFLOW_DEPLOYMENT__PACKAGE_VERSION"
        )
    artifact_identity = WorkerArtifactIdentity(
        deployment_name=settings.deployment.name,
        build_id=build_id,
        artifact_digest=artifact_digest,
        package_version=package_version,
        source_revision=settings.deployment.source_revision,
    )
    artifact_identity.validate_for_profile(settings.runtime.profile)
    compatible_abis = settings.deployment.compatible_engine_workflow_abis
    if ENGINE_WORKFLOW_ABI not in compatible_abis:
        logger.warning(
            "Current engine workflow ABI is not declared compatible",
            extra={"engine_workflow_abi": ENGINE_WORKFLOW_ABI},
        )
    return WorkerDeployment(
        artifact_identity=artifact_identity,
        compatible_engine_workflow_abis=compatible_abis,
    )
