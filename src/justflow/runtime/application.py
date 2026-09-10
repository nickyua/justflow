"""Host application composition for worker and gateway deployments."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Coroutine
from contextlib import AsyncExitStack
from dataclasses import dataclass
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from temporalio.client import Client

from justflow.brokers import BrokerRegistry, ConfiguredBroker
from justflow.brokers.sqs import builtin_broker_registry
from justflow.config.settings import FileConfigurationSettings, Settings
from justflow.config.triggers import TriggersConfig
from justflow.config.validator import ConfigValidator
from justflow.configuration import (
    ConfigurationSource,
    FileConfigurationSource,
    PlatformComponentCatalogSource,
    StoredConfigurationSource,
    TenantAuthoringPolicySource,
    configured_activation_store,
    configured_configuration_source,
    configured_configuration_store,
)
from justflow.configuration.activation_store import ActivationStore
from justflow.configuration.local_authoring import LocalAuthoringConfigurationSource
from justflow.configuration.ports import ConfigurationStore
from justflow.definitions.catalog import DefinitionCatalogStore
from justflow.definitions.configuration import configured_catalog_store
from justflow.definitions.environment import build_execution_environment_snapshots
from justflow.definitions.routing import (
    DefinitionStartTarget,
    WorkerDeploymentRouter,
    wait_for_worker_deployment,
)
from justflow.definitions.runtime import prepare_definitions
from justflow.engine.deduplication import BoundedDeduplicationStore
from justflow.engine.payload_protection import (
    PayloadProtectionBinding,
    configured_data_converter,
)
from justflow.engine.response_relay import ResponseRelay
from justflow.engine.trigger_ingress import TriggerIngress
from justflow.engine.worker import WorkflowEngine, configured_worker_deployment
from justflow.provenance import ExecutionConfigurationIdentity, RuntimeProfile
from justflow.resources.base import ResourceFactoryContext
from justflow.resources.builtins import builtin_resource_registry
from justflow.resources.registry import ResourceRegistry
from justflow.runtime.admin_panel import AdminPanel
from justflow.runtime.auth import AuthenticationProvider
from justflow.runtime.cloud_events import (
    CloudEventIngress,
    CloudEventMappingRegistry,
)
from justflow.runtime.configuration_activation import ScopedDefinitionCatalogSource
from justflow.runtime.configuration_api import (
    ConfigurationApiBinding,
    ConfigurationAuthoringMode,
)
from justflow.runtime.control_api import (
    AsgiMessage,
    AsgiReceive,
    AsgiScope,
    AsgiSend,
    ControlApi,
)
from justflow.runtime.health import HealthComponent, HealthReason, HealthRegistry
from justflow.runtime.local_authoring_api import LocalAuthoringConfigurationApi
from justflow.runtime.metrics import MetricsRegistry
from justflow.runtime.operations import WorkflowControlService
from justflow.runtime.operations_api import OperationsApi
from justflow.runtime.operations_query import (
    AuthoringReference,
    AuthoringReferenceSource,
    CatalogAuthoringReferenceSource,
    OperationsQueryService,
    ScopedDeclaredTriggerSource,
    ScopedScheduleOperationsSource,
    StaticAuthoringReferenceSource,
    UnavailableAuthoringReferenceSource,
    build_authoring_reference,
)
from justflow.runtime.schedule_operations import (
    ScheduleApplier,
    ScheduleControlService,
    ScheduleOperator,
    ScopedScheduleControlService,
)
from justflow.runtime.scheduled_start_service import (
    BestEffortLocalScheduledStartQuotaController,
    ScheduledStartQuotaController,
    ScheduledStartService,
    ScheduledStartWorkloadPolicy,
)
from justflow.runtime.starter import WorkflowStarter, WorkflowTargetResolver
from justflow.runtime.temporal import (
    TemporalConnectionBinding,
    TemporalConnectionPolicy,
    resolve_temporal_connection,
)
from justflow.runtime.webhooks import WebhookIngress, WebhookSourceRegistry
from justflow.transports.builtins import builtin_transport_registry
from justflow.transports.registry import TransportRegistry

if TYPE_CHECKING:
    from justflow.runtime.schedule_application import ScheduleRuntime

logger = logging.getLogger(__name__)

JSON_RESPONSE_HEADERS = ((b"content-type", b"application/json; charset=utf-8"),)
LIVE_RESPONSE_BODY = b'{"status":"live"}'
READY_RESPONSE_BODY = b'{"status":"ready"}'
UNAVAILABLE_RESPONSE_BODY = b'{"status":"unavailable"}'
NOT_FOUND_RESPONSE_BODY = b'{"error":{"code":"not_found","message":"Route not found"}}'
NOT_READY_RESPONSE_BODY = b'{"error":{"code":"not_ready","message":"Runtime is not ready"}}'


@runtime_checkable
class _ClosableStore(Protocol):
    def close(self) -> None: ...


@dataclass(frozen=True, kw_only=True)
class _PreparedGatewayRuntime:
    targets: dict[str, DefinitionStartTarget]
    router: WorkerDeploymentRouter
    catalog_store: DefinitionCatalogStore
    configuration_store: ConfigurationStore | None
    activation_store: ActivationStore | None
    triggers: TriggersConfig
    authoring_reference_source: AuthoringReferenceSource
    execution_identity: ExecutionConfigurationIdentity | None


@dataclass(frozen=True, kw_only=True)
class _CreatedOperations:
    api: OperationsApi
    schedule_controls: ScheduleControlService
    schedule_applier: ScheduleApplier


class RuntimeGateway:
    """Ingress and control plane that can scale independently of workers."""

    def __init__(
        self,
        *,
        settings: Settings,
        transport_registry: TransportRegistry,
        broker_registry: BrokerRegistry,
        resource_registry: ResourceRegistry,
        definition_catalog_store: DefinitionCatalogStore | None,
        configuration_source: ConfigurationSource | None,
        workflow_target_resolver: WorkflowTargetResolver | None,
        payload_protection: PayloadProtectionBinding | None,
        temporal_connection_policy: TemporalConnectionPolicy,
        authentication: AuthenticationProvider | None,
        configuration_api: ConfigurationApiBinding | None,
        component_catalog_source: PlatformComponentCatalogSource | None,
        tenant_authoring_policy_source: TenantAuthoringPolicySource | None,
        configuration_store: ConfigurationStore | None = None,
        activation_store: ActivationStore | None = None,
        operations_api: OperationsApi | None = None,
        schedule_controls: ScheduleControlService | None = None,
        scheduled_start_quota_controller: ScheduledStartQuotaController | None = None,
        scheduled_start_workload_policy: ScheduledStartWorkloadPolicy | None = None,
        admin_panel: AdminPanel | None = None,
        webhook_registry: WebhookSourceRegistry,
        cloud_event_registry: CloudEventMappingRegistry | None = None,
        cloud_event_broker_mapping: str | None = None,
        health: HealthRegistry,
        metrics: MetricsRegistry,
    ) -> None:
        self._settings = settings
        self._transport_registry = transport_registry
        self._broker_registry = broker_registry
        self._resource_registry = resource_registry
        self._definition_catalog_store = definition_catalog_store
        self._configuration_source = configuration_source
        self._workflow_target_resolver = workflow_target_resolver
        self._data_converter = configured_data_converter(
            settings.temporal.payload_protection,
            payload_protection,
        )
        self._connection_policy = temporal_connection_policy
        self._authentication = authentication
        self._configuration_api = configuration_api
        self._component_catalog_source = component_catalog_source
        self._tenant_authoring_policy_source = tenant_authoring_policy_source
        self._configuration_store = configuration_store
        self._activation_store = activation_store
        self._operations_api = operations_api
        self._schedule_controls = schedule_controls
        self._scheduled_start_quota_controller = scheduled_start_quota_controller
        self._scheduled_start_workload_policy = scheduled_start_workload_policy
        self._admin_panel = admin_panel
        self._webhook_registry = webhook_registry
        self._cloud_event_registry = cloud_event_registry or CloudEventMappingRegistry(
            scope=settings.runtime.scope
        )
        if cloud_event_broker_mapping is not None and (
            settings.messaging.trigger is None
            or cloud_event_broker_mapping not in self._cloud_event_registry.mappings
        ):
            raise ValueError(
                "Cloud-event broker mapping requires a configured consumer and registration"
            )
        self._cloud_event_broker_mapping = cloud_event_broker_mapping
        self._health = health
        self._metrics = metrics
        self._lifecycle = AsyncExitStack()
        self._client: Client | None = None
        self._brokers: dict[str, ConfiguredBroker] = {}
        self._tasks: set[asyncio.Task[None]] = set()
        self._response_relay_service: ResponseRelay | None = None
        self._trigger_ingress_service: TriggerIngress | None = None
        self._app: ControlApi | None = None
        self._started = False
        self._closed = False
        self._stop_lock = asyncio.Lock()

    @property
    def health(self) -> HealthRegistry:
        return self._health

    @property
    def metrics(self) -> MetricsRegistry:
        return self._metrics

    @property
    def app(self) -> ControlApi:
        if self._app is None:
            raise RuntimeError("Runtime gateway has not started")
        return self._app

    async def start(self) -> None:
        if self._closed:
            raise RuntimeError("Runtime gateway cannot restart after shutdown")
        if self._started:
            raise RuntimeError("Runtime gateway is already started")
        try:
            prepared = self._prepare_start_targets()
            self._health.mark_ready(HealthComponent.CATALOG)
            self._configure_brokers()
            self._health.mark_ready(HealthComponent.PROVIDERS)

            client = await self._connection_policy.connect(self._data_converter)
            self._client = client
            self._health.mark_ready(HealthComponent.TEMPORAL)
            await wait_for_worker_deployment(
                client,
                prepared.router.active,
                self._settings.temporal.task_queue,
                attempts=self._settings.temporal.deployment_registration.attempts,
                interval_seconds=(self._settings.temporal.deployment_registration.interval_seconds),
            )
            self._health.mark_ready(HealthComponent.WORKER_REGISTRATION)

            starter = WorkflowStarter(
                client,
                self._settings.temporal.task_queue,
                prepared.targets if self._workflow_target_resolver is None else None,
                triggers=(prepared.triggers if self._workflow_target_resolver is None else None),
                target_resolver=self._workflow_target_resolver,
                scope=self._settings.runtime.scope,
                limits=self._settings.limits.snapshot(),
                metrics=self._metrics,
                indexed_search_attributes_enabled=(
                    self._settings.operations.indexed_search_attributes_enabled
                ),
                pinned_start_retry=self._settings.runtime.pinned_start_retry,
            )
            controls = WorkflowControlService(
                client,
                scope=None,
                rpc_timeout_seconds=self._settings.control.temporal_rpc_timeout_seconds,
                max_payload_bytes=self._settings.limits.signal_payload_bytes,
                indexed_search_attributes_enabled=(
                    self._settings.operations.indexed_search_attributes_enabled
                ),
            )
            quota_controller = self._scheduled_start_quota_controller
            if quota_controller is None and self._settings.runtime.profile is RuntimeProfile.LOCAL:
                quota_controller = BestEffortLocalScheduledStartQuotaController()
            scheduled_starts = ScheduledStartService(
                client,
                starter,
                self._settings.scheduled_starts,
                task_queue=self._settings.schedules.task_queue,
                workload_policy=self._scheduled_start_workload_policy,
                quota_controller=quota_controller,
                metrics=self._metrics,
            )
            cloud_events = (
                CloudEventIngress(
                    self._cloud_event_registry,
                    starter,
                    limits=self._settings.limits.snapshot(),
                )
                if self._cloud_event_registry.mappings
                else None
            )
            self._start_response_relay(client)
            self._start_trigger_ingress(starter, cloud_events)
            await self._wait_for_consumer_readiness()
            webhooks = (
                WebhookIngress(self._webhook_registry, starter)
                if self._webhook_registry.sources
                else None
            )
            operations_api = self._operations_api
            schedule_controls = self._schedule_controls
            schedule_applier = None
            if operations_api is None:
                created_operations = self._create_operations_api(
                    client,
                    controls,
                    prepared,
                    scheduled_starts,
                )
                operations_api = created_operations.api
                schedule_controls = schedule_controls or created_operations.schedule_controls
                schedule_applier = created_operations.schedule_applier
            self._app = ControlApi(
                settings=self._settings.control,
                runtime_profile=self._settings.runtime.profile,
                starter=starter,
                controls=controls,
                health=self._health,
                metrics=self._metrics,
                authentication=self._authentication,
                webhooks=webhooks,
                cloud_events=cloud_events,
                configuration_api=self._configuration_api,
                operations_api=operations_api,
                schedule_controls=schedule_controls,
                schedule_applier=schedule_applier,
                scheduled_starts=scheduled_starts,
                admin_panel=(
                    self._admin_panel if self._settings.operations.admin_panel_enabled else None
                ),
                runtime_scope=self._settings.runtime.scope,
            )
            self._started = True
        except BaseException:
            await self.stop()
            raise

    async def stop(self) -> None:
        async with self._stop_lock:
            if self._closed:
                return
            self._closed = True
            for task in self._tasks:
                if not task.done():
                    task.cancel()
            if self._tasks:
                await asyncio.gather(*self._tasks, return_exceptions=True)
            await self._lifecycle.aclose()
            for component in HealthComponent:
                self._health.mark_unavailable(component, HealthReason.STOPPED)
            self._started = False

    def _prepare_start_targets(
        self,
    ) -> _PreparedGatewayRuntime:
        configuration_store = self._configuration_store
        activation_store = self._activation_store
        source = self._configuration_source
        if source is None:
            if not isinstance(self._settings.configuration, FileConfigurationSettings):
                if configuration_store is None:
                    configuration_store = configured_configuration_store(
                        self._settings.configuration
                    )
                    self._register_owned_store(configuration_store)
                if activation_store is None:
                    activation_store = configured_activation_store(self._settings.configuration)
                    self._register_owned_store(activation_store)
            source = configured_configuration_source(
                self._settings.configuration,
                scope=self._settings.runtime.scope,
                config_dir=self._settings.paths.config_dir,
                store=configuration_store,
            )
        configuration_snapshot = source.read(self._settings.runtime.scope)
        bundle = configuration_snapshot.bundle
        resources_config = bundle.resources
        services_config = bundle.services
        workflows = bundle.workflows
        validator = ConfigValidator(
            resources_config,
            services_config,
            workflows,
            transport_registry=self._transport_registry,
            resource_registry=self._resource_registry,
            limits=self._settings.limits.snapshot(),
            config_dir=self._settings.paths.config_dir,
            workflow_sources=(
                source.workflow_sources if isinstance(source, FileConfigurationSource) else {}
            ),
            triggers=bundle.triggers,
        )
        validator.validate().raise_if_invalid()
        catalog_store = self._definition_catalog_store or configured_catalog_store(
            self._settings.catalog,
            self._settings.paths.definition_catalog_dir,
            scope=self._settings.runtime.scope,
        )
        catalog = catalog_store.load()
        router = WorkerDeploymentRouter.for_deployment(configured_worker_deployment(self._settings))
        snapshots = build_execution_environment_snapshots(
            self._settings,
            catalog,
            router,
            catalog_store.backend_identity,
            configuration_snapshot.execution_identity,
        )
        prepared = prepare_definitions(
            workflows,
            validator.resolved_services,
            self._settings.limits.snapshot(),
            catalog,
            router,
            {name: snapshot.snapshot_digest for name, snapshot in snapshots.items()},
            resources=validator.resolved_resources,
            runtime_scope=self._settings.runtime.scope,
            execution_configuration=configuration_snapshot.execution_identity,
        )
        for snapshot in snapshots.values():
            catalog_store.store_environment_snapshot(snapshot)
        return _PreparedGatewayRuntime(
            targets=dict(prepared.start_targets),
            router=router,
            catalog_store=catalog_store,
            configuration_store=configuration_store,
            activation_store=activation_store,
            triggers=bundle.triggers,
            authoring_reference_source=self._authoring_reference_source(
                build_authoring_reference(
                    services=services_config,
                    resources=resources_config,
                    resource_registry=self._resource_registry,
                    workflows=bundle.workflows,
                )
            ),
            execution_identity=configuration_snapshot.execution_identity,
        )

    def _create_operations_api(
        self,
        client: Client,
        controls: WorkflowControlService,
        prepared: _PreparedGatewayRuntime,
        scheduled_starts: ScheduledStartService,
    ) -> _CreatedOperations:
        scope = self._settings.runtime.scope
        schedule_operator = ScheduleOperator(
            client,
            prepared.triggers,
            self._settings.schedules,
            scope=scope,
            metrics=self._metrics,
        )
        schedule_applier = ScheduleApplier(
            client,
            prepared.triggers,
            self._settings,
            catalog_store=prepared.catalog_store,
            router=prepared.router,
            execution_configuration=prepared.execution_identity,
            scope=scope,
            metrics=self._metrics,
        )
        return _CreatedOperations(
            api=OperationsApi(
                settings=self._settings.control,
                scheduled_start_settings=self._settings.scheduled_starts,
                scheduled_starts=scheduled_starts,
                query_service=OperationsQueryService(
                    executions=controls,
                    health=self._health,
                    definition_catalogs=ScopedDefinitionCatalogSource(
                        {scope: prepared.catalog_store}
                    ),
                    configuration_store=prepared.configuration_store,
                    activation_store=prepared.activation_store,
                    triggers=ScopedDeclaredTriggerSource({scope: prepared.triggers}),
                    schedules=ScopedScheduleOperationsSource({scope: schedule_operator}),
                    authoring_reference_source=prepared.authoring_reference_source,
                    metrics_links=self._settings.operations.metrics_links,
                ),
            ),
            schedule_controls=ScopedScheduleControlService({scope: schedule_operator}),
            schedule_applier=schedule_applier,
        )

    def _register_owned_store(self, store: object) -> None:
        if isinstance(store, _ClosableStore):
            self._lifecycle.callback(store.close)

    def _authoring_reference_source(
        self,
        local_reference: AuthoringReference,
    ) -> AuthoringReferenceSource:
        api = self._configuration_api
        if api is None or api.authoring_mode is ConfigurationAuthoringMode.LOCAL_SOURCE:
            return StaticAuthoringReferenceSource(
                self._settings.runtime.scope,
                local_reference,
            )
        if self._component_catalog_source is None or self._tenant_authoring_policy_source is None:
            return UnavailableAuthoringReferenceSource(
                "Managed component catalog or authoring policy is not configured"
            )
        return CatalogAuthoringReferenceSource(
            component_catalog_source=self._component_catalog_source,
            policy_source=self._tenant_authoring_policy_source,
        )

    def _configure_brokers(self) -> None:
        for name, declaration in self._settings.brokers.items():
            broker = self._broker_registry.configure(
                declaration.provider,
                declaration.config,
            )
            self._brokers[name] = broker
            self._lifecycle.push_async_callback(broker.close)
        response = self._settings.messaging.response
        if response is None:
            return
        broker = self._configured_broker(response.broker)
        if (
            self._settings.messaging.deduplication_retention_seconds
            <= broker.delivery_policy.max_redelivery_window_seconds
        ):
            raise ValueError(
                "Response deduplication retention must exceed the broker redelivery window"
            )

    def _start_response_relay(self, client: Client) -> None:
        endpoint = self._settings.messaging.response
        if endpoint is None:
            return
        broker = self._configured_broker(endpoint.broker)
        relay = ResponseRelay(
            temporal_client=client,
            consumer=broker.consumer(
                endpoint.destination,
                dead_letter_destination=endpoint.dead_letter_destination,
            ),
            deduplication_store=BoundedDeduplicationStore(
                capacity=self._settings.messaging.deduplication_capacity,
                retention_seconds=self._settings.messaging.deduplication_retention_seconds,
            ),
            message_concurrency=self._settings.messaging.message_concurrency,
            limits=self._settings.limits.snapshot(),
            metrics=self._metrics,
            scope=self._settings.runtime.scope,
            readiness_callback=lambda ready: self._set_consumer_health(
                HealthComponent.RESPONSE_CONSUMER,
                ready,
            ),
            reconnect_policy=self._settings.messaging.reconnect,
        )
        self._response_relay_service = relay
        self._lifecycle.push_async_callback(relay.stop)
        self._start_task(
            relay.start(),
            name="response-relay",
            health_component=HealthComponent.RESPONSE_CONSUMER,
        )

    def _start_trigger_ingress(
        self,
        starter: WorkflowStarter,
        cloud_events: CloudEventIngress | None,
    ) -> None:
        endpoint = self._settings.messaging.trigger
        if endpoint is None:
            return
        broker = self._configured_broker(endpoint.broker)
        ingress = TriggerIngress(
            workflow_starter=starter,
            consumer=broker.consumer(
                endpoint.destination,
                dead_letter_destination=endpoint.dead_letter_destination,
            ),
            source_name=endpoint.broker,
            scope=self._settings.runtime.scope,
            message_concurrency=self._settings.messaging.message_concurrency,
            limits=self._settings.limits.snapshot(),
            metrics=self._metrics,
            readiness_callback=lambda ready: self._set_consumer_health(
                HealthComponent.TRIGGER_CONSUMER,
                ready,
            ),
            cloud_event_ingress=cloud_events if self._cloud_event_broker_mapping else None,
            cloud_event_mapping=self._cloud_event_broker_mapping,
            reconnect_policy=self._settings.messaging.reconnect,
        )
        self._trigger_ingress_service = ingress
        self._lifecycle.push_async_callback(ingress.stop)
        self._start_task(
            ingress.start(),
            name="trigger-ingress",
            health_component=HealthComponent.TRIGGER_CONSUMER,
        )

    def _start_task(
        self,
        awaitable: Coroutine[Any, Any, None],
        *,
        name: str,
        health_component: HealthComponent,
    ) -> None:
        task = asyncio.create_task(awaitable, name=name)
        self._tasks.add(task)
        task.add_done_callback(
            lambda completed: self._background_task_done(completed, health_component)
        )

    def _background_task_done(
        self,
        task: asyncio.Task[None],
        health_component: HealthComponent,
    ) -> None:
        self._health.mark_unavailable(health_component)
        if task.cancelled():
            return
        exception = task.exception()
        if exception is not None:
            logger.error(
                "Runtime gateway service stopped",
                extra={
                    "component": health_component.value,
                    "exception_type": type(exception).__name__,
                },
            )

    async def _wait_for_consumer_readiness(self) -> None:
        timeout = self._settings.messaging.consumer_ready_timeout_seconds
        waits = []
        if self._response_relay_service is not None:
            waits.append(self._response_relay_service.wait_ready(timeout))
        if self._trigger_ingress_service is not None:
            waits.append(self._trigger_ingress_service.wait_ready(timeout))
        if waits:
            await asyncio.gather(*waits)

    def _set_consumer_health(self, component: HealthComponent, ready: bool) -> None:
        if ready:
            self._health.mark_ready(component)
        else:
            self._health.mark_unavailable(component)

    def _configured_broker(self, name: str) -> ConfiguredBroker:
        try:
            return self._brokers[name]
        except KeyError as exc:
            raise ValueError(f"Messaging broker '{name}' is not configured") from exc


async def _wait_for_worker_ready(
    worker: WorkflowEngine,
    worker_task: asyncio.Task[None],
) -> None:
    ready_task = asyncio.create_task(
        worker.wait_ready(worker.settings.runtime.worker_ready_timeout_seconds),
        name="justflow-worker-readiness",
    )
    done, _ = await asyncio.wait(
        {worker_task, ready_task},
        return_when=asyncio.FIRST_COMPLETED,
    )
    if worker_task in done:
        ready_task.cancel()
        await asyncio.gather(ready_task, return_exceptions=True)
        await worker_task
        raise RuntimeError("Worker stopped during startup")
    await ready_task


class RuntimeAsgiApplication:
    """ASGI lifecycle owner for a gateway and optional co-located worker."""

    def __init__(
        self,
        gateway: RuntimeGateway,
        *,
        worker: WorkflowEngine | None = None,
    ) -> None:
        self.gateway = gateway
        self.worker = worker
        self._worker_task: asyncio.Task[None] | None = None
        self._started = False

    async def __call__(
        self,
        scope: AsgiScope,
        receive: AsgiReceive,
        send: AsgiSend,
    ) -> None:
        if scope.get("type") == "lifespan":
            await self._lifespan(receive, send)
            return
        if not self._started:
            await send(
                AsgiMessage(
                    type="http.response.start",
                    status=HTTPStatus.SERVICE_UNAVAILABLE,
                    headers=JSON_RESPONSE_HEADERS,
                )
            )
            await send(
                AsgiMessage(
                    type="http.response.body",
                    body=NOT_READY_RESPONSE_BODY,
                )
            )
            return
        await self.gateway.app(scope, receive, send)

    async def startup(self) -> None:
        if self.worker is not None:
            self._worker_task = asyncio.create_task(
                self.worker.start(),
                name="justflow-worker",
            )
        try:
            if self.worker is None or self._worker_task is None:
                await self.gateway.start()
            else:
                await asyncio.gather(
                    self.gateway.start(),
                    _wait_for_worker_ready(self.worker, self._worker_task),
                )
        except BaseException:
            await self.shutdown()
            raise
        self._started = True

    async def shutdown(self) -> None:
        await self.gateway.stop()
        if self.worker is not None:
            await self.worker.stop()
        if self._worker_task is not None:
            if not self._worker_task.done():
                self._worker_task.cancel()
            await asyncio.gather(self._worker_task, return_exceptions=True)
        self._started = False

    async def _lifespan(self, receive: AsgiReceive, send: AsgiSend) -> None:
        while True:
            message = await receive()
            message_type = message.get("type")
            if message_type == "lifespan.startup":
                try:
                    await self.startup()
                except Exception as exc:  # noqa: BLE001 - ASGI lifespan failure boundary
                    await send(
                        AsgiMessage(
                            type="lifespan.startup.failed",
                            message=f"Runtime startup failed: {type(exc).__name__}",
                        )
                    )
                    return
                await send(AsgiMessage(type="lifespan.startup.complete"))
            elif message_type == "lifespan.shutdown":
                await self.shutdown()
                await send(AsgiMessage(type="lifespan.shutdown.complete"))
                return


class WorkerAsgiApplication:
    """ASGI lifecycle owner exposing only worker liveness and readiness."""

    def __init__(self, worker: WorkflowEngine, health: HealthRegistry) -> None:
        self.worker = worker
        self.health = health
        self._worker_task: asyncio.Task[None] | None = None
        self._started = False

    async def __call__(
        self,
        scope: AsgiScope,
        receive: AsgiReceive,
        send: AsgiSend,
    ) -> None:
        if scope.get("type") == "lifespan":
            await self._lifespan(receive, send)
            return
        if not self._started:
            await _send_probe_response(
                send,
                HTTPStatus.SERVICE_UNAVAILABLE,
                NOT_READY_RESPONSE_BODY,
            )
            return
        method = scope.get("method", "")
        path = scope.get("path", "")
        if method == "GET" and path == "/livez":
            await _send_probe_response(send, HTTPStatus.OK, LIVE_RESPONSE_BODY)
            return
        if method == "GET" and path == "/readyz":
            report = self.health.report()
            await _send_probe_response(
                send,
                HTTPStatus.OK if report.ready else HTTPStatus.SERVICE_UNAVAILABLE,
                READY_RESPONSE_BODY if report.ready else UNAVAILABLE_RESPONSE_BODY,
            )
            return
        await _send_probe_response(send, HTTPStatus.NOT_FOUND, NOT_FOUND_RESPONSE_BODY)

    async def startup(self) -> None:
        if self._started:
            raise RuntimeError("Worker application is already started")
        self._worker_task = asyncio.create_task(
            self.worker.start(),
            name="justflow-worker",
        )
        await _wait_for_worker_ready(self.worker, self._worker_task)
        self._started = True

    async def shutdown(self) -> None:
        await self.worker.stop()
        if self._worker_task is not None:
            if not self._worker_task.done():
                self._worker_task.cancel()
            await asyncio.gather(self._worker_task, return_exceptions=True)
        self._started = False

    async def _lifespan(self, receive: AsgiReceive, send: AsgiSend) -> None:
        while True:
            message = await receive()
            message_type = message.get("type")
            if message_type == "lifespan.startup":
                try:
                    await self.startup()
                except Exception as exc:  # noqa: BLE001 - ASGI lifespan failure boundary
                    await send(
                        AsgiMessage(
                            type="lifespan.startup.failed",
                            message=f"Runtime startup failed: {type(exc).__name__}",
                        )
                    )
                    return
                await send(AsgiMessage(type="lifespan.startup.complete"))
            elif message_type == "lifespan.shutdown":
                await self.shutdown()
                await send(AsgiMessage(type="lifespan.shutdown.complete"))
                return


async def _send_probe_response(
    send: AsgiSend,
    status: HTTPStatus,
    body: bytes,
) -> None:
    await send(
        AsgiMessage(
            type="http.response.start",
            status=status,
            headers=JSON_RESPONSE_HEADERS,
        )
    )
    await send(AsgiMessage(type="http.response.body", body=body))


class RuntimeApplication:
    """Validated host composition shared by worker and gateway processes.

    Hosts may supply custom transport, broker, resource, authentication, payload
    protection, webhook, catalog, and Temporal credential bindings. The Temporal
    connection policy is resolved once and reused by every client this composition
    creates.
    """

    def __init__(
        self,
        settings: Settings,
        *,
        transport_registry: TransportRegistry | None = None,
        broker_registry: BrokerRegistry | None = None,
        resource_registry: ResourceRegistry | None = None,
        definition_catalog_store: DefinitionCatalogStore | None = None,
        configuration_source: ConfigurationSource | None = None,
        workflow_target_resolver: WorkflowTargetResolver | None = None,
        payload_protection: PayloadProtectionBinding | None = None,
        resource_context: ResourceFactoryContext | None = None,
        authentication: AuthenticationProvider | None = None,
        configuration_api: ConfigurationApiBinding | None = None,
        component_catalog_source: PlatformComponentCatalogSource | None = None,
        tenant_authoring_policy_source: TenantAuthoringPolicySource | None = None,
        configuration_store: ConfigurationStore | None = None,
        activation_store: ActivationStore | None = None,
        operations_api: OperationsApi | None = None,
        schedule_controls: ScheduleControlService | None = None,
        scheduled_start_quota_controller: ScheduledStartQuotaController | None = None,
        scheduled_start_workload_policy: ScheduledStartWorkloadPolicy | None = None,
        admin_panel: AdminPanel | None = None,
        webhook_registry: WebhookSourceRegistry | None = None,
        cloud_event_registry: CloudEventMappingRegistry | None = None,
        cloud_event_broker_mapping: str | None = None,
        temporal_connection_binding: TemporalConnectionBinding | None = None,
        temporal_connection_policy: TemporalConnectionPolicy | None = None,
    ) -> None:
        if temporal_connection_binding is not None and temporal_connection_policy is not None:
            raise ValueError("Provide a Temporal connection binding or resolved policy, not both")
        self.settings = settings
        self.transport_registry = transport_registry or builtin_transport_registry()
        self.broker_registry = broker_registry or builtin_broker_registry()
        self.resource_registry = resource_registry or builtin_resource_registry()
        self.definition_catalog_store = definition_catalog_store
        if settings.operations.local_source_authoring_enabled:
            if configuration_source is not None or configuration_api is not None:
                raise ValueError(
                    "Local source authoring cannot replace an injected configuration boundary"
                )
            if configuration_store is not None or activation_store is not None:
                raise ValueError(
                    "Local source authoring cannot be combined with managed configuration stores"
                )
            local_source = LocalAuthoringConfigurationSource(
                settings.paths.config_dir,
                scope=settings.runtime.scope,
                transport_registry=self.transport_registry,
                resource_registry=self.resource_registry,
                limits=settings.limits.snapshot(),
                definition_catalog_store=configured_catalog_store(
                    settings.catalog,
                    settings.paths.definition_catalog_dir,
                    scope=settings.runtime.scope,
                ),
            )
            configuration_source = local_source
            configuration_api = LocalAuthoringConfigurationApi(local_source)
        self.configuration_source = configuration_source or (
            StoredConfigurationSource(configuration_store)
            if configuration_store is not None
            else None
        )
        self.workflow_target_resolver = workflow_target_resolver
        self.payload_protection = payload_protection
        self.resource_context = resource_context
        self.authentication = authentication
        self.configuration_api = configuration_api
        self.component_catalog_source = component_catalog_source
        self.tenant_authoring_policy_source = tenant_authoring_policy_source
        self.configuration_store = configuration_store
        self.activation_store = activation_store
        self.operations_api = operations_api
        self.schedule_controls = schedule_controls
        self.scheduled_start_quota_controller = scheduled_start_quota_controller
        self.scheduled_start_workload_policy = scheduled_start_workload_policy
        self.admin_panel = admin_panel
        self.webhook_registry = webhook_registry or WebhookSourceRegistry(
            scope=settings.runtime.scope
        )
        self.cloud_event_registry = cloud_event_registry or CloudEventMappingRegistry(
            scope=settings.runtime.scope
        )
        if cloud_event_broker_mapping is not None:
            if settings.messaging.trigger is None:
                raise ValueError(
                    "A cloud-event broker mapping requires a configured trigger consumer"
                )
            if cloud_event_broker_mapping not in self.cloud_event_registry.mappings:
                raise ValueError("Cloud-event broker mapping is not registered")
        self.cloud_event_broker_mapping = cloud_event_broker_mapping
        self.temporal_connection_policy = temporal_connection_policy or resolve_temporal_connection(
            settings.temporal,
            settings.runtime.profile,
            temporal_connection_binding,
        )
        self.metrics = MetricsRegistry()

    def create_worker(self, *, enable_ingress: bool = False) -> WorkflowEngine:
        health = self._worker_health()
        return self._worker(health=health, enable_ingress=enable_ingress)

    def create_worker_app(self) -> WorkerAsgiApplication:
        health = self._worker_health()
        return WorkerAsgiApplication(
            self._worker(health=health, enable_ingress=False),
            health,
        )

    def _worker_health(self) -> HealthRegistry:
        required = {
            HealthComponent.CATALOG,
            HealthComponent.TEMPORAL,
            HealthComponent.WORKER,
            HealthComponent.PROVIDERS,
        }
        return HealthRegistry(frozenset(required), metrics=self.metrics)

    async def create_schedule_runtime(self) -> ScheduleRuntime:
        """Create schedule reconciliation and operator facades for this host composition."""
        from justflow.runtime.schedule_application import ScheduleRuntime

        return await ScheduleRuntime.create(
            self.settings,
            temporal_connection_policy=self.temporal_connection_policy,
            payload_protection=self.payload_protection,
            definition_catalog_store=self.definition_catalog_store,
            configuration_source=self.configuration_source,
            metrics=self.metrics,
        )

    def create_gateway_app(self) -> RuntimeAsgiApplication:
        self._validate_control_authentication()
        health = self._gateway_health()
        gateway = self._gateway(health)
        return RuntimeAsgiApplication(gateway)

    def create_combined_app(self) -> RuntimeAsgiApplication:
        self._validate_control_authentication()
        health = self._gateway_health(require_worker=True)
        gateway = self._gateway(health)
        worker = self._worker(health=health, enable_ingress=False)
        return RuntimeAsgiApplication(gateway, worker=worker)

    def _gateway_health(self, *, require_worker: bool = False) -> HealthRegistry:
        required = {
            HealthComponent.CATALOG,
            HealthComponent.TEMPORAL,
            HealthComponent.WORKER_REGISTRATION,
            HealthComponent.PROVIDERS,
        }
        if require_worker:
            required.add(HealthComponent.WORKER)
        if self.settings.messaging.trigger is not None:
            required.add(HealthComponent.TRIGGER_CONSUMER)
        if self.settings.messaging.response is not None:
            required.add(HealthComponent.RESPONSE_CONSUMER)
        return HealthRegistry(frozenset(required), metrics=self.metrics)

    def _validate_control_authentication(self) -> None:
        if (
            self.settings.runtime.profile is not RuntimeProfile.LOCAL
            and self.authentication is None
        ):
            raise ValueError(
                "Control API authentication is required outside the local runtime profile"
            )

    def _worker(
        self,
        *,
        health: HealthRegistry,
        enable_ingress: bool,
    ) -> WorkflowEngine:
        return WorkflowEngine(
            self.settings,
            transport_registry=self.transport_registry,
            broker_registry=self.broker_registry,
            definition_catalog_store=self.definition_catalog_store,
            configuration_source=self.configuration_source,
            payload_protection=self.payload_protection,
            resource_registry=self.resource_registry,
            resource_context=self.resource_context,
            temporal_connection_policy=self.temporal_connection_policy,
            health_registry=health,
            metrics_registry=self.metrics,
            workflow_target_resolver=self.workflow_target_resolver,
            scheduled_start_workload_policy=self.scheduled_start_workload_policy,
            enable_ingress=enable_ingress,
        )

    def _gateway(self, health: HealthRegistry) -> RuntimeGateway:
        return RuntimeGateway(
            settings=self.settings,
            transport_registry=self.transport_registry,
            broker_registry=self.broker_registry,
            resource_registry=self.resource_registry,
            definition_catalog_store=self.definition_catalog_store,
            configuration_source=self.configuration_source,
            workflow_target_resolver=self.workflow_target_resolver,
            payload_protection=self.payload_protection,
            temporal_connection_policy=self.temporal_connection_policy,
            authentication=self.authentication,
            configuration_api=self.configuration_api,
            component_catalog_source=self.component_catalog_source,
            tenant_authoring_policy_source=self.tenant_authoring_policy_source,
            configuration_store=self.configuration_store,
            activation_store=self.activation_store,
            operations_api=self.operations_api,
            schedule_controls=self.schedule_controls,
            scheduled_start_quota_controller=self.scheduled_start_quota_controller,
            scheduled_start_workload_policy=self.scheduled_start_workload_policy,
            admin_panel=self.admin_panel,
            webhook_registry=self.webhook_registry,
            cloud_event_registry=self.cloud_event_registry,
            cloud_event_broker_mapping=self.cloud_event_broker_mapping,
            health=health,
            metrics=self.metrics,
        )
