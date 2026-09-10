"""Tests for host runtime composition and deployment shapes."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import justflow.__main__ as engine_cli
from justflow.config.settings import (
    ConsumerEndpoint,
    MessagingSettings,
    OperationsSettings,
    PathSettings,
    RuntimeSettings,
    Settings,
)
from justflow.configuration import StoredConfigurationSource
from justflow.configuration.local_authoring import LocalAuthoringConfigurationSource
from justflow.provenance import RuntimeProfile
from justflow.runtime.application import (
    RuntimeApplication,
    RuntimeAsgiApplication,
    WorkerAsgiApplication,
)
from justflow.runtime.auth import (
    AuthenticatedPrincipal,
    AuthenticationRequest,
    AuthorizationRequest,
)
from justflow.runtime.cloud_events import (
    CloudEventMappingRegistry,
    EventBridgeEventMapper,
)
from justflow.runtime.health import HealthComponent, HealthStatus
from justflow.runtime.local_authoring_api import LocalAuthoringConfigurationApi
from justflow.runtime.scheduled_start_service import (
    BestEffortLocalScheduledStartQuotaController,
    ScheduledStartService,
)
from tests.settings import PRODUCTION_RUNTIME


class FakeTemporalPolicy:
    async def connect(self, data_converter: object) -> object:
        raise AssertionError("Composition tests must not connect")


class ConnectingTemporalPolicy:
    def __init__(self) -> None:
        self.client = MagicMock()
        self.connect = AsyncMock(return_value=self.client)


class AllowAuthentication:
    async def authenticate(
        self,
        request: AuthenticationRequest,
    ) -> AuthenticatedPrincipal:
        return AuthenticatedPrincipal.for_local_development("host")

    async def authorize(
        self,
        principal: AuthenticatedPrincipal,
        request: AuthorizationRequest,
    ) -> bool:
        return True


@dataclass(frozen=True, kw_only=True)
class RuntimeServerCommandCase:
    id: str
    command: str
    factory: str


RUNTIME_SERVER_COMMAND_CASES = [
    RuntimeServerCommandCase(id="api", command="api", factory="gateway"),
    RuntimeServerCommandCase(id="combined", command="serve", factory="combined"),
    RuntimeServerCommandCase(
        id="worker-health",
        command="worker-server",
        factory="worker",
    ),
]

TEST_CONTROL_PORT = 8090
TEST_SHUTDOWN_GRACE_SECONDS = 17


def test_composition_reuses_one_resolved_temporal_policy() -> None:
    policy = FakeTemporalPolicy()
    configuration_source = MagicMock()
    composition = RuntimeApplication(
        Settings(runtime=RuntimeSettings(profile=RuntimeProfile.LOCAL)),
        temporal_connection_policy=policy,
        configuration_source=configuration_source,
    )

    worker = composition.create_worker()
    gateway_app = composition.create_gateway_app()
    combined_app = composition.create_combined_app()

    assert worker._temporal_connection_policy is policy
    assert worker._configuration_source is configuration_source
    assert worker._enable_ingress is False
    assert gateway_app.gateway._connection_policy is policy
    assert gateway_app.gateway._configuration_source is configuration_source
    assert combined_app.gateway._connection_policy is policy
    assert combined_app.worker is not None
    assert combined_app.worker._temporal_connection_policy is policy
    assert combined_app.worker._enable_ingress is False


def test_composition_uses_configuration_store_as_runtime_source() -> None:
    store = MagicMock()
    composition = RuntimeApplication(
        Settings(runtime=RuntimeSettings(profile=RuntimeProfile.LOCAL)),
        temporal_connection_policy=FakeTemporalPolicy(),
        configuration_store=store,
    )

    worker = composition.create_worker()
    gateway = composition.create_gateway_app().gateway

    assert isinstance(composition.configuration_source, StoredConfigurationSource)
    assert worker._configuration_source is composition.configuration_source
    assert gateway._configuration_source is composition.configuration_source


def test_local_authoring_composition_shares_a_restart_gated_file_source(tmp_path: Path) -> None:
    config_dir = tmp_path / "configs"
    config_dir.mkdir()
    (config_dir / "resources.yaml").write_text("resources: {}\n")
    (config_dir / "services.yaml").write_text("services: {}\n")
    (config_dir / "triggers.yaml").write_text("triggers: {}\n")
    workflows = config_dir / "workflows"
    workflows.mkdir()
    (workflows / "example.yaml").write_text(
        "workflow: example\nsteps: {}\nflow:\n  - name: done\n    terminal: true\n"
    )
    settings = Settings(
        runtime=RuntimeSettings(profile=RuntimeProfile.LOCAL),
        paths=PathSettings(config_dir=str(config_dir)),
        operations=OperationsSettings(
            admin_panel_enabled=True,
            local_source_authoring_enabled=True,
        ),
    )

    composition = RuntimeApplication(
        settings,
        temporal_connection_policy=FakeTemporalPolicy(),
    )
    worker = composition.create_worker()
    gateway = composition.create_gateway_app().gateway

    assert isinstance(composition.configuration_source, LocalAuthoringConfigurationSource)
    assert isinstance(composition.configuration_api, LocalAuthoringConfigurationApi)
    assert worker._configuration_source is composition.configuration_source
    assert gateway._configuration_source is composition.configuration_source
    assert gateway._admin_panel is None


def test_production_gateway_requires_host_authentication() -> None:
    composition = RuntimeApplication(
        Settings(runtime=PRODUCTION_RUNTIME),
        temporal_connection_policy=FakeTemporalPolicy(),
    )

    with pytest.raises(ValueError, match="authentication is required"):
        composition.create_gateway_app()
    with pytest.raises(ValueError, match="authentication is required"):
        composition.create_combined_app()

    authenticated = RuntimeApplication(
        Settings(runtime=PRODUCTION_RUNTIME),
        temporal_connection_policy=FakeTemporalPolicy(),
        authentication=AllowAuthentication(),
    )
    assert isinstance(authenticated.create_gateway_app(), RuntimeAsgiApplication)


def test_composition_binds_registered_cloud_event_to_existing_trigger_consumer() -> None:
    mapping_name = "order-events"
    registry = CloudEventMappingRegistry()
    registry.register(
        mapping_name,
        EventBridgeEventMapper(
            mapping_name=mapping_name,
            workflow_name="example",
            source="com.example.orders",
        ),
    )
    settings = Settings(
        runtime=RuntimeSettings(profile=RuntimeProfile.LOCAL),
        messaging=MessagingSettings(
            trigger=ConsumerEndpoint(
                broker="events",
                destination="orders",
                dead_letter_destination="orders-dead-letter",
            )
        ),
    )

    composition = RuntimeApplication(
        settings,
        temporal_connection_policy=FakeTemporalPolicy(),
        cloud_event_registry=registry,
        cloud_event_broker_mapping=mapping_name,
    )
    gateway = composition.create_gateway_app().gateway

    assert gateway._cloud_event_registry is registry
    assert gateway._cloud_event_broker_mapping == mapping_name


def test_cloud_event_broker_mapping_requires_consumer_and_registration() -> None:
    registry = CloudEventMappingRegistry()

    with pytest.raises(ValueError, match="trigger consumer"):
        RuntimeApplication(
            Settings(runtime=RuntimeSettings(profile=RuntimeProfile.LOCAL)),
            temporal_connection_policy=FakeTemporalPolicy(),
            cloud_event_registry=registry,
            cloud_event_broker_mapping="missing",
        )

    settings = Settings(
        runtime=RuntimeSettings(profile=RuntimeProfile.LOCAL),
        messaging=MessagingSettings(
            trigger=ConsumerEndpoint(
                broker="events",
                destination="orders",
                dead_letter_destination="orders-dead-letter",
            )
        ),
    )
    with pytest.raises(ValueError, match="not registered"):
        RuntimeApplication(
            settings,
            temporal_connection_policy=FakeTemporalPolicy(),
            cloud_event_registry=registry,
            cloud_event_broker_mapping="missing",
        )


@pytest.mark.parametrize(
    ("profile", "authentication", "expected_quota_type"),
    [
        pytest.param(
            RuntimeProfile.LOCAL,
            None,
            BestEffortLocalScheduledStartQuotaController,
            id="local-best-effort",
        ),
        pytest.param(
            RuntimeProfile.PRODUCTION,
            AllowAuthentication(),
            type(None),
            id="production-host-required",
        ),
    ],
)
async def test_gateway_startup_wires_profile_quota_and_dependencies(
    profile: RuntimeProfile,
    authentication: AllowAuthentication | None,
    expected_quota_type: type[object],
) -> None:
    policy = ConnectingTemporalPolicy()
    application = RuntimeApplication(
        Settings(runtime=RuntimeSettings(profile=profile, scope=PRODUCTION_RUNTIME.scope)),
        temporal_connection_policy=policy,
        authentication=authentication,
    ).create_gateway_app()
    router = MagicMock()
    router.active = MagicMock()
    prepared = MagicMock()
    prepared.router = router
    prepared.targets = {}
    prepared.catalog_store = MagicMock()
    prepared.configuration_store = None
    prepared.activation_store = None
    prepared.schedules.schedules = {}

    with (
        patch.object(
            application.gateway,
            "_prepare_start_targets",
            return_value=prepared,
        ),
        patch(
            "justflow.runtime.application.wait_for_worker_deployment",
            new=AsyncMock(),
        ) as wait_for_worker,
        patch(
            "justflow.runtime.application.ScheduledStartService",
            wraps=ScheduledStartService,
        ) as scheduled_start_service,
    ):
        await application.gateway.start()

    health_report = application.gateway.health.report()
    assert health_report.ready is True
    health_by_component = {component.component: component for component in health_report.components}
    assert health_by_component[HealthComponent.WORKER_REGISTRATION].required is True
    assert health_by_component[HealthComponent.WORKER_REGISTRATION].status is HealthStatus.READY
    assert health_by_component[HealthComponent.WORKER].required is False
    assert health_by_component[HealthComponent.WORKER].status is HealthStatus.UNAVAILABLE
    quota_controller = scheduled_start_service.call_args.kwargs["quota_controller"]
    assert isinstance(quota_controller, expected_quota_type)
    policy.connect.assert_awaited_once()
    wait_for_worker.assert_awaited_once_with(
        policy.client,
        router.active,
        "gateway-workflows",
        attempts=50,
        interval_seconds=0.1,
    )

    await application.gateway.stop()
    assert application.gateway.health.report().ready is False


async def test_asgi_application_rejects_http_before_runtime_startup() -> None:
    application = RuntimeApplication(
        Settings(runtime=RuntimeSettings(profile=RuntimeProfile.LOCAL)),
        temporal_connection_policy=FakeTemporalPolicy(),
    ).create_gateway_app()
    incoming = [{"type": "http.request", "body": b"", "more_body": False}]
    outgoing: list[dict[str, object]] = []

    async def receive() -> dict[str, object]:
        return incoming.pop(0)

    async def send(message: dict[str, object]) -> None:
        outgoing.append(message)

    await application(
        {
            "type": "http",
            "method": "GET",
            "path": "/livez",
            "query_string": b"",
            "headers": (),
        },
        receive,
        send,
    )

    assert outgoing[0]["status"] == 503
    assert b"not_ready" in outgoing[1]["body"]


@pytest.mark.parametrize(
    "case",
    RUNTIME_SERVER_COMMAND_CASES,
    ids=lambda case: case.id,
)
def test_runtime_server_command_builds_requested_application(
    monkeypatch,
    case: RuntimeServerCommandCase,
) -> None:
    captured: dict[str, object] = {}
    asgi_application = object()

    class FakeComposition:
        def __init__(self, settings: Settings, *, admin_panel: object | None = None) -> None:
            captured["settings"] = settings
            captured["admin_panel"] = admin_panel

        def create_gateway_app(self) -> object:
            captured["factory"] = "gateway"
            return asgi_application

        def create_combined_app(self) -> object:
            captured["factory"] = "combined"
            return asgi_application

        def create_worker_app(self) -> object:
            captured["factory"] = "worker"
            return asgi_application

    uvicorn = ModuleType("uvicorn")

    def run(app: object, **kwargs: object) -> None:
        captured["app"] = app
        captured["uvicorn"] = kwargs

    uvicorn.run = run
    monkeypatch.setitem(__import__("sys").modules, "uvicorn", uvicorn)
    monkeypatch.setattr(
        "sys.argv",
        [
            "justflow",
            case.command,
            "--runtime-profile",
            "local",
            "--host",
            "127.0.0.1",
            "--port",
            str(TEST_CONTROL_PORT),
            "--shutdown-grace-seconds",
            str(TEST_SHUTDOWN_GRACE_SECONDS),
        ],
    )

    with patch("justflow.runtime.application.RuntimeApplication", FakeComposition):
        engine_cli.main()

    assert captured["app"] is asgi_application
    assert captured["factory"] == case.factory
    assert captured["admin_panel"] is None
    assert captured["uvicorn"] == {
        "host": "127.0.0.1",
        "port": TEST_CONTROL_PORT,
        "log_level": "info",
        "timeout_graceful_shutdown": TEST_SHUTDOWN_GRACE_SECONDS,
    }
    assert captured["settings"].runtime.profile is RuntimeProfile.LOCAL


async def test_worker_application_exposes_only_live_and_ready_probes() -> None:
    worker_stopped = asyncio.Event()
    worker = MagicMock()

    async def run_worker() -> None:
        await worker_stopped.wait()

    async def stop_worker() -> None:
        worker_stopped.set()

    worker.start = run_worker
    worker.stop = stop_worker
    settings = Settings(runtime=RuntimeSettings(profile=RuntimeProfile.LOCAL))
    worker.settings = settings
    worker.wait_ready = AsyncMock()
    application = RuntimeApplication(
        settings,
        temporal_connection_policy=FakeTemporalPolicy(),
    ).create_worker_app()
    application.worker = worker

    await application.startup()
    live_response = await _request_worker_application(application, "/livez")
    unavailable_response = await _request_worker_application(application, "/readyz")
    for component in application.health.report().components:
        if component.required:
            application.health.mark_ready(component.component)
    ready_response = await _request_worker_application(application, "/readyz")
    hidden_response = await _request_worker_application(application, "/healthz")

    assert live_response == (200, b'{"status":"live"}')
    assert unavailable_response == (503, b'{"status":"unavailable"}')
    assert ready_response == (200, b'{"status":"ready"}')
    assert hidden_response[0] == 404
    worker.wait_ready.assert_awaited_once_with(60.0)

    await application.shutdown()
    assert worker_stopped.is_set()


async def _request_worker_application(
    application: WorkerAsgiApplication,
    path: str,
) -> tuple[int, bytes]:
    incoming = [{"type": "http.request", "body": b"", "more_body": False}]
    outgoing: list[dict[str, object]] = []

    async def receive() -> dict[str, object]:
        return incoming.pop(0)

    async def send(message: dict[str, object]) -> None:
        outgoing.append(message)

    await application(
        {
            "type": "http",
            "method": "GET",
            "path": path,
            "query_string": b"",
            "headers": (),
        },
        receive,
        send,
    )
    return int(outgoing[0]["status"]), bytes(outgoing[1]["body"])
