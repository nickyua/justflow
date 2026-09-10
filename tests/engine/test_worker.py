"""Tests for WorkflowEngine lifecycle wiring."""

from __future__ import annotations

import asyncio
import shutil
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from temporalio.client import TLSConfig

from justflow.brokers.sqs import builtin_broker_registry
from justflow.config.models import ServiceConfig
from justflow.config.settings import (
    BrokerDeclaration,
    CodecPayloadProtection,
    ConsumerEndpoint,
    DeploymentSettings,
    MessagingSettings,
    PathSettings,
    RuntimeSettings,
    Settings,
    TemporalSettings,
)
from justflow.definitions.configuration import configured_catalog_store
from justflow.definitions.migration import (
    export_catalog,
    import_catalog,
)
from justflow.engine import worker as worker_module
from justflow.engine.payload_protection import PayloadProtectionBinding, PayloadProtectionError
from justflow.engine.worker import EngineCleanupError, WorkflowEngine
from justflow.provenance import (
    LOCAL_ARTIFACT_DIGEST,
    ProvenanceError,
    RuntimeProfile,
)
from justflow.scope import LOCAL_RUNTIME_SCOPE
from justflow.transports.builtins import builtin_transport_registry
from tests.conftest import PRIME_STATS_CONFIG_DIR
from tests.settings import PRODUCTION_RUNTIME

QUEUE_SERVICE_TIMEOUT_SECONDS = 30
SQS_REDELIVERY_WINDOW_SECONDS = 25
TEST_ARTIFACT_DIGEST = f"sha256:{'a' * 64}"
WORKER_START_TEST_TIMEOUT_SECONDS = 5


@pytest.fixture
def isolated_config_dir(tmp_path: Path) -> Path:
    destination = tmp_path / "configs"
    shutil.copytree(PRIME_STATS_CONFIG_DIR, destination)
    import_catalog(
        configured_catalog_store(
            Settings(runtime=PRODUCTION_RUNTIME).catalog,
            destination,
            scope=PRODUCTION_RUNTIME.scope,
        ),
        export_catalog(
            configured_catalog_store(
                Settings(runtime=PRODUCTION_RUNTIME).catalog, destination, scope=LOCAL_RUNTIME_SCOPE
            )
        ),
    )
    return destination


def _deployment_settings() -> DeploymentSettings:
    return DeploymentSettings(
        build_id="test-build",
        artifact_digest=TEST_ARTIFACT_DIGEST,
        package_version="0.1.0",
    )


class TestWorkflowEngineLifecycle:
    async def test_stop_awaits_worker_shutdown_and_cleans_up(self):
        engine = WorkflowEngine(Settings(runtime=PRODUCTION_RUNTIME))
        engine._worker = AsyncMock()
        engine._response_relay = AsyncMock()
        engine._response_relay_task = asyncio.create_task(asyncio.sleep(3600))

        await engine.stop()

        engine._worker.shutdown.assert_awaited_once()
        engine._response_relay.stop.assert_awaited_once()
        assert engine._response_relay_task.cancelled()

    def test_relay_disabled_without_response_endpoint(self):
        engine = WorkflowEngine(Settings(runtime=PRODUCTION_RUNTIME))

        engine._start_response_relay()

        assert engine._response_relay is None
        assert engine._response_relay_task is None

    async def test_cleanup_continues_and_aggregates_component_failures(self):
        engine = WorkflowEngine(Settings(runtime=PRODUCTION_RUNTIME))
        engine._worker = AsyncMock()
        engine._worker.shutdown.side_effect = RuntimeError("worker close failed")
        engine._response_relay = AsyncMock()
        engine._response_relay.stop.side_effect = RuntimeError("relay close failed")

        with pytest.raises(EngineCleanupError) as exc_info:
            await engine.stop()

        engine._response_relay.stop.assert_awaited_once()
        engine._worker.shutdown.assert_awaited_once()
        assert [failure.component for failure in exc_info.value.failures] == [
            "response-relay",
            "temporal-worker",
        ]

        await engine.stop()
        engine._response_relay.stop.assert_awaited_once()
        engine._worker.shutdown.assert_awaited_once()


class TestWorkflowEngineStart:
    def test_production_startup_requires_artifact_digest(self) -> None:
        settings = Settings(
            runtime=PRODUCTION_RUNTIME,
            deployment=DeploymentSettings(
                build_id="build-1",
                package_version="1.2.3",
            ),
        )

        with pytest.raises(ProvenanceError, match="ARTIFACT_DIGEST"):
            WorkflowEngine(settings)._configured_deployment()

    def test_local_profile_accepts_only_explicit_local_artifact(self) -> None:
        settings = Settings(
            runtime=RuntimeSettings(profile=RuntimeProfile.LOCAL),
            deployment=DeploymentSettings(
                build_id="development",
                artifact_digest=LOCAL_ARTIFACT_DIGEST,
                package_version="development",
            ),
        )

        deployment = WorkflowEngine(settings)._configured_deployment()

        assert deployment.artifact_identity.artifact_digest == LOCAL_ARTIFACT_DIGEST

    def test_codec_mode_requires_host_binding(self):
        settings = Settings(
            runtime=PRODUCTION_RUNTIME,
            temporal=TemporalSettings(
                payload_protection=CodecPayloadProtection(
                    active_key_id="current",
                    readable_key_ids=frozenset({"current"}),
                )
            ),
        )

        with pytest.raises(PayloadProtectionError, match="host-provided binding"):
            WorkflowEngine(settings)

    def test_response_deduplication_outlives_broker_redelivery_window(self):
        settings = Settings(
            runtime=PRODUCTION_RUNTIME,
            brokers={
                "main": BrokerDeclaration(
                    provider="sqs",
                    config={
                        "destinations": {
                            "responses": "https://sqs.example/responses",
                            "dead": "https://sqs.example/dead",
                        }
                    },
                )
            },
            messaging=MessagingSettings(
                response=ConsumerEndpoint(
                    broker="main",
                    destination="responses",
                    dead_letter_destination="dead",
                ),
                deduplication_retention_seconds=SQS_REDELIVERY_WINDOW_SECONDS,
            ),
        )
        engine = WorkflowEngine(
            settings,
            broker_registry=builtin_broker_registry(sqs_client=MagicMock()),
        )

        with pytest.raises(ValueError, match="retention must exceed"):
            engine._configure_brokers()

    @pytest.mark.parametrize(
        ("settings", "expected"),
        [
            pytest.param(
                Settings(runtime=PRODUCTION_RUNTIME), "response relay", id="missing-relay"
            ),
            pytest.param(
                Settings(
                    runtime=PRODUCTION_RUNTIME,
                    messaging=MessagingSettings(
                        response=ConsumerEndpoint(
                            broker="secondary",
                            destination="responses",
                            dead_letter_destination="dead",
                        )
                    ),
                ),
                "configured response relay broker",
                id="broker-mismatch",
            ),
        ],
    )
    def test_queue_service_requires_compatible_response_relay(self, settings, expected):
        services = builtin_transport_registry().resolve_services(
            {
                "async": ServiceConfig(
                    transport="queue",
                    transport_config={
                        "broker": "primary",
                        "destination": "requests",
                        "idempotency": "durable",
                    },
                    dispatch_timeout_sec=5,
                    response_timeout_sec=QUEUE_SERVICE_TIMEOUT_SECONDS,
                    retries=0,
                )
            }
        )

        with pytest.raises(ValueError, match=expected):
            WorkflowEngine(settings)._queue_reply_destination(services)

    async def test_start_loads_validates_compiles_and_runs_worker(
        self,
        isolated_config_dir: Path,
    ):
        settings = Settings(
            runtime=PRODUCTION_RUNTIME,
            paths=PathSettings(config_dir=str(isolated_config_dir)),
            deployment=_deployment_settings(),
        )
        engine = WorkflowEngine(settings)

        fake_worker = MagicMock()
        fake_worker.run = AsyncMock()
        fake_worker.shutdown = AsyncMock()
        fake_schedule_worker = MagicMock()
        fake_schedule_worker.run = AsyncMock()
        fake_schedule_worker.shutdown = AsyncMock()

        with (
            patch.object(worker_module.Client, "connect", new=AsyncMock()) as connect_mock,
            patch.object(
                worker_module,
                "Worker",
                side_effect=[fake_worker, fake_schedule_worker],
            ) as worker_cls,
            patch.object(worker_module, "wait_for_worker_deployment", new=AsyncMock()),
        ):
            await engine.start()

        connect_kwargs = connect_mock.await_args.kwargs
        assert connect_mock.await_args.args == ("localhost:7233",)
        assert connect_kwargs["namespace"] == "default"
        assert isinstance(connect_kwargs["tls"], TLSConfig)
        assert connect_kwargs["api_key"] is None
        fake_worker.run.assert_awaited_once()
        fake_worker.shutdown.assert_awaited_once()
        fake_schedule_worker.run.assert_awaited_once()
        fake_schedule_worker.shutdown.assert_awaited_once()
        assert set(engine._start_targets) == {"prime_stats"}
        assert len(engine._compiled_workflows) == 1
        assert next(iter(engine._compiled_workflows)).startswith(
            f"jf1.workflow-type.{PRODUCTION_RUNTIME.scope.digest}."
        )
        assert engine._resource_loader.resources == {}
        assert engine._response_relay is None
        assert engine._trigger_ingress is None
        main_worker_call, schedule_worker_call = worker_cls.call_args_list
        assert main_worker_call.kwargs["task_queue"] == "gateway-workflows"
        assert main_worker_call.kwargs["deployment_config"].version.build_id == "test-build"
        assert schedule_worker_call.kwargs["task_queue"] == "justflow-schedules"
        assert "deployment_config" not in schedule_worker_call.kwargs

    async def test_start_passes_payload_codec_to_temporal_client(
        self,
        isolated_config_dir: Path,
    ):
        settings = Settings(
            runtime=PRODUCTION_RUNTIME,
            temporal=TemporalSettings(
                payload_protection=CodecPayloadProtection(
                    active_key_id="current",
                    readable_key_ids=frozenset({"previous", "current"}),
                )
            ),
            paths=PathSettings(config_dir=str(isolated_config_dir)),
            deployment=_deployment_settings(),
        )
        cipher = AsyncMock()
        binding = PayloadProtectionBinding(
            cipher=cipher,
            active_key_id="current",
            readable_key_ids=frozenset({"previous", "current"}),
        )
        engine = WorkflowEngine(settings, payload_protection=binding)
        fake_worker = MagicMock()
        fake_worker.run = AsyncMock()
        fake_worker.shutdown = AsyncMock()

        with (
            patch.object(worker_module.Client, "connect", new=AsyncMock()) as connect_mock,
            patch.object(worker_module, "Worker", return_value=fake_worker),
            patch.object(worker_module, "wait_for_worker_deployment", new=AsyncMock()),
        ):
            await engine.start()

        connect_kwargs = connect_mock.await_args.kwargs
        assert connect_kwargs["data_converter"].payload_codec is not None

    async def test_cancellation_still_cleans_up_resources(
        self,
        isolated_config_dir: Path,
    ):
        settings = Settings(
            runtime=PRODUCTION_RUNTIME,
            paths=PathSettings(config_dir=str(isolated_config_dir)),
            deployment=_deployment_settings(),
        )
        engine = WorkflowEngine(settings)
        worker_started = asyncio.Event()

        async def run_worker():
            worker_started.set()
            await asyncio.Future()

        fake_worker = MagicMock()
        fake_worker.run = run_worker
        fake_worker.shutdown = AsyncMock()
        fake_schedule_worker = MagicMock()
        fake_schedule_worker.run = run_worker
        fake_schedule_worker.shutdown = AsyncMock()

        with (
            patch.object(worker_module.Client, "connect", new=AsyncMock()),
            patch.object(
                worker_module,
                "Worker",
                side_effect=[fake_worker, fake_schedule_worker],
            ),
            patch.object(worker_module, "wait_for_worker_deployment", new=AsyncMock()),
        ):
            start_task = asyncio.create_task(engine.start())
            await asyncio.wait_for(worker_started.wait(), WORKER_START_TEST_TIMEOUT_SECONDS)
            start_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await start_task

        fake_worker.shutdown.assert_awaited_once()
        fake_schedule_worker.shutdown.assert_awaited_once()
        assert engine._resource_loader.resources == {}


class TestServiceSupervision:
    async def test_consumer_failure_cancels_the_temporal_worker(self):
        engine = WorkflowEngine(Settings(runtime=PRODUCTION_RUNTIME))
        worker_stopped = asyncio.Event()

        async def run_worker():
            try:
                await asyncio.Future()
            finally:
                worker_stopped.set()

        async def fail_consumer():
            raise RuntimeError("consumer crashed")

        worker = MagicMock()
        worker.run = run_worker
        engine._worker = worker
        schedule_worker = MagicMock()
        schedule_worker.run = run_worker
        engine._schedule_worker = schedule_worker
        engine._response_relay_task = asyncio.create_task(fail_consumer(), name="response-relay")

        with pytest.raises(RuntimeError, match="consumer crashed"):
            await engine._supervise_services()

        assert worker_stopped.is_set()
