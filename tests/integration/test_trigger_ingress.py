"""A broker-neutral trigger starts a real Temporal workflow."""

from __future__ import annotations

from pathlib import Path

import pytest
from temporalio.worker import Worker

from justflow.brokers import PublishedMessage
from justflow.config.loader import ConfigLoader
from justflow.config.models import ServiceConfig
from justflow.config.runtime_limits import DEFAULT_RUNTIME_LIMITS
from justflow.config.triggers import BrokerTriggerDeclaration, TriggersConfig
from justflow.definitions.manifest import (
    ENGINE_WORKFLOW_ABI,
    SHA256_HEX_LENGTH,
    build_definition_manifests,
)
from justflow.definitions.routing import (
    DefinitionStartTarget,
    WorkerDeployment,
    wait_for_worker_deployment,
    worker_deployment_config,
)
from justflow.engine.activities import WorkflowActivities
from justflow.engine.compiler import compile_workflow
from justflow.engine.local_temporal import start_local_environment
from justflow.engine.sandbox import workflow_sandbox_runner
from justflow.engine.trigger_ingress import TriggerIngress
from justflow.provenance import WorkerArtifactIdentity
from justflow.runtime import WorkflowStarter
from justflow.scope import LOCAL_RUNTIME_SCOPE
from justflow.sdk.message_contract import (
    PROTOCOL_VERSION,
    MessageKind,
    TriggerEnvelope,
    make_trigger_message_id,
    make_workflow_id,
)
from justflow.transports.builtins import builtin_transport_registry
from tests.messaging import TestBroker

TASK_QUEUE = "test-trigger-ingress"
REQUEST_ID = "trigger-e2e-1"
TRIGGER_DESTINATION = "triggers"
DEAD_LETTER_DESTINATION = "dead-letters"
DIRECT_TIMEOUT_SEC = 10
TEST_ARTIFACT_DIGEST = f"sha256:{'a' * 64}"
ENVIRONMENT_SNAPSHOT_DIGEST = "e" * SHA256_HEX_LENGTH
FIXTURE_CONFIGS = Path(__file__).parents[1] / "workflow_fixtures" / "configs"


def _direct_service(class_path: str) -> ServiceConfig:
    return ServiceConfig(
        transport="direct",
        transport_config={"class": class_path},
        dispatch_timeout_sec=DIRECT_TIMEOUT_SEC,
        retries=0,
    )


@pytest.fixture
def services():
    prefix = "tests.workflow_fixtures.actions"
    declarations = {
        "fetch_record": _direct_service(f"{prefix}.fetch_record.FetchRecord"),
        "validate_record": _direct_service(f"{prefix}.validate_record.ValidateRecord"),
        "enrich_record": _direct_service(f"{prefix}.enrich_record.EnrichRecord"),
        "classify_record": _direct_service(f"{prefix}.classify_record.ClassifyRecord"),
        "format_result": _direct_service(f"{prefix}.format_result.FormatResult"),
    }
    registry = builtin_transport_registry()
    resolved = registry.resolve_services(declarations)
    return resolved, registry.configure_services(resolved, resources={})


async def test_trigger_message_starts_workflow(services):
    resolved_services, configured_services = services
    workflow_config = ConfigLoader(FIXTURE_CONFIGS).load_workflows()["record_processing"]
    workflow_config = workflow_config.model_copy(update={"on_complete": None})
    manifest = build_definition_manifests(
        {"record_processing": workflow_config},
        resolved_services,
        DEFAULT_RUNTIME_LIMITS,
    )["record_processing"]
    deployment = WorkerDeployment(
        artifact_identity=WorkerArtifactIdentity(
            deployment_name="justflow",
            build_id="trigger-test",
            artifact_digest=TEST_ARTIFACT_DIGEST,
            package_version="0.1.0",
        ),
        compatible_engine_workflow_abis=frozenset({ENGINE_WORKFLOW_ABI}),
    )
    workflow_class = compile_workflow(
        workflow_config,
        resolved_services,
        manifest=manifest,
        deployment=deployment,
        environment_snapshot_digest=ENVIRONMENT_SNAPSHOT_DIGEST,
        runtime_scope_digest=LOCAL_RUNTIME_SCOPE.digest,
    )
    target = DefinitionStartTarget(
        manifest=manifest,
        workflow_class=workflow_class,
        deployment=deployment,
        environment_snapshot_digest=ENVIRONMENT_SNAPSHOT_DIGEST,
    )
    activities = WorkflowActivities(services=configured_services, resources={})
    broker = TestBroker()
    workflow_id = make_workflow_id("record_processing", REQUEST_ID)
    message_id = make_trigger_message_id(
        "record_processing",
        manifest.definition_digest,
        REQUEST_ID,
    )
    trigger = TriggerEnvelope(
        protocol_version=PROTOCOL_VERSION,
        message_id=message_id,
        kind=MessageKind.TRIGGER,
        workflow_name="record_processing",
        definition_digest=manifest.definition_digest,
        workflow_id=workflow_id,
        correlation_id=REQUEST_ID,
        trace_id="trigger-e2e-trace",
        business_request_id=REQUEST_ID,
        input={},
    )
    await broker.publish(
        TRIGGER_DESTINATION,
        PublishedMessage(body=trigger.model_dump_json(), message_id=message_id),
    )

    async with (
        await start_local_environment() as env,
        Worker(
            env.client,
            task_queue=TASK_QUEUE,
            workflows=[workflow_class],
            activities=[activities.execute_step],
            workflow_runner=workflow_sandbox_runner(),
            deployment_config=worker_deployment_config(deployment),
        ),
    ):
        await wait_for_worker_deployment(env.client, deployment, TASK_QUEUE)
        ingress = TriggerIngress(
            workflow_starter=WorkflowStarter(
                env.client,
                TASK_QUEUE,
                {"record_processing": target},
                triggers=TriggersConfig(
                    triggers={
                        "test_broker": BrokerTriggerDeclaration(
                            workflow="record_processing",
                            broker="test_broker",
                        )
                    }
                ),
            ),
            consumer=broker.consumer(
                TRIGGER_DESTINATION,
                dead_letter_destination=DEAD_LETTER_DESTINATION,
            ),
            source_name="test_broker",
        )

        await ingress._poll_once()

        scoped_workflow_id = make_workflow_id(
            "record_processing",
            REQUEST_ID,
            scope=LOCAL_RUNTIME_SCOPE,
        )
        result = await env.client.get_workflow_handle(scoped_workflow_id).result()

    assert result["status"] == "completed"
    assert result["request_id"] == REQUEST_ID
    assert await broker.receive(DEAD_LETTER_DESTINATION) == ()
