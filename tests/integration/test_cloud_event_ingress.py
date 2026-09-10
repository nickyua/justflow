"""Cloud-event delivery starts one real Temporal workflow across duplicate retries."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from temporalio.worker import Worker

from justflow.brokers import PublishedMessage
from justflow.config.loader import ConfigLoader
from justflow.config.models import ServiceConfig
from justflow.config.runtime_limits import DEFAULT_RUNTIME_LIMITS
from justflow.config.triggers import EventTriggerDeclaration, TriggersConfig
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
from justflow.runtime import (
    CloudEventIngress,
    CloudEventMappingRegistry,
    EventBridgeEventMapper,
    WorkflowStarter,
    make_cloud_event_business_request_id,
)
from justflow.scope import LOCAL_RUNTIME_SCOPE
from justflow.sdk.message_contract import make_workflow_id
from justflow.transports.builtins import builtin_transport_registry
from tests.messaging import TestBroker

TASK_QUEUE = "test-cloud-event-ingress"
MAPPING_NAME = "eventbridge"
EVENT_ID = "cloud-event-e2e-1"
EVENT_DESTINATION = "cloud-events"
DEAD_LETTER_DESTINATION = "cloud-event-dead-letters"
DIRECT_TIMEOUT_SECONDS = 10
TEST_ARTIFACT_DIGEST = f"sha256:{'a' * 64}"
ENVIRONMENT_SNAPSHOT_DIGEST = "e" * SHA256_HEX_LENGTH
FIXTURE_CONFIGS = Path(__file__).parents[1] / "workflow_fixtures" / "configs"


def _direct_service(class_path: str) -> ServiceConfig:
    return ServiceConfig(
        transport="direct",
        transport_config={"class": class_path},
        dispatch_timeout_sec=DIRECT_TIMEOUT_SECONDS,
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


async def test_duplicate_eventbridge_deliveries_start_one_workflow(services) -> None:
    resolved_services, configured_services = services
    workflow_name = "record_processing"
    workflow_config = ConfigLoader(FIXTURE_CONFIGS).load_workflows()[workflow_name]
    workflow_config = workflow_config.model_copy(update={"on_complete": None})
    manifest = build_definition_manifests(
        {workflow_name: workflow_config},
        resolved_services,
        DEFAULT_RUNTIME_LIMITS,
    )[workflow_name]
    deployment = WorkerDeployment(
        artifact_identity=WorkerArtifactIdentity(
            deployment_name="justflow",
            build_id="cloud-event-test",
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
    event = json.dumps(
        {
            "version": "0",
            "id": EVENT_ID,
            "detail-type": "Record Created",
            "source": "com.example.records",
            "account": "123456789012",
            "time": "2026-08-06T08:30:00Z",
            "region": "eu-central-1",
            "resources": [],
            "detail": {"record_id": "record-1"},
        }
    )
    broker = TestBroker()
    for message_id in ("delivery-1", "delivery-2"):
        await broker.publish(
            EVENT_DESTINATION,
            PublishedMessage(body=event, message_id=message_id),
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
        starter = WorkflowStarter(
            env.client,
            TASK_QUEUE,
            {workflow_name: target},
            triggers=TriggersConfig(
                triggers={
                    "eventbridge": EventTriggerDeclaration(
                        workflow=workflow_name,
                        mapping=MAPPING_NAME,
                    )
                }
            ),
        )
        registry = CloudEventMappingRegistry()
        registry.register(
            MAPPING_NAME,
            EventBridgeEventMapper(
                mapping_name=MAPPING_NAME,
                workflow_name=workflow_name,
                source="com.example.records",
                detail_type="Record Created",
            ),
        )
        ingress = TriggerIngress(
            workflow_starter=starter,
            consumer=broker.consumer(
                EVENT_DESTINATION,
                dead_letter_destination=DEAD_LETTER_DESTINATION,
            ),
            source_name="test_broker",
            cloud_event_ingress=CloudEventIngress(registry, starter),
            cloud_event_mapping=MAPPING_NAME,
        )

        await ingress._poll_once()

        business_request_id = make_cloud_event_business_request_id(MAPPING_NAME, EVENT_ID)
        workflow_id = make_workflow_id(
            workflow_name,
            business_request_id,
            scope=LOCAL_RUNTIME_SCOPE,
        )
        result = await env.client.get_workflow_handle(workflow_id).result()

    assert result["status"] == "completed"
    assert result["request_id"] == business_request_id
    assert await broker.receive(DEAD_LETTER_DESTINATION) == ()
