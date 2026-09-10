"""Integration tests for broker-neutral asynchronous step responses."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from temporalio.client import WorkflowFailureError
from temporalio.common import PinnedVersioningOverride
from temporalio.exceptions import ApplicationError, CancelledError
from temporalio.worker import Worker

from justflow.brokers import (
    BrokerProvider,
    BrokerRegistry,
    PublishedMessage,
    StrictBrokerConfig,
)
from justflow.config.models import (
    FlowStep,
    IterationFailStrategy,
    ServiceConfig,
    StepDefinition,
    WorkflowConfig,
)
from justflow.config.runtime_limits import DEFAULT_RUNTIME_LIMITS
from justflow.definitions.manifest import (
    ENGINE_WORKFLOW_ABI,
    SHA256_HEX_LENGTH,
    build_definition_manifests,
)
from justflow.definitions.routing import (
    WorkerDeployment,
    retry_pinned_workflow_start,
    wait_for_worker_deployment,
    worker_deployment_config,
)
from justflow.engine.activities import WorkflowActivities
from justflow.engine.compiler import compile_workflow
from justflow.engine.deduplication import BoundedDeduplicationStore
from justflow.engine.local_temporal import start_local_environment
from justflow.engine.response_relay import ResponseRelay
from justflow.engine.sandbox import workflow_sandbox_runner
from justflow.provenance import WorkerArtifactIdentity
from justflow.sdk.message_contract import (
    PROTOCOL_VERSION,
    MessageKind,
    StepErrorBody,
    StepRequestEnvelope,
    StepResponseEnvelope,
    StepSuccessBody,
    WorkflowTrigger,
)
from tests.conftest import BuiltinServiceBundle, configure_builtin_services
from tests.messaging import TestBroker

TASK_QUEUE = "test-queue-signals"
REQUEST_DESTINATION = "processing"
RESPONSE_DESTINATION = "responses"
DEAD_LETTER_DESTINATION = "dead-letters"
BROKER_NAME = "test"
SIGNAL_WAIT_TIMEOUT_SEC = 3
LONG_SIGNAL_WAIT_TIMEOUT_SEC = 30
PUBLISH_WAIT_TIMEOUT_SEC = 10
MAX_CONCURRENCY = 2
ENVIRONMENT_SNAPSHOT_DIGEST = "e" * SHA256_HEX_LENGTH
DEPLOYMENT = WorkerDeployment(
    artifact_identity=WorkerArtifactIdentity(
        deployment_name="justflow",
        build_id="queue-integration",
        artifact_digest=f"sha256:{'a' * 64}",
        package_version="0.1.0",
    ),
    compatible_engine_workflow_abis=frozenset({ENGINE_WORKFLOW_ABI}),
)
VERSIONING_OVERRIDE = PinnedVersioningOverride(DEPLOYMENT.temporal_version)


class MemoryBrokerConfig(StrictBrokerConfig):
    pass


def _build_test_broker(config: MemoryBrokerConfig) -> TestBroker:
    del config
    return TestBroker()


def _registered_test_broker() -> TestBroker:
    registry = BrokerRegistry()
    registry.register(
        BrokerProvider(
            name="test.memory",
            contract_version="1",
            config_model=MemoryBrokerConfig,
            factory=_build_test_broker,
        )
    )
    broker = registry.configure("test.memory", {})
    assert isinstance(broker, TestBroker)
    return broker


def _queue_service(
    broker: TestBroker,
    timeout_sec: int = SIGNAL_WAIT_TIMEOUT_SEC,
) -> BuiltinServiceBundle:
    return configure_builtin_services(
        {
            "queue_service": ServiceConfig(
                transport="queue",
                transport_config={
                    "broker": BROKER_NAME,
                    "destination": REQUEST_DESTINATION,
                    "idempotency": "durable",
                },
                dispatch_timeout_sec=5,
                response_timeout_sec=timeout_sec,
                retries=0,
            )
        },
        message_publishers={BROKER_NAME: broker.publisher},
        reply_destination=RESPONSE_DESTINATION,
    )


def _parallel_queue_workflow() -> WorkflowConfig:
    return WorkflowConfig(
        workflow="queue_parallel",
        steps={"process": StepDefinition(service="queue_service", action="process_item")},
        flow=[
            FlowStep(
                name="check",
                op="process",
                input="items",
                for_each="input",
                as_var="item",
                parallel=True,
                max_concurrency=MAX_CONCURRENCY,
                output="results",
                then="done",
            ),
            FlowStep(name="done", terminal=True),
        ],
    )


def _single_queue_workflow() -> WorkflowConfig:
    return WorkflowConfig(
        workflow="queue_single",
        steps={"process": StepDefinition(service="queue_service", action="process_item")},
        flow=[
            FlowStep(name="check", op="process", output="result", then="done"),
            FlowStep(name="done", terminal=True),
        ],
    )


def _compile(
    workflow_config: WorkflowConfig,
    services: BuiltinServiceBundle,
) -> tuple[type, str]:
    manifest = build_definition_manifests(
        {workflow_config.workflow: workflow_config},
        services.resolved,
        DEFAULT_RUNTIME_LIMITS,
    )[workflow_config.workflow]
    return (
        compile_workflow(
            workflow_config,
            services.resolved,
            manifest=manifest,
            deployment=DEPLOYMENT,
            environment_snapshot_digest=ENVIRONMENT_SNAPSHOT_DIGEST,
        ),
        manifest.definition_digest,
    )


def _workflow_trigger(request_id: str, definition_digest: str, **globals_: Any) -> dict[str, Any]:
    return WorkflowTrigger(
        request_id=request_id,
        correlation_id="independent-parent-correlation",
        globals=globals_,
        definition_digest=definition_digest,
        worker_deployment=DEPLOYMENT.name,
        worker_build_id=DEPLOYMENT.build_id,
        worker_artifact=DEPLOYMENT.artifact_identity,
        environment_snapshot_digest=ENVIRONMENT_SNAPSHOT_DIGEST,
    ).model_dump(mode="json")


async def _published_requests(
    broker: TestBroker,
    count: int,
    workflow_handle: Any,
) -> tuple[StepRequestEnvelope, ...]:
    publish_task = asyncio.create_task(broker.wait_for_published(REQUEST_DESTINATION, count))
    result_task = asyncio.create_task(workflow_handle.result())
    done, pending = await asyncio.wait(
        (publish_task, result_task),
        timeout=PUBLISH_WAIT_TIMEOUT_SEC,
        return_when=asyncio.FIRST_COMPLETED,
    )
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    if not done:
        raise TimeoutError(f"expected {count} published queue requests")
    if result_task in done:
        raise AssertionError(
            "workflow completed before publishing its queue request"
        ) from result_task.exception()
    published = publish_task.result()
    return tuple(StepRequestEnvelope.model_validate_json(item.body) for item in published)


def _response(
    request: StepRequestEnvelope,
    body: StepSuccessBody | StepErrorBody,
) -> StepResponseEnvelope:
    return StepResponseEnvelope(
        protocol_version=PROTOCOL_VERSION,
        message_id=f"response-{request.message_id}",
        kind=MessageKind.STEP_RESPONSE,
        workflow_name=request.workflow_name,
        definition_digest=request.definition_digest,
        workflow_id=request.workflow_id,
        correlation_id=request.correlation_id,
        causation_id=request.message_id,
        trace_id=request.trace_id,
        workflow_run_id=request.workflow_run_id,
        in_reply_to=request.message_id,
        step_invocation_id=request.step_invocation_id,
        step_name=request.step_name,
        action=request.action,
        body=body,
    )


def _relay(broker: TestBroker, temporal_client: Any) -> ResponseRelay:
    return ResponseRelay(
        temporal_client=temporal_client,
        consumer=broker.consumer(
            RESPONSE_DESTINATION,
            dead_letter_destination=DEAD_LETTER_DESTINATION,
        ),
        deduplication_store=BoundedDeduplicationStore(
            capacity=100,
            retention_seconds=3_600,
        ),
    )


class TestQueueSignalPath:
    @pytest.mark.parametrize("failed_child", [False, True], ids=["success", "one-child-fails"])
    async def test_child_workflows_keep_independent_queue_invocations(self, failed_child: bool):
        broker = _registered_test_broker()
        services = _queue_service(broker, timeout_sec=LONG_SIGNAL_WAIT_TIMEOUT_SEC)
        child = WorkflowConfig.model_validate(
            {**_single_queue_workflow().model_dump(), "result": "check.result"}
        )
        parent = _parallel_queue_workflow().model_copy(
            update={
                "workflow": "queue_parent",
                "steps": {"process": StepDefinition(workflow=child.workflow)},
                "flow": [
                    _parallel_queue_workflow()
                    .flow[0]
                    .model_copy(update={"on_iteration_fail": IterationFailStrategy.COLLECT}),
                    FlowStep(name="done", terminal=True),
                ],
            }
        )
        manifests = build_definition_manifests(
            {parent.workflow: parent, child.workflow: child},
            services.resolved,
            DEFAULT_RUNTIME_LIMITS,
        )
        child_class = compile_workflow(
            child,
            services.resolved,
            manifest=manifests[child.workflow],
            deployment=DEPLOYMENT,
            environment_snapshot_digest=ENVIRONMENT_SNAPSHOT_DIGEST,
        )
        parent_class = compile_workflow(
            parent,
            services.resolved,
            manifest=manifests[parent.workflow],
            deployment=DEPLOYMENT,
            environment_snapshot_digest=ENVIRONMENT_SNAPSHOT_DIGEST,
            child_manifests={child.workflow: manifests[child.workflow]},
            child_environment_snapshot_digests={child.workflow: ENVIRONMENT_SNAPSHOT_DIGEST},
        )
        activities = WorkflowActivities(services=dict(services.configured))
        async with (
            await start_local_environment() as env,
            Worker(
                env.client,
                task_queue=TASK_QUEUE,
                workflows=[parent_class, child_class],
                activities=[activities.execute_step],
                workflow_runner=workflow_sandbox_runner(),
                deployment_config=worker_deployment_config(DEPLOYMENT),
            ),
        ):
            await wait_for_worker_deployment(env.client, DEPLOYMENT, TASK_QUEUE)
            handle = await retry_pinned_workflow_start(
                lambda: env.client.start_workflow(
                    parent_class.run,
                    _workflow_trigger(
                        "queue-parent-request",
                        manifests[parent.workflow].definition_digest,
                        items=["alpha", "beta"],
                    ),
                    id="queue-parent-execution",
                    task_queue=TASK_QUEUE,
                    versioning_override=VERSIONING_OVERRIDE,
                ),
                DEPLOYMENT,
                TASK_QUEUE,
            )
            requests = await _published_requests(broker, MAX_CONCURRENCY, handle)
            assert len({request.workflow_id for request in requests}) == MAX_CONCURRENCY
            assert len({request.step_invocation_id for request in requests}) == MAX_CONCURRENCY
            assert {request.correlation_id for request in requests} == {
                "independent-parent-correlation"
            }
            for index, request in reversed(
                list(enumerate(sorted(requests, key=lambda r: r.workflow_id)))
            ):
                body = (
                    StepErrorBody(
                        code="CUSTOMER_REJECTED",
                        message="Synthetic permanent failure",
                        retryable=False,
                    )
                    if failed_child and index == 0
                    else StepSuccessBody(output={"child_index": index})
                )
                response = _response(request, body)
                await broker.publish(
                    RESPONSE_DESTINATION,
                    PublishedMessage(
                        body=response.model_dump_json(), message_id=response.message_id
                    ),
                )
            await _relay(broker, env.client)._poll_once()
            result = await asyncio.wait_for(handle.result(), timeout=PUBLISH_WAIT_TIMEOUT_SEC)
        outcomes = result["steps"]["check"]["output"]
        assert outcomes[1] == {"child_index": 1}
        if failed_child:
            assert outcomes[0]["_error"] is True
        else:
            assert outcomes[0] == {"child_index": 0}

    async def test_parallel_iterations_get_their_own_responses(self):
        broker = _registered_test_broker()
        services = _queue_service(broker, timeout_sec=LONG_SIGNAL_WAIT_TIMEOUT_SEC)
        workflow_class, definition_digest = _compile(_parallel_queue_workflow(), services)
        activities = WorkflowActivities(services=dict(services.configured))

        async with (
            await start_local_environment() as env,
            Worker(
                env.client,
                task_queue=TASK_QUEUE,
                workflows=[workflow_class],
                activities=[activities.execute_step],
                workflow_runner=workflow_sandbox_runner(),
                deployment_config=worker_deployment_config(DEPLOYMENT),
            ),
        ):
            await wait_for_worker_deployment(env.client, DEPLOYMENT, TASK_QUEUE)
            handle = await retry_pinned_workflow_start(
                lambda: env.client.start_workflow(
                    workflow_class.run,
                    _workflow_trigger(
                        "queue-req",
                        definition_digest,
                        items=["alpha", "beta"],
                    ),
                    id="queue-workflow",
                    task_queue=TASK_QUEUE,
                    versioning_override=VERSIONING_OVERRIDE,
                ),
                DEPLOYMENT,
                TASK_QUEUE,
            )
            requests = await _published_requests(broker, 2, handle)
            requests_by_step = {request.step_name: request for request in requests}
            for step_name, item in (("check[1]", "beta"), ("check[0]", "alpha")):
                response = _response(
                    requests_by_step[step_name],
                    StepSuccessBody(output={"processed": item}),
                )
                await broker.publish(
                    RESPONSE_DESTINATION,
                    PublishedMessage(
                        body=response.model_dump_json(),
                        message_id=response.message_id,
                    ),
                )
            await _relay(broker, env.client)._poll_once()
            result = await handle.result()

        assert result["steps"]["check"]["output"] == [
            {"processed": "alpha"},
            {"processed": "beta"},
        ]

    async def test_signal_wait_times_out(self):
        broker = _registered_test_broker()
        services = _queue_service(broker, timeout_sec=SIGNAL_WAIT_TIMEOUT_SEC)
        workflow_class, definition_digest = _compile(_single_queue_workflow(), services)
        activities = WorkflowActivities(services=dict(services.configured))

        async with (
            await start_local_environment() as env,
            Worker(
                env.client,
                task_queue=TASK_QUEUE,
                workflows=[workflow_class],
                activities=[activities.execute_step],
                workflow_runner=workflow_sandbox_runner(),
                deployment_config=worker_deployment_config(DEPLOYMENT),
            ),
        ):
            await wait_for_worker_deployment(env.client, DEPLOYMENT, TASK_QUEUE)
            handle = await retry_pinned_workflow_start(
                lambda: env.client.start_workflow(
                    workflow_class.run,
                    _workflow_trigger("queue-timeout", definition_digest),
                    id="queue-timeout-workflow",
                    task_queue=TASK_QUEUE,
                    versioning_override=VERSIONING_OVERRIDE,
                ),
                DEPLOYMENT,
                TASK_QUEUE,
            )

            with pytest.raises(WorkflowFailureError) as exc_info:
                await handle.result()

        cause = exc_info.value.cause
        assert isinstance(cause, ApplicationError)
        assert cause.type == "STEP_FAILED"
        assert cause.details[0]["error"]["cause_code"] == "SIGNAL_TIMEOUT"

    async def test_error_response_fails_step(self):
        broker = _registered_test_broker()
        services = _queue_service(broker, timeout_sec=LONG_SIGNAL_WAIT_TIMEOUT_SEC)
        workflow_class, definition_digest = _compile(_single_queue_workflow(), services)
        activities = WorkflowActivities(services=dict(services.configured))

        async with (
            await start_local_environment() as env,
            Worker(
                env.client,
                task_queue=TASK_QUEUE,
                workflows=[workflow_class],
                activities=[activities.execute_step],
                workflow_runner=workflow_sandbox_runner(),
                deployment_config=worker_deployment_config(DEPLOYMENT),
            ),
        ):
            await wait_for_worker_deployment(env.client, DEPLOYMENT, TASK_QUEUE)
            handle = await retry_pinned_workflow_start(
                lambda: env.client.start_workflow(
                    workflow_class.run,
                    _workflow_trigger("queue-error", definition_digest),
                    id="queue-error-workflow",
                    task_queue=TASK_QUEUE,
                    versioning_override=VERSIONING_OVERRIDE,
                ),
                DEPLOYMENT,
                TASK_QUEUE,
            )
            request = (await _published_requests(broker, 1, handle))[0]
            response = _response(
                request,
                StepErrorBody(
                    code="TIMEOUT",
                    message="remote service unavailable",
                    retryable=False,
                ),
            )
            await broker.publish(
                RESPONSE_DESTINATION,
                PublishedMessage(
                    body=response.model_dump_json(),
                    message_id=response.message_id,
                ),
            )
            await _relay(broker, env.client)._poll_once()

            with pytest.raises(WorkflowFailureError) as exc_info:
                await handle.result()

        cause = exc_info.value.cause
        assert isinstance(cause, ApplicationError)
        assert cause.details[0]["error"]["cause_code"] == "STEP_FAILED"

    async def test_cancelling_parallel_iteration_cancels_the_workflow(self):
        broker = _registered_test_broker()
        services = _queue_service(broker, timeout_sec=LONG_SIGNAL_WAIT_TIMEOUT_SEC)
        workflow_class, definition_digest = _compile(_parallel_queue_workflow(), services)
        activities = WorkflowActivities(services=dict(services.configured))

        async with (
            await start_local_environment() as env,
            Worker(
                env.client,
                task_queue=TASK_QUEUE,
                workflows=[workflow_class],
                activities=[activities.execute_step],
                workflow_runner=workflow_sandbox_runner(),
                deployment_config=worker_deployment_config(DEPLOYMENT),
            ),
        ):
            await wait_for_worker_deployment(env.client, DEPLOYMENT, TASK_QUEUE)
            handle = await retry_pinned_workflow_start(
                lambda: env.client.start_workflow(
                    workflow_class.run,
                    _workflow_trigger(
                        "queue-cancel",
                        definition_digest,
                        items=["alpha", "beta"],
                    ),
                    id="queue-cancel-workflow",
                    task_queue=TASK_QUEUE,
                    versioning_override=VERSIONING_OVERRIDE,
                ),
                DEPLOYMENT,
                TASK_QUEUE,
            )
            await _published_requests(broker, MAX_CONCURRENCY, handle)

            await handle.cancel()
            with pytest.raises(WorkflowFailureError) as exc_info:
                await handle.result()

        assert isinstance(exc_info.value.cause, CancelledError)
