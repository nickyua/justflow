"""Tests for broker-neutral workflow trigger ingress."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import AsyncMock

import pytest

from justflow.brokers import Ack, DeadLetter, ProcessingOutcome, ReceivedMessage, Retry
from justflow.config.runtime_limits import DEFAULT_RUNTIME_LIMITS, RuntimeLimits
from justflow.config.settings import ConsumerReconnectSettings
from justflow.engine.trigger_ingress import ConsumerReadinessError, TriggerIngress
from justflow.provenance import WorkerArtifactIdentity
from justflow.runtime import (
    BrokerSourceIdentity,
    CloudEventErrorCode,
    CloudEventIngress,
    CloudEventMappingError,
    CloudEventMappingRegistry,
    CloudEventSourceIdentity,
    EventBridgeEventMapper,
    MetricsRegistry,
    StartErrorCode,
    StartStatus,
    StartWorkflowRequest,
    StartWorkflowResult,
    WorkflowStarter,
    WorkflowStartError,
)
from justflow.sdk.message_contract import (
    PROTOCOL_VERSION,
    MessageKind,
    TriggerEnvelope,
    make_trigger_message_id,
    make_workflow_id,
)

SOURCE_NAME = "test_broker"
CLOUD_EVENT_MAPPING = "eventbridge"
TINY_BYTE_LIMIT = 1
SENSITIVE_SENTINEL = "synthetic-trigger-secret"
DEFINITION_DIGEST = "d" * 64
SOURCE_IDENTITY_DIGEST = "b" * 64
ENVIRONMENT_SNAPSHOT_DIGEST = "e" * 64
READINESS_TIMEOUT_SECONDS = 1.0
ARTIFACT_IDENTITY = WorkerArtifactIdentity(
    deployment_name="justflow",
    build_id="test-build",
    artifact_digest=f"sha256:{'a' * 64}",
    package_version="0.1.0",
)
EVENTBRIDGE_BODY = json.dumps(
    {
        "version": "0",
        "id": "event-1",
        "detail-type": "Order Created",
        "source": "com.example.orders",
        "account": "123456789012",
        "time": "2026-08-06T08:30:00Z",
        "region": "eu-central-1",
        "resources": [],
        "detail": {"order_id": "order-1"},
    }
)


@dataclass
class FakeConsumer:
    messages: list[ReceivedMessage] = field(default_factory=list)
    settlements: list[tuple[ReceivedMessage, ProcessingOutcome]] = field(default_factory=list)

    async def receive(self) -> tuple[ReceivedMessage, ...]:
        messages = tuple(self.messages)
        self.messages.clear()
        return messages

    async def settle(
        self,
        message: ReceivedMessage,
        outcome: ProcessingOutcome,
    ) -> None:
        self.settlements.append((message, outcome))

    async def close(self) -> None:
        return None


class ReadinessConsumer(FakeConsumer):
    def __init__(self) -> None:
        super().__init__()
        self._polls = 0
        self._closed = asyncio.Event()

    async def receive(self) -> tuple[ReceivedMessage, ...]:
        self._polls += 1
        if self._polls == 1:
            await asyncio.sleep(0)
            return ()
        await self._closed.wait()
        return ()

    async def close(self) -> None:
        self._closed.set()


class FlakyConsumer(FakeConsumer):
    def __init__(self) -> None:
        super().__init__()
        self._effects: list[Exception | tuple[ReceivedMessage, ...]] = [
            RuntimeError("broker unavailable"),
            RuntimeError("broker unavailable"),
            (),
            RuntimeError("broker unavailable"),
        ]
        self._closed = asyncio.Event()

    async def receive(self) -> tuple[ReceivedMessage, ...]:
        if self._effects:
            effect = self._effects.pop(0)
            if isinstance(effect, Exception):
                raise effect
            return effect
        await self._closed.wait()
        return ()

    async def close(self) -> None:
        self._closed.set()


def trigger_body(**overrides: Any) -> str:
    business_request_id = "req-1"
    workflow_name = "record_flow"
    values: dict[str, Any] = TriggerEnvelope(
        protocol_version=PROTOCOL_VERSION,
        message_id=make_trigger_message_id(
            workflow_name,
            DEFINITION_DIGEST,
            business_request_id,
        ),
        kind=MessageKind.TRIGGER,
        workflow_name=workflow_name,
        definition_digest=DEFINITION_DIGEST,
        workflow_id=make_workflow_id(workflow_name, business_request_id),
        correlation_id=business_request_id,
        causation_id=None,
        trace_id="trace-1",
        business_request_id=business_request_id,
        input={"source": "test"},
    ).model_dump(mode="json")
    values.update(overrides)
    return json.dumps(values)


def received(body: str, *, attempt: int = 1) -> ReceivedMessage:
    return ReceivedMessage(
        body=body,
        broker_message_id="broker-1",
        delivery_attempt=attempt,
        settlement_token="token-1",
    )


def start_result(status: StartStatus = StartStatus.STARTED) -> StartWorkflowResult:
    return StartWorkflowResult(
        workflow_id=make_workflow_id("record_flow", "req-1"),
        run_id="run-1",
        workflow_name="record_flow",
        definition_digest=DEFINITION_DIGEST,
        artifact_identity=ARTIFACT_IDENTITY,
        environment_snapshot_digest=ENVIRONMENT_SNAPSHOT_DIGEST,
        trigger_name="broker_trigger",
        source_identity_digest=SOURCE_IDENTITY_DIGEST,
        status=status,
    )


def make_ingress(
    consumer: FakeConsumer,
    starter: AsyncMock,
    limits: RuntimeLimits = DEFAULT_RUNTIME_LIMITS,
    metrics: MetricsRegistry | None = None,
) -> TriggerIngress:
    return TriggerIngress(
        workflow_starter=starter,
        consumer=consumer,
        source_name=SOURCE_NAME,
        limits=limits,
        metrics=metrics,
    )


def make_cloud_event_ingress(
    consumer: FakeConsumer,
    starter: AsyncMock,
    *,
    limits: RuntimeLimits = DEFAULT_RUNTIME_LIMITS,
) -> TriggerIngress:
    registry = CloudEventMappingRegistry()
    registry.register(
        CLOUD_EVENT_MAPPING,
        EventBridgeEventMapper(
            mapping_name=CLOUD_EVENT_MAPPING,
            workflow_name="record_flow",
            source="com.example.orders",
            detail_type="Order Created",
        ),
    )
    return TriggerIngress(
        workflow_starter=starter,
        consumer=consumer,
        source_name=SOURCE_NAME,
        limits=limits,
        cloud_event_ingress=CloudEventIngress(registry, starter),
        cloud_event_mapping=CLOUD_EVENT_MAPPING,
    )


class TestTriggerIngress:
    async def test_poll_retry_backoff_resets_after_success(self) -> None:
        consumer = FlakyConsumer()
        delays: list[float] = []
        third_retry = asyncio.Event()

        async def record_sleep(delay: float) -> None:
            delays.append(delay)
            if len(delays) == 3:
                third_retry.set()

        ingress = TriggerIngress(
            workflow_starter=AsyncMock(spec=WorkflowStarter),
            consumer=consumer,
            source_name=SOURCE_NAME,
            reconnect_policy=ConsumerReconnectSettings(
                initial_delay_seconds=0.1,
                max_delay_seconds=1,
                multiplier=2,
                jitter_fraction=0,
            ),
            sleep=record_sleep,
        )
        task = asyncio.create_task(ingress.start())

        await asyncio.wait_for(third_retry.wait(), timeout=READINESS_TIMEOUT_SECONDS)
        await ingress.stop()
        await task

        assert delays == [0.1, 0.2, 0.1]

    async def test_readiness_requires_successful_poll_and_clears_on_stop(self) -> None:
        consumer = ReadinessConsumer()
        readiness: list[bool] = []
        ingress = TriggerIngress(
            workflow_starter=AsyncMock(spec=WorkflowStarter),
            consumer=consumer,
            source_name=SOURCE_NAME,
            readiness_callback=readiness.append,
        )
        task = asyncio.create_task(ingress.start())

        await ingress.wait_ready(READINESS_TIMEOUT_SECONDS)
        await ingress.stop()
        await task

        assert readiness == [True, False]

    async def test_readiness_wait_is_bounded(self) -> None:
        ingress = TriggerIngress(
            workflow_starter=AsyncMock(spec=WorkflowStarter),
            consumer=FakeConsumer(),
            source_name=SOURCE_NAME,
        )

        with pytest.raises(ConsumerReadinessError, match="startup bound"):
            await ingress.wait_ready(0)

    async def test_queue_metrics_record_outcome_without_message_identity(self, caplog) -> None:
        metrics = MetricsRegistry()
        consumer = FakeConsumer(messages=[received(trigger_body(), attempt=2)])
        starter = AsyncMock(spec=WorkflowStarter)
        starter.start.return_value = start_result()

        await make_ingress(consumer, starter, metrics=metrics)._poll_once()

        rendered = metrics.render_prometheus().decode()
        assert 'kind="trigger",outcome="ack",redelivered="true"} 1' in rendered
        assert "broker-1" not in rendered
        assert "broker-1" not in caplog.text

    async def test_valid_trigger_calls_shared_starter_then_acknowledges(self):
        consumer = FakeConsumer(messages=[received(trigger_body())])
        starter = AsyncMock(spec=WorkflowStarter)
        starter.start.return_value = start_result()

        await make_ingress(consumer, starter)._poll_once()

        request = starter.start.await_args.args[0]
        assert isinstance(request, StartWorkflowRequest)
        assert request.workflow_name == "record_flow"
        assert request.business_request_id == "req-1"
        assert request.definition_digest == DEFINITION_DIGEST
        assert isinstance(request.source, BrokerSourceIdentity)
        assert request.source.broker == SOURCE_NAME
        assert isinstance(consumer.settlements[0][1], Ack)

    @pytest.mark.parametrize(
        "status",
        [
            pytest.param(StartStatus.STARTED, id="new-start"),
            pytest.param(StartStatus.DUPLICATE, id="duplicate-start"),
        ],
    )
    async def test_temporal_acceptance_or_duplicate_is_acknowledged(self, status: StartStatus):
        consumer = FakeConsumer(messages=[received(trigger_body())])
        starter = AsyncMock(spec=WorkflowStarter)
        starter.start.return_value = start_result(status)

        await make_ingress(consumer, starter)._poll_once()

        assert isinstance(consumer.settlements[0][1], Ack)

    @pytest.mark.parametrize(
        "code",
        [
            pytest.param(StartErrorCode.UNKNOWN_WORKFLOW, id="unknown-workflow"),
            pytest.param(StartErrorCode.DEFINITION_UNAVAILABLE, id="unavailable-definition"),
            pytest.param(StartErrorCode.INPUT_REJECTED, id="invalid-input"),
        ],
    )
    async def test_permanent_start_rejections_are_dead_lettered(self, code: StartErrorCode):
        consumer = FakeConsumer(messages=[received(trigger_body())])
        starter = AsyncMock(spec=WorkflowStarter)
        starter.start.side_effect = WorkflowStartError(code, "rejected", retryable=False)

        await make_ingress(consumer, starter)._poll_once()

        assert isinstance(consumer.settlements[0][1], DeadLetter)

    async def test_transient_start_failure_requests_redelivery(self, caplog):
        consumer = FakeConsumer(messages=[received(trigger_body())])
        starter = AsyncMock(spec=WorkflowStarter)
        starter.start.side_effect = WorkflowStartError(
            StartErrorCode.TEMPORAL_UNAVAILABLE,
            SENSITIVE_SENTINEL,
            retryable=True,
        )

        await make_ingress(consumer, starter)._poll_once()

        assert isinstance(consumer.settlements[0][1], Retry)
        assert SENSITIVE_SENTINEL not in caplog.text

    async def test_malformed_trigger_body_is_not_logged(self, caplog):
        consumer = FakeConsumer(messages=[received(f"not-json-{SENSITIVE_SENTINEL}")])
        starter = AsyncMock(spec=WorkflowStarter)

        await make_ingress(consumer, starter)._poll_once()

        starter.start.assert_not_awaited()
        assert isinstance(consumer.settlements[0][1], DeadLetter)
        assert SENSITIVE_SENTINEL not in caplog.text

    @pytest.mark.parametrize(
        "status",
        [
            pytest.param(StartStatus.STARTED, id="new-start"),
            pytest.param(StartStatus.DUPLICATE, id="duplicate-start"),
        ],
    )
    async def test_cloud_event_delivery_uses_shared_starter_and_acknowledges(
        self,
        status: StartStatus,
    ) -> None:
        consumer = FakeConsumer(messages=[received(EVENTBRIDGE_BODY)])
        starter = AsyncMock(spec=WorkflowStarter)
        starter.start.return_value = start_result(status)

        await make_cloud_event_ingress(consumer, starter)._poll_once()

        request = starter.start.await_args.args[0]
        scope_binding = starter.start.await_args.kwargs["scope_binding"]
        assert isinstance(request, StartWorkflowRequest)
        assert isinstance(request.source, CloudEventSourceIdentity)
        assert request.source.mapping == CLOUD_EVENT_MAPPING
        assert scope_binding.kind.value == "cloud_event"
        assert isinstance(consumer.settlements[0][1], Ack)

    async def test_malformed_cloud_event_is_dead_lettered(self) -> None:
        event = json.loads(EVENTBRIDGE_BODY)
        event["unsupported"] = "value"
        consumer = FakeConsumer(messages=[received(json.dumps(event))])
        starter = AsyncMock(spec=WorkflowStarter)

        await make_cloud_event_ingress(consumer, starter)._poll_once()

        starter.start.assert_not_awaited()
        outcome = consumer.settlements[0][1]
        assert isinstance(outcome, DeadLetter)
        assert outcome.reason == "invalid cloud-event payload"

    async def test_duplicate_cloud_event_json_key_is_dead_lettered(self) -> None:
        duplicate_key_body = EVENTBRIDGE_BODY.replace(
            '"version": "0",',
            '"version": "0", "version": "0",',
        )
        consumer = FakeConsumer(messages=[received(duplicate_key_body)])
        starter = AsyncMock(spec=WorkflowStarter)

        await make_cloud_event_ingress(consumer, starter)._poll_once()

        starter.start.assert_not_awaited()
        assert isinstance(consumer.settlements[0][1], DeadLetter)

    @pytest.mark.parametrize(
        ("retryable", "expected_type"),
        [
            pytest.param(False, DeadLetter, id="permanent"),
            pytest.param(True, Retry, id="transient"),
        ],
    )
    async def test_cloud_event_mapping_controls_delivery_disposition(
        self,
        retryable: bool,
        expected_type: type[Retry | DeadLetter],
    ) -> None:
        consumer = FakeConsumer(messages=[received(EVENTBRIDGE_BODY)])
        starter = AsyncMock(spec=WorkflowStarter)
        cloud_events = AsyncMock(spec=CloudEventIngress)
        cloud_events.receive.side_effect = CloudEventMappingError(
            "safe",
            code=(
                CloudEventErrorCode.MAPPING_UNAVAILABLE
                if retryable
                else CloudEventErrorCode.INVALID_PAYLOAD
            ),
            retryable=retryable,
            dead_letter_reason="source-specific disposition",
        )
        ingress = TriggerIngress(
            workflow_starter=starter,
            consumer=consumer,
            source_name=SOURCE_NAME,
            cloud_event_ingress=cloud_events,
            cloud_event_mapping=CLOUD_EVENT_MAPPING,
        )

        await ingress._poll_once()

        outcome = consumer.settlements[0][1]
        assert isinstance(outcome, expected_type)
        assert isinstance(outcome, (Retry, DeadLetter))
        assert outcome.reason == "source-specific disposition"

    async def test_oversized_trigger_is_dead_lettered_before_starter_call(self):
        consumer = FakeConsumer(messages=[received(trigger_body())])
        starter = AsyncMock(spec=WorkflowStarter)
        ingress = make_ingress(
            consumer,
            starter,
            limits=RuntimeLimits(trigger_payload_bytes=TINY_BYTE_LIMIT),
        )

        await ingress._poll_once()

        starter.start.assert_not_awaited()
        assert isinstance(consumer.settlements[0][1], DeadLetter)
