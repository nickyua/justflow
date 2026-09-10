"""Tests for broker-neutral workflow response delivery."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from temporalio.service import RPCError, RPCStatusCode

from justflow.brokers import Ack, DeadLetter, ProcessingOutcome, ReceivedMessage, Retry
from justflow.config.runtime_limits import DEFAULT_RUNTIME_LIMITS, RuntimeLimits
from justflow.config.settings import ConsumerReconnectSettings
from justflow.engine.deduplication import BoundedDeduplicationStore, DeliveryClaim
from justflow.engine.response_relay import ConsumerReadinessError, ResponseRelay
from justflow.runtime.metrics import MetricsRegistry
from justflow.sdk.message_contract import (
    PROTOCOL_VERSION,
    STEP_RESPONSE_SIGNAL,
    WORKFLOW_EVENT_SIGNAL,
    EventEnvelope,
    MessageKind,
    StepResponseEnvelope,
    StepSuccessBody,
    make_step_invocation_id,
    make_step_request_message_id,
)

TINY_BYTE_LIMIT = 1
SENSITIVE_SENTINEL = "synthetic-response-secret"
READINESS_TIMEOUT_SECONDS = 1.0


@pytest.mark.parametrize(
    "retention_seconds",
    [
        pytest.param(0.0, id="zero"),
        pytest.param(-1.0, id="negative"),
        pytest.param(float("nan"), id="not-a-number"),
        pytest.param(float("inf"), id="infinite"),
    ],
)
def test_deduplication_store_requires_finite_positive_retention(
    retention_seconds: float,
) -> None:
    with pytest.raises(ValueError, match="finite positive"):
        BoundedDeduplicationStore(capacity=1, retention_seconds=retention_seconds)


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


def response_body(*, message_id: str = "response-1") -> str:
    step_name = "check[0]"
    action = "process"
    step_invocation_id = make_step_invocation_id(
        "workflow-1",
        "run-1",
        step_name,
    )
    request_message_id = make_step_request_message_id(step_invocation_id)
    return StepResponseEnvelope(
        protocol_version=PROTOCOL_VERSION,
        message_id=message_id,
        kind=MessageKind.STEP_RESPONSE,
        workflow_name="flow",
        definition_digest="a" * 64,
        workflow_id="workflow-1",
        correlation_id="request-1",
        causation_id=request_message_id,
        trace_id="trace-1",
        workflow_run_id="run-1",
        in_reply_to=request_message_id,
        step_invocation_id=step_invocation_id,
        step_name=step_name,
        action=action,
        body=StepSuccessBody(output={"state": "ready"}),
    ).model_dump_json()


def event_body() -> str:
    return EventEnvelope(
        protocol_version=PROTOCOL_VERSION,
        message_id="event-1",
        kind=MessageKind.EVENT,
        workflow_name="flow",
        definition_digest="a" * 64,
        workflow_id="workflow-1",
        correlation_id="request-1",
        trace_id="trace-1",
        workflow_run_id="run-1",
        event_name="data_received",
        payload={"values": [1, 2]},
    ).model_dump_json()


def received(body: str, *, broker_message_id: str = "broker-1") -> ReceivedMessage:
    return ReceivedMessage(
        body=body,
        broker_message_id=broker_message_id,
        delivery_attempt=1,
        settlement_token=f"token-{broker_message_id}",
    )


def make_relay(
    consumer: FakeConsumer,
    temporal_client: Any,
    limits: RuntimeLimits = DEFAULT_RUNTIME_LIMITS,
    metrics: MetricsRegistry | None = None,
) -> ResponseRelay:
    return ResponseRelay(
        temporal_client=temporal_client,
        consumer=consumer,
        deduplication_store=BoundedDeduplicationStore(
            capacity=100,
            retention_seconds=3_600,
        ),
        limits=limits,
        metrics=metrics,
    )


class TestResponseRelay:
    @pytest.mark.parametrize("first_fails", [False, True], ids=["success", "failure"])
    async def test_duplicate_during_delivery_is_retried_until_delivery_finishes(
        self, first_fails: bool
    ) -> None:
        entered = asyncio.Event()
        finish = asyncio.Event()
        consumer = FakeConsumer()
        client = MagicMock()
        handle = AsyncMock()

        async def signal(*args: object, **kwargs: object) -> None:
            entered.set()
            await finish.wait()
            if first_fails:
                raise RuntimeError("delivery failed")

        handle.signal.side_effect = signal
        client.get_workflow_handle.return_value = handle
        relay = make_relay(consumer, client)
        first = asyncio.create_task(relay._handle_delivery(received(response_body())))
        await entered.wait()
        try:
            await relay._handle_delivery(received(response_body(), broker_message_id="duplicate"))
            assert isinstance(consumer.settlements[0][1], Retry)
        finally:
            finish.set()
            await first
        handle.signal.side_effect = None
        await relay._handle_delivery(received(response_body(), broker_message_id="retry"))
        assert isinstance(consumer.settlements[-1][1], Ack)
        assert handle.signal.await_count == (2 if first_fails else 1)

    async def test_cancelled_delivery_releases_ownership(self) -> None:
        entered = asyncio.Event()
        consumer = FakeConsumer()
        client = MagicMock()
        handle = AsyncMock()

        async def signal(*args: object, **kwargs: object) -> None:
            entered.set()
            await asyncio.Future()

        handle.signal.side_effect = signal
        client.get_workflow_handle.return_value = handle
        relay = make_relay(consumer, client)
        first = asyncio.create_task(relay._handle_delivery(received(response_body())))
        await entered.wait()
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        handle.signal.side_effect = None
        await relay._handle_delivery(received(response_body()))
        assert handle.signal.await_count == 2
        assert isinstance(consumer.settlements[-1][1], Ack)

    async def test_poll_retry_backoff_resets_after_success(self) -> None:
        consumer = FlakyConsumer()
        delays: list[float] = []
        third_retry = asyncio.Event()

        async def record_sleep(delay: float) -> None:
            delays.append(delay)
            if len(delays) == 3:
                third_retry.set()

        relay = ResponseRelay(
            temporal_client=MagicMock(),
            consumer=consumer,
            deduplication_store=BoundedDeduplicationStore(
                capacity=10,
                retention_seconds=60,
            ),
            reconnect_policy=ConsumerReconnectSettings(
                initial_delay_seconds=0.1,
                max_delay_seconds=1,
                multiplier=2,
                jitter_fraction=0,
            ),
            sleep=record_sleep,
        )
        task = asyncio.create_task(relay.start())

        await asyncio.wait_for(third_retry.wait(), timeout=READINESS_TIMEOUT_SECONDS)
        await relay.stop()
        await task

        assert delays == [0.1, 0.2, 0.1]

    async def test_readiness_requires_successful_poll_and_clears_on_stop(self) -> None:
        consumer = ReadinessConsumer()
        readiness: list[bool] = []
        relay = ResponseRelay(
            temporal_client=MagicMock(),
            consumer=consumer,
            deduplication_store=BoundedDeduplicationStore(
                capacity=10,
                retention_seconds=60,
            ),
            readiness_callback=readiness.append,
        )
        task = asyncio.create_task(relay.start())

        await relay.wait_ready(READINESS_TIMEOUT_SECONDS)
        await relay.stop()
        await task

        assert readiness == [True, False]

    async def test_readiness_wait_is_bounded(self) -> None:
        relay = ResponseRelay(
            temporal_client=MagicMock(),
            consumer=FakeConsumer(),
            deduplication_store=BoundedDeduplicationStore(
                capacity=10,
                retention_seconds=60,
            ),
        )

        with pytest.raises(ConsumerReadinessError, match="startup bound"):
            await relay.wait_ready(0)

    async def test_queue_metrics_record_response_outcome_without_identity(self, caplog) -> None:
        metrics = MetricsRegistry()
        consumer = FakeConsumer(messages=[received(response_body())])
        client = MagicMock()
        client.get_workflow_handle.return_value = AsyncMock()

        await make_relay(consumer, client, metrics=metrics)._poll_once()

        rendered = metrics.render_prometheus().decode()
        assert 'kind="response",outcome="ack",redelivered="false"} 1' in rendered
        assert "broker-1" not in rendered
        assert "broker-1" not in caplog.text

    async def test_stop_clears_process_local_deduplication_state(self):
        consumer = FakeConsumer()
        store = BoundedDeduplicationStore(capacity=10, retention_seconds=60)
        relay = ResponseRelay(
            temporal_client=MagicMock(),
            consumer=consumer,
            deduplication_store=store,
        )
        assert isinstance(await store.claim("message-1"), DeliveryClaim)

        await relay.stop()

        assert isinstance(await store.claim("message-1"), DeliveryClaim)

    async def test_valid_response_signals_exact_run_then_acknowledges(self):
        consumer = FakeConsumer(messages=[received(response_body())])
        client = MagicMock()
        handle = AsyncMock()
        client.get_workflow_handle.return_value = handle

        await make_relay(consumer, client)._poll_once()

        client.get_workflow_handle.assert_called_once_with(
            workflow_id="workflow-1",
            run_id="run-1",
        )
        signal_name, signal_data = handle.signal.await_args.args
        assert signal_name == STEP_RESPONSE_SIGNAL
        assert signal_data == {
            "request_id": make_step_invocation_id("workflow-1", "run-1", "check[0]"),
            "step_name": "check[0]",
            "action": "process",
            "status": "success",
            "step_response": {"state": "ready"},
            "error": None,
        }
        assert isinstance(consumer.settlements[0][1], Ack)

    async def test_event_signals_exact_run_then_acknowledges(self):
        consumer = FakeConsumer(messages=[received(event_body())])
        client = MagicMock()
        handle = AsyncMock()
        client.get_workflow_handle.return_value = handle

        await make_relay(consumer, client)._poll_once()

        client.get_workflow_handle.assert_called_once_with(
            workflow_id="workflow-1",
            run_id="run-1",
        )
        signal_name, payload = handle.signal.await_args.args
        assert signal_name == WORKFLOW_EVENT_SIGNAL
        assert payload == {
            "signal": "data_received",
            "data": {"values": [1, 2]},
        }
        assert isinstance(consumer.settlements[0][1], Ack)

    @pytest.mark.parametrize(
        "body",
        [
            pytest.param("not json", id="malformed-json"),
            pytest.param('{"kind":"unknown"}', id="unknown-kind"),
            pytest.param(
                '{"protocol_version":"1","kind":"trigger"}',
                id="wrong-channel-kind",
            ),
        ],
    )
    async def test_malformed_responses_are_dead_lettered(self, body: str):
        consumer = FakeConsumer(messages=[received(body)])
        client = MagicMock()

        await make_relay(consumer, client)._poll_once()

        client.get_workflow_handle.assert_not_called()
        assert isinstance(consumer.settlements[0][1], DeadLetter)

    async def test_transient_signal_failure_requests_retry_and_releases_claim(self, caplog):
        message = received(response_body())
        consumer = FakeConsumer(messages=[message])
        client = MagicMock()
        handle = AsyncMock()
        handle.signal.side_effect = [RuntimeError(SENSITIVE_SENTINEL), None]
        client.get_workflow_handle.return_value = handle
        relay = make_relay(consumer, client)

        await relay._poll_once()
        consumer.messages.append(message)
        await relay._poll_once()

        assert isinstance(consumer.settlements[0][1], Retry)
        assert isinstance(consumer.settlements[1][1], Ack)
        assert handle.signal.await_count == 2
        assert SENSITIVE_SENTINEL not in caplog.text

    async def test_malformed_response_body_is_not_logged(self, caplog):
        consumer = FakeConsumer(messages=[received(f"not-json-{SENSITIVE_SENTINEL}")])

        await make_relay(consumer, MagicMock())._poll_once()

        assert SENSITIVE_SENTINEL not in caplog.text

    async def test_missing_run_is_dead_lettered(self):
        consumer = FakeConsumer(messages=[received(response_body())])
        client = MagicMock()
        handle = AsyncMock()
        handle.signal.side_effect = RPCError("missing", RPCStatusCode.NOT_FOUND, b"")
        client.get_workflow_handle.return_value = handle

        await make_relay(consumer, client)._poll_once()

        assert isinstance(consumer.settlements[0][1], DeadLetter)

    async def test_duplicate_response_is_signaled_once_and_both_deliveries_acknowledge(self):
        body = response_body()
        consumer = FakeConsumer(
            messages=[
                received(body, broker_message_id="broker-1"),
                received(body, broker_message_id="broker-2"),
            ]
        )
        client = MagicMock()
        handle = AsyncMock()
        client.get_workflow_handle.return_value = handle

        await make_relay(consumer, client)._poll_once()

        handle.signal.assert_awaited_once()
        assert all(isinstance(outcome, Ack) for _, outcome in consumer.settlements)

    async def test_oversized_response_is_dead_lettered_before_signaling(self):
        consumer = FakeConsumer(messages=[received(response_body())])
        client = MagicMock()
        relay = make_relay(
            consumer,
            client,
            limits=RuntimeLimits(signal_payload_bytes=TINY_BYTE_LIMIT),
        )

        await relay._poll_once()

        client.get_workflow_handle.assert_not_called()
        assert isinstance(consumer.settlements[0][1], DeadLetter)
