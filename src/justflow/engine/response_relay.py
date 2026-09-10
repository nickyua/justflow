"""Broker-neutral response and event relay."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable

from pydantic import ValidationError
from temporalio.client import Client
from temporalio.service import RPCError, RPCStatusCode

from justflow.brokers import (
    Ack,
    DeadLetter,
    MessageConsumer,
    ProcessingOutcome,
    ReceivedMessage,
    Retry,
)
from justflow.config.runtime_limits import DEFAULT_RUNTIME_LIMITS, RuntimeLimits
from justflow.config.settings import (
    DEFAULT_MESSAGE_CONCURRENCY,
    ConsumerReconnectSettings,
)
from justflow.engine.consumer_backoff import ConsumerReconnectBackoff
from justflow.engine.deduplication import BoundedDeduplicationStore, DeliveryClaim, DuplicateState
from justflow.engine.limits import LimitExceededError, enforce_utf8_bytes
from justflow.runtime.metrics import (
    MetricsRegistry,
    QueueMetricKind,
    QueueMetricOutcome,
)
from justflow.scope import (
    LEGACY_LOCAL_UNSCOPED_POLICY,
    LOCAL_RUNTIME_SCOPE,
    RuntimeScope,
    identity_belongs_to_scope,
)
from justflow.sdk.logging_context import identity_log_digest, logging_context
from justflow.sdk.message_contract import (
    STEP_RESPONSE_SIGNAL,
    WORKFLOW_EVENT_SIGNAL,
    EventEnvelope,
    EventPayload,
    SignalPayload,
    StepResponseEnvelope,
    StepSuccessBody,
    parse_async_envelope,
)

logger = logging.getLogger(__name__)


class ConsumerReadinessError(RuntimeError):
    pass


class ResponseRelay:
    def __init__(
        self,
        temporal_client: Client,
        consumer: MessageConsumer,
        deduplication_store: BoundedDeduplicationStore,
        *,
        message_concurrency: int = DEFAULT_MESSAGE_CONCURRENCY,
        limits: RuntimeLimits = DEFAULT_RUNTIME_LIMITS,
        metrics: MetricsRegistry | None = None,
        readiness_callback: Callable[[bool], None] | None = None,
        scope: RuntimeScope = LOCAL_RUNTIME_SCOPE,
        reconnect_policy: ConsumerReconnectSettings | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        jitter_source: Callable[[], float] | None = None,
    ) -> None:
        if message_concurrency < 1:
            raise ValueError("Message concurrency must be positive")
        self._client = temporal_client
        self._consumer = consumer
        self._deduplication_store = deduplication_store
        self._message_semaphore = asyncio.Semaphore(message_concurrency)
        self._running = False
        self._limits = limits
        self._metrics = metrics
        self._readiness_callback = readiness_callback
        self._ready = asyncio.Event()
        self._reported_ready = False
        self._scope = scope
        reconnect = reconnect_policy or ConsumerReconnectSettings()
        self._reconnect_backoff = ConsumerReconnectBackoff(
            reconnect,
            **({"jitter_source": jitter_source} if jitter_source is not None else {}),
        )
        self._sleep = sleep

    async def start(self) -> None:
        self._running = True
        logger.info("Response relay started")
        while self._running:
            try:
                await self._poll_once()
                self._reconnect_backoff.reset()
                if self._running:
                    self._set_ready(True)
            except Exception as exc:  # noqa: BLE001 - background service boundary
                self._set_ready(False)
                retry_delay = self._reconnect_backoff.next_delay()
                logger.error(
                    "Response relay poll failed",
                    extra={
                        "exception_type": type(exc).__name__,
                        "consecutive_failures": self._reconnect_backoff.failure_count,
                        "retry_delay_seconds": retry_delay,
                    },
                )
                await self._sleep(retry_delay)

    async def stop(self) -> None:
        self._running = False
        self._set_ready(False)
        await self._consumer.close()
        await self._deduplication_store.clear()

    async def wait_ready(self, timeout_seconds: float) -> None:
        try:
            await asyncio.wait_for(self._ready.wait(), timeout=timeout_seconds)
        except TimeoutError:
            raise ConsumerReadinessError(
                "Response consumer did not become ready within its startup bound"
            ) from None

    def _set_ready(self, ready: bool) -> None:
        if ready:
            self._ready.set()
        else:
            self._ready.clear()
        if ready == self._reported_ready:
            return
        self._reported_ready = ready
        if self._readiness_callback is not None:
            self._readiness_callback(ready)

    async def _poll_once(self) -> None:
        messages = await self._consumer.receive()
        results = await asyncio.gather(
            *(self._handle_delivery(message) for message in messages),
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, BaseException):
                logger.error(
                    "Response delivery settlement failed",
                    extra={"exception_type": type(result).__name__},
                )

    async def _handle_delivery(self, message: ReceivedMessage) -> None:
        async with self._message_semaphore:
            outcome = await self._process_message(message)
            if self._metrics is not None:
                self._metrics.record_queue(
                    QueueMetricKind.RESPONSE,
                    _queue_metric_outcome(outcome),
                    redelivered=message.delivery_attempt > 1,
                )
            logger.info(
                "Response delivery processed",
                extra={
                    "broker_message_id_digest": identity_log_digest(message.broker_message_id),
                    "delivery_attempt": message.delivery_attempt,
                    "outcome": outcome.kind,
                },
            )
            await self._consumer.settle(message, outcome)

    async def _process_message(self, message: ReceivedMessage) -> ProcessingOutcome:
        try:
            enforce_utf8_bytes(
                message.body,
                boundary="messaging.response.body",
                limit=self._limits.signal_payload_bytes,
            )
            envelope = parse_async_envelope(json.loads(message.body))
            if not isinstance(envelope, (StepResponseEnvelope, EventEnvelope)):
                raise TypeError(f"Unsupported response kind '{envelope.kind.value}'")
            scoped = envelope.scope_digest == self._scope.digest and identity_belongs_to_scope(
                envelope.workflow_id, "workflow", self._scope
            )
            if (
                not LEGACY_LOCAL_UNSCOPED_POLICY.owns(envelope.scope_digest, self._scope)
                and not scoped
            ):
                raise TypeError("Response envelope does not belong to the runtime scope")
        except (json.JSONDecodeError, ValidationError, LimitExceededError, TypeError) as exc:
            logger.warning(
                "Response delivery is malformed",
                extra={
                    "broker_message_id_digest": identity_log_digest(message.broker_message_id),
                    "exception_type": type(exc).__name__,
                },
            )
            return DeadLetter(reason="malformed response envelope")

        claimed = await self._deduplication_store.claim(envelope.message_id)
        if claimed is DuplicateState.COMPLETED:
            logger.info(
                "Duplicate response delivery ignored",
                extra={"message_id_digest": identity_log_digest(envelope.message_id)},
            )
            return Ack()
        if not isinstance(claimed, DeliveryClaim):
            return Retry(reason="response delivery is in flight or capacity is exhausted")
        try:
            outcome = await self._deliver(envelope)
            if isinstance(outcome, Ack):
                await self._deduplication_store.complete(claimed)
            return outcome
        finally:
            await asyncio.shield(self._deduplication_store.release(claimed))

    async def _deliver(
        self,
        envelope: StepResponseEnvelope | EventEnvelope,
    ) -> ProcessingOutcome:
        with logging_context(
            request_id=identity_log_digest(envelope.correlation_id),
            flow_name=envelope.workflow_name,
        ):
            try:
                handle = self._client.get_workflow_handle(
                    workflow_id=envelope.workflow_id,
                    run_id=envelope.workflow_run_id,
                )
                if isinstance(envelope, StepResponseEnvelope):
                    error = None
                    step_response = None
                    if isinstance(envelope.body, StepSuccessBody):
                        step_response = envelope.body.output
                    else:
                        error = {
                            "code": envelope.body.code,
                            "message": envelope.body.message,
                            "retryable": envelope.body.retryable,
                        }
                    await handle.signal(
                        STEP_RESPONSE_SIGNAL,
                        SignalPayload(
                            request_id=envelope.step_invocation_id,
                            step_name=envelope.step_name,
                            action=envelope.action,
                            status=envelope.body.status,
                            step_response=step_response,
                            error=error,
                        ).model_dump(mode="json"),
                    )
                else:
                    await handle.signal(
                        WORKFLOW_EVENT_SIGNAL,
                        EventPayload(
                            signal=envelope.event_name,
                            data=envelope.payload,
                        ).model_dump(mode="json"),
                    )
            except RPCError as exc:
                if exc.status == RPCStatusCode.NOT_FOUND:
                    logger.warning("Response targets a missing workflow execution")
                    return DeadLetter(reason="workflow execution not found")
                logger.warning(
                    "Transient Temporal signal failure",
                    extra={"rpc_status": exc.status.name},
                )
                return Retry(reason="Temporal signal failed")
            except Exception as exc:  # noqa: BLE001 - Temporal client boundary
                logger.warning(
                    "Temporal signal failed",
                    extra={"exception_type": type(exc).__name__},
                )
                return Retry(reason="Temporal signal failed")
            logger.info(
                "Response delivered to workflow",
                extra={"workflow_run_id": envelope.workflow_run_id},
            )
            return Ack()


def _queue_metric_outcome(outcome: ProcessingOutcome) -> QueueMetricOutcome:
    if isinstance(outcome, Ack):
        return QueueMetricOutcome.ACK
    if isinstance(outcome, Retry):
        return QueueMetricOutcome.RETRY
    return QueueMetricOutcome.DEAD_LETTER
