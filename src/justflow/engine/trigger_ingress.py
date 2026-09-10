"""Broker-neutral workflow trigger ingress."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

from pydantic import ValidationError

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
from justflow.engine.limits import LimitExceededError, enforce_utf8_bytes
from justflow.engine.serialization import StrictJsonError, loads_strict_json
from justflow.runtime.cloud_events import CloudEventIngress, CloudEventMappingError
from justflow.runtime.metrics import (
    MetricsRegistry,
    QueueMetricKind,
    QueueMetricOutcome,
)
from justflow.runtime.starter import (
    BrokerSourceIdentity,
    StartWorkflowRequest,
    WorkflowStarter,
    WorkflowStartError,
)
from justflow.scope import (
    LEGACY_LOCAL_UNSCOPED_POLICY,
    LOCAL_RUNTIME_SCOPE,
    RuntimeScope,
    ScopeBindingKind,
    TrustedScopeBinding,
)
from justflow.sdk.logging_context import identity_log_digest, logging_context
from justflow.sdk.message_contract import TriggerEnvelope

logger = logging.getLogger(__name__)


class ConsumerReadinessError(RuntimeError):
    pass


class TriggerIngress:
    def __init__(
        self,
        workflow_starter: WorkflowStarter,
        consumer: MessageConsumer,
        source_name: str,
        *,
        scope: RuntimeScope = LOCAL_RUNTIME_SCOPE,
        message_concurrency: int = DEFAULT_MESSAGE_CONCURRENCY,
        limits: RuntimeLimits = DEFAULT_RUNTIME_LIMITS,
        metrics: MetricsRegistry | None = None,
        readiness_callback: Callable[[bool], None] | None = None,
        cloud_event_ingress: CloudEventIngress | None = None,
        cloud_event_mapping: str | None = None,
        reconnect_policy: ConsumerReconnectSettings | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        jitter_source: Callable[[], float] | None = None,
    ) -> None:
        if message_concurrency < 1:
            raise ValueError("Message concurrency must be positive")
        if (cloud_event_ingress is None) != (cloud_event_mapping is None):
            raise ValueError(
                "Cloud-event ingress and its broker mapping must be configured together"
            )
        self._workflow_starter = workflow_starter
        self._consumer = consumer
        self._source_name = source_name
        self._scope = scope
        self._message_semaphore = asyncio.Semaphore(message_concurrency)
        self._running = False
        self._limits = limits
        self._metrics = metrics
        self._readiness_callback = readiness_callback
        self._cloud_event_ingress = cloud_event_ingress
        self._cloud_event_mapping = cloud_event_mapping
        self._ready = asyncio.Event()
        self._reported_ready = False
        reconnect = reconnect_policy or ConsumerReconnectSettings()
        self._reconnect_backoff = ConsumerReconnectBackoff(
            reconnect,
            **({"jitter_source": jitter_source} if jitter_source is not None else {}),
        )
        self._sleep = sleep

    async def start(self) -> None:
        self._running = True
        logger.info("Trigger ingress started")
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
                    "Trigger ingress poll failed",
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

    async def wait_ready(self, timeout_seconds: float) -> None:
        try:
            await asyncio.wait_for(self._ready.wait(), timeout=timeout_seconds)
        except TimeoutError:
            raise ConsumerReadinessError(
                "Trigger consumer did not become ready within its startup bound"
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
                    "Trigger delivery settlement failed",
                    extra={"exception_type": type(result).__name__},
                )

    async def _handle_delivery(self, message: ReceivedMessage) -> None:
        async with self._message_semaphore:
            outcome = await self._process_message(message)
            if self._metrics is not None:
                self._metrics.record_queue(
                    QueueMetricKind.TRIGGER,
                    _queue_metric_outcome(outcome),
                    redelivered=message.delivery_attempt > 1,
                )
            logger.info(
                "Trigger delivery processed",
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
                boundary="messaging.trigger.body",
                limit=self._limits.trigger_payload_bytes,
            )
            payload = loads_strict_json(message.body)
        except (StrictJsonError, LimitExceededError, TypeError) as exc:
            logger.warning(
                "Trigger delivery is malformed",
                extra={
                    "broker_message_id_digest": identity_log_digest(message.broker_message_id),
                    "exception_type": type(exc).__name__,
                },
            )
            return DeadLetter(reason="malformed trigger envelope")

        cloud_event_ingress = self._cloud_event_ingress
        cloud_event_mapping = self._cloud_event_mapping
        if cloud_event_ingress is not None and cloud_event_mapping is not None:
            return await self._process_cloud_event(
                message,
                payload,
                cloud_event_ingress,
                cloud_event_mapping,
            )

        try:
            trigger = TriggerEnvelope.model_validate(payload)
        except ValidationError as exc:
            logger.warning(
                "Trigger delivery is malformed",
                extra={
                    "broker_message_id_digest": identity_log_digest(message.broker_message_id),
                    "exception_type": type(exc).__name__,
                },
            )
            return DeadLetter(reason="malformed trigger envelope")

        if not LEGACY_LOCAL_UNSCOPED_POLICY.owns(trigger.scope_digest, self._scope):
            logger.warning(
                "Trigger delivery scope does not match its trusted broker binding",
                extra={"broker_message_id_digest": identity_log_digest(message.broker_message_id)},
            )
            return DeadLetter(reason="invalid runtime scope")

        with logging_context(
            request_id=identity_log_digest(trigger.correlation_id or trigger.business_request_id),
            flow_name=trigger.workflow_name,
        ):
            try:
                result = await self._workflow_starter.start(
                    StartWorkflowRequest(
                        workflow_name=trigger.workflow_name,
                        business_request_id=trigger.business_request_id,
                        input=trigger.input,
                        source=BrokerSourceIdentity(
                            broker=self._source_name,
                            message_id=trigger.message_id,
                        ),
                        definition_digest=trigger.definition_digest,
                        correlation_id=trigger.correlation_id,
                        trace_id=trigger.trace_id,
                    ),
                    scope_binding=TrustedScopeBinding.create(
                        kind=ScopeBindingKind.BROKER,
                        scope=self._scope,
                        binding_id=self._source_name,
                    ),
                )
            except WorkflowStartError as exc:
                if not exc.retryable:
                    logger.warning(
                        "Trigger delivery cannot start a workflow",
                        extra={"start_error_code": exc.code.value},
                    )
                    return DeadLetter(reason=exc.code.value)
                logger.warning(
                    "Temporal workflow start failed",
                    extra={"start_error_code": exc.code.value},
                )
                return Retry(reason="Temporal workflow start failed")
            logger.info(
                "Workflow started from trigger",
                extra={
                    "workflow_run_id": result.run_id,
                    "start_status": result.status.value,
                },
            )
            return Ack()

    async def _process_cloud_event(
        self,
        message: ReceivedMessage,
        payload: object,
        ingress: CloudEventIngress,
        mapping_name: str,
    ) -> ProcessingOutcome:
        with logging_context(request_id=identity_log_digest(message.broker_message_id)):
            try:
                result = await ingress.receive(mapping_name, payload)
            except CloudEventMappingError as exc:
                logger.warning(
                    "Cloud-event delivery could not be mapped",
                    extra={"cloud_event_error_code": exc.code.value},
                )
                if exc.retryable:
                    return Retry(reason=exc.dead_letter_reason)
                return DeadLetter(reason=exc.dead_letter_reason)
            except WorkflowStartError as exc:
                if not exc.retryable:
                    logger.warning(
                        "Cloud-event delivery cannot start a workflow",
                        extra={"start_error_code": exc.code.value},
                    )
                    return DeadLetter(reason=exc.code.value)
                logger.warning(
                    "Temporal workflow start failed",
                    extra={"start_error_code": exc.code.value},
                )
                return Retry(reason="Temporal workflow start failed")
            logger.info(
                "Workflow started from cloud-event delivery",
                extra={
                    "workflow_run_id": result.run_id,
                    "start_status": result.status.value,
                },
            )
            return Ack()


def _queue_metric_outcome(outcome: ProcessingOutcome) -> QueueMetricOutcome:
    if isinstance(outcome, Ack):
        return QueueMetricOutcome.ACK
    if isinstance(outcome, Retry):
        return QueueMetricOutcome.RETRY
    return QueueMetricOutcome.DEAD_LETTER
