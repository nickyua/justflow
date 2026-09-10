"""Broker-neutral asynchronous queue transport."""

from __future__ import annotations

import asyncio
from typing import Literal

from pydantic import Field

from justflow.brokers.base import BrokerError, MessagePublisher, PublishedMessage
from justflow.config.models import MAX_PATH_LENGTH
from justflow.sdk.message_contract import (
    PROTOCOL_VERSION,
    MessageKind,
    StepRequestEnvelope,
    StepRequestPayload,
    make_signal_key,
    make_step_invocation_id,
    make_step_request_message_id,
)
from justflow.transports.base import (
    AwaitingResponse,
    DispatchTimeoutError,
    StrictTransportConfig,
    TimeoutPolicy,
    TransportConnectionError,
    TransportRequest,
)


class QueueTransportConfig(StrictTransportConfig):
    broker: str = Field(min_length=1, max_length=MAX_PATH_LENGTH)
    destination: str = Field(min_length=1, max_length=MAX_PATH_LENGTH)
    idempotency: Literal["durable", "none"]


class QueueTransport:
    def __init__(
        self,
        config: QueueTransportConfig,
        *,
        dispatch_timeout_sec: int,
        response_timeout_sec: int,
        publisher: MessagePublisher,
        reply_destination: str,
    ) -> None:
        self._config = config
        self._dispatch_timeout_sec = dispatch_timeout_sec
        self._timeout_policy = TimeoutPolicy(response_timeout_sec=response_timeout_sec)
        self._publisher = publisher
        self._reply_destination = reply_destination

    async def send(self, request: TransportRequest) -> AwaitingResponse:
        if request.definition_digest is None:
            raise TransportConnectionError(
                "Queue transport requires an immutable workflow definition"
            )
        step_invocation_id = make_step_invocation_id(
            request.workflow_id,
            request.workflow_run_id,
            request.step_name,
        )
        message_id = make_step_request_message_id(step_invocation_id)
        envelope = StepRequestEnvelope(
            protocol_version=PROTOCOL_VERSION,
            message_id=message_id,
            kind=MessageKind.STEP_REQUEST,
            workflow_name=request.flow_name,
            definition_digest=request.definition_digest,
            workflow_id=request.workflow_id,
            correlation_id=request.correlation_id,
            causation_id=None,
            trace_id=request.trace_id,
            scope_digest=request.scope_digest,
            workflow_run_id=request.workflow_run_id,
            step_invocation_id=step_invocation_id,
            step_name=request.step_name,
            action=request.action,
            payload=StepRequestPayload(
                input=request.input,
                globals=request.globals,
            ),
            reply_destination=self._reply_destination,
        )
        try:
            async with asyncio.timeout(self._dispatch_timeout_sec):
                await self._publisher.publish(
                    self._config.destination,
                    PublishedMessage(body=envelope.model_dump_json(), message_id=message_id),
                )
        except TimeoutError as exc:
            raise DispatchTimeoutError(
                f"Queue publish to destination '{self._config.destination}' exceeded its deadline"
            ) from exc
        except BrokerError as exc:
            raise TransportConnectionError(
                f"Queue publish to destination '{self._config.destination}' failed"
            ) from exc
        return AwaitingResponse(
            key=make_signal_key(
                step_invocation_id,
                request.step_name,
                request.action,
            ),
            timeout_policy=self._timeout_policy,
        )

    async def close(self) -> None:
        return None
