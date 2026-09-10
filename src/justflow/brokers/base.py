"""Broker-neutral publication, delivery, and settlement contracts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol, TypeAlias, runtime_checkable


class BrokerError(Exception):
    pass


class BrokerConnectionError(BrokerError):
    pass


class BrokerConfigurationError(BrokerError):
    pass


@dataclass(frozen=True, kw_only=True)
class PublishedMessage:
    body: str
    message_id: str


@dataclass(frozen=True, kw_only=True)
class ReceivedMessage:
    body: str
    broker_message_id: str
    delivery_attempt: int
    settlement_token: object


@dataclass(frozen=True, kw_only=True)
class DeliveryPolicy:
    max_delivery_attempts: int
    max_redelivery_window_seconds: float

    def __post_init__(self) -> None:
        if self.max_delivery_attempts < 1:
            raise ValueError("Maximum delivery attempts must be positive")
        if self.max_redelivery_window_seconds < 0:
            raise ValueError("Maximum redelivery window must not be negative")


@dataclass(frozen=True, kw_only=True)
class Ack:
    kind: Literal["ack"] = "ack"


@dataclass(frozen=True, kw_only=True)
class Retry:
    reason: str
    kind: Literal["retry"] = "retry"


@dataclass(frozen=True, kw_only=True)
class DeadLetter:
    reason: str
    kind: Literal["dead_letter"] = "dead_letter"


ProcessingOutcome: TypeAlias = Ack | Retry | DeadLetter


@runtime_checkable
class MessagePublisher(Protocol):
    async def publish(self, destination: str, message: PublishedMessage) -> None: ...

    async def close(self) -> None: ...


@runtime_checkable
class MessageConsumer(Protocol):
    async def receive(self) -> tuple[ReceivedMessage, ...]: ...

    async def settle(
        self,
        message: ReceivedMessage,
        outcome: ProcessingOutcome,
    ) -> None: ...

    async def close(self) -> None: ...


@runtime_checkable
class ConfiguredBroker(Protocol):
    @property
    def delivery_policy(self) -> DeliveryPolicy: ...

    @property
    def publisher(self) -> MessagePublisher: ...

    def consumer(
        self,
        source: str,
        *,
        dead_letter_destination: str,
    ) -> MessageConsumer: ...

    async def close(self) -> None: ...
