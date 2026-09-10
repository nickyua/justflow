"""Deterministic broker used by messaging tests."""

from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from dataclasses import dataclass

from justflow.brokers import (
    Ack,
    ConfiguredBroker,
    DeadLetter,
    DeliveryPolicy,
    MessageConsumer,
    MessagePublisher,
    ProcessingOutcome,
    PublishedMessage,
    ReceivedMessage,
    Retry,
)

TEST_BROKER_BATCH_SIZE = 10
TEST_BROKER_MAX_DELIVERY_ATTEMPTS = 5
TEST_BROKER_REDELIVERY_WINDOW_SECONDS = 300


@dataclass(frozen=True, kw_only=True)
class _StoredMessage:
    body: str
    application_message_id: str
    broker_message_id: str
    delivery_attempt: int


class TestBrokerConsumer(MessageConsumer):
    __test__ = False

    def __init__(
        self,
        broker: TestBroker,
        source: str,
        dead_letter_destination: str,
    ) -> None:
        self._broker = broker
        self._source = source
        self._dead_letter_destination = dead_letter_destination

    async def receive(self) -> tuple[ReceivedMessage, ...]:
        return await self._broker.receive(self._source)

    async def settle(
        self,
        message: ReceivedMessage,
        outcome: ProcessingOutcome,
    ) -> None:
        await self._broker.settle(
            self._source,
            self._dead_letter_destination,
            message,
            outcome,
        )

    async def close(self) -> None:
        return None


class TestBroker(ConfiguredBroker, MessagePublisher):
    __test__ = False

    def __init__(self) -> None:
        self._queues: dict[str, deque[_StoredMessage]] = defaultdict(deque)
        self._in_flight: dict[str, _StoredMessage] = {}
        self._published: dict[str, list[PublishedMessage]] = defaultdict(list)
        self._next_message_number = 1
        self._condition = asyncio.Condition()

    @property
    def publisher(self) -> MessagePublisher:
        return self

    @property
    def delivery_policy(self) -> DeliveryPolicy:
        return DeliveryPolicy(
            max_delivery_attempts=TEST_BROKER_MAX_DELIVERY_ATTEMPTS,
            max_redelivery_window_seconds=TEST_BROKER_REDELIVERY_WINDOW_SECONDS,
        )

    def consumer(
        self,
        source: str,
        *,
        dead_letter_destination: str,
    ) -> MessageConsumer:
        return TestBrokerConsumer(self, source, dead_letter_destination)

    async def publish(self, destination: str, message: PublishedMessage) -> None:
        async with self._condition:
            stored = _StoredMessage(
                body=message.body,
                application_message_id=message.message_id,
                broker_message_id=f"test-broker-{self._next_message_number}",
                delivery_attempt=1,
            )
            self._next_message_number += 1
            self._queues[destination].append(stored)
            self._published[destination].append(message)
            self._condition.notify_all()

    async def receive(self, source: str) -> tuple[ReceivedMessage, ...]:
        async with self._condition:
            messages: list[ReceivedMessage] = []
            queue = self._queues[source]
            while queue and len(messages) < TEST_BROKER_BATCH_SIZE:
                stored = queue.popleft()
                token = (
                    f"{stored.broker_message_id}:{stored.delivery_attempt}:{len(self._in_flight)}"
                )
                self._in_flight[token] = stored
                messages.append(
                    ReceivedMessage(
                        body=stored.body,
                        broker_message_id=stored.broker_message_id,
                        delivery_attempt=stored.delivery_attempt,
                        settlement_token=token,
                    )
                )
            return tuple(messages)

    async def settle(
        self,
        source: str,
        dead_letter_destination: str,
        message: ReceivedMessage,
        outcome: ProcessingOutcome,
    ) -> None:
        token = message.settlement_token
        if not isinstance(token, str):
            raise TypeError("Test broker settlement token must be text")
        async with self._condition:
            stored = self._in_flight.pop(token)
            if isinstance(outcome, Ack):
                return
            if isinstance(outcome, Retry):
                self._queues[source].append(
                    _StoredMessage(
                        body=stored.body,
                        application_message_id=stored.application_message_id,
                        broker_message_id=stored.broker_message_id,
                        delivery_attempt=stored.delivery_attempt + 1,
                    )
                )
                self._condition.notify_all()
                return
            if not isinstance(outcome, DeadLetter):
                raise TypeError(f"Unsupported outcome '{type(outcome).__name__}'")
            self._queues[dead_letter_destination].append(stored)
            self._condition.notify_all()

    async def wait_for_published(
        self,
        destination: str,
        count: int,
    ) -> tuple[PublishedMessage, ...]:
        async with self._condition:
            await self._condition.wait_for(lambda: len(self._published[destination]) >= count)
            return tuple(self._published[destination])

    async def close(self) -> None:
        return None
