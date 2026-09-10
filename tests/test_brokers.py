"""Tests for broker ports, registration, and delivery settlement."""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import MagicMock

import pytest
from pydantic import Field

from justflow.brokers import (
    Ack,
    BrokerProvider,
    BrokerRegistry,
    DeadLetter,
    DuplicateBrokerProviderError,
    PublishedMessage,
    ReceivedMessage,
    Retry,
    StrictBrokerConfig,
    UnknownBrokerProviderError,
)
from justflow.brokers.sqs import SqsBroker, SqsBrokerConfig, builtin_broker_registry
from tests.messaging import TestBroker

MAX_TEST_DELIVERY_ATTEMPTS = 5


class HostBrokerConfig(StrictBrokerConfig):
    prefix: str = Field(min_length=1)


def build_host_broker(config: HostBrokerConfig) -> TestBroker:
    assert config.prefix == "configured"
    return TestBroker()


def test_host_broker_registration_configures_without_core_changes():
    registry = BrokerRegistry()
    registry.register(
        BrokerProvider(
            name="test.host",
            contract_version="1",
            config_model=HostBrokerConfig,
            factory=build_host_broker,
        )
    )

    broker = registry.configure("test.host", {"prefix": "configured"})

    assert isinstance(broker, TestBroker)


def test_duplicate_and_unknown_broker_providers_are_rejected():
    provider = BrokerProvider(
        name="test.host",
        contract_version="1",
        config_model=HostBrokerConfig,
        factory=build_host_broker,
    )
    registry = BrokerRegistry()
    registry.register(provider)

    with pytest.raises(DuplicateBrokerProviderError, match="already registered"):
        registry.register(provider)
    with pytest.raises(UnknownBrokerProviderError, match="not registered"):
        registry.configure("missing", {})


async def test_deterministic_broker_redelivers_and_dead_letters():
    broker = TestBroker()
    consumer = broker.consumer("source", dead_letter_destination="dead")
    published = PublishedMessage(body='{"value":1}', message_id="application-1")
    await broker.publish("source", published)

    first = (await consumer.receive())[0]
    await consumer.settle(first, Retry(reason="temporary"))
    second = (await consumer.receive())[0]
    await consumer.settle(second, DeadLetter(reason="permanent"))

    assert first.broker_message_id == second.broker_message_id
    assert second.delivery_attempt == 2
    dead_letter = (await broker.receive("dead"))[0]
    assert dead_letter.body == published.body


async def test_sqs_publisher_resolves_logical_destination_inside_adapter():
    client = MagicMock()
    config = SqsBrokerConfig(destinations={"requests": "https://sqs.example/requests"})
    broker = SqsBroker(config, client, owns_client=False)

    await broker.publisher.publish(
        "requests",
        PublishedMessage(body='{"value":1}', message_id="application-1"),
    )

    client.send_message.assert_called_once_with(
        QueueUrl="https://sqs.example/requests",
        MessageBody='{"value":1}',
        MessageAttributes={
            "message_id": {
                "DataType": "String",
                "StringValue": "application-1",
            }
        },
    )


async def test_owned_sqs_client_is_closed_by_broker():
    client = MagicMock()
    broker = SqsBroker(
        SqsBrokerConfig(destinations={"requests": "https://sqs.example/requests"}),
        client,
        owns_client=True,
    )

    await broker.close()

    client.close.assert_called_once()


@dataclass(frozen=True, kw_only=True)
class SettlementCase:
    id: str
    outcome: Ack | Retry | DeadLetter
    delivery_attempt: int
    expected_call: str


SETTLEMENT_CASES = [
    SettlementCase(
        id="ack",
        outcome=Ack(),
        delivery_attempt=2,
        expected_call="delete_message",
    ),
    SettlementCase(
        id="retry",
        outcome=Retry(reason="temporary"),
        delivery_attempt=2,
        expected_call="change_message_visibility",
    ),
    SettlementCase(
        id="dead-letter",
        outcome=DeadLetter(reason="poison"),
        delivery_attempt=2,
        expected_call="send_message",
    ),
    SettlementCase(
        id="exhausted-retry",
        outcome=Retry(reason="still unavailable"),
        delivery_attempt=MAX_TEST_DELIVERY_ATTEMPTS,
        expected_call="send_message",
    ),
]


@pytest.mark.parametrize("case", SETTLEMENT_CASES, ids=lambda case: case.id)
async def test_sqs_settlement_is_contained_in_adapter(case: SettlementCase):
    client = MagicMock()
    client.receive_message.return_value = {
        "Messages": [
            {
                "Body": '{"value":1}',
                "MessageId": "sqs-1",
                "ReceiptHandle": "receipt-1",
                "Attributes": {"ApproximateReceiveCount": str(case.delivery_attempt)},
            }
        ]
    }
    config = SqsBrokerConfig(
        destinations={
            "source": "https://sqs.example/source",
            "dead": "https://sqs.example/dead",
        },
        max_delivery_attempts=MAX_TEST_DELIVERY_ATTEMPTS,
        poll_interval_sec=0.01,
    )
    broker = SqsBroker(config, client, owns_client=False)
    consumer = broker.consumer("source", dead_letter_destination="dead")
    message = (await consumer.receive())[0]

    await consumer.settle(message, case.outcome)

    assert message.delivery_attempt == case.delivery_attempt
    assert isinstance(message, ReceivedMessage)
    getattr(client, case.expected_call).assert_called_once()
    if isinstance(case.outcome, DeadLetter) or case.delivery_attempt == MAX_TEST_DELIVERY_ATTEMPTS:
        client.delete_message.assert_called_once()


def test_builtin_registry_registers_sqs_when_client_is_supplied():
    registry = builtin_broker_registry(sqs_client=MagicMock())

    assert set(registry.providers) == {"sqs"}
