"""Amazon SQS adapter for the broker-neutral messaging ports."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from pydantic import Field, field_validator

from justflow.brokers.base import (
    Ack,
    BrokerConfigurationError,
    BrokerConnectionError,
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
from justflow.brokers.registry import BrokerProvider, BrokerRegistry, StrictBrokerConfig
from justflow.optional_dependencies import load_optional_dependency

SQS_PROVIDER_NAME = "sqs"
SQS_PROVIDER_CONTRACT_VERSION = "1"
APPROXIMATE_RECEIVE_COUNT = "ApproximateReceiveCount"
MAX_DESTINATION_URL_LENGTH = 2_048
MAX_AWS_IDENTIFIER_LENGTH = 256
MAX_QUEUE_MESSAGES = 10
MAX_QUEUE_WAIT_SECONDS = 20
MAX_POLL_INTERVAL_SECONDS = 300.0
DEFAULT_MAX_DELIVERY_ATTEMPTS = 5
MAX_DELIVERY_ATTEMPTS = 1_000
DEFAULT_RETRY_VISIBILITY_SECONDS = 5
MAX_VISIBILITY_SECONDS = 43_200

logger = logging.getLogger(__name__)


class SqsBrokerConfig(StrictBrokerConfig):
    destinations: dict[str, str]
    region_name: str | None = Field(None, min_length=1, max_length=MAX_AWS_IDENTIFIER_LENGTH)
    endpoint_url: str | None = Field(None, min_length=1, max_length=MAX_DESTINATION_URL_LENGTH)
    profile_name: str | None = Field(None, min_length=1, max_length=MAX_AWS_IDENTIFIER_LENGTH)
    wait_time_seconds: int = Field(default=5, ge=0, le=MAX_QUEUE_WAIT_SECONDS)
    poll_interval_sec: float = Field(default=1.0, gt=0, le=MAX_POLL_INTERVAL_SECONDS)
    max_messages: int = Field(default=MAX_QUEUE_MESSAGES, ge=1, le=MAX_QUEUE_MESSAGES)
    max_delivery_attempts: int = Field(
        default=DEFAULT_MAX_DELIVERY_ATTEMPTS,
        ge=1,
        le=MAX_DELIVERY_ATTEMPTS,
    )
    retry_visibility_seconds: int = Field(
        default=DEFAULT_RETRY_VISIBILITY_SECONDS,
        ge=0,
        le=MAX_VISIBILITY_SECONDS,
    )

    @field_validator("destinations")
    @classmethod
    def validate_destinations(cls, value: dict[str, str]) -> dict[str, str]:
        if not value:
            raise ValueError("At least one SQS destination is required")
        for name, queue_url in value.items():
            if not name:
                raise ValueError("SQS destination names must not be empty")
            cls.validate_destination_url(queue_url)
        return value

    @classmethod
    def validate_destination_url(cls, value: str) -> str:
        if not value or len(value) > MAX_DESTINATION_URL_LENGTH:
            raise ValueError("SQS destination URL has an invalid length")
        return value


@dataclass(frozen=True, kw_only=True)
class _SqsSettlementToken:
    receipt_handle: str


class SqsPublisher(MessagePublisher):
    def __init__(self, client: Any, destinations: Mapping[str, str]) -> None:
        self._client = client
        self._destinations = dict(destinations)

    async def publish(self, destination: str, message: PublishedMessage) -> None:
        queue_url = self._destination(destination)
        try:
            await asyncio.to_thread(
                self._client.send_message,
                QueueUrl=queue_url,
                MessageBody=message.body,
                MessageAttributes={
                    "message_id": {
                        "DataType": "String",
                        "StringValue": message.message_id,
                    }
                },
            )
        except Exception as exc:
            raise BrokerConnectionError(
                f"SQS publish to destination '{destination}' failed"
            ) from exc

    async def close(self) -> None:
        return None

    def _destination(self, name: str) -> str:
        try:
            return self._destinations[name]
        except KeyError as exc:
            raise BrokerConfigurationError(f"Unknown SQS destination '{name}'") from exc


class SqsConsumer(MessageConsumer):
    def __init__(
        self,
        client: Any,
        destinations: Mapping[str, str],
        source: str,
        dead_letter_destination: str,
        config: SqsBrokerConfig,
    ) -> None:
        self._client = client
        self._destinations = dict(destinations)
        self._source = self._destination(source)
        self._dead_letter_destination = self._destination(dead_letter_destination)
        self._wait_time_seconds = config.wait_time_seconds
        self._poll_interval_sec = config.poll_interval_sec
        self._max_messages = config.max_messages
        self._max_delivery_attempts = config.max_delivery_attempts
        self._retry_visibility_seconds = config.retry_visibility_seconds

    async def receive(self) -> tuple[ReceivedMessage, ...]:
        try:
            response = await asyncio.to_thread(
                self._client.receive_message,
                QueueUrl=self._source,
                MaxNumberOfMessages=self._max_messages,
                WaitTimeSeconds=self._wait_time_seconds,
                MessageSystemAttributeNames=[APPROXIMATE_RECEIVE_COUNT],
            )
        except Exception as exc:
            raise BrokerConnectionError("SQS receive failed") from exc
        raw_messages = response.get("Messages", [])
        if not raw_messages:
            await asyncio.sleep(self._poll_interval_sec)
            return ()
        messages: list[ReceivedMessage] = []
        try:
            for raw in raw_messages:
                attributes = raw.get("Attributes", {})
                messages.append(
                    ReceivedMessage(
                        body=raw["Body"],
                        broker_message_id=raw["MessageId"],
                        delivery_attempt=int(attributes.get(APPROXIMATE_RECEIVE_COUNT, "1")),
                        settlement_token=_SqsSettlementToken(receipt_handle=raw["ReceiptHandle"]),
                    )
                )
        except (KeyError, TypeError, ValueError) as exc:
            raise BrokerConnectionError("SQS returned a malformed receive response") from exc
        return tuple(messages)

    async def settle(
        self,
        message: ReceivedMessage,
        outcome: ProcessingOutcome,
    ) -> None:
        token = message.settlement_token
        if not isinstance(token, _SqsSettlementToken):
            raise BrokerConfigurationError("SQS received an invalid settlement token")
        if isinstance(outcome, Ack):
            await self._delete(token)
            self._record_settlement(message, Ack())
            return
        if (
            isinstance(outcome, DeadLetter)
            or message.delivery_attempt >= self._max_delivery_attempts
        ):
            reason = (
                outcome.reason
                if isinstance(outcome, DeadLetter)
                else f"delivery attempts exhausted: {outcome.reason}"
            )
            await self._dead_letter(message, token, reason)
            self._record_settlement(message, DeadLetter(reason=reason))
            return
        if not isinstance(outcome, Retry):
            raise BrokerConfigurationError(
                f"Unsupported SQS processing outcome '{type(outcome).__name__}'"
            )
        try:
            await asyncio.to_thread(
                self._client.change_message_visibility,
                QueueUrl=self._source,
                ReceiptHandle=token.receipt_handle,
                VisibilityTimeout=self._retry_visibility_seconds,
            )
        except Exception as exc:
            raise BrokerConnectionError("SQS retry settlement failed") from exc
        self._record_settlement(message, outcome)

    async def close(self) -> None:
        return None

    async def _dead_letter(
        self,
        message: ReceivedMessage,
        token: _SqsSettlementToken,
        reason: str,
    ) -> None:
        try:
            await asyncio.to_thread(
                self._client.send_message,
                QueueUrl=self._dead_letter_destination,
                MessageBody=message.body,
                MessageAttributes={
                    "source_message_id": {
                        "DataType": "String",
                        "StringValue": message.broker_message_id,
                    },
                    "dead_letter_reason": {
                        "DataType": "String",
                        "StringValue": reason,
                    },
                },
            )
        except Exception as exc:
            raise BrokerConnectionError("SQS dead-letter publish failed") from exc
        await self._delete(token)

    async def _delete(self, token: _SqsSettlementToken) -> None:
        try:
            await asyncio.to_thread(
                self._client.delete_message,
                QueueUrl=self._source,
                ReceiptHandle=token.receipt_handle,
            )
        except Exception as exc:
            raise BrokerConnectionError("SQS acknowledgement failed") from exc

    def _destination(self, name: str) -> str:
        try:
            return self._destinations[name]
        except KeyError as exc:
            raise BrokerConfigurationError(f"Unknown SQS destination '{name}'") from exc

    @staticmethod
    def _record_settlement(
        message: ReceivedMessage,
        outcome: ProcessingOutcome,
    ) -> None:
        logger.info(
            "SQS delivery settled",
            extra={
                "broker_message_id": message.broker_message_id,
                "delivery_attempt": message.delivery_attempt,
                "outcome": outcome.kind,
            },
        )


class SqsBroker(ConfiguredBroker):
    def __init__(self, config: SqsBrokerConfig, client: Any, *, owns_client: bool) -> None:
        self._config = config
        self._client = client
        self._owns_client = owns_client
        self._publisher = SqsPublisher(client, config.destinations)

    @property
    def delivery_policy(self) -> DeliveryPolicy:
        return DeliveryPolicy(
            max_delivery_attempts=self._config.max_delivery_attempts,
            max_redelivery_window_seconds=(
                self._config.max_delivery_attempts * self._config.retry_visibility_seconds
            ),
        )

    @property
    def publisher(self) -> MessagePublisher:
        return self._publisher

    def consumer(
        self,
        source: str,
        *,
        dead_letter_destination: str,
    ) -> MessageConsumer:
        return SqsConsumer(
            self._client,
            self._config.destinations,
            source,
            dead_letter_destination,
            self._config,
        )

    async def close(self) -> None:
        if not self._owns_client:
            return
        close = getattr(self._client, "close", None)
        if callable(close):
            await asyncio.to_thread(close)


def builtin_broker_registry(*, sqs_client: Any | None = None) -> BrokerRegistry:
    providers: list[BrokerProvider[Any]] = []
    factory: Callable[[SqsBrokerConfig], SqsBroker] | None
    if sqs_client is not None:

        def build_injected(config: SqsBrokerConfig) -> SqsBroker:
            return SqsBroker(config, sqs_client, owns_client=False)

        factory = build_injected
    else:
        try:
            boto3 = load_optional_dependency("boto3", extra="aws", feature="the SQS broker")
        except ModuleNotFoundError:
            factory = None
        else:

            def build_owned(config: SqsBrokerConfig) -> SqsBroker:
                session = boto3.Session(profile_name=config.profile_name)
                client = session.client(
                    "sqs",
                    region_name=config.region_name,
                    endpoint_url=config.endpoint_url,
                )
                return SqsBroker(config, client, owns_client=True)

            factory = build_owned
    if factory is not None:
        providers.append(
            BrokerProvider(
                name=SQS_PROVIDER_NAME,
                contract_version=SQS_PROVIDER_CONTRACT_VERSION,
                config_model=SqsBrokerConfig,
                factory=factory,
            )
        )
    return BrokerRegistry.from_builtin_providers(providers)
