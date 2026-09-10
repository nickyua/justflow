"""Built-in transport providers registered through the public provider boundary."""

from __future__ import annotations

import re
from typing import Any

import httpx

from justflow.transports.base import (
    DeadlineMode,
    DeadlineRequirements,
    TransportFactoryContext,
)
from justflow.transports.direct import (
    DirectTransport,
    DirectTransportConfig,
    validate_direct_startup,
)
from justflow.transports.grpc import GrpcTransport, GrpcTransportConfig
from justflow.transports.http import HttpTransport, HttpTransportConfig
from justflow.transports.lambda_ import LambdaTransport, LambdaTransportConfig
from justflow.transports.queue import QueueTransport, QueueTransportConfig
from justflow.transports.registry import TransportProvider, TransportRegistry

BUILTIN_PROVIDER_CONTRACT_VERSION = "2"
DIRECT_PROVIDER_NAME = "direct"
GRPC_PROVIDER_NAME = "grpc"
HTTP_PROVIDER_NAME = "http"
LAMBDA_PROVIDER_NAME = "lambda"
QUEUE_PROVIDER_NAME = "queue"

HTTP_ACTION_PATTERN = re.compile(r"^(GET|POST|PUT|PATCH|DELETE):/.+")
IDENTIFIER_ACTION_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_http_action(action: str) -> str | None:
    if HTTP_ACTION_PATTERN.fullmatch(action) is None:
        return f"HTTP action must follow 'METHOD:/path' format, got '{action}'"
    return None


def _validate_direct_action(action: str) -> str | None:
    if IDENTIFIER_ACTION_PATTERN.fullmatch(action) is None:
        return f"direct action must be a method name, got '{action}'"
    return None


def _validate_grpc_action(action: str) -> str | None:
    if IDENTIFIER_ACTION_PATTERN.fullmatch(action) is None:
        return f"grpc action must be a method name, got '{action}'"
    return None


def _validate_non_http_action(action: str) -> str | None:
    if HTTP_ACTION_PATTERN.fullmatch(action) is not None:
        return f"'METHOD:/path' actions are only valid for HTTP services, got '{action}'"
    return None


def builtin_transport_registry(
    *,
    http_client: httpx.AsyncClient | None = None,
    lambda_client: Any | None = None,
) -> TransportRegistry:
    def build_direct(
        config: DirectTransportConfig,
        context: TransportFactoryContext,
    ) -> DirectTransport:
        return DirectTransport(
            config,
            dispatch_timeout_sec=context.dispatch_timeout_sec,
            resources=dict(context.resources),
        )

    def build_http(
        config: HttpTransportConfig,
        context: TransportFactoryContext,
    ) -> HttpTransport:
        return HttpTransport(
            config,
            connect_timeout_sec=_require_connect_deadline(context, HTTP_PROVIDER_NAME),
            dispatch_timeout_sec=context.dispatch_timeout_sec,
            endpoint_policy=context.security.http,
            client=http_client,
        )

    def build_grpc(
        config: GrpcTransportConfig,
        context: TransportFactoryContext,
    ) -> GrpcTransport:
        return GrpcTransport(
            config,
            connect_timeout_sec=_require_connect_deadline(context, GRPC_PROVIDER_NAME),
            dispatch_timeout_sec=context.dispatch_timeout_sec,
            security=context.security,
        )

    def build_queue(
        config: QueueTransportConfig,
        context: TransportFactoryContext,
    ) -> QueueTransport:
        try:
            publisher = context.message_publishers[config.broker]
        except KeyError as exc:
            raise ValueError(f"Queue broker '{config.broker}' is not configured") from exc
        if context.reply_destination is None:
            raise ValueError("Queue response relay destination is not configured")
        return QueueTransport(
            config,
            dispatch_timeout_sec=context.dispatch_timeout_sec,
            response_timeout_sec=_require_response_deadline(context, QUEUE_PROVIDER_NAME),
            publisher=publisher,
            reply_destination=context.reply_destination,
        )

    def build_lambda(
        config: LambdaTransportConfig,
        context: TransportFactoryContext,
    ) -> LambdaTransport:
        return LambdaTransport(
            config,
            connect_timeout_sec=_require_connect_deadline(context, LAMBDA_PROVIDER_NAME),
            dispatch_timeout_sec=context.dispatch_timeout_sec,
            lambda_client=lambda_client,
        )

    providers: tuple[TransportProvider[Any], ...] = (
        TransportProvider(
            name=DIRECT_PROVIDER_NAME,
            contract_version=BUILTIN_PROVIDER_CONTRACT_VERSION,
            config_model=DirectTransportConfig,
            factory=build_direct,
            action_validator=_validate_direct_action,
            startup_validator=validate_direct_startup,
            deadline_requirements=DeadlineRequirements(
                connect=DeadlineMode.FORBIDDEN,
                response=DeadlineMode.FORBIDDEN,
            ),
        ),
        TransportProvider(
            name=HTTP_PROVIDER_NAME,
            contract_version=BUILTIN_PROVIDER_CONTRACT_VERSION,
            config_model=HttpTransportConfig,
            factory=build_http,
            action_validator=_validate_http_action,
            deadline_requirements=DeadlineRequirements(
                connect=DeadlineMode.REQUIRED,
                response=DeadlineMode.FORBIDDEN,
            ),
        ),
        TransportProvider(
            name=GRPC_PROVIDER_NAME,
            contract_version=BUILTIN_PROVIDER_CONTRACT_VERSION,
            config_model=GrpcTransportConfig,
            factory=build_grpc,
            action_validator=_validate_grpc_action,
            deadline_requirements=DeadlineRequirements(
                connect=DeadlineMode.REQUIRED,
                response=DeadlineMode.FORBIDDEN,
            ),
        ),
        TransportProvider(
            name=QUEUE_PROVIDER_NAME,
            contract_version=BUILTIN_PROVIDER_CONTRACT_VERSION,
            config_model=QueueTransportConfig,
            factory=build_queue,
            supports_async_response=True,
            action_validator=_validate_non_http_action,
            deadline_requirements=DeadlineRequirements(
                connect=DeadlineMode.FORBIDDEN,
                response=DeadlineMode.REQUIRED,
            ),
        ),
        TransportProvider(
            name=LAMBDA_PROVIDER_NAME,
            contract_version=BUILTIN_PROVIDER_CONTRACT_VERSION,
            config_model=LambdaTransportConfig,
            factory=build_lambda,
            action_validator=_validate_non_http_action,
            deadline_requirements=DeadlineRequirements(
                connect=DeadlineMode.REQUIRED,
                response=DeadlineMode.FORBIDDEN,
            ),
        ),
    )
    return TransportRegistry.from_builtin_providers(providers)


def _require_connect_deadline(context: TransportFactoryContext, provider: str) -> int:
    if context.connect_timeout_sec is None:
        raise ValueError(f"Transport provider '{provider}' requires a connection deadline")
    return context.connect_timeout_sec


def _require_response_deadline(context: TransportFactoryContext, provider: str) -> int:
    if context.response_timeout_sec is None:
        raise ValueError(f"Transport provider '{provider}' requires a response deadline")
    return context.response_timeout_sec
