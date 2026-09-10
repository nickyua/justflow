"""Transport contracts shared by built-in and host-registered providers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Literal, Protocol, TypeAlias, runtime_checkable

from pydantic import BaseModel, ConfigDict

from justflow.brokers.base import MessagePublisher
from justflow.sdk.service_context import ServiceCallContext
from justflow.transports.security import TransportSecuritySettings


class StrictTransportConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


@dataclass(frozen=True, kw_only=True)
class TransportRequest:
    service_name: str
    action: str
    input: Any
    globals: dict[str, Any]
    request_id: str
    correlation_id: str
    trace_id: str | None
    workflow_id: str
    workflow_run_id: str
    flow_name: str
    definition_digest: str | None
    step_name: str
    scope_digest: str | None = None
    required_resources: tuple[str, ...] = ()

    @property
    def service_call_context(self) -> ServiceCallContext:
        return ServiceCallContext(scope_digest=self.scope_digest)


@dataclass(frozen=True, kw_only=True)
class TimeoutPolicy:
    response_timeout_sec: int

    def __post_init__(self) -> None:
        if self.response_timeout_sec < 1:
            raise ValueError("Response timeout must be at least one second")


class DeadlineMode(str, Enum):
    REQUIRED = "required"
    OPTIONAL = "optional"
    FORBIDDEN = "forbidden"


@dataclass(frozen=True, kw_only=True)
class DeadlineRequirements:
    connect: DeadlineMode = DeadlineMode.OPTIONAL
    response: DeadlineMode = DeadlineMode.FORBIDDEN


class DispatchKind(str, Enum):
    COMPLETED = "completed"
    AWAITING_RESPONSE = "awaiting_response"


@dataclass(frozen=True, kw_only=True)
class Completed:
    data: Any = None
    kind: Literal[DispatchKind.COMPLETED] = field(
        default=DispatchKind.COMPLETED,
        init=False,
    )


@dataclass(frozen=True, kw_only=True)
class AwaitingResponse:
    key: str
    timeout_policy: TimeoutPolicy
    kind: Literal[DispatchKind.AWAITING_RESPONSE] = field(
        default=DispatchKind.AWAITING_RESPONSE,
        init=False,
    )

    def __post_init__(self) -> None:
        if not self.key:
            raise ValueError("Asynchronous response key must not be empty")


TransportDispatch: TypeAlias = Completed | AwaitingResponse


@dataclass(frozen=True, kw_only=True)
class TransportFactoryContext:
    resources: Mapping[str, Any]
    connect_timeout_sec: int | None
    dispatch_timeout_sec: int
    response_timeout_sec: int | None
    security: TransportSecuritySettings = field(default_factory=TransportSecuritySettings)
    message_publishers: Mapping[str, MessagePublisher] = field(default_factory=dict)
    reply_destination: str | None = None


@runtime_checkable
class ConfiguredTransport(Protocol):
    async def send(self, request: TransportRequest) -> TransportDispatch: ...

    async def close(self) -> None: ...


class TransportError(Exception):
    code: str = "TRANSPORT_ERROR"
    retryable: bool = True

    def __init__(self, message: str, *, code: str | None = None):
        if code is not None:
            self.code = code
        super().__init__(message)


class TransportConnectionError(TransportError):
    code = "CONNECTION_ERROR"


class TransportConfigurationError(TransportError):
    code = "TRANSPORT_CONFIGURATION_ERROR"
    retryable = False


class ConnectionTimeoutError(TransportError):
    code = "CONNECT_TIMEOUT"


class DispatchTimeoutError(TransportError):
    code = "DISPATCH_TIMEOUT"


class ServiceError(TransportError):
    code = "SERVICE_ERROR"

    def __init__(self, message: str, *, code: str | None = None, retryable: bool = True):
        super().__init__(message, code=code)
        self.retryable = retryable


class MalformedResponseError(TransportError):
    code = "MALFORMED_RESPONSE"


class ActionDispatchError(TransportError):
    code = "ACTION_DISPATCH_ERROR"
    retryable = False
