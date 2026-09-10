"""gRPC transport - synchronous RPC calls."""

from __future__ import annotations

import asyncio
import ipaddress
import json
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Annotated, Literal, TypeAlias
from urllib.parse import urlsplit

from pydantic import Field, field_validator

from justflow.config.models import MAX_PATH_LENGTH
from justflow.optional_dependencies import load_optional_dependency
from justflow.sdk.service_context import GRPC_SCOPE_DIGEST_METADATA_KEY
from justflow.transports.base import (
    Completed,
    ConnectionTimeoutError,
    DispatchTimeoutError,
    MalformedResponseError,
    ServiceError,
    StrictTransportConfig,
    TransportConfigurationError,
    TransportConnectionError,
    TransportRequest,
)
from justflow.transports.security import TransportSecuritySettings, read_optional_bytes

if TYPE_CHECKING:
    import grpc

# Status codes that indicate a caller/deployment bug rather than a transient
# fault. Kept as names so the grpc module (optional ``grpc`` extra) is not
# needed at import time.
NON_RETRYABLE_GRPC_CODE_NAMES = frozenset(
    {
        "ALREADY_EXISTS",
        "FAILED_PRECONDITION",
        "INVALID_ARGUMENT",
        "NOT_FOUND",
        "OUT_OF_RANGE",
        "PERMISSION_DENIED",
        "UNAUTHENTICATED",
        "UNIMPLEMENTED",
    }
)
MAX_GRPC_PROFILE_NAME_LENGTH = 128
logger = logging.getLogger(__name__)


class GrpcTlsSecurity(StrictTransportConfig):
    mode: Literal["tls"] = "tls"
    profile: str = Field(min_length=1, max_length=MAX_GRPC_PROFILE_NAME_LENGTH)


class GrpcInsecureLocalSecurity(StrictTransportConfig):
    mode: Literal["insecure_local"] = "insecure_local"


GrpcChannelSecurity: TypeAlias = Annotated[
    GrpcTlsSecurity | GrpcInsecureLocalSecurity,
    Field(discriminator="mode"),
]


class GrpcTransportConfig(StrictTransportConfig):
    address: str = Field(min_length=1, max_length=MAX_PATH_LENGTH)
    security: GrpcChannelSecurity

    @field_validator("address")
    @classmethod
    def validate_address(cls, value: str) -> str:
        parsed = urlsplit(f"//{value}")
        try:
            port = parsed.port
        except ValueError as exc:
            raise ValueError("gRPC address has an invalid port") from exc
        if (
            parsed.hostname is None
            or port is None
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("gRPC address must contain only a host and port")
        return value


@dataclass(frozen=True, kw_only=True)
class _GrpcTlsMaterial:
    server_name: str
    root_certificates: bytes | None
    certificate_chain: bytes | None
    private_key: bytes | None


class GrpcTransport:
    """gRPC transport for synchronous service calls.

    Uses a generic JSON-based message format via a common proto service.
    Services implement a generic Execute(request) -> response RPC.

    grpcio ships in the ``grpc`` extra; it is imported on first use so the
    core package works without it.
    """

    def __init__(
        self,
        config: GrpcTransportConfig,
        *,
        connect_timeout_sec: int,
        dispatch_timeout_sec: int,
        security: TransportSecuritySettings,
    ):
        self._config = config
        self._connect_timeout_sec = connect_timeout_sec
        self._dispatch_timeout_sec = dispatch_timeout_sec
        self._tls_material = self._resolve_tls_material(security)
        self._channels: dict[str, grpc.aio.Channel] = {}
        self._channel_lock = asyncio.Lock()
        self._closed = False

    async def _get_channel(self, address: str) -> grpc.aio.Channel:
        async with self._channel_lock:
            if self._closed:
                raise TransportConnectionError("gRPC transport is closed")
            grpc = load_optional_dependency("grpc", extra="grpc", feature="the gRPC transport")
            if address not in self._channels:
                if self._tls_material is None:
                    channel = grpc.aio.insecure_channel(address)
                else:
                    channel_credentials = grpc.ssl_channel_credentials(
                        root_certificates=self._tls_material.root_certificates,
                        private_key=self._tls_material.private_key,
                        certificate_chain=self._tls_material.certificate_chain,
                    )
                    channel = grpc.aio.secure_channel(
                        address,
                        channel_credentials,
                        options=(
                            (
                                "grpc.ssl_target_name_override",
                                self._tls_material.server_name,
                            ),
                        ),
                    )
                try:
                    async with asyncio.timeout(self._connect_timeout_sec):
                        await channel.channel_ready()
                except asyncio.CancelledError as exc:
                    await asyncio.shield(_close_failed_channel(channel, exc))
                    raise
                except TimeoutError as exc:
                    await _close_failed_channel(channel, exc)
                    raise ConnectionTimeoutError("gRPC connection deadline exceeded") from exc
                except Exception as exc:
                    await _close_failed_channel(channel, exc)
                    raise TransportConnectionError("gRPC connection failed") from exc
                self._channels[address] = channel
            return self._channels[address]

    async def send(self, request: TransportRequest) -> Completed:
        grpc = load_optional_dependency("grpc", extra="grpc", feature="the gRPC transport")

        body = json.dumps(
            {
                "globals": request.globals,
                "input": request.input,
            }
        ).encode()
        context = request.service_call_context
        metadata = (
            ((GRPC_SCOPE_DIGEST_METADATA_KEY, context.scope_digest),)
            if context.scope_digest is not None
            else None
        )

        try:
            async with asyncio.timeout(self._dispatch_timeout_sec):
                channel = await self._get_channel(self._config.address)
                response = await channel.unary_unary(
                    f"/workflow.ActionService/{request.action}",
                    request_serializer=lambda x: x,
                    response_deserializer=lambda x: x,
                )(body, timeout=self._dispatch_timeout_sec, metadata=metadata)
        except grpc.aio.AioRpcError as e:
            if e.code().name == "DEADLINE_EXCEEDED":
                raise DispatchTimeoutError("gRPC dispatch deadline exceeded") from e
            raise ServiceError(
                e.details() or e.code().name,
                code=e.code().name,
                retryable=e.code().name not in NON_RETRYABLE_GRPC_CODE_NAMES,
            ) from e
        except TimeoutError as exc:
            raise DispatchTimeoutError("gRPC dispatch deadline exceeded") from exc

        try:
            return Completed(data=json.loads(response))
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            raise MalformedResponseError(
                f"Non-JSON gRPC response for action '{request.action}': {e}"
            ) from e

    def _resolve_tls_material(
        self,
        security: TransportSecuritySettings,
    ) -> _GrpcTlsMaterial | None:
        channel_security = self._config.security
        if isinstance(channel_security, GrpcInsecureLocalSecurity):
            if not _is_loopback_address(self._config.address):
                raise TransportConfigurationError(
                    "Insecure gRPC is permitted only for a loopback address"
                )
            return None
        try:
            profile = security.grpc_tls_profiles[channel_security.profile]
        except KeyError as exc:
            raise TransportConfigurationError(
                f"gRPC TLS profile '{channel_security.profile}' is not configured"
            ) from exc
        try:
            return _GrpcTlsMaterial(
                server_name=profile.server_name,
                root_certificates=read_optional_bytes(
                    profile.root_ca_path,
                    label="gRPC root CA certificate",
                ),
                certificate_chain=read_optional_bytes(
                    profile.client_certificate_path,
                    label="gRPC client certificate",
                ),
                private_key=read_optional_bytes(
                    profile.client_private_key_path,
                    label="gRPC client private key",
                ),
            )
        except ValueError as exc:
            raise TransportConfigurationError(str(exc)) from exc

    async def close(self) -> None:
        async with self._channel_lock:
            self._closed = True
            channels = tuple(self._channels.values())
            self._channels.clear()
        results = await asyncio.gather(
            *(channel.close() for channel in channels), return_exceptions=True
        )
        failures = [result for result in results if isinstance(result, BaseException)]
        if failures:
            raise TransportConnectionError("gRPC channel cleanup failed") from BaseExceptionGroup(
                "gRPC channel cleanup failures", failures
            )


async def _close_failed_channel(channel: grpc.aio.Channel, failure: BaseException) -> None:
    results = await asyncio.gather(channel.close(), return_exceptions=True)
    exc = results[0]
    if isinstance(exc, BaseException):
        failure.add_note(f"gRPC cleanup also failed: {type(exc).__name__}")
        logger.warning(
            "gRPC cleanup failed after connection failure", extra={"error_type": type(exc).__name__}
        )


def _is_loopback_address(address: str) -> bool:
    parsed = urlsplit(f"//{address}")
    if (
        parsed.hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path
        or parsed.query
        or parsed.fragment
    ):
        return False
    host = parsed.hostname.lower()
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False
