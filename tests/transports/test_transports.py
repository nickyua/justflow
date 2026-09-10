"""Tests for the transport layer and its error taxonomy."""

from __future__ import annotations

import asyncio
import io
import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import grpc
import httpx
import pytest
from botocore.exceptions import ClientError, ConnectTimeoutError, ReadTimeoutError
from pydantic import ValidationError

from justflow.brokers import BrokerConnectionError, PublishedMessage
from justflow.sdk.base_action import BaseAction
from justflow.sdk.service_context import (
    GRPC_SCOPE_DIGEST_METADATA_KEY,
    HTTP_SCOPE_DIGEST_HEADER,
    LAMBDA_SERVICE_CALL_CONTEXT_FIELD,
)
from justflow.transports import grpc as grpc_transport_module
from justflow.transports.base import (
    ActionDispatchError,
    AwaitingResponse,
    ConnectionTimeoutError,
    DispatchTimeoutError,
    MalformedResponseError,
    ServiceError,
    TransportConfigurationError,
    TransportConnectionError,
    TransportRequest,
)
from justflow.transports.direct import DirectTransport, DirectTransportConfig
from justflow.transports.grpc import GrpcTransport, GrpcTransportConfig
from justflow.transports.http import HttpRedirectPolicy, HttpTransport, HttpTransportConfig
from justflow.transports.lambda_ import LambdaTransport, LambdaTransportConfig
from justflow.transports.queue import QueueTransport, QueueTransportConfig
from justflow.transports.security import (
    GrpcTlsProfile,
    HttpEndpointPolicy,
    TransportSecuritySettings,
)

TRANSPORT_TIMEOUT_SECONDS = 30
CONNECT_TIMEOUT_SECONDS = 5
EXPIRED_DEADLINE_SECONDS = 0
SCOPE_DIGEST = "b" * 64


def _make_request(**overrides) -> TransportRequest:
    defaults = {
        "service_name": "test_service",
        "action": "test_action",
        "input": {"key": "value"},
        "globals": {"g": 1},
        "request_id": "req-123",
        "correlation_id": "req-123",
        "trace_id": "trace-123",
        "workflow_id": "workflow-123",
        "workflow_run_id": "run-456",
        "flow_name": "test_flow",
        "definition_digest": "a" * 64,
        "step_name": "test_step",
        "scope_digest": SCOPE_DIGEST,
    }
    defaults.update(overrides)
    return TransportRequest(**defaults)


class MockAction(BaseAction):
    async def test_action(self, input: Any) -> Any:
        return {"processed": input}

    async def exploding_action(self, input: Any) -> Any:
        raise ValueError("bad input data")

    async def timeout_action(self, input: Any) -> Any:
        raise TimeoutError("action timed out internally")

    async def blocking_action(self, input: Any) -> Any:
        await asyncio.Future()

    async def resource_names(self, input: Any) -> Any:
        return sorted(self.resources)

    async def scope_digest(self, input: Any) -> Any:
        return self.context.service_call.scope_digest

    async def denied_resource(self, input: Any) -> Any:
        return self.get_resource("other")


DIRECT_CONFIG = DirectTransportConfig(class_=f"{__name__}.MockAction")


class TestDirectTransport:
    @pytest.mark.parametrize("retryable", [False, True], ids=["permanent", "transient"])
    async def test_declared_failure_preserves_retryability(
        self, monkeypatch, retryable: bool
    ) -> None:
        error = ServiceError("declared failure", code="DECLARED", retryable=retryable)

        async def fail(self, input):
            raise error

        monkeypatch.setattr(MockAction, "test_action", fail)
        with pytest.raises(ServiceError) as raised:
            await DirectTransport(
                DIRECT_CONFIG, dispatch_timeout_sec=TRANSPORT_TIMEOUT_SECONDS
            ).send(_make_request())
        assert raised.value is error

    async def test_send_calls_action(self):
        response = await DirectTransport(
            DIRECT_CONFIG,
            dispatch_timeout_sec=TRANSPORT_TIMEOUT_SECONDS,
        ).send(_make_request())
        assert response.data == {"processed": {"key": "value"}}

    async def test_unknown_action_is_dispatch_error(self):
        with pytest.raises(ActionDispatchError, match="no action method"):
            await DirectTransport(
                DIRECT_CONFIG,
                dispatch_timeout_sec=TRANSPORT_TIMEOUT_SECONDS,
            ).send(_make_request(action="nonexistent"))
        assert ActionDispatchError.retryable is False

    @pytest.mark.parametrize(
        "class_path",
        ["no.such.module.Thing", "MissingDot"],
    )
    async def test_unimportable_class_is_dispatch_error(self, class_path: str):
        config = DirectTransportConfig(class_=class_path)
        with pytest.raises(ActionDispatchError, match="Cannot import"):
            await DirectTransport(
                config,
                dispatch_timeout_sec=TRANSPORT_TIMEOUT_SECONDS,
            ).send(_make_request())

    async def test_action_exception_is_service_error(self):
        with pytest.raises(ServiceError, match="Direct action failed") as exc_info:
            await DirectTransport(
                DIRECT_CONFIG,
                dispatch_timeout_sec=TRANSPORT_TIMEOUT_SECONDS,
            ).send(_make_request(action="exploding_action"))
        assert exc_info.value.code == "ValueError"
        assert exc_info.value.retryable is True

    async def test_action_deadline_is_typed_timeout(self):
        transport = DirectTransport(
            DIRECT_CONFIG,
            dispatch_timeout_sec=EXPIRED_DEADLINE_SECONDS,
        )

        with pytest.raises(DispatchTimeoutError, match="dispatch deadline"):
            await transport.send(_make_request(action="blocking_action"))

    async def test_action_timeout_error_is_not_misclassified_as_deadline(self):
        transport = DirectTransport(
            DIRECT_CONFIG,
            dispatch_timeout_sec=TRANSPORT_TIMEOUT_SECONDS,
        )

        with pytest.raises(ServiceError, match="Direct action failed") as exc_info:
            await transport.send(_make_request(action="timeout_action"))

        assert exc_info.value.code == "TimeoutError"

    async def test_action_receives_only_explicit_resource_grants(self) -> None:
        transport = DirectTransport(
            DIRECT_CONFIG,
            dispatch_timeout_sec=TRANSPORT_TIMEOUT_SECONDS,
            resources={"allowed": object(), "other": object()},
        )

        response = await transport.send(
            _make_request(
                action="resource_names",
                required_resources=("allowed",),
            )
        )

        assert response.data == ["allowed"]

    async def test_action_receives_trusted_scope_context(self) -> None:
        response = await DirectTransport(
            DIRECT_CONFIG,
            dispatch_timeout_sec=TRANSPORT_TIMEOUT_SECONDS,
        ).send(_make_request(action="scope_digest"))

        assert response.data == SCOPE_DIGEST

    async def test_action_denied_resource_access_is_non_retryable(self) -> None:
        transport = DirectTransport(
            DIRECT_CONFIG,
            dispatch_timeout_sec=TRANSPORT_TIMEOUT_SECONDS,
            resources={"allowed": object(), "other": object()},
        )

        with pytest.raises(ActionDispatchError, match="denied resource access") as exc_info:
            await transport.send(
                _make_request(
                    action="denied_resource",
                    required_resources=("allowed",),
                )
            )

        assert exc_info.value.retryable is False


HTTP_CONFIG = HttpTransportConfig(base_url="https://svc:8080")


def http_transport(handler) -> HttpTransport:
    return HttpTransport(
        HTTP_CONFIG,
        connect_timeout_sec=CONNECT_TIMEOUT_SECONDS,
        dispatch_timeout_sec=TRANSPORT_TIMEOUT_SECONDS,
        endpoint_policy=HttpEndpointPolicy(allowed_origins={"https://svc:8080"}),
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )


class TestHttpTransport:
    async def test_oversized_stream_is_stopped_and_closed(self) -> None:
        from justflow.config.runtime_limits import DEFAULT_ACTIVITY_OUTPUT_BYTES

        class OversizedStream(httpx.AsyncByteStream):
            def __init__(self) -> None:
                self.chunks = 0
                self.closed = False

            async def __aiter__(self):
                for _ in range(3):
                    self.chunks += 1
                    yield b"x" * DEFAULT_ACTIVITY_OUTPUT_BYTES

            async def aclose(self) -> None:
                self.closed = True

        stream = OversizedStream()
        transport = http_transport(lambda request: httpx.Response(200, stream=stream))
        with pytest.raises(ServiceError) as raised:
            await transport.send(_make_request(action="GET:/oversize"))
        assert raised.value.code == "HTTP_RESPONSE_TOO_LARGE"
        assert raised.value.retryable is False
        assert stream.chunks <= 2
        assert stream.closed

    async def test_send_success(self):
        seen = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["method"] = request.method
            seen["url"] = str(request.url)
            seen["body"] = json.loads(request.content)
            seen["scope_digest"] = request.headers[HTTP_SCOPE_DIGEST_HEADER]
            return httpx.Response(200, json={"result": "ok"})

        response = await http_transport(handler).send(_make_request(action="GET:/api/v1/data"))

        assert response.data == {"result": "ok"}
        assert seen == {
            "method": "GET",
            "url": "https://svc:8080/api/v1/data",
            "body": {"globals": {"g": 1}, "input": {"key": "value"}},
            "scope_digest": SCOPE_DIGEST,
        }

    async def test_business_input_cannot_override_trusted_scope_header(self):
        seen: dict[str, object] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["body"] = json.loads(request.content)
            seen["scope_digest"] = request.headers[HTTP_SCOPE_DIGEST_HEADER]
            return httpx.Response(200, json={})

        await http_transport(handler).send(
            _make_request(
                action="POST:/work",
                input={"scope_digest": "attacker-selected"},
            )
        )

        assert seen == {
            "body": {
                "globals": {"g": 1},
                "input": {"scope_digest": "attacker-selected"},
            },
            "scope_digest": SCOPE_DIGEST,
        }

    async def test_retained_unscoped_request_omits_scope_header(self):
        def handler(request: httpx.Request) -> httpx.Response:
            assert HTTP_SCOPE_DIGEST_HEADER not in request.headers
            return httpx.Response(200, json={})

        await http_transport(handler).send(_make_request(action="POST:/work", scope_digest=None))

    @pytest.mark.parametrize(
        ("status", "retryable"),
        [(500, True), (503, True), (429, True), (404, False), (400, False)],
    )
    async def test_status_errors_map_to_service_error(self, status, retryable):
        transport = http_transport(lambda request: httpx.Response(status))

        with pytest.raises(ServiceError) as exc_info:
            await transport.send(_make_request(action="GET:/x"))

        assert exc_info.value.code == f"HTTP_{status}"
        assert exc_info.value.retryable is retryable

    async def test_connection_failure_is_connection_error(self):
        def handler(request):
            raise httpx.ConnectError("refused")

        with pytest.raises(TransportConnectionError, match="refused"):
            await http_transport(handler).send(_make_request(action="GET:/x"))

    async def test_connection_timeout_is_typed_timeout(self):
        def handler(request):
            raise httpx.ConnectTimeout("slow handshake")

        with pytest.raises(ConnectionTimeoutError, match="connection deadline"):
            await http_transport(handler).send(_make_request(action="GET:/x"))

    async def test_non_json_body_is_malformed_response(self):
        transport = http_transport(lambda request: httpx.Response(200, text="<html>"))

        with pytest.raises(MalformedResponseError, match="Non-JSON"):
            await transport.send(_make_request(action="GET:/x"))

    async def test_unapproved_origin_fails_during_configuration(self):
        with pytest.raises(TransportConfigurationError, match="not approved"):
            HttpTransport(
                HTTP_CONFIG,
                connect_timeout_sec=CONNECT_TIMEOUT_SECONDS,
                dispatch_timeout_sec=TRANSPORT_TIMEOUT_SECONDS,
                endpoint_policy=HttpEndpointPolicy(allowed_origins={"https://different.example"}),
            )

    async def test_redirect_is_rejected_without_following_target(self):
        requests: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(str(request.url))
            return httpx.Response(302, headers={"location": "https://attacker.example/x"})

        with pytest.raises(ServiceError) as exc_info:
            await http_transport(handler).send(_make_request(action="GET:/x"))

        assert exc_info.value.code == "HTTP_REDIRECT_REJECTED"
        assert requests == ["https://svc:8080/x"]

    async def test_same_origin_redirect_can_be_followed(self):
        config = HttpTransportConfig(
            base_url="https://svc:8080",
            redirects=HttpRedirectPolicy.SAME_ORIGIN,
        )

        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/before":
                return httpx.Response(307, headers={"location": "/after"})
            return httpx.Response(200, json={"redirected": True})

        transport = HttpTransport(
            config,
            connect_timeout_sec=CONNECT_TIMEOUT_SECONDS,
            dispatch_timeout_sec=TRANSPORT_TIMEOUT_SECONDS,
            endpoint_policy=HttpEndpointPolicy(allowed_origins={"https://svc:8080"}),
            client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )

        response = await transport.send(_make_request(action="GET:/before"))

        assert response.data == {"redirected": True}


GRPC_CONFIG = GrpcTransportConfig(
    address="svc:50051",
    security={"mode": "tls", "profile": "service"},
)
GRPC_SECURITY = TransportSecuritySettings(
    grpc_tls_profiles={"service": GrpcTlsProfile(server_name="svc")}
)


def grpc_transport_with(call: AsyncMock) -> GrpcTransport:
    transport = GrpcTransport(
        GRPC_CONFIG,
        connect_timeout_sec=CONNECT_TIMEOUT_SECONDS,
        dispatch_timeout_sec=TRANSPORT_TIMEOUT_SECONDS,
        security=GRPC_SECURITY,
    )
    channel = MagicMock()
    channel.unary_unary = MagicMock(return_value=call)
    transport._channels[GRPC_CONFIG.address] = channel
    return transport


def aio_rpc_error(code: grpc.StatusCode, details: str) -> grpc.aio.AioRpcError:
    return grpc.aio.AioRpcError(code, grpc.aio.Metadata(), grpc.aio.Metadata(), details=details)


class TestGrpcTransport:
    @pytest.mark.parametrize(
        "cancel", [False, True], ids=["concurrent-connect", "canceled-connect"]
    )
    async def test_connection_ownership_survives_concurrency_and_cancellation(
        self, monkeypatch, cancel
    ):
        entered = asyncio.Event()
        release = asyncio.Event()
        channels = []

        async def ready():
            entered.set()
            await release.wait()

        def build_channel(*args, **kwargs):
            channel = MagicMock()
            channel.channel_ready = AsyncMock(side_effect=ready)
            channel.close = AsyncMock()
            channels.append(channel)
            return channel

        grpc_module = MagicMock()
        grpc_module.aio.secure_channel.side_effect = build_channel
        monkeypatch.setattr(
            grpc_transport_module, "load_optional_dependency", lambda *args, **kwargs: grpc_module
        )
        transport = GrpcTransport(
            GRPC_CONFIG,
            connect_timeout_sec=CONNECT_TIMEOUT_SECONDS,
            dispatch_timeout_sec=TRANSPORT_TIMEOUT_SECONDS,
            security=GRPC_SECURITY,
        )
        first = asyncio.create_task(transport._get_channel(GRPC_CONFIG.address))
        await entered.wait()
        if cancel:
            first.cancel()
            with pytest.raises(asyncio.CancelledError):
                await first
        else:
            second = asyncio.create_task(transport._get_channel(GRPC_CONFIG.address))
            await asyncio.sleep(0)
            release.set()
            connected = await asyncio.gather(first, second)
            assert connected[0] is connected[1]
            assert len(channels) == 1
        await transport.close()
        await transport.close()
        for channel in channels:
            channel.close.assert_awaited_once()

    def test_transport_config_cannot_contain_private_key_material(self):
        with pytest.raises(ValidationError, match="client_private_key"):
            GrpcTransportConfig.model_validate(
                {
                    "address": "svc:50051",
                    "security": {"mode": "tls", "profile": "service"},
                    "client_private_key": "secret",
                }
            )

    @pytest.mark.parametrize(
        "address",
        [
            pytest.param("user:password@svc:50051", id="credentials"),
            pytest.param("svc", id="missing-port"),
            pytest.param("svc:invalid", id="invalid-port"),
        ],
    )
    def test_address_is_strict_host_and_port(self, address: str):
        with pytest.raises(ValidationError, match="gRPC address"):
            GrpcTransportConfig(
                address=address,
                security={"mode": "tls", "profile": "service"},
            )

    async def test_send_success(self):
        call = AsyncMock(return_value=b'{"result": 7}')
        response = await grpc_transport_with(call).send(_make_request(action="Preprocess"))
        assert response.data == {"result": 7}
        assert call.await_args.kwargs["metadata"] == (
            (GRPC_SCOPE_DIGEST_METADATA_KEY, SCOPE_DIGEST),
        )

    @pytest.mark.parametrize(
        ("code", "retryable"),
        [
            (grpc.StatusCode.UNAVAILABLE, True),
            (grpc.StatusCode.UNIMPLEMENTED, False),
            (grpc.StatusCode.INVALID_ARGUMENT, False),
            (grpc.StatusCode.UNAUTHENTICATED, False),
        ],
    )
    async def test_rpc_errors_map_to_service_error(self, code, retryable):
        call = AsyncMock(side_effect=aio_rpc_error(code, "boom"))

        with pytest.raises(ServiceError) as exc_info:
            await grpc_transport_with(call).send(_make_request(action="Run"))

        assert exc_info.value.code == code.name
        assert exc_info.value.retryable is retryable

    async def test_rpc_deadline_is_typed_timeout(self):
        call = AsyncMock(side_effect=aio_rpc_error(grpc.StatusCode.DEADLINE_EXCEEDED, "slow"))

        with pytest.raises(DispatchTimeoutError, match="dispatch deadline"):
            await grpc_transport_with(call).send(_make_request(action="Run"))

    async def test_local_rpc_deadline_is_typed_timeout(self):
        async def block_dispatch(*args, **kwargs):
            await asyncio.Future()

        call = AsyncMock(side_effect=block_dispatch)
        transport = grpc_transport_with(call)
        transport._dispatch_timeout_sec = EXPIRED_DEADLINE_SECONDS

        with pytest.raises(DispatchTimeoutError, match="dispatch deadline"):
            await transport.send(_make_request(action="Run"))

    async def test_non_json_response_is_malformed(self):
        call = AsyncMock(return_value=b"not-json")

        with pytest.raises(MalformedResponseError, match="Non-JSON"):
            await grpc_transport_with(call).send(_make_request(action="Run"))

    def test_insecure_mode_is_restricted_to_loopback(self):
        config = GrpcTransportConfig(
            address="service.internal:50051",
            security={"mode": "insecure_local"},
        )

        with pytest.raises(TransportConfigurationError, match="loopback"):
            GrpcTransport(
                config,
                connect_timeout_sec=CONNECT_TIMEOUT_SECONDS,
                dispatch_timeout_sec=TRANSPORT_TIMEOUT_SECONDS,
                security=TransportSecuritySettings(),
            )

    async def test_tls_channel_uses_ca_validation_and_server_name(self, monkeypatch):
        channel = MagicMock()
        channel.channel_ready = AsyncMock()
        channel.close = AsyncMock()
        grpc_module = MagicMock()
        grpc_module.ssl_channel_credentials.return_value = "credentials"
        grpc_module.aio.secure_channel.return_value = channel
        monkeypatch.setattr(
            grpc_transport_module,
            "load_optional_dependency",
            lambda *args, **kwargs: grpc_module,
        )
        transport = GrpcTransport(
            GRPC_CONFIG,
            connect_timeout_sec=CONNECT_TIMEOUT_SECONDS,
            dispatch_timeout_sec=TRANSPORT_TIMEOUT_SECONDS,
            security=GRPC_SECURITY,
        )

        resolved = await transport._get_channel(GRPC_CONFIG.address)

        assert resolved is channel
        grpc_module.ssl_channel_credentials.assert_called_once_with(
            root_certificates=None,
            private_key=None,
            certificate_chain=None,
        )
        grpc_module.aio.secure_channel.assert_called_once_with(
            "svc:50051",
            "credentials",
            options=(("grpc.ssl_target_name_override", "svc"),),
        )

    async def test_mtls_files_are_loaded_from_runtime_profile(
        self,
        monkeypatch,
        tmp_path,
    ):
        root_ca = tmp_path / "ca.pem"
        certificate = tmp_path / "client.pem"
        private_key = tmp_path / "client.key"
        root_ca.write_bytes(b"root-ca")
        certificate.write_bytes(b"client-certificate")
        private_key.write_bytes(b"client-private-key")
        channel = MagicMock()
        channel.channel_ready = AsyncMock()
        grpc_module = MagicMock()
        grpc_module.aio.secure_channel.return_value = channel
        monkeypatch.setattr(
            grpc_transport_module,
            "load_optional_dependency",
            lambda *args, **kwargs: grpc_module,
        )
        security = TransportSecuritySettings(
            grpc_tls_profiles={
                "service": GrpcTlsProfile(
                    server_name="svc",
                    root_ca_path=str(root_ca),
                    client_certificate_path=str(certificate),
                    client_private_key_path=str(private_key),
                )
            }
        )
        transport = GrpcTransport(
            GRPC_CONFIG,
            connect_timeout_sec=CONNECT_TIMEOUT_SECONDS,
            dispatch_timeout_sec=TRANSPORT_TIMEOUT_SECONDS,
            security=security,
        )

        await transport._get_channel(GRPC_CONFIG.address)

        grpc_module.ssl_channel_credentials.assert_called_once_with(
            root_certificates=b"root-ca",
            private_key=b"client-private-key",
            certificate_chain=b"client-certificate",
        )

    async def test_connection_deadline_closes_unready_channel(self, monkeypatch):
        async def block_ready():
            await asyncio.Future()

        channel = MagicMock()
        channel.channel_ready = AsyncMock(side_effect=block_ready)
        channel.close = AsyncMock()
        grpc_module = MagicMock()
        grpc_module.aio.secure_channel.return_value = channel
        monkeypatch.setattr(
            grpc_transport_module,
            "load_optional_dependency",
            lambda *args, **kwargs: grpc_module,
        )
        transport = GrpcTransport(
            GRPC_CONFIG,
            connect_timeout_sec=EXPIRED_DEADLINE_SECONDS,
            dispatch_timeout_sec=TRANSPORT_TIMEOUT_SECONDS,
            security=GRPC_SECURITY,
        )

        with pytest.raises(ConnectionTimeoutError, match="connection deadline"):
            await transport._get_channel(GRPC_CONFIG.address)

        channel.close.assert_awaited_once()


LAMBDA_CONFIG = LambdaTransportConfig(function_name="enricher")


class FakeLambdaClient:
    def __init__(
        self,
        payload: bytes = b"{}",
        function_error: str | None = None,
        raises: Exception | None = None,
    ):
        self._payload = payload
        self._function_error = function_error
        self._raises = raises
        self.invocations: list[dict] = []

    def invoke(self, **kwargs):
        self.invocations.append(kwargs)
        if self._raises:
            raise self._raises
        response = {"Payload": io.BytesIO(self._payload)}
        if self._function_error:
            response["FunctionError"] = self._function_error
        return response


class TestLambdaTransport:
    async def test_send_success(self):
        client = FakeLambdaClient(payload=b'{"enriched": true}')
        response = await LambdaTransport(
            LAMBDA_CONFIG,
            connect_timeout_sec=CONNECT_TIMEOUT_SECONDS,
            dispatch_timeout_sec=TRANSPORT_TIMEOUT_SECONDS,
            lambda_client=client,
        ).send(_make_request(action="enrich"))
        assert response.data == {"enriched": True}
        sent = json.loads(client.invocations[0]["Payload"])
        assert sent["action"] == "enrich"
        assert sent[LAMBDA_SERVICE_CALL_CONTEXT_FIELD] == {
            "scope_digest": SCOPE_DIGEST,
            "version": "1",
        }

    async def test_function_error_is_service_error(self):
        client = FakeLambdaClient(payload=b'{"errorMessage": "died"}', function_error="Unhandled")
        with pytest.raises(ServiceError, match="died") as exc_info:
            await LambdaTransport(
                LAMBDA_CONFIG,
                connect_timeout_sec=CONNECT_TIMEOUT_SECONDS,
                dispatch_timeout_sec=TRANSPORT_TIMEOUT_SECONDS,
                lambda_client=client,
            ).send(_make_request())
        assert exc_info.value.code == "LAMBDA_ERROR"

    async def test_malformed_function_error_is_typed(self):
        client = FakeLambdaClient(payload=b"[]", function_error="Unhandled")

        with pytest.raises(MalformedResponseError, match="Invalid error payload"):
            await LambdaTransport(
                LAMBDA_CONFIG,
                connect_timeout_sec=CONNECT_TIMEOUT_SECONDS,
                dispatch_timeout_sec=TRANSPORT_TIMEOUT_SECONDS,
                lambda_client=client,
            ).send(_make_request())

    async def test_invoke_failure_is_connection_error(self):
        client = FakeLambdaClient(
            raises=ClientError({"Error": {"Code": "Throttling", "Message": "slow down"}}, "Invoke")
        )
        with pytest.raises(TransportConnectionError, match="Throttling"):
            await LambdaTransport(
                LAMBDA_CONFIG,
                connect_timeout_sec=CONNECT_TIMEOUT_SECONDS,
                dispatch_timeout_sec=TRANSPORT_TIMEOUT_SECONDS,
                lambda_client=client,
            ).send(_make_request())

    @pytest.mark.parametrize(
        ("error", "expected"),
        [
            pytest.param(
                ConnectTimeoutError(endpoint_url="https://lambda.example"),
                ConnectionTimeoutError,
                id="connect",
            ),
            pytest.param(
                ReadTimeoutError(endpoint_url="https://lambda.example"),
                DispatchTimeoutError,
                id="read",
            ),
        ],
    )
    async def test_sdk_timeouts_use_typed_taxonomy(self, error, expected):
        client = FakeLambdaClient(raises=error)
        transport = LambdaTransport(
            LAMBDA_CONFIG,
            connect_timeout_sec=CONNECT_TIMEOUT_SECONDS,
            dispatch_timeout_sec=TRANSPORT_TIMEOUT_SECONDS,
            lambda_client=client,
        )

        with pytest.raises(expected):
            await transport.send(_make_request())

    async def test_permanent_aws_error_is_non_retryable_service_error(self):
        client = FakeLambdaClient(
            raises=ClientError(
                {"Error": {"Code": "AccessDeniedException", "Message": "denied"}},
                "Invoke",
            )
        )

        with pytest.raises(ServiceError) as exc_info:
            await LambdaTransport(
                LAMBDA_CONFIG,
                connect_timeout_sec=CONNECT_TIMEOUT_SECONDS,
                dispatch_timeout_sec=TRANSPORT_TIMEOUT_SECONDS,
                lambda_client=client,
            ).send(_make_request())

        assert exc_info.value.code == "AccessDeniedException"
        assert exc_info.value.retryable is False

    async def test_dispatch_deadline_is_typed_timeout(self, monkeypatch):
        async def block_invoke(*args, **kwargs):
            await asyncio.Future()

        monkeypatch.setattr(asyncio, "to_thread", block_invoke)
        transport = LambdaTransport(
            LAMBDA_CONFIG,
            connect_timeout_sec=CONNECT_TIMEOUT_SECONDS,
            dispatch_timeout_sec=EXPIRED_DEADLINE_SECONDS,
            lambda_client=FakeLambdaClient(),
        )

        with pytest.raises(DispatchTimeoutError, match="dispatch deadline"):
            await transport.send(_make_request())

    async def test_non_json_payload_is_malformed(self):
        client = FakeLambdaClient(payload=b"garbage")
        with pytest.raises(MalformedResponseError, match="Non-JSON"):
            await LambdaTransport(
                LAMBDA_CONFIG,
                connect_timeout_sec=CONNECT_TIMEOUT_SECONDS,
                dispatch_timeout_sec=TRANSPORT_TIMEOUT_SECONDS,
                lambda_client=client,
            ).send(_make_request())


QUEUE_CONFIG = QueueTransportConfig(
    broker="main",
    destination="requests",
    idempotency="durable",
)


class TestQueueTransport:
    def test_idempotency_contract_must_be_explicit(self):
        with pytest.raises(ValidationError, match="idempotency"):
            QueueTransportConfig.model_validate({"broker": "main", "destination": "requests"})

    async def test_send_publishes_message_and_signals_wait(self):
        publisher = AsyncMock()
        response = await QueueTransport(
            QUEUE_CONFIG,
            dispatch_timeout_sec=TRANSPORT_TIMEOUT_SECONDS,
            response_timeout_sec=TRANSPORT_TIMEOUT_SECONDS,
            publisher=publisher,
            reply_destination="responses",
        ).send(_make_request())

        assert isinstance(response, AwaitingResponse)
        assert response.timeout_policy.response_timeout_sec == TRANSPORT_TIMEOUT_SECONDS
        destination, published = publisher.publish.await_args.args
        assert destination == "requests"
        assert isinstance(published, PublishedMessage)
        body = json.loads(published.body)
        assert response.key == f"{body['step_invocation_id']}:test_step:test_action"
        assert body["step_name"] == "test_step"
        assert body["reply_destination"] == "responses"
        assert body["message_id"] == published.message_id
        assert body["scope_digest"] == SCOPE_DIGEST

    async def test_send_failure_is_connection_error(self):
        publisher = AsyncMock()
        publisher.publish.side_effect = BrokerConnectionError("unavailable")
        with pytest.raises(TransportConnectionError, match="requests"):
            await QueueTransport(
                QUEUE_CONFIG,
                dispatch_timeout_sec=TRANSPORT_TIMEOUT_SECONDS,
                response_timeout_sec=TRANSPORT_TIMEOUT_SECONDS,
                publisher=publisher,
                reply_destination="responses",
            ).send(_make_request())

    async def test_send_requires_immutable_definition_identity(self):
        publisher = AsyncMock()
        transport = QueueTransport(
            QUEUE_CONFIG,
            dispatch_timeout_sec=TRANSPORT_TIMEOUT_SECONDS,
            response_timeout_sec=TRANSPORT_TIMEOUT_SECONDS,
            publisher=publisher,
            reply_destination="responses",
        )

        with pytest.raises(TransportConnectionError, match="immutable workflow definition"):
            await transport.send(_make_request(definition_digest=None))

        publisher.publish.assert_not_awaited()

    async def test_publish_deadline_is_typed_timeout(self):
        async def block_publish(*args, **kwargs):
            await asyncio.Future()

        publisher = AsyncMock()
        publisher.publish.side_effect = block_publish
        transport = QueueTransport(
            QUEUE_CONFIG,
            dispatch_timeout_sec=EXPIRED_DEADLINE_SECONDS,
            response_timeout_sec=TRANSPORT_TIMEOUT_SECONDS,
            publisher=publisher,
            reply_destination="responses",
        )

        with pytest.raises(DispatchTimeoutError, match="exceeded its deadline"):
            await transport.send(_make_request())
