"""HTTP transport - synchronous HTTP requests."""

from __future__ import annotations

import asyncio
import json
from enum import Enum
from urllib.parse import urlsplit

import httpx
from pydantic import Field, field_validator

from justflow.config.models import MAX_PATH_LENGTH
from justflow.config.runtime_limits import DEFAULT_ACTIVITY_OUTPUT_BYTES
from justflow.sdk.service_context import HTTP_SCOPE_DIGEST_HEADER
from justflow.transports.base import (
    ActionDispatchError,
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
from justflow.transports.security import HttpEndpointPolicy

# 4xx responses (except 429) are the caller's fault and won't heal on retry.
RETRYABLE_STATUS_MIN = 500
RETRYABLE_STATUS_EXTRA = frozenset({408, 429})
HTTPS_SCHEME = "https"
MAX_HTTP_REDIRECTS = 5
HTTP_READ_CHUNK_BYTES = 64 * 1024


class HttpRedirectPolicy(str, Enum):
    DENY = "deny"
    SAME_ORIGIN = "same_origin"


class HttpTransportConfig(StrictTransportConfig):
    base_url: str = Field(min_length=1, max_length=MAX_PATH_LENGTH)
    redirects: HttpRedirectPolicy = HttpRedirectPolicy.DENY
    max_response_bytes: int = Field(default=DEFAULT_ACTIVITY_OUTPUT_BYTES, ge=1)

    @field_validator("base_url")
    @classmethod
    def validate_https_endpoint(cls, value: str) -> str:
        parsed = urlsplit(value)
        if (
            parsed.scheme != HTTPS_SCHEME
            or not parsed.netloc
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("HTTP transport base_url must be an absolute HTTPS URL")
        return value.rstrip("/")


class HttpTransport:
    def __init__(
        self,
        config: HttpTransportConfig,
        *,
        connect_timeout_sec: int,
        dispatch_timeout_sec: int,
        endpoint_policy: HttpEndpointPolicy,
        client: httpx.AsyncClient | None = None,
    ):
        self._config = config
        self._connect_timeout_sec = connect_timeout_sec
        self._dispatch_timeout_sec = dispatch_timeout_sec
        try:
            self._approved_origin = endpoint_policy.require_approved(config.base_url)
        except ValueError as exc:
            raise TransportConfigurationError(str(exc)) from exc
        self._endpoint_policy = endpoint_policy
        self._client = client

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(follow_redirects=False)
        return self._client

    async def send(self, request: TransportRequest) -> Completed:
        client = await self._get_client()

        try:
            method, path = request.action.split(":", 1)
        except ValueError as exc:
            raise ActionDispatchError(
                f"HTTP action must follow 'METHOD:/path' format, got '{request.action}'"
            ) from exc
        url = f"{self._config.base_url}{path}"

        body = {
            "globals": request.globals,
            "input": request.input,
        }
        context = request.service_call_context
        headers = {"Accept-Encoding": "identity"}
        if context.scope_digest is not None:
            headers[HTTP_SCOPE_DIGEST_HEADER] = context.scope_digest

        timeout = httpx.Timeout(
            self._dispatch_timeout_sec,
            connect=self._connect_timeout_sec,
        )
        try:
            async with asyncio.timeout(self._dispatch_timeout_sec):
                outgoing = client.build_request(
                    method=method,
                    url=url,
                    headers=headers,
                    json=body,
                    timeout=timeout,
                )
                response = await client.send(outgoing, stream=True, follow_redirects=False)
                try:
                    response = await self._follow_redirects(client, response)
                    response.raise_for_status()
                    if response.headers.get("content-encoding", "identity").lower() != "identity":
                        raise ServiceError(
                            "HTTP transport requires an identity-encoded response",
                            code="HTTP_CONTENT_ENCODING_UNSUPPORTED",
                            retryable=False,
                        )
                    payload = bytearray()
                    async for chunk in response.aiter_bytes(chunk_size=HTTP_READ_CHUNK_BYTES):
                        if len(payload) + len(chunk) > self._config.max_response_bytes:
                            raise ServiceError(
                                "HTTP response exceeds its configured byte limit",
                                code="HTTP_RESPONSE_TOO_LARGE",
                                retryable=False,
                            )
                        payload.extend(chunk)
                    return Completed(data=json.loads(payload))
                finally:
                    await response.aclose()
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise MalformedResponseError("Non-JSON HTTP response") from exc
        except httpx.ConnectTimeout as e:
            raise ConnectionTimeoutError("HTTP connection deadline exceeded") from e
        except httpx.TimeoutException as e:
            raise DispatchTimeoutError("HTTP dispatch deadline exceeded") from e
        except TimeoutError as e:
            raise DispatchTimeoutError("HTTP dispatch deadline exceeded") from e
        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            raise ServiceError(
                str(e),
                code=f"HTTP_{status}",
                retryable=status >= RETRYABLE_STATUS_MIN or status in RETRYABLE_STATUS_EXTRA,
            ) from e
        except httpx.RequestError as e:
            raise TransportConnectionError(str(e)) from e

    async def _follow_redirects(
        self,
        client: httpx.AsyncClient,
        response: httpx.Response,
    ) -> httpx.Response:
        try:
            redirects = 0
            while response.has_redirect_location:
                if self._config.redirects is HttpRedirectPolicy.DENY:
                    raise ServiceError(
                        "HTTP redirect rejected by service policy",
                        code="HTTP_REDIRECT_REJECTED",
                        retryable=False,
                    )
                redirects += 1
                if redirects > MAX_HTTP_REDIRECTS:
                    raise ServiceError(
                        "HTTP redirect limit exceeded",
                        code="HTTP_REDIRECT_LIMIT",
                        retryable=False,
                    )
                next_request = response.next_request
                if next_request is None:
                    raise ServiceError(
                        "HTTP redirect did not provide a valid target",
                        code="HTTP_REDIRECT_INVALID",
                        retryable=False,
                    )
                try:
                    redirect_origin = self._endpoint_policy.require_approved(str(next_request.url))
                except ValueError as exc:
                    raise ServiceError(
                        "HTTP redirect target is not approved",
                        code="HTTP_REDIRECT_REJECTED",
                        retryable=False,
                    ) from exc
                if redirect_origin != self._approved_origin:
                    raise ServiceError(
                        "Cross-origin HTTP redirect rejected",
                        code="HTTP_REDIRECT_REJECTED",
                        retryable=False,
                    )
                await response.aclose()
                response = await client.send(next_request, stream=True, follow_redirects=False)
            return response
        except BaseException:
            await response.aclose()
            raise

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
