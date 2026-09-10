"""Expiring credentials and a same-origin browser boundary for a small trusted cohort."""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
from collections.abc import Callable
from datetime import UTC, datetime
from http import HTTPStatus
from typing import Any, Protocol, Self
from urllib.parse import urlsplit

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from justflow.runtime.auth import (
    AuthenticatedPrincipal,
    AuthenticationError,
    AuthenticationRequest,
    AuthorizationAction,
    AuthorizationRequest,
)
from justflow.runtime.control_api import AsgiMessage, AsgiReceive, AsgiScope, AsgiSend
from justflow.scope import LOCAL_RUNTIME_SCOPE, RuntimeScope

MAX_CREDENTIALS = 100
MAX_CREDENTIAL_ID_LENGTH = 128
MAX_AUTHORIZATION_BYTES = 8192
MIN_TOKEN_BYTES = 32
SHA256_HEX_LENGTH = 64
SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})
PUBLIC_PROBES = frozenset({"/livez", "/readyz"})
BROWSER_CHALLENGE = b'Basic realm="Justflow", charset="UTF-8"'
BROWSER_SECURITY_HEADERS = (
    (b"cache-control", b"no-store"),
    (b"strict-transport-security", b"max-age=31536000"),
    (b"x-frame-options", b"DENY"),
    (b"referrer-policy", b"no-referrer"),
)


class CredentialGrant(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    credential_id: str = Field(pattern=r"^[a-zA-Z0-9_-]+$", max_length=MAX_CREDENTIAL_ID_LENGTH)
    principal_id: str = Field(min_length=1, max_length=MAX_CREDENTIAL_ID_LENGTH)
    token_sha256: SecretStr
    expires_at: AwareDatetime
    scope: RuntimeScope
    actions: frozenset[AuthorizationAction] = Field(min_length=1)

    @field_validator("token_sha256")
    @classmethod
    def validate_digest(cls, value: SecretStr) -> SecretStr:
        digest = value.get_secret_value()
        if len(digest) != SHA256_HEX_LENGTH or any(c not in "0123456789abcdef" for c in digest):
            raise ValueError("Credential digest must be a lowercase SHA-256 digest")
        return value

    @field_validator("scope")
    @classmethod
    def validate_scope(cls, value: RuntimeScope) -> RuntimeScope:
        if value == LOCAL_RUNTIME_SCOPE:
            raise ValueError("Production credentials require an explicit non-local scope")
        return value


class HostSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="HOST_APPLICATION_", extra="forbid", frozen=True, hide_input_in_errors=True
    )

    public_origin: str
    credentials: tuple[CredentialGrant, ...] = Field(min_length=1, max_length=MAX_CREDENTIALS)

    def __init__(self, **values: Any) -> None:
        super().__init__(**values)

    @field_validator("public_origin")
    @classmethod
    def validate_origin(cls, value: str) -> str:
        parsed = urlsplit(value)
        if (
            not value.isascii()
            or any(character.isspace() for character in value)
            or parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path
            or parsed.query
            or parsed.fragment
            or parsed.netloc != parsed.netloc.lower()
            or (parsed.port is not None and parsed.port <= 0)
        ):
            raise ValueError("Public origin must be an HTTPS origin without a path or credentials")
        return value

    @field_validator("credentials")
    @classmethod
    def validate_grants(cls, values: tuple[CredentialGrant, ...]) -> tuple[CredentialGrant, ...]:
        identities: set[str] = set()
        digests: set[str] = set()
        principals: dict[str, CredentialGrant] = {}
        for grant in values:
            digest = grant.token_sha256.get_secret_value()
            if grant.credential_id in identities or digest in digests:
                raise ValueError("Credential identities and token digests must be unique")
            previous = principals.get(grant.principal_id)
            if previous is not None and (previous.scope, previous.actions) != (
                grant.scope,
                grant.actions,
            ):
                raise ValueError(
                    "Rotating credentials must preserve the principal's scope and actions"
                )
            identities.add(grant.credential_id)
            digests.add(digest)
            principals[grant.principal_id] = grant
        return values

    def for_scope(self, scope: RuntimeScope) -> Self:
        if any(grant.scope != scope for grant in self.credentials):
            raise ValueError("Every host credential must grant the configured runtime scope")
        return self


def utc_now() -> datetime:
    return datetime.now(UTC)


class HostAuthentication:
    def __init__(self, settings: HostSettings, *, now: Callable[[], datetime] = utc_now) -> None:
        self._settings = settings
        self._now = now

    async def authenticate(self, request: AuthenticationRequest) -> AuthenticatedPrincipal:
        headers = [value for name, value in request.headers if name.lower() == b"authorization"]
        if len(headers) != 1 or len(headers[0]) > MAX_AUTHORIZATION_BYTES:
            raise AuthenticationError("A single bounded credential is required")
        scheme, separator, value = headers[0].partition(b" ")
        identity: str | None = None
        if separator and scheme.lower() == b"basic":
            try:
                decoded = base64.b64decode(value, validate=True)
                username, delimiter, token = decoded.partition(b":")
                identity = username.decode("ascii")
            except (binascii.Error, UnicodeDecodeError) as exc:
                raise AuthenticationError("Invalid credential") from exc
            if not delimiter:
                raise AuthenticationError("Invalid credential")
        elif separator and scheme.lower() == b"bearer":
            token = value
        else:
            raise AuthenticationError("Unsupported credential")
        if len(token) < MIN_TOKEN_BYTES:
            raise AuthenticationError("Invalid credential")
        digest = hashlib.sha256(token).hexdigest()
        for grant in self._settings.credentials:
            matches = hmac.compare_digest(digest, grant.token_sha256.get_secret_value())
            if matches and (identity is None or identity == grant.credential_id):
                if self._now() >= grant.expires_at:
                    raise AuthenticationError("Expired credential")
                return AuthenticatedPrincipal(
                    principal_id=grant.principal_id, scope_grants=frozenset({grant.scope})
                )
        raise AuthenticationError("Invalid credential")

    async def authorize(
        self, principal: AuthenticatedPrincipal, request: AuthorizationRequest
    ) -> bool:
        return any(
            grant.principal_id == principal.principal_id
            and grant.scope == request.scope == principal.effective_scope
            and request.scope in principal.scope_grants
            and request.action in grant.actions
            and self._now() < grant.expires_at
            for grant in self._settings.credentials
        )


class AsgiApplication(Protocol):
    async def __call__(self, scope: AsgiScope, receive: AsgiReceive, send: AsgiSend) -> None: ...


class BrowserAuthenticationBoundary:
    def __init__(
        self, app: AsgiApplication, authentication: HostAuthentication, settings: HostSettings
    ) -> None:
        self._app = app
        self._authentication = authentication
        self._origin = settings.public_origin.encode("ascii")

    async def __call__(self, scope: AsgiScope, receive: AsgiReceive, send: AsgiSend) -> None:
        if scope.get("type") != "http" or scope.get("path") in PUBLIC_PROBES:
            await self._app(scope, receive, send)
            return
        if scope.get("scheme") != "https":
            await self._respond(send, HTTPStatus.BAD_REQUEST)
            return
        headers = tuple(scope.get("headers", ()))
        origins = [value for name, value in headers if name.lower() == b"origin"]
        basic = any(
            name.lower() == b"authorization" and value.lower().startswith(b"basic ")
            for name, value in headers
        )
        if (origins and origins != [self._origin]) or (
            basic and scope.get("method") not in SAFE_METHODS and origins != [self._origin]
        ):
            await self._respond(send, HTTPStatus.FORBIDDEN)
            return

        async def secured_send(message: AsgiMessage) -> None:
            if message["type"] == "http.response.start":
                response_headers = list(message.get("headers", ()))
                response_headers.extend(BROWSER_SECURITY_HEADERS)
                if message.get("status") == HTTPStatus.UNAUTHORIZED:
                    response_headers.append((b"www-authenticate", BROWSER_CHALLENGE))
                message = {**message, "headers": response_headers}
            await send(message)

        if scope.get("path") == "/" and scope.get("method") == "GET":
            try:
                principal = await self._authentication.authenticate(
                    AuthenticationRequest(method="GET", path="/", headers=headers)
                )
            except AuthenticationError:
                await self._respond(secured_send, HTTPStatus.UNAUTHORIZED)
                return
            allowed = await self._authentication.authorize(
                principal,
                AuthorizationRequest(
                    action=AuthorizationAction.ADMIN_PANEL_VIEW, scope=principal.scope_binding.scope
                ),
            )
            await self._respond(
                secured_send,
                HTTPStatus.SEE_OTHER if allowed else HTTPStatus.FORBIDDEN,
                headers=[(b"location", b"/admin")] if allowed else [],
            )
            return
        await self._app(scope, receive, secured_send)

    @staticmethod
    async def _respond(
        send: AsgiSend, status: HTTPStatus, *, headers: list[tuple[bytes, bytes]] | None = None
    ) -> None:
        await send(AsgiMessage(type="http.response.start", status=status, headers=headers or []))
        await send(AsgiMessage(type="http.response.body", body=b""))
