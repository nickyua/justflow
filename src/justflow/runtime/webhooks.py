"""Host-registered signed webhook normalization and start ingress."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from justflow.config.grammar import ProviderName
from justflow.config.provider_names import is_valid_provider_name
from justflow.runtime.starter import (
    StartWorkflowRequest,
    StartWorkflowResult,
    WebhookSourceIdentity,
    WorkflowStarter,
)
from justflow.scope import (
    LOCAL_RUNTIME_SCOPE,
    RuntimeScope,
    ScopeBindingKind,
    TrustedScopeBinding,
)

MAX_WEBHOOK_IDENTITY_LENGTH = 128


class StrictWebhookModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)


class WebhookEventIdentity(StrictWebhookModel):
    provider: ProviderName
    event_id: str = Field(
        min_length=1,
        max_length=MAX_WEBHOOK_IDENTITY_LENGTH,
        repr=False,
    )


@dataclass(frozen=True, kw_only=True)
class SignedWebhookRequest:
    headers: tuple[tuple[bytes, bytes], ...] = field(repr=False)
    body: bytes = field(repr=False)


class WebhookErrorCode(str, Enum):
    UNKNOWN_SOURCE = "unknown_source"
    VERIFICATION_FAILED = "verification_failed"
    INVALID_PAYLOAD = "invalid_payload"
    INVALID_IDENTITY = "invalid_identity"
    PROVIDER_UNAVAILABLE = "provider_unavailable"


class WebhookError(Exception):
    def __init__(self, code: WebhookErrorCode, message: str, *, retryable: bool) -> None:
        self.code = code
        self.retryable = retryable
        super().__init__(message)


class WebhookVerificationError(Exception):
    pass


class WebhookPayloadError(Exception):
    pass


class WebhookSourceAdapter(Protocol):
    async def verify(self, request: SignedWebhookRequest) -> None: ...

    async def parse(self, request: SignedWebhookRequest) -> object: ...

    async def event_identity(self, event: object) -> WebhookEventIdentity: ...

    async def to_start_request(
        self,
        event: object,
        identity: WebhookEventIdentity,
    ) -> StartWorkflowRequest: ...


class WebhookSourceRegistry:
    def __init__(self, *, scope: RuntimeScope = LOCAL_RUNTIME_SCOPE) -> None:
        self._sources: dict[str, WebhookSourceAdapter] = {}
        self._scopes: dict[str, RuntimeScope] = {}
        self._default_scope = scope

    @property
    def sources(self) -> Mapping[str, WebhookSourceAdapter]:
        return MappingProxyType(self._sources)

    @property
    def scope(self) -> RuntimeScope:
        return self._default_scope

    def register(
        self,
        name: str,
        adapter: WebhookSourceAdapter,
        *,
        scope: RuntimeScope | None = None,
    ) -> None:
        if not is_valid_provider_name(name):
            raise ValueError(f"Invalid webhook source name '{name}'")
        if name in self._sources:
            raise ValueError(f"Webhook source '{name}' is already registered")
        self._sources[name] = adapter
        self._scopes[name] = scope or self._default_scope

    def resolve(self, name: str) -> WebhookSourceAdapter:
        try:
            return self._sources[name]
        except KeyError as exc:
            raise WebhookError(
                WebhookErrorCode.UNKNOWN_SOURCE,
                "Webhook source is not registered",
                retryable=False,
            ) from exc

    def scope_for(self, name: str) -> RuntimeScope:
        self.resolve(name)
        return self._scopes[name]


class WebhookIngress:
    def __init__(
        self,
        registry: WebhookSourceRegistry,
        starter: WorkflowStarter,
    ) -> None:
        self._registry = registry
        self._starter = starter

    async def receive(
        self,
        source_name: str,
        request: SignedWebhookRequest,
    ) -> StartWorkflowResult:
        adapter = self._registry.resolve(source_name)
        bound_scope = self._registry.scope_for(source_name)
        try:
            await adapter.verify(request)
        except WebhookVerificationError as exc:
            raise WebhookError(
                WebhookErrorCode.VERIFICATION_FAILED,
                "Webhook signature verification failed",
                retryable=False,
            ) from exc
        except Exception as exc:
            raise WebhookError(
                WebhookErrorCode.PROVIDER_UNAVAILABLE,
                "Webhook verification provider is unavailable",
                retryable=True,
            ) from exc
        try:
            event = await adapter.parse(request)
            identity = await adapter.event_identity(event)
            start_request = await adapter.to_start_request(event, identity)
        except (ValidationError, WebhookPayloadError) as exc:
            raise WebhookError(
                WebhookErrorCode.INVALID_PAYLOAD,
                "Webhook payload is invalid",
                retryable=False,
            ) from exc
        except Exception as exc:
            raise WebhookError(
                WebhookErrorCode.PROVIDER_UNAVAILABLE,
                "Webhook source provider could not normalize the event",
                retryable=True,
            ) from exc
        _validate_conversion(source_name, identity, start_request)
        return await self._starter.start(
            start_request,
            scope_binding=TrustedScopeBinding.create(
                kind=ScopeBindingKind.WEBHOOK,
                scope=bound_scope,
                binding_id=source_name,
            ),
        )


def make_webhook_business_request_id(identity: WebhookEventIdentity) -> str:
    canonical = json.dumps(
        ("webhook", identity.provider, identity.event_id),
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _validate_conversion(
    source_name: str,
    identity: WebhookEventIdentity,
    request: StartWorkflowRequest,
) -> None:
    source = request.source
    if (
        identity.provider != source_name
        or not isinstance(source, WebhookSourceIdentity)
        or source.provider != identity.provider
        or source.event_id != identity.event_id
        or request.business_request_id != make_webhook_business_request_id(identity)
    ):
        raise WebhookError(
            WebhookErrorCode.INVALID_IDENTITY,
            "Webhook conversion did not preserve its stable source identity",
            retryable=False,
        )
