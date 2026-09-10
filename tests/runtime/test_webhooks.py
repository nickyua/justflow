"""Tests for signed webhook normalization and idempotent workflow starts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TypeAlias

import pytest

from justflow.provenance import WorkerArtifactIdentity
from justflow.runtime.starter import (
    StartStatus,
    StartWorkflowRequest,
    StartWorkflowResult,
    WebhookSourceIdentity,
)
from justflow.runtime.webhooks import (
    SignedWebhookRequest,
    WebhookError,
    WebhookErrorCode,
    WebhookEventIdentity,
    WebhookIngress,
    WebhookPayloadError,
    WebhookSourceRegistry,
    WebhookVerificationError,
    make_webhook_business_request_id,
)
from justflow.scope import TrustedScopeBinding

DEFINITION_DIGEST = "a" * 64
SOURCE_DIGEST = "b" * 64
ENVIRONMENT_SNAPSHOT_DIGEST = "e" * 64


@dataclass(frozen=True, kw_only=True)
class Returns:
    status: StartStatus


@dataclass(frozen=True, kw_only=True)
class Raises:
    code: WebhookErrorCode
    retryable: bool


Outcome: TypeAlias = Returns | Raises


@dataclass(frozen=True, kw_only=True)
class WebhookIngressCase:
    id: str
    verify_error: Exception | None
    parse_error: Exception | None
    request_provider: str
    outcome: Outcome


CASES = [
    WebhookIngressCase(
        id="accepted",
        verify_error=None,
        parse_error=None,
        request_provider="example",
        outcome=Returns(status=StartStatus.STARTED),
    ),
    WebhookIngressCase(
        id="signature-rejected",
        verify_error=WebhookVerificationError("invalid"),
        parse_error=None,
        request_provider="example",
        outcome=Raises(code=WebhookErrorCode.VERIFICATION_FAILED, retryable=False),
    ),
    WebhookIngressCase(
        id="payload-rejected",
        verify_error=None,
        parse_error=WebhookPayloadError("invalid"),
        request_provider="example",
        outcome=Raises(code=WebhookErrorCode.INVALID_PAYLOAD, retryable=False),
    ),
    WebhookIngressCase(
        id="provider-unavailable",
        verify_error=None,
        parse_error=RuntimeError("offline"),
        request_provider="example",
        outcome=Raises(code=WebhookErrorCode.PROVIDER_UNAVAILABLE, retryable=True),
    ),
    WebhookIngressCase(
        id="identity-not-preserved",
        verify_error=None,
        parse_error=None,
        request_provider="other",
        outcome=Raises(code=WebhookErrorCode.INVALID_IDENTITY, retryable=False),
    ),
]


class FakeStarter:
    def __init__(self) -> None:
        self.requests: list[StartWorkflowRequest] = []
        self.scope_bindings: list[TrustedScopeBinding] = []

    async def start(
        self,
        request: StartWorkflowRequest,
        *,
        scope_binding: TrustedScopeBinding,
    ) -> StartWorkflowResult:
        self.requests.append(request)
        self.scope_bindings.append(scope_binding)
        return StartWorkflowResult(
            workflow_id="example-workflow",
            run_id="run-1",
            workflow_name="example",
            definition_digest=DEFINITION_DIGEST,
            environment_snapshot_digest=ENVIRONMENT_SNAPSHOT_DIGEST,
            trigger_name="example_webhook",
            artifact_identity=WorkerArtifactIdentity(
                deployment_name="deployment",
                build_id="build",
                artifact_digest=f"sha256:{'c' * 64}",
                package_version="1.0.0",
            ),
            source_identity_digest=SOURCE_DIGEST,
            status=StartStatus.STARTED,
        )


class FakeAdapter:
    def __init__(
        self,
        *,
        verify_error: Exception | None,
        parse_error: Exception | None,
        request_provider: str,
    ) -> None:
        self._verify_error = verify_error
        self._parse_error = parse_error
        self._request_provider = request_provider
        self.seen_request: SignedWebhookRequest | None = None

    async def verify(self, request: SignedWebhookRequest) -> None:
        self.seen_request = request
        if self._verify_error is not None:
            raise self._verify_error

    async def parse(self, request: SignedWebhookRequest) -> object:
        if self._parse_error is not None:
            raise self._parse_error
        return {"kind": "created"}

    async def event_identity(self, event: object) -> WebhookEventIdentity:
        return WebhookEventIdentity(provider="example", event_id="delivery-1")

    async def to_start_request(
        self,
        event: object,
        identity: WebhookEventIdentity,
    ) -> StartWorkflowRequest:
        request_identity = WebhookEventIdentity(
            provider=self._request_provider,
            event_id=identity.event_id,
        )
        return StartWorkflowRequest(
            workflow_name="example",
            business_request_id=make_webhook_business_request_id(identity),
            input={"kind": "created"},
            source=WebhookSourceIdentity(
                provider=request_identity.provider,
                event_id=request_identity.event_id,
            ),
        )


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.id)
async def test_webhook_ingress_contract(case: WebhookIngressCase) -> None:
    starter = FakeStarter()
    adapter = FakeAdapter(
        verify_error=case.verify_error,
        parse_error=case.parse_error,
        request_provider=case.request_provider,
    )
    registry = WebhookSourceRegistry()
    registry.register("example", adapter)
    ingress = WebhookIngress(registry, starter)
    signed_request = SignedWebhookRequest(
        headers=((b"x-signature", b"signature-material"),),
        body=b'{"kind":"created"}',
    )

    if isinstance(case.outcome, Raises):
        with pytest.raises(WebhookError) as exc_info:
            await ingress.receive("example", signed_request)
        assert exc_info.value.code is case.outcome.code
        assert exc_info.value.retryable is case.outcome.retryable
        assert starter.requests == []
        return

    result = await ingress.receive("example", signed_request)

    assert result.status is case.outcome.status
    assert adapter.seen_request is signed_request
    assert len(starter.requests) == 1
    assert starter.requests[0].business_request_id == make_webhook_business_request_id(
        WebhookEventIdentity(provider="example", event_id="delivery-1")
    )
    assert starter.scope_bindings[0].scope == registry.scope_for("example")


def test_webhook_registry_rejects_unknown_and_duplicate_sources() -> None:
    adapter = FakeAdapter(verify_error=None, parse_error=None, request_provider="example")
    registry = WebhookSourceRegistry()
    registry.register("example", adapter)

    with pytest.raises(ValueError, match="already registered"):
        registry.register("example", adapter)
    with pytest.raises(WebhookError) as exc_info:
        registry.resolve("missing")

    assert exc_info.value.code is WebhookErrorCode.UNKNOWN_SOURCE


def test_signed_webhook_values_are_hidden_from_representations() -> None:
    request = SignedWebhookRequest(
        headers=((b"authorization", b"private-header-value"),),
        body=b"private-body-value",
    )
    identity = WebhookEventIdentity(provider="example", event_id="private-event-value")

    rendered = f"{request!r} {identity!r}"

    assert "private-header-value" not in rendered
    assert "private-body-value" not in rendered
    assert "private-event-value" not in rendered
