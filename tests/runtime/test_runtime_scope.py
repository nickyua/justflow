from __future__ import annotations

from dataclasses import dataclass

import pytest
from pydantic import ValidationError

from justflow.runtime.auth import (
    AuthenticatedPrincipal,
    AuthorizationAction,
    AuthorizationRequest,
    authorization_resource_digest,
)
from justflow.scope import (
    LEGACY_LOCAL_UNSCOPED_POLICY,
    LOCAL_RUNTIME_SCOPE,
    ApplicationIdentity,
    EnvironmentIdentity,
    RuntimeScope,
    ScopeBindingKind,
    ScopeResolutionError,
    TenantIdentity,
    TrustedScopeBinding,
    decode_scope_cursor,
    encode_scope_cursor,
    require_bound_scope,
    require_scope_grant,
    scoped_identity,
    validate_scope_digest,
)
from justflow.sdk.message_contract import make_workflow_id

SCOPE_A = RuntimeScope.create(
    tenant="tenant-a",
    application="orders",
    environment="production",
)
SCOPE_B = RuntimeScope.create(
    tenant="tenant-b",
    application="orders",
    environment="production",
)


@dataclass(frozen=True, kw_only=True)
class IdentityCase:
    id: str
    model: type[TenantIdentity | ApplicationIdentity | EnvironmentIdentity]
    value: str
    valid: bool


IDENTITY_CASES = [
    IdentityCase(id="tenant-valid", model=TenantIdentity, value="tenant-a", valid=True),
    IdentityCase(id="application-empty", model=ApplicationIdentity, value="", valid=False),
    IdentityCase(
        id="environment-path",
        model=EnvironmentIdentity,
        value="../../production",
        valid=False,
    ),
]


@pytest.mark.parametrize("case", IDENTITY_CASES, ids=lambda case: case.id)
def test_scope_identity_contract(case: IdentityCase) -> None:
    if case.valid:
        assert str(case.model(case.value)) == case.value
    else:
        with pytest.raises(ValidationError):
            case.model(case.value)


def test_scoped_idempotency_and_authorization_resources_do_not_collide() -> None:
    workflow_a = make_workflow_id("orders", "request-1", scope=SCOPE_A)
    workflow_b = make_workflow_id("orders", "request-1", scope=SCOPE_B)

    assert workflow_a != workflow_b
    assert scoped_identity("schedule", SCOPE_A, "daily") != scoped_identity(
        "schedule", SCOPE_B, "daily"
    )
    assert authorization_resource_digest(SCOPE_A, workflow_a) != authorization_resource_digest(
        SCOPE_B, workflow_b
    )


def test_principal_grants_and_authorization_request_are_explicitly_scoped() -> None:
    principal = AuthenticatedPrincipal(
        principal_id="principal-1",
        scope_grants=frozenset({SCOPE_A}),
    )
    request = AuthorizationRequest(
        action=AuthorizationAction.DESCRIBE,
        scope=SCOPE_A,
        resource_identity_digest=authorization_resource_digest(SCOPE_A, "workflow-1"),
    )

    assert require_scope_grant(principal.scope_grants, SCOPE_A) == SCOPE_A
    assert request.scope == SCOPE_A
    assert "tenant-a" not in repr(principal)
    assert "tenant-a" not in repr(request)
    assert "tenant-a" not in repr(SCOPE_A)
    with pytest.raises(ScopeResolutionError):
        require_scope_grant(principal.scope_grants, SCOPE_B)


def test_principal_requires_one_effective_scope_from_its_grants() -> None:
    principal = AuthenticatedPrincipal(
        principal_id="principal-1",
        scope_grants=frozenset({SCOPE_A, SCOPE_B}),
        effective_scope=SCOPE_B,
    )

    assert principal.scope_binding.scope == SCOPE_B
    with pytest.raises(ValueError, match="requires one effective"):
        AuthenticatedPrincipal(
            principal_id="principal-1",
            scope_grants=frozenset({SCOPE_A, SCOPE_B}),
        )
    with pytest.raises(ValueError, match="not granted"):
        AuthenticatedPrincipal(
            principal_id="principal-1",
            scope_grants=frozenset({SCOPE_A}),
            effective_scope=SCOPE_B,
        )


def test_local_development_principal_factory_grants_only_local_scope() -> None:
    principal = AuthenticatedPrincipal.for_local_development("local-operator")

    assert principal.scope_grants == frozenset({LOCAL_RUNTIME_SCOPE})
    assert principal.effective_scope == LOCAL_RUNTIME_SCOPE


def test_unscoped_compatibility_is_confined_to_local_runtime() -> None:
    assert LEGACY_LOCAL_UNSCOPED_POLICY.owns(None, LOCAL_RUNTIME_SCOPE) is True
    assert LEGACY_LOCAL_UNSCOPED_POLICY.owns(None, SCOPE_A) is False


def test_scope_compatibility_accepts_only_the_runtime_digest() -> None:
    assert LEGACY_LOCAL_UNSCOPED_POLICY.owns(SCOPE_A.digest, SCOPE_A) is True
    assert LEGACY_LOCAL_UNSCOPED_POLICY.owns(SCOPE_B.digest, SCOPE_A) is False


def test_continuation_cursor_cannot_cross_scope() -> None:
    cursor = encode_scope_cursor(SCOPE_A, {"position": "next"})

    assert decode_scope_cursor(SCOPE_A, cursor) == {"position": "next"}
    with pytest.raises(ValueError, match="does not belong"):
        decode_scope_cursor(SCOPE_B, cursor)


def test_runtime_scope_public_and_trusted_binding_contracts() -> None:
    assert RuntimeScope.from_public(SCOPE_A.model_dump_public()) == SCOPE_A
    binding = TrustedScopeBinding.create(
        kind=ScopeBindingKind.BROKER,
        scope=SCOPE_A,
        binding_id="orders-subscription",
    )
    assert (
        require_bound_scope(
            binding,
            expected_kind=ScopeBindingKind.BROKER,
            required_scope=SCOPE_A,
        )
        == SCOPE_A
    )
    with pytest.raises(ScopeResolutionError):
        require_bound_scope(
            binding,
            expected_kind=ScopeBindingKind.WEBHOOK,
            required_scope=SCOPE_A,
        )
    with pytest.raises(TypeError, match="object"):
        RuntimeScope.from_public("tenant-a")
    with pytest.raises(ValueError, match="fields"):
        RuntimeScope.from_public({"tenant": "tenant-a"})
    with pytest.raises(ValueError, match="SHA-256"):
        validate_scope_digest("not-a-digest")
