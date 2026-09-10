"""Host-supplied authentication and authorization boundary."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Protocol

from justflow.scope import (
    LOCAL_RUNTIME_SCOPE,
    RuntimeScope,
    ScopeBindingKind,
    TrustedScopeBinding,
    safe_identity_digest,
)

MAX_PRINCIPAL_IDENTITY_LENGTH = 256
MAX_AUTHORIZATION_RESOURCE_LENGTH = 512
AUTHORIZATION_DIGEST_LENGTH = 64
MAX_PRINCIPAL_SCOPE_GRANTS = 1_000


class AuthorizationAction(str, Enum):
    """Closed set of host permissions checked by the public control and authoring APIs."""

    START = "start"
    LIST = "list"
    DESCRIBE = "describe"
    SIGNAL = "signal"
    CANCEL = "cancel"
    TERMINATE = "terminate"
    TRIGGER_PAUSE = "trigger_pause"
    TRIGGER_RESUME = "trigger_resume"
    TRIGGER_RUN = "trigger_run"
    TRIGGER_DELETE = "trigger_delete"
    TRIGGER_APPLY = "trigger_apply"
    SCHEDULED_START_CREATE = "scheduled_start_create"
    SCHEDULED_START_VIEW = "scheduled_start_view"
    SCHEDULED_START_RESCHEDULE = "scheduled_start_reschedule"
    SCHEDULED_START_CANCEL = "scheduled_start_cancel"
    HEALTH = "health"
    METRICS = "metrics"
    CONFIGURATION_VIEW = "configuration_view"
    CONFIGURATION_EDIT = "configuration_edit"
    CONFIGURATION_VALIDATE = "configuration_validate"
    CONFIGURATION_APPLY = "configuration_apply"
    CONFIGURATION_DISCARD = "configuration_discard"
    CONFIGURATION_PUBLISH = "configuration_publish"
    CONFIGURATION_ACTIVATE = "configuration_activate"
    CONFIGURATION_ROLLBACK = "configuration_rollback"
    CAPABILITY_POLICY_ADMINISTER = "capability_policy_administer"
    OPERATIONS_VIEW = "operations_view"
    ADMIN_PANEL_VIEW = "admin_panel_view"


@dataclass(frozen=True, kw_only=True)
class AuthenticationRequest:
    """Validated request metadata; credentials are sensitive and excluded from repr."""

    method: str
    path: str
    headers: tuple[tuple[bytes, bytes], ...] = field(repr=False)


@dataclass(frozen=True, kw_only=True)
class AuthenticatedPrincipal:
    """Verified host identity with granted scopes and one selected effective scope.

    Supply grants from trusted host identity policy, never from request body fields.
    Keep principal_id stable across credential rotation so operation retry ownership
    and actor digests remain stable. Customer IDs belong in workflow input, not here.
    """

    principal_id: str = field(repr=False)
    scope_grants: frozenset[RuntimeScope] = field(repr=False)
    effective_scope: RuntimeScope | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if not self.principal_id or len(self.principal_id) > MAX_PRINCIPAL_IDENTITY_LENGTH:
            raise ValueError("Authenticated principal identity is invalid")
        if not self.scope_grants:
            raise ValueError("Authenticated principal requires at least one runtime-scope grant")
        if len(self.scope_grants) > MAX_PRINCIPAL_SCOPE_GRANTS:
            raise ValueError("Authenticated principal has too many runtime-scope grants")
        if self.effective_scope is None:
            if len(self.scope_grants) != 1:
                raise ValueError(
                    "Authenticated principal with multiple grants requires one effective scope"
                )
            object.__setattr__(self, "effective_scope", next(iter(self.scope_grants)))
        elif self.effective_scope not in self.scope_grants:
            raise ValueError("Authenticated principal effective scope is not granted")

    @classmethod
    def for_local_development(cls, principal_id: str) -> AuthenticatedPrincipal:
        return cls(
            principal_id=principal_id,
            scope_grants=frozenset({LOCAL_RUNTIME_SCOPE}),
        )

    @property
    def actor_digest(self) -> str:
        return safe_identity_digest("actor", self.principal_id)

    @property
    def scope_binding(self) -> TrustedScopeBinding:
        effective_scope = self.effective_scope
        if effective_scope is None:
            raise RuntimeError("Authenticated principal has no effective runtime scope")
        return TrustedScopeBinding.create(
            kind=ScopeBindingKind.API,
            scope=effective_scope,
            binding_id=self.actor_digest,
        )


@dataclass(frozen=True, kw_only=True)
class AuthorizationRequest:
    """Action and trusted scope to authorize, optionally with a scoped resource digest.

    A None resource is a capability-level check; it does not grant access to every
    resource. The provider is called again for the concrete operation.
    """

    action: AuthorizationAction
    scope: RuntimeScope = field(repr=False)
    resource_identity_digest: str | None = None

    def __post_init__(self) -> None:
        if self.resource_identity_digest is not None and (
            len(self.resource_identity_digest) != AUTHORIZATION_DIGEST_LENGTH
            or any(
                character not in "0123456789abcdef" for character in self.resource_identity_digest
            )
        ):
            raise ValueError("Authorization resource identity is invalid")


def authorization_resource_digest(
    scope: RuntimeScope,
    resource: str | None,
) -> str | None:
    if resource is None:
        return None
    if not resource or len(resource) > MAX_AUTHORIZATION_RESOURCE_LENGTH:
        raise ValueError("Authorization resource identity is invalid")
    return safe_identity_digest("authorization-resource", f"{scope.digest}:{resource}")


class AuthenticationError(Exception):
    """Credentials are absent, invalid or expired; the API returns a generic 401."""


class AuthenticationProvider(Protocol):
    """Host-owned identity boundary shared by custom clients and the administration UI.

    Methods run on the API event loop: use async I/O or bounded offloading for remote
    identity lookups. Raise AuthenticationError for rejected credentials; unexpected
    provider failures become a generic 503. Return False for denied authorization.
    """

    async def authenticate(self, request: AuthenticationRequest) -> AuthenticatedPrincipal:
        """Verify the credential and resolve its trusted principal and scope grants."""
        ...

    async def authorize(
        self,
        principal: AuthenticatedPrincipal,
        request: AuthorizationRequest,
    ) -> bool:
        """Check the requested action, scope and resource against host policy."""
        ...
