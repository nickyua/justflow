"""Validated runtime-scope identities and safe external encodings."""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
from dataclasses import dataclass
from enum import Enum
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, RootModel

MAX_SCOPE_COMPONENT_LENGTH = 64
SCOPE_COMPONENT_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._-]*$"
SCOPE_DIGEST_LENGTH = 64
SCOPE_DIGEST_PATTERN = re.compile(rf"^[0-9a-f]{{{SCOPE_DIGEST_LENGTH}}}$")
SCOPED_IDENTITY_VERSION = "jf1"
MAX_SCOPED_IDENTITY_KIND_LENGTH = 32
MAX_CURSOR_BYTES = 8_192
CURSOR_FORMAT_VERSION = 1


class _ScopeIdentity(RootModel[str]):
    model_config = ConfigDict(frozen=True, hide_input_in_errors=True)
    root: str = Field(
        min_length=1,
        max_length=MAX_SCOPE_COMPONENT_LENGTH,
        pattern=SCOPE_COMPONENT_PATTERN,
    )

    def __str__(self) -> str:
        return self.root


class TenantIdentity(_ScopeIdentity):
    """Host-defined tenant identity retained only at trusted boundaries."""


class ApplicationIdentity(_ScopeIdentity):
    """Host-defined application identity within a tenant."""


class EnvironmentIdentity(_ScopeIdentity):
    """Host-defined deployment environment identity within an application."""


class RuntimeScope(BaseModel):
    """One validated tenant/application/environment authority boundary."""

    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    tenant: TenantIdentity = Field(repr=False)
    application: ApplicationIdentity = Field(repr=False)
    environment: EnvironmentIdentity = Field(repr=False)

    @classmethod
    def create(
        cls,
        *,
        tenant: str | TenantIdentity,
        application: str | ApplicationIdentity,
        environment: str | EnvironmentIdentity,
    ) -> Self:
        return cls(
            tenant=tenant if isinstance(tenant, TenantIdentity) else TenantIdentity(tenant),
            application=(
                application
                if isinstance(application, ApplicationIdentity)
                else ApplicationIdentity(application)
            ),
            environment=(
                environment
                if isinstance(environment, EnvironmentIdentity)
                else EnvironmentIdentity(environment)
            ),
        )

    @property
    def digest(self) -> str:
        payload = json.dumps(
            (str(self.tenant), str(self.application), str(self.environment)),
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def model_dump_public(self) -> dict[str, str]:
        return {
            "tenant": str(self.tenant),
            "application": str(self.application),
            "environment": str(self.environment),
        }

    @classmethod
    def from_public(cls, value: object) -> Self:
        if not isinstance(value, dict):
            raise TypeError("Runtime scope must be an object")
        if set(value) != {"tenant", "application", "environment"}:
            raise ValueError("Runtime scope fields are invalid")
        return cls.model_validate(value)


LOCAL_RUNTIME_SCOPE = RuntimeScope.create(
    tenant="local",
    application="justflow",
    environment="development",
)


class UnscopedCompatibilityMode(str, Enum):
    REJECT = "reject"
    LOCAL_ONLY = "local_only"


@dataclass(frozen=True, kw_only=True)
class ScopeCompatibilityPolicy:
    unscoped_mode: UnscopedCompatibilityMode

    def owns(self, observed_scope_digest: str | None, runtime_scope: RuntimeScope) -> bool:
        if observed_scope_digest == runtime_scope.digest:
            return True
        return (
            observed_scope_digest is None
            and self.unscoped_mode is UnscopedCompatibilityMode.LOCAL_ONLY
            and runtime_scope == LOCAL_RUNTIME_SCOPE
        )


# Retire after all unscoped schedules and messages are drained and broker redelivery windows close.
LEGACY_LOCAL_UNSCOPED_POLICY = ScopeCompatibilityPolicy(
    unscoped_mode=UnscopedCompatibilityMode.LOCAL_ONLY
)


class ScopeBindingKind(str, Enum):
    API = "api"
    WEBHOOK = "webhook"
    BROKER = "broker"
    SCHEDULE = "schedule"
    CLOUD_EVENT = "cloud_event"
    HOST = "host"


class TrustedScopeBinding(BaseModel):
    """A scope assignment supplied by host policy rather than event data."""

    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    kind: ScopeBindingKind
    scope: RuntimeScope = Field(repr=False)
    binding_id: str = Field(min_length=1, max_length=MAX_SCOPE_COMPONENT_LENGTH, repr=False)

    @classmethod
    def create(
        cls,
        *,
        kind: ScopeBindingKind,
        scope: RuntimeScope,
        binding_id: str,
    ) -> Self:
        return cls(kind=kind, scope=scope, binding_id=binding_id)


class ScopeResolutionError(PermissionError):
    """Trusted policy cannot resolve the requested runtime scope."""


def require_scope_grant(
    granted_scopes: frozenset[RuntimeScope],
    required_scope: RuntimeScope,
) -> RuntimeScope:
    if required_scope not in granted_scopes:
        raise ScopeResolutionError("The principal has no grant for the runtime scope")
    return required_scope


def require_bound_scope(
    binding: TrustedScopeBinding,
    *,
    expected_kind: ScopeBindingKind,
    required_scope: RuntimeScope,
) -> RuntimeScope:
    if binding.kind is not expected_kind or binding.scope != required_scope:
        raise ScopeResolutionError("The trusted source binding does not match the runtime scope")
    return required_scope


def validate_scope_digest(value: str) -> str:
    if SCOPE_DIGEST_PATTERN.fullmatch(value) is None:
        raise ValueError("Runtime scope digest must be a full lowercase SHA-256 digest")
    return value


def scoped_identity_prefix(kind: str, scope: RuntimeScope) -> str:
    _validate_identity_kind(kind)
    return f"{SCOPED_IDENTITY_VERSION}.{kind}.{scope.digest}."


def scoped_identity(kind: str, scope: RuntimeScope, *parts: str) -> str:
    return scoped_identity_from_digest(kind, scope.digest, *parts)


def scoped_identity_from_digest(kind: str, scope_digest: str, *parts: str) -> str:
    _validate_identity_kind(kind)
    validate_scope_digest(scope_digest)
    prefix = f"{SCOPED_IDENTITY_VERSION}.{kind}.{scope_digest}."
    payload = json.dumps(parts, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return f"{prefix}{hashlib.sha256(payload).hexdigest()}"


def safe_identity_digest(kind: str, value: str) -> str:
    _validate_identity_kind(kind)
    payload = json.dumps((kind, value), ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def identity_belongs_to_scope(value: str, kind: str, scope: RuntimeScope) -> bool:
    return value.startswith(scoped_identity_prefix(kind, scope))


def encode_scope_cursor(scope: RuntimeScope, position: object) -> str:
    payload = json.dumps(
        {
            "format_version": CURSOR_FORMAT_VERSION,
            "position": position,
            "scope_digest": scope.digest,
        },
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    if len(payload) > MAX_CURSOR_BYTES:
        raise ValueError("Continuation cursor exceeds its byte limit")
    return base64.urlsafe_b64encode(payload).decode("ascii")


def decode_scope_cursor(scope: RuntimeScope, cursor: str) -> object:
    if not cursor or len(cursor) > MAX_CURSOR_BYTES * 2:
        raise ValueError("Continuation cursor is invalid")
    try:
        payload = base64.b64decode(cursor.encode("ascii"), altchars=b"-_", validate=True)
        if len(payload) > MAX_CURSOR_BYTES:
            raise ValueError("Continuation cursor exceeds its byte limit")
        value = json.loads(payload)
    except (UnicodeEncodeError, UnicodeDecodeError, binascii.Error, json.JSONDecodeError) as exc:
        raise ValueError("Continuation cursor is invalid") from exc
    if (
        not isinstance(value, dict)
        or set(value) != {"format_version", "position", "scope_digest"}
        or value["format_version"] != CURSOR_FORMAT_VERSION
        or value["scope_digest"] != scope.digest
    ):
        raise ValueError("Continuation cursor does not belong to the runtime scope")
    return value["position"]


def _validate_identity_kind(kind: str) -> None:
    if (
        not kind
        or len(kind) > MAX_SCOPED_IDENTITY_KIND_LENGTH
        or re.fullmatch(r"[a-z][a-z0-9_-]*", kind) is None
    ):
        raise ValueError("Scoped identity kind is invalid")
