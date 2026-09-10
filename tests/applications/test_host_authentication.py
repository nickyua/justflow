"""Credential lifecycle, browser request protection and real API authorization."""

from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from http import HTTPStatus
from pathlib import Path
from typing import Generic, TypeVar
from unittest.mock import MagicMock

import httpx
import pytest
from host_application.authentication import (
    BROWSER_CHALLENGE,
    BrowserAuthenticationBoundary,
    CredentialGrant,
    HostAuthentication,
    HostSettings,
)
from justflow_admin import BetaAdminPanel
from pydantic import SecretStr, ValidationError

from justflow.config.settings import ControlSettings
from justflow.provenance import RuntimeProfile
from justflow.runtime.auth import (
    AuthenticationError,
    AuthenticationRequest,
    AuthorizationAction,
    AuthorizationRequest,
)
from justflow.runtime.control_api import ControlApi
from justflow.runtime.health import HealthRegistry
from justflow.runtime.metrics import MetricsRegistry
from justflow.runtime.operations import WorkflowControlService
from justflow.runtime.starter import WorkflowStarter
from justflow.scope import RuntimeScope
from tests.settings import PRODUCTION_SCOPE

NOW = datetime(2026, 9, 9, tzinfo=UTC)
TOKEN = "test-only-opaque-credential-with-32-bytes-minimum"
ROTATED_TOKEN = "test-only-rotated-credential-with-32-bytes-minimum"
ORIGIN = "https://dashboard.example.test"
PRINCIPAL = "operator"
CREDENTIAL_ID = "operator-current"
GRANTED_ACTIONS = frozenset(
    {
        AuthorizationAction.ADMIN_PANEL_VIEW,
        AuthorizationAction.OPERATIONS_VIEW,
        AuthorizationAction.HEALTH,
    }
)
EXPIRY = NOW + timedelta(days=1)
ReturnedValue = TypeVar("ReturnedValue")


def credential(*, token: str = TOKEN, credential_id: str = CREDENTIAL_ID) -> CredentialGrant:
    return CredentialGrant(
        credential_id=credential_id,
        principal_id=PRINCIPAL,
        token_sha256=SecretStr(hashlib.sha256(token.encode()).hexdigest()),
        expires_at=EXPIRY,
        scope=PRODUCTION_SCOPE,
        actions=GRANTED_ACTIONS,
    )


def settings() -> HostSettings:
    return HostSettings(public_origin=ORIGIN, credentials=(credential(),))


def test_environment_template_matches_host_settings_schema(monkeypatch) -> None:
    template = Path("examples/host_application/.env.example").read_text()
    declarations = {
        line.split("=", maxsplit=1)[0]: line.split("=", maxsplit=1)[1]
        for line in template.splitlines()
        if line and not line.startswith("#")
    }
    prefix = HostSettings.model_config["env_prefix"]
    assert declarations == {f"{prefix}{name.upper()}": "" for name in HostSettings.model_fields}
    monkeypatch.setenv(f"{prefix}PUBLIC_ORIGIN", ORIGIN)
    grant = credential().model_dump(mode="json")
    grant["token_sha256"] = hashlib.sha256(TOKEN.encode()).hexdigest()
    monkeypatch.setenv(f"{prefix}CREDENTIALS", json.dumps([grant]))
    loaded = HostSettings().for_scope(PRODUCTION_SCOPE)
    assert loaded.public_origin == ORIGIN
    assert loaded.credentials[0].principal_id == PRINCIPAL


def basic(*, identity: str = CREDENTIAL_ID, token: str = TOKEN) -> bytes:
    return b"Basic " + base64.b64encode(f"{identity}:{token}".encode())


@dataclass(frozen=True, kw_only=True)
class Returns(Generic[ReturnedValue]):
    value: ReturnedValue


@dataclass(frozen=True, kw_only=True)
class Raises:
    exc: type[Exception]
    match: str


@dataclass(frozen=True, kw_only=True)
class AuthenticationCase:
    id: str
    headers: tuple[tuple[bytes, bytes], ...]
    outcome: Returns[str] | Raises
    now: datetime = NOW


AUTHENTICATION_CASES = [
    AuthenticationCase(
        id="bearer",
        headers=((b"authorization", f"Bearer {TOKEN}".encode()),),
        outcome=Returns(value=PRINCIPAL),
    ),
    AuthenticationCase(
        id="basic", headers=((b"authorization", basic()),), outcome=Returns(value=PRINCIPAL)
    ),
    AuthenticationCase(
        id="missing", headers=(), outcome=Raises(exc=AuthenticationError, match="credential")
    ),
    AuthenticationCase(
        id="demo-header",
        headers=((b"x-host-subject", b"local-operator"),),
        outcome=Raises(exc=AuthenticationError, match="credential"),
    ),
    AuthenticationCase(
        id="duplicate",
        headers=((b"authorization", basic()), (b"authorization", basic())),
        outcome=Raises(exc=AuthenticationError, match="single"),
    ),
    AuthenticationCase(
        id="wrong-principal",
        headers=((b"authorization", basic(identity="another")),),
        outcome=Raises(exc=AuthenticationError, match="Invalid"),
    ),
    AuthenticationCase(
        id="malformed-base64",
        headers=((b"authorization", b"Basic !!"),),
        outcome=Raises(exc=AuthenticationError, match="Invalid"),
    ),
    AuthenticationCase(
        id="wrong-token",
        headers=((b"authorization", basic(token=ROTATED_TOKEN)),),
        outcome=Raises(exc=AuthenticationError, match="Invalid"),
    ),
    AuthenticationCase(
        id="expiry-boundary",
        headers=((b"authorization", basic()),),
        now=EXPIRY,
        outcome=Raises(exc=AuthenticationError, match="Expired"),
    ),
]


@pytest.mark.parametrize("case", AUTHENTICATION_CASES, ids=lambda c: c.id)
async def test_authentication(case: AuthenticationCase) -> None:
    authentication = HostAuthentication(settings(), now=lambda: case.now)
    request = AuthenticationRequest(method="GET", path="/healthz", headers=case.headers)
    if isinstance(case.outcome, Raises):
        with pytest.raises(case.outcome.exc, match=case.outcome.match):
            await authentication.authenticate(request)
    else:
        principal = await authentication.authenticate(request)
        assert principal.principal_id == case.outcome.value
        assert principal.scope_grants == frozenset({PRODUCTION_SCOPE})


@dataclass(frozen=True, kw_only=True)
class AuthorizationCase:
    id: str
    scope: RuntimeScope
    action: AuthorizationAction
    outcome: Returns[bool]


AUTHORIZATION_CASES = [
    AuthorizationCase(
        id="granted",
        scope=PRODUCTION_SCOPE,
        action=AuthorizationAction.HEALTH,
        outcome=Returns(value=True),
    ),
    AuthorizationCase(
        id="action-denied",
        scope=PRODUCTION_SCOPE,
        action=AuthorizationAction.TERMINATE,
        outcome=Returns(value=False),
    ),
    AuthorizationCase(
        id="scope-denied",
        scope=RuntimeScope.create(tenant="foreign", application="host", environment="production"),
        action=AuthorizationAction.HEALTH,
        outcome=Returns(value=False),
    ),
]


@pytest.mark.parametrize("case", AUTHORIZATION_CASES, ids=lambda c: c.id)
async def test_authorization(case: AuthorizationCase) -> None:
    authentication = HostAuthentication(settings(), now=lambda: NOW)
    principal = await authentication.authenticate(
        AuthenticationRequest(method="GET", path="/", headers=((b"authorization", basic()),))
    )
    assert (
        await authentication.authorize(
            principal, AuthorizationRequest(scope=case.scope, action=case.action)
        )
        is case.outcome.value
    )


async def test_rotation_preserves_actor_and_revocation_rejects_old_credentials() -> None:
    rotated = credential(token=ROTATED_TOKEN, credential_id="operator-next")
    overlap = HostAuthentication(
        HostSettings(public_origin=ORIGIN, credentials=(credential(), rotated)), now=lambda: NOW
    )
    revoked = HostAuthentication(
        HostSettings(public_origin=ORIGIN, credentials=(rotated,)), now=lambda: NOW
    )
    old_request = AuthenticationRequest(
        method="GET", path="/", headers=((b"authorization", basic()),)
    )
    new_request = AuthenticationRequest(
        method="GET",
        path="/",
        headers=((b"authorization", basic(identity="operator-next", token=ROTATED_TOKEN)),),
    )
    assert (await overlap.authenticate(old_request)).actor_digest == (
        await overlap.authenticate(new_request)
    ).actor_digest
    with pytest.raises(AuthenticationError):
        await revoked.authenticate(old_request)
    assert (await revoked.authenticate(new_request)).principal_id == PRINCIPAL


def boundary() -> BrowserAuthenticationBoundary:
    host_settings = settings()
    authentication = HostAuthentication(host_settings, now=lambda: NOW)
    api = ControlApi(
        settings=ControlSettings(),
        runtime_profile=RuntimeProfile.PRODUCTION,
        starter=MagicMock(spec=WorkflowStarter),
        controls=MagicMock(spec=WorkflowControlService),
        health=HealthRegistry(frozenset()),
        metrics=MetricsRegistry(),
        authentication=authentication,
        admin_panel=BetaAdminPanel(),
        runtime_scope=PRODUCTION_SCOPE,
    )
    return BrowserAuthenticationBoundary(api, authentication, host_settings)


@dataclass(frozen=True, kw_only=True)
class BrowserCase:
    id: str
    method: str
    path: str
    headers: tuple[tuple[bytes, bytes], ...]
    expected_status: HTTPStatus
    origin: str = ORIGIN


BROWSER_CASES = [
    BrowserCase(
        id="login-challenge",
        method="GET",
        path="/",
        headers=(),
        expected_status=HTTPStatus.UNAUTHORIZED,
    ),
    BrowserCase(
        id="root-login",
        method="GET",
        path="/",
        headers=((b"authorization", basic()),),
        expected_status=HTTPStatus.SEE_OTHER,
    ),
    BrowserCase(
        id="browser-panel",
        method="GET",
        path="/admin",
        headers=((b"authorization", basic()),),
        expected_status=HTTPStatus.OK,
    ),
    BrowserCase(
        id="browser-api",
        method="GET",
        path="/v1/operations/capabilities",
        headers=((b"authorization", basic()),),
        expected_status=HTTPStatus.OK,
    ),
    BrowserCase(
        id="standalone-api",
        method="GET",
        path="/v1/operations/capabilities",
        headers=((b"authorization", f"Bearer {TOKEN}".encode()),),
        expected_status=HTTPStatus.OK,
    ),
    BrowserCase(
        id="denied-action",
        method="GET",
        path="/metrics",
        headers=((b"authorization", basic()),),
        expected_status=HTTPStatus.FORBIDDEN,
    ),
    BrowserCase(
        id="missing-write-origin",
        method="POST",
        path="/v1/workflows",
        headers=((b"authorization", basic()),),
        expected_status=HTTPStatus.FORBIDDEN,
    ),
    BrowserCase(
        id="foreign-origin",
        method="GET",
        path="/healthz",
        headers=((b"authorization", basic()), (b"origin", b"https://hostile.example")),
        expected_status=HTTPStatus.FORBIDDEN,
    ),
    BrowserCase(
        id="origin-suffix",
        method="GET",
        path="/healthz",
        headers=((b"authorization", basic()), (b"origin", f"{ORIGIN}.hostile.example".encode())),
        expected_status=HTTPStatus.FORBIDDEN,
    ),
    BrowserCase(
        id="insecure-transport",
        method="GET",
        path="/healthz",
        headers=((b"authorization", basic()),),
        origin="http://dashboard.example.test",
        expected_status=HTTPStatus.BAD_REQUEST,
    ),
    BrowserCase(
        id="private-http-probe",
        method="GET",
        path="/livez",
        headers=(),
        origin="http://localhost",
        expected_status=HTTPStatus.OK,
    ),
]


@pytest.mark.parametrize("case", BROWSER_CASES, ids=lambda c: c.id)
async def test_browser_boundary(case: BrowserCase) -> None:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=boundary()), base_url=case.origin
    ) as client:
        response = await client.request(case.method, case.path, headers=case.headers)
    assert response.status_code == case.expected_status
    if response.status_code == HTTPStatus.UNAUTHORIZED:
        assert response.headers["www-authenticate"].encode() == BROWSER_CHALLENGE
    if response.status_code == HTTPStatus.SEE_OTHER:
        assert response.headers["location"] == "/admin"
    assert TOKEN not in response.text


@pytest.mark.parametrize(
    "origin",
    [
        "http://example.test",
        "https://user:pass@example.test",
        "https://example.test/",
        "https://example.test?q=1",
    ],
)
def test_host_settings_reject_invalid_browser_origins(origin: str) -> None:
    with pytest.raises(ValidationError, match="HTTPS origin"):
        HostSettings(public_origin=origin, credentials=(credential(),))
