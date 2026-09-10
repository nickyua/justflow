"""Tests for runtime-only transport security settings."""

from __future__ import annotations

from dataclasses import dataclass

import pytest
from pydantic import ValidationError

from justflow.transports.security import (
    GrpcTlsProfile,
    HttpEndpointPolicy,
    canonical_https_origin,
)


@dataclass(frozen=True, kw_only=True)
class OriginReturns:
    value: str


@dataclass(frozen=True, kw_only=True)
class OriginRaises:
    exc: type[Exception]
    match: str


OriginOutcome = OriginReturns | OriginRaises


@dataclass(frozen=True, kw_only=True)
class OriginCase:
    id: str
    value: str
    outcome: OriginOutcome


ORIGIN_CASES = [
    OriginCase(
        id="canonical-host",
        value="https://API.Example",
        outcome=OriginReturns(value="https://api.example"),
    ),
    OriginCase(
        id="default-port",
        value="https://api.example:443/",
        outcome=OriginReturns(value="https://api.example"),
    ),
    OriginCase(
        id="insecure-scheme",
        value="http://api.example",
        outcome=OriginRaises(exc=ValueError, match="HTTPS scheme"),
    ),
    OriginCase(
        id="path-is-not-origin",
        value="https://api.example/path",
        outcome=OriginRaises(exc=ValueError, match="only an HTTPS scheme"),
    ),
    OriginCase(
        id="embedded-credentials",
        value="https://user:secret@api.example",
        outcome=OriginRaises(exc=ValueError, match="only an HTTPS scheme"),
    ),
]


@pytest.mark.parametrize("case", ORIGIN_CASES, ids=lambda case: case.id)
def test_http_origin_normalization(case: OriginCase) -> None:
    if isinstance(case.outcome, OriginRaises):
        with pytest.raises(case.outcome.exc, match=case.outcome.match):
            canonical_https_origin(case.value)
        return

    assert canonical_https_origin(case.value) == case.outcome.value


def test_http_endpoint_policy_is_deny_by_default() -> None:
    with pytest.raises(ValueError, match="not approved"):
        HttpEndpointPolicy().require_approved("https://api.example/path")


def test_grpc_client_certificate_and_private_key_are_atomic() -> None:
    with pytest.raises(ValidationError, match="configured together"):
        GrpcTlsProfile(
            server_name="service.internal",
            client_certificate_path="/runtime/client.crt",
        )


def test_grpc_server_name_is_validated() -> None:
    with pytest.raises(ValidationError, match="valid DNS name or IP"):
        GrpcTlsProfile(server_name="not a server name")
