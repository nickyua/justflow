"""Tests for resolved Temporal connection security policy."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TypeAlias
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import SecretStr, ValidationError
from temporalio.client import TLSConfig

from justflow.config.settings import (
    LocalTemporalConnectionSettings,
    Settings,
    TemporalSettings,
    TlsTemporalConnectionSettings,
)
from justflow.provenance import RuntimeProfile
from justflow.runtime import (
    TemporalConnectionBinding,
    TemporalConnectionConfigurationError,
    TemporalConnectionError,
    resolve_temporal_connection,
)
from justflow.runtime.temporal import (
    CERTIFICATE_PEM_MARKER,
    MAX_TEMPORAL_TLS_MATERIAL_BYTES,
    PRIVATE_KEY_PEM_MARKERS,
)

LOCAL_ADDRESS = "localhost:7233"
REMOTE_ADDRESS = "temporal.example:7233"
COMPOSE_ADDRESS = "temporal:7233"
SERVER_NAME = "temporal.example"
SYNTHETIC_API_KEY = "synthetic-api-key-value"
CERTIFICATE_BYTES = CERTIFICATE_PEM_MARKER + b"\nsynthetic\n-----END CERTIFICATE-----\n"
PRIVATE_KEY_BYTES = PRIVATE_KEY_PEM_MARKERS[0] + b"\nsynthetic\n-----END PRIVATE KEY-----\n"


@dataclass(frozen=True, kw_only=True)
class Returns:
    mode: str
    tls: bool
    api_key: str | None = None
    client_identity: bool = False


@dataclass(frozen=True, kw_only=True)
class Raises:
    exc: type[Exception]
    match: str


Outcome: TypeAlias = Returns | Raises


@dataclass(frozen=True, kw_only=True)
class ConnectionCase:
    id: str
    temporal: TemporalSettings
    profile: RuntimeProfile
    outcome: Outcome
    binding: TemporalConnectionBinding | None = None


CONNECTION_CASES = [
    ConnectionCase(
        id="local-loopback-plaintext",
        temporal=TemporalSettings(
            address=LOCAL_ADDRESS,
            connection=LocalTemporalConnectionSettings(),
        ),
        profile=RuntimeProfile.LOCAL,
        outcome=Returns(mode="local_plaintext", tls=False),
    ),
    ConnectionCase(
        id="local-compose-explicit-host",
        temporal=TemporalSettings(
            address=COMPOSE_ADDRESS,
            connection=LocalTemporalConnectionSettings(host="temporal"),
        ),
        profile=RuntimeProfile.LOCAL,
        outcome=Returns(mode="local_plaintext", tls=False),
    ),
    ConnectionCase(
        id="remote-plaintext-rejected",
        temporal=TemporalSettings(
            address=REMOTE_ADDRESS,
            connection=LocalTemporalConnectionSettings(),
        ),
        profile=RuntimeProfile.LOCAL,
        outcome=Raises(
            exc=TemporalConnectionConfigurationError,
            match="loopback or an explicit",
        ),
    ),
    ConnectionCase(
        id="production-plaintext-rejected",
        temporal=TemporalSettings(
            address=LOCAL_ADDRESS,
            connection=LocalTemporalConnectionSettings(),
        ),
        profile=RuntimeProfile.PRODUCTION,
        outcome=Raises(
            exc=TemporalConnectionConfigurationError,
            match="explicit local runtime profile",
        ),
    ),
    ConnectionCase(
        id="server-authenticated-tls",
        temporal=TemporalSettings(
            address=REMOTE_ADDRESS,
            connection=TlsTemporalConnectionSettings(server_name=SERVER_NAME),
        ),
        profile=RuntimeProfile.PRODUCTION,
        outcome=Returns(mode="tls", tls=True),
    ),
    ConnectionCase(
        id="mutual-tls-host-binding",
        temporal=TemporalSettings(
            address=REMOTE_ADDRESS,
            connection=TlsTemporalConnectionSettings(server_name=SERVER_NAME),
        ),
        profile=RuntimeProfile.PRODUCTION,
        binding=TemporalConnectionBinding(
            client_certificate=CERTIFICATE_BYTES,
            client_private_key=PRIVATE_KEY_BYTES,
        ),
        outcome=Returns(mode="tls", tls=True, client_identity=True),
    ),
    ConnectionCase(
        id="api-key-over-tls",
        temporal=TemporalSettings(
            address=REMOTE_ADDRESS,
            connection=TlsTemporalConnectionSettings(
                server_name=SERVER_NAME,
                api_key=SecretStr(SYNTHETIC_API_KEY),
            ),
        ),
        profile=RuntimeProfile.PRODUCTION,
        outcome=Returns(mode="tls", tls=True, api_key=SYNTHETIC_API_KEY),
    ),
    ConnectionCase(
        id="binding-rejected-with-plaintext",
        temporal=TemporalSettings(
            address=LOCAL_ADDRESS,
            connection=LocalTemporalConnectionSettings(),
        ),
        profile=RuntimeProfile.LOCAL,
        binding=TemporalConnectionBinding(root_ca=CERTIFICATE_BYTES),
        outcome=Raises(
            exc=TemporalConnectionConfigurationError,
            match="bindings require TLS",
        ),
    ),
]


@pytest.mark.parametrize("case", CONNECTION_CASES, ids=lambda case: case.id)
async def test_connection_policy_matrix(case: ConnectionCase):
    if isinstance(case.outcome, Raises):
        with pytest.raises(case.outcome.exc, match=case.outcome.match):
            resolve_temporal_connection(case.temporal, case.profile, case.binding)
        return

    policy = resolve_temporal_connection(case.temporal, case.profile, case.binding)
    connected_client = MagicMock()
    with patch(
        "justflow.runtime.temporal.Client.connect",
        new=AsyncMock(return_value=connected_client),
    ) as connect:
        result = await policy.connect()

    assert result is connected_client
    assert policy.mode == case.outcome.mode
    kwargs = connect.await_args.kwargs
    assert (kwargs["tls"] is not None) is case.outcome.tls
    assert kwargs["api_key"] == case.outcome.api_key
    if isinstance(kwargs["tls"], TLSConfig):
        assert kwargs["tls"].domain == SERVER_NAME
        assert (kwargs["tls"].client_cert is not None) is case.outcome.client_identity
        assert (kwargs["tls"].client_private_key is not None) is case.outcome.client_identity


def test_local_plaintext_requires_local_runtime_at_settings_boundary():
    with pytest.raises(ValidationError, match="explicit local runtime profile"):
        Settings.model_validate(
            {
                "runtime": {"profile": "production"},
                "temporal": {"connection": {"mode": "local_plaintext"}},
            }
        )


def test_client_certificate_and_key_paths_are_a_pair():
    with pytest.raises(ValidationError, match="must be configured together"):
        TlsTemporalConnectionSettings(
            server_name=SERVER_NAME,
            client_certificate_path="/runtime/client.pem",
        )


def test_tls_material_loads_from_bounded_paths(tmp_path: Path):
    root_ca_path = tmp_path / "ca.pem"
    client_certificate_path = tmp_path / "client.pem"
    client_private_key_path = tmp_path / "client.key"
    root_ca_path.write_bytes(CERTIFICATE_BYTES)
    client_certificate_path.write_bytes(CERTIFICATE_BYTES)
    client_private_key_path.write_bytes(PRIVATE_KEY_BYTES)
    settings = TemporalSettings(
        address=REMOTE_ADDRESS,
        connection=TlsTemporalConnectionSettings(
            server_name=SERVER_NAME,
            root_ca_path=str(root_ca_path),
            client_certificate_path=str(client_certificate_path),
            client_private_key_path=str(client_private_key_path),
        ),
    )

    policy = resolve_temporal_connection(settings, RuntimeProfile.PRODUCTION)

    assert "client.key" not in repr(policy)


@pytest.mark.parametrize(
    ("filename", "payload", "match"),
    [
        pytest.param("empty.pem", b"", "empty", id="empty"),
        pytest.param("invalid.pem", b"not-pem", "not PEM", id="invalid-pem"),
        pytest.param(
            "oversized.pem",
            CERTIFICATE_BYTES + b"x" * MAX_TEMPORAL_TLS_MATERIAL_BYTES,
            "byte bound",
            id="oversized",
        ),
    ],
)
def test_invalid_tls_material_is_rejected_before_connect(
    tmp_path: Path,
    filename: str,
    payload: bytes,
    match: str,
):
    path = tmp_path / filename
    path.write_bytes(payload)
    settings = TemporalSettings(
        address=REMOTE_ADDRESS,
        connection=TlsTemporalConnectionSettings(
            server_name=SERVER_NAME,
            root_ca_path=str(path),
        ),
    )

    with pytest.raises(TemporalConnectionConfigurationError, match=match):
        resolve_temporal_connection(settings, RuntimeProfile.PRODUCTION)


def test_unreadable_tls_path_is_rejected_before_connect(tmp_path: Path):
    settings = TemporalSettings(
        address=REMOTE_ADDRESS,
        connection=TlsTemporalConnectionSettings(
            server_name=SERVER_NAME,
            root_ca_path=str(tmp_path / "missing.pem"),
        ),
    )

    with pytest.raises(TemporalConnectionConfigurationError, match="Cannot read"):
        resolve_temporal_connection(settings, RuntimeProfile.PRODUCTION)


def test_secret_values_are_redacted_from_settings_and_policy_representations():
    settings = TemporalSettings(
        address=REMOTE_ADDRESS,
        connection=TlsTemporalConnectionSettings(
            server_name=SERVER_NAME,
            api_key=SecretStr(SYNTHETIC_API_KEY),
        ),
    )
    policy = resolve_temporal_connection(settings, RuntimeProfile.PRODUCTION)

    assert SYNTHETIC_API_KEY not in repr(settings)
    assert SYNTHETIC_API_KEY not in settings.model_dump_json()
    assert SYNTHETIC_API_KEY not in repr(policy)


async def test_connection_errors_do_not_leak_library_exceptions():
    policy = resolve_temporal_connection(
        TemporalSettings(
            address=REMOTE_ADDRESS,
            connection=TlsTemporalConnectionSettings(server_name=SERVER_NAME),
        ),
        RuntimeProfile.PRODUCTION,
    )
    with (
        patch(
            "justflow.runtime.temporal.Client.connect",
            new=AsyncMock(side_effect=RuntimeError(SYNTHETIC_API_KEY)),
        ),
        pytest.raises(TemporalConnectionError, match="configured Temporal service") as raised,
    ):
        await policy.connect()

    assert SYNTHETIC_API_KEY not in str(raised.value)
