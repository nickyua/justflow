"""Resolved secure connection policy for every Temporal client role."""

from __future__ import annotations

import ipaddress
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit

from pydantic import SecretStr
from temporalio.client import Client, TLSConfig
from temporalio.converter import DataConverter

from justflow.config.settings import (
    LocalTemporalConnectionSettings,
    TemporalSettings,
    TlsTemporalConnectionSettings,
)
from justflow.provenance import RuntimeProfile
from justflow.transports.security import validate_server_identity

MAX_TEMPORAL_TLS_MATERIAL_BYTES = 1_048_576
CERTIFICATE_PEM_MARKER = b"-----BEGIN CERTIFICATE-----"
PRIVATE_KEY_PEM_MARKERS = (
    b"-----BEGIN PRIVATE KEY-----",
    b"-----BEGIN RSA PRIVATE KEY-----",
    b"-----BEGIN EC PRIVATE KEY-----",
)


class TemporalConnectionConfigurationError(ValueError):
    pass


class TemporalConnectionError(RuntimeError):
    pass


@dataclass(frozen=True, kw_only=True)
class TemporalConnectionBinding:
    root_ca: bytes | None = field(default=None, repr=False)
    client_certificate: bytes | None = field(default=None, repr=False)
    client_private_key: bytes | None = field(default=None, repr=False)
    api_key: SecretStr | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if (self.client_certificate is None) != (self.client_private_key is None):
            raise TemporalConnectionConfigurationError(
                "Temporal client certificate and private key bindings must be supplied together"
            )


@dataclass(frozen=True, kw_only=True)
class TemporalConnectionPolicy:
    address: str
    namespace: str
    mode: str
    _tls: TLSConfig | None = field(repr=False)
    _api_key: str | None = field(repr=False)

    async def connect(self, data_converter: DataConverter | None = None) -> Client:
        try:
            if data_converter is None:
                return await Client.connect(
                    self.address,
                    namespace=self.namespace,
                    tls=self._tls,
                    api_key=self._api_key,
                )
            return await Client.connect(
                self.address,
                namespace=self.namespace,
                tls=self._tls,
                api_key=self._api_key,
                data_converter=data_converter,
            )
        except Exception as exc:
            raise TemporalConnectionError(
                "Cannot connect to the configured Temporal service"
            ) from exc


def resolve_temporal_connection(
    settings: TemporalSettings,
    runtime_profile: RuntimeProfile,
    binding: TemporalConnectionBinding | None = None,
) -> TemporalConnectionPolicy:
    _target_host(settings.address)
    connection = settings.connection
    if isinstance(connection, LocalTemporalConnectionSettings):
        if binding is not None:
            raise TemporalConnectionConfigurationError(
                "Temporal connection bindings require TLS mode"
            )
        if runtime_profile is not RuntimeProfile.LOCAL:
            raise TemporalConnectionConfigurationError(
                "Temporal local plaintext requires the explicit local runtime profile"
            )
        _validate_plaintext_target(settings.address, connection.host)
        return TemporalConnectionPolicy(
            address=settings.address,
            namespace=settings.namespace,
            mode=connection.mode,
            _tls=None,
            _api_key=None,
        )

    validate_server_identity(connection.server_name, label="Temporal TLS server_name")
    resolved_binding = binding or TemporalConnectionBinding()
    root_ca = _resolve_material(
        path=connection.root_ca_path,
        bound=resolved_binding.root_ca,
        label="Temporal root CA certificate",
        required_marker=CERTIFICATE_PEM_MARKER,
    )
    client_certificate, client_private_key = _resolve_client_identity(
        connection,
        resolved_binding,
    )
    api_key = _resolve_api_key(connection, resolved_binding)
    return TemporalConnectionPolicy(
        address=settings.address,
        namespace=settings.namespace,
        mode=connection.mode,
        _tls=TLSConfig(
            server_root_ca_cert=root_ca,
            domain=connection.server_name,
            client_cert=client_certificate,
            client_private_key=client_private_key,
        ),
        _api_key=api_key,
    )


def _validate_plaintext_target(address: str, explicit_host: str | None) -> None:
    host = _target_host(address)
    if explicit_host is not None:
        if host != explicit_host:
            raise TemporalConnectionConfigurationError(
                "Temporal plaintext address does not match its explicit local host binding"
            )
        return
    try:
        is_loopback = ipaddress.ip_address(host).is_loopback
    except ValueError:
        is_loopback = host.lower() == "localhost"
    if not is_loopback:
        raise TemporalConnectionConfigurationError(
            "Temporal plaintext requires loopback or an explicit local host binding"
        )


def _target_host(address: str) -> str:
    parsed = urlsplit(f"//{address}")
    try:
        port = parsed.port
    except ValueError as exc:
        raise TemporalConnectionConfigurationError("Temporal address has an invalid port") from exc
    if (
        parsed.hostname is None
        or port is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path
        or parsed.query
        or parsed.fragment
    ):
        raise TemporalConnectionConfigurationError(
            "Temporal address must contain only a host and port"
        )
    return parsed.hostname


def _resolve_client_identity(
    settings: TlsTemporalConnectionSettings,
    binding: TemporalConnectionBinding,
) -> tuple[bytes | None, bytes | None]:
    paths_configured = settings.client_certificate_path is not None
    binding_configured = binding.client_certificate is not None
    if paths_configured and binding_configured:
        raise TemporalConnectionConfigurationError(
            "Temporal client identity must come from paths or a host binding, not both"
        )
    certificate = _resolve_material(
        path=settings.client_certificate_path,
        bound=binding.client_certificate,
        label="Temporal client certificate",
        required_marker=CERTIFICATE_PEM_MARKER,
    )
    private_key = _resolve_material(
        path=settings.client_private_key_path,
        bound=binding.client_private_key,
        label="Temporal client private key",
        required_marker=None,
    )
    if private_key is not None and not any(
        marker in private_key for marker in PRIVATE_KEY_PEM_MARKERS
    ):
        raise TemporalConnectionConfigurationError("Temporal client private key is not PEM encoded")
    return certificate, private_key


def _resolve_api_key(
    settings: TlsTemporalConnectionSettings,
    binding: TemporalConnectionBinding,
) -> str | None:
    if settings.api_key is not None and binding.api_key is not None:
        raise TemporalConnectionConfigurationError(
            "Temporal API key must come from settings or a host binding, not both"
        )
    secret = settings.api_key or binding.api_key
    return secret.get_secret_value() if secret is not None else None


def _resolve_material(
    *,
    path: str | None,
    bound: bytes | None,
    label: str,
    required_marker: bytes | None,
) -> bytes | None:
    if path is not None and bound is not None:
        raise TemporalConnectionConfigurationError(
            f"{label} must come from a path or a host binding, not both"
        )
    material = _read_bounded_path(path, label=label) if path is not None else bound
    if material is None:
        return None
    if not material or len(material) > MAX_TEMPORAL_TLS_MATERIAL_BYTES:
        raise TemporalConnectionConfigurationError(f"{label} exceeds its byte bound or is empty")
    if required_marker is not None and required_marker not in material:
        raise TemporalConnectionConfigurationError(f"{label} is not PEM encoded")
    return material


def _read_bounded_path(path: str, *, label: str) -> bytes:
    source = Path(path)
    try:
        size = source.stat().st_size
        if not source.is_file():
            raise TemporalConnectionConfigurationError(f"{label} path must identify a file")
        if size < 1 or size > MAX_TEMPORAL_TLS_MATERIAL_BYTES:
            raise TemporalConnectionConfigurationError(
                f"{label} file exceeds its byte bound or is empty"
            )
        return source.read_bytes()
    except TemporalConnectionConfigurationError:
        raise
    except OSError as exc:
        raise TemporalConnectionConfigurationError(
            f"Cannot read {label} from its configured path"
        ) from exc
