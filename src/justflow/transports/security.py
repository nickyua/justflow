"""Runtime-only transport security policy."""

from __future__ import annotations

import ipaddress
import re
from pathlib import Path
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, model_validator

MAX_ORIGIN_LENGTH = 2_048
MAX_PATH_LENGTH = 4_096
MAX_PROFILE_NAME_LENGTH = 128
MAX_SERVER_NAME_LENGTH = 253
MAX_DNS_LABEL_LENGTH = 63
DEFAULT_HTTPS_PORT = 443
HTTPS_SCHEME = "https"
DNS_LABEL_PATTERN = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?$")
PROFILE_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")


class StrictSecurityModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def canonical_https_origin(value: str) -> str:
    if len(value) > MAX_ORIGIN_LENGTH:
        raise ValueError("HTTP origin is too long")
    parsed = urlsplit(value)
    if (
        parsed.scheme != HTTPS_SCHEME
        or parsed.hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("HTTP origins must contain only an HTTPS scheme, host, and optional port")
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("HTTP origin has an invalid port") from exc
    host = parsed.hostname.lower()
    if ":" in host:
        host = f"[{host}]"
    if port in {None, DEFAULT_HTTPS_PORT}:
        return f"{HTTPS_SCHEME}://{host}"
    return f"{HTTPS_SCHEME}://{host}:{port}"


class HttpEndpointPolicy(StrictSecurityModel):
    allowed_origins: frozenset[str] = Field(default_factory=frozenset)

    @model_validator(mode="after")
    def validate_origins(self) -> HttpEndpointPolicy:
        canonical = frozenset(canonical_https_origin(origin) for origin in self.allowed_origins)
        if canonical != self.allowed_origins:
            return self.model_copy(update={"allowed_origins": canonical})
        return self

    def require_approved(self, url: str) -> str:
        parsed = urlsplit(url)
        if (
            len(url) > MAX_ORIGIN_LENGTH
            or parsed.hostname is None
            or parsed.username is not None
            or parsed.password is not None
        ):
            raise ValueError("HTTP endpoint must use an approved origin without credentials")
        try:
            port = parsed.port
        except ValueError as exc:
            raise ValueError("HTTP endpoint has an invalid port") from exc
        host = parsed.hostname
        if ":" in host:
            host = f"[{host}]"
        origin = canonical_https_origin(
            f"{parsed.scheme}://{host}" + (f":{port}" if port is not None else "")
        )
        if origin not in self.allowed_origins:
            raise ValueError(f"HTTP origin '{origin}' is not approved")
        return origin


class GrpcTlsProfile(StrictSecurityModel):
    server_name: str = Field(min_length=1, max_length=MAX_SERVER_NAME_LENGTH)
    root_ca_path: str | None = Field(None, min_length=1, max_length=MAX_PATH_LENGTH)
    client_certificate_path: str | None = Field(None, min_length=1, max_length=MAX_PATH_LENGTH)
    client_private_key_path: str | None = Field(None, min_length=1, max_length=MAX_PATH_LENGTH)

    @model_validator(mode="after")
    def validate_client_identity(self) -> GrpcTlsProfile:
        validate_server_identity(self.server_name, label="gRPC TLS server_name")
        if (self.client_certificate_path is None) != (self.client_private_key_path is None):
            raise ValueError(
                "gRPC client certificate and private key paths must be configured together"
            )
        return self


def validate_server_identity(value: str, *, label: str) -> None:
    try:
        ipaddress.ip_address(value)
    except ValueError:
        dns_name = value.removesuffix(".")
        labels = dns_name.split(".")
        if any(
            not label
            or len(label) > MAX_DNS_LABEL_LENGTH
            or DNS_LABEL_PATTERN.fullmatch(label) is None
            for label in labels
        ):
            raise ValueError(f"{label} must be a valid DNS name or IP address")


class TransportSecuritySettings(StrictSecurityModel):
    http: HttpEndpointPolicy = HttpEndpointPolicy()
    grpc_tls_profiles: dict[str, GrpcTlsProfile] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_profile_names(self) -> TransportSecuritySettings:
        for name in self.grpc_tls_profiles:
            if len(name) > MAX_PROFILE_NAME_LENGTH or PROFILE_NAME_PATTERN.fullmatch(name) is None:
                raise ValueError(
                    "gRPC TLS profile names must use letters, digits, '.', '_', or '-'"
                )
        return self


def read_optional_bytes(path: str | None, *, label: str) -> bytes | None:
    if path is None:
        return None
    try:
        return Path(path).read_bytes()
    except OSError as exc:
        raise ValueError(f"Cannot read {label} from configured path") from exc
