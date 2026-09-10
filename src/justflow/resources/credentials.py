"""Typed references and parsing for resource-owned connection credentials."""

from __future__ import annotations

from dataclasses import dataclass

from pydantic import SecretStr

from justflow.config.grammar import Identifier
from justflow.engine.serialization import StrictJsonError, loads_strict_json
from justflow.resources.base import (
    ResourceCapability,
    ResourceDependency,
    ResourceOperationError,
    StrictResourceConfig,
)

MAX_CREDENTIAL_USERNAME_BYTES = 1024
MAX_CREDENTIAL_PASSWORD_BYTES = 64 * 1024


class SecretResourceCredentials(StrictResourceConfig):
    secret_resource: Identifier
    secret_alias: Identifier


@dataclass(frozen=True, kw_only=True)
class UsernamePasswordCredentials:
    username: SecretStr | None
    password: SecretStr


def secret_resource_dependency(
    credentials: SecretResourceCredentials,
) -> ResourceDependency:
    return ResourceDependency(
        resource_name=credentials.secret_resource,
        capability=ResourceCapability.SECRET_READER,
        secret_alias=credentials.secret_alias,
    )


def parse_username_password_secret(
    secret: SecretStr,
    *,
    require_username: bool,
) -> UsernamePasswordCredentials:
    try:
        value = loads_strict_json(secret.get_secret_value())
    except StrictJsonError as exc:
        raise ResourceOperationError(
            "Connection credential secret must be strict JSON",
            retryable=False,
        ) from exc
    if not isinstance(value, dict):
        raise ResourceOperationError(
            "Connection credential secret must be a JSON object",
            retryable=False,
        )
    username = value.get("username")
    password = value.get("password")
    if require_username and (not isinstance(username, str) or not username):
        raise ResourceOperationError(
            "Connection credential secret requires a non-empty username",
            retryable=False,
        )
    if username is not None and (not isinstance(username, str) or not username):
        raise ResourceOperationError(
            "Connection credential secret username must be non-empty text",
            retryable=False,
        )
    if not isinstance(password, str) or not password:
        raise ResourceOperationError(
            "Connection credential secret requires a non-empty password",
            retryable=False,
        )
    if username is not None and len(username.encode("utf-8")) > MAX_CREDENTIAL_USERNAME_BYTES:
        raise ResourceOperationError(
            "Connection credential username exceeds the supported byte limit",
            retryable=False,
        )
    if len(password.encode("utf-8")) > MAX_CREDENTIAL_PASSWORD_BYTES:
        raise ResourceOperationError(
            "Connection credential password exceeds the supported byte limit",
            retryable=False,
        )
    return UsernamePasswordCredentials(
        username=SecretStr(username) if username is not None else None,
        password=SecretStr(password),
    )
