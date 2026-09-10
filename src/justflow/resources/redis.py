"""TLS-capable Redis cache and key-value resource."""

from __future__ import annotations

from typing import Any, Self
from urllib.parse import urlsplit

from pydantic import Field, field_validator, model_validator

from justflow.config.grammar import Identifier
from justflow.engine.serialization import (
    StrictJsonError,
    StrictJsonLayout,
    dumps_strict_json,
    loads_strict_json,
)
from justflow.optional_dependencies import load_optional_dependency
from justflow.resources.base import (
    ResourceCapability,
    ResourceDependency,
    ResourceFactoryContext,
    ResourceOperationError,
    StrictResourceConfig,
)
from justflow.resources.credentials import (
    SecretResourceCredentials,
    parse_username_password_secret,
    secret_resource_dependency,
)
from justflow.resources.registry import ResourceProvider

MAX_REDIS_HOST_LENGTH = 253
DEFAULT_REDIS_PORT = 6379
MAX_NETWORK_PORT = 65_535
MAX_REDIS_DATABASE = 65_535
MAX_REDIS_CONNECTIONS = 1000
MAX_REDIS_TIMEOUT_SECONDS = 300.0
MAX_REDIS_KEY_BYTES = 4096
DEFAULT_REDIS_VALUE_BYTES = 1024 * 1024
MAX_REDIS_VALUE_BYTES = 100 * 1024 * 1024
MAX_REDIS_TTL_SECONDS = 31_536_000


class RedisEndpoint(StrictResourceConfig):
    host: str = Field(min_length=1, max_length=MAX_REDIS_HOST_LENGTH)
    port: int = Field(default=DEFAULT_REDIS_PORT, ge=1, le=MAX_NETWORK_PORT)
    database: int = Field(default=0, ge=0, le=MAX_REDIS_DATABASE)
    tls: bool = True

    @field_validator("host")
    @classmethod
    def validate_host(cls, value: str) -> str:
        if any(character.isspace() for character in value) or any(
            character in value for character in "/@?#\x00"
        ):
            raise ValueError("Redis host must be a hostname or IP address")
        return value


class RedisSecretConnection(StrictResourceConfig):
    endpoint: RedisEndpoint
    credentials: SecretResourceCredentials


class RedisRuntimeUrlConnection(StrictResourceConfig):
    runtime_url: Identifier
    require_tls: bool = True


class RedisConfig(StrictResourceConfig):
    connection: RedisSecretConnection | RedisRuntimeUrlConnection
    key_prefix: str = Field(default="", max_length=MAX_REDIS_KEY_BYTES)
    max_connections: int = Field(default=20, ge=1, le=MAX_REDIS_CONNECTIONS)
    connect_timeout_seconds: float = Field(default=5.0, gt=0, le=MAX_REDIS_TIMEOUT_SECONDS)
    operation_timeout_seconds: float = Field(default=10.0, gt=0, le=MAX_REDIS_TIMEOUT_SECONDS)
    default_ttl_seconds: int | None = Field(default=None, ge=1, le=MAX_REDIS_TTL_SECONDS)
    max_value_bytes: int = Field(
        default=DEFAULT_REDIS_VALUE_BYTES,
        ge=1,
        le=MAX_REDIS_VALUE_BYTES,
    )

    @model_validator(mode="after")
    def validate_prefix(self) -> Self:
        if "\x00" in self.key_prefix:
            raise ValueError("Redis key_prefix must not contain null bytes")
        return self


class RedisResource:
    def __init__(
        self,
        config: RedisConfig,
        context: ResourceFactoryContext,
        client: Any | None = None,
    ) -> None:
        self._config = config
        self._context = context
        self._client = client

    async def initialize(self) -> None:
        if self._client is None:
            connection = self._config.connection
            runtime_url: str | None = None
            connection_options: dict[str, object] = {}
            if isinstance(connection, RedisRuntimeUrlConnection):
                runtime_url = self._context.redis_url(connection.runtime_url).get_secret_value()
                parsed = urlsplit(runtime_url)
                allowed_schemes = {"rediss"} if connection.require_tls else {"redis", "rediss"}
                if parsed.scheme not in allowed_schemes or not parsed.hostname:
                    raise ResourceOperationError(
                        "Redis connection binding has an invalid or disallowed URL",
                        retryable=False,
                    )
            else:
                secret = await self._context.secret_reader(
                    connection.credentials.secret_resource
                ).read_secret(connection.credentials.secret_alias)
                credentials = parse_username_password_secret(secret, require_username=False)
                connection_options = {
                    "host": connection.endpoint.host,
                    "port": connection.endpoint.port,
                    "db": connection.endpoint.database,
                    "ssl": connection.endpoint.tls,
                    "username": (
                        credentials.username.get_secret_value()
                        if credentials.username is not None
                        else None
                    ),
                    "password": credentials.password.get_secret_value(),
                }
            redis_asyncio = load_optional_dependency(
                "redis.asyncio",
                extra="redis",
                feature="the Redis resource",
            )
            common_options = {
                "decode_responses": True,
                "max_connections": self._config.max_connections,
                "socket_connect_timeout": self._config.connect_timeout_seconds,
                "socket_timeout": self._config.operation_timeout_seconds,
            }
            if runtime_url is not None:
                self._client = redis_asyncio.from_url(runtime_url, **common_options)
            else:
                self._client = redis_asyncio.Redis(**connection_options, **common_options)
        try:
            await self._client.ping()
        except Exception as exc:
            raise ResourceOperationError("Redis connection failed", retryable=True) from exc

    async def close(self) -> None:
        client = self._client
        self._client = None
        if client is not None:
            await client.aclose()

    async def get(self, key: str) -> str | None:
        try:
            value = await self._require_client().get(self._key(key))
        except Exception as exc:
            raise ResourceOperationError("Redis cache read failed", retryable=True) from exc
        if value is None:
            return None
        if not isinstance(value, str):
            raise ResourceOperationError("Redis cache value is not text", retryable=False)
        self._enforce_value(value)
        return value

    async def set(self, key: str, value: str, ttl_sec: int | None = None) -> None:
        self._enforce_value(value)
        ttl = ttl_sec if ttl_sec is not None else self._config.default_ttl_seconds
        if ttl is not None and not 1 <= ttl <= MAX_REDIS_TTL_SECONDS:
            raise ResourceOperationError(
                "Redis TTL is outside the supported range", retryable=False
            )
        try:
            await self._require_client().set(self._key(key), value, ex=ttl)
        except Exception as exc:
            raise ResourceOperationError("Redis cache write failed", retryable=True) from exc

    async def get_value(self, key: str) -> Any | None:
        value = await self.get(key)
        if value is None:
            return None
        try:
            return loads_strict_json(value)
        except StrictJsonError as exc:
            raise ResourceOperationError(
                "Redis key-value entry failed strict JSON validation",
                retryable=False,
            ) from exc

    async def put_value(
        self,
        key: str,
        value: Any,
        *,
        ttl_seconds: int | None = None,
        if_absent: bool = False,
    ) -> bool:
        try:
            encoded = dumps_strict_json(value, layout=StrictJsonLayout.CANONICAL)
        except StrictJsonError as exc:
            raise ResourceOperationError(
                "Redis value must be strict JSON", retryable=False
            ) from exc
        self._enforce_value(encoded)
        ttl = ttl_seconds if ttl_seconds is not None else self._config.default_ttl_seconds
        if ttl is not None and not 1 <= ttl <= MAX_REDIS_TTL_SECONDS:
            raise ResourceOperationError(
                "Redis TTL is outside the supported range", retryable=False
            )
        try:
            stored = await self._require_client().set(
                self._key(key),
                encoded,
                ex=ttl,
                nx=if_absent,
            )
        except Exception as exc:
            raise ResourceOperationError("Redis key-value write failed", retryable=True) from exc
        return bool(stored)

    async def delete_value(self, key: str) -> None:
        try:
            await self._require_client().delete(self._key(key))
        except Exception as exc:
            raise ResourceOperationError("Redis key-value delete failed", retryable=True) from exc

    def _key(self, key: str) -> str:
        if not key:
            raise ResourceOperationError("Redis key must not be empty", retryable=False)
        scoped = f"{self._config.key_prefix}{key}"
        if len(scoped.encode("utf-8")) > MAX_REDIS_KEY_BYTES:
            raise ResourceOperationError(
                f"Redis key exceeds {MAX_REDIS_KEY_BYTES} UTF-8 bytes",
                retryable=False,
            )
        return scoped

    def _enforce_value(self, value: str) -> None:
        if len(value.encode("utf-8")) > self._config.max_value_bytes:
            raise ResourceOperationError(
                "Redis value exceeds the configured byte limit",
                retryable=False,
            )

    def _require_client(self) -> Any:
        if self._client is None:
            raise ResourceOperationError("Redis resource is not initialized", retryable=False)
        return self._client


def redis_resource_provider() -> ResourceProvider[RedisConfig]:
    return ResourceProvider(
        name="redis",
        contract_version="2",
        config_model=RedisConfig,
        capabilities=frozenset({ResourceCapability.CACHE, ResourceCapability.KEY_VALUE}),
        factory=lambda config, context: RedisResource(config, context),
        dependency_resolver=_redis_dependencies,
    )


def _redis_dependencies(config: RedisConfig) -> tuple[ResourceDependency, ...]:
    connection = config.connection
    if isinstance(connection, RedisRuntimeUrlConnection):
        return ()
    return (secret_resource_dependency(connection.credentials),)
