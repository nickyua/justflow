"""Async PostgreSQL resource with parameterized query methods."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping, Sequence
from enum import Enum
from typing import Any, Self

from pydantic import Field, field_validator, model_validator

from justflow.config.grammar import Identifier
from justflow.engine.limits import PayloadSerializationError, strict_json_bytes
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

MAX_POSTGRES_HOST_LENGTH = 253
MAX_POSTGRES_DATABASE_LENGTH = 128
DEFAULT_POSTGRES_PORT = 5432
MAX_NETWORK_PORT = 65_535
MAX_POSTGRES_POOL_SIZE = 100
MAX_POSTGRES_QUERY_BYTES = 1024 * 1024
MAX_POSTGRES_PARAMETERS = 65_535
DEFAULT_POSTGRES_MAX_ROWS = 10_000
MAX_POSTGRES_ROWS = 100_000
DEFAULT_POSTGRES_RESULT_BYTES = 10 * 1024 * 1024
MAX_POSTGRES_RESULT_BYTES = 100 * 1024 * 1024
DEFAULT_POSTGRES_COMMAND_TIMEOUT_SECONDS = 30.0
MAX_POSTGRES_COMMAND_TIMEOUT_SECONDS = 300.0
POSTGRES_CURSOR_PREFETCH_ROWS = 1
JSON_ARRAY_BOUNDARY_BYTES = len(b"[]")
JSON_ITEM_SEPARATOR_BYTES = len(b",")


class PostgresSslMode(str, Enum):
    DISABLE = "disable"
    REQUIRE = "require"


class PostgresEndpoint(StrictResourceConfig):
    host: str = Field(min_length=1, max_length=MAX_POSTGRES_HOST_LENGTH)
    port: int = Field(default=DEFAULT_POSTGRES_PORT, ge=1, le=MAX_NETWORK_PORT)
    database: str = Field(min_length=1, max_length=MAX_POSTGRES_DATABASE_LENGTH)
    sslmode: PostgresSslMode = PostgresSslMode.REQUIRE

    @field_validator("host")
    @classmethod
    def validate_host(cls, value: str) -> str:
        if any(character.isspace() for character in value) or any(
            character in value for character in "/@?#\x00"
        ):
            raise ValueError("PostgreSQL host must be a hostname or IP address")
        return value


class PostgresSecretConnection(StrictResourceConfig):
    endpoint: PostgresEndpoint
    credentials: SecretResourceCredentials


class PostgresRuntimeDsnConnection(StrictResourceConfig):
    runtime_dsn: Identifier


class PostgresConfig(StrictResourceConfig):
    connection: PostgresSecretConnection | PostgresRuntimeDsnConnection
    min_pool_size: int = Field(default=1, ge=1, le=MAX_POSTGRES_POOL_SIZE)
    max_pool_size: int = Field(default=10, ge=1, le=MAX_POSTGRES_POOL_SIZE)
    command_timeout_seconds: float = Field(
        default=DEFAULT_POSTGRES_COMMAND_TIMEOUT_SECONDS,
        gt=0,
        le=MAX_POSTGRES_COMMAND_TIMEOUT_SECONDS,
    )
    max_rows: int = Field(default=DEFAULT_POSTGRES_MAX_ROWS, ge=1, le=MAX_POSTGRES_ROWS)
    max_result_bytes: int = Field(
        default=DEFAULT_POSTGRES_RESULT_BYTES,
        ge=1,
        le=MAX_POSTGRES_RESULT_BYTES,
    )

    @model_validator(mode="after")
    def validate_pool_bounds(self) -> Self:
        if self.min_pool_size > self.max_pool_size:
            raise ValueError("PostgreSQL min_pool_size cannot exceed max_pool_size")
        return self


PostgresPoolFactory = Callable[..., Any]


class PostgresResource:
    def __init__(
        self,
        config: PostgresConfig,
        context: ResourceFactoryContext,
        pool_factory: PostgresPoolFactory | None = None,
    ) -> None:
        self._config = config
        self._context = context
        self._pool_factory = pool_factory
        self._pool: Any | None = None

    async def initialize(self) -> None:
        factory = self._pool_factory
        if factory is None:
            asyncpg = load_optional_dependency(
                "asyncpg",
                extra="postgres",
                feature="the PostgreSQL resource",
            )
            factory = asyncpg.create_pool
        options: dict[str, object] = {
            "min_size": self._config.min_pool_size,
            "max_size": self._config.max_pool_size,
            "command_timeout": self._config.command_timeout_seconds,
        }
        connection = self._config.connection
        if isinstance(connection, PostgresRuntimeDsnConnection):
            options["dsn"] = self._context.postgres_dsn(connection.runtime_dsn).get_secret_value()
        else:
            secret = await self._context.secret_reader(
                connection.credentials.secret_resource
            ).read_secret(connection.credentials.secret_alias)
            credentials = parse_username_password_secret(secret, require_username=True)
            if credentials.username is None:
                raise RuntimeError("Validated PostgreSQL credentials have no username")
            options.update(
                {
                    "host": connection.endpoint.host,
                    "port": connection.endpoint.port,
                    "database": connection.endpoint.database,
                    "user": credentials.username.get_secret_value(),
                    "password": credentials.password.get_secret_value(),
                    "ssl": connection.endpoint.sslmode is PostgresSslMode.REQUIRE,
                }
            )
        try:
            self._pool = await factory(**options)
        except Exception as exc:
            raise ResourceOperationError(
                "PostgreSQL pool initialization failed",
                retryable=True,
            ) from exc

    async def close(self) -> None:
        pool = self._pool
        self._pool = None
        if pool is not None:
            await pool.close()

    async def execute(self, query: str, parameters: Sequence[Any]) -> str:
        self._validate_query(query)
        bound_parameters = self._validate_parameters(parameters)
        try:
            result = await self._require_pool().execute(query, *bound_parameters)
        except Exception as exc:
            raise self._query_error(exc) from exc
        if not isinstance(result, str):
            raise ResourceOperationError(
                "PostgreSQL execute returned an invalid result",
                retryable=False,
            )
        return result

    async def fetch(
        self,
        query: str,
        parameters: Sequence[Any],
    ) -> list[Mapping[str, Any]]:
        self._validate_query(query)
        bound_parameters = self._validate_parameters(parameters)
        rows: list[Mapping[str, Any]] = []
        size = JSON_ARRAY_BOUNDARY_BYTES
        if size > self._config.max_result_bytes:
            raise ResourceOperationError(
                "PostgreSQL result exceeds the configured byte limit", retryable=False
            )
        try:
            async with asyncio.timeout(self._config.command_timeout_seconds):
                async with self._require_pool().acquire() as connection:
                    async with connection.transaction():
                        async for record in connection.cursor(
                            query, *bound_parameters, prefetch=POSTGRES_CURSOR_PREFETCH_ROWS
                        ):
                            if len(rows) >= self._config.max_rows:
                                raise ResourceOperationError(
                                    f"PostgreSQL result exceeds the configured {self._config.max_rows}-row limit",
                                    retryable=False,
                                )
                            row = dict(record)
                            size += len(strict_json_bytes(row))
                            if rows:
                                size += JSON_ITEM_SEPARATOR_BYTES
                            if size > self._config.max_result_bytes:
                                raise ResourceOperationError(
                                    "PostgreSQL result exceeds the configured byte limit",
                                    retryable=False,
                                )
                            rows.append(row)
        except ResourceOperationError:
            raise
        except PayloadSerializationError as exc:
            raise ResourceOperationError(
                "PostgreSQL result is not strict JSON", retryable=False
            ) from exc
        except Exception as exc:
            raise self._query_error(exc) from exc
        return rows

    def _validate_query(self, query: str) -> None:
        if not query.strip() or len(query.encode("utf-8")) > MAX_POSTGRES_QUERY_BYTES:
            raise ResourceOperationError(
                f"PostgreSQL query must contain 1-{MAX_POSTGRES_QUERY_BYTES} UTF-8 bytes",
                retryable=False,
            )

    @staticmethod
    def _validate_parameters(parameters: Sequence[Any]) -> tuple[Any, ...]:
        if isinstance(parameters, (str, bytes, bytearray)):
            raise ResourceOperationError(
                "PostgreSQL parameters must be a sequence of bound values",
                retryable=False,
            )
        if len(parameters) > MAX_POSTGRES_PARAMETERS:
            raise ResourceOperationError(
                f"PostgreSQL parameter count exceeds {MAX_POSTGRES_PARAMETERS}",
                retryable=False,
            )
        return tuple(parameters)

    def _require_pool(self) -> Any:
        if self._pool is None:
            raise ResourceOperationError("PostgreSQL resource is not initialized", retryable=False)
        return self._pool

    @staticmethod
    def _query_error(error: Exception) -> ResourceOperationError:
        non_retryable_suffixes = (
            "DataError",
            "IntegrityConstraintViolationError",
            "PostgresSyntaxError",
        )
        retryable = not type(error).__name__.endswith(non_retryable_suffixes)
        return ResourceOperationError("PostgreSQL query failed", retryable=retryable)


def postgres_resource_provider() -> ResourceProvider[PostgresConfig]:
    return ResourceProvider(
        name="postgresql",
        contract_version="2",
        config_model=PostgresConfig,
        capabilities=frozenset({ResourceCapability.DATABASE}),
        factory=lambda config, context: PostgresResource(config, context),
        dependency_resolver=_postgres_dependencies,
    )


def _postgres_dependencies(config: PostgresConfig) -> tuple[ResourceDependency, ...]:
    connection = config.connection
    if isinstance(connection, PostgresRuntimeDsnConnection):
        return ()
    return (secret_resource_dependency(connection.credentials),)
