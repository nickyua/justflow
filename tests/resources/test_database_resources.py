"""Deterministic PostgreSQL and Redis resource tests."""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass

import pytest
from pydantic import SecretStr, ValidationError

from justflow.resources.base import (
    ResourceCapability,
    ResourceFactoryContext,
    ResourceOperationError,
)
from justflow.resources.credentials import SecretResourceCredentials
from justflow.resources.postgres import (
    PostgresConfig,
    PostgresEndpoint,
    PostgresResource,
    PostgresRuntimeDsnConnection,
    PostgresSecretConnection,
)
from justflow.resources.redis import (
    RedisConfig,
    RedisEndpoint,
    RedisResource,
    RedisRuntimeUrlConnection,
    RedisSecretConnection,
)


class FakePostgresPool:
    def __init__(self, records: list[dict[str, object]] | None = None) -> None:
        self.calls: list[tuple[str, str, tuple[object, ...]]] = []
        self.closed = False
        self.records = [{"id": 1, "state": "synthetic"}] if records is None else records
        self.consumed = 0
        self.acquired = False
        self.in_transaction = False

    @asynccontextmanager
    async def acquire(self):
        self.acquired = True
        try:
            yield self
        finally:
            self.acquired = False

    @asynccontextmanager
    async def transaction(self):
        self.in_transaction = True
        try:
            yield
        finally:
            self.in_transaction = False

    async def cursor(self, query: str, *parameters: object, prefetch: int):
        assert self.acquired and self.in_transaction
        assert prefetch == 1
        self.calls.append(("fetch", query, parameters))
        for record in self.records:
            self.consumed += 1
            yield record

    async def execute(self, query: str, *parameters: object) -> str:
        self.calls.append(("execute", query, parameters))
        return "UPDATE 1"

    async def close(self) -> None:
        self.closed = True


class FakePostgresPoolFactory:
    def __init__(self, pool: FakePostgresPool) -> None:
        self.pool = pool
        self.options: dict[str, object] = {}

    async def __call__(self, **options: object) -> FakePostgresPool:
        self.options = options
        return self.pool


@dataclass(frozen=True, kw_only=True)
class FetchReturns:
    value: int


@dataclass(frozen=True, kw_only=True)
class FetchRaises:
    exc: type[Exception]
    match: str


@dataclass(frozen=True, kw_only=True)
class BoundedFetchCase:
    id: str
    row_count: int
    max_rows: int
    max_bytes: int
    consumed: int
    outcome: FetchReturns | FetchRaises


BOUNDED_FETCH_CASES = [
    BoundedFetchCase(
        id="empty",
        row_count=0,
        max_rows=2,
        max_bytes=100,
        consumed=0,
        outcome=FetchReturns(value=0),
    ),
    BoundedFetchCase(
        id="row-boundary",
        row_count=2,
        max_rows=2,
        max_bytes=100,
        consumed=2,
        outcome=FetchReturns(value=2),
    ),
    BoundedFetchCase(
        id="row-overflow",
        row_count=100,
        max_rows=2,
        max_bytes=100,
        consumed=3,
        outcome=FetchRaises(exc=ResourceOperationError, match="row limit"),
    ),
    BoundedFetchCase(
        id="byte-overflow",
        row_count=100,
        max_rows=100,
        max_bytes=2,
        consumed=1,
        outcome=FetchRaises(exc=ResourceOperationError, match="byte limit"),
    ),
]


@pytest.mark.parametrize("case", BOUNDED_FETCH_CASES, ids=lambda case: case.id)
async def test_postgres_stops_at_bounds_and_releases_the_connection(case: BoundedFetchCase) -> None:
    pool = FakePostgresPool([{"id": row} for row in range(case.row_count)])
    resource = PostgresResource(
        PostgresConfig(
            connection=PostgresRuntimeDsnConnection(runtime_dsn="primary"),
            max_rows=case.max_rows,
            max_result_bytes=case.max_bytes,
        ),
        ResourceFactoryContext(
            postgres_dsns={"primary": SecretStr("postgresql://localhost/synthetic")}
        ),
        pool_factory=FakePostgresPoolFactory(pool),
    )
    await resource.initialize()
    try:
        if isinstance(case.outcome, FetchRaises):
            with pytest.raises(case.outcome.exc, match=case.outcome.match) as failure:
                await resource.fetch("SELECT id FROM records", [])
            assert isinstance(failure.value, ResourceOperationError)
            assert failure.value.retryable is False
        else:
            assert len(await resource.fetch("SELECT id FROM records", [])) == case.outcome.value
        assert pool.consumed == case.consumed
        assert not pool.acquired and not pool.in_transaction
    finally:
        await resource.close()


class FakeRedisClient:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.closed = False
        self.pinged = False

    async def ping(self) -> None:
        self.pinged = True

    async def get(self, key: str) -> str | None:
        return self.values.get(key)

    async def set(
        self,
        key: str,
        value: str,
        *,
        ex: int | None,
        nx: bool = False,
    ) -> bool:
        if nx and key in self.values:
            return False
        self.values[key] = value
        return True

    async def delete(self, key: str) -> None:
        self.values.pop(key, None)

    async def aclose(self) -> None:
        self.closed = True


class FakeRedisModule:
    def __init__(self, client: FakeRedisClient) -> None:
        self.client = client
        self.options: dict[str, object] = {}

    def from_url(self, _url: str, **options: object) -> FakeRedisClient:
        self.options = options
        return self.client

    def Redis(self, **options: object) -> FakeRedisClient:
        self.options = options
        return self.client


class FakeSecretReader:
    def __init__(self, value: str) -> None:
        self.value = value
        self.requests: list[str] = []

    async def read_secret(self, name: str) -> SecretStr:
        self.requests.append(name)
        return SecretStr(self.value)


async def test_postgres_pool_uses_runtime_binding_and_parameterized_facade() -> None:
    pool = FakePostgresPool()
    factory = FakePostgresPoolFactory(pool)
    context = ResourceFactoryContext(
        postgres_dsns={"primary": SecretStr("postgresql://localhost/synthetic")}
    )
    resource = PostgresResource(
        PostgresConfig(
            connection=PostgresRuntimeDsnConnection(runtime_dsn="primary"),
            min_pool_size=2,
            max_pool_size=4,
        ),
        context,
        pool_factory=factory,
    )

    await resource.initialize()
    assert await resource.execute("UPDATE records SET state = $1 WHERE id = $2", ["ready", 7]) == (
        "UPDATE 1"
    )
    assert await resource.fetch("SELECT id, state FROM records WHERE id = $1", [7]) == [
        {"id": 1, "state": "synthetic"}
    ]
    assert factory.options["min_size"] == 2
    assert factory.options["max_size"] == 4
    assert pool.calls == [
        ("execute", "UPDATE records SET state = $1 WHERE id = $2", ("ready", 7)),
        ("fetch", "SELECT id, state FROM records WHERE id = $1", (7,)),
    ]

    with pytest.raises(ResourceOperationError) as exc_info:
        await resource.execute("SELECT $1", "not-a-parameter-sequence")
    assert exc_info.value.retryable is False

    await resource.close()
    assert pool.closed is True


def test_postgres_pool_bounds_are_validated_before_construction() -> None:
    with pytest.raises(ValidationError, match="min_pool_size cannot exceed"):
        PostgresConfig(
            connection=PostgresRuntimeDsnConnection(runtime_dsn="primary"),
            min_pool_size=3,
            max_pool_size=2,
        )


async def test_postgres_builds_connection_from_secret_reader() -> None:
    pool = FakePostgresPool()
    factory = FakePostgresPoolFactory(pool)
    reader = FakeSecretReader('{"username":"fixture-user","password":"fixture-password"}')

    def resolve(name: str, capability: ResourceCapability) -> object:
        assert name == "platform_secrets"
        assert capability is ResourceCapability.SECRET_READER
        return reader

    resource = PostgresResource(
        PostgresConfig(
            connection=PostgresSecretConnection(
                endpoint=PostgresEndpoint(
                    host="database.local",
                    database="application",
                ),
                credentials=SecretResourceCredentials(
                    secret_resource="platform_secrets",
                    secret_alias="database_credentials",
                ),
            )
        ),
        ResourceFactoryContext(resource_resolver=resolve),
        pool_factory=factory,
    )

    await resource.initialize()

    assert reader.requests == ["database_credentials"]
    assert factory.options["host"] == "database.local"
    assert factory.options["port"] == 5432
    assert factory.options["database"] == "application"
    assert factory.options["ssl"] is True
    assert "dsn" not in factory.options
    await resource.close()


async def test_postgres_rejects_secret_without_username() -> None:
    factory = FakePostgresPoolFactory(FakePostgresPool())
    reader = FakeSecretReader('{"password":"fixture-password"}')
    resource = PostgresResource(
        PostgresConfig(
            connection=PostgresSecretConnection(
                endpoint=PostgresEndpoint(host="database.local", database="application"),
                credentials=SecretResourceCredentials(
                    secret_resource="platform_secrets",
                    secret_alias="database_credentials",
                ),
            )
        ),
        ResourceFactoryContext(resource_resolver=lambda _name, _capability: reader),
        pool_factory=factory,
    )

    with pytest.raises(ResourceOperationError, match="requires a non-empty username") as exc_info:
        await resource.initialize()

    assert exc_info.value.retryable is False
    assert factory.options == {}


async def test_redis_tls_timeouts_cache_and_key_value_contracts(monkeypatch) -> None:
    client = FakeRedisClient()
    module = FakeRedisModule(client)
    monkeypatch.setattr(
        "justflow.resources.redis.load_optional_dependency",
        lambda *_args, **_kwargs: module,
    )
    context = ResourceFactoryContext(redis_urls={"primary": SecretStr("rediss://cache.local/0")})
    resource = RedisResource(
        RedisConfig(
            connection=RedisRuntimeUrlConnection(runtime_url="primary"),
            key_prefix="workflow:",
            connect_timeout_seconds=2.0,
            operation_timeout_seconds=3.0,
        ),
        context,
    )

    await resource.initialize()
    await resource.set("cache-key", "synthetic", ttl_sec=30)
    assert await resource.get("cache-key") == "synthetic"
    assert await resource.put_value("state", {"status": "ready"}, if_absent=True)
    assert not await resource.put_value("state", {"status": "duplicate"}, if_absent=True)
    assert await resource.get_value("state") == {"status": "ready"}
    assert client.pinged is True
    assert module.options == {
        "decode_responses": True,
        "max_connections": 20,
        "socket_connect_timeout": 2.0,
        "socket_timeout": 3.0,
    }

    await resource.close()
    assert client.closed is True


async def test_redis_rejects_plaintext_binding_when_tls_is_required(monkeypatch) -> None:
    module = FakeRedisModule(FakeRedisClient())
    monkeypatch.setattr(
        "justflow.resources.redis.load_optional_dependency",
        lambda *_args, **_kwargs: module,
    )
    resource = RedisResource(
        RedisConfig(connection=RedisRuntimeUrlConnection(runtime_url="primary")),
        ResourceFactoryContext(redis_urls={"primary": SecretStr("redis://cache.local/0")}),
    )

    with pytest.raises(ResourceOperationError) as exc_info:
        await resource.initialize()

    assert exc_info.value.retryable is False
    assert module.options == {}


async def test_redis_builds_tls_connection_from_secret_reader(monkeypatch) -> None:
    client = FakeRedisClient()
    module = FakeRedisModule(client)
    monkeypatch.setattr(
        "justflow.resources.redis.load_optional_dependency",
        lambda *_args, **_kwargs: module,
    )
    reader = FakeSecretReader('{"password":"fixture-password"}')

    def resolve(name: str, capability: ResourceCapability) -> object:
        assert name == "platform_secrets"
        assert capability is ResourceCapability.SECRET_READER
        return reader

    resource = RedisResource(
        RedisConfig(
            connection=RedisSecretConnection(
                endpoint=RedisEndpoint(host="cache.local"),
                credentials=SecretResourceCredentials(
                    secret_resource="platform_secrets",
                    secret_alias="cache_credentials",
                ),
            )
        ),
        ResourceFactoryContext(resource_resolver=resolve),
    )

    await resource.initialize()

    assert reader.requests == ["cache_credentials"]
    assert module.options["host"] == "cache.local"
    assert module.options["port"] == 6379
    assert module.options["ssl"] is True
    assert module.options["username"] is None
    assert client.pinged is True
    await resource.close()


async def test_redis_rejects_non_json_secret_before_client_creation(monkeypatch) -> None:
    module = FakeRedisModule(FakeRedisClient())
    monkeypatch.setattr(
        "justflow.resources.redis.load_optional_dependency",
        lambda *_args, **_kwargs: module,
    )
    reader = FakeSecretReader("not-json")
    resource = RedisResource(
        RedisConfig(
            connection=RedisSecretConnection(
                endpoint=RedisEndpoint(host="cache.local"),
                credentials=SecretResourceCredentials(
                    secret_resource="platform_secrets",
                    secret_alias="cache_credentials",
                ),
            )
        ),
        ResourceFactoryContext(resource_resolver=lambda _name, _capability: reader),
    )

    with pytest.raises(ResourceOperationError, match="must be strict JSON") as exc_info:
        await resource.initialize()

    assert exc_info.value.retryable is False
    assert module.options == {}
