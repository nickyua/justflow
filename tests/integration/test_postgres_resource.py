"""Opt-in real PostgreSQL checks; set JUSTFLOW_TEST_POSTGRES_DSN for a disposable database."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest
from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from justflow.resources.base import ResourceFactoryContext, ResourceOperationError
from justflow.resources.postgres import (
    PostgresConfig,
    PostgresResource,
    PostgresRuntimeDsnConnection,
)

MAX_ROWS = 2
MAX_RESULT_BYTES = 128
SLOW_QUERY_SECONDS = 30
CANCEL_AFTER_SECONDS = 0.05
RECOVERY_TIMEOUT_SECONDS = 5


class DatabaseTestSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="JUSTFLOW_TEST_")
    postgres_dsn: SecretStr | None = None


@pytest.fixture
async def database() -> AsyncIterator[PostgresResource]:
    dsn = DatabaseTestSettings().postgres_dsn
    if dsn is None:
        pytest.skip("Set JUSTFLOW_TEST_POSTGRES_DSN to run the PostgreSQL provider integration")
    resource = PostgresResource(
        PostgresConfig(
            connection=PostgresRuntimeDsnConnection(runtime_dsn="integration"),
            min_pool_size=1,
            max_pool_size=1,
            max_rows=MAX_ROWS,
            max_result_bytes=MAX_RESULT_BYTES,
        ),
        ResourceFactoryContext(postgres_dsns={"integration": dsn}),
    )
    await resource.initialize()
    try:
        yield resource
    finally:
        await resource.close()


async def test_native_cursor_parameters_and_empty_results(database: PostgresResource) -> None:
    assert await database.fetch("SELECT $1::text AS value", ["'; SELECT pg_sleep(30); --"]) == [
        {"value": "'; SELECT pg_sleep(30); --"}
    ]
    assert await database.fetch("SELECT 1 WHERE false", []) == []


@pytest.mark.parametrize(
    "query,parameters,match",
    [
        pytest.param(
            "SELECT generate_series(1, $1::int) AS value", [MAX_ROWS + 1], "row limit", id="rows"
        ),
        pytest.param(
            "SELECT repeat('x', $1::int) AS value", [MAX_RESULT_BYTES], "byte limit", id="bytes"
        ),
    ],
)
async def test_bound_failure_releases_the_only_pool_connection(
    database: PostgresResource, query: str, parameters: list[int], match: str
) -> None:
    with pytest.raises(ResourceOperationError, match=match) as failure:
        await database.fetch(query, parameters)
    assert failure.value.retryable is False
    async with asyncio.timeout(RECOVERY_TIMEOUT_SECONDS):
        assert await database.fetch("SELECT 1 AS ready", []) == [{"ready": 1}]


async def test_cancelled_cursor_releases_the_only_pool_connection(
    database: PostgresResource,
) -> None:
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(CANCEL_AFTER_SECONDS):
            await database.fetch("SELECT pg_sleep($1::float8)", [SLOW_QUERY_SECONDS])
    async with asyncio.timeout(RECOVERY_TIMEOUT_SECONDS):
        assert await database.fetch("SELECT 1 AS ready", []) == [{"ready": 1}]
