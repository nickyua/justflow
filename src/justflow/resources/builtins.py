"""Built-in resource provider registrations."""

from __future__ import annotations

import math
from typing import Any, Self

from pydantic import Field, model_validator

from justflow.resources.aws import aws_resource_providers
from justflow.resources.base import (
    ResourceCapability,
    ResourceFactoryContext,
    StrictResourceConfig,
)
from justflow.resources.memory import MemoryCache, MemoryStore, StaticConfig
from justflow.resources.postgres import postgres_resource_provider
from justflow.resources.redis import redis_resource_provider
from justflow.resources.registry import ResourceProvider, ResourceRegistry
from justflow.resources.s3 import s3_resource_providers

MAX_MEMORY_RETENTION_SECONDS = 10 * 365 * 24 * 60 * 60


class MemoryArchiveConfig(StrictResourceConfig):
    retention_policies: dict[str, float] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_retention(self) -> Self:
        if any(not name for name in self.retention_policies):
            raise ValueError("retention policy names must not be empty")
        if any(
            not math.isfinite(seconds) or seconds <= 0 or seconds > MAX_MEMORY_RETENTION_SECONDS
            for seconds in self.retention_policies.values()
        ):
            raise ValueError(
                "retention values must be finite positive seconds no greater than "
                f"{MAX_MEMORY_RETENTION_SECONDS}"
            )
        return self


class MemoryCacheConfig(StrictResourceConfig):
    pass


class StaticConfigConfig(StrictResourceConfig):
    values: dict[str, Any] = Field(default_factory=dict)


def _memory_archive_factory(
    config: MemoryArchiveConfig,
    _context: ResourceFactoryContext,
) -> MemoryStore:
    return MemoryStore(retention_policies=config.retention_policies)


def _memory_cache_factory(
    _config: MemoryCacheConfig,
    _context: ResourceFactoryContext,
) -> MemoryCache:
    return MemoryCache()


def _static_config_factory(
    config: StaticConfigConfig,
    _context: ResourceFactoryContext,
) -> StaticConfig:
    return StaticConfig(**config.values)


def builtin_resource_providers() -> tuple[ResourceProvider[Any], ...]:
    local_providers: tuple[ResourceProvider[Any], ...] = (
        ResourceProvider(
            name="memory_archive",
            contract_version="1",
            config_model=MemoryArchiveConfig,
            capabilities=frozenset({ResourceCapability.ARCHIVE}),
            factory=_memory_archive_factory,
        ),
        ResourceProvider(
            name="memory_cache",
            contract_version="1",
            config_model=MemoryCacheConfig,
            capabilities=frozenset({ResourceCapability.CACHE}),
            factory=_memory_cache_factory,
        ),
        ResourceProvider(
            name="static",
            contract_version="1",
            config_model=StaticConfigConfig,
            capabilities=frozenset({ResourceCapability.CONFIG}),
            factory=_static_config_factory,
        ),
    )
    return (
        *local_providers,
        *s3_resource_providers(),
        *aws_resource_providers(),
        postgres_resource_provider(),
        redis_resource_provider(),
    )


def builtin_resource_registry() -> ResourceRegistry:
    return ResourceRegistry.from_builtin_providers(builtin_resource_providers())
