"""Typed lifecycle and capability contracts for configured resources."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, SecretStr


class StrictResourceConfig(BaseModel):
    """Provider-owned resource configuration parsed before construction."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class ResourceCapability(str, Enum):
    ARCHIVE = "archive"
    CACHE = "cache"
    CONFIG = "config"
    DATABASE = "database"
    KEY_VALUE = "key-value"
    OBJECT = "object"
    SECRET_READER = "secret-reader"


@dataclass(frozen=True, kw_only=True)
class ResourceDependency:
    resource_name: str
    capability: ResourceCapability
    secret_alias: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.resource_name, str):
            raise TypeError("Resource dependency name must be text")
        if not self.resource_name:
            raise ValueError("Resource dependency name must not be empty")
        if not isinstance(self.capability, ResourceCapability):
            raise TypeError("Resource dependency capability is invalid")
        if self.secret_alias is not None and not isinstance(self.secret_alias, str):
            raise TypeError("Resource dependency secret alias must be text")
        if self.secret_alias is not None and not self.secret_alias:
            raise ValueError("Resource dependency secret alias must not be empty")
        if (
            self.secret_alias is not None
            and self.capability is not ResourceCapability.SECRET_READER
        ):
            raise ValueError(
                "Resource dependency secret alias requires the secret-reader capability"
            )


class ResourceError(Exception):
    """Base error exposed by the resource boundary."""


class ResourceBindingError(ResourceError):
    """A runtime-only resource binding is unavailable or invalid."""


class ResourceAccessError(ResourceError):
    """A resource is missing, denied, or lacks a required capability."""


class ResourceNotFoundError(ResourceAccessError):
    pass


class ResourceCapabilityError(ResourceAccessError):
    pass


class ResourceOperationError(ResourceError):
    """A provider operation failed behind its public facade."""

    def __init__(self, message: str, *, retryable: bool) -> None:
        self.retryable = retryable
        super().__init__(message)


class SecretNotFoundError(ResourceOperationError):
    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__(f"Secret '{name}' was not found", retryable=False)


@runtime_checkable
class ManagedResource(Protocol):
    async def initialize(self) -> None: ...

    async def close(self) -> None: ...


@runtime_checkable
class ArchiveStore(Protocol):
    async def write(self, path: str, data: str, *, retention_policy: str) -> None: ...


@runtime_checkable
class CacheStore(Protocol):
    async def get(self, key: str) -> str | None: ...

    async def set(self, key: str, value: str, ttl_sec: int | None = None) -> None: ...


@runtime_checkable
class ConfigReader(Protocol):
    def get(self, key: str, default: Any = None) -> Any: ...


@runtime_checkable
class ObjectStore(Protocol):
    async def read_object(self, key: str) -> bytes | None: ...

    async def write_object(self, key: str, data: bytes) -> None: ...

    async def delete_object(self, key: str) -> None: ...


@runtime_checkable
class SecretReader(Protocol):
    async def read_secret(self, name: str) -> SecretStr: ...


@runtime_checkable
class KeyValueStore(Protocol):
    async def get_value(self, key: str) -> Any | None: ...

    async def put_value(
        self,
        key: str,
        value: Any,
        *,
        ttl_seconds: int | None = None,
        if_absent: bool = False,
    ) -> bool: ...

    async def delete_value(self, key: str) -> None: ...


@runtime_checkable
class Database(Protocol):
    async def execute(self, query: str, parameters: Sequence[Any]) -> str: ...

    async def fetch(
        self,
        query: str,
        parameters: Sequence[Any],
    ) -> list[Mapping[str, Any]]: ...


@runtime_checkable
class AwsClientFactory(Protocol):
    def client(
        self,
        service_name: str,
        *,
        region_name: str | None,
        endpoint_url: str | None,
    ) -> Any: ...


ResourceResolver = Callable[[str, ResourceCapability], object]


def _empty_secret_mapping() -> Mapping[str, SecretStr]:
    return MappingProxyType({})


@dataclass(frozen=True, kw_only=True)
class ResourceFactoryContext:
    postgres_dsns: Mapping[str, SecretStr] = field(default_factory=_empty_secret_mapping)
    redis_urls: Mapping[str, SecretStr] = field(default_factory=_empty_secret_mapping)
    aws_client_factory: AwsClientFactory | None = None
    resource_resolver: ResourceResolver | None = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        for binding_type, bindings in (
            ("PostgreSQL", self.postgres_dsns),
            ("Redis", self.redis_urls),
        ):
            if any(not isinstance(name, str) or not name for name in bindings):
                raise ResourceBindingError(f"{binding_type} binding names must not be empty")
            if any(not isinstance(value, SecretStr) for value in bindings.values()):
                raise ResourceBindingError(f"{binding_type} binding values must use SecretStr")
        object.__setattr__(
            self,
            "postgres_dsns",
            MappingProxyType(dict(self.postgres_dsns)),
        )
        object.__setattr__(
            self,
            "redis_urls",
            MappingProxyType(dict(self.redis_urls)),
        )

    def postgres_dsn(self, name: str) -> SecretStr:
        try:
            return self.postgres_dsns[name]
        except KeyError as exc:
            raise ResourceBindingError(
                f"PostgreSQL connection binding '{name}' is not configured"
            ) from exc

    def redis_url(self, name: str) -> SecretStr:
        try:
            return self.redis_urls[name]
        except KeyError as exc:
            raise ResourceBindingError(
                f"Redis connection binding '{name}' is not configured"
            ) from exc

    def with_resource_resolver(self, resolver: ResourceResolver) -> ResourceFactoryContext:
        return ResourceFactoryContext(
            postgres_dsns=self.postgres_dsns,
            redis_urls=self.redis_urls,
            aws_client_factory=self.aws_client_factory,
            resource_resolver=resolver,
        )

    def secret_reader(self, name: str) -> SecretReader:
        if self.resource_resolver is None:
            raise ResourceBindingError("Resource dependency resolution is unavailable")
        resource = self.resource_resolver(name, ResourceCapability.SECRET_READER)
        if not isinstance(resource, SecretReader):
            raise ResourceCapabilityError(
                f"Resource '{name}' does not implement the secret-reader capability"
            )
        return resource


class ConfiguredResource(ABC):
    """Contract required by application-local resource classes."""

    config_model: type[StrictResourceConfig]
    contract_version: str
    capabilities: frozenset[ResourceCapability]

    def __init__(
        self,
        _config: StrictResourceConfig,
        _context: ResourceFactoryContext,
    ) -> None:
        pass

    @classmethod
    def create(
        cls,
        config: StrictResourceConfig,
        context: ResourceFactoryContext,
    ) -> ManagedResource:
        return cls(config, context)

    @classmethod
    def resource_dependencies(
        cls,
        _config: StrictResourceConfig,
    ) -> tuple[ResourceDependency, ...]:
        return ()

    @classmethod
    def secret_aliases(cls, _config: StrictResourceConfig) -> frozenset[str] | None:
        return None

    @abstractmethod
    async def initialize(self) -> None: ...

    @abstractmethod
    async def close(self) -> None: ...


CAPABILITY_PROTOCOLS: Mapping[ResourceCapability, type[object]] = MappingProxyType(
    {
        ResourceCapability.ARCHIVE: ArchiveStore,
        ResourceCapability.CACHE: CacheStore,
        ResourceCapability.CONFIG: ConfigReader,
        ResourceCapability.DATABASE: Database,
        ResourceCapability.KEY_VALUE: KeyValueStore,
        ResourceCapability.OBJECT: ObjectStore,
        ResourceCapability.SECRET_READER: SecretReader,
    }
)
