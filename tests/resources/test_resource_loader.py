"""Tests for the ResourceLoader lifecycle and error wrapping."""

from __future__ import annotations

from typing import ClassVar

import pytest
from pydantic import SecretStr

from justflow.config.models import ResourceConfig
from justflow.resources.base import (
    ResourceCapability,
    ResourceDependency,
    ResourceFactoryContext,
    StrictResourceConfig,
)
from justflow.resources.registry import ResourceProvider, ResourceRegistry
from justflow.sdk.resource_loader import (
    ResourceCleanupError,
    ResourceLoader,
    ResourceLoadError,
)


class TrackedConfig(StrictResourceConfig):
    label: str
    fail_initialize: bool = False
    fail_close: bool = False


class TrackedResource:
    events: ClassVar[list[str]] = []
    instances: ClassVar[list[TrackedResource]] = []

    def __init__(self, config: TrackedConfig) -> None:
        self.label = config.label
        self.fail_initialize = config.fail_initialize
        self.fail_close = config.fail_close
        self.initialized = False
        self.closed = False
        self.instances.append(self)

    async def initialize(self) -> None:
        self.events.append(f"initialize:{self.label}")
        if self.fail_initialize:
            raise RuntimeError(f"initialize failed: {self.label}")
        self.initialized = True

    async def close(self) -> None:
        self.events.append(f"close:{self.label}")
        self.closed = True
        if self.fail_close:
            raise RuntimeError(f"close failed: {self.label}")

    def get(self, key: str, default: object = None) -> object:
        return self.label if key == "label" else default


class SecretConfig(StrictResourceConfig):
    pass


class DependentConfig(StrictResourceConfig):
    secret_resource: str
    secret_alias: str


class OrderedSecretResource:
    events: ClassVar[list[str]] = []

    async def initialize(self) -> None:
        self.events.append("initialize:secrets")

    async def close(self) -> None:
        self.events.append("close:secrets")

    async def read_secret(self, name: str) -> SecretStr:
        self.events.append(f"read:{name}")
        return SecretStr("synthetic")


class OrderedDependentResource:
    def __init__(
        self,
        config: DependentConfig,
        context: ResourceFactoryContext,
    ) -> None:
        self._config = config
        self._context = context

    async def initialize(self) -> None:
        OrderedSecretResource.events.append("initialize:dependent")
        await self._context.secret_reader(self._config.secret_resource).read_secret(
            self._config.secret_alias
        )

    async def close(self) -> None:
        OrderedSecretResource.events.append("close:dependent")

    def get(self, key: str, default: object = None) -> object:
        return "ready" if key == "state" else default


def _registry() -> ResourceRegistry:
    registry = ResourceRegistry()
    registry.register(
        ResourceProvider(
            name="tracked",
            contract_version="1",
            config_model=TrackedConfig,
            capabilities=frozenset({ResourceCapability.CONFIG}),
            factory=lambda config, _context: TrackedResource(config),
        )
    )
    return registry


def _dependency_registry() -> ResourceRegistry:
    registry = ResourceRegistry()
    registry.register(
        ResourceProvider(
            name="ordered_secret",
            contract_version="1",
            config_model=SecretConfig,
            capabilities=frozenset({ResourceCapability.SECRET_READER}),
            factory=lambda _config, _context: OrderedSecretResource(),
            secret_alias_resolver=lambda _config: frozenset({"connection"}),
        )
    )
    registry.register(
        ResourceProvider(
            name="ordered_dependent",
            contract_version="1",
            config_model=DependentConfig,
            capabilities=frozenset({ResourceCapability.CONFIG}),
            factory=lambda config, context: OrderedDependentResource(config, context),
            dependency_resolver=lambda config: (
                ResourceDependency(
                    resource_name=config.secret_resource,
                    capability=ResourceCapability.SECRET_READER,
                    secret_alias=config.secret_alias,
                ),
            ),
        )
    )
    return registry


def _definitions(
    registry: ResourceRegistry,
    configs: dict[str, dict[str, object]],
):
    return registry.resolve_resources(
        {
            name: ResourceConfig(provider="tracked", config=config)
            for name, config in configs.items()
        }
    )


class TestResourceLoader:
    async def test_load_initialize_get_close(self) -> None:
        TrackedResource.events.clear()
        TrackedResource.instances.clear()
        registry = _registry()
        loader = ResourceLoader(registry, ResourceFactoryContext())

        await loader.load(_definitions(registry, {"lifecycle": {"label": "db"}}))
        resource = loader.get("lifecycle")
        assert isinstance(resource, TrackedResource)
        assert resource.label == "db"
        assert resource.initialized is True

        await loader.close()
        assert resource.closed is True

    async def test_initialization_failure_uses_safe_resource_error(self) -> None:
        registry = _registry()
        loader = ResourceLoader(registry)

        with pytest.raises(
            ResourceLoadError,
            match="Resource 'broken'.*initialization raised RuntimeError",
        ):
            await loader.load(
                _definitions(
                    registry,
                    {"broken": {"label": "broken", "fail_initialize": True}},
                )
            )

    def test_get_missing_raises(self) -> None:
        loader = ResourceLoader(_registry())
        with pytest.raises(KeyError, match="not loaded"):
            loader.get("nope")

    async def test_later_initialization_failure_rolls_back_owned_resources(self) -> None:
        TrackedResource.events.clear()
        registry = _registry()
        loader = ResourceLoader(registry)

        with pytest.raises(ResourceLoadError, match="initialization raised RuntimeError"):
            await loader.load(
                _definitions(
                    registry,
                    {
                        "first": {"label": "first"},
                        "second": {"label": "second", "fail_initialize": True},
                    },
                )
            )

        assert TrackedResource.events == [
            "initialize:first",
            "initialize:second",
            "close:second",
            "close:first",
        ]
        assert dict(loader.resources) == {}

    async def test_cleanup_is_reverse_order_continues_and_aggregates_failures(self) -> None:
        TrackedResource.events.clear()
        registry = _registry()
        loader = ResourceLoader(registry)
        await loader.load(
            _definitions(
                registry,
                {
                    label: {"label": label, "fail_close": label != "third"}
                    for label in ("first", "second", "third")
                },
            )
        )

        with pytest.raises(ResourceCleanupError) as exc_info:
            await loader.close()

        assert TrackedResource.events[-3:] == ["close:third", "close:second", "close:first"]
        assert [failure.resource_name for failure in exc_info.value.failures] == [
            "second",
            "first",
        ]
        assert dict(loader.resources) == {}

        await loader.close()
        assert TrackedResource.events.count("close:first") == 1

    async def test_dependencies_initialize_first_and_close_last(self) -> None:
        OrderedSecretResource.events.clear()
        registry = _dependency_registry()
        definitions = registry.resolve_resources(
            {
                "dependent": ResourceConfig(
                    provider="ordered_dependent",
                    config={
                        "secret_resource": "secrets",
                        "secret_alias": "connection",
                    },
                ),
                "secrets": ResourceConfig(provider="ordered_secret"),
            }
        )
        loader = ResourceLoader(registry)

        await loader.load(definitions)
        await loader.close()

        assert OrderedSecretResource.events == [
            "initialize:secrets",
            "initialize:dependent",
            "read:connection",
            "close:dependent",
            "close:secrets",
        ]
