"""Resource provider registration, validation, and grant contracts."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TypeAlias

import pytest
from pydantic import Field

from justflow.config.loader import ConfigLoader, ConfigLoadError
from justflow.config.models import ResourceConfig
from justflow.resources.base import (
    ConfiguredResource,
    ResourceCapability,
    ResourceDependency,
    ResourceFactoryContext,
    ResourceNotFoundError,
    StrictResourceConfig,
)
from justflow.resources.registry import (
    DuplicateResourceProviderError,
    ResolvedResource,
    ResourceClassImportError,
    ResourceConfigError,
    ResourceDependencyError,
    ResourceFactoryError,
    ResourceProvider,
    ResourceRegistry,
    UnknownResourceProviderError,
)
from justflow.sdk.base_action import BaseAction
from justflow.sdk.resource_loader import ResourceLoader


class HostConfig(StrictResourceConfig):
    values: dict[str, str] = Field(default_factory=dict)


class HostResource:
    def __init__(self, config: HostConfig) -> None:
        self._values = dict(config.values)
        self.initialized = False
        self.closed = False

    async def initialize(self) -> None:
        self.initialized = True

    async def close(self) -> None:
        self.closed = True

    def get(self, key: str, default: object = None) -> object:
        return self._values.get(key, default)


class HostClassResource(ConfiguredResource):
    config_model = HostConfig
    contract_version = "1"
    capabilities = frozenset({ResourceCapability.CONFIG})

    def __init__(
        self,
        config: StrictResourceConfig,
        _context: ResourceFactoryContext,
    ) -> None:
        if not isinstance(config, HostConfig):
            raise TypeError("HostClassResource requires HostConfig")
        self._values = dict(config.values)
        self.initialized = False
        self.closed = False

    async def initialize(self) -> None:
        self.initialized = True

    async def close(self) -> None:
        self.closed = True

    def get(self, key: str, default: object = None) -> object:
        return self._values.get(key, default)


@dataclass(frozen=True, kw_only=True)
class DependencySpec:
    name: str
    capabilities: frozenset[ResourceCapability]
    dependencies: tuple[ResourceDependency, ...] = ()
    secret_aliases: frozenset[str] | None = None


@dataclass(frozen=True, kw_only=True)
class DependencyReturns:
    value: tuple[str, ...]


@dataclass(frozen=True, kw_only=True)
class DependencyRaises:
    exc: type[Exception]
    match: str


DependencyOutcome: TypeAlias = DependencyReturns | DependencyRaises


@dataclass(frozen=True, kw_only=True)
class DependencyCase:
    id: str
    resources: tuple[DependencySpec, ...]
    outcome: DependencyOutcome


SECRET_READER_SPEC = DependencySpec(
    name="secrets",
    capabilities=frozenset({ResourceCapability.SECRET_READER}),
    secret_aliases=frozenset({"database_credentials"}),
)
DATABASE_DEPENDENCY = ResourceDependency(
    resource_name="secrets",
    capability=ResourceCapability.SECRET_READER,
    secret_alias="database_credentials",
)
DEPENDENCY_CASES = (
    DependencyCase(
        id="ordered",
        resources=(
            DependencySpec(
                name="database",
                capabilities=frozenset({ResourceCapability.DATABASE}),
                dependencies=(DATABASE_DEPENDENCY,),
            ),
            SECRET_READER_SPEC,
        ),
        outcome=DependencyReturns(value=("secrets", "database")),
    ),
    DependencyCase(
        id="missing-resource",
        resources=(
            DependencySpec(
                name="database",
                capabilities=frozenset({ResourceCapability.DATABASE}),
                dependencies=(DATABASE_DEPENDENCY,),
            ),
        ),
        outcome=DependencyRaises(
            exc=ResourceDependencyError,
            match="requires missing resource 'secrets'",
        ),
    ),
    DependencyCase(
        id="wrong-capability",
        resources=(
            DependencySpec(
                name="database",
                capabilities=frozenset({ResourceCapability.DATABASE}),
                dependencies=(DATABASE_DEPENDENCY,),
            ),
            DependencySpec(
                name="secrets",
                capabilities=frozenset({ResourceCapability.CONFIG}),
            ),
        ),
        outcome=DependencyRaises(
            exc=ResourceDependencyError,
            match="capability 'secret-reader'",
        ),
    ),
    DependencyCase(
        id="missing-alias",
        resources=(
            DependencySpec(
                name="database",
                capabilities=frozenset({ResourceCapability.DATABASE}),
                dependencies=(DATABASE_DEPENDENCY,),
            ),
            DependencySpec(
                name="secrets",
                capabilities=frozenset({ResourceCapability.SECRET_READER}),
                secret_aliases=frozenset({"another_secret"}),
            ),
        ),
        outcome=DependencyRaises(
            exc=ResourceDependencyError,
            match="undeclared secret alias",
        ),
    ),
    DependencyCase(
        id="cycle",
        resources=(
            DependencySpec(
                name="first",
                capabilities=frozenset({ResourceCapability.CONFIG}),
                dependencies=(
                    ResourceDependency(
                        resource_name="second",
                        capability=ResourceCapability.CONFIG,
                    ),
                ),
            ),
            DependencySpec(
                name="second",
                capabilities=frozenset({ResourceCapability.CONFIG}),
                dependencies=(
                    ResourceDependency(
                        resource_name="first",
                        capability=ResourceCapability.CONFIG,
                    ),
                ),
            ),
        ),
        outcome=DependencyRaises(
            exc=ResourceDependencyError,
            match="cycle: first -> second -> first",
        ),
    ),
)


def _provider(
    *,
    capabilities: frozenset[ResourceCapability] = frozenset({ResourceCapability.CONFIG}),
) -> ResourceProvider[HostConfig]:
    return ResourceProvider(
        name="host_config",
        contract_version="1",
        config_model=HostConfig,
        capabilities=capabilities,
        factory=lambda config, _context: HostResource(config),
    )


async def test_host_registered_provider_loads_from_yaml_without_core_edit(tmp_path) -> None:
    resources_path = tmp_path / "resources.yaml"
    resources_path.write_text(
        """resources:
  runtime:
    provider: host_config
    config:
      values:
        mode: synthetic
"""
    )
    declaration = ConfigLoader(tmp_path).load_resources()
    registry = ResourceRegistry()
    registry.register(_provider())
    resolved = registry.resolve_resources(declaration.resources)
    loader = ResourceLoader(registry, ResourceFactoryContext())

    await loader.load(resolved)

    resource = loader.resources.require("runtime", ResourceCapability.CONFIG)
    assert isinstance(resource, HostResource)
    assert resource.initialized is True
    assert resource.get("mode") == "synthetic"
    await loader.close()
    assert resource.closed is True


def test_duplicate_provider_registration_is_rejected() -> None:
    registry = ResourceRegistry()
    provider = _provider()
    registry.register(provider)

    with pytest.raises(DuplicateResourceProviderError, match="already registered"):
        registry.register(provider)


def test_unknown_provider_is_rejected_before_construction() -> None:
    with pytest.raises(UnknownResourceProviderError, match="not registered"):
        ResourceRegistry().resolve_resource(
            "runtime",
            ResourceConfig(provider="unknown_resource"),
        )


def test_provider_owned_config_is_strict_and_safe() -> None:
    registry = ResourceRegistry()
    registry.register(_provider())

    with pytest.raises(ResourceConfigError, match="invalid config.*unexpected"):
        registry.resolve_resource(
            "runtime",
            ResourceConfig(
                provider="host_config",
                config={"values": {}, "unexpected": "synthetic"},
            ),
        )


def test_declared_capabilities_must_match_runtime_protocols() -> None:
    registry = ResourceRegistry()
    registry.register(_provider(capabilities=frozenset({ResourceCapability.CACHE})))
    resolved = registry.resolve_resource(
        "runtime",
        ResourceConfig(provider="host_config"),
    )

    with pytest.raises(ResourceFactoryError, match="does not implement.*cache"):
        registry.build(resolved, ResourceFactoryContext())


def test_action_resource_view_is_immutable_and_denies_undeclared_access() -> None:
    allowed = object()
    action = BaseAction(resources={"allowed": allowed})
    resources: Mapping[str, object] = action.resources

    assert resources["allowed"] is allowed
    assert not hasattr(resources, "__setitem__")
    with pytest.raises(ResourceNotFoundError, match="not included in this grant"):
        action.get_resource("denied")


async def test_application_resource_class_loads_without_manual_registration(tmp_path) -> None:
    (tmp_path / "resources.yaml").write_text(
        """resources:
  runtime:
    class: tests.resources.test_resource_registry.HostClassResource
    config:
      values:
        mode: synthetic
"""
    )
    declaration = ConfigLoader(tmp_path).load_resources()
    registry = ResourceRegistry()
    resolved = registry.resolve_resources(declaration.resources)
    loader = ResourceLoader(registry)

    assert resolved["runtime"].provider_name == (
        "python:tests.resources.test_resource_registry.HostClassResource"
    )
    assert resolved["runtime"].provider_contract_version == "1"
    await loader.load(resolved)

    resource = loader.resources.require("runtime", ResourceCapability.CONFIG)
    assert isinstance(resource, HostClassResource)
    assert resource.get("mode") == "synthetic"
    await loader.close()
    assert resource.closed is True


def test_resource_declaration_requires_exactly_one_implementation(tmp_path) -> None:
    (tmp_path / "resources.yaml").write_text(
        """resources:
  ambiguous:
    provider: static
    class: tests.resources.test_resource_registry.HostClassResource
"""
    )

    with pytest.raises(ConfigLoadError, match="exactly one of 'provider' or 'class'"):
        ConfigLoader(tmp_path).load_resources()


def test_application_resource_class_must_implement_contract() -> None:
    declaration = ResourceConfig.model_validate(
        {"class": "tests.resources.test_resource_registry.HostResource"}
    )

    with pytest.raises(ResourceClassImportError, match="must extend ConfiguredResource"):
        ResourceRegistry().resolve_resource("runtime", declaration)


def test_application_resource_class_config_is_strict() -> None:
    declaration = ResourceConfig.model_validate(
        {
            "class": "tests.resources.test_resource_registry.HostClassResource",
            "config": {"unexpected": "synthetic"},
        }
    )

    with pytest.raises(ResourceConfigError, match="invalid config.*unexpected"):
        ResourceRegistry().resolve_resource("runtime", declaration)


@pytest.mark.parametrize("case", DEPENDENCY_CASES, ids=lambda case: case.id)
def test_resource_dependency_order_and_validation(case: DependencyCase) -> None:
    resources = {
        spec.name: ResolvedResource(
            name=spec.name,
            provider_name="fixture",
            provider_contract_version="1",
            config=StrictResourceConfig(),
            capabilities=spec.capabilities,
            dependencies=spec.dependencies,
            secret_aliases=spec.secret_aliases,
        )
        for spec in case.resources
    }

    if isinstance(case.outcome, DependencyRaises):
        with pytest.raises(case.outcome.exc, match=case.outcome.match):
            ResourceRegistry().resource_load_order(resources)
    else:
        assert ResourceRegistry().resource_load_order(resources) == case.outcome.value
