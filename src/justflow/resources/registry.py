"""Explicit registration, validation, and access for resource providers."""

from __future__ import annotations

import importlib
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Generic, TypeVar, cast

from pydantic import ValidationError

from justflow.config.models import ResourceConfig
from justflow.config.provider_names import is_valid_contract_version, is_valid_provider_name
from justflow.resources.base import (
    CAPABILITY_PROTOCOLS,
    ConfiguredResource,
    ManagedResource,
    ResourceCapability,
    ResourceCapabilityError,
    ResourceDependency,
    ResourceFactoryContext,
    ResourceNotFoundError,
    StrictResourceConfig,
)

ConfigT = TypeVar("ConfigT", bound=StrictResourceConfig)
ResourceFactory = Callable[[ConfigT, ResourceFactoryContext], ManagedResource]
ResourceDependencyResolver = Callable[[ConfigT], tuple[ResourceDependency, ...]]
SecretAliasResolver = Callable[[ConfigT], frozenset[str] | None]


class ResourceRegistryError(Exception):
    pass


class ResourceProviderDefinitionError(ResourceRegistryError):
    pass


class DuplicateResourceProviderError(ResourceRegistryError):
    pass


class UnknownResourceProviderError(ResourceRegistryError):
    pass


class ResourceClassImportError(ResourceRegistryError):
    pass


class ResourceConfigError(ResourceRegistryError):
    pass


class ResourceFactoryError(ResourceRegistryError):
    pass


class ResourceDependencyError(ResourceRegistryError):
    pass


@dataclass(frozen=True, kw_only=True)
class ResourceProvider(Generic[ConfigT]):
    name: str
    contract_version: str
    config_model: type[ConfigT]
    capabilities: frozenset[ResourceCapability]
    factory: ResourceFactory[ConfigT]
    dependency_resolver: ResourceDependencyResolver[ConfigT] | None = None
    secret_alias_resolver: SecretAliasResolver[ConfigT] | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not is_valid_provider_name(self.name):
            raise ResourceProviderDefinitionError(f"Invalid resource provider name '{self.name}'")
        if not isinstance(self.contract_version, str) or not is_valid_contract_version(
            self.contract_version
        ):
            raise ResourceProviderDefinitionError(
                f"Invalid contract version '{self.contract_version}' for resource provider "
                f"'{self.name}'"
            )
        if not isinstance(self.config_model, type) or not issubclass(
            self.config_model,
            StrictResourceConfig,
        ):
            raise ResourceProviderDefinitionError(
                f"Resource provider '{self.name}' config model must extend StrictResourceConfig"
            )
        if not self.capabilities or any(
            not isinstance(capability, ResourceCapability) for capability in self.capabilities
        ):
            raise ResourceProviderDefinitionError(
                f"Resource provider '{self.name}' must declare valid capabilities"
            )
        if not callable(self.factory):
            raise ResourceProviderDefinitionError(
                f"Resource provider '{self.name}' factory must be callable"
            )
        for resolver_name, resolver in (
            ("dependency", self.dependency_resolver),
            ("secret alias", self.secret_alias_resolver),
        ):
            if resolver is not None and not callable(resolver):
                raise ResourceProviderDefinitionError(
                    f"Resource provider '{self.name}' {resolver_name} resolver must be callable"
                )

    def parse_config(self, value: Mapping[str, object]) -> StrictResourceConfig:
        return self.config_model.model_validate(dict(value))

    def build(
        self,
        config: StrictResourceConfig,
        context: ResourceFactoryContext,
    ) -> ManagedResource:
        if not isinstance(config, self.config_model):
            raise ResourceFactoryError(
                f"Resource provider '{self.name}' received config type "
                f"'{type(config).__name__}', expected '{self.config_model.__name__}'"
            )
        resource = self.factory(config, context)
        if not isinstance(resource, ManagedResource):
            raise ResourceFactoryError(
                f"Resource provider '{self.name}' returned an object without async lifecycle methods"
            )
        missing = [
            capability.value
            for capability in sorted(self.capabilities, key=lambda item: item.value)
            if not isinstance(resource, CAPABILITY_PROTOCOLS[capability])
        ]
        if missing:
            raise ResourceFactoryError(
                f"Resource provider '{self.name}' does not implement declared capabilities: {missing}"
            )
        return resource

    def dependencies(self, config: StrictResourceConfig) -> tuple[ResourceDependency, ...]:
        self._validate_config_type(config)
        if self.dependency_resolver is None:
            return ()
        dependencies = self.dependency_resolver(cast(ConfigT, config))
        if not isinstance(dependencies, tuple) or any(
            not isinstance(dependency, ResourceDependency) for dependency in dependencies
        ):
            raise ResourceProviderDefinitionError(
                f"Resource provider '{self.name}' returned invalid resource dependencies"
            )
        identities = [
            (dependency.resource_name, dependency.capability, dependency.secret_alias)
            for dependency in dependencies
        ]
        if len(identities) != len(set(identities)):
            raise ResourceProviderDefinitionError(
                f"Resource provider '{self.name}' returned duplicate resource dependencies"
            )
        return dependencies

    def secret_aliases(self, config: StrictResourceConfig) -> frozenset[str] | None:
        self._validate_config_type(config)
        if self.secret_alias_resolver is None:
            return None
        aliases = self.secret_alias_resolver(cast(ConfigT, config))
        if aliases is not None and (
            not isinstance(aliases, frozenset)
            or any(not isinstance(alias, str) or not alias for alias in aliases)
        ):
            raise ResourceProviderDefinitionError(
                f"Resource provider '{self.name}' returned invalid secret aliases"
            )
        return aliases

    def _validate_config_type(self, config: StrictResourceConfig) -> None:
        if not isinstance(config, self.config_model):
            raise ResourceProviderDefinitionError(
                f"Resource provider '{self.name}' received an incompatible resolved config"
            )


@dataclass(frozen=True, kw_only=True)
class ResolvedResource:
    name: str
    provider_name: str
    provider_contract_version: str
    config: StrictResourceConfig
    capabilities: frozenset[ResourceCapability]
    class_path: str | None = None
    dependencies: tuple[ResourceDependency, ...] = ()
    secret_aliases: frozenset[str] | None = None


@dataclass(frozen=True, kw_only=True)
class LoadedResource:
    definition: ResolvedResource
    instance: object


class ResourceGrant(Mapping[str, object]):
    def __init__(self, resources: Mapping[str, object]) -> None:
        self._resources = MappingProxyType(dict(resources))

    def __getitem__(self, name: str) -> object:
        try:
            return self._resources[name]
        except KeyError as exc:
            raise ResourceNotFoundError(f"Resource '{name}' is not included in this grant") from exc

    def __iter__(self) -> Iterator[str]:
        return iter(self._resources)

    def __len__(self) -> int:
        return len(self._resources)


class ResourceCollection(Mapping[str, object]):
    def __init__(self, resources: Mapping[str, LoadedResource]) -> None:
        self._resources = MappingProxyType(dict(resources))

    @classmethod
    def from_instances(cls, resources: Mapping[str, object]) -> ResourceCollection:
        loaded: dict[str, LoadedResource] = {}
        for name, instance in resources.items():
            capabilities = frozenset(
                capability
                for capability, protocol in CAPABILITY_PROTOCOLS.items()
                if isinstance(instance, protocol)
            )
            loaded[name] = LoadedResource(
                definition=ResolvedResource(
                    name=name,
                    provider_name="injected",
                    provider_contract_version="1",
                    config=StrictResourceConfig(),
                    capabilities=capabilities,
                ),
                instance=instance,
            )
        return cls(loaded)

    @property
    def definitions(self) -> Mapping[str, ResolvedResource]:
        return MappingProxyType(
            {name: loaded.definition for name, loaded in self._resources.items()}
        )

    def __getitem__(self, name: str) -> object:
        return self._resources[name].instance

    def __iter__(self) -> Iterator[str]:
        return iter(self._resources)

    def __len__(self) -> int:
        return len(self._resources)

    def require(self, name: str, capability: ResourceCapability) -> object:
        try:
            loaded = self._resources[name]
        except KeyError as exc:
            raise ResourceNotFoundError(f"Resource '{name}' is not loaded") from exc
        if capability not in loaded.definition.capabilities:
            raise ResourceCapabilityError(
                f"Resource '{name}' does not provide the '{capability.value}' capability"
            )
        return loaded.instance

    def grant(self, names: Iterable[str]) -> ResourceGrant:
        granted: dict[str, object] = {}
        for name in names:
            try:
                granted[name] = self._resources[name].instance
            except KeyError as exc:
                raise ResourceNotFoundError(f"Resource '{name}' is not loaded") from exc
        return ResourceGrant(granted)


class ResourceRegistry:
    def __init__(self) -> None:
        self._providers: dict[str, ResourceProvider[Any]] = {}
        self._class_providers: dict[str, ResourceProvider[StrictResourceConfig]] = {}

    @classmethod
    def from_builtin_providers(
        cls,
        providers: Iterable[ResourceProvider[Any]],
    ) -> ResourceRegistry:
        registry = cls()
        for provider in providers:
            registry._register(provider)
        return registry

    @property
    def providers(self) -> Mapping[str, ResourceProvider[Any]]:
        return MappingProxyType(self._providers)

    def register(self, provider: ResourceProvider[Any]) -> None:
        self._register(provider)

    def _register(self, provider: ResourceProvider[Any]) -> None:
        if provider.name in self._providers:
            raise DuplicateResourceProviderError(
                f"Resource provider '{provider.name}' is already registered"
            )
        self._providers[provider.name] = provider

    def resolve_resources(
        self,
        resources: Mapping[str, ResourceConfig],
    ) -> dict[str, ResolvedResource]:
        return {
            name: self.resolve_resource(name, declaration)
            for name, declaration in resources.items()
        }

    def resolve_resource(self, name: str, declaration: ResourceConfig) -> ResolvedResource:
        if declaration.provider is not None:
            provider = self._provider(declaration.provider)
            provider_identity = provider.name
            class_path = None
        else:
            class_path = declaration.class_path
            if class_path is None:
                raise ResourceConfigError(
                    f"Resource '{name}' has no provider or application resource class"
                )
            provider = self._class_provider(class_path)
            provider_identity = f"python:{class_path}"
        try:
            config = provider.parse_config(declaration.config)
        except ValidationError as exc:
            locations = sorted(
                {
                    ".".join(str(component) for component in error["loc"])
                    for error in exc.errors(
                        include_url=False, include_context=False, include_input=False
                    )
                }
            )
            suffix = f" at {locations}" if locations else ""
            raise ResourceConfigError(
                f"Resource '{name}' has invalid config for '{provider_identity}'{suffix}"
            ) from exc
        except Exception as exc:
            raise ResourceConfigError(
                f"Resource implementation '{provider_identity}' could not validate resource '{name}'"
            ) from exc
        try:
            dependencies = provider.dependencies(config)
            secret_aliases = provider.secret_aliases(config)
        except ResourceProviderDefinitionError:
            raise
        except Exception as exc:
            raise ResourceConfigError(
                f"Resource implementation '{provider_identity}' returned invalid metadata"
            ) from exc
        return ResolvedResource(
            name=name,
            provider_name=provider_identity,
            provider_contract_version=provider.contract_version,
            config=config,
            capabilities=provider.capabilities,
            class_path=class_path,
            dependencies=dependencies,
            secret_aliases=secret_aliases,
        )

    def build(
        self,
        resource: ResolvedResource,
        context: ResourceFactoryContext,
    ) -> ManagedResource:
        provider = (
            self._class_provider(resource.class_path)
            if resource.class_path is not None
            else self._provider(resource.provider_name)
        )
        try:
            return provider.build(resource.config, context)
        except ResourceFactoryError:
            raise
        except Exception as exc:
            raise ResourceFactoryError(
                f"Resource provider '{provider.name}' failed to configure resource "
                f"'{resource.name}'"
            ) from exc

    def resource_load_order(
        self,
        resources: Mapping[str, ResolvedResource],
    ) -> tuple[str, ...]:
        self._validate_dependencies(resources)
        states: dict[str, str] = {}
        stack: list[str] = []
        ordered: list[str] = []

        def visit(resource_name: str) -> None:
            state = states.get(resource_name)
            if state == "complete":
                return
            if state == "visiting":
                cycle_start = stack.index(resource_name)
                cycle = " -> ".join([*stack[cycle_start:], resource_name])
                raise ResourceDependencyError(f"Resource dependency cycle: {cycle}")
            states[resource_name] = "visiting"
            stack.append(resource_name)
            for dependency in resources[resource_name].dependencies:
                visit(dependency.resource_name)
            stack.pop()
            states[resource_name] = "complete"
            ordered.append(resource_name)

        for resource_name in resources:
            visit(resource_name)
        return tuple(ordered)

    def require_capability(
        self,
        resource: ResolvedResource,
        capability: ResourceCapability,
    ) -> None:
        if capability not in resource.capabilities:
            raise ResourceCapabilityError(
                f"Resource '{resource.name}' from provider '{resource.provider_name}' does not "
                f"provide the '{capability.value}' capability"
            )

    def _provider(self, name: str) -> ResourceProvider[Any]:
        try:
            return self._providers[name]
        except KeyError as exc:
            raise UnknownResourceProviderError(
                f"Resource provider '{name}' is not registered"
            ) from exc

    def _class_provider(self, class_path: str) -> ResourceProvider[StrictResourceConfig]:
        cached = self._class_providers.get(class_path)
        if cached is not None:
            return cached
        module_name, separator, class_name = class_path.rpartition(".")
        if not separator:
            raise ResourceClassImportError(
                f"Resource class '{class_path}' must include its module path"
            )
        try:
            module = importlib.import_module(module_name)
            candidate = getattr(module, class_name)
        except Exception as exc:
            raise ResourceClassImportError(
                f"Resource class '{class_path}' could not be imported"
            ) from exc
        if not isinstance(candidate, type) or not issubclass(candidate, ConfiguredResource):
            raise ResourceClassImportError(
                f"Resource class '{class_path}' must extend ConfiguredResource"
            )
        try:
            provider = ResourceProvider(
                name="python_class",
                contract_version=candidate.contract_version,
                config_model=candidate.config_model,
                capabilities=candidate.capabilities,
                factory=candidate.create,
                dependency_resolver=candidate.resource_dependencies,
                secret_alias_resolver=candidate.secret_aliases,
            )
        except (AttributeError, ResourceProviderDefinitionError) as exc:
            raise ResourceClassImportError(
                f"Resource class '{class_path}' has an invalid resource contract"
            ) from exc
        self._class_providers[class_path] = provider
        return provider

    @staticmethod
    def _validate_dependencies(resources: Mapping[str, ResolvedResource]) -> None:
        for resource in resources.values():
            for dependency in resource.dependencies:
                target = resources.get(dependency.resource_name)
                if target is None:
                    raise ResourceDependencyError(
                        f"Resource '{resource.name}' requires missing resource "
                        f"'{dependency.resource_name}'"
                    )
                if dependency.capability not in target.capabilities:
                    raise ResourceDependencyError(
                        f"Resource '{resource.name}' requires resource "
                        f"'{dependency.resource_name}' with capability "
                        f"'{dependency.capability.value}'"
                    )
                if (
                    dependency.secret_alias is not None
                    and target.secret_aliases is not None
                    and dependency.secret_alias not in target.secret_aliases
                ):
                    raise ResourceDependencyError(
                        f"Resource '{resource.name}' requires undeclared secret alias "
                        f"'{dependency.secret_alias}' from resource "
                        f"'{dependency.resource_name}'"
                    )
