"""Explicit registration and configuration of message brokers."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ConfigDict, ValidationError

from justflow.brokers.base import ConfiguredBroker
from justflow.config.provider_names import is_valid_contract_version, is_valid_provider_name


class StrictBrokerConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


ConfigT = TypeVar("ConfigT", bound=StrictBrokerConfig)
BrokerFactory = Callable[[ConfigT], ConfiguredBroker]


class BrokerRegistryError(Exception):
    pass


class BrokerProviderDefinitionError(BrokerRegistryError):
    pass


class DuplicateBrokerProviderError(BrokerRegistryError):
    pass


class UnknownBrokerProviderError(BrokerRegistryError):
    pass


class BrokerConfigError(BrokerRegistryError):
    pass


class BrokerFactoryError(BrokerRegistryError):
    pass


@dataclass(frozen=True, kw_only=True)
class BrokerProvider(Generic[ConfigT]):
    name: str
    contract_version: str
    config_model: type[ConfigT]
    factory: BrokerFactory[ConfigT]

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not is_valid_provider_name(self.name):
            raise BrokerProviderDefinitionError(f"Invalid broker provider name '{self.name}'")
        if not isinstance(self.contract_version, str) or not is_valid_contract_version(
            self.contract_version
        ):
            raise BrokerProviderDefinitionError(
                f"Invalid contract version '{self.contract_version}' for broker '{self.name}'"
            )
        if not isinstance(self.config_model, type) or not issubclass(
            self.config_model,
            StrictBrokerConfig,
        ):
            raise BrokerProviderDefinitionError(
                f"Broker '{self.name}' config model must extend StrictBrokerConfig"
            )
        if not callable(self.factory):
            raise BrokerProviderDefinitionError(f"Broker '{self.name}' factory must be callable")


class BrokerRegistry:
    def __init__(self) -> None:
        self._providers: dict[str, BrokerProvider[Any]] = {}

    @classmethod
    def from_builtin_providers(
        cls,
        providers: Iterable[BrokerProvider[Any]],
    ) -> BrokerRegistry:
        registry = cls()
        for provider in providers:
            registry._register(provider)
        return registry

    @property
    def providers(self) -> Mapping[str, BrokerProvider[Any]]:
        return MappingProxyType(self._providers)

    def register(self, provider: BrokerProvider[Any]) -> None:
        self._register(provider)

    def _register(self, provider: BrokerProvider[Any]) -> None:
        if provider.name in self._providers:
            raise DuplicateBrokerProviderError(
                f"Broker provider '{provider.name}' is already registered"
            )
        self._providers[provider.name] = provider

    def configure(self, provider_name: str, raw_config: Mapping[str, object]) -> ConfiguredBroker:
        try:
            provider = self._providers[provider_name]
        except KeyError as exc:
            raise UnknownBrokerProviderError(
                f"Broker provider '{provider_name}' is not registered"
            ) from exc
        try:
            config = provider.config_model.model_validate(dict(raw_config))
        except ValidationError as exc:
            raise BrokerConfigError(
                f"Broker provider '{provider_name}' has invalid configuration: {exc}"
            ) from exc
        try:
            broker = provider.factory(config)
        except Exception as exc:
            raise BrokerFactoryError(
                f"Broker provider '{provider_name}' could not be configured"
            ) from exc
        if not isinstance(broker, ConfiguredBroker):
            raise BrokerFactoryError(
                f"Broker provider '{provider_name}' returned an invalid configured broker"
            )
        return broker
