"""Explicit registration and startup resolution for transport providers."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Generic, TypeVar

from pydantic import ValidationError

from justflow.brokers.base import MessagePublisher
from justflow.config.models import ServiceConfig
from justflow.config.provider_names import (
    is_valid_contract_version,
    is_valid_provider_name,
)
from justflow.transports.base import (
    ConfiguredTransport,
    DeadlineMode,
    DeadlineRequirements,
    StrictTransportConfig,
    TransportFactoryContext,
)
from justflow.transports.security import TransportSecuritySettings

ConfigT = TypeVar("ConfigT", bound=StrictTransportConfig)
TransportFactory = Callable[[ConfigT, TransportFactoryContext], ConfiguredTransport]
TransportActionValidator = Callable[[str], str | None]
TransportStartupValidator = Callable[[ConfigT], str | None]


class TransportRegistryError(Exception):
    pass


class TransportProviderDefinitionError(TransportRegistryError):
    pass


class DuplicateTransportProviderError(TransportRegistryError):
    pass


class UnknownTransportProviderError(TransportRegistryError):
    pass


class TransportConfigError(TransportRegistryError):
    pass


class TransportFactoryError(TransportRegistryError):
    pass


class TransportProviderValidationError(TransportRegistryError):
    pass


class UnsupportedAsyncTransportError(TransportRegistryError):
    pass


@dataclass(frozen=True, kw_only=True)
class TransportProvider(Generic[ConfigT]):
    name: str
    contract_version: str
    config_model: type[ConfigT]
    factory: TransportFactory[ConfigT]
    supports_async_response: bool = False
    action_validator: TransportActionValidator | None = None
    startup_validator: TransportStartupValidator[ConfigT] | None = None
    deadline_requirements: DeadlineRequirements = field(default_factory=DeadlineRequirements)

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not is_valid_provider_name(self.name):
            raise TransportProviderDefinitionError(f"Invalid transport provider name '{self.name}'")
        if not isinstance(self.contract_version, str) or not is_valid_contract_version(
            self.contract_version
        ):
            raise TransportProviderDefinitionError(
                f"Invalid contract version '{self.contract_version}' for provider '{self.name}'"
            )
        if not isinstance(self.config_model, type) or not issubclass(
            self.config_model,
            StrictTransportConfig,
        ):
            raise TransportProviderDefinitionError(
                f"Provider '{self.name}' config model must extend StrictTransportConfig"
            )
        if not callable(self.factory):
            raise TransportProviderDefinitionError(
                f"Provider '{self.name}' factory must be callable"
            )
        if not isinstance(self.supports_async_response, bool):
            raise TransportProviderDefinitionError(
                f"Provider '{self.name}' async-response capability must be boolean"
            )
        for validator in (self.action_validator, self.startup_validator):
            if validator is not None and not callable(validator):
                raise TransportProviderDefinitionError(
                    f"Provider '{self.name}' validators must be callable"
                )

    def parse_config(self, value: Mapping[str, object]) -> StrictTransportConfig:
        return self.config_model.model_validate(dict(value))

    def build(
        self,
        config: StrictTransportConfig,
        context: TransportFactoryContext,
    ) -> ConfiguredTransport:
        if not isinstance(config, self.config_model):
            raise TransportFactoryError(
                f"Provider '{self.name}' received config type "
                f"'{type(config).__name__}', expected '{self.config_model.__name__}'"
            )
        return self.factory(config, context)

    def validate_action(self, action: str) -> str | None:
        if self.action_validator is None:
            return None
        return self.action_validator(action)

    def validate_startup(self, config: StrictTransportConfig) -> str | None:
        if self.startup_validator is None:
            return None
        if not isinstance(config, self.config_model):
            return (
                f"provider config type is '{type(config).__name__}', "
                f"expected '{self.config_model.__name__}'"
            )
        return self.startup_validator(config)


@dataclass(frozen=True, kw_only=True)
class ResolvedService:
    name: str
    provider_name: str
    provider_contract_version: str
    transport_config: StrictTransportConfig
    connect_timeout_sec: int | None
    dispatch_timeout_sec: int
    response_timeout_sec: int | None
    retries: int
    params: Mapping[str, object]


@dataclass(frozen=True, kw_only=True)
class ConfiguredService:
    definition: ResolvedService
    transport: ConfiguredTransport
    supports_async_response: bool


class TransportRegistry:
    def __init__(self) -> None:
        self._providers: dict[str, TransportProvider[Any]] = {}

    @classmethod
    def from_builtin_providers(
        cls,
        providers: Iterable[TransportProvider[Any]],
    ) -> TransportRegistry:
        registry = cls()
        for provider in providers:
            registry._register(provider, allow_async=True)
        return registry

    @property
    def providers(self) -> Mapping[str, TransportProvider[Any]]:
        return MappingProxyType(self._providers)

    def register(self, provider: TransportProvider[Any]) -> None:
        self._register(provider, allow_async=False)

    def _register(
        self,
        provider: TransportProvider[Any],
        *,
        allow_async: bool,
    ) -> None:
        if provider.name in self._providers:
            raise DuplicateTransportProviderError(
                f"Transport provider '{provider.name}' is already registered"
            )
        if provider.supports_async_response and not allow_async:
            raise UnsupportedAsyncTransportError(
                f"Host transport provider '{provider.name}' cannot use asynchronous responses"
            )
        self._providers[provider.name] = provider

    def resolve_services(
        self,
        services: Mapping[str, ServiceConfig],
    ) -> dict[str, ResolvedService]:
        return {
            name: self.resolve_service(name, declaration) for name, declaration in services.items()
        }

    def resolve_service(
        self,
        name: str,
        declaration: ServiceConfig,
    ) -> ResolvedService:
        provider = self._provider(declaration.transport)
        try:
            config = provider.parse_config(declaration.transport_config)
        except ValidationError as exc:
            raise TransportConfigError(
                f"Service '{name}' has invalid config for transport provider "
                f"'{provider.name}': {exc}"
            ) from exc
        except Exception as exc:
            raise TransportConfigError(
                f"Transport provider '{provider.name}' could not validate "
                f"configuration for service '{name}'"
            ) from exc
        self._validate_deadlines(name, declaration, provider.deadline_requirements)
        return ResolvedService(
            name=name,
            provider_name=provider.name,
            provider_contract_version=provider.contract_version,
            transport_config=config,
            connect_timeout_sec=declaration.connect_timeout_sec,
            dispatch_timeout_sec=declaration.dispatch_timeout_sec,
            response_timeout_sec=declaration.response_timeout_sec,
            retries=declaration.retries,
            params=MappingProxyType(dict(declaration.params)),
        )

    def configure_services(
        self,
        services: Mapping[str, ResolvedService],
        *,
        resources: Mapping[str, object],
        message_publishers: Mapping[str, MessagePublisher] | None = None,
        reply_destination: str | None = None,
        security: TransportSecuritySettings | None = None,
    ) -> dict[str, ConfiguredService]:
        configured: dict[str, ConfiguredService] = {}
        for name, service in services.items():
            provider = self._provider(service.provider_name)
            context = TransportFactoryContext(
                resources=resources,
                connect_timeout_sec=service.connect_timeout_sec,
                dispatch_timeout_sec=service.dispatch_timeout_sec,
                response_timeout_sec=service.response_timeout_sec,
                security=security or TransportSecuritySettings(),
                message_publishers=message_publishers or {},
                reply_destination=reply_destination,
            )
            try:
                transport = provider.build(service.transport_config, context)
            except TransportFactoryError:
                raise
            except Exception as exc:
                raise TransportFactoryError(
                    f"Transport provider '{provider.name}' failed to configure service '{name}'"
                ) from exc
            if not isinstance(transport, ConfiguredTransport):
                raise TransportFactoryError(
                    f"Transport provider '{provider.name}' returned an invalid "
                    f"configured transport for service '{name}'"
                )
            configured[name] = ConfiguredService(
                definition=service,
                transport=transport,
                supports_async_response=provider.supports_async_response,
            )
        return configured

    @staticmethod
    def _validate_deadlines(
        service_name: str,
        declaration: ServiceConfig,
        requirements: DeadlineRequirements,
    ) -> None:
        fields = (
            (
                "connect_timeout_sec",
                declaration.connect_timeout_sec,
                requirements.connect,
            ),
            (
                "response_timeout_sec",
                declaration.response_timeout_sec,
                requirements.response,
            ),
        )
        for field_name, value, mode in fields:
            if mode is DeadlineMode.REQUIRED and value is None:
                raise TransportConfigError(
                    f"Service '{service_name}' requires '{field_name}' for its transport"
                )
            if mode is DeadlineMode.FORBIDDEN and value is not None:
                raise TransportConfigError(
                    f"Service '{service_name}' cannot set '{field_name}' for its transport"
                )
        if (
            declaration.connect_timeout_sec is not None
            and declaration.connect_timeout_sec > declaration.dispatch_timeout_sec
        ):
            raise TransportConfigError(
                f"Service '{service_name}' connection deadline cannot exceed its dispatch deadline"
            )

    def validate_action(self, service: ResolvedService, action: str) -> str | None:
        provider = self._provider(service.provider_name)
        try:
            return provider.validate_action(action)
        except Exception as exc:
            raise TransportProviderValidationError(
                f"Transport provider '{provider.name}' could not validate action "
                f"'{action}' for service '{service.name}'"
            ) from exc

    def validate_startup(self, service: ResolvedService) -> str | None:
        provider = self._provider(service.provider_name)
        try:
            return provider.validate_startup(service.transport_config)
        except Exception as exc:
            raise TransportProviderValidationError(
                f"Transport provider '{provider.name}' could not validate startup "
                f"configuration for service '{service.name}'"
            ) from exc

    def _provider(self, name: str) -> TransportProvider[Any]:
        try:
            return self._providers[name]
        except KeyError as exc:
            raise UnknownTransportProviderError(
                f"Transport provider '{name}' is not registered"
            ) from exc
