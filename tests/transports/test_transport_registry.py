"""Tests for explicit transport-provider registration and startup resolution."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest
from pydantic import Field

from justflow.config.models import ServiceConfig
from justflow.transports.base import (
    Completed,
    StrictTransportConfig,
    TransportFactoryContext,
    TransportRequest,
)
from justflow.transports.builtins import builtin_transport_registry
from justflow.transports.registry import (
    DuplicateTransportProviderError,
    TransportConfigError,
    TransportFactoryError,
    TransportProvider,
    TransportProviderDefinitionError,
    TransportProviderValidationError,
    TransportRegistry,
    UnknownTransportProviderError,
    UnsupportedAsyncTransportError,
)

SERVICE_TIMEOUT_SECONDS = 15


class EchoConfig(StrictTransportConfig):
    prefix: str = Field(min_length=1)


class EchoTransport:
    def __init__(self, config: EchoConfig):
        self._config = config

    async def send(self, request: TransportRequest) -> Completed:
        return Completed(data=f"{self._config.prefix}:{request.input}")

    async def close(self) -> None:
        return None


def build_echo(
    config: EchoConfig,
    context: TransportFactoryContext,
) -> EchoTransport:
    del context
    return EchoTransport(config)


def echo_provider(
    *,
    name: str = "test.echo",
    contract_version: str = "1",
    config_model: Any = EchoConfig,
    supports_async_response: bool = False,
) -> TransportProvider[EchoConfig]:
    return TransportProvider(
        name=name,
        contract_version=contract_version,
        config_model=config_model,
        factory=build_echo,
        supports_async_response=supports_async_response,
    )


@dataclass(frozen=True, kw_only=True)
class DefinitionReturns:
    name: str


@dataclass(frozen=True, kw_only=True)
class DefinitionRaises:
    exc: type[Exception]
    match: str


DefinitionOutcome = DefinitionReturns | DefinitionRaises


@dataclass(frozen=True, kw_only=True)
class ProviderDefinitionCase:
    id: str
    name: str
    contract_version: str
    config_model: Any
    outcome: DefinitionOutcome


PROVIDER_DEFINITION_CASES = [
    ProviderDefinitionCase(
        id="valid-open-name",
        name="acme.echo-v2",
        contract_version="2026.1",
        config_model=EchoConfig,
        outcome=DefinitionReturns(name="acme.echo-v2"),
    ),
    ProviderDefinitionCase(
        id="uppercase-name",
        name="AcmeEcho",
        contract_version="1",
        config_model=EchoConfig,
        outcome=DefinitionRaises(
            exc=TransportProviderDefinitionError,
            match="Invalid transport provider name",
        ),
    ),
    ProviderDefinitionCase(
        id="invalid-contract-version",
        name="acme.echo",
        contract_version="version one",
        config_model=EchoConfig,
        outcome=DefinitionRaises(
            exc=TransportProviderDefinitionError,
            match="Invalid contract version",
        ),
    ),
    ProviderDefinitionCase(
        id="invalid-config-model",
        name="acme.echo",
        contract_version="1",
        config_model=object,
        outcome=DefinitionRaises(
            exc=TransportProviderDefinitionError,
            match="must extend StrictTransportConfig",
        ),
    ),
]


@pytest.mark.parametrize(
    "case",
    PROVIDER_DEFINITION_CASES,
    ids=lambda case: case.id,
)
def test_provider_definition(case: ProviderDefinitionCase) -> None:
    if isinstance(case.outcome, DefinitionRaises):
        with pytest.raises(case.outcome.exc, match=case.outcome.match):
            echo_provider(
                name=case.name,
                contract_version=case.contract_version,
                config_model=case.config_model,
            )
        return

    provider = echo_provider(
        name=case.name,
        contract_version=case.contract_version,
        config_model=case.config_model,
    )
    assert provider.name == case.outcome.name


def test_registry_rejects_duplicate_provider_names() -> None:
    registry = TransportRegistry()
    registry.register(echo_provider())

    with pytest.raises(DuplicateTransportProviderError, match="already registered"):
        registry.register(echo_provider())


def test_host_provider_cannot_enable_async_responses() -> None:
    registry = TransportRegistry()

    with pytest.raises(UnsupportedAsyncTransportError, match="cannot use asynchronous"):
        registry.register(echo_provider(supports_async_response=True))


def test_builtin_providers_use_the_registry_boundary() -> None:
    assert set(builtin_transport_registry().providers) == {
        "direct",
        "grpc",
        "http",
        "lambda",
        "queue",
    }


@dataclass(frozen=True, kw_only=True)
class DeadlineReturns:
    connect_timeout_sec: int | None
    response_timeout_sec: int | None


@dataclass(frozen=True, kw_only=True)
class DeadlineRaises:
    exc: type[Exception]
    match: str


DeadlineOutcome = DeadlineReturns | DeadlineRaises


@dataclass(frozen=True, kw_only=True)
class DeadlineCase:
    id: str
    transport: str
    transport_config: dict[str, object]
    connect_timeout_sec: int | None
    response_timeout_sec: int | None
    outcome: DeadlineOutcome


DEADLINE_CASES = [
    DeadlineCase(
        id="direct-dispatch-only",
        transport="direct",
        transport_config={"class": "tests.workflow_fixtures.actions.fetch_record.FetchRecord"},
        connect_timeout_sec=None,
        response_timeout_sec=None,
        outcome=DeadlineReturns(connect_timeout_sec=None, response_timeout_sec=None),
    ),
    DeadlineCase(
        id="direct-rejects-connect",
        transport="direct",
        transport_config={"class": "tests.workflow_fixtures.actions.fetch_record.FetchRecord"},
        connect_timeout_sec=2,
        response_timeout_sec=None,
        outcome=DeadlineRaises(exc=TransportConfigError, match="cannot set.*connect"),
    ),
    DeadlineCase(
        id="http-requires-connect",
        transport="http",
        transport_config={"base_url": "https://service.example"},
        connect_timeout_sec=None,
        response_timeout_sec=None,
        outcome=DeadlineRaises(exc=TransportConfigError, match="requires.*connect"),
    ),
    DeadlineCase(
        id="http-separates-connect",
        transport="http",
        transport_config={"base_url": "https://service.example"},
        connect_timeout_sec=2,
        response_timeout_sec=None,
        outcome=DeadlineReturns(connect_timeout_sec=2, response_timeout_sec=None),
    ),
    DeadlineCase(
        id="connect-cannot-exceed-dispatch",
        transport="http",
        transport_config={"base_url": "https://service.example"},
        connect_timeout_sec=SERVICE_TIMEOUT_SECONDS + 1,
        response_timeout_sec=None,
        outcome=DeadlineRaises(exc=TransportConfigError, match="cannot exceed.*dispatch"),
    ),
    DeadlineCase(
        id="queue-requires-response",
        transport="queue",
        transport_config={
            "broker": "main",
            "destination": "requests",
            "idempotency": "durable",
        },
        connect_timeout_sec=None,
        response_timeout_sec=None,
        outcome=DeadlineRaises(exc=TransportConfigError, match="requires.*response"),
    ),
    DeadlineCase(
        id="queue-separates-response",
        transport="queue",
        transport_config={
            "broker": "main",
            "destination": "requests",
            "idempotency": "durable",
        },
        connect_timeout_sec=None,
        response_timeout_sec=60,
        outcome=DeadlineReturns(connect_timeout_sec=None, response_timeout_sec=60),
    ),
]


@pytest.mark.parametrize("case", DEADLINE_CASES, ids=lambda case: case.id)
def test_provider_deadline_contract(case: DeadlineCase) -> None:
    declaration = ServiceConfig(
        transport=case.transport,
        transport_config=case.transport_config,
        connect_timeout_sec=case.connect_timeout_sec,
        dispatch_timeout_sec=SERVICE_TIMEOUT_SECONDS,
        response_timeout_sec=case.response_timeout_sec,
        retries=0,
    )

    if isinstance(case.outcome, DeadlineRaises):
        with pytest.raises(case.outcome.exc, match=case.outcome.match):
            builtin_transport_registry().resolve_service("service", declaration)
        return

    resolved = builtin_transport_registry().resolve_service("service", declaration)
    assert resolved.connect_timeout_sec == case.outcome.connect_timeout_sec
    assert resolved.response_timeout_sec == case.outcome.response_timeout_sec


def test_unknown_provider_is_rejected_during_resolution() -> None:
    service = ServiceConfig(
        transport="unknown",
        transport_config={},
        dispatch_timeout_sec=SERVICE_TIMEOUT_SECONDS,
        retries=0,
    )

    with pytest.raises(UnknownTransportProviderError, match="is not registered"):
        TransportRegistry().resolve_service("service", service)


@dataclass(frozen=True, kw_only=True)
class ConfigReturns:
    prefix: str


@dataclass(frozen=True, kw_only=True)
class ConfigRaises:
    exc: type[Exception]
    match: str


ConfigOutcome = ConfigReturns | ConfigRaises


@dataclass(frozen=True, kw_only=True)
class ProviderConfigCase:
    id: str
    value: dict[str, object]
    outcome: ConfigOutcome


PROVIDER_CONFIG_CASES = [
    ProviderConfigCase(
        id="valid",
        value={"prefix": "echo"},
        outcome=ConfigReturns(prefix="echo"),
    ),
    ProviderConfigCase(
        id="missing-required-field",
        value={},
        outcome=ConfigRaises(exc=TransportConfigError, match="prefix"),
    ),
    ProviderConfigCase(
        id="unknown-field",
        value={"prefix": "echo", "unexpected": True},
        outcome=ConfigRaises(exc=TransportConfigError, match="unexpected"),
    ),
]


@pytest.mark.parametrize(
    "case",
    PROVIDER_CONFIG_CASES,
    ids=lambda case: case.id,
)
def test_provider_owned_config_is_strict(case: ProviderConfigCase) -> None:
    registry = TransportRegistry()
    registry.register(echo_provider())
    declaration = ServiceConfig(
        transport="test.echo",
        transport_config=case.value,
        dispatch_timeout_sec=SERVICE_TIMEOUT_SECONDS,
        retries=0,
    )

    if isinstance(case.outcome, ConfigRaises):
        with pytest.raises(case.outcome.exc, match=case.outcome.match):
            registry.resolve_service("service", declaration)
        return

    resolved = registry.resolve_service("service", declaration)
    assert isinstance(resolved.transport_config, EchoConfig)
    assert resolved.transport_config.prefix == case.outcome.prefix


def test_factory_exception_is_contained_by_registry_taxonomy() -> None:
    def broken_factory(
        config: EchoConfig,
        context: TransportFactoryContext,
    ) -> EchoTransport:
        del config, context
        raise KeyError("implementation detail")

    registry = TransportRegistry()
    registry.register(
        TransportProvider(
            name="test.broken",
            contract_version="1",
            config_model=EchoConfig,
            factory=broken_factory,
        )
    )
    resolved = registry.resolve_services(
        {
            "service": ServiceConfig(
                transport="test.broken",
                transport_config={"prefix": "echo"},
                dispatch_timeout_sec=SERVICE_TIMEOUT_SECONDS,
                retries=0,
            )
        }
    )

    with pytest.raises(
        TransportFactoryError,
        match="failed to configure service",
    ) as exc_info:
        registry.configure_services(resolved, resources={})

    assert "implementation detail" not in str(exc_info.value)
    assert isinstance(exc_info.value.__cause__, KeyError)


def test_action_validator_exception_is_contained_by_registry_taxonomy() -> None:
    def broken_validator(action: str) -> str | None:
        del action
        raise KeyError("implementation detail")

    registry = TransportRegistry()
    registry.register(
        TransportProvider(
            name="test.broken",
            contract_version="1",
            config_model=EchoConfig,
            factory=build_echo,
            action_validator=broken_validator,
        )
    )
    service = registry.resolve_service(
        "service",
        ServiceConfig(
            transport="test.broken",
            transport_config={"prefix": "echo"},
            dispatch_timeout_sec=SERVICE_TIMEOUT_SECONDS,
            retries=0,
        ),
    )

    with pytest.raises(TransportProviderValidationError, match="could not validate action"):
        registry.validate_action(service, "send")


def test_startup_validator_exception_is_contained_by_registry_taxonomy() -> None:
    def broken_validator(config: EchoConfig) -> str | None:
        del config
        raise KeyError("implementation detail")

    registry = TransportRegistry()
    registry.register(
        TransportProvider(
            name="test.broken",
            contract_version="1",
            config_model=EchoConfig,
            factory=build_echo,
            startup_validator=broken_validator,
        )
    )
    service = registry.resolve_service(
        "service",
        ServiceConfig(
            transport="test.broken",
            transport_config={"prefix": "echo"},
            dispatch_timeout_sec=SERVICE_TIMEOUT_SECONDS,
            retries=0,
        ),
    )

    with pytest.raises(TransportProviderValidationError, match="could not validate startup"):
        registry.validate_startup(service)
