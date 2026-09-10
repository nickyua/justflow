"""Shared test fixtures."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from justflow.brokers import MessagePublisher
from justflow.config.models import ServiceConfig
from justflow.transports.builtins import builtin_transport_registry
from justflow.transports.registry import (
    ConfiguredService,
    ResolvedService,
    TransportRegistry,
)

REPOSITORY_ROOT = Path(__file__).parent.parent
PRIME_STATS_CONFIG_DIR = REPOSITORY_ROOT / "examples" / "prime_stats" / "configs"
RUN_AWS_INTEGRATION_OPTION = "--run-aws-integration"


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        RUN_AWS_INTEGRATION_OPTION,
        action="store_true",
        default=False,
        help="run opt-in tests against a caller-configured AWS account",
    )


@pytest.fixture
def run_aws_integration(pytestconfig: pytest.Config) -> bool:
    return bool(pytestconfig.getoption(RUN_AWS_INTEGRATION_OPTION))


@dataclass(frozen=True, kw_only=True)
class BuiltinServiceBundle:
    declarations: Mapping[str, ServiceConfig]
    registry: TransportRegistry
    resolved: dict[str, ResolvedService]
    configured: dict[str, ConfiguredService]


def configure_builtin_services(
    declarations: Mapping[str, ServiceConfig],
    *,
    resources: Mapping[str, Any] | None = None,
    message_publishers: Mapping[str, MessagePublisher] | None = None,
    reply_destination: str | None = None,
) -> BuiltinServiceBundle:
    registry = builtin_transport_registry()
    resolved = registry.resolve_services(declarations)
    configured = registry.configure_services(
        resolved,
        resources=resources or {},
        message_publishers=message_publishers,
        reply_destination=reply_destination,
    )
    return BuiltinServiceBundle(
        declarations=declarations,
        registry=registry,
        resolved=resolved,
        configured=configured,
    )


@pytest.fixture
def configs_dir():
    return PRIME_STATS_CONFIG_DIR
