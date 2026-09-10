"""Shared release contracts for every maintained example package."""

from __future__ import annotations

import tomllib
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pytest
from host_application.resources import create_resource_registry

from justflow.config.loader import ConfigLoader
from justflow.config.triggers import TriggerKind
from justflow.config.validator import ConfigValidator
from justflow.resources import ResourceRegistry, builtin_resource_registry


@dataclass(frozen=True, kw_only=True)
class MaintainedExampleCase:
    id: str
    root: Path
    config_dir: Path
    distribution_name: str
    workflows: frozenset[str]
    trigger_kinds: dict[str, TriggerKind]
    resource_registry_factory: Callable[[], ResourceRegistry] = builtin_resource_registry


CASES = [
    MaintainedExampleCase(
        id="prime-statistics",
        root=Path("examples/prime_stats"),
        config_dir=Path("examples/prime_stats/configs"),
        distribution_name="justflow-example-prime-stats",
        workflows=frozenset({"prime_stats"}),
        trigger_kinds={"prime_stats_api": TriggerKind.API},
    ),
    MaintainedExampleCase(
        id="product-onboarding",
        root=Path("examples/product_onboarding"),
        config_dir=Path("examples/product_onboarding/configs"),
        distribution_name="justflow-example-product-onboarding",
        workflows=frozenset({"product_onboarding", "provision_workspace"}),
        trigger_kinds={"product_onboarding_api": TriggerKind.API},
    ),
    MaintainedExampleCase(
        id="scheduled-reporting",
        root=Path("examples/scheduled_reporting"),
        config_dir=Path("examples/scheduled_reporting/configs"),
        distribution_name="justflow-example-scheduled-reporting",
        workflows=frozenset({"scheduled_reporting"}),
        trigger_kinds={"daily_reporting": TriggerKind.SCHEDULE},
    ),
    MaintainedExampleCase(
        id="object-ingestion",
        root=Path("examples/object_ingestion"),
        config_dir=Path("examples/object_ingestion/configs"),
        distribution_name="justflow-example-object-ingestion",
        workflows=frozenset({"object_ingestion"}),
        trigger_kinds={"object_created": TriggerKind.EVENT},
    ),
    MaintainedExampleCase(
        id="host-application",
        root=Path("examples/host_application"),
        config_dir=Path("examples/host_application/src/host_application/configs"),
        distribution_name="justflow-host-application-example",
        workflows=frozenset({"hello"}),
        trigger_kinds={"hello_api": TriggerKind.API},
        resource_registry_factory=create_resource_registry,
    ),
]


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.id)
def test_maintained_example_has_valid_configuration_and_package(
    case: MaintainedExampleCase,
) -> None:
    loader = ConfigLoader(case.config_dir)
    resources, services, workflows = loader.load_all()
    triggers = loader.load_triggers()
    validator = ConfigValidator(
        resources,
        services,
        workflows,
        resource_registry=case.resource_registry_factory(),
        config_dir=case.config_dir,
        workflow_sources=loader.workflow_sources,
        triggers=triggers,
    )

    validator.validate().raise_if_invalid()
    package = tomllib.loads((case.root / "pyproject.toml").read_text())
    readme = (case.root / "README.md").read_text()

    assert frozenset(workflows) == case.workflows
    assert {
        name: declaration.kind for name, declaration in triggers.triggers.items()
    } == case.trigger_kinds
    assert package["project"]["name"] == case.distribution_name
    assert package["project"]["version"] == "0.1.1"
    assert "justflow" in package["project"]["dependencies"][0]
    assert "install" in readme.lower()


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.id)
def test_maintained_example_configuration_contains_no_secret_material(
    case: MaintainedExampleCase,
) -> None:
    configuration = "\n".join(
        path.read_text() for path in sorted(case.config_dir.rglob("*.yaml"))
    ).lower()

    assert "password:" not in configuration
    assert "credential:" not in configuration
    assert "secret_access_key:" not in configuration
    assert "private_key:" not in configuration
