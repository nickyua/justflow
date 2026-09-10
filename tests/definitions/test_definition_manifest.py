"""Tests for canonical workflow definition identities."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

import pytest

from justflow.config.models import (
    EvaluatorCondition,
    FlowStep,
    OnResultBranch,
    ServiceConfig,
    ServiceOperationTarget,
    StepDefinition,
    WorkflowConfig,
)
from justflow.config.runtime_limits import RuntimeLimits
from justflow.definitions.manifest import (
    DefinitionManifestError,
    build_definition_manifests,
    canonical_json_bytes,
    definition_digest,
    workflow_type_name,
)
from justflow.resources.base import ResourceCapability, ResourceDependency, StrictResourceConfig
from justflow.resources.registry import ResolvedResource
from justflow.transports.builtins import builtin_transport_registry


@dataclass(frozen=True, kw_only=True)
class IdentityCase:
    id: str
    workflow_change: dict[str, Any] | None = None
    service_change: dict[str, Any] | None = None
    limit_change: dict[str, Any] | None = None
    changes_digest: bool


IDENTITY_CASES = [
    IdentityCase(
        id="description-is-definition-content",
        workflow_change={"description": "changed"},
        changes_digest=True,
    ),
    IdentityCase(
        id="retry-policy-is-deterministic",
        service_change={"retries": 3},
        changes_digest=True,
    ),
    IdentityCase(
        id="connection-deadline-is-deterministic",
        service_change={"connect_timeout_sec": 6},
        changes_digest=True,
    ),
    IdentityCase(
        id="runtime-limit-is-deterministic",
        limit_change={"fanout_chunk_items": 99},
        changes_digest=True,
    ),
    IdentityCase(
        id="endpoint-is-environment-binding",
        service_change={"transport_config": {"base_url": "https://other.example"}},
        changes_digest=False,
    ),
]


def _workflow(**changes: Any) -> WorkflowConfig:
    values: dict[str, Any] = {
        "workflow": "parent",
        "description": "base",
        "input_schema": {"type": "object"},
        "steps": {
            "call": StepDefinition(
                service="api",
                action="GET:/value",
                output_schema={"type": "object"},
            ),
        },
        "flow": [
            FlowStep(
                name="call",
                op="call",
                on_result=[
                    OnResultBranch(
                        when=EvaluatorCondition(
                            evaluator="tests.workflow_fixtures.evaluators.is_valid"
                        ),
                        then="done",
                    ),
                    OnResultBranch(default="done"),
                ],
            ),
            FlowStep(name="done", terminal=True),
        ],
    }
    values.update(changes)
    return WorkflowConfig(**values)


def _service(**changes: Any) -> ServiceConfig:
    values: dict[str, Any] = {
        "transport": "http",
        "transport_config": {"base_url": "https://api.example"},
        "connect_timeout_sec": 5,
        "dispatch_timeout_sec": 10,
        "retries": 2,
        "params": {"tenant": "public"},
    }
    values.update(changes)
    return ServiceConfig(**values)


def _manifest(
    *,
    workflow: WorkflowConfig | None = None,
    service: ServiceConfig | None = None,
    limits: RuntimeLimits | None = None,
    resources: dict[str, ResolvedResource] | None = None,
):
    services = builtin_transport_registry().resolve_services({"api": service or _service()})
    return build_definition_manifests(
        {"parent": workflow or _workflow()},
        services,
        limits or RuntimeLimits(),
        resources=resources,
    )["parent"]


@pytest.mark.parametrize("case", IDENTITY_CASES, ids=lambda case: case.id)
def test_definition_identity_boundaries(case: IdentityCase) -> None:
    baseline = _manifest()
    workflow = _workflow(**(case.workflow_change or {}))
    service = _service(**(case.service_change or {}))
    limits = replace(RuntimeLimits(), **(case.limit_change or {}))

    changed = _manifest(workflow=workflow, service=service, limits=limits)

    assert (changed.definition_digest != baseline.definition_digest) is case.changes_digest


def test_manifest_includes_contract_provider_evaluator_and_policy_identity() -> None:
    manifest = _manifest()

    assert [(provider.name, provider.version) for provider in manifest.provider_contracts] == [
        ("http", "2")
    ]
    assert [contract.location for contract in manifest.contracts] == [
        "workflow.input",
        "steps.call.output",
    ]
    assert [evaluator.path for evaluator in manifest.evaluators] == [
        "tests.workflow_fixtures.evaluators.is_valid"
    ]
    assert manifest.deterministic_policy.services[0].connect_timeout_sec == 5
    assert manifest.deterministic_policy.services[0].dispatch_timeout_sec == 10
    assert manifest.deterministic_policy.services[0].response_timeout_sec is None
    assert "base_url" not in manifest.canonical_bytes().decode()


def test_typed_step_target_preserves_manifest_v2_identity() -> None:
    legacy_manifest = _manifest()
    typed_workflow = _workflow(
        steps={
            "call": StepDefinition(
                target=ServiceOperationTarget(service="api", action="GET:/value"),
                output_schema={"type": "object"},
            )
        }
    )

    typed_manifest = _manifest(workflow=typed_workflow)

    assert typed_manifest.definition_digest == legacy_manifest.definition_digest
    assert typed_manifest.workflow["steps"]["call"] == {
        "service": "api",
        "action": "GET:/value",
        "workflow": None,
        "params": {},
        "required_resources": [],
        "cache": None,
        "input_schema": None,
        "output_schema": {"type": "object"},
    }


def test_manifest_includes_every_used_resource_provider_contract() -> None:
    workflow = _workflow(
        on_complete={
            "resource": "shared",
            "path": "audit/${request_id}.json",
            "retention_policy": "standard",
        },
        steps={
            "call": StepDefinition(
                service="api",
                action="GET:/value",
                required_resources=["shared"],
                cache={"resource": "shared", "key": "cache-key"},
            )
        },
        flow=[
            FlowStep(
                name="call",
                op="call",
                on_result=[
                    OnResultBranch(
                        when=EvaluatorCondition(
                            evaluator="tests.workflow_fixtures.evaluators.is_valid",
                            resources=["shared"],
                        ),
                        then="done",
                    ),
                    OnResultBranch(default="done"),
                ],
            ),
            FlowStep(name="done", terminal=True),
        ],
    )
    resource = ResolvedResource(
        name="shared",
        provider_name="host_resource",
        provider_contract_version="7",
        config=StrictResourceConfig(),
        capabilities=frozenset({ResourceCapability.CONFIG}),
    )

    manifest = _manifest(workflow=workflow, resources={"shared": resource})

    assert [(provider.name, provider.version) for provider in manifest.provider_contracts] == [
        ("host_resource", "7"),
        ("http", "2"),
    ]


def test_manifest_rejects_an_unresolved_resource_reference() -> None:
    workflow = _workflow(
        steps={
            "call": StepDefinition(
                service="api",
                action="GET:/value",
                required_resources=["missing"],
            )
        }
    )

    with pytest.raises(DefinitionManifestError, match="unresolved resource 'missing'"):
        _manifest(workflow=workflow)


def test_manifest_includes_transitive_resource_dependency_contracts() -> None:
    workflow = _workflow(
        steps={
            "call": StepDefinition(
                service="api",
                action="GET:/value",
                required_resources=["database"],
            )
        }
    )
    resources = {
        "database": ResolvedResource(
            name="database",
            provider_name="postgresql",
            provider_contract_version="2",
            config=StrictResourceConfig(),
            capabilities=frozenset({ResourceCapability.DATABASE}),
            dependencies=(
                ResourceDependency(
                    resource_name="platform_secrets",
                    capability=ResourceCapability.SECRET_READER,
                    secret_alias="database_credentials",
                ),
            ),
        ),
        "platform_secrets": ResolvedResource(
            name="platform_secrets",
            provider_name="aws_secrets_manager",
            provider_contract_version="1",
            config=StrictResourceConfig(),
            capabilities=frozenset({ResourceCapability.SECRET_READER}),
            secret_aliases=frozenset({"database_credentials"}),
        ),
    }

    manifest = _manifest(workflow=workflow, resources=resources)

    assert [(provider.name, provider.version) for provider in manifest.provider_contracts] == [
        ("aws_secrets_manager", "1"),
        ("http", "2"),
        ("postgresql", "2"),
    ]


def test_child_definition_change_changes_parent_identity() -> None:
    child = WorkflowConfig(
        workflow="child",
        steps={},
        flow=[FlowStep(name="done", terminal=True)],
    )
    parent = WorkflowConfig(
        workflow="parent",
        steps={"child": StepDefinition(workflow="child")},
        flow=[
            FlowStep(name="child", op="child", then="done"),
            FlowStep(name="done", terminal=True),
        ],
    )
    original = build_definition_manifests(
        {"parent": parent, "child": child},
        {},
        RuntimeLimits(),
    )
    changed_child = child.model_copy(update={"description": "new child"})
    changed = build_definition_manifests(
        {"parent": parent, "child": changed_child},
        {},
        RuntimeLimits(),
    )

    assert changed["child"].definition_digest != original["child"].definition_digest
    assert changed["parent"].definition_digest != original["parent"].definition_digest
    assert changed["parent"].children[0].definition_digest == changed["child"].definition_digest


def test_canonical_json_sorts_mappings_and_preserves_list_order() -> None:
    left = canonical_json_bytes({"b": 1, "a": [1, 2]})
    same = canonical_json_bytes({"a": [1, 2], "b": 1})
    reordered = canonical_json_bytes({"a": [2, 1], "b": 1})

    assert left == same
    assert left != reordered


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_canonical_json_rejects_non_finite_numbers(value: float) -> None:
    with pytest.raises(DefinitionManifestError, match="canonical JSON"):
        definition_digest({"value": value})


def test_workflow_type_uses_full_definition_digest() -> None:
    manifest = _manifest()

    assert workflow_type_name("parent", manifest.definition_digest) == (
        f"parent__{manifest.definition_digest}"
    )
