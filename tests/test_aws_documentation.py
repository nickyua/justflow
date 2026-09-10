"""Offline AWS deployment documentation contracts."""

import json
from dataclasses import dataclass
from pathlib import Path

import pytest

from scripts.verify_aws_docs import verify_aws_documentation

REFERENCE_ROOT = Path(__file__).parents[1] / "docs" / "aws" / "reference"
CONFIGURATION_TRANSACTION_ACTIONS = frozenset(
    {
        "dynamodb:GetItem",
        "dynamodb:PutItem",
        "dynamodb:UpdateItem",
        "dynamodb:DeleteItem",
        "dynamodb:ConditionCheckItem",
        "dynamodb:Query",
    }
)


def test_aws_reference_artifacts_are_valid_and_sanitized() -> None:
    verify_aws_documentation()


@pytest.mark.parametrize("role", ["gateway", "worker"])
def test_ecs_selects_the_explicit_production_host(role: str) -> None:
    tasks = json.loads((REFERENCE_ROOT / "ecs-task-definitions.json").read_text())[
        "task_definitions"
    ]
    assert (
        tasks[role]["containerDefinitions"][0]["command"][0]
        == f"host_application.production:create_{role}_application"
    )


def test_temporal_network_targets_private_self_hosted_frontends() -> None:
    flows = json.loads((REFERENCE_ROOT / "security-group-flows.json").read_text())["flows"]
    actual = {(flow["from"], flow["to"], flow["port"]) for flow in flows}
    assert {
        ("gateway", "temporal-frontend", 7233),
        ("worker", "temporal-frontend", 7233),
        ("temporal", "temporal-postgres", 5432),
    } <= actual
    assert all(flow["to"] != "temporal-cloud" for flow in flows)


@dataclass(frozen=True, kw_only=True)
class TransactionRoleCase:
    id: str
    role: str
    required_actions: frozenset[str]


TRANSACTION_ROLES = [
    TransactionRoleCase(id=role, role=role, required_actions=CONFIGURATION_TRANSACTION_ACTIONS)
    for role in ("configuration-authoring", "activation-controller", "gateway")
]


@pytest.mark.parametrize("case", TRANSACTION_ROLES, ids=lambda case: case.id)
def test_configuration_roles_authorize_transaction_member_actions(
    case: TransactionRoleCase,
) -> None:
    policies = json.loads((REFERENCE_ROOT / "iam-policies.json").read_text())["policies"]
    actions = {
        action
        for statement in policies[case.role]["Statement"]
        for action in (
            [statement["Action"]] if isinstance(statement["Action"], str) else statement["Action"]
        )
    }
    assert case.required_actions <= actions
    assert "dynamodb:TransactWriteItems" not in actions


@pytest.mark.parametrize("role", ["gateway", "worker"])
def test_runtime_can_only_write_scoped_execution_snapshots(role: str) -> None:
    policies = json.loads((REFERENCE_ROOT / "iam-policies.json").read_text())["policies"]
    statement = next(
        item
        for item in policies[role]["Statement"]
        if item["Sid"] == "WriteExecutionEnvironmentSnapshots"
    )
    assert (
        statement["Resource"]
        == "arn:aws:s3:::${DEFINITION_BUCKET}/${DEFINITION_PREFIX}/scopes/${SCOPE_DIGEST}/environments/*"
    )


@pytest.mark.parametrize("role", ["gateway", "worker"])
def test_queue_names_are_identical_for_gateway_and_worker(role: str) -> None:
    document = json.loads((REFERENCE_ROOT / "runtime-settings.json").read_text())
    settings = {**document["common"], **document[role]}
    assert settings["JUSTFLOW_TEMPORAL__TASK_QUEUE"] == "${BUSINESS_TASK_QUEUE}"
    assert settings["JUSTFLOW_SCHEDULES__TASK_QUEUE"] == "${SCHEDULE_TASK_QUEUE}"


@pytest.mark.parametrize("role", ["gateway", "worker"])
def test_initial_capacity_does_not_require_an_unpublished_metric(role: str) -> None:
    service = json.loads((REFERENCE_ROOT / "service-configuration.json").read_text())["services"][
        role
    ]
    assert service["autoscaling"] == []
    assert service["desired_tasks"] == service["minimum_tasks"] == service["maximum_tasks"]
    assert service["alarms"]
