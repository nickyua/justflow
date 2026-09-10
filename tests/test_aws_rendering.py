"""Render real AWS fixtures and validate them against the production Settings schema."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass

import pytest

from justflow.scope import RuntimeScope
from scripts.render_aws_reference import REFERENCE_ROOT, render_reference, validate_environment


def deployment_values() -> dict[str, str]:
    placeholders = {
        name
        for path in REFERENCE_ROOT.glob("*.json")
        for name in re.findall(r"\$\{([A-Z][A-Z0-9_]*)\}", path.read_text())
        if name != "SCOPE_DIGEST"
    }
    return {
        **dict.fromkeys(placeholders, "synthetic"),
        "AWS_ACCOUNT_ID": "000000000000",
        "AWS_REGION": "eu-central-1",
        "APPLICATION_IMAGE_DIGEST": "a" * 64,
        "JUSTFLOW_VERSION": "0.1.0",
        "SOURCE_REVISION": "b" * 40,
        "BUSINESS_TASK_QUEUE": "business",
        "SCHEDULE_TASK_QUEUE": "scheduling",
        "TEMPORAL_ROOT_CA_PATH": "/opt/justflow/trust/temporal-ca.pem",
        "TEMPORAL_SERVER_NAME": "temporal.example.test",
        "TEMPORAL_ENDPOINT": "temporal.example.test",
        "APPLICATION_DOMAIN": "dashboard.example.test",
        "ALB_SUBNET_CIDRS": "10.0.0.0/24,10.0.1.0/24",
    }


@pytest.mark.parametrize(
    "include_optional", [False, True], ids=["first-project", "optional-providers"]
)
def test_rendered_production_roles_agree_and_omit_unselected_secrets(
    include_optional: bool,
) -> None:
    values = deployment_values()
    rendered = render_reference(
        values, include_redis=include_optional, include_application_dynamodb=include_optional
    )
    gateway = json.loads(rendered["gateway-task-definition.json"])["containerDefinitions"][0]
    worker = json.loads(rendered["worker-task-definition.json"])["containerDefinitions"][0]
    configured = validate_environment(
        {entry["name"]: entry["value"] for entry in gateway["environment"]}
    )
    assert configured.runtime.scope == RuntimeScope.create(
        tenant=values["TENANT_ID"],
        application=values["APPLICATION_ID"],
        environment=values["ENVIRONMENT_ID"],
    )
    assert all("${" not in content for content in rendered.values())
    assert gateway["command"][0] == "host_application.production:create_gateway_application"
    assert any(entry["name"] == "HOST_APPLICATION_CREDENTIALS" for entry in gateway["secrets"])
    assert not any(entry["name"] == "HOST_APPLICATION_CREDENTIALS" for entry in worker["secrets"])
    assert any("REDIS" in entry["name"] for entry in worker["secrets"]) is include_optional
    assert ("ApplicationState" in rendered["worker-policy.json"]) is include_optional


@dataclass(frozen=True, kw_only=True)
class Raises:
    exc: type[Exception]
    match: str


@dataclass(frozen=True, kw_only=True)
class InvalidDeploymentCase:
    id: str
    changes: tuple[tuple[str, str], ...]
    outcome: Raises


INVALID_DEPLOYMENTS = [
    InvalidDeploymentCase(
        id="scope-digest",
        changes=(("SCOPE_DIGEST", "different"),),
        outcome=Raises(exc=ValueError, match="SCOPE_DIGEST"),
    ),
    InvalidDeploymentCase(
        id="json-injection",
        changes=(("TENANT_ID", 'tenant"},"admin":true'),),
        outcome=Raises(exc=ValueError, match="portable"),
    ),
    InvalidDeploymentCase(
        id="invalid-artifact",
        changes=(("APPLICATION_IMAGE_DIGEST", "not-a-digest"),),
        outcome=Raises(exc=ValueError, match="digest"),
    ),
]


@pytest.mark.parametrize("case", INVALID_DEPLOYMENTS, ids=lambda c: c.id)
def test_invalid_deployment_values_fail_before_any_output(case: InvalidDeploymentCase) -> None:
    with pytest.raises(case.outcome.exc, match=case.outcome.match):
        render_reference({**deployment_values(), **dict(case.changes)})


def test_rendering_does_not_inherit_an_operators_runtime_settings(monkeypatch) -> None:
    monkeypatch.setenv("JUSTFLOW_RUNTIME__PROFILE", "local")
    monkeypatch.setenv("JUSTFLOW_TEMPORAL__CONNECTION", '{"mode":"local_plaintext"}')
    outputs = render_reference(deployment_values())
    assert '"value": "production"' in outputs["gateway-task-definition.json"]
