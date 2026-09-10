"""Render application ECS artifacts offline; infrastructure and live acceptance remain separate."""

from __future__ import annotations

import argparse
import json
import logging
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from pydantic_settings import BaseSettings, PydanticBaseSettingsSource

from justflow.config.settings import ENV_NESTED_DELIMITER, ENV_PREFIX, Settings
from justflow.scope import RuntimeScope

REFERENCE_ROOT = Path(__file__).resolve().parents[1] / "docs" / "aws" / "reference"
MAX_VALUES_FILE_BYTES = 1024 * 1024
PLACEHOLDER = re.compile(r"\$\{([A-Z][A-Z0-9_]*)\}")
SAFE_VALUE = re.compile(r"[A-Za-z0-9._:/,@+\-]+")
IMAGE_DIGEST = re.compile(r"[0-9a-f]{64}")
APPLICATION_ROLES = ("gateway", "worker")
REDIS_BINDING = "JUSTFLOW_RESOURCE_CONNECTIONS__REDIS_URLS__application"
logger = logging.getLogger(__name__)


class DeploymentRenderingError(ValueError):
    """Deployment inputs do not satisfy the application reference contract."""


class RenderedSettings(Settings):
    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (init_settings,)


def validate_environment(environment: Mapping[str, str]) -> Settings:
    document: dict[str, Any] = {}
    for name, raw in environment.items():
        if not name.startswith(ENV_PREFIX):
            continue
        parts = name.removeprefix(ENV_PREFIX).lower().split(ENV_NESTED_DELIMITER)
        target = document
        for part in parts[:-1]:
            child = target.setdefault(part, {})
            if not isinstance(child, dict):
                raise DeploymentRenderingError(
                    "Rendered settings contain conflicting nested fields"
                )
            target = child
        target[parts[-1]] = json.loads(raw) if raw.startswith(("{", "[")) else raw
    return RenderedSettings.model_validate(document)


def render_reference(
    values: Mapping[str, str],
    *,
    include_redis: bool = False,
    include_application_dynamodb: bool = False,
) -> dict[str, str]:
    if any(
        not isinstance(value, str) or SAFE_VALUE.fullmatch(value) is None
        for value in values.values()
    ):
        raise ValueError(
            "Deployment values must be nonempty portable identifiers, paths, ARNs or URLs"
        )
    scope_keys = ("TENANT_ID", "APPLICATION_ID", "ENVIRONMENT_ID")
    if any(key not in values for key in scope_keys):
        raise ValueError("Deployment values require TENANT_ID, APPLICATION_ID and ENVIRONMENT_ID")
    scope = RuntimeScope.create(
        tenant=values[scope_keys[0]],
        application=values[scope_keys[1]],
        environment=values[scope_keys[2]],
    )
    if "SCOPE_DIGEST" in values and values["SCOPE_DIGEST"] != scope.digest:
        raise ValueError("SCOPE_DIGEST does not match the configured runtime scope")
    replacements = {**values, "SCOPE_DIGEST": scope.digest}
    if IMAGE_DIGEST.fullmatch(values.get("APPLICATION_IMAGE_DIGEST", "")) is None:
        raise DeploymentRenderingError(
            "Application image digest must contain 64 lowercase hex characters"
        )
    if values.get("BUSINESS_TASK_QUEUE") == values.get("SCHEDULE_TASK_QUEUE"):
        raise DeploymentRenderingError("Business and scheduling task queues must differ")
    tasks = json.loads((REFERENCE_ROOT / "ecs-task-definitions.json").read_text())
    runtime = json.loads((REFERENCE_ROOT / "runtime-settings.json").read_text())
    policies = json.loads((REFERENCE_ROOT / "iam-policies.json").read_text())["policies"]
    if not include_redis:
        runtime["secret_injection"]["worker"].pop(REDIS_BINDING)
        for statement in policies["worker-execution-secret-injection"]["Statement"]:
            if statement["Sid"] == "ReadInjectedSecrets":
                statement["Resource"].remove("${APPLICATION_REDIS_URL_SECRET_ARN}")
    if not include_application_dynamodb:
        policies["worker"]["Statement"] = [
            statement
            for statement in policies["worker"]["Statement"]
            if statement["Sid"] != "ApplicationState"
        ]
    documents: dict[str, Any] = {"runtime-settings.json": runtime}
    for role in APPLICATION_ROLES:
        task = tasks["task_definitions"][role]
        container = task["containerDefinitions"][0]
        container["environment"] = [
            {"name": name, "value": value}
            for name, value in {**runtime["common"], **runtime[role]}.items()
        ]
        container["secrets"] = [
            {"name": name, "valueFrom": value}
            for name, value in runtime["secret_injection"][role].items()
        ]
        documents[f"{role}-task-definition.json"] = task
    documents.update({f"{role}-policy.json": policy for role, policy in policies.items()})
    serialized = {
        name: json.dumps(document, indent=2) + "\n" for name, document in documents.items()
    }
    required = {
        match.group(1) for content in serialized.values() for match in PLACEHOLDER.finditer(content)
    }
    missing = required - replacements.keys()
    if missing:
        raise ValueError("Missing deployment values: " + ", ".join(sorted(missing)))
    rendered = {
        name: PLACEHOLDER.sub(lambda match: replacements[match.group(1)], content)
        for name, content in serialized.items()
    }
    parsed_runtime = json.loads(rendered["runtime-settings.json"])
    settings = {
        role: validate_environment({**parsed_runtime["common"], **parsed_runtime[role]})
        for role in APPLICATION_ROLES
    }
    if settings["gateway"].runtime.scope != scope or settings["worker"].runtime.scope != scope:
        raise ValueError("Rendered role scopes disagree")
    if (
        settings["gateway"].temporal.task_queue != settings["worker"].temporal.task_queue
        or settings["gateway"].schedules.task_queue != settings["worker"].schedules.task_queue
    ):
        raise ValueError("Rendered role task queues disagree")
    return rendered


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--values", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--include-redis", action="store_true")
    parser.add_argument("--include-application-dynamodb", action="store_true")
    args = parser.parse_args()
    with args.values.open("rb") as source:
        raw = source.read(MAX_VALUES_FILE_BYTES + 1)
    if len(raw) > MAX_VALUES_FILE_BYTES:
        parser.error("Deployment values file exceeds its byte limit")
    values = json.loads(raw)
    if not isinstance(values, dict) or any(
        not isinstance(key, str) or not isinstance(value, str) for key, value in values.items()
    ):
        parser.error("Deployment values must be a JSON object containing string values")
    outputs = render_reference(
        values,
        include_redis=args.include_redis,
        include_application_dynamodb=args.include_application_dynamodb,
    )
    args.output.mkdir(parents=True, exist_ok=False)
    for name, content in outputs.items():
        (args.output / name).write_text(content, encoding="utf-8")
    logger.info(
        "Rendered %s application artifacts; live deployment remains unverified", len(outputs)
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
