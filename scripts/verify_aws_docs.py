"""Validate sanitized AWS deployment reference artifacts offline."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

import yaml

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
REFERENCE_ROOT = REPOSITORY_ROOT / "docs" / "aws" / "reference"
REFERENCE_VERSION = "0.1.0"
REFERENCE_FILE_NAMES = (
    "ecs-task-definitions.json",
    "data-services.json",
    "iam-policies.json",
    "runtime-settings.json",
    "security-group-flows.json",
    "service-configuration.json",
)
RESOURCE_DECLARATIONS_FILE_NAME = "application-resources.yaml"
TOPOLOGY_FILE_NAME = "production-topology.svg"
SANITIZED_REFERENCE_FILE_NAMES = REFERENCE_FILE_NAMES + (
    RESOURCE_DECLARATIONS_FILE_NAME,
    TOPOLOGY_FILE_NAME,
)
EXPECTED_TASK_ROLES = frozenset({"gateway", "worker"})
EXPECTED_IAM_POLICIES = frozenset(
    {
        "configuration-authoring",
        "activation-controller",
        "gateway",
        "worker",
        "gateway-execution-secret-injection",
        "worker-execution-secret-injection",
        "migration-execution-secret-injection",
    }
)
REQUIRED_POLICY_SERVICES = {
    "configuration-authoring": frozenset({"dynamodb", "kms", "s3"}),
    "activation-controller": frozenset({"dynamodb", "ecs", "kms", "s3"}),
    "gateway": frozenset({"dynamodb", "kms", "s3", "sqs"}),
    "worker": frozenset({"dynamodb", "kms", "s3", "sqs", "ssm"}),
    "gateway-execution-secret-injection": frozenset({"kms", "secretsmanager"}),
    "worker-execution-secret-injection": frozenset({"kms", "secretsmanager"}),
    "migration-execution-secret-injection": frozenset({"kms", "secretsmanager"}),
}
EXPECTED_ROLE_COMMANDS = {
    "gateway": "host_application.production:create_gateway_application",
    "worker": "host_application.production:create_worker_application",
}
PLACEHOLDER_PATTERN = re.compile(r"\$\{([A-Z][A-Z0-9_]*)\}")
IMAGE_PATTERN = re.compile(
    r"^\$\{AWS_ACCOUNT_ID\}\.dkr\.ecr\.\$\{AWS_REGION\}\.amazonaws\.com/"
    r"\$\{APPLICATION_NAME\}@sha256:\$\{APPLICATION_IMAGE_DIGEST\}$"
)
AWS_ACCESS_KEY_PATTERN = re.compile(r"AKIA[0-9A-Z]{16}")
AWS_ACCOUNT_ID_PATTERN = re.compile(r"(?<!\d)\d{12}(?!\d)")
AWS_REGION_PATTERN = re.compile(
    r"\b(?:af|ap|ca|eu|il|me|sa|us)-(?:central|east|north|northeast|south|southeast|"
    r"southwest|west)-\d\b"
)
MINIMUM_SERVICE_TASKS = 2
MINIMUM_DRAIN_SECONDS = 60
PUBLIC_TLS_PORT = 443
GATEWAY_PORT = 8080
TEMPORAL_PORT = 7233
REDIS_PORT = 6379
POSTGRES_PORT = 5432
READY_PATH = "/readyz"
TEMPORAL_SECRET_NAME = "JUSTFLOW_TEMPORAL__CONNECTION__API_KEY"
POSTGRES_SECRET_NAME = "JUSTFLOW_RESOURCE_CONNECTIONS__POSTGRES_DSNS__application"
REDIS_SECRET_NAME = "JUSTFLOW_RESOURCE_CONNECTIONS__REDIS_URLS__application"
GATEWAY_TEMPORAL_SECRET_ARN = "${GATEWAY_TEMPORAL_TOKEN_SECRET_ARN}"
WORKER_TEMPORAL_SECRET_ARN = "${WORKER_TEMPORAL_TOKEN_SECRET_ARN}"
HOST_CREDENTIALS_SECRET_NAME = "HOST_APPLICATION_CREDENTIALS"
HOST_CREDENTIALS_SECRET_ARN = "${HOST_CREDENTIALS_SECRET_ARN}"
POSTGRES_SECRET_ARN = "${APPLICATION_POSTGRES_DSN_SECRET_ARN}"
REDIS_SECRET_ARN = "${APPLICATION_REDIS_URL_SECRET_ARN}"
MIGRATION_SECRET_ARN = "${MIGRATION_POSTGRES_DSN_SECRET_ARN}"
EXPECTED_ROLE_SECRETS = {
    "gateway": {
        TEMPORAL_SECRET_NAME: GATEWAY_TEMPORAL_SECRET_ARN,
        HOST_CREDENTIALS_SECRET_NAME: HOST_CREDENTIALS_SECRET_ARN,
    },
    "worker": {
        TEMPORAL_SECRET_NAME: WORKER_TEMPORAL_SECRET_ARN,
        POSTGRES_SECRET_NAME: POSTGRES_SECRET_ARN,
        REDIS_SECRET_NAME: REDIS_SECRET_ARN,
    },
}
EXPECTED_EXECUTION_ROLE_ARNS = {
    role: f"arn:aws:iam::${{AWS_ACCOUNT_ID}}:role/${{APPLICATION_NAME}}-{role}-execution"
    for role in EXPECTED_TASK_ROLES
}
EXPECTED_EXECUTION_POLICY_SECRET_RESOURCES = {
    "gateway-execution-secret-injection": frozenset(
        {GATEWAY_TEMPORAL_SECRET_ARN, HOST_CREDENTIALS_SECRET_ARN}
    ),
    "worker-execution-secret-injection": frozenset(
        {WORKER_TEMPORAL_SECRET_ARN, POSTGRES_SECRET_ARN, REDIS_SECRET_ARN}
    ),
    "migration-execution-secret-injection": frozenset({MIGRATION_SECRET_ARN}),
}
SVG_NAMESPACE = "http://www.w3.org/2000/svg"
TOPOLOGY_WIDTH = 1560
TOPOLOGY_HEIGHT = 920
EXPECTED_TOPOLOGY_LABELS = frozenset(
    {
        "ALB + WAF",
        "Gateway service",
        "Worker service",
        "Activation",
        "Migration task",
        "Redis / Valkey",
        "PostgreSQL",
        "S3",
        "DynamoDB control table",
        "DynamoDB application table",
        "Secrets Manager + KMS",
        "SQS + EventBridge",
        "CloudWatch + ECS control",
        "Temporal ECS/Fargate",
    }
)
FORBIDDEN_ENVIRONMENT_NAMES = frozenset(
    {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"}
)


class AwsDocumentationError(ValueError):
    """An AWS reference artifact violates its public contract."""


def _load(name: str) -> dict[str, Any]:
    path = REFERENCE_ROOT / name
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise AwsDocumentationError(f"{name} must contain one JSON object")
    if value.get("version") != REFERENCE_VERSION:
        raise AwsDocumentationError(f"{name} has an unexpected reference version")
    return value


def _mapping(value: object, boundary: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise AwsDocumentationError(f"{boundary} must be an object")
    return value


def _sequence(value: object, boundary: str) -> Sequence[Any]:
    if not isinstance(value, list):
        raise AwsDocumentationError(f"{boundary} must be an array")
    return value


def _actions(statement: Mapping[str, Any]) -> tuple[str, ...]:
    value = statement.get("Action")
    if isinstance(value, str):
        return (value,)
    return tuple(str(item) for item in _sequence(value, "IAM Action"))


def _resources(statement: Mapping[str, Any]) -> tuple[str, ...]:
    value = statement.get("Resource")
    if isinstance(value, str):
        return (value,)
    return tuple(str(item) for item in _sequence(value, "IAM Resource"))


def _verify_task_definitions(document: Mapping[str, Any]) -> None:
    tasks = _mapping(document.get("task_definitions"), "task_definitions")
    if frozenset(tasks) != EXPECTED_TASK_ROLES:
        raise AwsDocumentationError("task definitions must contain only gateway and worker roles")
    for role, raw_task in tasks.items():
        task = _mapping(raw_task, f"{role} task")
        if task.get("networkMode") != "awsvpc" or task.get("requiresCompatibilities") != [
            "FARGATE"
        ]:
            raise AwsDocumentationError(f"{role} task must use Fargate awsvpc networking")
        if task.get("executionRoleArn") != EXPECTED_EXECUTION_ROLE_ARNS[role]:
            raise AwsDocumentationError(f"{role} must use its own task execution role")
        containers = _sequence(task.get("containerDefinitions"), f"{role} containers")
        if len(containers) != 1:
            raise AwsDocumentationError(f"{role} task must have exactly one application container")
        container = _mapping(containers[0], f"{role} container")
        if IMAGE_PATTERN.fullmatch(str(container.get("image"))) is None:
            raise AwsDocumentationError(f"{role} image must use the validated digest placeholder")
        if container.get("readonlyRootFilesystem") is not True:
            raise AwsDocumentationError(f"{role} root filesystem must be read-only")
        if container.get("user") != "65532:65532":
            raise AwsDocumentationError(f"{role} must preserve the application image user")
        command = _sequence(container.get("command"), f"{role} command")
        if not command or command[0] != EXPECTED_ROLE_COMMANDS[role]:
            raise AwsDocumentationError(f"{role} command does not select its host application")
        if int(container.get("stopTimeout", 0)) < MINIMUM_DRAIN_SECONDS:
            raise AwsDocumentationError(f"{role} stop timeout is shorter than the drain contract")
        health = _mapping(container.get("healthCheck"), f"{role} health check")
        if READY_PATH not in " ".join(str(item) for item in health.get("command", ())):
            raise AwsDocumentationError(f"{role} health check must use {READY_PATH}")
        environment = _sequence(container.get("environment"), f"{role} environment")
        names = {
            str(_mapping(item, f"{role} environment item").get("name")) for item in environment
        }
        if names & FORBIDDEN_ENVIRONMENT_NAMES:
            raise AwsDocumentationError(f"{role} task contains a static AWS credential variable")
        secrets = _sequence(container.get("secrets"), f"{role} secrets")
        secret_bindings = {
            str(secret.get("name")): str(secret.get("valueFrom"))
            for secret in (_mapping(item, f"{role} secret") for item in secrets)
        }
        if secret_bindings != EXPECTED_ROLE_SECRETS[role]:
            raise AwsDocumentationError(f"{role} task has an unexpected secret boundary")
        log_configuration = _mapping(container.get("logConfiguration"), f"{role} logging")
        if log_configuration.get("logDriver") != "awslogs":
            raise AwsDocumentationError(f"{role} must use CloudWatch awslogs")


def _verify_iam(document: Mapping[str, Any]) -> None:
    policies = _mapping(document.get("policies"), "policies")
    if frozenset(policies) != EXPECTED_IAM_POLICIES:
        raise AwsDocumentationError(
            "IAM policies must separate application roles from ECS secret injection"
        )
    for role, raw_policy in policies.items():
        policy = _mapping(raw_policy, f"{role} policy")
        if policy.get("Version") != "2012-10-17":
            raise AwsDocumentationError(f"{role} policy must use the current IAM document version")
        services: set[str] = set()
        for raw_statement in _sequence(policy.get("Statement"), f"{role} statements"):
            statement = _mapping(raw_statement, f"{role} statement")
            if statement.get("Effect") != "Allow":
                raise AwsDocumentationError(f"{role} reference may contain only explicit allows")
            actions = _actions(statement)
            resources = _resources(statement)
            if "*" in actions or "*" in resources:
                raise AwsDocumentationError(f"{role} policy contains an unrestricted grant")
            services.update(action.partition(":")[0] for action in actions)
        if not REQUIRED_POLICY_SERVICES[role].issubset(services):
            raise AwsDocumentationError(f"{role} policy is missing a required service boundary")
        expected_secret_resources = EXPECTED_EXECUTION_POLICY_SECRET_RESOURCES.get(role)
        if expected_secret_resources is not None:
            actual_secret_resources = {
                resource
                for raw_statement in _sequence(policy.get("Statement"), f"{role} statements")
                for statement in [_mapping(raw_statement, f"{role} statement")]
                if "secretsmanager:GetSecretValue" in _actions(statement)
                for resource in _resources(statement)
            }
            if actual_secret_resources != expected_secret_resources:
                raise AwsDocumentationError(f"{role} can resolve an unexpected secret")
    eventbridge = _mapping(
        document.get("eventbridge_sqs_resource_policy"),
        "EventBridge SQS resource policy",
    )
    statement = _mapping(
        _sequence(eventbridge.get("Statement"), "EventBridge statements")[0],
        "EventBridge statement",
    )
    principal = _mapping(statement.get("Principal"), "EventBridge principal")
    if principal.get("Service") != "events.amazonaws.com":
        raise AwsDocumentationError("EventBridge ingress must use the AWS service principal")
    if _actions(statement) != ("sqs:SendMessage",) or "Condition" not in statement:
        raise AwsDocumentationError("EventBridge ingress must be queue- and rule-constrained")


def _verify_runtime_settings(document: Mapping[str, Any]) -> None:
    common = _mapping(document.get("common"), "common runtime settings")
    gateway = _mapping(document.get("gateway"), "gateway runtime settings")
    worker = _mapping(document.get("worker"), "worker runtime settings")
    secret_injection = _mapping(document.get("secret_injection"), "secret injection")
    for settings in (common, gateway, worker):
        if set(settings) & FORBIDDEN_ENVIRONMENT_NAMES:
            raise AwsDocumentationError("runtime settings contain static AWS credentials")
    queue_names = ("JUSTFLOW_TEMPORAL__TASK_QUEUE", "JUSTFLOW_SCHEDULES__TASK_QUEUE")
    for name in queue_names:
        if name not in common or name in gateway or name in worker:
            raise AwsDocumentationError("Gateway and worker task queues must share common settings")
    if common[queue_names[0]] == common[queue_names[1]]:
        raise AwsDocumentationError("Business and scheduling task queues must differ")
    if common.get("JUSTFLOW_RUNTIME__PROFILE") != "production":
        raise AwsDocumentationError("AWS runtime profile must be production")
    if common.get("JUSTFLOW_CATALOG__BACKEND") != "s3":
        raise AwsDocumentationError("AWS definition catalog must use S3")
    if common.get("JUSTFLOW_CONFIGURATION__BACKEND") != "aws":
        raise AwsDocumentationError("AWS configuration store must use S3 and DynamoDB")
    if frozenset(secret_injection) != EXPECTED_TASK_ROLES:
        raise AwsDocumentationError("secret injection must be split by ECS task role")
    for role in EXPECTED_TASK_ROLES:
        role_secrets = _mapping(secret_injection.get(role), f"{role} secret injection")
        if role_secrets != EXPECTED_ROLE_SECRETS[role]:
            raise AwsDocumentationError(f"{role} runtime secret injection is incomplete")


def _verify_key(
    value: object,
    *,
    boundary: str,
    name: str,
) -> None:
    key = _mapping(value, boundary)
    if key != {"name": name, "type": "S"}:
        raise AwsDocumentationError(f"{boundary} must be the string key '{name}'")


def _verify_data_services(document: Mapping[str, Any]) -> None:
    dynamodb = _mapping(document.get("dynamodb"), "DynamoDB data services")
    configuration = _mapping(dynamodb.get("configuration"), "configuration table")
    _verify_key(
        configuration.get("partition_key"),
        boundary="configuration partition key",
        name="scope_key",
    )
    _verify_key(
        configuration.get("sort_key"),
        boundary="configuration sort key",
        name="metadata_key",
    )
    indexes = _mapping(
        configuration.get("global_secondary_indexes"),
        "configuration indexes",
    )
    expected_indexes = {
        "revisions": ("${CONFIGURATION_REVISION_INDEX}", "revision_order"),
        "activations": ("${CONFIGURATION_ACTIVATION_INDEX}", "activation_order"),
    }
    if frozenset(indexes) != frozenset(expected_indexes):
        raise AwsDocumentationError("configuration table must define both sparse indexes")
    for index_name, (rendered_name, sort_key) in expected_indexes.items():
        index = _mapping(indexes.get(index_name), f"{index_name} index")
        if index.get("name") != rendered_name or index.get("projection") != "ALL":
            raise AwsDocumentationError(f"{index_name} index has an invalid identity or projection")
        _verify_key(
            index.get("partition_key"),
            boundary=f"{index_name} partition key",
            name="scope_digest",
        )
        _verify_key(
            index.get("sort_key"),
            boundary=f"{index_name} sort key",
            name=sort_key,
        )
    if any(
        configuration.get(name) is not True
        for name in ("point_in_time_recovery", "deletion_protection")
    ):
        raise AwsDocumentationError("configuration table must enable recovery and deletion safety")

    application = _mapping(dynamodb.get("application"), "application table")
    _verify_key(
        application.get("partition_key"),
        boundary="application partition key",
        name="entry_key",
    )
    if (
        application.get("optional") is not True
        or application.get("ttl_attribute") != "expires_at"
        or application.get("point_in_time_recovery") is not True
        or application.get("deletion_protection") is not True
    ):
        raise AwsDocumentationError("application DynamoDB table contract is incomplete")

    redis = _mapping(document.get("redis"), "Redis data service")
    if (
        redis.get("port") != REDIS_PORT
        or redis.get("cluster_mode") is not False
        or redis.get("public_access") is not False
        or redis.get("multi_az") is not True
        or redis.get("automatic_failover") is not True
        or redis.get("transit_encryption") is not True
        or redis.get("at_rest_encryption") is not True
        or redis.get("connection_secret_arn") != REDIS_SECRET_ARN
    ):
        raise AwsDocumentationError("Redis must be private, encrypted, and multi-AZ")

    postgres = _mapping(document.get("postgres"), "PostgreSQL data service")
    if (
        postgres.get("port") != POSTGRES_PORT
        or postgres.get("public_access") is not False
        or postgres.get("multi_az") is not True
        or postgres.get("storage_encryption") is not True
        or postgres.get("tls_required") is not True
        or postgres.get("application_credentials_separate_from_master") is not True
        or postgres.get("deletion_protection") is not True
        or postgres.get("runtime_connection_secret_arn") != POSTGRES_SECRET_ARN
        or postgres.get("migration_connection_secret_arn") != MIGRATION_SECRET_ARN
    ):
        raise AwsDocumentationError("PostgreSQL must be private, encrypted, and multi-AZ")

    bindings = _mapping(document.get("ecs_bindings"), "ECS data-service bindings")
    gateway = _mapping(bindings.get("gateway"), "gateway data-service bindings")
    worker = _mapping(bindings.get("worker"), "worker data-service bindings")
    migration = _mapping(bindings.get("migration"), "migration data-service bindings")
    if gateway.get("application_data") != [] or gateway.get("connection_secrets") != []:
        raise AwsDocumentationError("gateway must not receive application data access")
    if frozenset(_sequence(worker.get("connection_secrets"), "worker connection secrets")) != {
        POSTGRES_SECRET_NAME,
        REDIS_SECRET_NAME,
    }:
        raise AwsDocumentationError("worker connection bindings are incomplete")
    if migration.get("application_data") != ["postgres"]:
        raise AwsDocumentationError("migration task must be isolated to PostgreSQL")


def _verify_resource_declarations() -> None:
    path = REFERENCE_ROOT / RESOURCE_DECLARATIONS_FILE_NAME
    document = _mapping(yaml.safe_load(path.read_text(encoding="utf-8")), "resources document")
    resources = _mapping(document.get("resources"), "resources")
    expected_providers = {
        "application_state": "dynamodb",
        "application_postgres": "postgresql",
        "application_cache": "redis",
    }
    if {
        name: _mapping(resources.get(name), f"{name} resource").get("provider")
        for name in expected_providers
    } != expected_providers:
        raise AwsDocumentationError("application resource providers are incomplete")
    postgres = _mapping(resources["application_postgres"], "PostgreSQL resource")
    postgres_config = _mapping(postgres.get("config"), "PostgreSQL resource config")
    postgres_connection = _mapping(postgres_config.get("connection"), "PostgreSQL connection")
    redis = _mapping(resources["application_cache"], "Redis resource")
    redis_config = _mapping(redis.get("config"), "Redis resource config")
    redis_connection = _mapping(redis_config.get("connection"), "Redis connection")
    if postgres_connection.get("runtime_dsn") != "application":
        raise AwsDocumentationError("PostgreSQL resource must use the application runtime binding")
    if (
        redis_connection.get("runtime_url") != "application"
        or redis_connection.get("require_tls") is not True
    ):
        raise AwsDocumentationError("Redis resource must use the TLS application runtime binding")


def _verify_topology() -> None:
    path = REFERENCE_ROOT / TOPOLOGY_FILE_NAME
    root = ElementTree.fromstring(path.read_text(encoding="utf-8"))
    if (
        root.tag != f"{{{SVG_NAMESPACE}}}svg"
        or root.get("width") != str(TOPOLOGY_WIDTH)
        or root.get("height") != str(TOPOLOGY_HEIGHT)
        or root.get("viewBox") != f"0 0 {TOPOLOGY_WIDTH} {TOPOLOGY_HEIGHT}"
    ):
        raise AwsDocumentationError("topology SVG must preserve its complete viewport")
    if root.get("role") != "img" or root.get("aria-labelledby") != "title description":
        raise AwsDocumentationError("topology SVG must expose an accessible image description")
    labels = {
        "".join(element.itertext()).strip() for element in root.iter(f"{{{SVG_NAMESPACE}}}text")
    }
    if not EXPECTED_TOPOLOGY_LABELS.issubset(labels):
        raise AwsDocumentationError("topology SVG is missing a required service boundary")
    if any(element.tag == f"{{{SVG_NAMESPACE}}}script" for element in root.iter()):
        raise AwsDocumentationError("topology SVG must remain a static artifact")


def _verify_service_configuration(document: Mapping[str, Any]) -> None:
    network = _mapping(document.get("network"), "network")
    if network.get("assign_public_ip") is not False:
        raise AwsDocumentationError("ECS application tasks must not receive public IPs")
    alb = _mapping(document.get("alb"), "ALB")
    if (
        alb.get("target_role") != "gateway"
        or alb.get("listener_port") != PUBLIC_TLS_PORT
        or alb.get("target_port") != GATEWAY_PORT
        or alb.get("health_check_path") != READY_PATH
        or alb.get("authentication_required") is not True
    ):
        raise AwsDocumentationError("ALB must expose only the authenticated gateway readiness path")
    if int(alb.get("deregistration_delay_seconds", 0)) < MINIMUM_DRAIN_SECONDS:
        raise AwsDocumentationError("ALB deregistration delay is shorter than the drain contract")
    services = _mapping(document.get("services"), "services")
    for role in EXPECTED_TASK_ROLES:
        service = _mapping(services.get(role), f"{role} service")
        minimum = int(service.get("minimum_tasks", 0))
        maximum = int(service.get("maximum_tasks", 0))
        if minimum < MINIMUM_SERVICE_TASKS or maximum < minimum:
            raise AwsDocumentationError(f"{role} service does not preserve multi-AZ capacity")
        if (
            service.get("desired_tasks") != minimum
            or maximum != minimum
            or service.get("autoscaling") != []
        ):
            raise AwsDocumentationError(f"{role} must use the fixed initial capacity baseline")
        alarms = _sequence(service.get("alarms"), f"{role} alarms")
        if not alarms or any(
            _mapping(alarm, f"{role} alarm").get("Namespace") != "AWS/ECS" for alarm in alarms
        ):
            raise AwsDocumentationError(f"{role} requires native ECS capacity alarms")


def _verify_security_groups(document: Mapping[str, Any]) -> None:
    flows = _sequence(document.get("flows"), "security-group flows")
    public_flows = [
        _mapping(flow, "security-group flow")
        for flow in flows
        if _mapping(flow, "security-group flow").get("from") == "internet"
    ]
    if len(public_flows) != 1 or (
        public_flows[0].get("to") != "alb" or public_flows[0].get("port") != PUBLIC_TLS_PORT
    ):
        raise AwsDocumentationError("public ingress must terminate only at the TLS ALB")
    temporal_flows = [
        _mapping(flow, "security-group flow")
        for flow in flows
        if _mapping(flow, "security-group flow").get("to") == "temporal-frontend"
    ]
    if not temporal_flows or any(flow.get("port") != TEMPORAL_PORT for flow in temporal_flows):
        raise AwsDocumentationError(
            "Private Temporal frontend access must use the TLS service port"
        )
    required_private_flows = {
        ("gateway", "temporal-frontend", TEMPORAL_PORT),
        ("worker", "temporal-frontend", TEMPORAL_PORT),
        ("temporal", "temporal-postgres", POSTGRES_PORT),
        ("worker", "redis", REDIS_PORT),
        ("worker", "postgres", POSTGRES_PORT),
        ("migration", "postgres", POSTGRES_PORT),
    }
    actual_private_flows = {
        (flow.get("from"), flow.get("to"), flow.get("port"))
        for flow in (_mapping(item, "security-group flow") for item in flows)
    }
    if not required_private_flows.issubset(actual_private_flows):
        raise AwsDocumentationError("worker and migration data-service flows are incomplete")
    forbidden = _sequence(document.get("forbidden_flows"), "forbidden flows")
    forbidden_pairs = {
        (
            _mapping(flow, "forbidden flow").get("from"),
            _mapping(flow, "forbidden flow").get("to"),
        )
        for flow in forbidden
    }
    required_forbidden_pairs = {
        ("internet", "worker"),
        ("internet", "temporal"),
        ("internet", "redis"),
        ("internet", "postgres"),
        ("gateway", "redis"),
        ("gateway", "postgres"),
    }
    if not required_forbidden_pairs.issubset(forbidden_pairs):
        raise AwsDocumentationError("public and cross-role data access must be forbidden")


def _verify_sanitization() -> None:
    for name in SANITIZED_REFERENCE_FILE_NAMES:
        content = (REFERENCE_ROOT / name).read_text(encoding="utf-8")
        if AWS_ACCESS_KEY_PATTERN.search(content):
            raise AwsDocumentationError(f"{name} contains an AWS access-key-shaped value")
        without_placeholders = PLACEHOLDER_PATTERN.sub("", content)
        if "${" in without_placeholders:
            raise AwsDocumentationError(f"{name} contains a malformed placeholder")
        if AWS_ACCOUNT_ID_PATTERN.search(without_placeholders):
            raise AwsDocumentationError(f"{name} contains a concrete AWS account ID")
        if AWS_REGION_PATTERN.search(without_placeholders):
            raise AwsDocumentationError(f"{name} contains a concrete AWS region")
        if "PRIVATE KEY" in content or '"AWS_SECRET_ACCESS_KEY"' in content:
            raise AwsDocumentationError(f"{name} contains forbidden credential material")


def verify_aws_documentation() -> None:
    documents = {name: _load(name) for name in REFERENCE_FILE_NAMES}
    _verify_task_definitions(documents["ecs-task-definitions.json"])
    _verify_data_services(documents["data-services.json"])
    _verify_iam(documents["iam-policies.json"])
    _verify_runtime_settings(documents["runtime-settings.json"])
    _verify_service_configuration(documents["service-configuration.json"])
    _verify_security_groups(documents["security-group-flows.json"])
    _verify_resource_declarations()
    _verify_topology()
    _verify_sanitization()


if __name__ == "__main__":
    verify_aws_documentation()
