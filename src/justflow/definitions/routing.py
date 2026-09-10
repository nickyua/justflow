"""Worker deployment compatibility, execution metadata, and retirement guards."""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Any, TypeVar

from temporalio.api.enums.v1 import TaskQueueType
from temporalio.api.workflowservice.v1 import DescribeWorkerDeploymentVersionRequest
from temporalio.client import Client
from temporalio.common import (
    PinnedVersioningOverride,
    VersioningBehavior,
    WorkerDeploymentVersion,
)
from temporalio.service import RPCError, RPCStatusCode
from temporalio.worker import WorkerDeploymentConfig

from justflow.definitions.catalog import DefinitionCatalog
from justflow.definitions.manifest import (
    SHA256_HEX_LENGTH,
    DefinitionManifest,
    workflow_type_name,
)
from justflow.provenance import ExecutionConfigurationIdentity, WorkerArtifactIdentity
from justflow.scope import (
    LOCAL_RUNTIME_SCOPE,
    scoped_identity_from_digest,
    validate_scope_digest,
)

logger = logging.getLogger(__name__)

MAX_DEPLOYMENT_IDENTITY_LENGTH = 128
DEPLOYMENT_IDENTITY_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
OPEN_EXECUTION_QUERY = 'ExecutionStatus = "Running"'
MEMO_LOGICAL_WORKFLOW = "justflow.logical_workflow"
MEMO_DEFINITION_DIGEST = "justflow.definition_digest"
MEMO_WORKER_DEPLOYMENT = "justflow.worker_deployment"
MEMO_WORKER_BUILD_ID = "justflow.worker_build_id"
MEMO_WORKER_ARTIFACT_DIGEST = "justflow.worker_artifact_digest"
MEMO_WORKER_PACKAGE_VERSION = "justflow.worker_package_version"
MEMO_WORKER_SOURCE_REVISION = "justflow.worker_source_revision"
MEMO_ENVIRONMENT_SNAPSHOT_DIGEST = "justflow.environment_snapshot_digest"
MEMO_SCOPE_DIGEST = "justflow.scope_digest"
MEMO_CONFIGURATION_REVISION = "justflow.configuration_revision"
MEMO_TENANT_CONFIGURATION_REVISION = "justflow.tenant_configuration_revision"
MEMO_COMPONENT_CATALOG_REVISION = "justflow.component_catalog_revision"
MEMO_COMPONENT_IDENTITY_DIGEST = "justflow.component_identity_digest"
MEMO_CONFIGURATION_RESOLUTION_DIGEST = "justflow.configuration_resolution_digest"
DEFAULT_DEPLOYMENT_REGISTRATION_ATTEMPTS = 50
DEFAULT_DEPLOYMENT_REGISTRATION_INTERVAL_SECONDS = 0.1
DEFAULT_PINNED_START_RETRY_ATTEMPTS = 50
DEFAULT_PINNED_START_RETRY_INTERVAL_SECONDS = 0.1
DEPLOYMENT_PENDING_STATUSES = frozenset(
    {RPCStatusCode.NOT_FOUND, RPCStatusCode.FAILED_PRECONDITION}
)
PINNED_VERSION_NOT_PRESENT_MESSAGE = (
    "Pinned version '{deployment_name}:{build_id}' is not present in task queue "
    "'{task_queue}' of type 'Workflow'"
)
WORKFLOW_TASK_QUEUE_TYPE = TaskQueueType.TASK_QUEUE_TYPE_WORKFLOW
PinnedStartResultT = TypeVar("PinnedStartResultT")


class DeploymentRoutingError(Exception):
    """No unambiguous compatible worker deployment can run a definition."""


class DeploymentUnavailableError(DeploymentRoutingError):
    """A selected deployment did not become reachable within the startup bound."""


class RetirementBlocker(str, Enum):
    APPROVAL_REQUIRED = "approval_required"
    ACTIVE_ALIAS = "active_alias"
    OPEN_EXECUTIONS = "open_executions"
    REPLAY_FIXTURE_REQUIRED = "replay_fixture_required"
    ACTIVE_DEPLOYMENT = "active_deployment"
    ACTIVE_SCHEDULES = "active_schedules"


@dataclass(frozen=True, kw_only=True)
class WorkerDeployment:
    artifact_identity: WorkerArtifactIdentity
    compatible_engine_workflow_abis: frozenset[str]

    def __post_init__(self) -> None:
        for kind, value in (
            ("deployment name", self.name),
            ("build id", self.build_id),
        ):
            if (
                not value
                or len(value) > MAX_DEPLOYMENT_IDENTITY_LENGTH
                or DEPLOYMENT_IDENTITY_PATTERN.fullmatch(value) is None
            ):
                raise DeploymentRoutingError(f"Invalid worker {kind} '{value}'")
        if not self.compatible_engine_workflow_abis:
            raise DeploymentRoutingError(
                f"Worker deployment '{self.canonical_name}' has no compatible workflow ABI"
            )

    @property
    def name(self) -> str:
        return self.artifact_identity.deployment_name

    @property
    def build_id(self) -> str:
        return self.artifact_identity.build_id

    @property
    def canonical_name(self) -> str:
        return f"{self.name}.{self.build_id}"

    @property
    def temporal_version(self) -> WorkerDeploymentVersion:
        return WorkerDeploymentVersion(deployment_name=self.name, build_id=self.build_id)

    def supports(self, manifest: DefinitionManifest) -> bool:
        return manifest.required_engine_workflow_abi in self.compatible_engine_workflow_abis


class WorkerDeploymentRouter:
    def __init__(
        self,
        deployments: Iterable[WorkerDeployment],
        *,
        active_deployment: str,
    ) -> None:
        indexed: dict[str, WorkerDeployment] = {}
        for deployment in deployments:
            if deployment.canonical_name in indexed:
                raise DeploymentRoutingError(
                    f"Worker deployment '{deployment.canonical_name}' is duplicated"
                )
            indexed[deployment.canonical_name] = deployment
        if active_deployment not in indexed:
            raise DeploymentRoutingError(
                f"Active worker deployment '{active_deployment}' is not registered"
            )
        self._deployments = MappingProxyType(indexed)
        self._active_deployment = active_deployment

    @classmethod
    def for_deployment(cls, deployment: WorkerDeployment) -> WorkerDeploymentRouter:
        return cls([deployment], active_deployment=deployment.canonical_name)

    @property
    def active(self) -> WorkerDeployment:
        return self._deployments[self._active_deployment]

    @property
    def deployments(self) -> Mapping[str, WorkerDeployment]:
        return self._deployments

    def select(self, manifest: DefinitionManifest) -> WorkerDeployment:
        deployment = self.active
        if not deployment.supports(manifest):
            raise DeploymentRoutingError(
                f"Active worker deployment '{deployment.canonical_name}' does not support "
                f"workflow ABI '{manifest.required_engine_workflow_abi}' required by "
                f"'{manifest.logical_name}@{manifest.definition_digest}'"
            )
        return deployment


@dataclass(frozen=True, kw_only=True)
class WorkflowStartTarget:
    manifest: DefinitionManifest
    deployment: WorkerDeployment
    environment_snapshot_digest: str
    scope_digest: str | None = None
    execution_configuration: ExecutionConfigurationIdentity | None = None

    def __post_init__(self) -> None:
        if (
            re.fullmatch(
                rf"[0-9a-f]{{{SHA256_HEX_LENGTH}}}",
                self.environment_snapshot_digest,
            )
            is None
        ):
            raise DeploymentRoutingError("Invalid execution environment snapshot digest")
        if self.scope_digest is not None:
            validate_scope_digest(self.scope_digest)

    @property
    def workflow_type(self) -> str:
        return runtime_workflow_type_name(
            self.manifest.logical_name,
            self.manifest.definition_digest,
            self.scope_digest,
        )

    @property
    def memo(self) -> dict[str, str]:
        return execution_memo(
            self.manifest,
            self.deployment,
            self.environment_snapshot_digest,
            execution_configuration=self.execution_configuration,
        )

    @property
    def versioning_override(self) -> PinnedVersioningOverride:
        return PinnedVersioningOverride(self.deployment.temporal_version)


@dataclass(frozen=True, kw_only=True)
class DefinitionStartTarget(WorkflowStartTarget):
    workflow_class: type


def runtime_workflow_type_name(
    logical_name: str,
    definition_digest: str,
    scope_digest: str | None,
) -> str:
    if scope_digest in {None, LOCAL_RUNTIME_SCOPE.digest}:
        return workflow_type_name(logical_name, definition_digest)
    return scoped_identity_from_digest(
        "workflow-type",
        scope_digest,
        logical_name,
        definition_digest,
    )


@dataclass(frozen=True, kw_only=True)
class OpenExecutionDependency:
    workflow_id: str
    run_id: str
    logical_workflow: str
    definition_digest: str
    worker_deployment: str
    worker_build_id: str
    worker_artifact_digest: str | None = None
    worker_package_version: str | None = None
    worker_source_revision: str | None = None
    environment_snapshot_digest: str | None = None


@dataclass(frozen=True, kw_only=True)
class RetirementAssessment:
    blockers: frozenset[RetirementBlocker]

    @property
    def allowed(self) -> bool:
        return not self.blockers


def execution_memo(
    manifest: DefinitionManifest,
    deployment: WorkerDeployment,
    environment_snapshot_digest: str,
    *,
    execution_configuration: ExecutionConfigurationIdentity | None = None,
) -> dict[str, str]:
    return execution_identity_memo(
        logical_name=manifest.logical_name,
        definition_digest=manifest.definition_digest,
        artifact_identity=deployment.artifact_identity,
        environment_snapshot_digest=environment_snapshot_digest,
        execution_configuration=execution_configuration,
    )


def execution_identity_memo(
    *,
    logical_name: str,
    definition_digest: str,
    artifact_identity: WorkerArtifactIdentity,
    environment_snapshot_digest: str | None = None,
    scope_digest: str | None = None,
    execution_configuration: ExecutionConfigurationIdentity | None = None,
) -> dict[str, str]:
    memo = {
        MEMO_LOGICAL_WORKFLOW: logical_name,
        MEMO_DEFINITION_DIGEST: definition_digest,
        MEMO_WORKER_DEPLOYMENT: artifact_identity.deployment_name,
        MEMO_WORKER_BUILD_ID: artifact_identity.build_id,
        MEMO_WORKER_ARTIFACT_DIGEST: artifact_identity.artifact_digest,
        MEMO_WORKER_PACKAGE_VERSION: artifact_identity.package_version,
    }
    if environment_snapshot_digest is not None:
        memo[MEMO_ENVIRONMENT_SNAPSHOT_DIGEST] = environment_snapshot_digest
    if scope_digest is not None:
        memo[MEMO_SCOPE_DIGEST] = scope_digest
    if execution_configuration is not None:
        memo[MEMO_CONFIGURATION_REVISION] = execution_configuration.configuration_revision_id
        memo[MEMO_CONFIGURATION_RESOLUTION_DIGEST] = execution_configuration.resolution_digest
        if execution_configuration.tenant_configuration_revision_id is not None:
            memo[MEMO_TENANT_CONFIGURATION_REVISION] = (
                execution_configuration.tenant_configuration_revision_id
            )
        if execution_configuration.component_catalog_revision is not None:
            memo[MEMO_COMPONENT_CATALOG_REVISION] = (
                execution_configuration.component_catalog_revision
            )
        if execution_configuration.component_identity_digest is not None:
            memo[MEMO_COMPONENT_IDENTITY_DIGEST] = execution_configuration.component_identity_digest
    if artifact_identity.source_revision is not None:
        memo[MEMO_WORKER_SOURCE_REVISION] = artifact_identity.source_revision
    return memo


def worker_deployment_config(deployment: WorkerDeployment) -> WorkerDeploymentConfig:
    return WorkerDeploymentConfig(
        version=deployment.temporal_version,
        use_worker_versioning=True,
        default_versioning_behavior=VersioningBehavior.PINNED,
    )


async def list_open_execution_dependencies(client: Client) -> tuple[OpenExecutionDependency, ...]:
    dependencies: list[OpenExecutionDependency] = []
    async for execution in client.list_workflows(query=OPEN_EXECUTION_QUERY):
        description = await client.get_workflow_handle(
            execution.id,
            run_id=execution.run_id,
        ).describe()
        memo = await description.memo()
        if MEMO_DEFINITION_DIGEST not in memo:
            continue
        dependencies.append(_dependency_from_memo(execution.id, execution.run_id, memo))
    return tuple(dependencies)


async def wait_for_worker_deployment(
    client: Client,
    deployment: WorkerDeployment,
    task_queue: str,
    *,
    attempts: int = DEFAULT_DEPLOYMENT_REGISTRATION_ATTEMPTS,
    interval_seconds: float = DEFAULT_DEPLOYMENT_REGISTRATION_INTERVAL_SECONDS,
) -> None:
    """Wait until Temporal records the deployment route for the workflow queue."""
    if attempts < 1:
        raise ValueError("Deployment registration attempts must be positive")
    request = DescribeWorkerDeploymentVersionRequest(
        namespace=client.namespace,
        version=deployment.temporal_version.to_canonical_string(),
    )
    for attempt in range(attempts):
        try:
            response = await client.workflow_service.describe_worker_deployment_version(request)
        except RPCError as exc:
            if exc.status not in DEPLOYMENT_PENDING_STATUSES:
                raise DeploymentUnavailableError(
                    f"Cannot inspect worker deployment '{deployment.canonical_name}': {exc}"
                ) from exc
        else:
            if any(
                queue.name == task_queue and queue.type == WORKFLOW_TASK_QUEUE_TYPE
                for queue in response.version_task_queues
            ):
                return
        if attempt + 1 < attempts:
            await asyncio.sleep(interval_seconds)
    raise DeploymentUnavailableError(
        f"Worker deployment route '{deployment.canonical_name}' was not registered for workflow "
        f"task queue '{task_queue}' after {attempts} checks"
    )


async def retry_pinned_workflow_start(
    start: Callable[[], Awaitable[PinnedStartResultT]],
    deployment: WorkerDeployment,
    task_queue: str,
    *,
    attempts: int = DEFAULT_PINNED_START_RETRY_ATTEMPTS,
    interval_seconds: float = DEFAULT_PINNED_START_RETRY_INTERVAL_SECONDS,
) -> PinnedStartResultT:
    """Retry a pinned start while Temporal propagates worker registration."""
    if attempts < 1:
        raise ValueError("Pinned workflow start attempts must be positive")
    attempt = 1
    while True:
        try:
            return await start()
        except RPCError as exc:
            if not _is_missing_pinned_version_rejection(
                exc,
                deployment=deployment,
                task_queue=task_queue,
            ):
                if exc.status is RPCStatusCode.FAILED_PRECONDITION:
                    logger.warning(
                        "Pinned workflow start received an unrecognized precondition rejection",
                        extra={
                            "worker_deployment": deployment.canonical_name,
                            "task_queue": task_queue,
                            "rpc_status": exc.status.name,
                        },
                    )
                raise
            if attempt >= attempts:
                raise
            logger.info(
                "Pinned worker routing is not ready; retrying workflow start",
                extra={
                    "worker_deployment": deployment.canonical_name,
                    "task_queue": task_queue,
                    "attempt": attempt,
                },
            )
            attempt += 1
            await asyncio.sleep(interval_seconds)


def _is_missing_pinned_version_rejection(
    exc: RPCError,
    *,
    deployment: WorkerDeployment,
    task_queue: str,
) -> bool:
    expected_message = PINNED_VERSION_NOT_PRESENT_MESSAGE.format(
        deployment_name=deployment.name,
        build_id=deployment.build_id,
        task_queue=task_queue,
    )
    return exc.status is RPCStatusCode.FAILED_PRECONDITION and exc.message == expected_message


def assess_definition_retirement(
    catalog: DefinitionCatalog,
    *,
    logical_name: str,
    definition_digest: str,
    open_executions: Iterable[OpenExecutionDependency],
    replay_fixture_digests: frozenset[str],
    scheduled_definition_digests: Iterable[str] = (),
    approved: bool,
) -> RetirementAssessment:
    catalog.get(logical_name, definition_digest)
    blockers: set[RetirementBlocker] = set()
    if not approved:
        blockers.add(RetirementBlocker.APPROVAL_REQUIRED)
    if catalog.aliases.get(logical_name) == definition_digest:
        blockers.add(RetirementBlocker.ACTIVE_ALIAS)
    if any(
        execution.logical_workflow == logical_name
        and execution.definition_digest == definition_digest
        for execution in open_executions
    ):
        blockers.add(RetirementBlocker.OPEN_EXECUTIONS)
    if definition_digest not in replay_fixture_digests:
        blockers.add(RetirementBlocker.REPLAY_FIXTURE_REQUIRED)
    if definition_digest in scheduled_definition_digests:
        blockers.add(RetirementBlocker.ACTIVE_SCHEDULES)
    return RetirementAssessment(blockers=frozenset(blockers))


def assess_worker_retirement(
    router: WorkerDeploymentRouter,
    deployment: WorkerDeployment,
    *,
    open_executions: Iterable[OpenExecutionDependency],
    replay_fixture_deployments: frozenset[tuple[str, str]],
    scheduled_worker_deployments: Iterable[tuple[str, str]] = (),
    approved: bool,
) -> RetirementAssessment:
    registered = router.deployments.get(deployment.canonical_name)
    if registered != deployment:
        raise DeploymentRoutingError(
            f"Worker deployment '{deployment.canonical_name}' is not registered"
        )
    blockers: set[RetirementBlocker] = set()
    if not approved:
        blockers.add(RetirementBlocker.APPROVAL_REQUIRED)
    if router.active == deployment:
        blockers.add(RetirementBlocker.ACTIVE_DEPLOYMENT)
    if any(
        execution.worker_deployment == deployment.name
        and execution.worker_build_id == deployment.build_id
        for execution in open_executions
    ):
        blockers.add(RetirementBlocker.OPEN_EXECUTIONS)
    if (deployment.name, deployment.build_id) not in replay_fixture_deployments:
        blockers.add(RetirementBlocker.REPLAY_FIXTURE_REQUIRED)
    if (deployment.name, deployment.build_id) in scheduled_worker_deployments:
        blockers.add(RetirementBlocker.ACTIVE_SCHEDULES)
    return RetirementAssessment(blockers=frozenset(blockers))


def _dependency_from_memo(
    workflow_id: str,
    run_id: str,
    memo: Mapping[str, Any],
) -> OpenExecutionDependency:
    values: dict[str, str] = {}
    for key in (
        MEMO_LOGICAL_WORKFLOW,
        MEMO_DEFINITION_DIGEST,
        MEMO_WORKER_DEPLOYMENT,
        MEMO_WORKER_BUILD_ID,
    ):
        value = memo.get(key)
        if not isinstance(value, str) or not value:
            raise DeploymentRoutingError(
                f"Open execution '{workflow_id}/{run_id}' has invalid metadata '{key}'"
            )
        values[key] = value
    return OpenExecutionDependency(
        workflow_id=workflow_id,
        run_id=run_id,
        logical_workflow=values[MEMO_LOGICAL_WORKFLOW],
        definition_digest=values[MEMO_DEFINITION_DIGEST],
        worker_deployment=values[MEMO_WORKER_DEPLOYMENT],
        worker_build_id=values[MEMO_WORKER_BUILD_ID],
        worker_artifact_digest=_optional_memo_value(
            memo,
            MEMO_WORKER_ARTIFACT_DIGEST,
            workflow_id=workflow_id,
            run_id=run_id,
        ),
        worker_package_version=_optional_memo_value(
            memo,
            MEMO_WORKER_PACKAGE_VERSION,
            workflow_id=workflow_id,
            run_id=run_id,
        ),
        worker_source_revision=_optional_memo_value(
            memo,
            MEMO_WORKER_SOURCE_REVISION,
            workflow_id=workflow_id,
            run_id=run_id,
        ),
        environment_snapshot_digest=_optional_memo_value(
            memo,
            MEMO_ENVIRONMENT_SNAPSHOT_DIGEST,
            workflow_id=workflow_id,
            run_id=run_id,
        ),
    )


def _optional_memo_value(
    memo: Mapping[str, Any],
    key: str,
    *,
    workflow_id: str,
    run_id: str,
) -> str | None:
    value = memo.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise DeploymentRoutingError(
            f"Open execution '{workflow_id}/{run_id}' has invalid metadata '{key}'"
        )
    return value
