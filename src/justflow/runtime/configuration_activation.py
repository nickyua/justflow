"""Resumable configuration activation across host-owned runtime boundaries."""

from __future__ import annotations

import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from typing import Protocol

from pydantic import Field, TypeAdapter, model_validator

from justflow.config.settings import (
    DEFAULT_ACTIVATION_SCOPE_LIMIT,
    DEFAULT_ACTIVATION_TARGET_LIMIT,
    MAX_ACTIVATION_SCOPE_LIMIT,
    MAX_ACTIVATION_TARGET_LIMIT,
    ActivationSettings,
)
from justflow.config.triggers import TriggersConfig
from justflow.configuration.activation import (
    MAX_ACTIVATION_CHANGES,
    MAX_ACTIVATION_PAGE_SIZE,
    ActivationCheckpointKind,
    ActivationExternalOutcome,
    ActivationObservations,
    ActivationOutcomeCode,
    ActivationPage,
    ActivationPlan,
    ActivationRecord,
    ActivationState,
    DefinitionActivationAction,
    ProvenanceDigest,
    RoutingActivationAction,
    ScheduleActivationAction,
    StrictConfigurationModel,
    WorkerActivationAction,
    WorkerReadinessRegistration,
    activation_identity,
    activation_request_digest,
    control_identity_digest,
    plan_configuration_activation,
)
from justflow.configuration.activation_errors import (
    ActivationConflictError,
    ActivationError,
    ActivationIntegrityError,
    ActivationLimitError,
    ActivationNotFoundError,
    ActivationUnavailableError,
)
from justflow.configuration.activation_store import (
    ActivationStore,
    acquire_activation_lease,
    record_activation_checkpoint,
    register_worker_readiness,
    release_activation_lease,
    transition_activation,
)
from justflow.configuration.errors import ConfigurationConflictError, ConfigurationError
from justflow.configuration.models import (
    ConfigurationBundle,
    ConfigurationSnapshot,
    RevisionIdentity,
    RevisionRecord,
)
from justflow.configuration.ports import ConfigurationStore
from justflow.definitions.catalog import (
    CatalogConflictError,
    CatalogError,
    CatalogState,
    DefinitionCatalogStore,
)
from justflow.definitions.manifest import DefinitionManifest
from justflow.definitions.routing import WorkflowStartTarget
from justflow.provenance import (
    ExecutionConfigurationIdentity,
    ExecutionEnvironmentSnapshot,
    WorkerArtifactIdentity,
    provenance_digest,
)
from justflow.runtime.blocking_io import run_blocking
from justflow.runtime.metrics import ActivationMetricState, MetricsRegistry
from justflow.runtime.schedule_reconciler import ScheduleReconciler
from justflow.runtime.schedules import DesiredSchedule, ObservedSchedule
from justflow.runtime.starter import WorkflowStartRegistration, WorkflowTargetResolver
from justflow.scope import RuntimeScope

_PROVENANCE_DIGEST = TypeAdapter(ProvenanceDigest)


class ActivationControllerError(ActivationError):
    def __init__(
        self,
        code: ActivationOutcomeCode,
        message: str,
        *,
        retryable: bool,
    ) -> None:
        self.code = code
        self.retryable = retryable
        super().__init__(message)


class ExternalOperationConfirmation(StrictConfigurationModel):
    outcome: ActivationExternalOutcome
    observation_digest: ProvenanceDigest


class WorkerDeploymentObservation(StrictConfigurationModel):
    observation_digest: ProvenanceDigest
    registered_definition_digests: frozenset[str] = Field(
        default_factory=frozenset,
        max_length=MAX_ACTIVATION_CHANGES,
    )
    active_artifact: WorkerArtifactIdentity | None = None


class WorkerDeploymentRetirement(StrictConfigurationModel):
    retiring_artifact: WorkerArtifactIdentity
    replacement_artifact: WorkerArtifactIdentity
    open_execution_count: int = Field(default=0, ge=0, le=0, strict=True)
    observation_digest: ProvenanceDigest
    observed_at: datetime

    @model_validator(mode="after")
    def validate_retirement(self) -> WorkerDeploymentRetirement:
        if self.retiring_artifact == self.replacement_artifact:
            raise ValueError("Worker retirement requires a distinct replacement artifact")
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise ValueError("Worker retirement observation must be timezone-aware")
        return self


@dataclass(frozen=True, kw_only=True)
class PreparedActivation:
    scope: RuntimeScope
    revision: RevisionRecord
    policy_digest: str
    artifact: WorkerArtifactIdentity
    task_queue_identity_digest: str
    manifests: Mapping[str, DefinitionManifest]
    environment_snapshots: Mapping[str, ExecutionEnvironmentSnapshot]
    start_targets: Mapping[str, WorkflowStartTarget]
    desired_schedules: Mapping[str, DesiredSchedule]

    def __post_init__(self) -> None:
        for field_name in (
            "manifests",
            "environment_snapshots",
            "start_targets",
            "desired_schedules",
        ):
            object.__setattr__(self, field_name, MappingProxyType(dict(getattr(self, field_name))))
        _PROVENANCE_DIGEST.validate_python(self.policy_digest)
        _PROVENANCE_DIGEST.validate_python(self.task_queue_identity_digest)
        if self.revision.scope_digest != self.scope.digest:
            raise ValueError("Prepared configuration belongs to another runtime scope")
        if not isinstance(self.revision.bundle, ConfigurationBundle):
            raise TypeError("Activation requires a resolved configuration bundle")
        if len(self.manifests) > MAX_ACTIVATION_CHANGES:
            raise ValueError("Prepared definition collection exceeds its bound")
        if set(self.bundle.workflows) != set(self.manifests):
            raise ValueError("Prepared definitions do not match configured workflows")
        if set(self.manifests) != set(self.start_targets):
            raise ValueError("Prepared workflow targets do not match definition manifests")
        for name, target in self.start_targets.items():
            manifest = self.manifests[name]
            if target.manifest != manifest or target.scope_digest != self.scope.digest:
                raise ValueError("Prepared workflow target does not match its runtime scope")
            if target.deployment.artifact_identity != self.artifact:
                raise ValueError("Prepared workflow target uses another worker artifact")
            if target.environment_snapshot_digest not in self.environment_snapshots:
                raise ValueError("Prepared workflow target has no retained environment snapshot")
        for digest, snapshot in self.environment_snapshots.items():
            if digest != snapshot.snapshot_digest or snapshot.scope_digest != self.scope.digest:
                raise ValueError("Prepared environment snapshot identity is inconsistent")
        manifest_digests = {manifest.definition_digest for manifest in self.manifests.values()}
        for schedule in self.desired_schedules.values():
            if schedule.target.definition_digest not in manifest_digests:
                raise ValueError("Prepared schedule targets an unavailable definition")
            if schedule.target.scope_digest != self.scope.digest:
                raise ValueError("Prepared schedule belongs to another runtime scope")
        if set(self.bundle.triggers.schedules) != {
            schedule.schedule_name for schedule in self.desired_schedules.values()
        }:
            raise ValueError("Prepared schedules do not match configured schedules")

    @property
    def bundle(self) -> ConfigurationBundle:
        bundle = self.revision.bundle
        if not isinstance(bundle, ConfigurationBundle):
            raise TypeError("Prepared activation lost its resolved bundle")
        return bundle

    @property
    def definition_digests(self) -> tuple[str, ...]:
        return tuple(sorted({manifest.definition_digest for manifest in self.manifests.values()}))

    @property
    def execution_configuration(self) -> ExecutionConfigurationIdentity:
        return ConfigurationSnapshot(
            scope_digest=self.scope.digest,
            revision_id=self.revision.revision_id,
            bundle=self.bundle,
        ).execution_identity


@dataclass(frozen=True, kw_only=True)
class PreparedWorkerDeployment:
    target: PreparedActivation
    assignments: tuple[PreparedActivation, ...]

    def __post_init__(self) -> None:
        if not self.assignments or self.target not in self.assignments:
            raise ValueError("Worker deployment preparation omits its activation target")
        if len(self.assignments) > MAX_ACTIVATION_CHANGES:
            raise ValueError("Worker deployment scope collection exceeds its bound")
        scope_digests = [assignment.scope.digest for assignment in self.assignments]
        if len(set(scope_digests)) != len(scope_digests):
            raise ValueError("Worker deployment preparation contains duplicate scopes")
        if tuple(scope_digests) != tuple(sorted(scope_digests)):
            raise ValueError("Worker deployment scopes must use deterministic ordering")
        if any(assignment.artifact != self.target.artifact for assignment in self.assignments):
            raise ValueError("Worker deployment scopes require different artifacts")
        if len(self.definition_digests) > MAX_ACTIVATION_CHANGES:
            raise ValueError("Worker deployment definition collection exceeds its bound")

    @property
    def definition_digests(self) -> tuple[str, ...]:
        return tuple(
            sorted(
                {
                    digest
                    for assignment in self.assignments
                    for digest in assignment.definition_digests
                }
            )
        )

    @property
    def registration_digest(self) -> str:
        return provenance_digest(
            {
                "artifact": self.target.artifact.model_dump(mode="json"),
                "assignments": [
                    {
                        "configuration_revision_id": str(assignment.revision.revision_id),
                        "scope_digest": assignment.scope.digest,
                        "task_queue_identity_digest": assignment.task_queue_identity_digest,
                        "workflow_types": sorted(
                            target.workflow_type for target in assignment.start_targets.values()
                        ),
                    }
                    for assignment in self.assignments
                ],
            }
        )


class ActivationPreparationSource(Protocol):
    def prepare(
        self,
        scope: RuntimeScope,
        revision: RevisionRecord,
    ) -> PreparedActivation: ...

    def prepare_worker_deployment(
        self,
        target: PreparedActivation,
    ) -> PreparedWorkerDeployment: ...


class DefinitionCatalogSource(Protocol):
    def read(self, scope: RuntimeScope) -> DefinitionCatalogStore: ...


class WorkerDeploymentBinding(Protocol):
    async def observe(self, scope: RuntimeScope) -> WorkerDeploymentObservation: ...

    async def rollout(
        self,
        prepared: PreparedWorkerDeployment,
        *,
        plan_digest: str,
    ) -> ExternalOperationConfirmation: ...

    async def readiness(
        self,
        prepared: PreparedWorkerDeployment,
    ) -> WorkerReadinessRegistration | None: ...

    async def route(
        self,
        prepared: PreparedActivation,
        *,
        plan_digest: str,
    ) -> ExternalOperationConfirmation: ...

    async def retire(
        self,
        retirement: WorkerDeploymentRetirement,
    ) -> ExternalOperationConfirmation: ...


class ScheduleActivationBinding(Protocol):
    async def observe(self, scope: RuntimeScope) -> str: ...

    async def reconcile(
        self,
        prepared: PreparedActivation,
    ) -> ExternalOperationConfirmation: ...


class ScopedDefinitionCatalogSource:
    def __init__(
        self,
        stores: Mapping[RuntimeScope, DefinitionCatalogStore],
    ) -> None:
        indexed: dict[str, tuple[RuntimeScope, DefinitionCatalogStore]] = {}
        for scope, store in stores.items():
            if scope.digest in indexed:
                raise ValueError("Definition catalog source has a duplicate runtime scope")
            indexed[scope.digest] = (scope, store)
        self._stores = indexed

    def read(self, scope: RuntimeScope) -> DefinitionCatalogStore:
        entry = self._stores.get(scope.digest)
        if entry is None or entry[0] != scope:
            raise ActivationUnavailableError(
                "Definition catalog is not registered for the runtime scope"
            )
        return entry[1]


@dataclass(frozen=True, kw_only=True)
class RuntimeIndexSnapshot:
    revision_id: RevisionIdentity
    policy_digest: str
    artifact: WorkerArtifactIdentity
    targets: Mapping[str, WorkflowStartTarget]
    triggers: TriggersConfig


class PreparedRuntimeIndex(WorkflowTargetResolver):
    def __init__(
        self,
        *,
        max_scopes: int = DEFAULT_ACTIVATION_SCOPE_LIMIT,
        max_targets_per_scope: int = DEFAULT_ACTIVATION_TARGET_LIMIT,
    ) -> None:
        if not 1 <= max_scopes <= MAX_ACTIVATION_SCOPE_LIMIT:
            raise ValueError("Runtime index scope limit is invalid")
        if not 1 <= max_targets_per_scope <= MAX_ACTIVATION_TARGET_LIMIT:
            raise ValueError("Runtime index target limit is invalid")
        self._max_scopes = max_scopes
        self._max_targets_per_scope = max_targets_per_scope
        self._entries: dict[str, tuple[RuntimeScope, RuntimeIndexSnapshot]] = {}
        self._lock = threading.Lock()

    @classmethod
    def from_settings(cls, settings: ActivationSettings) -> PreparedRuntimeIndex:
        return cls(
            max_scopes=settings.max_scopes,
            max_targets_per_scope=settings.max_targets_per_scope,
        )

    def replace(
        self,
        scope: RuntimeScope,
        *,
        revision_id: RevisionIdentity,
        policy_digest: str,
        artifact: WorkerArtifactIdentity,
        targets: Mapping[str, WorkflowStartTarget],
        triggers: TriggersConfig,
    ) -> None:
        _PROVENANCE_DIGEST.validate_python(policy_digest)
        copied = dict(targets)
        if len(copied) > self._max_targets_per_scope:
            raise ActivationLimitError("Runtime index workflow collection exceeds its bound")
        for name, target in copied.items():
            if name != target.manifest.logical_name or target.scope_digest != scope.digest:
                raise ActivationIntegrityError(
                    "Runtime index target does not belong to its trusted scope"
                )
            if target.deployment.artifact_identity != artifact:
                raise ActivationIntegrityError("Runtime index target artifact is inconsistent")
        copied_triggers = TriggersConfig(triggers=dict(triggers.triggers))
        if any(
            declaration.workflow not in copied for declaration in copied_triggers.triggers.values()
        ):
            raise ActivationIntegrityError("Runtime index trigger targets an unavailable workflow")
        snapshot = RuntimeIndexSnapshot(
            revision_id=revision_id,
            policy_digest=policy_digest,
            artifact=artifact,
            targets=MappingProxyType(copied),
            triggers=copied_triggers,
        )
        with self._lock:
            if scope.digest not in self._entries and len(self._entries) >= self._max_scopes:
                raise ActivationLimitError("Runtime index scope collection exceeds its bound")
            self._entries = {**self._entries, scope.digest: (scope, snapshot)}

    def snapshot(self, scope: RuntimeScope) -> RuntimeIndexSnapshot | None:
        with self._lock:
            entry = self._entries.get(scope.digest)
        if entry is None or entry[0] != scope:
            return None
        return entry[1]

    async def resolve(
        self,
        scope: RuntimeScope,
        workflow_name: str,
    ) -> WorkflowStartRegistration | None:
        snapshot = self.snapshot(scope)
        if snapshot is None:
            return None
        target = snapshot.targets.get(workflow_name)
        if target is None:
            return None
        return WorkflowStartRegistration(
            target=target,
            triggers={
                name: declaration
                for name, declaration in snapshot.triggers.triggers.items()
                if declaration.workflow == workflow_name
            },
        )


class ReconcilerScheduleActivationBinding:
    def __init__(
        self,
        reconcilers: Mapping[RuntimeScope, ScheduleReconciler],
    ) -> None:
        indexed: dict[str, tuple[RuntimeScope, ScheduleReconciler]] = {}
        for scope, reconciler in reconcilers.items():
            if scope.digest in indexed:
                raise ValueError("Schedule binding has a duplicate runtime scope")
            indexed[scope.digest] = (scope, reconciler)
        self._reconcilers = indexed

    async def observe(self, scope: RuntimeScope) -> str:
        reconciler = self._reconciler(scope)
        observed = await reconciler.observe()
        return _schedule_observation_digest(observed)

    async def reconcile(
        self,
        prepared: PreparedActivation,
    ) -> ExternalOperationConfirmation:
        reconciler = self._reconciler(prepared.scope)
        plan = await reconciler.plan(dict(prepared.desired_schedules))
        result = await reconciler.apply(plan, confirmation=plan.plan_digest)
        if not result.successful:
            raise ActivationControllerError(
                ActivationOutcomeCode.SCHEDULES_UNAVAILABLE,
                "Configuration schedules did not reach their reconciliation boundary",
                retryable=True,
            )
        outcome = (
            ActivationExternalOutcome.IDEMPOTENT
            if all(item.status.value == "already_applied" for item in result.items)
            else ActivationExternalOutcome.CONFIRMED
        )
        return ExternalOperationConfirmation(
            outcome=outcome,
            observation_digest=provenance_digest(
                {
                    "items": [
                        {
                            "change": item.change.value,
                            "schedule_id": item.schedule_id,
                            "status": item.status.value,
                        }
                        for item in result.items
                    ],
                    "plan_digest": result.plan_digest,
                    "scope_digest": prepared.scope.digest,
                }
            ),
        )

    def _reconciler(self, scope: RuntimeScope) -> ScheduleReconciler:
        entry = self._reconcilers.get(scope.digest)
        if entry is None or entry[0] != scope:
            raise ActivationControllerError(
                ActivationOutcomeCode.SCHEDULES_UNAVAILABLE,
                "Schedule reconciliation is not registered for the runtime scope",
                retryable=True,
            )
        return entry[1]


class ConfigurationActivationController:
    def __init__(
        self,
        *,
        configuration_store: ConfigurationStore,
        activation_store: ActivationStore,
        preparation_source: ActivationPreparationSource,
        catalog_source: DefinitionCatalogSource,
        runtime_index: PreparedRuntimeIndex,
        controller_identity: str,
        worker_binding: WorkerDeploymentBinding | None = None,
        schedule_binding: ScheduleActivationBinding | None = None,
        metrics: MetricsRegistry | None = None,
        activation_settings: ActivationSettings | None = None,
        lease_duration: timedelta | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        configured_activation = activation_settings or ActivationSettings()
        if lease_duration is None:
            lease_duration = timedelta(seconds=configured_activation.lease_seconds)
        if lease_duration.total_seconds() <= 0:
            raise ValueError("Activation lease duration must be positive")
        self._configuration_store = configuration_store
        self._activation_store = activation_store
        self._preparation_source = preparation_source
        self._catalog_source = catalog_source
        self._runtime_index = runtime_index
        self._controller_digest = control_identity_digest(
            "activation-controller",
            controller_identity,
        )
        self._worker_binding = worker_binding
        self._schedule_binding = schedule_binding
        self._metrics = metrics
        self._lease_duration = lease_duration
        self._clock = clock

    def _inspect_catalog(self, scope: RuntimeScope) -> CatalogState:
        return self._catalog_source.read(scope).inspect()

    async def plan(
        self,
        scope: RuntimeScope,
        target_revision_id: RevisionIdentity,
    ) -> ActivationPlan:
        target = await run_blocking(
            self._configuration_store.read_revision, scope, target_revision_id
        )
        prepared = await run_blocking(self._preparation_source.prepare, scope, target)
        worker_deployment = await run_blocking(
            self._preparation_source.prepare_worker_deployment, prepared
        )
        active_pointer = await run_blocking(self._configuration_store.read_active, scope)
        active_revision = (
            (
                await run_blocking(
                    self._configuration_store.read_revision, scope, active_pointer.revision_id
                )
            )
            if active_pointer is not None
            else None
        )
        active_bundle = _resolved_bundle(active_revision) if active_revision is not None else None
        catalog = await run_blocking(self._inspect_catalog, scope)
        worker = await self._worker_observation(scope)
        schedule_digest = await self._schedule_observation(scope)
        indexed = self._runtime_index.snapshot(scope)
        active_artifact = (
            indexed.artifact
            if indexed is not None
            and active_pointer is not None
            and indexed.revision_id == active_pointer.revision_id
            else worker.active_artifact
        )
        active_policy_digest = (
            indexed.policy_digest
            if indexed is not None
            and active_pointer is not None
            and indexed.revision_id == active_pointer.revision_id
            else None
        )
        observations = ActivationObservations(
            active_pointer_version=(active_pointer.version if active_pointer is not None else None),
            catalog_alias_version=catalog.alias_version,
            policy_digest=prepared.policy_digest,
            worker_observation_digest=worker.observation_digest,
            schedule_observation_digest=schedule_digest,
            registered_definition_digests=worker.registered_definition_digests,
        )
        return plan_configuration_activation(
            scope_digest=scope.digest,
            active_revision_id=(active_pointer.revision_id if active_pointer is not None else None),
            active=active_bundle,
            target_revision_id=target_revision_id,
            target=prepared.bundle,
            active_artifact=active_artifact,
            target_artifact=prepared.artifact,
            target_task_queue_identity_digest=prepared.task_queue_identity_digest,
            target_worker_registration_digest=worker_deployment.registration_digest,
            active_policy_digest=active_policy_digest,
            target_definition_digests={
                f"{assignment.scope.digest}:{name}": manifest.definition_digest
                for assignment in worker_deployment.assignments
                for name, manifest in assignment.manifests.items()
            },
            observations=observations,
        )

    async def activate(
        self,
        scope: RuntimeScope,
        target_revision_id: RevisionIdentity,
        *,
        plan_digest: str,
        idempotency_key: str,
        actor_identity: str,
        correlation_identity: str,
        rollback_of_activation_id: str | None = None,
    ) -> ActivationRecord:
        identity = activation_identity(scope.digest, idempotency_key)
        actor_digest = control_identity_digest("activation-actor", actor_identity)
        try:
            existing = await run_blocking(self._activation_store.read_activation, scope, identity)
        except ActivationNotFoundError:
            existing = None
        if existing is not None:
            if (
                existing.plan.plan_digest != plan_digest
                or existing.plan.target_revision_id != target_revision_id
                or existing.rollback_of_activation_id != rollback_of_activation_id
                or existing.actor_digest != actor_digest
            ):
                raise ActivationConflictError(identity)
            return await self.resume(scope, identity)
        planned = await self.plan(scope, target_revision_id)
        if planned.plan_digest != plan_digest:
            raise ActivationControllerError(
                ActivationOutcomeCode.STALE_PLAN,
                "Activation plan no longer matches current runtime observations",
                retryable=False,
            )
        now = self._now()
        request_digest = provenance_digest(
            {
                "activation": activation_request_digest(planned),
                "rollback_of_activation_id": rollback_of_activation_id,
            }
        )
        pending = ActivationRecord(
            activation_id=activation_identity(scope.digest, idempotency_key),
            scope_digest=scope.digest,
            idempotency_key_digest=control_identity_digest(
                "activation-key",
                idempotency_key,
            ),
            request_digest=request_digest,
            actor_digest=control_identity_digest("activation-actor", actor_identity),
            correlation_digest=control_identity_digest(
                "activation-correlation",
                correlation_identity,
            ),
            plan=planned,
            rollback_of_activation_id=rollback_of_activation_id,
            created_at=now,
            updated_at=now,
        )
        record = await run_blocking(self._activation_store.create_activation, scope, pending)
        if record.request_digest != request_digest:
            raise ActivationConflictError(record.activation_id)
        return await self.resume(scope, record.activation_id)

    async def resume(
        self,
        scope: RuntimeScope,
        activation_id: str,
    ) -> ActivationRecord:
        record = await run_blocking(self._activation_store.read_activation, scope, activation_id)
        if record.state in {
            ActivationState.APPLIED,
            ActivationState.SUPERSEDED,
            ActivationState.ROLLED_BACK,
        }:
            return self._observe(record)
        record = await run_blocking(self._acquire, scope, record)
        try:
            record = await run_blocking(self._start_or_resume, scope, record)
            if record.state is ActivationState.WAITING_FOR_READINESS:
                if record.worker_readiness is None:
                    readiness = await self._read_worker_readiness(scope, record)
                    if readiness is None:
                        return self._observe(await run_blocking(self._release, scope, record))
                    record = await run_blocking(self._register_readiness, scope, record, readiness)
                record = await run_blocking(
                    self._transition, scope, record, ActivationState.RUNNING
                )
            prepared, worker_deployment = await run_blocking(self._prepare_record, scope, record)
            record = await run_blocking(
                self._store_definition_objects, scope, record, worker_deployment
            )
            record = await self._ensure_worker_ready(
                scope,
                record,
                prepared,
                worker_deployment,
            )
            if record.state is ActivationState.WAITING_FOR_READINESS:
                return self._observe(record)
            record = await self._reconcile_schedules(scope, record, prepared)
            record = await run_blocking(self._switch_catalog, scope, record, prepared)
            record = await self._switch_worker_routing(scope, record, prepared)
            record = await run_blocking(self._advance_active_pointer, scope, record)
            record = await run_blocking(self._replace_runtime_index, scope, record, prepared)
            return self._observe(
                await run_blocking(
                    self._transition,
                    scope,
                    record,
                    ActivationState.APPLIED,
                    release_lease=True,
                )
            )
        except ConfigurationConflictError:
            return await run_blocking(
                self._terminal,
                scope,
                record,
                ActivationState.SUPERSEDED,
                ActivationOutcomeCode.CONFIGURATION_CONFLICT,
            )
        except CatalogConflictError:
            return await run_blocking(
                self._terminal,
                scope,
                record,
                ActivationState.SUPERSEDED,
                ActivationOutcomeCode.STALE_PLAN,
            )
        except ActivationControllerError as exc:
            (
                await run_blocking(
                    self._terminal,
                    scope,
                    record,
                    ActivationState.FAILED,
                    exc.code,
                )
            )
            raise
        except CatalogError as exc:
            (
                await run_blocking(
                    self._terminal,
                    scope,
                    record,
                    ActivationState.FAILED,
                    ActivationOutcomeCode.DEFINITIONS_UNAVAILABLE,
                )
            )
            raise ActivationControllerError(
                ActivationOutcomeCode.DEFINITIONS_UNAVAILABLE,
                "Definition catalog could not complete activation",
                retryable=True,
            ) from exc
        except ConfigurationError as exc:
            (
                await run_blocking(
                    self._terminal,
                    scope,
                    record,
                    ActivationState.FAILED,
                    ActivationOutcomeCode.INTERNAL_ERROR,
                )
            )
            raise ActivationControllerError(
                ActivationOutcomeCode.INTERNAL_ERROR,
                "Resolved configuration could not be activated",
                retryable=False,
            ) from exc

    async def register_readiness(
        self,
        scope: RuntimeScope,
        activation_id: str,
        registration: WorkerReadinessRegistration,
    ) -> ActivationRecord:
        record = await run_blocking(self._activation_store.read_activation, scope, activation_id)
        updated = register_worker_readiness(record, registration, occurred_at=self._now())
        if updated != record:
            record = await run_blocking(
                self._activation_store.update_activation,
                scope,
                updated,
                expected_version=record.version,
            )
        return await self.resume(scope, activation_id)

    async def rollback(
        self,
        scope: RuntimeScope,
        activation_id: str,
        *,
        plan_digest: str,
        idempotency_key: str,
        actor_identity: str,
        correlation_identity: str,
    ) -> ActivationRecord:
        original = await run_blocking(self._activation_store.read_activation, scope, activation_id)
        target_revision_id = original.plan.expected_active_revision_id
        if original.state not in {ActivationState.APPLIED, ActivationState.ROLLED_BACK}:
            raise ActivationConflictError(original.activation_id)
        if target_revision_id is None:
            raise ActivationControllerError(
                ActivationOutcomeCode.CONFIGURATION_CONFLICT,
                "Activation has no retained predecessor revision",
                retryable=False,
            )
        rollback = await self.activate(
            scope,
            target_revision_id,
            plan_digest=plan_digest,
            idempotency_key=idempotency_key,
            actor_identity=actor_identity,
            correlation_identity=correlation_identity,
            rollback_of_activation_id=original.activation_id,
        )
        if rollback.state is ActivationState.APPLIED and original.state is ActivationState.APPLIED:
            updated = transition_activation(
                original,
                ActivationState.ROLLED_BACK,
                occurred_at=self._now(),
            )
            try:
                (
                    await run_blocking(
                        self._activation_store.update_activation,
                        scope,
                        updated,
                        expected_version=original.version,
                    )
                )
            except ActivationConflictError:
                current = await run_blocking(
                    self._activation_store.read_activation, scope, original.activation_id
                )
                if current.state is not ActivationState.ROLLED_BACK:
                    raise
        return rollback

    def read(self, scope: RuntimeScope, activation_id: str) -> ActivationRecord:
        return self._activation_store.read_activation(scope, activation_id)

    def list(
        self,
        scope: RuntimeScope,
        *,
        limit: int,
        cursor: str | None = None,
    ) -> ActivationPage:
        if not 1 <= limit <= MAX_ACTIVATION_PAGE_SIZE:
            raise ActivationLimitError("Activation page size is invalid")
        return self._activation_store.list_activations(scope, limit=limit, cursor=cursor)

    def _prepare_record(
        self,
        scope: RuntimeScope,
        record: ActivationRecord,
    ) -> tuple[PreparedActivation, PreparedWorkerDeployment]:
        revision = self._configuration_store.read_revision(scope, record.plan.target_revision_id)
        prepared = self._preparation_source.prepare(scope, revision)
        worker_deployment = self._preparation_source.prepare_worker_deployment(prepared)
        if (
            prepared.policy_digest != record.plan.policy_digest
            or prepared.artifact != record.plan.target_artifact
            or prepared.task_queue_identity_digest != record.plan.target_task_queue_identity_digest
            or worker_deployment.registration_digest
            != record.plan.target_worker_registration_digest
            or worker_deployment.definition_digests != record.plan.target_definition_digests
        ):
            raise ActivationControllerError(
                ActivationOutcomeCode.STALE_PLAN,
                "Prepared runtime inputs no longer match the activation plan",
                retryable=False,
            )
        return prepared, worker_deployment

    def _store_definition_objects(
        self,
        scope: RuntimeScope,
        record: ActivationRecord,
        prepared: PreparedWorkerDeployment,
    ) -> ActivationRecord:
        if _has_checkpoint(record, ActivationCheckpointKind.DEFINITIONS):
            return record
        for assignment in prepared.assignments:
            catalog = self._catalog_source.read(assignment.scope)
            for manifest in assignment.manifests.values():
                catalog.store_definition_manifest(manifest)
            for snapshot in assignment.environment_snapshots.values():
                catalog.store_environment_snapshot(snapshot)
        return self._checkpoint(
            scope,
            record,
            ActivationCheckpointKind.DEFINITIONS,
            ExternalOperationConfirmation(
                outcome=ActivationExternalOutcome.CONFIRMED,
                observation_digest=provenance_digest(
                    {
                        "definitions": prepared.definition_digests,
                        "registrations": prepared.registration_digest,
                    }
                ),
            ),
        )

    async def _ensure_worker_ready(
        self,
        scope: RuntimeScope,
        record: ActivationRecord,
        prepared: PreparedActivation,
        worker_deployment: PreparedWorkerDeployment,
    ) -> ActivationRecord:
        if record.plan.worker_action is WorkerActivationAction.NONE:
            if not _has_checkpoint(record, ActivationCheckpointKind.WORKER_READINESS):
                record = await run_blocking(
                    self._checkpoint,
                    scope,
                    record,
                    ActivationCheckpointKind.WORKER_READINESS,
                    ExternalOperationConfirmation(
                        outcome=ActivationExternalOutcome.IDEMPOTENT,
                        observation_digest=record.plan.worker_observation_digest,
                    ),
                )
            return record
        if record.worker_readiness is None and not _has_checkpoint(
            record,
            ActivationCheckpointKind.WORKER_ROLLOUT,
        ):
            if self._worker_binding is None:
                return await run_blocking(self._waiting, scope, record)
            try:
                confirmation = await self._worker_binding.rollout(
                    worker_deployment,
                    plan_digest=record.plan.plan_digest,
                )
            except ActivationControllerError:
                raise
            except Exception as exc:
                raise ActivationControllerError(
                    ActivationOutcomeCode.WORKER_UNAVAILABLE,
                    "Worker rollout binding is unavailable",
                    retryable=True,
                ) from exc
            record = await run_blocking(
                self._checkpoint,
                scope,
                record,
                ActivationCheckpointKind.WORKER_ROLLOUT,
                confirmation,
            )
        if record.worker_readiness is None:
            readiness = await self._read_worker_readiness(
                scope,
                record,
                prepared=worker_deployment,
            )
            if readiness is None:
                return await run_blocking(self._waiting, scope, record)
            record = await run_blocking(self._register_readiness, scope, record, readiness)
        _validate_worker_readiness(record, prepared)
        if not _has_checkpoint(record, ActivationCheckpointKind.WORKER_READINESS):
            record = await run_blocking(
                self._checkpoint,
                scope,
                record,
                ActivationCheckpointKind.WORKER_READINESS,
                ExternalOperationConfirmation(
                    outcome=ActivationExternalOutcome.CONFIRMED,
                    observation_digest=provenance_digest(
                        record.worker_readiness.model_dump(mode="json")
                        if record.worker_readiness is not None
                        else {}
                    ),
                ),
            )
        return record

    async def _reconcile_schedules(
        self,
        scope: RuntimeScope,
        record: ActivationRecord,
        prepared: PreparedActivation,
    ) -> ActivationRecord:
        if record.plan.schedule_action is ScheduleActivationAction.NONE or _has_checkpoint(
            record,
            ActivationCheckpointKind.SCHEDULES,
        ):
            return record
        if self._schedule_binding is None:
            raise ActivationControllerError(
                ActivationOutcomeCode.SCHEDULES_UNAVAILABLE,
                "Schedule reconciliation is unavailable for this runtime scope",
                retryable=True,
            )
        try:
            confirmation = await self._schedule_binding.reconcile(prepared)
        except ActivationControllerError:
            raise
        except Exception as exc:
            raise ActivationControllerError(
                ActivationOutcomeCode.SCHEDULES_UNAVAILABLE,
                "Schedule reconciliation binding is unavailable",
                retryable=True,
            ) from exc
        return await run_blocking(
            self._checkpoint,
            scope,
            record,
            ActivationCheckpointKind.SCHEDULES,
            confirmation,
        )

    def _switch_catalog(
        self,
        scope: RuntimeScope,
        record: ActivationRecord,
        prepared: PreparedActivation,
    ) -> ActivationRecord:
        if record.plan.definition_action is DefinitionActivationAction.NONE or _has_checkpoint(
            record,
            ActivationCheckpointKind.CATALOG_ROUTING,
        ):
            return record
        aliases = {
            name: manifest.definition_digest for name, manifest in prepared.manifests.items()
        }
        catalog = self._catalog_source.read(scope)
        inspected = catalog.inspect()
        if dict(inspected.catalog.aliases) == aliases:
            outcome = ActivationExternalOutcome.IDEMPOTENT
            version = inspected.alias_version
        else:
            if inspected.alias_version != record.plan.expected_catalog_alias_version:
                raise CatalogConflictError("Definition catalog aliases changed after planning")
            stored = catalog.replace_aliases(
                aliases,
                expected_version=record.plan.expected_catalog_alias_version,
            )
            outcome = ActivationExternalOutcome.CONFIRMED
            version = stored.version
        return self._checkpoint(
            scope,
            record,
            ActivationCheckpointKind.CATALOG_ROUTING,
            ExternalOperationConfirmation(
                outcome=outcome,
                observation_digest=provenance_digest({"aliases": aliases, "version": version}),
            ),
        )

    async def _switch_worker_routing(
        self,
        scope: RuntimeScope,
        record: ActivationRecord,
        prepared: PreparedActivation,
    ) -> ActivationRecord:
        if record.plan.routing_action is RoutingActivationAction.NONE or _has_checkpoint(
            record,
            ActivationCheckpointKind.WORKER_ROUTING,
        ):
            return record
        if self._worker_binding is None:
            observation = (
                provenance_digest(record.worker_readiness.model_dump(mode="json"))
                if record.worker_readiness is not None
                else record.plan.worker_observation_digest
            )
            confirmation = ExternalOperationConfirmation(
                outcome=ActivationExternalOutcome.IDEMPOTENT,
                observation_digest=observation,
            )
        else:
            try:
                confirmation = await self._worker_binding.route(
                    prepared,
                    plan_digest=record.plan.plan_digest,
                )
            except ActivationControllerError:
                raise
            except Exception as exc:
                raise ActivationControllerError(
                    ActivationOutcomeCode.ROUTING_UNAVAILABLE,
                    "Worker routing binding is unavailable",
                    retryable=True,
                ) from exc
        return await run_blocking(
            self._checkpoint,
            scope,
            record,
            ActivationCheckpointKind.WORKER_ROUTING,
            confirmation,
        )

    def _advance_active_pointer(
        self,
        scope: RuntimeScope,
        record: ActivationRecord,
    ) -> ActivationRecord:
        if _has_checkpoint(record, ActivationCheckpointKind.ACTIVE_POINTER):
            return record
        current = self._configuration_store.read_active(scope)
        expected_version = record.plan.expected_active_pointer_version
        expected_next_version = 1 if expected_version is None else expected_version + 1
        if (
            current is not None
            and current.revision_id == record.plan.target_revision_id
            and current.version == expected_next_version
        ):
            outcome = ActivationExternalOutcome.IDEMPOTENT
            pointer = current
        else:
            pointer = self._configuration_store.compare_and_swap_active(
                scope,
                record.plan.target_revision_id,
                expected_revision_id=record.plan.expected_active_revision_id,
                expected_version=expected_version,
            )
            outcome = ActivationExternalOutcome.CONFIRMED
        return self._checkpoint(
            scope,
            record,
            ActivationCheckpointKind.ACTIVE_POINTER,
            ExternalOperationConfirmation(
                outcome=outcome,
                observation_digest=provenance_digest(pointer.model_dump(mode="json")),
            ),
        )

    def _replace_runtime_index(
        self,
        scope: RuntimeScope,
        record: ActivationRecord,
        prepared: PreparedActivation,
    ) -> ActivationRecord:
        if _has_checkpoint(record, ActivationCheckpointKind.RUNTIME_INDEX):
            return record
        self._runtime_index.replace(
            scope,
            revision_id=prepared.revision.revision_id,
            policy_digest=prepared.policy_digest,
            artifact=prepared.artifact,
            targets=prepared.start_targets,
            triggers=prepared.bundle.triggers,
        )
        return self._checkpoint(
            scope,
            record,
            ActivationCheckpointKind.RUNTIME_INDEX,
            ExternalOperationConfirmation(
                outcome=ActivationExternalOutcome.CONFIRMED,
                observation_digest=provenance_digest(
                    {
                        "definitions": prepared.definition_digests,
                        "revision_id": str(prepared.revision.revision_id),
                        "scope_digest": scope.digest,
                    }
                ),
            ),
        )

    async def _worker_observation(self, scope: RuntimeScope) -> WorkerDeploymentObservation:
        if self._worker_binding is not None:
            try:
                return await self._worker_binding.observe(scope)
            except ActivationControllerError:
                raise
            except Exception as exc:
                raise ActivationControllerError(
                    ActivationOutcomeCode.WORKER_UNAVAILABLE,
                    "Worker deployment observation is unavailable",
                    retryable=True,
                ) from exc
        indexed = self._runtime_index.snapshot(scope)
        definitions = (
            frozenset(target.manifest.definition_digest for target in indexed.targets.values())
            if indexed is not None
            else frozenset()
        )
        return WorkerDeploymentObservation(
            observation_digest=provenance_digest(
                {
                    "artifact": (
                        indexed.artifact.model_dump(mode="json") if indexed is not None else None
                    ),
                    "definitions": sorted(definitions),
                    "scope_digest": scope.digest,
                }
            ),
            registered_definition_digests=definitions,
            active_artifact=indexed.artifact if indexed is not None else None,
        )

    async def _schedule_observation(self, scope: RuntimeScope) -> str:
        if self._schedule_binding is None:
            return provenance_digest({"binding": "unavailable", "scope_digest": scope.digest})
        try:
            observed = await self._schedule_binding.observe(scope)
            return _PROVENANCE_DIGEST.validate_python(observed)
        except ActivationControllerError:
            raise
        except Exception as exc:
            raise ActivationControllerError(
                ActivationOutcomeCode.SCHEDULES_UNAVAILABLE,
                "Schedule observation is unavailable",
                retryable=True,
            ) from exc

    async def _read_worker_readiness(
        self,
        scope: RuntimeScope,
        record: ActivationRecord,
        *,
        prepared: PreparedWorkerDeployment | None = None,
    ) -> WorkerReadinessRegistration | None:
        if record.worker_readiness is not None:
            return record.worker_readiness
        if self._worker_binding is None:
            return None
        materialized = prepared or (await run_blocking(self._prepare_record, scope, record))[1]
        try:
            return await self._worker_binding.readiness(materialized)
        except ActivationControllerError:
            raise
        except Exception as exc:
            raise ActivationControllerError(
                ActivationOutcomeCode.WORKER_UNAVAILABLE,
                "Worker readiness observation is unavailable",
                retryable=True,
            ) from exc

    def _register_readiness(
        self,
        scope: RuntimeScope,
        record: ActivationRecord,
        readiness: WorkerReadinessRegistration,
    ) -> ActivationRecord:
        updated = register_worker_readiness(record, readiness, occurred_at=self._now())
        if updated == record:
            return record
        return self._activation_store.update_activation(
            scope,
            updated,
            expected_version=record.version,
        )

    def _start_or_resume(
        self,
        scope: RuntimeScope,
        record: ActivationRecord,
    ) -> ActivationRecord:
        if record.state in {ActivationState.PENDING, ActivationState.FAILED}:
            return self._transition(scope, record, ActivationState.RUNNING)
        return record

    def _waiting(
        self,
        scope: RuntimeScope,
        record: ActivationRecord,
    ) -> ActivationRecord:
        if record.state is ActivationState.WAITING_FOR_READINESS:
            return self._release(scope, record)
        return self._transition(
            scope,
            record,
            ActivationState.WAITING_FOR_READINESS,
            release_lease=True,
        )

    def _terminal(
        self,
        scope: RuntimeScope,
        record: ActivationRecord,
        state: ActivationState,
        outcome: ActivationOutcomeCode,
    ) -> ActivationRecord:
        current = self._activation_store.read_activation(scope, record.activation_id)
        if current.state in {
            ActivationState.APPLIED,
            ActivationState.SUPERSEDED,
            ActivationState.ROLLED_BACK,
        }:
            return self._observe(current)
        return self._observe(
            self._transition(
                scope,
                current,
                state,
                outcome=outcome,
                release_lease=True,
            )
        )

    def _acquire(
        self,
        scope: RuntimeScope,
        record: ActivationRecord,
    ) -> ActivationRecord:
        now = self._now()
        try:
            leased = acquire_activation_lease(
                record,
                owner_digest=self._controller_digest,
                acquired_at=now,
                expires_at=now + self._lease_duration,
            )
        except ValueError as exc:
            raise ActivationConflictError(record.activation_id) from exc
        return self._activation_store.update_activation(
            scope,
            leased,
            expected_version=record.version,
        )

    def _release(
        self,
        scope: RuntimeScope,
        record: ActivationRecord,
    ) -> ActivationRecord:
        if record.lease_owner_digest is None:
            return record
        released = release_activation_lease(
            record,
            owner_digest=self._controller_digest,
            released_at=self._now(),
        )
        return self._activation_store.update_activation(
            scope,
            released,
            expected_version=record.version,
        )

    def _transition(
        self,
        scope: RuntimeScope,
        record: ActivationRecord,
        state: ActivationState,
        *,
        outcome: ActivationOutcomeCode | None = None,
        release_lease: bool = False,
    ) -> ActivationRecord:
        updated = transition_activation(
            record,
            state,
            occurred_at=self._now(),
            outcome_code=outcome,
            release_lease=release_lease,
        )
        return self._activation_store.update_activation(
            scope,
            updated,
            expected_version=record.version,
        )

    def _checkpoint(
        self,
        scope: RuntimeScope,
        record: ActivationRecord,
        kind: ActivationCheckpointKind,
        confirmation: ExternalOperationConfirmation,
    ) -> ActivationRecord:
        updated = record_activation_checkpoint(
            record,
            kind=kind,
            outcome=confirmation.outcome,
            observation_digest=confirmation.observation_digest,
            occurred_at=self._now(),
        )
        if updated == record:
            return record
        return self._activation_store.update_activation(
            scope,
            updated,
            expected_version=record.version,
        )

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ActivationUnavailableError("Activation clock must be timezone-aware")
        return value.astimezone(UTC)

    def _observe(self, record: ActivationRecord) -> ActivationRecord:
        if self._metrics is not None:
            self._metrics.record_activation(ActivationMetricState(record.state.value))
        return record


def _resolved_bundle(record: RevisionRecord) -> ConfigurationBundle:
    if not isinstance(record.bundle, ConfigurationBundle):
        raise ActivationIntegrityError("Activation revision is not a resolved configuration bundle")
    return record.bundle


def _has_checkpoint(record: ActivationRecord, kind: ActivationCheckpointKind) -> bool:
    return any(checkpoint.kind is kind for checkpoint in record.checkpoints)


def _validate_worker_readiness(
    record: ActivationRecord,
    prepared: PreparedActivation,
) -> None:
    readiness = record.worker_readiness
    if readiness is None:
        raise ActivationIntegrityError("Worker readiness was not registered")
    if readiness.task_queue_identity_digest != prepared.task_queue_identity_digest:
        raise ActivationControllerError(
            ActivationOutcomeCode.WORKER_UNAVAILABLE,
            "Worker readiness targets an incompatible task queue",
            retryable=False,
        )


def _schedule_observation_digest(observed: Mapping[str, ObservedSchedule]) -> str:
    return provenance_digest(
        {
            "schedules": [
                {
                    "desired_digest": schedule.desired_digest,
                    "owner": schedule.owner,
                    "schedule_id": schedule.schedule_id,
                    "schedule_name": schedule.schedule_name,
                    "scope_digest": schedule.scope_digest,
                }
                for schedule in sorted(observed.values(), key=lambda value: value.schedule_id)
            ]
        }
    )
