from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from justflow.config.models import FlowStep, WorkflowConfig
from justflow.config.runtime_limits import DEFAULT_RUNTIME_LIMITS
from justflow.config.schedules import IntervalScheduleSpec
from justflow.config.triggers import ScheduleTriggerDeclaration, TriggersConfig
from justflow.configuration.activation import (
    ActivationCheckpointKind,
    ActivationExternalOutcome,
    ActivationOutcomeCode,
    ActivationState,
    WorkerActivationAction,
    WorkerReadinessRegistration,
)
from justflow.configuration.activation_errors import ActivationConflictError
from justflow.configuration.activation_sqlite import SqliteActivationStore
from justflow.configuration.models import (
    ConfigurationBundle,
    ConfigurationSnapshot,
    RevisionIdentity,
    RevisionRecord,
)
from justflow.configuration.sqlite import SqliteConfigurationStore
from justflow.definitions.catalog import CatalogError, CatalogStore, StoredCatalogObject
from justflow.definitions.environment import (
    build_execution_environment_snapshot,
    sanitized_runtime_configuration,
)
from justflow.definitions.manifest import ENGINE_WORKFLOW_ABI, build_definition_manifests
from justflow.definitions.routing import DefinitionStartTarget, WorkerDeployment
from justflow.provenance import (
    LOCAL_ARTIFACT_DIGEST,
    RuntimeProfile,
    WorkerArtifactIdentity,
    provenance_digest,
)
from justflow.runtime.configuration_activation import (
    ActivationControllerError,
    ConfigurationActivationController,
    ExternalOperationConfirmation,
    PreparedActivation,
    PreparedRuntimeIndex,
    PreparedWorkerDeployment,
    ReconcilerScheduleActivationBinding,
    ScopedDefinitionCatalogSource,
    WorkerDeploymentObservation,
    WorkerDeploymentRetirement,
)
from justflow.runtime.schedules import compile_schedule
from justflow.scope import LOCAL_RUNTIME_SCOPE, RuntimeScope

NOW = datetime(2026, 8, 4, 12, tzinfo=UTC)
POLICY_DIGEST = provenance_digest({"policy": "current"})
TASK_QUEUE_DIGEST = provenance_digest({"task_queue": "orders"})
SCHEDULE_NAME = "hourly_orders"
WORKFLOW_NAME = "orders"
TASK_QUEUE = "orders"
TEMPORAL_NAMESPACE = "default"
SCHEDULE_INTERVAL_SECONDS = 60
ARTIFACT = WorkerArtifactIdentity(
    deployment_name="justflow",
    build_id="current",
    artifact_digest=LOCAL_ARTIFACT_DIGEST,
    package_version="development",
)


class StaticPreparationSource:
    def __init__(self, preparations: dict[RevisionIdentity, PreparedActivation]) -> None:
        self._preparations = preparations

    def prepare(
        self,
        scope: RuntimeScope,
        revision: RevisionRecord,
    ) -> PreparedActivation:
        prepared = self._preparations[revision.revision_id]
        if prepared.scope != scope or prepared.revision != revision:
            raise ValueError("Prepared activation registration is inconsistent")
        return prepared

    def prepare_worker_deployment(
        self,
        target: PreparedActivation,
    ) -> PreparedWorkerDeployment:
        return PreparedWorkerDeployment(target=target, assignments=(target,))


class ReadyWorkerBinding:
    def __init__(self) -> None:
        self.rollouts = 0
        self.routes = 0
        self._prepared: PreparedWorkerDeployment | None = None
        self._active_artifact: WorkerArtifactIdentity | None = None

    async def observe(self, scope: RuntimeScope) -> WorkerDeploymentObservation:
        return WorkerDeploymentObservation(
            observation_digest=provenance_digest(
                {
                    "active_artifact": (
                        self._active_artifact.model_dump(mode="json")
                        if self._active_artifact is not None
                        else None
                    ),
                    "scope_digest": scope.digest,
                }
            ),
            registered_definition_digests=frozenset(),
            active_artifact=self._active_artifact,
        )

    async def rollout(
        self,
        prepared: PreparedWorkerDeployment,
        *,
        plan_digest: str,
    ) -> ExternalOperationConfirmation:
        self.rollouts += 1
        self._prepared = prepared
        return ExternalOperationConfirmation(
            outcome=ActivationExternalOutcome.CONFIRMED,
            observation_digest=provenance_digest({"rollout": plan_digest}),
        )

    async def readiness(
        self,
        prepared: PreparedWorkerDeployment,
    ) -> WorkerReadinessRegistration | None:
        if self._prepared != prepared:
            return None
        return readiness(prepared.target, worker_deployment=prepared)

    async def route(
        self,
        prepared: PreparedActivation,
        *,
        plan_digest: str,
    ) -> ExternalOperationConfirmation:
        self.routes += 1
        self._active_artifact = prepared.artifact
        return ExternalOperationConfirmation(
            outcome=ActivationExternalOutcome.CONFIRMED,
            observation_digest=provenance_digest({"routing": plan_digest}),
        )

    async def retire(
        self,
        retirement: WorkerDeploymentRetirement,
    ) -> ExternalOperationConfirmation:
        return ExternalOperationConfirmation(
            outcome=ActivationExternalOutcome.CONFIRMED,
            observation_digest=retirement.observation_digest,
        )


class FailingWorkerBinding(ReadyWorkerBinding):
    async def rollout(
        self,
        prepared: PreparedWorkerDeployment,
        *,
        plan_digest: str,
    ) -> ExternalOperationConfirmation:
        raise ActivationControllerError(
            ActivationOutcomeCode.WORKER_UNAVAILABLE,
            "Worker rollout is unavailable",
            retryable=True,
        )


class BlockingWorkerBinding(ReadyWorkerBinding):
    def __init__(self) -> None:
        super().__init__()
        self.rollout_started = asyncio.Event()
        self.release_rollout = asyncio.Event()

    async def rollout(
        self,
        prepared: PreparedWorkerDeployment,
        *,
        plan_digest: str,
    ) -> ExternalOperationConfirmation:
        self.rollout_started.set()
        await self.release_rollout.wait()
        return await super().rollout(prepared, plan_digest=plan_digest)


class FailingScheduleBinding:
    def __init__(self) -> None:
        self.reconciliations = 0

    async def observe(self, scope: RuntimeScope) -> str:
        return provenance_digest({"schedules": "empty", "scope_digest": scope.digest})

    async def reconcile(
        self,
        prepared: PreparedActivation,
    ) -> ExternalOperationConfirmation:
        self.reconciliations += 1
        raise RuntimeError("synthetic-schedule-credential")


class SuccessfulScheduleBinding(FailingScheduleBinding):
    async def reconcile(
        self,
        prepared: PreparedActivation,
    ) -> ExternalOperationConfirmation:
        self.reconciliations += 1
        return ExternalOperationConfirmation(
            outcome=ActivationExternalOutcome.CONFIRMED,
            observation_digest=provenance_digest(
                {
                    "schedules": sorted(prepared.desired_schedules),
                    "scope_digest": prepared.scope.digest,
                }
            ),
        )


class FakeScheduleReconciler:
    async def observe(self) -> dict[str, object]:
        return {}

    async def plan(self, desired: dict[str, object]) -> SimpleNamespace:
        return SimpleNamespace(
            desired=desired,
            plan_digest=provenance_digest({"desired": sorted(desired)}),
        )

    async def apply(
        self,
        plan: SimpleNamespace,
        *,
        confirmation: str,
    ) -> SimpleNamespace:
        assert confirmation == plan.plan_digest
        return SimpleNamespace(
            successful=True,
            items=(),
            plan_digest=plan.plan_digest,
        )


class FailingCatalogStore(CatalogStore):
    def replace_aliases(
        self,
        aliases: Mapping[str, str],
        *,
        expected_version: str | None,
    ) -> StoredCatalogObject:
        del aliases, expected_version
        raise CatalogError("synthetic-catalog-credential")


class CrashAfterActiveStore(SqliteConfigurationStore):
    def __init__(self, path) -> None:
        super().__init__(path)
        self.crashed = False

    def compare_and_swap_active(
        self,
        scope: RuntimeScope,
        revision_id: RevisionIdentity,
        *,
        expected_revision_id: RevisionIdentity | None,
        expected_version: int | None = None,
    ):
        pointer = super().compare_and_swap_active(
            scope,
            revision_id,
            expected_revision_id=expected_revision_id,
            expected_version=expected_version,
        )
        if not self.crashed:
            self.crashed = True
            raise RuntimeError("simulated process crash")
        return pointer


class SyntheticWorkflow:
    pass


def prepared(revision: RevisionRecord) -> PreparedActivation:
    return PreparedActivation(
        scope=LOCAL_RUNTIME_SCOPE,
        revision=revision,
        policy_digest=POLICY_DIGEST,
        artifact=ARTIFACT,
        task_queue_identity_digest=TASK_QUEUE_DIGEST,
        manifests={},
        environment_snapshots={},
        start_targets={},
        desired_schedules={},
    )


def test_worker_retirement_requires_replay_safe_evidence() -> None:
    replacement = ARTIFACT.model_copy(update={"build_id": "replacement"})
    retirement = WorkerDeploymentRetirement(
        retiring_artifact=ARTIFACT,
        replacement_artifact=replacement,
        observation_digest=provenance_digest({"retirement": "safe"}),
        observed_at=NOW,
    )

    assert retirement.open_execution_count == 0
    with pytest.raises(ValueError, match="distinct replacement"):
        WorkerDeploymentRetirement(
            retiring_artifact=ARTIFACT,
            replacement_artifact=ARTIFACT,
            observation_digest=retirement.observation_digest,
            observed_at=NOW,
        )
    with pytest.raises(ValueError, match="timezone-aware"):
        WorkerDeploymentRetirement(
            retiring_artifact=ARTIFACT,
            replacement_artifact=replacement,
            observation_digest=retirement.observation_digest,
            observed_at=NOW.replace(tzinfo=None),
        )


def test_worker_deployment_preparation_requires_target_and_unique_scopes(tmp_path) -> None:
    configuration_store = SqliteConfigurationStore(tmp_path / "configuration.sqlite3")
    try:
        revision = configuration_store.create_revision(
            LOCAL_RUNTIME_SCOPE,
            ConfigurationBundle(workflows={}, triggers=TriggersConfig(triggers={})),
            parent_revision_id=None,
        )
        target = prepared(revision)

        with pytest.raises(ValueError, match="omits"):
            PreparedWorkerDeployment(target=target, assignments=())
        with pytest.raises(ValueError, match="duplicate scopes"):
            PreparedWorkerDeployment(target=target, assignments=(target, target))
    finally:
        configuration_store.close()


def activation_bundle(*, scheduled: bool = False) -> ConfigurationBundle:
    workflow = WorkflowConfig(
        workflow=WORKFLOW_NAME,
        steps={},
        flow=[FlowStep(name="done", terminal=True)],
    )
    triggers = (
        TriggersConfig(
            triggers={
                SCHEDULE_NAME: ScheduleTriggerDeclaration(
                    workflow=WORKFLOW_NAME,
                    spec=IntervalScheduleSpec(every_seconds=SCHEDULE_INTERVAL_SECONDS),
                )
            }
        )
        if scheduled
        else TriggersConfig(triggers={})
    )
    return ConfigurationBundle(
        workflows={WORKFLOW_NAME: workflow},
        triggers=triggers,
    )


def prepared_runtime(
    revision: RevisionRecord,
    tmp_path,
) -> PreparedActivation:
    bundle = revision.bundle
    if not isinstance(bundle, ConfigurationBundle):
        raise TypeError("Test activation revision is not resolved")
    manifest = build_definition_manifests(
        bundle.workflows,
        {},
        DEFAULT_RUNTIME_LIMITS,
    )[WORKFLOW_NAME]
    deployment = WorkerDeployment(
        artifact_identity=ARTIFACT,
        compatible_engine_workflow_abis=frozenset({ENGINE_WORKFLOW_ABI}),
    )
    execution_configuration = ConfigurationSnapshot(
        scope_digest=LOCAL_RUNTIME_SCOPE.digest,
        revision_id=revision.revision_id,
        bundle=bundle,
    ).execution_identity
    snapshot = build_execution_environment_snapshot(
        manifest=manifest,
        artifact_identity=ARTIFACT,
        catalog_backend=CatalogStore(tmp_path / "preparation-catalog").backend_identity,
        runtime_profile=RuntimeProfile.LOCAL,
        configuration=sanitized_runtime_configuration(
            temporal_namespace=TEMPORAL_NAMESPACE,
            temporal_task_queue=TASK_QUEUE,
            payload_protection_mode="plaintext",
            broker_providers={},
            runtime_limits=manifest.deterministic_policy.runtime_limits,
        ),
        scope=LOCAL_RUNTIME_SCOPE,
        execution_configuration=execution_configuration,
    )
    target = DefinitionStartTarget(
        manifest=manifest,
        workflow_class=SyntheticWorkflow,
        deployment=deployment,
        environment_snapshot_digest=snapshot.snapshot_digest,
        scope_digest=LOCAL_RUNTIME_SCOPE.digest,
        execution_configuration=execution_configuration,
    )
    desired_schedules = {}
    for name, declaration in bundle.triggers.schedules.items():
        desired = compile_schedule(
            name,
            declaration,
            target,
            task_queue=TASK_QUEUE,
            scope=LOCAL_RUNTIME_SCOPE,
        )
        desired_schedules[desired.schedule_id] = desired
    return PreparedActivation(
        scope=LOCAL_RUNTIME_SCOPE,
        revision=revision,
        policy_digest=POLICY_DIGEST,
        artifact=ARTIFACT,
        task_queue_identity_digest=TASK_QUEUE_DIGEST,
        manifests={WORKFLOW_NAME: manifest},
        environment_snapshots={snapshot.snapshot_digest: snapshot},
        start_targets={WORKFLOW_NAME: target},
        desired_schedules=desired_schedules,
    )


def readiness(
    value: PreparedActivation,
    *,
    worker_deployment: PreparedWorkerDeployment | None = None,
) -> WorkerReadinessRegistration:
    deployment = worker_deployment or PreparedWorkerDeployment(
        target=value,
        assignments=(value,),
    )
    return WorkerReadinessRegistration(
        configuration_revision_id=value.revision.revision_id,
        artifact=value.artifact,
        definition_digests=deployment.definition_digests,
        task_queue_identity_digest=value.task_queue_identity_digest,
        deployment_registration_digest=deployment.registration_digest,
        registered_at=NOW,
    )


def controller(
    tmp_path,
    *,
    worker_binding=None,
    schedule_binding=None,
    configuration_store: SqliteConfigurationStore | None = None,
    bundle: ConfigurationBundle | None = None,
    preparation_factory: Callable[[RevisionRecord], PreparedActivation] | None = None,
    catalog_store: CatalogStore | None = None,
    controller_identity: str = "controller-one",
):
    config_store = configuration_store or SqliteConfigurationStore(
        tmp_path / "configuration.sqlite3"
    )
    activation_store = SqliteActivationStore(tmp_path / "activation.sqlite3")
    revision = config_store.create_revision(
        LOCAL_RUNTIME_SCOPE,
        bundle or ConfigurationBundle(workflows={}, triggers=TriggersConfig(triggers={})),
        parent_revision_id=None,
    )
    prepared_value = (
        preparation_factory(revision) if preparation_factory is not None else prepared(revision)
    )
    runtime_index = PreparedRuntimeIndex()
    value = ConfigurationActivationController(
        configuration_store=config_store,
        activation_store=activation_store,
        preparation_source=StaticPreparationSource({revision.revision_id: prepared_value}),
        catalog_source=ScopedDefinitionCatalogSource(
            {LOCAL_RUNTIME_SCOPE: catalog_store or CatalogStore(tmp_path / "catalog")}
        ),
        runtime_index=runtime_index,
        controller_identity=controller_identity,
        worker_binding=worker_binding,
        schedule_binding=schedule_binding,
        clock=lambda: NOW,
    )
    return value, config_store, activation_store, runtime_index, revision


@pytest.mark.asyncio
async def test_activation_waits_for_exact_external_worker_readiness(tmp_path) -> None:
    value, config_store, activation_store, runtime_index, revision = controller(tmp_path)
    try:
        plan = await value.plan(LOCAL_RUNTIME_SCOPE, revision.revision_id)
        waiting = await value.activate(
            LOCAL_RUNTIME_SCOPE,
            revision.revision_id,
            plan_digest=plan.plan_digest,
            idempotency_key="activate-orders",
            actor_identity="operator",
            correlation_identity="request-1",
        )

        assert waiting.state is ActivationState.WAITING_FOR_READINESS
        assert config_store.read_active(LOCAL_RUNTIME_SCOPE) is None
        assert runtime_index.snapshot(LOCAL_RUNTIME_SCOPE) is None

        applied = await value.register_readiness(
            LOCAL_RUNTIME_SCOPE,
            waiting.activation_id,
            readiness(prepared(revision)),
        )

        assert applied.state is ActivationState.APPLIED
        active = config_store.read_active(LOCAL_RUNTIME_SCOPE)
        indexed = runtime_index.snapshot(LOCAL_RUNTIME_SCOPE)
        assert active is not None and active.revision_id == revision.revision_id
        assert indexed is not None and indexed.revision_id == revision.revision_id
    finally:
        activation_store.close()
        config_store.close()


@pytest.mark.asyncio
async def test_activation_rejects_stale_plan_before_side_effects(tmp_path) -> None:
    binding = ReadyWorkerBinding()
    value, config_store, activation_store, _, revision = controller(
        tmp_path,
        worker_binding=binding,
    )
    try:
        plan = await value.plan(LOCAL_RUNTIME_SCOPE, revision.revision_id)
        config_store.compare_and_swap_active(
            LOCAL_RUNTIME_SCOPE,
            revision.revision_id,
            expected_revision_id=None,
        )

        with pytest.raises(ActivationControllerError) as error:
            await value.activate(
                LOCAL_RUNTIME_SCOPE,
                revision.revision_id,
                plan_digest=plan.plan_digest,
                idempotency_key="activate-orders",
                actor_identity="operator",
                correlation_identity="request-1",
            )

        assert error.value.code is ActivationOutcomeCode.STALE_PLAN
        assert binding.rollouts == 0
    finally:
        activation_store.close()
        config_store.close()


async def test_activation_retry_recovers_original_result_after_switch(tmp_path) -> None:
    binding = ReadyWorkerBinding()
    value, config_store, activation_store, _, revision = controller(
        tmp_path, worker_binding=binding
    )
    try:
        plan = await value.plan(LOCAL_RUNTIME_SCOPE, revision.revision_id)
        args = {
            "plan_digest": plan.plan_digest,
            "idempotency_key": "activation-retry",
            "actor_identity": "operator",
            "correlation_identity": "request",
        }
        first = await value.activate(LOCAL_RUNTIME_SCOPE, revision.revision_id, **args)
        assert first.state is ActivationState.APPLIED
        assert await value.activate(LOCAL_RUNTIME_SCOPE, revision.revision_id, **args) == first
        assert binding.rollouts == 1
    finally:
        activation_store.close()
        config_store.close()


@pytest.mark.asyncio
async def test_activation_resumes_after_crash_following_active_pointer_write(tmp_path) -> None:
    config_store = CrashAfterActiveStore(tmp_path / "configuration.sqlite3")
    binding = ReadyWorkerBinding()
    value, _, activation_store, runtime_index, revision = controller(
        tmp_path,
        worker_binding=binding,
        configuration_store=config_store,
    )
    try:
        plan = await value.plan(LOCAL_RUNTIME_SCOPE, revision.revision_id)
        with pytest.raises(RuntimeError, match="simulated process crash"):
            await value.activate(
                LOCAL_RUNTIME_SCOPE,
                revision.revision_id,
                plan_digest=plan.plan_digest,
                idempotency_key="activate-orders",
                actor_identity="operator",
                correlation_identity="request-1",
            )
        activation_id = next(
            iter(activation_store.list_activations(LOCAL_RUNTIME_SCOPE, limit=1).activations)
        ).activation_id

        applied = await value.resume(LOCAL_RUNTIME_SCOPE, activation_id)

        assert applied.state is ActivationState.APPLIED
        indexed = runtime_index.snapshot(LOCAL_RUNTIME_SCOPE)
        assert indexed is not None and indexed.revision_id == revision.revision_id
        assert [checkpoint.kind for checkpoint in applied.checkpoints].count(
            ActivationCheckpointKind.ACTIVE_POINTER
        ) == 1
        assert binding.rollouts == 1
    finally:
        activation_store.close()
        config_store.close()


@pytest.mark.asyncio
async def test_activation_records_safe_external_failure(tmp_path) -> None:
    value, config_store, activation_store, _, revision = controller(
        tmp_path,
        worker_binding=FailingWorkerBinding(),
    )
    try:
        plan = await value.plan(LOCAL_RUNTIME_SCOPE, revision.revision_id)
        with pytest.raises(ActivationControllerError) as error:
            await value.activate(
                LOCAL_RUNTIME_SCOPE,
                revision.revision_id,
                plan_digest=plan.plan_digest,
                idempotency_key="activate-orders",
                actor_identity="operator@example.invalid",
                correlation_identity="request-1",
            )

        assert error.value.code is ActivationOutcomeCode.WORKER_UNAVAILABLE
        failed = activation_store.list_activations(LOCAL_RUNTIME_SCOPE, limit=1).activations[0]
        detail = activation_store.read_activation(LOCAL_RUNTIME_SCOPE, failed.activation_id)
        assert detail.state is ActivationState.FAILED
        assert detail.outcome_code is ActivationOutcomeCode.WORKER_UNAVAILABLE
        assert "operator@example.invalid" not in detail.model_dump_json()
    finally:
        activation_store.close()
        config_store.close()


@pytest.mark.asyncio
async def test_schedule_partial_failure_stops_routing_and_active_pointer(tmp_path) -> None:
    worker_binding = ReadyWorkerBinding()
    schedule_binding = FailingScheduleBinding()
    bundle = activation_bundle(scheduled=True)
    value, config_store, activation_store, _, revision = controller(
        tmp_path,
        worker_binding=worker_binding,
        schedule_binding=schedule_binding,
        bundle=bundle,
        preparation_factory=lambda record: prepared_runtime(record, tmp_path),
    )
    try:
        plan = await value.plan(LOCAL_RUNTIME_SCOPE, revision.revision_id)

        with pytest.raises(ActivationControllerError) as error:
            await value.activate(
                LOCAL_RUNTIME_SCOPE,
                revision.revision_id,
                plan_digest=plan.plan_digest,
                idempotency_key="activate-schedules",
                actor_identity="operator",
                correlation_identity="request-1",
            )

        assert error.value.code is ActivationOutcomeCode.SCHEDULES_UNAVAILABLE
        failed = activation_store.list_activations(LOCAL_RUNTIME_SCOPE, limit=1).activations[0]
        detail = activation_store.read_activation(LOCAL_RUNTIME_SCOPE, failed.activation_id)
        assert detail.state is ActivationState.FAILED
        assert schedule_binding.reconciliations == 1
        assert worker_binding.routes == 0
        assert config_store.read_active(LOCAL_RUNTIME_SCOPE) is None
        assert "credential" not in detail.model_dump_json()
    finally:
        activation_store.close()
        config_store.close()


@pytest.mark.asyncio
async def test_schedule_only_activation_reuses_the_compatible_registered_worker(tmp_path) -> None:
    config_store = SqliteConfigurationStore(tmp_path / "configuration.sqlite3")
    activation_store = SqliteActivationStore(tmp_path / "activation.sqlite3")
    catalog_store = CatalogStore(tmp_path / "catalog")
    source_revision = config_store.create_revision(
        LOCAL_RUNTIME_SCOPE,
        activation_bundle(),
        parent_revision_id=None,
    )
    target_revision = config_store.create_revision(
        LOCAL_RUNTIME_SCOPE,
        activation_bundle(scheduled=True),
        parent_revision_id=source_revision.revision_id,
    )
    source_prepared = prepared_runtime(source_revision, tmp_path)
    target_prepared = prepared_runtime(target_revision, tmp_path)
    catalog_store.publish(source_prepared.manifests)
    config_store.compare_and_swap_active(
        LOCAL_RUNTIME_SCOPE,
        source_revision.revision_id,
        expected_revision_id=None,
    )
    runtime_index = PreparedRuntimeIndex()
    runtime_index.replace(
        LOCAL_RUNTIME_SCOPE,
        revision_id=source_revision.revision_id,
        policy_digest=source_prepared.policy_digest,
        artifact=source_prepared.artifact,
        targets=source_prepared.start_targets,
        triggers=source_prepared.bundle.triggers,
    )
    schedules = SuccessfulScheduleBinding()
    value = ConfigurationActivationController(
        configuration_store=config_store,
        activation_store=activation_store,
        preparation_source=StaticPreparationSource(
            {
                source_revision.revision_id: source_prepared,
                target_revision.revision_id: target_prepared,
            }
        ),
        catalog_source=ScopedDefinitionCatalogSource({LOCAL_RUNTIME_SCOPE: catalog_store}),
        runtime_index=runtime_index,
        controller_identity="controller-one",
        schedule_binding=schedules,
        clock=lambda: NOW,
    )
    try:
        plan = await value.plan(LOCAL_RUNTIME_SCOPE, target_revision.revision_id)
        assert plan.worker_action is WorkerActivationAction.NONE

        applied = await value.activate(
            LOCAL_RUNTIME_SCOPE,
            target_revision.revision_id,
            plan_digest=plan.plan_digest,
            idempotency_key="activate-schedule-only",
            actor_identity="operator",
            correlation_identity="request-1",
        )

        assert applied.state is ActivationState.APPLIED
        assert schedules.reconciliations == 1
        active = config_store.read_active(LOCAL_RUNTIME_SCOPE)
        assert active is not None and active.revision_id == target_revision.revision_id
    finally:
        activation_store.close()
        config_store.close()


@pytest.mark.asyncio
async def test_schedule_activation_facade_is_scope_registered_and_idempotent(tmp_path) -> None:
    revision_store = SqliteConfigurationStore(tmp_path / "configuration.sqlite3")
    revision = revision_store.create_revision(
        LOCAL_RUNTIME_SCOPE,
        ConfigurationBundle(workflows={}, triggers=TriggersConfig(triggers={})),
        parent_revision_id=None,
    )
    prepared_value = prepared(revision)
    binding = ReconcilerScheduleActivationBinding({LOCAL_RUNTIME_SCOPE: FakeScheduleReconciler()})
    try:
        observed = await binding.observe(LOCAL_RUNTIME_SCOPE)
        confirmation = await binding.reconcile(prepared_value)

        assert observed == provenance_digest({"schedules": []})
        assert confirmation.outcome is ActivationExternalOutcome.IDEMPOTENT
        with pytest.raises(ActivationControllerError) as error:
            await binding.observe(
                RuntimeScope.create(
                    tenant="other",
                    application="orders",
                    environment="production",
                )
            )
        assert error.value.code is ActivationOutcomeCode.SCHEDULES_UNAVAILABLE
    finally:
        revision_store.close()


@pytest.mark.asyncio
async def test_catalog_partial_failure_stops_worker_routing_and_active_pointer(tmp_path) -> None:
    worker_binding = ReadyWorkerBinding()
    catalog_store = FailingCatalogStore(tmp_path / "catalog")
    bundle = activation_bundle()
    value, config_store, activation_store, _, revision = controller(
        tmp_path,
        worker_binding=worker_binding,
        bundle=bundle,
        preparation_factory=lambda record: prepared_runtime(record, tmp_path),
        catalog_store=catalog_store,
    )
    try:
        plan = await value.plan(LOCAL_RUNTIME_SCOPE, revision.revision_id)

        with pytest.raises(ActivationControllerError) as error:
            await value.activate(
                LOCAL_RUNTIME_SCOPE,
                revision.revision_id,
                plan_digest=plan.plan_digest,
                idempotency_key="activate-catalog",
                actor_identity="operator",
                correlation_identity="request-1",
            )

        assert error.value.code is ActivationOutcomeCode.DEFINITIONS_UNAVAILABLE
        failed = activation_store.list_activations(LOCAL_RUNTIME_SCOPE, limit=1).activations[0]
        detail = activation_store.read_activation(LOCAL_RUNTIME_SCOPE, failed.activation_id)
        assert detail.state is ActivationState.FAILED
        assert worker_binding.routes == 0
        assert config_store.read_active(LOCAL_RUNTIME_SCOPE) is None
        assert catalog_store.inspect().catalog.aliases == {}
        assert "credential" not in detail.model_dump_json()
    finally:
        activation_store.close()
        config_store.close()


@pytest.mark.asyncio
async def test_concurrent_controller_cannot_take_an_unexpired_activation_lease(tmp_path) -> None:
    worker_binding = BlockingWorkerBinding()
    first, config_store, first_store, _, revision = controller(
        tmp_path,
        worker_binding=worker_binding,
        controller_identity="controller-one",
    )
    second, _, second_store, _, _ = controller(
        tmp_path,
        worker_binding=worker_binding,
        configuration_store=config_store,
        controller_identity="controller-two",
    )
    activation_task = None
    try:
        plan = await first.plan(LOCAL_RUNTIME_SCOPE, revision.revision_id)
        activation_task = asyncio.create_task(
            first.activate(
                LOCAL_RUNTIME_SCOPE,
                revision.revision_id,
                plan_digest=plan.plan_digest,
                idempotency_key="activate-concurrently",
                actor_identity="operator",
                correlation_identity="request-1",
            )
        )
        await worker_binding.rollout_started.wait()
        activation_id = (
            first_store.list_activations(
                LOCAL_RUNTIME_SCOPE,
                limit=1,
            )
            .activations[0]
            .activation_id
        )

        with pytest.raises(ActivationConflictError):
            await second.resume(LOCAL_RUNTIME_SCOPE, activation_id)

        worker_binding.release_rollout.set()
        applied = await activation_task
        assert applied.state is ActivationState.APPLIED
        assert worker_binding.rollouts == 1
    finally:
        worker_binding.release_rollout.set()
        if activation_task is not None and not activation_task.done():
            await asyncio.gather(activation_task, return_exceptions=True)
        second_store.close()
        first_store.close()
        config_store.close()


@pytest.mark.asyncio
async def test_rollback_is_a_new_activation_of_retained_revision(tmp_path) -> None:
    config_store = SqliteConfigurationStore(tmp_path / "configuration.sqlite3")
    activation_store = SqliteActivationStore(tmp_path / "activation.sqlite3")
    first = config_store.create_revision(
        LOCAL_RUNTIME_SCOPE,
        ConfigurationBundle(workflows={}, triggers=TriggersConfig(triggers={})),
        parent_revision_id=None,
    )
    second = config_store.create_revision(
        LOCAL_RUNTIME_SCOPE,
        ConfigurationBundle(workflows={}, triggers=TriggersConfig(triggers={})),
        parent_revision_id=first.revision_id,
    )
    config_store.compare_and_swap_active(
        LOCAL_RUNTIME_SCOPE,
        first.revision_id,
        expected_revision_id=None,
    )
    runtime_index = PreparedRuntimeIndex()
    runtime_index.replace(
        LOCAL_RUNTIME_SCOPE,
        revision_id=first.revision_id,
        policy_digest=POLICY_DIGEST,
        artifact=ARTIFACT,
        targets={},
        triggers=TriggersConfig(triggers={}),
    )
    value = ConfigurationActivationController(
        configuration_store=config_store,
        activation_store=activation_store,
        preparation_source=StaticPreparationSource(
            {
                first.revision_id: prepared(first),
                second.revision_id: prepared(second),
            }
        ),
        catalog_source=ScopedDefinitionCatalogSource(
            {LOCAL_RUNTIME_SCOPE: CatalogStore(tmp_path / "catalog")}
        ),
        runtime_index=runtime_index,
        controller_identity="controller-one",
        clock=lambda: NOW,
    )
    try:
        forward_plan = await value.plan(LOCAL_RUNTIME_SCOPE, second.revision_id)
        forward = await value.activate(
            LOCAL_RUNTIME_SCOPE,
            second.revision_id,
            plan_digest=forward_plan.plan_digest,
            idempotency_key="activate-second",
            actor_identity="operator",
            correlation_identity="request-1",
        )
        rollback_plan = await value.plan(LOCAL_RUNTIME_SCOPE, first.revision_id)

        rollback = await value.rollback(
            LOCAL_RUNTIME_SCOPE,
            forward.activation_id,
            plan_digest=rollback_plan.plan_digest,
            idempotency_key="rollback-first",
            actor_identity="operator",
            correlation_identity="request-2",
        )

        assert rollback.state is ActivationState.APPLIED
        assert rollback.rollback_of_activation_id == forward.activation_id
        assert rollback.activation_id != forward.activation_id
        active = config_store.read_active(LOCAL_RUNTIME_SCOPE)
        assert active is not None and active.revision_id == first.revision_id
        assert (
            activation_store.read_activation(
                LOCAL_RUNTIME_SCOPE,
                forward.activation_id,
            ).state
            is ActivationState.ROLLED_BACK
        )
    finally:
        activation_store.close()
        config_store.close()
