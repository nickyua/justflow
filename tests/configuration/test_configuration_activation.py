from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest

from justflow.config.models import WorkflowConfig
from justflow.config.schedules import IntervalScheduleSpec
from justflow.config.settings import (
    AwsConfigurationSettings,
    FileConfigurationSettings,
    SqliteConfigurationSettings,
)
from justflow.config.triggers import ScheduleTriggerDeclaration, TriggersConfig
from justflow.configuration.activation import (
    ActivationChangeKind,
    ActivationCompatibilityRisk,
    ActivationObservations,
    ActivationOutcomeCode,
    ActivationPlan,
    ActivationRecord,
    ActivationState,
    DefinitionActivationAction,
    PublicationRecord,
    RoutingActivationAction,
    ScheduleActivationAction,
    WorkerActivationAction,
    WorkerReadinessRegistration,
    activation_identity,
    activation_request_digest,
    control_identity_digest,
    plan_configuration_activation,
    publication_identity,
)
from justflow.configuration.activation_aws import DynamoDbActivationStore
from justflow.configuration.activation_configuration import configured_activation_store
from justflow.configuration.activation_errors import (
    ActivationConflictError,
    ActivationIntegrityError,
    ActivationLimitError,
    ActivationNotFoundError,
)
from justflow.configuration.activation_sqlite import SqliteActivationStore
from justflow.configuration.activation_store import (
    acquire_activation_lease,
    complete_publication,
    register_worker_readiness,
    release_activation_lease,
    transition_activation,
)
from justflow.configuration.aws import DynamoConfigurationClient
from justflow.configuration.lifecycle import (
    ConfigurationDiscardRecord,
    complete_discard,
    discard_identity,
)
from justflow.configuration.models import ConfigurationBundle, RevisionIdentity
from justflow.provenance import LOCAL_ARTIFACT_DIGEST, WorkerArtifactIdentity, provenance_digest
from justflow.scope import LOCAL_RUNTIME_SCOPE, RuntimeScope

ACTIVE_REVISION = RevisionIdentity("1" * 64)
TARGET_REVISION = RevisionIdentity("2" * 64)
DEFINITION_DIGEST = "3" * 64
UPDATED_DEFINITION_DIGEST = "4" * 64
POLICY_DIGEST = provenance_digest({"policy": "current"})
WORKER_OBSERVATION_DIGEST = provenance_digest({"worker": "current"})
SCHEDULE_OBSERVATION_DIGEST = provenance_digest({"schedules": "current"})
TASK_QUEUE_IDENTITY_DIGEST = provenance_digest({"task_queue": "current"})
WORKER_REGISTRATION_DIGEST = provenance_digest({"worker_registration": "current"})
ARTIFACT = WorkerArtifactIdentity(
    deployment_name="justflow",
    build_id="current",
    artifact_digest=LOCAL_ARTIFACT_DIGEST,
    package_version="development",
)
UPDATED_ARTIFACT = WorkerArtifactIdentity(
    deployment_name="justflow",
    build_id="updated",
    artifact_digest=LOCAL_ARTIFACT_DIGEST,
    package_version="development",
)
NOW = datetime(2026, 8, 4, 12, tzinfo=UTC)
SECOND_SCOPE = RuntimeScope.create(
    tenant="other",
    application="justflow",
    environment="development",
)


def workflow(*, description: str = "") -> WorkflowConfig:
    return WorkflowConfig.model_validate(
        {
            "workflow": "orders",
            "description": description,
            "steps": {},
            "flow": [{"name": "done", "terminal": True}],
        }
    )


def bundle(
    *,
    description: str = "",
    include_workflow: bool = True,
    include_schedule: bool = False,
) -> ConfigurationBundle:
    workflows = {"orders": workflow(description=description)} if include_workflow else {}
    triggers = (
        TriggersConfig(
            triggers={
                "daily": ScheduleTriggerDeclaration(
                    workflow="orders",
                    spec=IntervalScheduleSpec(every_seconds=60),
                )
            }
        )
        if include_schedule
        else TriggersConfig(triggers={})
    )
    return ConfigurationBundle(workflows=workflows, triggers=triggers)


@dataclass(frozen=True, kw_only=True)
class ActivationPlanningCase:
    id: str
    active: ConfigurationBundle
    target: ConfigurationBundle
    active_revision_id: RevisionIdentity
    target_revision_id: RevisionIdentity
    active_artifact: WorkerArtifactIdentity
    target_artifact: WorkerArtifactIdentity
    definition_digest: str
    registered_definition_digests: frozenset[str]
    expected_change_kinds: frozenset[ActivationChangeKind]
    expected_worker_action: WorkerActivationAction
    expected_definition_action: DefinitionActivationAction
    expected_routing_action: RoutingActivationAction
    expected_schedule_action: ScheduleActivationAction
    expected_risks: tuple[ActivationCompatibilityRisk, ...]


ACTIVATION_PLANNING_CASES = [
    ActivationPlanningCase(
        id="unchanged-active-revision",
        active=bundle(),
        target=bundle(),
        active_revision_id=ACTIVE_REVISION,
        target_revision_id=ACTIVE_REVISION,
        active_artifact=ARTIFACT,
        target_artifact=ARTIFACT,
        definition_digest=DEFINITION_DIGEST,
        registered_definition_digests=frozenset({DEFINITION_DIGEST}),
        expected_change_kinds=frozenset(),
        expected_worker_action=WorkerActivationAction.NONE,
        expected_definition_action=DefinitionActivationAction.NONE,
        expected_routing_action=RoutingActivationAction.NONE,
        expected_schedule_action=ScheduleActivationAction.NONE,
        expected_risks=(),
    ),
    ActivationPlanningCase(
        id="schedule-only",
        active=bundle(),
        target=bundle(include_schedule=True),
        active_revision_id=ACTIVE_REVISION,
        target_revision_id=TARGET_REVISION,
        active_artifact=ARTIFACT,
        target_artifact=ARTIFACT,
        definition_digest=DEFINITION_DIGEST,
        registered_definition_digests=frozenset({DEFINITION_DIGEST}),
        expected_change_kinds=frozenset({ActivationChangeKind.SCHEDULE}),
        expected_worker_action=WorkerActivationAction.NONE,
        expected_definition_action=DefinitionActivationAction.NONE,
        expected_routing_action=RoutingActivationAction.SWITCH,
        expected_schedule_action=ScheduleActivationAction.RECONCILE,
        expected_risks=(ActivationCompatibilityRisk.EXTERNAL_ROUTING_CHANGE,),
    ),
    ActivationPlanningCase(
        id="workflow-update",
        active=bundle(),
        target=bundle(description="updated"),
        active_revision_id=ACTIVE_REVISION,
        target_revision_id=TARGET_REVISION,
        active_artifact=ARTIFACT,
        target_artifact=ARTIFACT,
        definition_digest=UPDATED_DEFINITION_DIGEST,
        registered_definition_digests=frozenset({DEFINITION_DIGEST}),
        expected_change_kinds=frozenset({ActivationChangeKind.WORKFLOW}),
        expected_worker_action=WorkerActivationAction.ROLLOUT,
        expected_definition_action=DefinitionActivationAction.PUBLISH,
        expected_routing_action=RoutingActivationAction.SWITCH,
        expected_schedule_action=ScheduleActivationAction.NONE,
        expected_risks=(
            ActivationCompatibilityRisk.REPLAY_COMPATIBILITY,
            ActivationCompatibilityRisk.EXTERNAL_ROUTING_CHANGE,
        ),
    ),
    ActivationPlanningCase(
        id="workflow-deletion",
        active=bundle(),
        target=bundle(include_workflow=False),
        active_revision_id=ACTIVE_REVISION,
        target_revision_id=TARGET_REVISION,
        active_artifact=ARTIFACT,
        target_artifact=ARTIFACT,
        definition_digest=UPDATED_DEFINITION_DIGEST,
        registered_definition_digests=frozenset({DEFINITION_DIGEST}),
        expected_change_kinds=frozenset({ActivationChangeKind.DELETION}),
        expected_worker_action=WorkerActivationAction.ROLLOUT,
        expected_definition_action=DefinitionActivationAction.PUBLISH,
        expected_routing_action=RoutingActivationAction.SWITCH,
        expected_schedule_action=ScheduleActivationAction.NONE,
        expected_risks=(
            ActivationCompatibilityRisk.OPEN_EXECUTION_RETENTION,
            ActivationCompatibilityRisk.REPLAY_COMPATIBILITY,
            ActivationCompatibilityRisk.EXTERNAL_ROUTING_CHANGE,
        ),
    ),
    ActivationPlanningCase(
        id="artifact-update",
        active=bundle(),
        target=bundle(),
        active_revision_id=ACTIVE_REVISION,
        target_revision_id=TARGET_REVISION,
        active_artifact=ARTIFACT,
        target_artifact=UPDATED_ARTIFACT,
        definition_digest=DEFINITION_DIGEST,
        registered_definition_digests=frozenset({DEFINITION_DIGEST}),
        expected_change_kinds=frozenset({ActivationChangeKind.ARTIFACT}),
        expected_worker_action=WorkerActivationAction.ROLLOUT,
        expected_definition_action=DefinitionActivationAction.NONE,
        expected_routing_action=RoutingActivationAction.SWITCH,
        expected_schedule_action=ScheduleActivationAction.NONE,
        expected_risks=(
            ActivationCompatibilityRisk.REPLAY_COMPATIBILITY,
            ActivationCompatibilityRisk.EXTERNAL_ROUTING_CHANGE,
        ),
    ),
]


@pytest.mark.parametrize(
    "case",
    ACTIVATION_PLANNING_CASES,
    ids=lambda case: case.id,
)
def test_activation_planning(case: ActivationPlanningCase) -> None:
    observations = ActivationObservations(
        active_pointer_version=1,
        catalog_alias_version="catalog-version",
        policy_digest=POLICY_DIGEST,
        worker_observation_digest=WORKER_OBSERVATION_DIGEST,
        schedule_observation_digest=SCHEDULE_OBSERVATION_DIGEST,
        registered_definition_digests=case.registered_definition_digests,
    )

    plan = plan_configuration_activation(
        scope_digest=LOCAL_RUNTIME_SCOPE.digest,
        active_revision_id=case.active_revision_id,
        active=case.active,
        target_revision_id=case.target_revision_id,
        target=case.target,
        active_artifact=case.active_artifact,
        target_artifact=case.target_artifact,
        target_task_queue_identity_digest=TASK_QUEUE_IDENTITY_DIGEST,
        target_worker_registration_digest=WORKER_REGISTRATION_DIGEST,
        active_policy_digest=POLICY_DIGEST,
        target_definition_digests={"orders": case.definition_digest},
        observations=observations,
    )
    repeated = plan_configuration_activation(
        scope_digest=LOCAL_RUNTIME_SCOPE.digest,
        active_revision_id=case.active_revision_id,
        active=case.active,
        target_revision_id=case.target_revision_id,
        target=case.target,
        active_artifact=case.active_artifact,
        target_artifact=case.target_artifact,
        target_task_queue_identity_digest=TASK_QUEUE_IDENTITY_DIGEST,
        target_worker_registration_digest=WORKER_REGISTRATION_DIGEST,
        active_policy_digest=POLICY_DIGEST,
        target_definition_digests={"orders": case.definition_digest},
        observations=observations,
    )

    assert plan.plan_digest == repeated.plan_digest
    assert {change.kind for change in plan.changes} == case.expected_change_kinds
    assert plan.worker_action is case.expected_worker_action
    assert plan.definition_action is case.expected_definition_action
    assert plan.routing_action is case.expected_routing_action
    assert plan.schedule_action is case.expected_schedule_action
    assert plan.compatibility_risks == case.expected_risks
    assert all(len(change.subject_digest) == 64 for change in plan.changes)


def activation_plan() -> ActivationPlan:
    return plan_configuration_activation(
        scope_digest=LOCAL_RUNTIME_SCOPE.digest,
        active_revision_id=ACTIVE_REVISION,
        active=bundle(),
        target_revision_id=TARGET_REVISION,
        target=bundle(description="updated"),
        active_artifact=ARTIFACT,
        target_artifact=ARTIFACT,
        target_task_queue_identity_digest=TASK_QUEUE_IDENTITY_DIGEST,
        target_worker_registration_digest=WORKER_REGISTRATION_DIGEST,
        active_policy_digest=POLICY_DIGEST,
        target_definition_digests={"orders": UPDATED_DEFINITION_DIGEST},
        observations=ActivationObservations(
            active_pointer_version=1,
            catalog_alias_version="catalog-version",
            policy_digest=POLICY_DIGEST,
            worker_observation_digest=WORKER_OBSERVATION_DIGEST,
            schedule_observation_digest=SCHEDULE_OBSERVATION_DIGEST,
            registered_definition_digests=frozenset({DEFINITION_DIGEST}),
        ),
    )


def publication_record(*, request_digest: str) -> PublicationRecord:
    idempotency_key = "publish-orders"
    return PublicationRecord(
        publication_id=publication_identity(LOCAL_RUNTIME_SCOPE.digest, idempotency_key),
        scope_digest=LOCAL_RUNTIME_SCOPE.digest,
        idempotency_key_digest=control_identity_digest(
            "publication-key",
            idempotency_key,
        ),
        request_digest=request_digest,
        actor_digest=control_identity_digest("actor", "operator"),
        correlation_digest=control_identity_digest("correlation", "request"),
        policy_digest=POLICY_DIGEST,
        expected_active_revision_id=ACTIVE_REVISION,
        created_at=NOW,
        updated_at=NOW,
    )


def activation_record(*, created_at: datetime = NOW) -> ActivationRecord:
    plan = activation_plan()
    idempotency_key = f"activate-{created_at.isoformat()}"
    return ActivationRecord(
        activation_id=activation_identity(LOCAL_RUNTIME_SCOPE.digest, idempotency_key),
        scope_digest=LOCAL_RUNTIME_SCOPE.digest,
        idempotency_key_digest=control_identity_digest("activation-key", idempotency_key),
        request_digest=activation_request_digest(plan),
        actor_digest=control_identity_digest("actor", "operator"),
        correlation_digest=control_identity_digest("correlation", "request"),
        plan=plan,
        created_at=created_at,
        updated_at=created_at,
    )


def discard_record() -> ConfigurationDiscardRecord:
    idempotency_key = "discard-active"
    return ConfigurationDiscardRecord(
        discard_id=discard_identity(LOCAL_RUNTIME_SCOPE.digest, idempotency_key),
        scope_digest=LOCAL_RUNTIME_SCOPE.digest,
        idempotency_key_digest=control_identity_digest("discard-key", idempotency_key),
        request_digest=provenance_digest({"request": "discard"}),
        actor_digest=control_identity_digest("discard-actor", "operator"),
        correlation_digest=control_identity_digest("discard-correlation", "request"),
        expected_draft_version=1,
        expected_active_identity=ACTIVE_REVISION,
        active_document_digest=provenance_digest({"configuration": "active"}),
        created_at=NOW,
        updated_at=NOW,
    )


def test_sqlite_publication_idempotency_and_conflict(tmp_path) -> None:
    store = SqliteActivationStore(tmp_path / "activation.sqlite3")
    record = publication_record(request_digest=provenance_digest({"request": "one"}))
    try:
        assert store.create_publication(LOCAL_RUNTIME_SCOPE, record) == record
        assert store.create_publication(LOCAL_RUNTIME_SCOPE, record) == record
        assert store.read_publication(LOCAL_RUNTIME_SCOPE, record.publication_id) == record

        applied = complete_publication(
            record,
            source_revision_id=ACTIVE_REVISION,
            published_revision_id=TARGET_REVISION,
            occurred_at=NOW + timedelta(seconds=1),
        )
        assert (
            store.update_publication(
                LOCAL_RUNTIME_SCOPE,
                applied,
                expected_version=record.version,
            )
            == applied
        )
        assert store.read_publication(LOCAL_RUNTIME_SCOPE, record.publication_id) == applied
        with pytest.raises(ActivationConflictError):
            store.update_publication(
                LOCAL_RUNTIME_SCOPE,
                applied,
                expected_version=record.version,
            )

        with pytest.raises(ActivationConflictError):
            store.create_publication(
                LOCAL_RUNTIME_SCOPE,
                record.model_copy(update={"request_digest": provenance_digest({"request": "two"})}),
            )
        with pytest.raises(ActivationNotFoundError):
            store.read_publication(LOCAL_RUNTIME_SCOPE, "missing")
    finally:
        store.close()


def test_sqlite_discard_idempotency_scope_and_conditional_update(tmp_path) -> None:
    store = SqliteActivationStore(tmp_path / "activation.sqlite3")
    pending = discard_record()
    try:
        assert store.create_discard(LOCAL_RUNTIME_SCOPE, pending) == pending
        assert store.create_discard(LOCAL_RUNTIME_SCOPE, pending) == pending
        assert store.read_discard(LOCAL_RUNTIME_SCOPE, pending.discard_id) == pending
        with pytest.raises(ActivationConflictError):
            store.create_discard(
                LOCAL_RUNTIME_SCOPE,
                pending.model_copy(
                    update={"request_digest": provenance_digest({"request": "changed"})}
                ),
            )
        with pytest.raises(ActivationIntegrityError, match="version transition"):
            store.update_discard(
                LOCAL_RUNTIME_SCOPE,
                pending,
                expected_version=pending.version,
            )

        applied = complete_discard(
            pending,
            result_draft_version=2,
            occurred_at=NOW + timedelta(seconds=1),
        )
        assert (
            store.update_discard(
                LOCAL_RUNTIME_SCOPE,
                applied,
                expected_version=pending.version,
            )
            == applied
        )
        with pytest.raises(ActivationConflictError):
            store.update_discard(
                LOCAL_RUNTIME_SCOPE,
                applied,
                expected_version=pending.version,
            )
        with pytest.raises(ActivationNotFoundError):
            store.read_discard(SECOND_SCOPE, pending.discard_id)
        with pytest.raises(ActivationIntegrityError):
            store.create_discard(SECOND_SCOPE, pending)
    finally:
        store.close()


def test_sqlite_activation_lease_update_is_conditional(tmp_path) -> None:
    store = SqliteActivationStore(tmp_path / "activation.sqlite3")
    pending = activation_record()
    owner = control_identity_digest("controller", "controller-one")
    try:
        store.create_activation(LOCAL_RUNTIME_SCOPE, pending)
        assert store.create_activation(LOCAL_RUNTIME_SCOPE, pending) == pending
        assert store.read_activation(LOCAL_RUNTIME_SCOPE, pending.activation_id) == pending
        with pytest.raises(ActivationConflictError):
            store.create_activation(
                LOCAL_RUNTIME_SCOPE,
                pending.model_copy(
                    update={"request_digest": provenance_digest({"request": "changed"})}
                ),
            )
        leased = acquire_activation_lease(
            pending,
            owner_digest=owner,
            acquired_at=NOW,
            expires_at=NOW + timedelta(seconds=30),
        )
        store.update_activation(
            LOCAL_RUNTIME_SCOPE,
            leased,
            expected_version=pending.version,
        )

        with pytest.raises(ActivationConflictError):
            store.update_activation(
                LOCAL_RUNTIME_SCOPE,
                leased,
                expected_version=pending.version,
            )
        with pytest.raises(ActivationNotFoundError):
            store.read_activation(LOCAL_RUNTIME_SCOPE, "missing")
    finally:
        store.close()


def test_sqlite_activation_pagination_is_scope_bound(tmp_path) -> None:
    store = SqliteActivationStore(tmp_path / "activation.sqlite3")
    first = activation_record(created_at=NOW)
    second = activation_record(created_at=NOW + timedelta(seconds=1))
    try:
        store.create_activation(LOCAL_RUNTIME_SCOPE, first)
        store.create_activation(LOCAL_RUNTIME_SCOPE, second)
        page = store.list_activations(LOCAL_RUNTIME_SCOPE, limit=1)

        assert [item.activation_id for item in page.activations] == [second.activation_id]
        assert page.next_cursor is not None
        next_page = store.list_activations(
            LOCAL_RUNTIME_SCOPE,
            limit=1,
            cursor=page.next_cursor,
        )
        assert [item.activation_id for item in next_page.activations] == [first.activation_id]
        with pytest.raises(ActivationIntegrityError, match="cursor"):
            store.list_activations(SECOND_SCOPE, limit=1, cursor=page.next_cursor)
        with pytest.raises(ActivationLimitError):
            store.list_activations(LOCAL_RUNTIME_SCOPE, limit=0)
        with pytest.raises(ActivationIntegrityError, match="another runtime scope"):
            store.create_activation(SECOND_SCOPE, first)
    finally:
        store.close()


def test_activation_transition_history_is_explicit() -> None:
    pending = activation_record()

    running = transition_activation(
        pending,
        ActivationState.RUNNING,
        occurred_at=NOW + timedelta(seconds=1),
    )
    waiting = transition_activation(
        running,
        ActivationState.WAITING_FOR_READINESS,
        occurred_at=NOW + timedelta(seconds=2),
    )
    resumed = transition_activation(
        waiting,
        ActivationState.RUNNING,
        occurred_at=NOW + timedelta(seconds=3),
    )

    assert [transition.to_state for transition in resumed.transitions] == [
        ActivationState.RUNNING,
        ActivationState.WAITING_FOR_READINESS,
        ActivationState.RUNNING,
    ]
    assert resumed.version == pending.version + 3
    with pytest.raises(ValueError, match="cannot transition"):
        transition_activation(
            resumed,
            ActivationState.PENDING,
            occurred_at=NOW + timedelta(seconds=4),
        )


def test_worker_readiness_registration_is_exact_and_idempotent() -> None:
    pending = activation_record()
    valid = WorkerReadinessRegistration(
        configuration_revision_id=pending.plan.target_revision_id,
        artifact=pending.plan.target_artifact,
        definition_digests=pending.plan.target_definition_digests,
        task_queue_identity_digest=pending.plan.target_task_queue_identity_digest,
        deployment_registration_digest=pending.plan.target_worker_registration_digest,
        registered_at=NOW,
    )

    registered = register_worker_readiness(pending, valid, occurred_at=NOW)

    assert registered.worker_readiness == valid
    assert register_worker_readiness(registered, valid, occurred_at=NOW) == registered
    with pytest.raises(ValueError, match="another artifact"):
        register_worker_readiness(
            pending,
            valid.model_copy(update={"artifact": UPDATED_ARTIFACT}),
            occurred_at=NOW,
        )
    with pytest.raises(ValueError, match="deployment registration"):
        register_worker_readiness(
            pending,
            valid.model_copy(
                update={
                    "deployment_registration_digest": provenance_digest(
                        {"worker_registration": "other"}
                    )
                }
            ),
            occurred_at=NOW,
        )
    with pytest.raises(ValueError, match="definition set"):
        register_worker_readiness(
            pending,
            valid.model_copy(update={"definition_digests": ()}),
            occurred_at=NOW,
        )


def test_activation_lease_takeover_requires_expiry_and_exact_owner() -> None:
    pending = activation_record()
    first_owner = control_identity_digest("controller", "first")
    second_owner = control_identity_digest("controller", "second")
    leased = acquire_activation_lease(
        pending,
        owner_digest=first_owner,
        acquired_at=NOW,
        expires_at=NOW + timedelta(seconds=1),
    )

    with pytest.raises(ValueError, match="another controller"):
        acquire_activation_lease(
            leased,
            owner_digest=second_owner,
            acquired_at=NOW,
            expires_at=NOW + timedelta(seconds=1),
        )
    taken_over = acquire_activation_lease(
        leased,
        owner_digest=second_owner,
        acquired_at=NOW + timedelta(seconds=2),
        expires_at=NOW + timedelta(seconds=3),
    )
    with pytest.raises(ValueError, match="not owned"):
        release_activation_lease(taken_over, owner_digest=first_owner, released_at=NOW)
    released = release_activation_lease(
        taken_over,
        owner_digest=second_owner,
        released_at=NOW + timedelta(seconds=2),
    )
    assert released.lease_owner_digest is None
    assert released.lease_expires_at is None


def test_activation_failure_transition_requires_a_matching_outcome() -> None:
    running = transition_activation(
        activation_record(),
        ActivationState.RUNNING,
        occurred_at=NOW,
    )

    with pytest.raises(ValueError, match="outcome"):
        transition_activation(
            running,
            ActivationState.FAILED,
            occurred_at=NOW,
        )
    with pytest.raises(ValueError, match="outcome"):
        transition_activation(
            running,
            ActivationState.WAITING_FOR_READINESS,
            occurred_at=NOW,
            outcome_code=ActivationOutcomeCode.INTERNAL_ERROR,
        )


def test_configured_activation_store_requires_a_writable_typed_backend(tmp_path) -> None:
    sqlite = configured_activation_store(
        SqliteConfigurationSettings(path=str(tmp_path / "activation.sqlite3"))
    )
    try:
        assert isinstance(sqlite, SqliteActivationStore)
    finally:
        sqlite.close()

    with pytest.raises(TypeError, match="no activation store"):
        configured_activation_store(FileConfigurationSettings())

    dynamodb = configured_activation_store(
        AwsConfigurationSettings(
            bucket="example-configuration",
            table_name="justflow-configuration",
            revision_index_name="scope-revisions",
            activation_index_name="scope-activations",
        ),
        dynamodb_client=cast(DynamoConfigurationClient, object()),
    )
    assert isinstance(dynamodb, DynamoDbActivationStore)
