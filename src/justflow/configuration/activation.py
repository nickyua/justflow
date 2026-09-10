"""Bounded publication and activation control-plane contracts."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import datetime
from enum import Enum
from typing import Annotated, Self

from pydantic import Field, StrictStr, model_validator

from justflow.config.triggers import TriggersConfig
from justflow.configuration.models import (
    ConfigurationBundle,
    RevisionIdentity,
    StrictConfigurationModel,
)
from justflow.provenance import WorkerArtifactIdentity, provenance_digest
from justflow.scope import safe_identity_digest

CONTROL_IDENTITY_LENGTH = 64
MAX_CONTROL_IDENTITY_INPUT_LENGTH = 256
MAX_ACTIVATION_CHANGES = 1_000
MAX_ACTIVATION_HISTORY = 100
MAX_ACTIVATION_PAGE_SIZE = 100
MAX_EXTERNAL_VERSION_LENGTH = 256
MAX_ACTIVATION_RECORD_BYTES = 350_000

Digest = Annotated[
    StrictStr,
    Field(
        min_length=CONTROL_IDENTITY_LENGTH,
        max_length=CONTROL_IDENTITY_LENGTH,
        pattern=rf"^[0-9a-f]{{{CONTROL_IDENTITY_LENGTH}}}$",
    ),
]
ProvenanceDigest = Annotated[
    StrictStr,
    Field(
        min_length=len("sha256:") + CONTROL_IDENTITY_LENGTH,
        max_length=len("sha256:") + CONTROL_IDENTITY_LENGTH,
        pattern=rf"^sha256:[0-9a-f]{{{CONTROL_IDENTITY_LENGTH}}}$",
    ),
]


class PublicationState(str, Enum):
    PENDING = "pending"
    APPLIED = "applied"
    FAILED = "failed"


class PublicationErrorCode(str, Enum):
    INVALID_CONFIGURATION = "invalid_configuration"
    CONFLICT = "conflict"
    CATALOG_UNAVAILABLE = "catalog_unavailable"
    STORE_UNAVAILABLE = "store_unavailable"


class PublicationRecord(StrictConfigurationModel):
    publication_id: Digest
    scope_digest: Digest
    idempotency_key_digest: Digest
    request_digest: ProvenanceDigest
    actor_digest: Digest
    correlation_digest: Digest
    policy_digest: ProvenanceDigest
    source_parent_revision_id: RevisionIdentity | None = None
    expected_active_revision_id: RevisionIdentity | None = None
    source_revision_id: RevisionIdentity | None = None
    published_revision_id: RevisionIdentity | None = None
    state: PublicationState = PublicationState.PENDING
    error_code: PublicationErrorCode | None = None
    version: int = Field(default=1, ge=1)
    created_at: datetime
    updated_at: datetime

    @model_validator(mode="after")
    def validate_outcome(self) -> Self:
        if self.state is PublicationState.APPLIED:
            if self.source_revision_id is None or self.published_revision_id is None:
                raise ValueError("Applied publication requires immutable revision identities")
            if self.error_code is not None:
                raise ValueError("Applied publication cannot contain an error code")
        elif self.state is PublicationState.FAILED:
            if self.error_code is None:
                raise ValueError("Failed publication requires an error code")
        elif (
            self.source_revision_id is not None
            or self.published_revision_id is not None
            or self.error_code is not None
        ):
            raise ValueError("Pending publication cannot contain an outcome")
        if self.updated_at < self.created_at:
            raise ValueError("Publication timestamps are inconsistent")
        return self


class ActivationState(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    WAITING_FOR_READINESS = "waiting_for_readiness"
    APPLIED = "applied"
    FAILED = "failed"
    SUPERSEDED = "superseded"
    ROLLED_BACK = "rolled_back"


class ActivationChangeKind(str, Enum):
    WORKFLOW = "workflow"
    SERVICE = "service"
    RESOURCE = "resource"
    POLICY = "policy"
    SCHEDULE = "schedule"
    ARTIFACT = "artifact"
    DELETION = "deletion"


class ActivationSubjectKind(str, Enum):
    WORKFLOW = "workflow"
    SERVICE = "service"
    RESOURCE = "resource"
    POLICY = "policy"
    SCHEDULE = "schedule"
    ARTIFACT = "artifact"


class ActivationChangeOperation(str, Enum):
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"


class WorkerActivationAction(str, Enum):
    NONE = "none"
    ROLLOUT = "rollout"


class DefinitionActivationAction(str, Enum):
    NONE = "none"
    PUBLISH = "publish"


class RoutingActivationAction(str, Enum):
    NONE = "none"
    SWITCH = "switch"


class ScheduleActivationAction(str, Enum):
    NONE = "none"
    RECONCILE = "reconcile"


class ActivationCompatibilityRisk(str, Enum):
    OPEN_EXECUTION_RETENTION = "open_execution_retention"
    REPLAY_COMPATIBILITY = "replay_compatibility"
    COMPONENT_POLICY_CHANGE = "component_policy_change"
    EXTERNAL_ROUTING_CHANGE = "external_routing_change"


class ActivationOutcomeCode(str, Enum):
    DEFINITIONS_UNAVAILABLE = "definitions_unavailable"
    WORKER_UNAVAILABLE = "worker_unavailable"
    SCHEDULES_UNAVAILABLE = "schedules_unavailable"
    ROUTING_UNAVAILABLE = "routing_unavailable"
    CONFIGURATION_CONFLICT = "configuration_conflict"
    STALE_PLAN = "stale_plan"
    INTERNAL_ERROR = "internal_error"


class ActivationCheckpointKind(str, Enum):
    DEFINITIONS = "definitions"
    WORKER_ROLLOUT = "worker_rollout"
    WORKER_READINESS = "worker_readiness"
    SCHEDULES = "schedules"
    CATALOG_ROUTING = "catalog_routing"
    WORKER_ROUTING = "worker_routing"
    ACTIVE_POINTER = "active_pointer"
    RUNTIME_INDEX = "runtime_index"


class ActivationExternalOutcome(str, Enum):
    CONFIRMED = "confirmed"
    IDEMPOTENT = "idempotent"


class ActivationChange(StrictConfigurationModel):
    kind: ActivationChangeKind
    subject_kind: ActivationSubjectKind
    operation: ActivationChangeOperation
    subject_digest: Digest


class ActivationObservations(StrictConfigurationModel):
    active_pointer_version: int | None = Field(default=None, ge=1)
    catalog_alias_version: str | None = Field(
        default=None,
        min_length=1,
        max_length=MAX_EXTERNAL_VERSION_LENGTH,
    )
    policy_digest: ProvenanceDigest
    worker_observation_digest: ProvenanceDigest
    schedule_observation_digest: ProvenanceDigest
    registered_definition_digests: frozenset[Digest] = Field(
        default_factory=frozenset,
        max_length=MAX_ACTIVATION_CHANGES,
    )


class ActivationPlan(StrictConfigurationModel):
    plan_digest: ProvenanceDigest
    scope_digest: Digest
    source_revision_id: RevisionIdentity | None
    target_revision_id: RevisionIdentity
    expected_active_revision_id: RevisionIdentity | None
    expected_active_pointer_version: int | None = Field(default=None, ge=1)
    expected_catalog_alias_version: str | None = Field(
        default=None,
        min_length=1,
        max_length=MAX_EXTERNAL_VERSION_LENGTH,
    )
    policy_digest: ProvenanceDigest
    worker_observation_digest: ProvenanceDigest
    schedule_observation_digest: ProvenanceDigest
    target_artifact: WorkerArtifactIdentity
    target_task_queue_identity_digest: ProvenanceDigest
    target_worker_registration_digest: ProvenanceDigest
    target_definition_digests: tuple[Digest, ...] = Field(max_length=MAX_ACTIVATION_CHANGES)
    changes: tuple[ActivationChange, ...] = Field(max_length=MAX_ACTIVATION_CHANGES)
    compatibility_risks: tuple[ActivationCompatibilityRisk, ...] = Field(
        max_length=len(ActivationCompatibilityRisk),
    )
    worker_action: WorkerActivationAction
    definition_action: DefinitionActivationAction
    routing_action: RoutingActivationAction
    schedule_action: ScheduleActivationAction

    @classmethod
    def create(
        cls,
        *,
        scope_digest: str,
        source_revision_id: RevisionIdentity | None,
        target_revision_id: RevisionIdentity,
        expected_active_revision_id: RevisionIdentity | None,
        observations: ActivationObservations,
        target_artifact: WorkerArtifactIdentity,
        target_task_queue_identity_digest: str,
        target_worker_registration_digest: str,
        target_definition_digests: tuple[str, ...],
        changes: tuple[ActivationChange, ...],
        compatibility_risks: tuple[ActivationCompatibilityRisk, ...],
        worker_action: WorkerActivationAction,
        definition_action: DefinitionActivationAction,
        routing_action: RoutingActivationAction,
        schedule_action: ScheduleActivationAction,
    ) -> Self:
        value = {
            "scope_digest": scope_digest,
            "source_revision_id": (
                str(source_revision_id) if source_revision_id is not None else None
            ),
            "target_revision_id": str(target_revision_id),
            "expected_active_revision_id": (
                str(expected_active_revision_id)
                if expected_active_revision_id is not None
                else None
            ),
            "expected_active_pointer_version": observations.active_pointer_version,
            "expected_catalog_alias_version": observations.catalog_alias_version,
            "policy_digest": observations.policy_digest,
            "worker_observation_digest": observations.worker_observation_digest,
            "schedule_observation_digest": observations.schedule_observation_digest,
            "target_artifact": target_artifact.model_dump(mode="json"),
            "target_task_queue_identity_digest": target_task_queue_identity_digest,
            "target_worker_registration_digest": target_worker_registration_digest,
            "target_definition_digests": target_definition_digests,
            "changes": [change.model_dump(mode="json") for change in changes],
            "compatibility_risks": [risk.value for risk in compatibility_risks],
            "worker_action": worker_action.value,
            "definition_action": definition_action.value,
            "routing_action": routing_action.value,
            "schedule_action": schedule_action.value,
        }
        return cls(
            plan_digest=provenance_digest(value),
            scope_digest=scope_digest,
            source_revision_id=source_revision_id,
            target_revision_id=target_revision_id,
            expected_active_revision_id=expected_active_revision_id,
            expected_active_pointer_version=observations.active_pointer_version,
            expected_catalog_alias_version=observations.catalog_alias_version,
            policy_digest=observations.policy_digest,
            worker_observation_digest=observations.worker_observation_digest,
            schedule_observation_digest=observations.schedule_observation_digest,
            target_artifact=target_artifact,
            target_task_queue_identity_digest=target_task_queue_identity_digest,
            target_worker_registration_digest=target_worker_registration_digest,
            target_definition_digests=target_definition_digests,
            changes=changes,
            compatibility_risks=compatibility_risks,
            worker_action=worker_action,
            definition_action=definition_action,
            routing_action=routing_action,
            schedule_action=schedule_action,
        )


class ActivationTransition(StrictConfigurationModel):
    sequence: int = Field(ge=1, le=MAX_ACTIVATION_HISTORY)
    from_state: ActivationState
    to_state: ActivationState
    occurred_at: datetime
    outcome_code: ActivationOutcomeCode | None = None


class ActivationCheckpoint(StrictConfigurationModel):
    sequence: int = Field(ge=1, le=len(ActivationCheckpointKind))
    kind: ActivationCheckpointKind
    outcome: ActivationExternalOutcome
    occurred_at: datetime
    observation_digest: ProvenanceDigest


class WorkerReadinessRegistration(StrictConfigurationModel):
    configuration_revision_id: RevisionIdentity
    artifact: WorkerArtifactIdentity
    definition_digests: tuple[Digest, ...] = Field(max_length=MAX_ACTIVATION_CHANGES)
    task_queue_identity_digest: ProvenanceDigest
    deployment_registration_digest: ProvenanceDigest
    registered_at: datetime


class ActivationRecord(StrictConfigurationModel):
    activation_id: Digest
    scope_digest: Digest
    idempotency_key_digest: Digest
    request_digest: ProvenanceDigest
    actor_digest: Digest
    correlation_digest: Digest
    plan: ActivationPlan
    state: ActivationState = ActivationState.PENDING
    version: int = Field(default=1, ge=1)
    transitions: tuple[ActivationTransition, ...] = Field(
        default_factory=tuple,
        max_length=MAX_ACTIVATION_HISTORY,
    )
    checkpoints: tuple[ActivationCheckpoint, ...] = Field(
        default_factory=tuple,
        max_length=len(ActivationCheckpointKind),
    )
    lease_owner_digest: Digest | None = None
    lease_expires_at: datetime | None = None
    worker_readiness: WorkerReadinessRegistration | None = None
    rollback_of_activation_id: Digest | None = None
    outcome_code: ActivationOutcomeCode | None = None
    created_at: datetime
    updated_at: datetime

    @model_validator(mode="after")
    def validate_state(self) -> Self:
        if self.plan.scope_digest != self.scope_digest:
            raise ValueError("Activation plan belongs to another runtime scope")
        if (self.lease_owner_digest is None) != (self.lease_expires_at is None):
            raise ValueError("Activation lease owner and expiry must be recorded together")
        if self.updated_at < self.created_at:
            raise ValueError("Activation timestamps are inconsistent")
        if self.transitions:
            if self.transitions[-1].to_state is not self.state:
                raise ValueError("Activation transition history disagrees with current state")
            for index, transition in enumerate(self.transitions, start=1):
                if transition.sequence != index:
                    raise ValueError("Activation transition sequence is not contiguous")
                if index > 1 and self.transitions[index - 2].to_state is not transition.from_state:
                    raise ValueError("Activation transition history is discontinuous")
        elif self.state is not ActivationState.PENDING:
            raise ValueError("Non-pending activation requires transition history")
        if len({checkpoint.kind for checkpoint in self.checkpoints}) != len(self.checkpoints):
            raise ValueError("Activation checkpoint kinds must be unique")
        for index, checkpoint in enumerate(self.checkpoints, start=1):
            if checkpoint.sequence != index:
                raise ValueError("Activation checkpoint sequence is not contiguous")
        if self.state in {ActivationState.FAILED, ActivationState.SUPERSEDED}:
            if self.outcome_code is None:
                raise ValueError("Failed or superseded activation requires an outcome code")
        elif self.outcome_code is not None:
            raise ValueError("Successful or pending activation cannot contain an outcome code")
        return self


class ActivationSummary(StrictConfigurationModel):
    activation_id: Digest
    source_revision_id: RevisionIdentity | None
    target_revision_id: RevisionIdentity
    state: ActivationState
    rollback_of_activation_id: Digest | None = None
    created_at: datetime
    updated_at: datetime


class ActivationPage(StrictConfigurationModel):
    activations: tuple[ActivationSummary, ...] = Field(max_length=MAX_ACTIVATION_PAGE_SIZE)
    next_cursor: str | None = Field(default=None, repr=False)


_WORKER_CHANGE_KINDS = frozenset(
    {
        ActivationChangeKind.WORKFLOW,
        ActivationChangeKind.SERVICE,
        ActivationChangeKind.RESOURCE,
        ActivationChangeKind.POLICY,
        ActivationChangeKind.ARTIFACT,
    }
)


def plan_configuration_activation(
    *,
    scope_digest: str,
    target_revision_id: RevisionIdentity,
    target: ConfigurationBundle,
    target_artifact: WorkerArtifactIdentity,
    target_task_queue_identity_digest: str,
    target_worker_registration_digest: str,
    target_definition_digests: Mapping[str, str],
    observations: ActivationObservations,
    active_revision_id: RevisionIdentity | None = None,
    active: ConfigurationBundle | None = None,
    active_artifact: WorkerArtifactIdentity | None = None,
    active_policy_digest: str | None = None,
) -> ActivationPlan:
    changes = _configuration_changes(
        scope_digest,
        target,
        active,
        target_artifact=target_artifact,
        active_artifact=active_artifact,
        target_policy_digest=observations.policy_digest,
        active_policy_digest=active_policy_digest,
    )
    if len(changes) > MAX_ACTIVATION_CHANGES:
        raise ValueError("Activation plan exceeds its change count limit")
    target_digests = tuple(sorted(set(target_definition_digests.values())))
    worker_action = (
        WorkerActivationAction.ROLLOUT
        if any(
            change.kind in _WORKER_CHANGE_KINDS
            or (
                change.kind is ActivationChangeKind.DELETION
                and change.subject_kind is not ActivationSubjectKind.SCHEDULE
            )
            for change in changes
        )
        or not set(target_digests).issubset(observations.registered_definition_digests)
        else WorkerActivationAction.NONE
    )
    definition_action = (
        DefinitionActivationAction.PUBLISH
        if not set(target_digests).issubset(observations.registered_definition_digests)
        or any(change.subject_kind is ActivationSubjectKind.WORKFLOW for change in changes)
        else DefinitionActivationAction.NONE
    )
    schedule_action = (
        ScheduleActivationAction.RECONCILE
        if any(change.subject_kind is ActivationSubjectKind.SCHEDULE for change in changes)
        else ScheduleActivationAction.NONE
    )
    routing_action = (
        RoutingActivationAction.NONE
        if active_revision_id == target_revision_id
        else RoutingActivationAction.SWITCH
    )
    risks: list[ActivationCompatibilityRisk] = []
    if any(
        change.kind is ActivationChangeKind.DELETION
        and change.subject_kind is ActivationSubjectKind.WORKFLOW
        for change in changes
    ):
        risks.append(ActivationCompatibilityRisk.OPEN_EXECUTION_RETENTION)
    if worker_action is WorkerActivationAction.ROLLOUT:
        risks.append(ActivationCompatibilityRisk.REPLAY_COMPATIBILITY)
    if any(change.kind is ActivationChangeKind.POLICY for change in changes):
        risks.append(ActivationCompatibilityRisk.COMPONENT_POLICY_CHANGE)
    if routing_action is RoutingActivationAction.SWITCH:
        risks.append(ActivationCompatibilityRisk.EXTERNAL_ROUTING_CHANGE)
    source_revision_id = (
        target.tenant_resolution.tenant_configuration_revision_id
        if target.tenant_resolution is not None
        else None
    )
    return ActivationPlan.create(
        scope_digest=scope_digest,
        source_revision_id=source_revision_id,
        target_revision_id=target_revision_id,
        expected_active_revision_id=active_revision_id,
        observations=observations,
        target_artifact=target_artifact,
        target_task_queue_identity_digest=target_task_queue_identity_digest,
        target_worker_registration_digest=target_worker_registration_digest,
        target_definition_digests=target_digests,
        changes=changes,
        compatibility_risks=tuple(risks),
        worker_action=worker_action,
        definition_action=definition_action,
        routing_action=routing_action,
        schedule_action=schedule_action,
    )


def publication_identity(scope_digest: str, idempotency_key: str) -> str:
    _validate_control_identity_input(idempotency_key, kind="publication idempotency key")
    return safe_identity_digest("publication", f"{scope_digest}:{idempotency_key}")


def activation_identity(scope_digest: str, idempotency_key: str) -> str:
    _validate_control_identity_input(idempotency_key, kind="activation idempotency key")
    return safe_identity_digest("activation", f"{scope_digest}:{idempotency_key}")


def control_identity_digest(kind: str, value: str) -> str:
    _validate_control_identity_input(value, kind=kind)
    return safe_identity_digest(kind, value)


def activation_request_digest(plan: ActivationPlan) -> str:
    return provenance_digest(
        {
            "plan_digest": plan.plan_digest,
            "scope_digest": plan.scope_digest,
            "target_revision_id": str(plan.target_revision_id),
        }
    )


def publication_request_digest(
    *,
    scope_digest: str,
    draft_version: int,
    document_bytes: bytes,
) -> str:
    return provenance_digest(
        {
            "document_digest": hashlib.sha256(document_bytes).hexdigest(),
            "draft_version": draft_version,
            "scope_digest": scope_digest,
        }
    )


def _configuration_changes(
    scope_digest: str,
    target: ConfigurationBundle,
    active: ConfigurationBundle | None,
    *,
    target_artifact: WorkerArtifactIdentity,
    active_artifact: WorkerArtifactIdentity | None,
    target_policy_digest: str,
    active_policy_digest: str | None,
) -> tuple[ActivationChange, ...]:
    changes: list[ActivationChange] = []
    active_bundle = active or ConfigurationBundle(
        workflows={},
        triggers=TriggersConfig(triggers={}),
    )
    for kind, subject_kind, target_values, active_values in (
        (
            ActivationChangeKind.WORKFLOW,
            ActivationSubjectKind.WORKFLOW,
            target.workflows,
            active_bundle.workflows,
        ),
        (
            ActivationChangeKind.SERVICE,
            ActivationSubjectKind.SERVICE,
            target.services.services,
            active_bundle.services.services,
        ),
        (
            ActivationChangeKind.RESOURCE,
            ActivationSubjectKind.RESOURCE,
            target.resources.resources,
            active_bundle.resources.resources,
        ),
        (
            ActivationChangeKind.SCHEDULE,
            ActivationSubjectKind.SCHEDULE,
            target.triggers.triggers,
            active_bundle.triggers.triggers,
        ),
    ):
        changes.extend(
            _mapping_changes(
                scope_digest,
                kind=kind,
                subject_kind=subject_kind,
                target=target_values,
                active=active_values,
            )
        )
    if active_policy_digest != target_policy_digest:
        changes.append(
            _singleton_change(
                scope_digest,
                kind=ActivationChangeKind.POLICY,
                subject_kind=ActivationSubjectKind.POLICY,
                operation=(
                    ActivationChangeOperation.CREATE
                    if active_policy_digest is None
                    else ActivationChangeOperation.UPDATE
                ),
                identity=target_policy_digest,
            )
        )
    if active_artifact != target_artifact:
        changes.append(
            _singleton_change(
                scope_digest,
                kind=ActivationChangeKind.ARTIFACT,
                subject_kind=ActivationSubjectKind.ARTIFACT,
                operation=(
                    ActivationChangeOperation.CREATE
                    if active_artifact is None
                    else ActivationChangeOperation.UPDATE
                ),
                identity=provenance_digest(target_artifact.model_dump(mode="json")),
            )
        )
    return tuple(
        sorted(
            changes,
            key=lambda change: (
                change.kind.value,
                change.subject_kind.value,
                change.operation.value,
                change.subject_digest,
            ),
        )
    )


def _mapping_changes(
    scope_digest: str,
    *,
    kind: ActivationChangeKind,
    subject_kind: ActivationSubjectKind,
    target: Mapping[str, object],
    active: Mapping[str, object],
) -> tuple[ActivationChange, ...]:
    changes: list[ActivationChange] = []
    for name in sorted(set(target) | set(active)):
        if name not in target:
            change_kind = ActivationChangeKind.DELETION
            operation = ActivationChangeOperation.DELETE
        elif name not in active:
            change_kind = kind
            operation = ActivationChangeOperation.CREATE
        elif _canonical_value(target[name]) != _canonical_value(active[name]):
            change_kind = kind
            operation = ActivationChangeOperation.UPDATE
        else:
            continue
        changes.append(
            _singleton_change(
                scope_digest,
                kind=change_kind,
                subject_kind=subject_kind,
                operation=operation,
                identity=name,
            )
        )
    return tuple(changes)


def _singleton_change(
    scope_digest: str,
    *,
    kind: ActivationChangeKind,
    subject_kind: ActivationSubjectKind,
    operation: ActivationChangeOperation,
    identity: str,
) -> ActivationChange:
    return ActivationChange(
        kind=kind,
        subject_kind=subject_kind,
        operation=operation,
        subject_digest=safe_identity_digest(
            "activation-change",
            f"{scope_digest}:{subject_kind.value}:{identity}",
        ),
    )


def _canonical_value(value: object) -> bytes:
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json", by_alias=True)
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _validate_control_identity_input(value: str, *, kind: str) -> None:
    if not value or len(value) > MAX_CONTROL_IDENTITY_INPUT_LENGTH:
        raise ValueError(f"{kind.capitalize()} is invalid")
