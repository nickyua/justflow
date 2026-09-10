"""Conditional publication and activation persistence boundary."""

from __future__ import annotations

from datetime import datetime
from typing import Protocol, runtime_checkable

from justflow.configuration.activation import (
    MAX_ACTIVATION_HISTORY,
    ActivationCheckpoint,
    ActivationCheckpointKind,
    ActivationExternalOutcome,
    ActivationOutcomeCode,
    ActivationPage,
    ActivationRecord,
    ActivationState,
    ActivationTransition,
    PublicationErrorCode,
    PublicationRecord,
    PublicationState,
    WorkerReadinessRegistration,
)
from justflow.configuration.lifecycle import ConfigurationDiscardRecord
from justflow.configuration.models import RevisionIdentity
from justflow.scope import RuntimeScope


@runtime_checkable
class ActivationStore(Protocol):
    def create_discard(
        self,
        scope: RuntimeScope,
        record: ConfigurationDiscardRecord,
    ) -> ConfigurationDiscardRecord: ...

    def read_discard(
        self,
        scope: RuntimeScope,
        discard_id: str,
    ) -> ConfigurationDiscardRecord: ...

    def update_discard(
        self,
        scope: RuntimeScope,
        record: ConfigurationDiscardRecord,
        *,
        expected_version: int,
    ) -> ConfigurationDiscardRecord: ...

    def create_publication(
        self,
        scope: RuntimeScope,
        record: PublicationRecord,
    ) -> PublicationRecord: ...

    def read_publication(
        self,
        scope: RuntimeScope,
        publication_id: str,
    ) -> PublicationRecord: ...

    def update_publication(
        self,
        scope: RuntimeScope,
        record: PublicationRecord,
        *,
        expected_version: int,
    ) -> PublicationRecord: ...

    def create_activation(
        self,
        scope: RuntimeScope,
        record: ActivationRecord,
    ) -> ActivationRecord: ...

    def read_activation(
        self,
        scope: RuntimeScope,
        activation_id: str,
    ) -> ActivationRecord: ...

    def update_activation(
        self,
        scope: RuntimeScope,
        record: ActivationRecord,
        *,
        expected_version: int,
    ) -> ActivationRecord: ...

    def list_activations(
        self,
        scope: RuntimeScope,
        *,
        limit: int,
        cursor: str | None = None,
    ) -> ActivationPage: ...


def complete_publication(
    record: PublicationRecord,
    *,
    source_revision_id: RevisionIdentity,
    published_revision_id: RevisionIdentity,
    occurred_at: datetime,
) -> PublicationRecord:
    return PublicationRecord.model_validate(
        {
            **record.model_dump(),
            "source_revision_id": source_revision_id,
            "published_revision_id": published_revision_id,
            "state": PublicationState.APPLIED,
            "error_code": None,
            "version": record.version + 1,
            "updated_at": occurred_at,
        }
    )


def fail_publication(
    record: PublicationRecord,
    *,
    error_code: PublicationErrorCode,
    occurred_at: datetime,
) -> PublicationRecord:
    return PublicationRecord.model_validate(
        {
            **record.model_dump(),
            "state": PublicationState.FAILED,
            "error_code": error_code,
            "version": record.version + 1,
            "updated_at": occurred_at,
        }
    )


def transition_activation(
    record: ActivationRecord,
    to_state: ActivationState,
    *,
    occurred_at: datetime,
    outcome_code: ActivationOutcomeCode | None = None,
    release_lease: bool = False,
) -> ActivationRecord:
    allowed = _ALLOWED_ACTIVATION_TRANSITIONS[record.state]
    if to_state not in allowed:
        raise ValueError(
            f"Activation cannot transition from '{record.state.value}' to '{to_state.value}'"
        )
    if len(record.transitions) >= MAX_ACTIVATION_HISTORY:
        raise ValueError("Activation transition history exceeds its bound")
    terminal_failure = to_state in {ActivationState.FAILED, ActivationState.SUPERSEDED}
    if terminal_failure != (outcome_code is not None):
        raise ValueError("Activation transition outcome does not match its state")
    transition = ActivationTransition(
        sequence=len(record.transitions) + 1,
        from_state=record.state,
        to_state=to_state,
        occurred_at=occurred_at,
        outcome_code=outcome_code,
    )
    return ActivationRecord.model_validate(
        {
            **record.model_dump(),
            "state": to_state,
            "version": record.version + 1,
            "transitions": (*record.transitions, transition),
            "lease_owner_digest": None if release_lease else record.lease_owner_digest,
            "lease_expires_at": None if release_lease else record.lease_expires_at,
            "outcome_code": outcome_code,
            "updated_at": occurred_at,
        }
    )


def record_activation_checkpoint(
    record: ActivationRecord,
    *,
    kind: ActivationCheckpointKind,
    outcome: ActivationExternalOutcome,
    observation_digest: str,
    occurred_at: datetime,
) -> ActivationRecord:
    existing = next(
        (checkpoint for checkpoint in record.checkpoints if checkpoint.kind is kind),
        None,
    )
    if existing is not None:
        if existing.outcome is outcome and existing.observation_digest == observation_digest:
            return record
        raise ValueError("Activation checkpoint already has a different outcome")
    checkpoint = ActivationCheckpoint(
        sequence=len(record.checkpoints) + 1,
        kind=kind,
        outcome=outcome,
        occurred_at=occurred_at,
        observation_digest=observation_digest,
    )
    return ActivationRecord.model_validate(
        {
            **record.model_dump(),
            "checkpoints": (*record.checkpoints, checkpoint),
            "version": record.version + 1,
            "updated_at": occurred_at,
        }
    )


def acquire_activation_lease(
    record: ActivationRecord,
    *,
    owner_digest: str,
    acquired_at: datetime,
    expires_at: datetime,
) -> ActivationRecord:
    if expires_at <= acquired_at:
        raise ValueError("Activation lease expiry must follow acquisition")
    if (
        record.lease_owner_digest is not None
        and record.lease_owner_digest != owner_digest
        and record.lease_expires_at is not None
        and record.lease_expires_at > acquired_at
    ):
        raise ValueError("Activation lease is held by another controller")
    return ActivationRecord.model_validate(
        {
            **record.model_dump(),
            "lease_owner_digest": owner_digest,
            "lease_expires_at": expires_at,
            "version": record.version + 1,
            "updated_at": acquired_at,
        }
    )


def release_activation_lease(
    record: ActivationRecord,
    *,
    owner_digest: str,
    released_at: datetime,
) -> ActivationRecord:
    if record.lease_owner_digest != owner_digest:
        raise ValueError("Activation lease is not owned by this controller")
    return ActivationRecord.model_validate(
        {
            **record.model_dump(),
            "lease_owner_digest": None,
            "lease_expires_at": None,
            "version": record.version + 1,
            "updated_at": released_at,
        }
    )


def register_worker_readiness(
    record: ActivationRecord,
    registration: WorkerReadinessRegistration,
    *,
    occurred_at: datetime,
) -> ActivationRecord:
    if registration.configuration_revision_id != record.plan.target_revision_id:
        raise ValueError("Worker readiness targets another configuration revision")
    if registration.artifact != record.plan.target_artifact:
        raise ValueError("Worker readiness targets another artifact")
    if registration.deployment_registration_digest != record.plan.target_worker_registration_digest:
        raise ValueError("Worker readiness targets another deployment registration")
    if tuple(sorted(registration.definition_digests)) != tuple(
        sorted(record.plan.target_definition_digests)
    ):
        raise ValueError("Worker readiness does not cover the planned definition set")
    if record.worker_readiness is not None:
        if record.worker_readiness != registration:
            raise ValueError("Worker readiness was already registered with different content")
        return record
    return ActivationRecord.model_validate(
        {
            **record.model_dump(),
            "worker_readiness": registration,
            "version": record.version + 1,
            "updated_at": occurred_at,
        }
    )


_ALLOWED_ACTIVATION_TRANSITIONS = {
    ActivationState.PENDING: frozenset(
        {
            ActivationState.RUNNING,
            ActivationState.SUPERSEDED,
        }
    ),
    ActivationState.RUNNING: frozenset(
        {
            ActivationState.WAITING_FOR_READINESS,
            ActivationState.APPLIED,
            ActivationState.FAILED,
            ActivationState.SUPERSEDED,
            ActivationState.ROLLED_BACK,
        }
    ),
    ActivationState.WAITING_FOR_READINESS: frozenset(
        {
            ActivationState.RUNNING,
            ActivationState.FAILED,
            ActivationState.SUPERSEDED,
        }
    ),
    ActivationState.FAILED: frozenset(
        {
            ActivationState.RUNNING,
            ActivationState.SUPERSEDED,
        }
    ),
    ActivationState.APPLIED: frozenset({ActivationState.ROLLED_BACK}),
    ActivationState.SUPERSEDED: frozenset(),
    ActivationState.ROLLED_BACK: frozenset(),
}
