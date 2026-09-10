"""Bounded authoring lifecycle projections and discard audit contracts."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from enum import Enum
from typing import Annotated, Literal, Self

from pydantic import Field, StrictStr, model_validator

from justflow.configuration.activation import Digest, ProvenanceDigest
from justflow.configuration.errors import ConfigurationLimitError
from justflow.configuration.models import RevisionIdentity, StrictConfigurationModel
from justflow.scope import safe_identity_digest

MAX_CONFIGURATION_RELATIONSHIPS = 1_000
MAX_RELATIONSHIP_NAME_LENGTH = 128
MAX_APPLY_STAGES = 8
LifecycleIdentity = Annotated[
    StrictStr,
    Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$"),
]


class ConfigurationRelationshipState(str, Enum):
    ACTIVE = "active"
    MODIFIED = "modified"
    NEW_PENDING_APPLY = "new_pending_apply"
    REMOVED_PENDING_APPLY = "removed_pending_apply"


class ConfigurationDeclarationKind(str, Enum):
    WORKFLOW = "workflow"
    TRIGGER = "trigger"


class ConfigurationRelationship(StrictConfigurationModel):
    kind: ConfigurationDeclarationKind
    name: str = Field(min_length=1, max_length=MAX_RELATIONSHIP_NAME_LENGTH)
    state: ConfigurationRelationshipState


class ConfigurationRelationships(StrictConfigurationModel):
    working_version: int = Field(ge=1)
    active_identity: LifecycleIdentity | None = None
    relationships: tuple[ConfigurationRelationship, ...] = Field(
        max_length=MAX_CONFIGURATION_RELATIONSHIPS
    )
    restart_required: bool = False


class ConfigurationApplyStageKind(str, Enum):
    VALIDATION = "validation"
    DEFINITION_PUBLICATION = "definition_publication"
    PUBLICATION = "publication"
    PLAN_CONFIRMATION = "plan_confirmation"
    ACTIVATION = "activation"
    READINESS = "readiness"
    PROCESS_RESTART = "process_restart"


class ConfigurationApplyStageState(str, Enum):
    COMPLETED = "completed"
    PENDING = "pending"
    RESTART_REQUIRED = "restart_required"
    NOT_APPLICABLE = "not_applicable"


class ConfigurationApplyStage(StrictConfigurationModel):
    kind: ConfigurationApplyStageKind
    state: ConfigurationApplyStageState


class LocalConfigurationApplyResult(StrictConfigurationModel):
    mode: Literal["local_source"] = "local_source"
    working_version: int = Field(ge=1)
    stages: tuple[ConfigurationApplyStage, ...] = Field(max_length=MAX_APPLY_STAGES)
    definitions_published: bool
    restart_required: bool
    running_process_changed: Literal[False] = False


class ConfigurationDiscardState(str, Enum):
    PENDING = "pending"
    APPLIED = "applied"
    FAILED = "failed"


class ConfigurationDiscardErrorCode(str, Enum):
    CONFLICT = "conflict"


class ConfigurationDiscardRecord(StrictConfigurationModel):
    discard_id: Digest
    scope_digest: Digest
    idempotency_key_digest: Digest
    request_digest: ProvenanceDigest
    actor_digest: Digest
    correlation_digest: Digest
    expected_draft_version: int = Field(ge=1)
    expected_active_identity: RevisionIdentity
    active_document_digest: ProvenanceDigest
    result_draft_version: int | None = Field(default=None, ge=1)
    definitions_published: bool | None = None
    state: ConfigurationDiscardState = ConfigurationDiscardState.PENDING
    error_code: ConfigurationDiscardErrorCode | None = None
    version: int = Field(default=1, ge=1)
    created_at: datetime
    updated_at: datetime

    @model_validator(mode="after")
    def validate_outcome(self) -> Self:
        if self.state is ConfigurationDiscardState.APPLIED:
            if self.result_draft_version is None or self.error_code is not None:
                raise ValueError("Applied discard requires exactly one draft outcome")
        elif self.state is ConfigurationDiscardState.FAILED:
            if (
                self.error_code is None
                or self.result_draft_version is not None
                or self.definitions_published is not None
            ):
                raise ValueError("Failed discard requires exactly one error outcome")
        elif (
            self.result_draft_version is not None
            or self.error_code is not None
            or self.definitions_published is not None
        ):
            raise ValueError("Pending discard cannot contain an outcome")
        if self.updated_at < self.created_at:
            raise ValueError("Discard timestamps are inconsistent")
        return self


class ConfigurationDiscardResult(StrictConfigurationModel):
    discard_id: Digest
    working_version: int = Field(ge=1)
    active_identity: RevisionIdentity
    state: Literal[ConfigurationDiscardState.APPLIED] = ConfigurationDiscardState.APPLIED
    definitions_published: bool | None = None
    restart_required: bool = False
    running_process_changed: Literal[False] = False


def build_configuration_relationships(
    *,
    working_version: int,
    active_identity: str | None,
    working: Mapping[tuple[ConfigurationDeclarationKind, str], object],
    active: Mapping[tuple[ConfigurationDeclarationKind, str], object],
    restart_required: bool = False,
) -> ConfigurationRelationships:
    keys = sorted(set(working) | set(active), key=lambda key: (key[0].value, key[1]))
    if len(keys) > MAX_CONFIGURATION_RELATIONSHIPS:
        raise ConfigurationLimitError(
            "Configuration relationship projection exceeds its item bound"
        )
    relationships = tuple(
        ConfigurationRelationship(
            kind=kind,
            name=name,
            state=_relationship_state(working.get((kind, name)), active.get((kind, name))),
        )
        for kind, name in keys
    )
    return ConfigurationRelationships(
        working_version=working_version,
        active_identity=active_identity,
        relationships=relationships,
        restart_required=restart_required,
    )


def configuration_declarations(
    *,
    workflows: Mapping[str, object],
    triggers: Mapping[str, object],
) -> dict[tuple[ConfigurationDeclarationKind, str], object]:
    declarations = {
        (ConfigurationDeclarationKind.WORKFLOW, name): declaration
        for name, declaration in workflows.items()
    }
    declarations.update(
        {
            (ConfigurationDeclarationKind.TRIGGER, name): declaration
            for name, declaration in triggers.items()
        }
    )
    return declarations


def discard_identity(scope_digest: str, idempotency_key: str) -> str:
    return safe_identity_digest(
        "configuration-discard",
        f"{scope_digest}:{idempotency_key}",
    )


def complete_discard(
    record: ConfigurationDiscardRecord,
    *,
    result_draft_version: int,
    occurred_at: datetime,
    definitions_published: bool | None = None,
) -> ConfigurationDiscardRecord:
    return ConfigurationDiscardRecord.model_validate(
        {
            **record.model_dump(),
            "result_draft_version": result_draft_version,
            "definitions_published": definitions_published,
            "state": ConfigurationDiscardState.APPLIED,
            "error_code": None,
            "version": record.version + 1,
            "updated_at": occurred_at,
        }
    )


def fail_discard(
    record: ConfigurationDiscardRecord,
    *,
    error_code: ConfigurationDiscardErrorCode,
    occurred_at: datetime,
) -> ConfigurationDiscardRecord:
    return ConfigurationDiscardRecord.model_validate(
        {
            **record.model_dump(),
            "result_draft_version": None,
            "definitions_published": None,
            "state": ConfigurationDiscardState.FAILED,
            "error_code": error_code,
            "version": record.version + 1,
            "updated_at": occurred_at,
        }
    )


def _relationship_state(
    working: object | None, active: object | None
) -> ConfigurationRelationshipState:
    if working is None:
        return ConfigurationRelationshipState.REMOVED_PENDING_APPLY
    if active is None:
        return ConfigurationRelationshipState.NEW_PENDING_APPLY
    if working == active:
        return ConfigurationRelationshipState.ACTIVE
    return ConfigurationRelationshipState.MODIFIED
